#![doc = "Runtime-neutral memory accounting, admission, and payload ownership."]

mod spill;

pub use spill::*;

use std::{
    collections::BTreeMap,
    fs,
    future::Future,
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
};

use arrow_array::{Array, RecordBatch};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use bytes::Bytes;
use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_PROCESS_BUDGET_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub const DEFAULT_SPILL_BUDGET_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const MINIMUM_NATIVE_HEADROOM_BYTES: u64 = 512 * 1024 * 1024;
pub const NATIVE_HEADROOM_PERCENT: u64 = 15;
pub const HEADROOM_POLICY_VERSION: &str = "native-headroom-v1";
pub const CGROUP_V2_MEMORY_PROVIDER_VERSION: &str = "cdf-cgroup-v2-memory-v1";

#[cfg(target_os = "linux")]
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
#[cfg(target_os = "linux")]
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CgroupV2MemoryReport {
    pub root: PathBuf,
    pub max_bytes: Option<u64>,
    pub current_bytes: Option<u64>,
    pub peak_bytes: Option<u64>,
    pub events: BTreeMap<String, u64>,
    pub read_errors: BTreeMap<String, String>,
}

pub fn current_cgroup_v2_memory_report() -> std::result::Result<CgroupV2MemoryReport, String> {
    #[cfg(target_os = "linux")]
    {
        current_cgroup_v2_memory_report_from(Path::new(CGROUP_ROOT), Path::new(PROC_SELF_CGROUP))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("cgroup v2 memory reporting is only available on Linux".to_owned())
    }
}

pub fn current_cgroup_v2_memory_report_from(
    cgroup_root: &Path,
    proc_self_cgroup: &Path,
) -> std::result::Result<CgroupV2MemoryReport, String> {
    let proc_cgroup = fs::read_to_string(proc_self_cgroup)
        .map_err(|error| format!("read {}: {error}", proc_self_cgroup.display()))?;
    let relative = parse_cgroup_v2_relative_path(&proc_cgroup)?;
    Ok(cgroup_v2_memory_report_from_root(
        &cgroup_root.join(relative),
    ))
}

fn cgroup_v2_memory_report_from_root(root: &Path) -> CgroupV2MemoryReport {
    let mut read_errors = BTreeMap::new();
    let max_bytes = read_cgroup_file(root, "memory.max", &mut read_errors)
        .and_then(|value| parse_memory_max(&value).transpose())
        .transpose()
        .unwrap_or_else(|error| {
            read_errors.insert("memory.max".to_owned(), error);
            None
        });
    let current_bytes = read_cgroup_file(root, "memory.current", &mut read_errors)
        .and_then(|value| parse_nonnegative_file_u64("memory.current", &value, &mut read_errors));
    let peak_bytes = read_cgroup_file(root, "memory.peak", &mut read_errors)
        .and_then(|value| parse_nonnegative_file_u64("memory.peak", &value, &mut read_errors));
    let events = read_cgroup_file(root, "memory.events", &mut read_errors)
        .map(|value| parse_memory_events(&value, &mut read_errors))
        .unwrap_or_default();
    CgroupV2MemoryReport {
        root: root.to_path_buf(),
        max_bytes,
        current_bytes,
        peak_bytes,
        events,
        read_errors,
    }
}

fn parse_cgroup_v2_relative_path(value: &str) -> std::result::Result<PathBuf, String> {
    let path = value
        .lines()
        .find_map(|line| {
            let mut parts = line.splitn(3, ':');
            let hierarchy = parts.next()?;
            let controllers = parts.next()?;
            let path = parts.next()?;
            (hierarchy == "0" && controllers.is_empty()).then_some(path)
        })
        .ok_or_else(|| "no cgroup v2 `0::` entry found in /proc/self/cgroup".to_owned())?;
    let trimmed = path.trim_start_matches('/');
    let relative = Path::new(trimmed);
    let mut sanitized = PathBuf::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => sanitized.push(part),
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) | Component::ParentDir => {
                return Err(format!("unsafe cgroup v2 path component in `{path}`"));
            }
        }
    }
    Ok(sanitized)
}

fn read_cgroup_file(
    root: &Path,
    name: &'static str,
    read_errors: &mut BTreeMap<String, String>,
) -> Option<String> {
    match fs::read_to_string(root.join(name)) {
        Ok(value) => Some(value),
        Err(error) => {
            read_errors.insert(name.to_owned(), error.to_string());
            None
        }
    }
}

fn parse_memory_max(value: &str) -> std::result::Result<Option<u64>, String> {
    let trimmed = value.trim();
    if trimmed == "max" || trimmed.is_empty() {
        return Ok(None);
    }
    parse_positive_u64("memory.max", trimmed).map(Some)
}

fn parse_nonnegative_file_u64(
    name: &'static str,
    value: &str,
    read_errors: &mut BTreeMap<String, String>,
) -> Option<u64> {
    match value.trim().parse::<u64>() {
        Ok(value) => Some(value),
        Err(error) => {
            read_errors.insert(name.to_owned(), format!("invalid {name}: {error}"));
            None
        }
    }
}

fn parse_positive_u64(name: &'static str, value: &str) -> std::result::Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|error| format!("invalid {name}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{name} must be nonzero when bounded"));
    }
    Ok(parsed)
}

fn parse_memory_events(
    value: &str,
    read_errors: &mut BTreeMap<String, String>,
) -> BTreeMap<String, u64> {
    let mut events = BTreeMap::new();
    for (index, line) in value.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let Some(name) = parts.next() else {
            continue;
        };
        let Some(count) = parts.next() else {
            read_errors.insert(
                "memory.events".to_owned(),
                format!("line {} omitted event count", index + 1),
            );
            continue;
        };
        if parts.next().is_some() {
            read_errors.insert(
                "memory.events".to_owned(),
                format!("line {} contains more than two fields", index + 1),
            );
            continue;
        }
        match count.parse::<u64>() {
            Ok(count) => {
                events.insert(name.to_owned(), count);
            }
            Err(error) => {
                read_errors.insert(
                    "memory.events".to_owned(),
                    format!("line {} has invalid count: {error}", index + 1),
                );
            }
        }
    }
    events
}

/// Returns retained Arrow allocation bytes without counting shared backing
/// allocations once per sliced column.
pub fn record_batch_retained_bytes(batch: &RecordBatch) -> Result<u64> {
    fn record_buffer(allocations: &mut BTreeMap<usize, u64>, buffer: &Buffer) -> Result<()> {
        let allocation = buffer.data_ptr().as_ptr() as usize;
        let visible_extent = buffer
            .ptr_offset()
            .checked_add(buffer.len())
            .ok_or_else(|| CdfError::data("Arrow buffer extent overflow"))?;
        let bytes = u64::try_from(buffer.capacity().max(visible_extent))
            .map_err(|_| CdfError::data("Arrow buffer allocation exceeds u64"))?;
        allocations
            .entry(allocation)
            .and_modify(|observed| *observed = (*observed).max(bytes))
            .or_insert(bytes);
        Ok(())
    }

    fn record_data(allocations: &mut BTreeMap<usize, u64>, data: &ArrayData) -> Result<()> {
        for buffer in data.buffers() {
            record_buffer(allocations, buffer)?;
        }
        if let Some(nulls) = data.nulls() {
            record_buffer(allocations, nulls.inner().inner())?;
        }
        for child in data.child_data() {
            record_data(allocations, child)?;
        }
        Ok(())
    }

    let mut allocations = BTreeMap::new();
    let mut container_bytes = u64::try_from(std::mem::size_of::<RecordBatch>())
        .map_err(|_| CdfError::data("Arrow record batch container size exceeds u64"))?;
    for column in batch.columns() {
        let array_bytes = column.get_array_memory_size();
        let buffer_bytes = column.get_buffer_memory_size();
        container_bytes = container_bytes
            .checked_add(
                u64::try_from(array_bytes.saturating_sub(buffer_bytes))
                    .map_err(|_| CdfError::data("Arrow array container memory exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("Arrow container memory overflow"))?;
        record_data(&mut allocations, &column.to_data())?;
    }
    allocations
        .values()
        .try_fold(container_bytes, |total, bytes| {
            total
                .checked_add(*bytes)
                .ok_or_else(|| CdfError::data("Arrow retained memory overflow"))
        })
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct ConsumerKey {
    pub name: String,
    pub class: MemoryClass,
}

impl From<ConsumerKey> for String {
    fn from(value: ConsumerKey) -> Self {
        format!("{}:{}", value.class.as_str(), value.name)
    }
}

impl TryFrom<String> for ConsumerKey {
    type Error = CdfError;

    fn try_from(value: String) -> Result<Self> {
        let (class, name) = value.split_once(':').ok_or_else(|| {
            CdfError::contract("serialized memory consumer key requires `class:name`")
        })?;
        Self::new(name, MemoryClass::from_str(class)?)
    }
}

impl ConsumerKey {
    pub fn new(name: impl Into<String>, class: MemoryClass) -> Result<Self> {
        let name = name.into();
        if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
            return Err(CdfError::contract(
                "memory consumer name must contain 1..=256 non-control characters",
            ));
        }
        Ok(Self { name, class })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryClass {
    Source,
    Decode,
    Transform,
    Validation,
    Queue,
    Package,
    Destination,
    Discovery,
    Control,
    QueryEngine,
}

impl MemoryClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Decode => "decode",
            Self::Transform => "transform",
            Self::Validation => "validation",
            Self::Queue => "queue",
            Self::Package => "package",
            Self::Destination => "destination",
            Self::Discovery => "discovery",
            Self::Control => "control",
            Self::QueryEngine => "query_engine",
        }
    }

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "source" => Ok(Self::Source),
            "decode" => Ok(Self::Decode),
            "transform" => Ok(Self::Transform),
            "validation" => Ok(Self::Validation),
            "queue" => Ok(Self::Queue),
            "package" => Ok(Self::Package),
            "destination" => Ok(Self::Destination),
            "discovery" => Ok(Self::Discovery),
            "control" => Ok(Self::Control),
            "query_engine" => Ok(Self::QueryEngine),
            _ => Err(CdfError::contract(format!(
                "unknown memory consumer class `{value}`"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BudgetTag(String);

impl BudgetTag {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(CdfError::contract(
                "memory budget tag must contain 1..=128 ASCII alphanumeric, `-`, `_`, or `.` bytes",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReservationRequest {
    pub consumer: ConsumerKey,
    pub bytes: u64,
    pub subcap: Option<BudgetTag>,
    pub minimum_working_set: bool,
}

impl ReservationRequest {
    pub fn new(consumer: ConsumerKey, bytes: u64) -> Result<Self> {
        if bytes == 0 {
            return Err(CdfError::contract(
                "memory reservation must request at least one byte",
            ));
        }
        Ok(Self {
            consumer,
            bytes,
            subcap: None,
            minimum_working_set: false,
        })
    }

    pub fn with_subcap(mut self, subcap: BudgetTag) -> Self {
        self.subcap = Some(subcap);
        self
    }

    pub fn as_minimum_working_set(mut self) -> Self {
        self.minimum_working_set = true;
        self
    }
}

pub trait LeaseAccount: Send + Sync {
    fn resize(&self, current_bytes: u64, new_bytes: u64) -> Result<()>;
    fn release(&self, bytes: u64);
}

struct LeaseState {
    bytes: u64,
}

struct LeaseInner {
    account: Arc<dyn LeaseAccount>,
    state: Mutex<LeaseState>,
}

impl Drop for LeaseInner {
    fn drop(&mut self) {
        let bytes = self
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .bytes;
        self.account.release(bytes);
    }
}

#[derive(Clone)]
pub struct MemoryLease {
    inner: Arc<LeaseInner>,
}

impl std::fmt::Debug for MemoryLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryLease")
            .field("bytes", &self.bytes())
            .finish_non_exhaustive()
    }
}

impl MemoryLease {
    pub fn from_account(bytes: u64, account: Arc<dyn LeaseAccount>) -> Result<Self> {
        if bytes == 0 {
            return Err(CdfError::contract("memory lease cannot own zero bytes"));
        }
        Ok(Self {
            inner: Arc::new(LeaseInner {
                account,
                state: Mutex::new(LeaseState { bytes }),
            }),
        })
    }

    pub fn bytes(&self) -> u64 {
        self.inner.state.lock().unwrap().bytes
    }

    pub fn reconcile(&self, observed_bytes: u64) -> Result<()> {
        if observed_bytes == 0 {
            return Err(CdfError::contract(
                "accounted payload cannot reconcile to zero bytes",
            ));
        }
        let mut state = self.inner.state.lock().unwrap();
        self.inner.account.resize(state.bytes, observed_bytes)?;
        state.bytes = observed_bytes;
        Ok(())
    }

    /// Transfers one exclusive reservation into independently owned payload leases.
    ///
    /// This is the atomic-publication path for codecs that must validate a complete
    /// decode unit before exposing any of its batches. The caller reserves the unit's
    /// output authority once, builds every payload under that authority, and then
    /// partitions the reservation without a second admission cycle. Unused authority
    /// is released immediately.
    pub fn into_partitions(self, partition_bytes: Vec<u64>) -> Result<Vec<Self>> {
        if partition_bytes.is_empty() || partition_bytes.contains(&0) {
            return Err(CdfError::contract(
                "memory lease partitions must be nonempty and individually nonzero",
            ));
        }
        let required = partition_bytes.iter().try_fold(0_u64, |total, bytes| {
            total
                .checked_add(*bytes)
                .ok_or_else(|| CdfError::data("memory lease partition total overflowed"))
        })?;
        let mut inner = Arc::try_unwrap(self.inner).map_err(|_| {
            CdfError::contract("only an exclusively owned memory lease can be partitioned")
        })?;
        let reserved = inner
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .bytes;
        if required > reserved {
            return Err(CdfError::data(format!(
                "memory lease partitions require {required} bytes but the lease owns {reserved}"
            )));
        }
        let account = Arc::clone(&inner.account);
        inner
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .bytes = 0;
        account.release(reserved - required);
        Ok(partition_bytes
            .into_iter()
            .map(|bytes| Self {
                inner: Arc::new(LeaseInner {
                    account: Arc::clone(&account),
                    state: Mutex::new(LeaseState { bytes }),
                }),
            })
            .collect())
    }
}

#[derive(Clone, Debug)]
pub struct AccountedBytes {
    payload: Bytes,
    lease: MemoryLease,
}

impl AccountedBytes {
    pub fn new(payload: Bytes, lease: MemoryLease) -> Result<Self> {
        let observed = u64::try_from(payload.len())
            .map_err(|_| CdfError::data("byte payload length exceeds u64"))?;
        if observed == 0 || lease.bytes() < observed {
            return Err(CdfError::data(format!(
                "byte payload requires {observed} accounted bytes but lease holds {}",
                lease.bytes()
            )));
        }
        lease.reconcile(observed)?;
        Ok(Self { payload, lease })
    }

    /// Creates an accounted payload while deliberately retaining a conservative reservation.
    ///
    /// Streaming transports use this when the provider does not declare a body length before
    /// allocation. The complete configured receive window remains charged for the payload's
    /// lifetime because `Bytes` does not expose the capacity of its backing allocation.
    pub fn new_conservative(payload: Bytes, lease: MemoryLease) -> Result<Self> {
        let observed = u64::try_from(payload.len())
            .map_err(|_| CdfError::data("byte payload length exceeds u64"))?;
        if observed == 0 || lease.bytes() < observed {
            return Err(CdfError::data(format!(
                "byte payload requires {observed} accounted bytes but lease holds {}",
                lease.bytes()
            )));
        }
        Ok(Self { payload, lease })
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    pub fn lease(&self) -> &MemoryLease {
        &self.lease
    }

    /// Transfers this payload into a zero-copy `Bytes` owner while retaining its lease.
    ///
    /// Foreign readers that accept `Bytes` can therefore hold a CDF-managed transport buffer
    /// without copying it or escaping the memory ledger. The reservation is released only after
    /// the final clone or slice of the returned `Bytes` is dropped.
    pub fn into_retained_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }

    /// Returns a zero-copy logical slice while retaining the lease for the complete
    /// physical allocation. This is intentionally conservative: coalesced I/O is
    /// accounted until every logical slice of the response has been released.
    pub fn slice(&self, range: std::ops::Range<usize>) -> Result<Self> {
        if range.start >= range.end || range.end > self.payload.len() {
            return Err(CdfError::contract(
                "accounted byte slice requires a nonempty in-bounds range",
            ));
        }
        Ok(Self {
            payload: self.payload.slice(range),
            lease: self.lease.clone(),
        })
    }
}

impl AsRef<[u8]> for AccountedBytes {
    fn as_ref(&self) -> &[u8] {
        self.payload()
    }
}

#[derive(Clone, Debug)]
pub struct AccountedBatch {
    batch: RecordBatch,
    lease: MemoryLease,
}

impl AccountedBatch {
    pub fn new(batch: RecordBatch, lease: MemoryLease) -> Result<Self> {
        let observed = record_batch_retained_bytes(&batch)?;
        if observed == 0 || lease.bytes() < observed {
            return Err(CdfError::data(format!(
                "Arrow batch requires {observed} accounted bytes but lease holds {}",
                lease.bytes()
            )));
        }
        lease.reconcile(observed)?;
        Ok(Self { batch, lease })
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub fn lease(&self) -> &MemoryLease {
        &self.lease
    }

    pub fn into_parts(self) -> (RecordBatch, MemoryLease) {
        (self.batch, self.lease)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerMemorySnapshot {
    pub current_bytes: u64,
    pub peak_bytes: u64,
    pub waits: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySnapshot {
    pub budget_bytes: u64,
    pub current_bytes: u64,
    pub peak_bytes: u64,
    pub flushes: u64,
    pub spill_bytes: u64,
    pub spill_count: u64,
    pub consumers: BTreeMap<ConsumerKey, ConsumerMemorySnapshot>,
    pub subcaps: BTreeMap<BudgetTag, ConsumerMemorySnapshot>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryEvent {
    Flush,
    Spill { bytes: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PressureStrategy {
    Backpressure,
    Flush,
    Spill,
    Fixed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorMemoryProfile {
    pub minimum_working_set_bytes: u64,
    pub maximum_operation_bytes: u64,
    pub pressure_strategy: PressureStrategy,
    pub pausable: bool,
}

impl OperatorMemoryProfile {
    pub fn new(
        minimum_working_set_bytes: u64,
        maximum_operation_bytes: u64,
        pressure_strategy: PressureStrategy,
        pausable: bool,
    ) -> Result<Self> {
        if minimum_working_set_bytes == 0
            || maximum_operation_bytes == 0
            || minimum_working_set_bytes > maximum_operation_bytes
        {
            return Err(CdfError::contract(
                "operator memory profile requires 0 < minimum working set <= maximum operation bytes",
            ));
        }
        if !pausable && !matches!(pressure_strategy, PressureStrategy::Spill) {
            return Err(CdfError::contract(
                "a non-pausable operator must declare spill as its pressure strategy",
            ));
        }
        Ok(Self {
            minimum_working_set_bytes,
            maximum_operation_bytes,
            pressure_strategy,
            pausable,
        })
    }

    pub fn poll_request(&self, consumer: ConsumerKey) -> Result<ReservationRequest> {
        Ok(
            ReservationRequest::new(consumer, self.maximum_operation_bytes)?
                .as_minimum_working_set(),
        )
    }

    pub fn verify_observed_operation(&self, observed_bytes: u64) -> Result<()> {
        if observed_bytes > self.maximum_operation_bytes {
            return Err(CdfError::contract(format!(
                "operator retained {observed_bytes} bytes but declared a maximum operation working set of {} bytes",
                self.maximum_operation_bytes
            )));
        }
        Ok(())
    }
}

pub trait MemoryCoordinator: Send + Sync {
    fn try_reserve(&self, request: &ReservationRequest) -> Result<Option<MemoryLease>>;
    fn register_waiter(&self, waker: &Waker);
    fn unregister_waiter(&self, waker: &Waker);
    fn snapshot(&self) -> MemorySnapshot;
    fn record_event(&self, event: MemoryEvent);
}

#[derive(Debug, Default)]
pub struct MemoryWaiterSet {
    waiters: Vec<RegisteredMemoryWaiter>,
}

#[derive(Debug)]
struct RegisteredMemoryWaiter {
    waker: Waker,
    registrations: usize,
}

impl MemoryWaiterSet {
    pub fn register(&mut self, waker: &Waker) {
        if let Some(waiter) = self
            .waiters
            .iter_mut()
            .find(|waiter| waiter.waker.will_wake(waker))
        {
            waiter.registrations = waiter.registrations.saturating_add(1);
        } else {
            self.waiters.push(RegisteredMemoryWaiter {
                waker: waker.clone(),
                registrations: 1,
            });
        }
    }

    pub fn unregister(&mut self, waker: &Waker) {
        if let Some(index) = self
            .waiters
            .iter()
            .position(|waiter| waiter.waker.will_wake(waker))
        {
            if self.waiters[index].registrations == 1 {
                self.waiters.swap_remove(index);
            } else {
                self.waiters[index].registrations -= 1;
            }
        }
    }

    /// Removes every registered task and returns its waker for invocation after
    /// the coordinator state lock has been released. A waker is arbitrary user
    /// code and may immediately attempt another reservation.
    pub fn take_all(&mut self) -> Vec<Waker> {
        std::mem::take(&mut self.waiters)
            .into_iter()
            .map(|waiter| waiter.waker)
            .collect()
    }
}

pub struct ReserveFuture {
    coordinator: Arc<dyn MemoryCoordinator>,
    request: ReservationRequest,
    registered_waker: Option<Waker>,
}

impl ReserveFuture {
    fn register_waiter(&mut self, waker: &Waker) {
        if self
            .registered_waker
            .as_ref()
            .is_some_and(|registered| registered.will_wake(waker))
        {
            return;
        }
        self.clear_waiter();
        self.coordinator.register_waiter(waker);
        self.registered_waker = Some(waker.clone());
    }

    fn clear_waiter(&mut self) {
        if let Some(waker) = self.registered_waker.take() {
            self.coordinator.unregister_waiter(&waker);
        }
    }
}

impl Future for ReserveFuture {
    type Output = Result<MemoryLease>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        // A wake consumes the coordinator's registration. Clear any previous local record
        // before rechecking so a still-blocked future always registers again in this poll.
        this.clear_waiter();
        match this.coordinator.try_reserve(&this.request) {
            Ok(Some(lease)) => {
                this.clear_waiter();
                Poll::Ready(Ok(lease))
            }
            Ok(None) => {
                this.register_waiter(context.waker());
                // Close the check/register race: capacity can be released after the first
                // attempt but before this waker is visible to the coordinator. Once registered,
                // a second attempt either acquires that capacity or is guaranteed a later wake.
                match this.coordinator.try_reserve(&this.request) {
                    Ok(Some(lease)) => {
                        this.clear_waiter();
                        Poll::Ready(Ok(lease))
                    }
                    Ok(None) => Poll::Pending,
                    Err(error) => {
                        this.clear_waiter();
                        Poll::Ready(Err(error))
                    }
                }
            }
            Err(error) => {
                this.clear_waiter();
                Poll::Ready(Err(error))
            }
        }
    }
}

impl Drop for ReserveFuture {
    fn drop(&mut self) {
        self.clear_waiter();
    }
}

pub fn reserve(
    coordinator: Arc<dyn MemoryCoordinator>,
    request: ReservationRequest,
) -> ReserveFuture {
    ReserveFuture {
        coordinator,
        request,
        registered_waker: None,
    }
}

pub fn reserve_blocking(
    coordinator: Arc<dyn MemoryCoordinator>,
    request: &ReservationRequest,
) -> Result<MemoryLease> {
    struct ThreadWake(std::thread::Thread);

    impl std::task::Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    loop {
        if let Some(lease) = coordinator.try_reserve(request)? {
            coordinator.unregister_waiter(&waker);
            return Ok(lease);
        }
        coordinator.register_waiter(&waker);
        if let Some(lease) = coordinator.try_reserve(request)? {
            coordinator.unregister_waiter(&waker);
            return Ok(lease);
        }
        std::thread::park();
    }
}

#[derive(Clone, Debug)]
pub struct DeterministicMemoryCoordinator {
    inner: Arc<CoordinatorInner>,
}

#[derive(Debug)]
struct CoordinatorInner {
    state: Mutex<CoordinatorState>,
}

#[derive(Debug)]
struct CoordinatorState {
    snapshot: MemorySnapshot,
    subcap_limits: BTreeMap<BudgetTag, u64>,
    waiters: MemoryWaiterSet,
}

impl DeterministicMemoryCoordinator {
    pub fn new(budget_bytes: u64, subcap_limits: BTreeMap<BudgetTag, u64>) -> Result<Self> {
        if budget_bytes == 0 || subcap_limits.values().any(|limit| *limit == 0) {
            return Err(CdfError::contract(
                "memory coordinator and sub-cap budgets must be nonzero",
            ));
        }
        if subcap_limits.values().any(|limit| *limit > budget_bytes) {
            return Err(CdfError::contract(
                "memory sub-cap cannot exceed the shared managed budget",
            ));
        }
        Ok(Self {
            inner: Arc::new(CoordinatorInner {
                state: Mutex::new(CoordinatorState {
                    snapshot: MemorySnapshot {
                        budget_bytes,
                        ..MemorySnapshot::default()
                    },
                    subcap_limits,
                    waiters: MemoryWaiterSet::default(),
                }),
            }),
        })
    }
}

impl MemoryCoordinator for DeterministicMemoryCoordinator {
    fn try_reserve(&self, request: &ReservationRequest) -> Result<Option<MemoryLease>> {
        let mut state = self.inner.state.lock().unwrap();
        if let Some(tag) = &request.subcap
            && !state.subcap_limits.contains_key(tag)
        {
            return Err(CdfError::contract(format!(
                "memory sub-cap `{}` is not declared by the coordinator",
                tag.as_str()
            )));
        }
        if request.bytes > state.snapshot.budget_bytes {
            return Err(CdfError::data(format!(
                "memory working set {} bytes exceeds managed budget {} bytes",
                request.bytes, state.snapshot.budget_bytes
            )));
        }
        let total_available = state
            .snapshot
            .budget_bytes
            .saturating_sub(state.snapshot.current_bytes);
        let subcap_available = request.subcap.as_ref().map(|tag| {
            state
                .subcap_limits
                .get(tag)
                .copied()
                .unwrap_or(0)
                .saturating_sub(
                    state
                        .snapshot
                        .subcaps
                        .get(tag)
                        .map(|usage| usage.current_bytes)
                        .unwrap_or(0),
                )
        });
        if total_available < request.bytes || subcap_available.is_some_and(|v| v < request.bytes) {
            state
                .snapshot
                .consumers
                .entry(request.consumer.clone())
                .or_default()
                .waits += 1;
            return Ok(None);
        }
        apply_growth(&mut state.snapshot, request, request.bytes);
        let account: Arc<dyn LeaseAccount> = Arc::new(DeterministicLeaseAccount {
            coordinator: Arc::downgrade(&self.inner),
            request: request.clone(),
        });
        drop(state);
        Ok(Some(MemoryLease::from_account(request.bytes, account)?))
    }

    fn register_waiter(&self, waker: &Waker) {
        self.inner.state.lock().unwrap().waiters.register(waker);
    }

    fn unregister_waiter(&self, waker: &Waker) {
        self.inner.state.lock().unwrap().waiters.unregister(waker);
    }

    fn snapshot(&self) -> MemorySnapshot {
        self.inner.state.lock().unwrap().snapshot.clone()
    }

    fn record_event(&self, event: MemoryEvent) {
        let mut state = self.inner.state.lock().unwrap();
        match event {
            MemoryEvent::Flush => state.snapshot.flushes += 1,
            MemoryEvent::Spill { bytes } => {
                state.snapshot.spill_count += 1;
                state.snapshot.spill_bytes = state.snapshot.spill_bytes.saturating_add(bytes);
            }
        }
    }
}

struct DeterministicLeaseAccount {
    coordinator: Weak<CoordinatorInner>,
    request: ReservationRequest,
}

impl LeaseAccount for DeterministicLeaseAccount {
    fn resize(&self, current_bytes: u64, new_bytes: u64) -> Result<()> {
        let Some(coordinator) = self.coordinator.upgrade() else {
            return Err(CdfError::internal(
                "memory coordinator was dropped before its lease",
            ));
        };
        let waiters = {
            let mut state = coordinator.state.lock().unwrap();
            if let Some(tag) = &self.request.subcap
                && !state.subcap_limits.contains_key(tag)
            {
                return Err(CdfError::contract(format!(
                    "memory sub-cap `{}` is not declared by the coordinator",
                    tag.as_str()
                )));
            }
            if new_bytes > current_bytes {
                let additional = new_bytes - current_bytes;
                let available = state
                    .snapshot
                    .budget_bytes
                    .saturating_sub(state.snapshot.current_bytes);
                let subcap_available = self.request.subcap.as_ref().map(|tag| {
                    state
                        .subcap_limits
                        .get(tag)
                        .copied()
                        .unwrap_or(0)
                        .saturating_sub(
                            state
                                .snapshot
                                .subcaps
                                .get(tag)
                                .map(|usage| usage.current_bytes)
                                .unwrap_or(0),
                        )
                });
                if available < additional || subcap_available.is_some_and(|v| v < additional) {
                    return Err(CdfError::data(format!(
                        "memory lease growth by {additional} bytes exceeds available managed capacity"
                    )));
                }
                apply_growth(&mut state.snapshot, &self.request, additional);
                Vec::new()
            } else if current_bytes > new_bytes {
                apply_release(
                    &mut state.snapshot,
                    &self.request,
                    current_bytes - new_bytes,
                );
                state.waiters.take_all()
            } else {
                Vec::new()
            }
        };
        for waiter in waiters {
            waiter.wake();
        }
        Ok(())
    }

    fn release(&self, bytes: u64) {
        if let Some(coordinator) = self.coordinator.upgrade() {
            let waiters = {
                let mut state = coordinator.state.lock().unwrap();
                apply_release(&mut state.snapshot, &self.request, bytes);
                state.waiters.take_all()
            };
            for waiter in waiters {
                waiter.wake();
            }
        }
    }
}

fn apply_growth(snapshot: &mut MemorySnapshot, request: &ReservationRequest, bytes: u64) {
    snapshot.current_bytes += bytes;
    snapshot.peak_bytes = snapshot.peak_bytes.max(snapshot.current_bytes);
    let consumer = snapshot
        .consumers
        .entry(request.consumer.clone())
        .or_default();
    consumer.current_bytes += bytes;
    consumer.peak_bytes = consumer.peak_bytes.max(consumer.current_bytes);
    if let Some(tag) = &request.subcap {
        let subcap = snapshot.subcaps.entry(tag.clone()).or_default();
        subcap.current_bytes += bytes;
        subcap.peak_bytes = subcap.peak_bytes.max(subcap.current_bytes);
    }
}

fn apply_release(snapshot: &mut MemorySnapshot, request: &ReservationRequest, bytes: u64) {
    snapshot.current_bytes = snapshot.current_bytes.saturating_sub(bytes);
    if let Some(consumer) = snapshot.consumers.get_mut(&request.consumer) {
        consumer.current_bytes = consumer.current_bytes.saturating_sub(bytes);
    }
    if let Some(tag) = &request.subcap
        && let Some(subcap) = snapshot.subcaps.get_mut(tag)
    {
        subcap.current_bytes = subcap.current_bytes.saturating_sub(bytes);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryBudgetResolution {
    pub requested_process_bytes: Option<u64>,
    pub effective_authority_bytes: u64,
    pub process_budget_bytes: u64,
    pub native_headroom_bytes: u64,
    pub managed_pool_bytes: u64,
    pub spill_budget_bytes: u64,
    pub headroom_policy_version: String,
}

impl MemoryBudgetResolution {
    pub fn validate(&self) -> Result<()> {
        if self.effective_authority_bytes == 0
            || self.process_budget_bytes == 0
            || self.native_headroom_bytes == 0
            || self.managed_pool_bytes == 0
            || self.spill_budget_bytes == 0
            || self.headroom_policy_version.is_empty()
        {
            return Err(CdfError::contract(
                "memory budget resolution requires nonzero authority, process, native headroom, managed pool, spill, and policy version",
            ));
        }
        if self.process_budget_bytes > self.effective_authority_bytes
            || self.requested_process_bytes.is_some_and(|requested| {
                requested != self.process_budget_bytes || requested > self.effective_authority_bytes
            })
            || self
                .managed_pool_bytes
                .checked_add(self.native_headroom_bytes)
                != Some(self.process_budget_bytes)
        {
            return Err(CdfError::contract(
                "memory budget resolution is internally inconsistent",
            ));
        }
        Ok(())
    }
}

pub fn resolve_memory_budget(
    requested_process_bytes: Option<u64>,
    effective_authority_bytes: u64,
    minimum_working_set_bytes: u64,
    spill_budget_bytes: u64,
) -> Result<MemoryBudgetResolution> {
    resolve_memory_budget_inner(
        requested_process_bytes,
        effective_authority_bytes,
        true,
        minimum_working_set_bytes,
        spill_budget_bytes,
    )
}

pub fn resolve_unenforced_memory_budget(
    requested_process_bytes: Option<u64>,
    effective_policy_bytes: u64,
    minimum_working_set_bytes: u64,
    spill_budget_bytes: u64,
) -> Result<MemoryBudgetResolution> {
    resolve_memory_budget_inner(
        requested_process_bytes,
        effective_policy_bytes,
        false,
        minimum_working_set_bytes,
        spill_budget_bytes,
    )
}

fn resolve_memory_budget_inner(
    requested_process_bytes: Option<u64>,
    effective_authority_bytes: u64,
    reserve_external_authority_margin: bool,
    minimum_working_set_bytes: u64,
    spill_budget_bytes: u64,
) -> Result<MemoryBudgetResolution> {
    if effective_authority_bytes == 0 || minimum_working_set_bytes == 0 || spill_budget_bytes == 0 {
        return Err(CdfError::contract(
            "memory authority, minimum working set, and spill budget must be nonzero",
        ));
    }
    let authority_ceiling = if reserve_external_authority_margin {
        effective_authority_bytes.saturating_mul(80) / 100
    } else {
        effective_authority_bytes
    };
    let process_budget_bytes = match requested_process_bytes {
        Some(requested) if requested > effective_authority_bytes => {
            return Err(CdfError::contract(format!(
                "requested process memory budget {requested} exceeds effective authority {effective_authority_bytes}"
            )));
        }
        Some(requested) => requested,
        None => DEFAULT_PROCESS_BUDGET_BYTES.min(authority_ceiling),
    };
    let native_headroom_bytes = MINIMUM_NATIVE_HEADROOM_BYTES
        .max(process_budget_bytes.saturating_mul(NATIVE_HEADROOM_PERCENT) / 100);
    let managed_pool_bytes = process_budget_bytes
        .checked_sub(native_headroom_bytes)
        .filter(|managed| *managed >= minimum_working_set_bytes)
        .ok_or_else(|| {
            CdfError::data(format!(
                "process memory budget {process_budget_bytes} leaves less than the {minimum_working_set_bytes}-byte minimum working set after {native_headroom_bytes} bytes of native headroom; raise the budget or reduce the working set"
            ))
        })?;
    let resolution = MemoryBudgetResolution {
        requested_process_bytes,
        effective_authority_bytes,
        process_budget_bytes,
        native_headroom_bytes,
        managed_pool_bytes,
        spill_budget_bytes,
        headroom_policy_version: HEADROOM_POLICY_VERSION.to_owned(),
    };
    resolution.validate()?;
    Ok(resolution)
}

#[cfg(test)]
mod tests;
