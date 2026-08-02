use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
};

use arrow_array::{Array, RecordBatch};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use bytes::Bytes;
use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

pub(crate) fn lock_infallible<'a, T>(mutex: &'a Mutex<T>, authority: &str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(_) => panic!("{authority} lock is poisoned"),
    }
}

fn get_mut_infallible<'a, T>(mutex: &'a mut Mutex<T>, authority: &str) -> &'a mut T {
    match mutex.get_mut() {
        Ok(value) => value,
        Err(_) => panic!("{authority} lock is poisoned"),
    }
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
        let bytes = get_mut_infallible(&mut self.state, "memory lease state").bytes;
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
        lock_infallible(&self.inner.state, "memory lease state").bytes
    }

    pub fn reconcile(&self, observed_bytes: u64) -> Result<()> {
        if observed_bytes == 0 {
            return Err(CdfError::contract(
                "accounted payload cannot reconcile to zero bytes",
            ));
        }
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| CdfError::internal("memory lease state lock is poisoned"))?;
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
        let reserved = get_mut_infallible(&mut inner.state, "memory lease state").bytes;
        if required > reserved {
            return Err(CdfError::data(format!(
                "memory lease partitions require {required} bytes but the lease owns {reserved}"
            )));
        }
        let account = Arc::clone(&inner.account);
        get_mut_infallible(&mut inner.state, "memory lease state").bytes = 0;
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

    #[cfg(test)]
    pub(crate) fn poison_state_for_test(&self) {
        let inner = Arc::clone(&self.inner);
        assert!(
            std::thread::spawn(move || {
                let _state = inner.state.lock().unwrap();
                panic!("poison lease state");
            })
            .join()
            .is_err()
        );
    }

    #[cfg(test)]
    pub(crate) fn clear_state_poison_for_test(&self) {
        self.inner.state.clear_poison();
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
