#![doc = "Runtime-neutral memory accounting, admission, and payload ownership."]

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
};

use arrow_array::RecordBatch;
use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_PROCESS_BUDGET_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub const MINIMUM_NATIVE_HEADROOM_BYTES: u64 = 512 * 1024 * 1024;
pub const NATIVE_HEADROOM_PERCENT: u64 = 15;
pub const HEADROOM_POLICY_VERSION: &str = "native-headroom-v1";

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
}

#[derive(Clone, Debug)]
pub struct AccountedBytes {
    payload: Arc<[u8]>,
    lease: MemoryLease,
}

impl AccountedBytes {
    pub fn new(payload: Arc<[u8]>, lease: MemoryLease) -> Result<Self> {
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

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn lease(&self) -> &MemoryLease {
        &self.lease
    }
}

#[derive(Clone, Debug)]
pub struct AccountedBatch {
    batch: RecordBatch,
    lease: MemoryLease,
}

impl AccountedBatch {
    pub fn new(batch: RecordBatch, lease: MemoryLease) -> Result<Self> {
        let observed = u64::try_from(batch.get_array_memory_size())
            .map_err(|_| CdfError::data("Arrow batch memory size exceeds u64"))?;
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

pub trait MemoryCoordinator: Send + Sync {
    fn try_reserve(&self, request: &ReservationRequest) -> Result<Option<MemoryLease>>;
    fn register_waiter(&self, waker: &Waker);
    fn snapshot(&self) -> MemorySnapshot;
    fn record_event(&self, event: MemoryEvent);
}

pub struct ReserveFuture {
    coordinator: Arc<dyn MemoryCoordinator>,
    request: ReservationRequest,
}

impl Future for ReserveFuture {
    type Output = Result<MemoryLease>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.coordinator.try_reserve(&self.request) {
            Ok(Some(lease)) => Poll::Ready(Ok(lease)),
            Ok(None) => {
                self.coordinator.register_waiter(context.waker());
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

pub fn reserve(
    coordinator: Arc<dyn MemoryCoordinator>,
    request: ReservationRequest,
) -> ReserveFuture {
    ReserveFuture {
        coordinator,
        request,
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
    waiters: Vec<Waker>,
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
                    waiters: Vec::new(),
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
        let mut state = self.inner.state.lock().unwrap();
        if !state
            .waiters
            .iter()
            .any(|existing| existing.will_wake(waker))
        {
            state.waiters.push(waker.clone());
        }
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
        } else if current_bytes > new_bytes {
            apply_release(
                &mut state.snapshot,
                &self.request,
                current_bytes - new_bytes,
            );
            wake_waiters(&mut state.waiters);
        }
        Ok(())
    }

    fn release(&self, bytes: u64) {
        if let Some(coordinator) = self.coordinator.upgrade() {
            let mut state = coordinator.state.lock().unwrap();
            apply_release(&mut state.snapshot, &self.request, bytes);
            wake_waiters(&mut state.waiters);
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

fn wake_waiters(waiters: &mut Vec<Waker>) {
    for waiter in std::mem::take(waiters) {
        waiter.wake();
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

pub fn resolve_memory_budget(
    requested_process_bytes: Option<u64>,
    effective_authority_bytes: u64,
    minimum_working_set_bytes: u64,
    spill_budget_bytes: u64,
) -> Result<MemoryBudgetResolution> {
    if effective_authority_bytes == 0 || minimum_working_set_bytes == 0 || spill_budget_bytes == 0 {
        return Err(CdfError::contract(
            "memory authority, minimum working set, and spill budget must be nonzero",
        ));
    }
    let authority_ceiling = effective_authority_bytes.saturating_mul(80) / 100;
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
    Ok(MemoryBudgetResolution {
        requested_process_bytes,
        effective_authority_bytes,
        process_budget_bytes,
        native_headroom_bytes,
        managed_pool_bytes,
        spill_budget_bytes,
        headroom_policy_version: HEADROOM_POLICY_VERSION.to_owned(),
    })
}

#[cfg(test)]
mod tests;
