use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    sync::{Arc, Mutex, Weak},
    task::Waker,
};

use cdf_kernel::{CdfError, Result};
use cdf_memory::{
    BudgetTag, LeaseAccount, MemoryClass, MemoryCoordinator, MemoryEvent, MemoryLease,
    MemorySnapshot, MemoryWaiterSet, ReservationRequest,
};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};

#[derive(Clone)]
pub struct DataFusionMemoryCoordinator {
    pool: Arc<dyn MemoryPool>,
    inner: Arc<CoordinatorInner>,
}

impl std::fmt::Debug for DataFusionMemoryCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DataFusionMemoryCoordinator")
            .field("pool", &self.pool.name())
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

struct CoordinatorInner {
    state: Mutex<CoordinatorState>,
}

struct CoordinatorState {
    snapshot: MemorySnapshot,
    subcap_limits: BTreeMap<BudgetTag, u64>,
    waiters: MemoryWaiterSet,
}

impl DataFusionMemoryCoordinator {
    pub fn new(pool: Arc<dyn MemoryPool>, subcap_limits: BTreeMap<BudgetTag, u64>) -> Result<Self> {
        let budget_bytes = match pool.memory_limit() {
            MemoryLimit::Finite(limit) => u64::try_from(limit)
                .map_err(|_| CdfError::contract("DataFusion memory limit exceeds u64"))?,
            MemoryLimit::Infinite | MemoryLimit::Unknown => {
                return Err(CdfError::contract(
                    "CDF requires a finite DataFusion memory pool",
                ));
            }
        };
        if budget_bytes == 0
            || subcap_limits
                .values()
                .any(|limit| *limit == 0 || *limit > budget_bytes)
        {
            return Err(CdfError::contract(
                "DataFusion memory pool and borrowing sub-caps must be finite, nonzero, and within the shared budget",
            ));
        }
        Ok(Self {
            pool,
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

    pub fn pool(&self) -> Arc<dyn MemoryPool> {
        Arc::new(self.clone())
    }
}

impl Display for DataFusionMemoryCoordinator {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "cdf-coordinated({})", self.pool.name())
    }
}

impl MemoryPool for DataFusionMemoryCoordinator {
    fn name(&self) -> &str {
        "cdf_coordinated"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.pool.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.pool.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.pool.grow(reservation, additional);
        self.record_external_growth(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.pool.shrink(reservation, shrink);
        self.record_external_release(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::common::Result<()> {
        self.pool.try_grow(reservation, additional)?;
        self.record_external_growth(reservation, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.pool.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.pool.memory_limit()
    }
}

impl DataFusionMemoryCoordinator {
    fn record_external_growth(&self, reservation: &MemoryReservation, bytes: usize) {
        if let Ok(bytes) = u64::try_from(bytes) {
            let mut state = self.inner.state.lock().unwrap();
            state.snapshot.current_bytes = state.snapshot.current_bytes.saturating_add(bytes);
            state.snapshot.peak_bytes = state.snapshot.peak_bytes.max(state.snapshot.current_bytes);
            if let Some(key) = datafusion_consumer_key(reservation) {
                let usage = state.snapshot.consumers.entry(key).or_default();
                usage.current_bytes = usage.current_bytes.saturating_add(bytes);
                usage.peak_bytes = usage.peak_bytes.max(usage.current_bytes);
            }
        }
    }

    fn record_external_release(&self, reservation: &MemoryReservation, bytes: usize) {
        if let Ok(bytes) = u64::try_from(bytes) {
            let waiters = {
                let mut state = self.inner.state.lock().unwrap();
                state.snapshot.current_bytes = state.snapshot.current_bytes.saturating_sub(bytes);
                if let Some(key) = datafusion_consumer_key(reservation)
                    && let Some(usage) = state.snapshot.consumers.get_mut(&key)
                {
                    usage.current_bytes = usage.current_bytes.saturating_sub(bytes);
                }
                state.waiters.take_all()
            };
            for waiter in waiters {
                waiter.wake();
            }
        }
    }
}

fn datafusion_consumer_key(reservation: &MemoryReservation) -> Option<cdf_memory::ConsumerKey> {
    let name = reservation
        .consumer()
        .name()
        .chars()
        .filter(|character| !character.is_control())
        .take(240)
        .collect::<String>();
    cdf_memory::ConsumerKey::new(format!("datafusion/{name}"), MemoryClass::QueryEngine).ok()
}

impl MemoryCoordinator for DataFusionMemoryCoordinator {
    fn try_reserve(&self, request: &ReservationRequest) -> Result<Option<MemoryLease>> {
        let bytes = usize::try_from(request.bytes)
            .map_err(|_| CdfError::data("memory reservation exceeds platform usize"))?;
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
        if subcap_available(&state, request) < request.bytes {
            note_wait(&mut state.snapshot, request);
            return Ok(None);
        }
        let reservation = MemoryConsumer::new(format!(
            "cdf/{:?}/{}",
            request.consumer.class, request.consumer.name
        ))
        .with_can_spill(class_can_spill(request.consumer.class))
        .register(&self.pool);
        if reservation.try_grow(bytes).is_err() {
            note_wait(&mut state.snapshot, request);
            return Ok(None);
        }
        apply_growth(&mut state.snapshot, request, request.bytes);
        let account: Arc<dyn LeaseAccount> = Arc::new(DataFusionLeaseAccount {
            coordinator: Arc::downgrade(&self.inner),
            request: request.clone(),
            reservation,
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

struct DataFusionLeaseAccount {
    coordinator: Weak<CoordinatorInner>,
    request: ReservationRequest,
    reservation: MemoryReservation,
}

impl LeaseAccount for DataFusionLeaseAccount {
    fn resize(&self, current_bytes: u64, new_bytes: u64) -> Result<()> {
        let coordinator = self.coordinator.upgrade().ok_or_else(|| {
            CdfError::internal("DataFusion memory coordinator was dropped before its lease")
        })?;
        let waiters = {
            let mut state = coordinator.state.lock().unwrap();
            if new_bytes > current_bytes {
                let additional = new_bytes - current_bytes;
                if subcap_available(&state, &self.request) < additional {
                    return Err(CdfError::data(format!(
                        "memory lease growth by {additional} bytes exceeds its borrowing sub-cap"
                    )));
                }
                self.reservation
                    .try_grow(usize::try_from(additional).map_err(|_| {
                        CdfError::data("memory lease growth exceeds platform usize")
                    })?)
                    .map_err(|error| {
                        CdfError::data(format!(
                            "memory lease growth by {additional} bytes exceeds shared DataFusion capacity: {error}"
                        ))
                    })?;
                apply_growth(&mut state.snapshot, &self.request, additional);
                Vec::new()
            } else if current_bytes > new_bytes {
                let released = current_bytes - new_bytes;
                self.reservation
                    .shrink(usize::try_from(released).map_err(|_| {
                        CdfError::internal("memory lease release exceeds platform usize")
                    })?);
                apply_release(&mut state.snapshot, &self.request, released);
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
        self.reservation.free();
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

fn class_can_spill(class: MemoryClass) -> bool {
    matches!(
        class,
        MemoryClass::Transform
            | MemoryClass::Validation
            | MemoryClass::Queue
            | MemoryClass::Package
            | MemoryClass::Destination
    )
}

fn subcap_available(state: &CoordinatorState, request: &ReservationRequest) -> u64 {
    request.subcap.as_ref().map_or(u64::MAX, |tag| {
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
    })
}

fn note_wait(snapshot: &mut MemorySnapshot, request: &ReservationRequest) {
    snapshot
        .consumers
        .entry(request.consumer.clone())
        .or_default()
        .waits += 1;
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

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest};
    use datafusion::execution::memory_pool::GreedyMemoryPool;

    #[test]
    fn cdf_and_datafusion_consumers_compete_for_the_same_finite_pool() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128));
        let coordinator =
            DataFusionMemoryCoordinator::new(Arc::clone(&pool), BTreeMap::new()).unwrap();
        let shared_pool = coordinator.pool();
        let datafusion = MemoryConsumer::new("datafusion-test").register(&shared_pool);
        datafusion.try_grow(80).unwrap();
        assert_eq!(coordinator.snapshot().current_bytes, 80);
        assert!(coordinator.snapshot().consumers.iter().any(|(key, usage)| {
            key.name == "datafusion/datafusion-test"
                && key.class == MemoryClass::QueryEngine
                && usage.current_bytes == 80
                && usage.peak_bytes == 80
        }));

        let request = ReservationRequest::new(
            ConsumerKey::new("cdf-test", MemoryClass::Decode).unwrap(),
            64,
        )
        .unwrap();
        assert!(coordinator.try_reserve(&request).unwrap().is_none());
        datafusion.free();
        assert_eq!(coordinator.snapshot().current_bytes, 0);
        let lease = coordinator.try_reserve(&request).unwrap().unwrap();
        assert_eq!(pool.reserved(), 64);
        drop(lease);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn adapter_rejects_unbounded_pool_and_reconciles_subcap() {
        let unbounded: Arc<dyn MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        assert!(DataFusionMemoryCoordinator::new(unbounded, BTreeMap::new()).is_err());

        let tag = BudgetTag::new("discovery.metadata").unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256));
        let coordinator = DataFusionMemoryCoordinator::new(
            Arc::clone(&pool),
            BTreeMap::from([(tag.clone(), 64)]),
        )
        .unwrap();
        let request = ReservationRequest::new(
            ConsumerKey::new("probe", MemoryClass::Discovery).unwrap(),
            64,
        )
        .unwrap()
        .with_subcap(tag);
        let lease = coordinator.try_reserve(&request).unwrap().unwrap();
        assert_eq!(coordinator.snapshot().current_bytes, 64);
        assert!(coordinator.try_reserve(&request).unwrap().is_none());
        drop(lease);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
        assert_eq!(pool.reserved(), 0);
    }
}
