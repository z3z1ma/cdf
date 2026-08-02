use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
};

use cdf_kernel::{CdfError, Result};

use crate::accounting::{
    BudgetTag, LeaseAccount, MemoryEvent, MemoryLease, MemorySnapshot, ReservationRequest,
    lock_infallible,
};

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
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| CdfError::internal("memory coordinator state lock is poisoned"))?;
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
        lock_infallible(&self.inner.state, "memory coordinator state")
            .waiters
            .register(waker);
    }

    fn unregister_waiter(&self, waker: &Waker) {
        lock_infallible(&self.inner.state, "memory coordinator state")
            .waiters
            .unregister(waker);
    }

    fn snapshot(&self) -> MemorySnapshot {
        lock_infallible(&self.inner.state, "memory coordinator state")
            .snapshot
            .clone()
    }

    fn record_event(&self, event: MemoryEvent) {
        let mut state = lock_infallible(&self.inner.state, "memory coordinator state");
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
            let mut state = coordinator
                .state
                .lock()
                .map_err(|_| CdfError::internal("memory coordinator state lock is poisoned"))?;
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
                let mut state = lock_infallible(&coordinator.state, "memory coordinator state");
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
