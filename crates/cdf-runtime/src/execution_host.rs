use std::{
    any::Any,
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use cdf_kernel::{BoxFuture, CdfError, InvocationTermination, Result};
use cdf_memory::MemoryCoordinator;
use futures_channel::{mpsc, oneshot};
use futures_util::{SinkExt, Stream, future::Either};
use serde::{Deserialize, Serialize};

pub type IoTask = BoxFuture<'static, Result<()>>;
pub type CpuFutureTask = BoxFuture<'static, Result<()>>;
pub type IoValue = Box<dyn Any + Send + 'static>;
pub type IoValueTask = BoxFuture<'static, Result<IoValue>>;
pub type BlockingTask = Box<dyn FnOnce() -> Result<()> + Send + 'static>;
pub type BlockingValueTask = Box<dyn FnOnce() -> Result<IoValue> + Send + 'static>;

pub struct TaskStreamSender<T> {
    sender: mpsc::Sender<T>,
    cancellation: RunCancellation,
}

/// One blocking-lane result plus invocation-wide cancellation and join ownership.
pub struct ScopedBlockingTask<T> {
    receiver: oneshot::Receiver<Result<T>>,
    termination: InvocationTermination,
    terminal: bool,
}

impl<T> ScopedBlockingTask<T> {
    pub fn termination(&self) -> InvocationTermination {
        self.termination.clone()
    }
}

impl<T> Unpin for ScopedBlockingTask<T> {}

impl<T> Future for ScopedBlockingTask<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let task = self.get_mut();
        match Pin::new(&mut task.receiver).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(result)) => {
                task.terminal = true;
                Poll::Ready(result)
            }
            Poll::Ready(Err(_)) => {
                task.terminal = true;
                Poll::Ready(Err(CdfError::internal(
                    "blocking task scope ended before publishing its result",
                )))
            }
        }
    }
}

impl<T> Drop for ScopedBlockingTask<T> {
    fn drop(&mut self) {
        if !self.terminal {
            self.termination.cancel();
        }
    }
}

impl<T> TaskStreamSender<T> {
    pub async fn send(&mut self, item: T) -> Result<()> {
        self.cancellation.check()?;
        self.sender
            .send(item)
            .await
            .map_err(|_| CdfError::internal("task stream receiver closed"))?;
        self.cancellation.check()
    }
}

pub struct ScopedTaskStream<T> {
    receiver: mpsc::Receiver<T>,
    termination: InvocationTermination,
    join: Option<BoxFuture<'static, Result<()>>>,
    cancellation: RunCancellation,
    terminal: bool,
}

impl<T> ScopedTaskStream<T> {
    /// Returns the invocation-wide task-scope termination barrier.
    ///
    /// The barrier remains valid after this stream is dropped. Callers that may stop before EOF
    /// must retain and await it before reopening the same logical invocation.
    pub fn termination(&self) -> InvocationTermination {
        self.termination.clone()
    }
}

impl<T> Unpin for ScopedTaskStream<T> {}

impl<T> Stream for ScopedTaskStream<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        if stream.terminal {
            return Poll::Ready(None);
        }
        match Pin::new(&mut stream.receiver).poll_next(context) {
            Poll::Ready(Some(item)) => return Poll::Ready(Some(Ok(item))),
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {}
        }
        if stream.join.is_none() {
            let termination = stream.termination.clone();
            stream.join = Some(Box::pin(async move { termination.join().await }));
        }
        match stream
            .join
            .as_mut()
            .expect("task stream join future was initialized")
            .as_mut()
            .poll(context)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_)) => {
                stream.terminal = true;
                Poll::Ready(None)
            }
            Poll::Ready(Err(error)) => {
                stream.terminal = true;
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}

impl<T> Drop for ScopedTaskStream<T> {
    fn drop(&mut self) {
        if !self.terminal {
            self.cancellation.cancel();
        }
    }
}

#[derive(Debug, Default)]
struct CancellationState {
    cancelled: AtomicBool,
    waiters: Mutex<WakerRegistry>,
}

#[derive(Debug, Default)]
struct WakerRegistry {
    next_id: u64,
    waiters: BTreeMap<u64, Waker>,
}

impl WakerRegistry {
    fn register(&mut self, waiter_id: Option<u64>, waker: &Waker) -> u64 {
        let waiter_id = waiter_id.unwrap_or_else(|| {
            let mut candidate = self.next_id;
            while self.waiters.contains_key(&candidate) {
                candidate = candidate.wrapping_add(1);
            }
            self.next_id = candidate.wrapping_add(1);
            candidate
        });
        match self.waiters.get_mut(&waiter_id) {
            Some(waiter) if waiter.will_wake(waker) => {}
            Some(waiter) => waiter.clone_from(waker),
            None => {
                self.waiters.insert(waiter_id, waker.clone());
            }
        }
        waiter_id
    }

    fn unregister(&mut self, waiter_id: u64) {
        self.waiters.remove(&waiter_id);
    }

    fn take_all(&mut self) -> Vec<Waker> {
        std::mem::take(&mut self.waiters).into_values().collect()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.waiters.len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct RunCancellation(Arc<CancellationState>);

impl RunCancellation {
    pub fn cancel(&self) {
        if self.0.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        let waiters = self.0.waiters.lock().unwrap().take_all();
        for waiter in waiters {
            waiter.wake();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(CdfError::internal("run execution scope is cancelled"))
        } else {
            Ok(())
        }
    }

    pub fn cancelled(&self) -> CancellationFuture {
        CancellationFuture {
            cancellation: self.clone(),
            waiter_id: None,
        }
    }

    /// Awaits a fallible operation until this run is cancelled.
    ///
    /// Dropping the operation future is the cancellation boundary for providers
    /// whose own pending I/O does not cooperatively observe [`RunCancellation`].
    pub async fn await_or_cancel<T, F>(&self, operation: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        self.check()?;
        let cancelled = self.cancelled();
        futures_util::pin_mut!(operation, cancelled);
        match futures_util::future::select(operation, cancelled).await {
            Either::Left((result, _)) => {
                self.check()?;
                result
            }
            Either::Right(((), _)) => self.check().and_then(|()| {
                Err(CdfError::internal(
                    "run cancellation notification completed without cancellation",
                ))
            }),
        }
    }
}

pub struct CancellationFuture {
    cancellation: RunCancellation,
    waiter_id: Option<u64>,
}

impl Future for CancellationFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.get_mut();
        if future.cancellation.is_cancelled() {
            return Poll::Ready(());
        }
        let mut waiters = future.cancellation.0.waiters.lock().unwrap();
        if future.cancellation.is_cancelled() {
            return Poll::Ready(());
        }
        let waiter_id = waiters.register(future.waiter_id, context.waker());
        future.waiter_id = Some(waiter_id);
        Poll::Pending
    }
}

impl Drop for CancellationFuture {
    fn drop(&mut self) {
        if let Some(waiter_id) = self.waiter_id.take()
            && let Ok(mut waiters) = self.cancellation.0.waiters.lock()
        {
            waiters.unregister(waiter_id);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaneAffinity {
    Shared,
    Pinned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionSafety {
    CooperativeOnly,
    SafeToInterrupt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockingLaneSpec {
    pub lane_id: String,
    pub maximum_concurrency: u16,
    pub cpu_slot_cost: u16,
    pub native_internal_parallelism: u16,
    pub affinity: LaneAffinity,
    pub interruption: InterruptionSafety,
}

impl BlockingLaneSpec {
    pub fn claimed_cpu_slots(&self) -> u16 {
        self.cpu_slot_cost.max(self.native_internal_parallelism)
    }

    pub fn validate(&self) -> Result<()> {
        if self.lane_id.is_empty()
            || self.lane_id.len() > 128
            || !self
                .lane_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || self.maximum_concurrency == 0
            || self.cpu_slot_cost == 0
            || self.native_internal_parallelism == 0
        {
            return Err(CdfError::contract(
                "blocking lane requires a safe id and nonzero concurrency/CPU declarations",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuTaskSpec {
    pub task_kind: String,
    pub cpu_slot_cost: u16,
    pub native_internal_parallelism: u16,
}

impl CpuTaskSpec {
    pub fn claimed_cpu_slots(&self) -> u16 {
        self.cpu_slot_cost.max(self.native_internal_parallelism)
    }

    pub fn validate(&self) -> Result<()> {
        if self.task_kind.is_empty()
            || self.task_kind.len() > 128
            || !self
                .task_kind
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || self.cpu_slot_cost == 0
            || self.native_internal_parallelism == 0
        {
            return Err(CdfError::contract(
                "CPU task requires a safe kind and nonzero slot/internal-parallelism declarations",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionHostCapabilities {
    pub logical_cpu_slots: u16,
    pub io_workers: u16,
    pub blocking_lanes: Vec<BlockingLaneSpec>,
}

impl ExecutionHostCapabilities {
    pub fn validate(&self) -> Result<()> {
        if self.logical_cpu_slots == 0 || self.io_workers == 0 {
            return Err(CdfError::contract(
                "execution host requires nonzero CPU and I/O worker capacity",
            ));
        }
        for lane in &self.blocking_lanes {
            lane.validate()?;
            if lane.claimed_cpu_slots() > self.logical_cpu_slots {
                return Err(CdfError::contract(format!(
                    "blocking lane `{}` claims {} CPU slots but the host provides {}",
                    lane.lane_id,
                    lane.claimed_cpu_slots(),
                    self.logical_cpu_slots
                )));
            }
        }
        let mut ids = self
            .blocking_lanes
            .iter()
            .map(|lane| lane.lane_id.as_str())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CdfError::contract(
                "execution host blocking lane ids must be unique",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskScopeReport {
    pub submitted_io: u64,
    pub submitted_cpu: u64,
    pub submitted_blocking: u64,
    pub completed: u64,
    pub cancelled: u64,
    pub failed: u64,
    pub peak_cpu_slots: u16,
    pub queue_wait_ns: u64,
}

pub trait ExecutionTaskScope: Send {
    fn cancellation(&self) -> RunCancellation;
    fn spawn_io(&mut self, task: IoTask) -> Result<()>;
    fn spawn_cpu(&mut self, spec: CpuTaskSpec, task: BlockingTask) -> Result<()>;
    /// Polls an asynchronous CPU-dominant task on the bounded CPU executor while allowing the
    /// future to await resources driven by the host I/O runtime.
    fn spawn_cpu_future(&mut self, spec: CpuTaskSpec, task: CpuFutureTask) -> Result<()>;
    fn spawn_blocking(&mut self, lane: &str, task: BlockingTask) -> Result<()>;
    fn cancel(&self);
    /// Starts joining every task owned by this scope and returns its completion future.
    ///
    /// Joining MUST remain active if the returned future is dropped. This makes nested scoped
    /// streams structurally safe: dropping a parent cancels child streams, while the child scope
    /// continues draining its task handles without requiring a detached caller-owned future.
    fn join(self: Box<Self>) -> BoxFuture<'static, Result<TaskScopeReport>>;
}

pub trait ExecutionHost: Send + Sync {
    fn capabilities(&self) -> ExecutionHostCapabilities;
    fn memory(&self) -> Arc<dyn MemoryCoordinator>;
    fn spill(&self) -> Arc<dyn crate::SpillBudgetCoordinator>;
    fn open_scope(&self, run_id: &str) -> Result<Box<dyn ExecutionTaskScope>>;
    fn run_io_blocking(&self, task: IoValueTask) -> Result<IoValue>;
    fn delay(
        &self,
        duration: Duration,
        cancellation: RunCancellation,
    ) -> BoxFuture<'static, Result<()>>;
    /// Monotonic process-local time used only for runtime deadlines and telemetry.
    fn monotonic_now(&self) -> Duration;
    /// Runtime entropy used for nonidentity scheduling choices such as retry jitter.
    fn entropy_u64(&self) -> u64;
    fn ensure_blocking_lanes(&self, lanes: &[BlockingLaneSpec]) -> Result<()>;
    fn run_blocking_value(&self, lane: &str, task: BlockingValueTask) -> Result<IoValue>;
}

#[derive(Clone)]
pub struct ExecutionServices {
    host: Arc<dyn ExecutionHost>,
    run_work: Option<Arc<RunWorkAdmission>>,
    staging_leases: Option<Arc<crate::StagingLeaseSupervisor>>,
}

struct RunWorkAdmission {
    state: Mutex<RunWorkAdmissionState>,
}

struct RunWorkAdmissionState {
    ceiling: u16,
    active: u16,
    waiters: WakerRegistry,
}

struct RunWorkAcquisition {
    admission: Arc<RunWorkAdmission>,
    waiter_id: Option<u64>,
}

impl Future for RunWorkAcquisition {
    type Output = Result<RunWorkPermit>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let acquisition = self.get_mut();
        let mut state = match acquisition.admission.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return Poll::Ready(Err(CdfError::internal(
                    "run work admission lock is poisoned",
                )));
            }
        };
        if state.active < state.ceiling {
            state.active += 1;
            if let Some(waiter_id) = acquisition.waiter_id.take() {
                state.waiters.unregister(waiter_id);
            }
            drop(state);
            return Poll::Ready(Ok(RunWorkPermit {
                admission: Some(Arc::clone(&acquisition.admission)),
            }));
        }
        let waiter_id = state
            .waiters
            .register(acquisition.waiter_id, context.waker());
        acquisition.waiter_id = Some(waiter_id);
        Poll::Pending
    }
}

impl Drop for RunWorkAcquisition {
    fn drop(&mut self) {
        if let Some(waiter_id) = self.waiter_id.take()
            && let Ok(mut state) = self.admission.state.lock()
        {
            state.waiters.unregister(waiter_id);
        }
    }
}

/// One run-scoped leaf-work permit. Parent orchestration does not retain a
/// permit while opening nested work, so a configured jobs ceiling cannot
/// deadlock a codec whose units are the actual admitted leaves.
pub struct RunWorkPermit {
    admission: Option<Arc<RunWorkAdmission>>,
}

impl Drop for RunWorkPermit {
    fn drop(&mut self) {
        let Some(admission) = self.admission.take() else {
            return;
        };
        let waiters = {
            let mut state = admission.state.lock().unwrap();
            debug_assert!(state.active > 0);
            state.active = state.active.saturating_sub(1);
            state.waiters.take_all()
        };
        for waiter in waiters {
            waiter.wake();
        }
    }
}

impl std::fmt::Debug for ExecutionServices {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionServices")
            .field("capabilities", &self.host.capabilities())
            .field("run_job_ceiling", &self.run_job_ceiling().ok().flatten())
            .finish_non_exhaustive()
    }
}

impl ExecutionServices {
    pub fn new(host: Arc<dyn ExecutionHost>) -> Result<Self> {
        host.capabilities().validate()?;
        Ok(Self {
            host,
            run_work: None,
            staging_leases: None,
        })
    }

    /// Creates invocation-local services whose nested leaf work shares one jobs
    /// ceiling. The host, memory, spill, and adapter lanes remain shared.
    pub fn with_run_job_ceiling(&self, jobs: u16) -> Result<Self> {
        if jobs == 0 {
            return Err(CdfError::contract("run jobs ceiling must be nonzero"));
        }
        Ok(Self {
            host: Arc::clone(&self.host),
            run_work: Some(Arc::new(RunWorkAdmission {
                state: Mutex::new(RunWorkAdmissionState {
                    ceiling: jobs,
                    active: 0,
                    waiters: WakerRegistry::default(),
                }),
            })),
            staging_leases: self.staging_leases.clone(),
        })
    }

    pub fn with_staging_lease_authority(
        &self,
        authority: Arc<dyn crate::StagingLeaseAuthority>,
    ) -> Result<Self> {
        Ok(Self {
            host: Arc::clone(&self.host),
            run_work: self.run_work.clone(),
            staging_leases: Some(crate::StagingLeaseSupervisor::new(authority)?),
        })
    }

    pub fn acquire_staging_lease(
        &self,
        identity: crate::StagingLeaseIdentity,
    ) -> Result<crate::ManagedStagingLease> {
        let supervisor = self.staging_leases.as_ref().ok_or_else(|| {
            CdfError::contract(
                "externally durable staged ingress requires an injected staging lease authority",
            )
        })?;
        let owner = cdf_kernel::LeaseOwnerId::new(format!(
            "cdf-{}-{:016x}",
            std::process::id(),
            self.entropy_u64()
        ))?;
        supervisor.acquire(identity, owner)
    }

    pub fn prove_expired_staging_lease(
        &self,
        lease: &crate::StagingLease,
    ) -> Result<Option<crate::ManagedExpiredStagingLeaseProof>> {
        let supervisor = self.staging_leases.as_ref().ok_or_else(|| {
            CdfError::contract("staging cleanup requires an injected staging lease authority")
        })?;
        let collector = cdf_kernel::LeaseOwnerId::new(format!(
            "cdf-cleanup-{}-{:016x}",
            std::process::id(),
            self.entropy_u64()
        ))?;
        supervisor.prove_expired(lease, collector)
    }

    /// Tightens the provisional run ceiling after source/destination/memory
    /// capabilities have been joined and before execution begins.
    pub fn tighten_run_job_ceiling(&self, jobs: u16) -> Result<()> {
        if jobs == 0 {
            return Err(CdfError::contract("run jobs ceiling must be nonzero"));
        }
        let admission = self.run_work.as_ref().ok_or_else(|| {
            CdfError::internal("run jobs ceiling cannot be tightened before it is configured")
        })?;
        let mut state = admission
            .state
            .lock()
            .map_err(|_| CdfError::internal("run work admission lock is poisoned"))?;
        if state.active != 0 {
            return Err(CdfError::internal(
                "run jobs ceiling cannot change after leaf work begins",
            ));
        }
        if jobs > state.ceiling {
            return Err(CdfError::contract(format!(
                "run jobs ceiling cannot increase from {} to {jobs}",
                state.ceiling
            )));
        }
        state.ceiling = jobs;
        Ok(())
    }

    pub fn run_job_ceiling(&self) -> Result<Option<u16>> {
        self.run_work
            .as_ref()
            .map(|admission| {
                admission
                    .state
                    .lock()
                    .map(|state| state.ceiling)
                    .map_err(|_| CdfError::internal("run work admission lock is poisoned"))
            })
            .transpose()
    }

    pub fn acquire_run_work(
        &self,
        cancellation: RunCancellation,
    ) -> BoxFuture<'static, Result<RunWorkPermit>> {
        let Some(admission) = self.run_work.clone() else {
            return Box::pin(async { Ok(RunWorkPermit { admission: None }) });
        };
        Box::pin(async move {
            cancellation
                .await_or_cancel(RunWorkAcquisition {
                    admission,
                    waiter_id: None,
                })
                .await
        })
    }

    pub fn host(&self) -> &Arc<dyn ExecutionHost> {
        &self.host
    }

    pub fn memory(&self) -> Arc<dyn MemoryCoordinator> {
        self.host.memory()
    }

    pub fn spill(&self) -> Arc<dyn crate::SpillBudgetCoordinator> {
        self.host.spill()
    }

    pub fn capabilities(&self) -> ExecutionHostCapabilities {
        self.host.capabilities()
    }

    /// Waits on the host I/O runtime rather than occupying a CPU or adapter
    /// blocking lane. Cancellation always wins over a pending delay.
    pub fn delay(
        &self,
        duration: Duration,
        cancellation: RunCancellation,
    ) -> BoxFuture<'static, Result<()>> {
        self.host.delay(duration, cancellation)
    }

    pub fn monotonic_now(&self) -> Duration {
        self.host.monotonic_now()
    }

    pub fn entropy_u64(&self) -> u64 {
        self.host.entropy_u64()
    }

    pub fn open_scope(&self, run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
        if run_id.is_empty() || run_id.len() > 256 || run_id.chars().any(char::is_control) {
            return Err(CdfError::contract(
                "execution run id must contain 1..=256 non-control characters",
            ));
        }
        self.host.open_scope(run_id)
    }

    pub fn spawn_io_stream<T, F, Fut>(
        &self,
        run_id: &str,
        maximum_items: usize,
        producer: F,
    ) -> Result<ScopedTaskStream<T>>
    where
        T: Send + 'static,
        F: FnOnce(TaskStreamSender<T>, RunCancellation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        if maximum_items == 0 {
            return Err(CdfError::contract(
                "I/O stream requires a nonzero item bound",
            ));
        }
        let mut scope = self.open_scope(run_id)?;
        let cancellation = scope.cancellation();
        let (sender, receiver) = mpsc::channel(maximum_items);
        let stream_sender = TaskStreamSender {
            sender,
            cancellation: cancellation.clone(),
        };
        let task_cancellation = cancellation.clone();
        scope.spawn_io(Box::pin(async move {
            producer(stream_sender, task_cancellation).await
        }))?;
        let scope_join = scope.join();
        let cancel = cancellation.clone();
        let termination = InvocationTermination::new(
            move || cancel.cancel(),
            Box::pin(async move { scope_join.await.map(|_| ()) }),
        );
        Ok(ScopedTaskStream {
            receiver,
            termination,
            join: None,
            cancellation,
            terminal: false,
        })
    }

    /// Runs blocking preparation and asynchronous streaming under one invocation scope.
    ///
    /// This is the neutral source seam for adapters whose control plane is currently synchronous
    /// while payload transport is asynchronous. One cancellation and join barrier covers both
    /// phases; adapters never expose a prepared payload before the blocking task has joined the
    /// same scope.
    pub fn spawn_blocking_prepared_io_stream<T, P, Prepare, Produce, Fut>(
        &self,
        run_id: &str,
        lane: &str,
        maximum_items: usize,
        prepare: Prepare,
        produce: Produce,
    ) -> Result<ScopedTaskStream<T>>
    where
        T: Send + 'static,
        P: Send + 'static,
        Prepare: FnOnce(RunCancellation) -> Result<P> + Send + 'static,
        Produce: FnOnce(P, TaskStreamSender<T>, RunCancellation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        if maximum_items == 0 {
            return Err(CdfError::contract(
                "prepared I/O stream requires a nonzero item bound",
            ));
        }
        let mut scope = self.open_scope(run_id)?;
        let cancellation = scope.cancellation();
        let (sender, receiver) = mpsc::channel(maximum_items);
        let stream_sender = TaskStreamSender {
            sender,
            cancellation: cancellation.clone(),
        };
        let (prepared_sender, prepared_receiver) = oneshot::channel();
        let preparation_cancellation = cancellation.clone();
        scope.spawn_blocking(
            lane,
            Box::new(move || {
                let result = prepare(preparation_cancellation);
                let _ = prepared_sender.send(result);
                Ok(())
            }),
        )?;
        let producer_cancellation = cancellation.clone();
        scope.spawn_io(Box::pin(async move {
            let prepared = prepared_receiver.await.map_err(|_| {
                CdfError::internal("blocking preparation ended without publishing its result")
            })??;
            produce(prepared, stream_sender, producer_cancellation).await
        }))?;
        let scope_join = scope.join();
        let cancel = cancellation.clone();
        let termination = InvocationTermination::new(
            move || cancel.cancel(),
            Box::pin(async move { scope_join.await.map(|_| ()) }),
        );
        Ok(ScopedTaskStream {
            receiver,
            termination,
            join: None,
            cancellation,
            terminal: false,
        })
    }

    /// Runs one bounded, CPU-dominant asynchronous producer on the host CPU executor. The
    /// producer may await host-driven I/O, but its polling and native compute consume the exact
    /// declared shared CPU-slot demand.
    pub fn spawn_cpu_stream<T, F, Fut>(
        &self,
        run_id: &str,
        spec: CpuTaskSpec,
        maximum_items: usize,
        producer: F,
    ) -> Result<ScopedTaskStream<T>>
    where
        T: Send + 'static,
        F: FnOnce(TaskStreamSender<T>, RunCancellation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        if maximum_items == 0 {
            return Err(CdfError::contract(
                "CPU stream requires a nonzero item bound",
            ));
        }
        spec.validate()?;
        let mut scope = self.open_scope(run_id)?;
        let cancellation = scope.cancellation();
        let (sender, receiver) = mpsc::channel(maximum_items);
        let stream_sender = TaskStreamSender {
            sender,
            cancellation: cancellation.clone(),
        };
        let task_cancellation = cancellation.clone();
        scope.spawn_cpu_future(
            spec,
            Box::pin(async move { producer(stream_sender, task_cancellation).await }),
        )?;
        let scope_join = scope.join();
        let cancel = cancellation.clone();
        let termination = InvocationTermination::new(
            move || cancel.cancel(),
            Box::pin(async move { scope_join.await.map(|_| ()) }),
        );
        Ok(ScopedTaskStream {
            receiver,
            termination,
            join: None,
            cancellation,
            terminal: false,
        })
    }

    /// Runs one source/destination invocation on a declared blocking lane without blocking the
    /// caller and returns structural cancel-and-join ownership with its value.
    pub fn spawn_blocking_value<T, F>(
        &self,
        run_id: &str,
        lane: &str,
        task: F,
    ) -> Result<ScopedBlockingTask<T>>
    where
        T: Send + 'static,
        F: FnOnce(RunCancellation) -> Result<T> + Send + 'static,
    {
        let mut scope = self.open_scope(run_id)?;
        let cancellation = scope.cancellation();
        let task_cancellation = cancellation.clone();
        let (sender, receiver) = oneshot::channel();
        scope.spawn_blocking(
            lane,
            Box::new(move || {
                let result = task(task_cancellation);
                let _ = sender.send(result);
                Ok(())
            }),
        )?;
        let scope_join = scope.join();
        let cancel = cancellation;
        let termination = InvocationTermination::new(
            move || cancel.cancel(),
            Box::pin(async move { scope_join.await.map(|_| ()) }),
        );
        Ok(ScopedBlockingTask {
            receiver,
            termination,
            terminal: false,
        })
    }

    pub fn run_io<T, F>(&self, future: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let value = self.host.run_io_blocking(Box::pin(async move {
            future.await.map(|value| Box::new(value) as IoValue)
        }))?;
        value.downcast::<T>().map(|value| *value).map_err(|_| {
            CdfError::internal("execution host returned an unexpected I/O result type")
        })
    }

    pub fn ensure_blocking_lanes(&self, lanes: &[BlockingLaneSpec]) -> Result<()> {
        self.host.ensure_blocking_lanes(lanes)
    }

    pub fn run_blocking<T, F>(&self, lane: &str, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let value = self.host.run_blocking_value(
            lane,
            Box::new(move || operation().map(|value| Box::new(value) as IoValue)),
        )?;
        value.downcast::<T>().map(|value| *value).map_err(|_| {
            CdfError::internal("execution host returned an unexpected blocking result type")
        })
    }
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};

    use super::*;

    #[test]
    fn cancellation_future_unregisters_its_unique_waiter_on_drop() {
        let cancellation = RunCancellation::default();
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut first = Box::pin(cancellation.cancelled());
        let mut second = Box::pin(cancellation.cancelled());

        assert!(matches!(first.as_mut().poll(&mut context), Poll::Pending));
        assert!(matches!(second.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(cancellation.0.waiters.lock().unwrap().len(), 2);

        drop(first);
        assert_eq!(cancellation.0.waiters.lock().unwrap().len(), 1);
        cancellation.cancel();
        assert!(cancellation.0.waiters.lock().unwrap().is_empty());
        assert!(matches!(
            second.as_mut().poll(&mut context),
            Poll::Ready(())
        ));
    }

    #[test]
    fn run_work_admission_is_shared_tightenable_and_cancellable() {
        futures_executor::block_on(async {
            let admission = Arc::new(RunWorkAdmission {
                state: Mutex::new(RunWorkAdmissionState {
                    ceiling: 3,
                    active: 0,
                    waiters: WakerRegistry::default(),
                }),
            });
            let services = ExecutionServices {
                host: Arc::new(TestHost),
                run_work: Some(Arc::clone(&admission)),
                staging_leases: None,
            };
            services.tighten_run_job_ceiling(2).unwrap();
            assert_eq!(services.run_job_ceiling().unwrap(), Some(2));

            let first = services
                .acquire_run_work(RunCancellation::default())
                .await
                .unwrap();
            let second = services
                .acquire_run_work(RunCancellation::default())
                .await
                .unwrap();
            assert!(services.tighten_run_job_ceiling(1).is_err());

            let third = services.acquire_run_work(RunCancellation::default());
            futures_util::pin_mut!(third);
            assert!(matches!(futures_util::poll!(third.as_mut()), Poll::Pending));
            drop(first);
            let third = third.await.unwrap();

            let cancellation = RunCancellation::default();
            let cancelled = services.acquire_run_work(cancellation.clone());
            futures_util::pin_mut!(cancelled);
            assert!(matches!(
                futures_util::poll!(cancelled.as_mut()),
                Poll::Pending
            ));
            cancellation.cancel();
            assert!(cancelled.await.is_err());
            assert!(admission.state.lock().unwrap().waiters.is_empty());
            drop(second);
            drop(third);
            services.tighten_run_job_ceiling(1).unwrap();
        });
    }

    struct TestHost;

    impl ExecutionHost for TestHost {
        fn capabilities(&self) -> ExecutionHostCapabilities {
            ExecutionHostCapabilities {
                logical_cpu_slots: 4,
                io_workers: 2,
                blocking_lanes: Vec::new(),
            }
        }

        fn memory(&self) -> Arc<dyn MemoryCoordinator> {
            panic!("test does not use memory")
        }

        fn spill(&self) -> Arc<dyn crate::SpillBudgetCoordinator> {
            panic!("test does not use spill")
        }

        fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
            panic!("test does not open scopes")
        }

        fn run_io_blocking(&self, _task: IoValueTask) -> Result<IoValue> {
            panic!("test does not run I/O")
        }

        fn delay(
            &self,
            _duration: Duration,
            _cancellation: RunCancellation,
        ) -> BoxFuture<'static, Result<()>> {
            panic!("test does not delay")
        }

        fn monotonic_now(&self) -> Duration {
            Duration::ZERO
        }

        fn entropy_u64(&self) -> u64 {
            0
        }

        fn ensure_blocking_lanes(&self, _lanes: &[BlockingLaneSpec]) -> Result<()> {
            panic!("test does not configure lanes")
        }

        fn run_blocking_value(&self, _lane: &str, _task: BlockingValueTask) -> Result<IoValue> {
            panic!("test does not run blocking work")
        }
    }
}
