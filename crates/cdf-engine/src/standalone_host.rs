use std::{
    cell::Cell,
    collections::{BTreeMap, VecDeque},
    hash::BuildHasher,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicU16, AtomicU64, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use cdf_kernel::{CdfError, Result};
use cdf_memory::{MemoryBudgetResolution, MemoryCoordinator};
use cdf_runtime::{
    BlockingLaneSpec, BlockingTask, BlockingValueTask, CpuFutureTask, CpuTaskSpec, ExecutionHost,
    ExecutionHostCapabilities, ExecutionTaskScope, IoTask, IoValue, IoValueTask, RunCancellation,
    TaskScopeReport,
};
use futures_util::{FutureExt, StreamExt, future::Either, stream::FuturesUnordered};
use tokio::{runtime::Runtime, sync::oneshot, task::JoinHandle as TokioJoinHandle};

struct WorkCompletion {
    outcome: WorkOutcome,
    queue_wait_ns: u64,
}

enum WorkOutcome {
    Completed(Result<()>),
    CancelledBeforeAdmission,
}

struct WorkItem {
    id: u64,
    slot_cost: u16,
    enqueued: Instant,
    cancellation: RunCancellation,
    task: BlockingTask,
    completion: oneshot::Sender<WorkCompletion>,
    released: Option<mpsc::SyncSender<()>>,
    after_release: Option<Box<dyn FnOnce() + Send>>,
    usage: Option<Arc<CpuUsageTracker>>,
}

struct PoolState {
    queue: VecDeque<WorkItem>,
    shutdown: bool,
}

struct CpuSlots {
    capacity: u16,
    next_work_id: AtomicU64,
    state: Mutex<CpuSlotState>,
    changed: Condvar,
}

struct CpuSlotState {
    available: u16,
    waiting: BTreeMap<u64, u16>,
    reservation: Option<u64>,
}

const MAX_SLOT_BYPASSES: u16 = 8;

thread_local! {
    static CDF_MANAGED_EXECUTION_WORKER: Cell<bool> = const { Cell::new(false) };
}

fn on_managed_execution_worker() -> bool {
    CDF_MANAGED_EXECUTION_WORKER.get()
}

fn requires_nonblocking_teardown() -> bool {
    on_managed_execution_worker() || tokio::runtime::Handle::try_current().is_ok()
}

struct ManagedExecutionWorkerGuard;

impl ManagedExecutionWorkerGuard {
    fn enter() -> Self {
        CDF_MANAGED_EXECUTION_WORKER.set(true);
        Self
    }
}

impl Drop for ManagedExecutionWorkerGuard {
    fn drop(&mut self) {
        CDF_MANAGED_EXECUTION_WORKER.set(false);
    }
}

#[derive(Default)]
struct CpuUsageTracker {
    current: AtomicU16,
    peak: AtomicU16,
}

impl CpuUsageTracker {
    fn admit(&self, slots: u16) {
        let current = self.current.fetch_add(slots, Ordering::AcqRel) + slots;
        self.peak.fetch_max(current, Ordering::AcqRel);
    }

    fn release(&self, slots: u16) {
        self.current.fetch_sub(slots, Ordering::AcqRel);
    }
}

struct FixedTaskPool {
    capacity: u16,
    state: Arc<Mutex<PoolState>>,
    slots: Arc<CpuSlots>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

struct CpuFutureState {
    inner: Mutex<CpuFutureInner>,
    pool: Weak<FixedTaskPool>,
    slot_cost: u16,
    cancellation: RunCancellation,
    runtime: tokio::runtime::Handle,
    usage: Arc<CpuUsageTracker>,
}

struct CpuFutureInner {
    task: Option<CpuFutureTask>,
    completion: Option<oneshot::Sender<WorkCompletion>>,
    queued: bool,
    polling: bool,
    release_pending: bool,
    notified: bool,
    terminal: bool,
    terminal_outcome: Option<WorkOutcome>,
    enqueued: Option<Instant>,
    queue_wait_ns: u64,
}

impl CpuFutureState {
    fn new(
        task: CpuFutureTask,
        pool: Arc<FixedTaskPool>,
        slot_cost: u16,
        cancellation: RunCancellation,
        runtime: tokio::runtime::Handle,
        usage: Arc<CpuUsageTracker>,
        completion: oneshot::Sender<WorkCompletion>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(CpuFutureInner {
                task: Some(task),
                completion: Some(completion),
                queued: false,
                polling: false,
                release_pending: false,
                notified: false,
                terminal: false,
                terminal_outcome: None,
                enqueued: None,
                queue_wait_ns: 0,
            }),
            pool: Arc::downgrade(&pool),
            slot_cost,
            cancellation,
            runtime,
            usage,
        })
    }

    fn request_poll(self: &Arc<Self>) {
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.terminal || inner.queued {
                return;
            }
            if inner.polling || inner.release_pending {
                inner.notified = true;
                return;
            }
            inner.queued = true;
            inner.release_pending = true;
            inner.enqueued = Some(Instant::now());
        }

        let Some(pool) = self.pool.upgrade() else {
            self.finish(WorkOutcome::Completed(Err(CdfError::internal(
                "CPU executor stopped before the asynchronous task completed",
            ))));
            self.inner.lock().unwrap().release_pending = false;
            self.publish_terminal();
            return;
        };
        let state = Arc::clone(self);
        let released_state = Arc::clone(self);
        let submission = pool.submit_after_release(
            self.slot_cost,
            RunCancellation::default(),
            Box::new(move || {
                state.poll_once();
                Ok(())
            }),
            Arc::clone(&self.usage),
            Some(Box::new(move || released_state.after_poll_release())),
        );
        if let Err(error) = submission {
            self.finish(WorkOutcome::Completed(Err(error)));
            self.inner.lock().unwrap().release_pending = false;
            self.publish_terminal();
        }
    }

    fn poll_once(self: &Arc<Self>) {
        let (mut task, queue_wait_ns) = {
            let mut inner = self.inner.lock().unwrap();
            if inner.terminal {
                return;
            }
            inner.queued = false;
            inner.polling = true;
            let queue_wait_ns = inner
                .enqueued
                .take()
                .map(|enqueued| u64::try_from(enqueued.elapsed().as_nanos()).unwrap_or(u64::MAX))
                .unwrap_or_default();
            (
                inner
                    .task
                    .take()
                    .expect("nonterminal CPU future retains its task"),
                queue_wait_ns,
            )
        };

        if self.cancellation.is_cancelled() {
            self.finish_with_wait(WorkOutcome::CancelledBeforeAdmission, queue_wait_ns);
            return;
        }

        let waker = Waker::from(Arc::clone(self));
        let mut context = Context::from_waker(&waker);
        let poll = catch_unwind(AssertUnwindSafe(|| {
            let _runtime_context = self.runtime.enter();
            task.as_mut().poll(&mut context)
        }));

        match poll {
            Ok(Poll::Ready(result)) => {
                self.finish_with_wait(WorkOutcome::Completed(result), queue_wait_ns);
            }
            Err(_) => self.finish_with_wait(
                WorkOutcome::Completed(Err(CdfError::internal("asynchronous CPU worker panicked"))),
                queue_wait_ns,
            ),
            Ok(Poll::Pending) => {
                let mut inner = self.inner.lock().unwrap();
                if inner.terminal {
                    return;
                }
                inner.queue_wait_ns = inner.queue_wait_ns.saturating_add(queue_wait_ns);
                inner.polling = false;
                inner.task = Some(task);
            }
        }
    }

    fn finish(&self, outcome: WorkOutcome) {
        self.finish_with_wait(outcome, 0);
    }

    fn finish_with_wait(&self, outcome: WorkOutcome, queue_wait_ns: u64) {
        let mut inner = self.inner.lock().unwrap();
        if inner.terminal {
            return;
        }
        inner.queue_wait_ns = inner.queue_wait_ns.saturating_add(queue_wait_ns);
        inner.terminal = true;
        inner.queued = false;
        inner.polling = false;
        inner.task.take();
        inner.terminal_outcome = Some(outcome);
        // The completion remains in the state until the worker has released both
        // its CPU slots and usage accounting. `publish_terminal` is invoked only
        // from that release barrier (or when submission itself failed).
        drop(inner);
    }

    fn publish_terminal(&self) {
        let completion = {
            let mut inner = self.inner.lock().unwrap();
            if !inner.terminal {
                return;
            }
            let outcome = inner.terminal_outcome.take().unwrap_or_else(|| {
                WorkOutcome::Completed(Err(CdfError::internal(
                    "asynchronous CPU task reached an invalid terminal state",
                )))
            });
            inner.completion.take().map(|completion| {
                (
                    completion,
                    WorkCompletion {
                        outcome,
                        queue_wait_ns: inner.queue_wait_ns,
                    },
                )
            })
        };
        if let Some((completion, result)) = completion {
            let _ = completion.send(result);
        }
    }

    fn after_poll_release(self: &Arc<Self>) {
        let (terminal, schedule) = {
            let mut inner = self.inner.lock().unwrap();
            inner.release_pending = false;
            let terminal = inner.terminal;
            let schedule = !terminal && std::mem::take(&mut inner.notified);
            (terminal, schedule)
        };
        if terminal {
            self.publish_terminal();
        } else if schedule {
            self.request_poll();
        }
    }
}

impl Wake for CpuFutureState {
    fn wake(self: Arc<Self>) {
        self.request_poll();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.request_poll();
    }
}

impl FixedTaskPool {
    fn new(name: &str, workers: u16, slots: Arc<CpuSlots>) -> Result<Arc<Self>> {
        if workers == 0 || slots.capacity == 0 {
            return Err(CdfError::contract("task pool capacity must be nonzero"));
        }
        let pool = Arc::new(Self {
            capacity: slots.capacity,
            state: Arc::new(Mutex::new(PoolState {
                queue: VecDeque::new(),
                shutdown: false,
            })),
            slots,
            workers: Mutex::new(Vec::new()),
        });
        let mut handles = Vec::with_capacity(usize::from(workers));
        for index in 0..workers {
            let state = Arc::clone(&pool.state);
            let slots = Arc::clone(&pool.slots);
            let thread_name = format!("cdf-{name}-{index}");
            handles.push(
                std::thread::Builder::new()
                    .name(thread_name)
                    .spawn(move || worker_loop(state, slots))
                    .map_err(|error| {
                        CdfError::internal(format!("task worker spawn failed: {error}"))
                    })?,
            );
        }
        *pool.workers.lock().unwrap() = handles;
        Ok(pool)
    }

    fn submit(
        &self,
        slot_cost: u16,
        cancellation: RunCancellation,
        task: BlockingTask,
        usage: Arc<CpuUsageTracker>,
    ) -> Result<oneshot::Receiver<WorkCompletion>> {
        self.submit_after_release(slot_cost, cancellation, task, usage, None)
    }

    fn submit_after_release(
        &self,
        slot_cost: u16,
        cancellation: RunCancellation,
        task: BlockingTask,
        usage: Arc<CpuUsageTracker>,
        after_release: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<oneshot::Receiver<WorkCompletion>> {
        if slot_cost == 0 || slot_cost > self.capacity {
            return Err(CdfError::contract(format!(
                "task slot cost {slot_cost} exceeds pool capacity {}",
                self.capacity
            )));
        }
        cancellation.check()?;
        let (sender, receiver) = oneshot::channel();
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            return Err(CdfError::internal("task pool is shutting down"));
        }
        state.queue.push_back(WorkItem {
            id: self.slots.next_work_id.fetch_add(1, Ordering::Relaxed),
            slot_cost,
            enqueued: Instant::now(),
            cancellation,
            task,
            completion: sender,
            released: None,
            after_release,
            usage: Some(usage),
        });
        let _slots = self.slots.state.lock().unwrap();
        self.slots.changed.notify_all();
        Ok(receiver)
    }

    fn submit_with_release(
        &self,
        slot_cost: u16,
        cancellation: RunCancellation,
        task: BlockingTask,
    ) -> Result<mpsc::Receiver<()>> {
        if slot_cost == 0 || slot_cost > self.capacity {
            return Err(CdfError::contract(format!(
                "task slot cost {slot_cost} exceeds pool capacity {}",
                self.capacity
            )));
        }
        cancellation.check()?;
        let (completion, _receiver) = oneshot::channel();
        let (released, release_receiver) = mpsc::sync_channel(1);
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            return Err(CdfError::internal("task pool is shutting down"));
        }
        state.queue.push_back(WorkItem {
            id: self.slots.next_work_id.fetch_add(1, Ordering::Relaxed),
            slot_cost,
            enqueued: Instant::now(),
            cancellation,
            task,
            completion,
            released: Some(released),
            after_release: None,
            usage: None,
        });
        let _slots = self.slots.state.lock().unwrap();
        self.slots.changed.notify_all();
        Ok(release_receiver)
    }
}

impl Drop for FixedTaskPool {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.shutdown = true;
            if let Ok(_slots) = self.slots.state.lock() {
                self.slots.changed.notify_all();
            }
        }
        if let Ok(workers) = self.workers.get_mut() {
            let workers = std::mem::take(workers);
            if requires_nonblocking_teardown() {
                // A legal CPU, I/O, or FFI task may own the final host
                // reference. No managed worker may synchronously join any
                // managed pool: a peer can be waiting for work performed after
                // this Drop returns. An external reaper owns every handle and
                // joins them once the current worker releases its resources.
                let _ = std::thread::Builder::new()
                    .name("cdf-worker-reaper".to_owned())
                    .spawn(move || {
                        for worker in workers {
                            let _ = worker.join();
                        }
                    });
            } else {
                for worker in workers {
                    let _ = worker.join();
                }
            }
        }
    }
}

fn worker_loop(state: Arc<Mutex<PoolState>>, slots: Arc<CpuSlots>) {
    let _worker_guard = ManagedExecutionWorkerGuard::enter();
    loop {
        let item = {
            loop {
                let mut state = state.lock().unwrap();
                let mut slot_state = slots.state.lock().unwrap();
                if state.shutdown && state.queue.is_empty() {
                    return;
                }
                let selected = select_work_item(&mut state.queue, &mut slot_state);
                if let Some(index) = selected {
                    let item = state
                        .queue
                        .remove(index)
                        .expect("eligible work item index remains present");
                    slot_state.available -= item.slot_cost;
                    slot_state.waiting.remove(&item.id);
                    if slot_state.reservation == Some(item.id) {
                        slot_state.reservation = None;
                    }
                    break item;
                }
                drop(state);
                drop(slots.changed.wait(slot_state).unwrap());
            }
        };
        if let Some(usage) = &item.usage {
            usage.admit(item.slot_cost);
        }
        let queue_wait_ns = u64::try_from(item.enqueued.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let outcome = if item.cancellation.is_cancelled() {
            WorkOutcome::CancelledBeforeAdmission
        } else {
            WorkOutcome::Completed(
                catch_unwind(AssertUnwindSafe(item.task))
                    .unwrap_or_else(|_| Err(CdfError::internal("execution worker panicked"))),
            )
        };
        let slot_cost = item.slot_cost;
        if let Some(usage) = &item.usage {
            usage.release(slot_cost);
        }
        let mut slot_state = slots.state.lock().unwrap();
        slot_state.available = slot_state.available.saturating_add(slot_cost);
        slots.changed.notify_all();
        drop(slot_state);
        if let Some(after_release) = item.after_release {
            let _ = catch_unwind(AssertUnwindSafe(after_release));
        }
        let _ = item.completion.send(WorkCompletion {
            outcome,
            queue_wait_ns,
        });
        if let Some(released) = item.released {
            let _ = released.send(());
        }
    }
}

fn select_work_item(queue: &mut VecDeque<WorkItem>, slots: &mut CpuSlotState) -> Option<usize> {
    if let Some(reserved) = slots.reservation {
        return queue
            .iter()
            .position(|item| item.id == reserved && item.slot_cost <= slots.available);
    }

    let head = queue.front()?;
    let selected = if head.slot_cost <= slots.available {
        Some(0)
    } else {
        slots.waiting.entry(head.id).or_default();
        queue
            .iter()
            .enumerate()
            .skip(1)
            .find_map(|(index, item)| (item.slot_cost <= slots.available).then_some(index))
    }?;

    let selected_id = queue[selected].id;
    if let Some((oldest_waiter, bypasses)) = slots
        .waiting
        .first_key_value()
        .map(|(waiter, bypasses)| (*waiter, *bypasses))
        && oldest_waiter < selected_id
    {
        if bypasses >= MAX_SLOT_BYPASSES {
            // A reservation intentionally allows slots to accumulate. That is
            // the bounded-fairness boundary: work remains conserving for a
            // finite number of global bypasses, after which the oldest observed
            // blocked item wins across the CPU pool and every FFI lane.
            slots.reservation = Some(oldest_waiter);
            return None;
        }
        *slots
            .waiting
            .get_mut(&oldest_waiter)
            .expect("oldest waiting work remains registered") += 1;
    }
    Some(selected)
}

pub struct StandaloneExecutionHost {
    capabilities: Mutex<ExecutionHostCapabilities>,
    runtime: Option<Runtime>,
    memory: Arc<dyn MemoryCoordinator>,
    spill: Arc<dyn cdf_runtime::SpillBudgetCoordinator>,
    slots: Arc<CpuSlots>,
    cpu: Arc<FixedTaskPool>,
    lanes: Mutex<BTreeMap<String, (BlockingLaneSpec, Arc<FixedTaskPool>)>>,
    monotonic_origin: Instant,
    entropy_counter: AtomicU64,
}

impl StandaloneExecutionHost {
    pub fn default_services_with_budget_resolution(
        resolution: MemoryBudgetResolution,
    ) -> Result<(Arc<Self>, cdf_runtime::ExecutionServices)> {
        resolution.validate()?;
        let (host, services) = Self::default_services_with_spill(
            resolution.managed_pool_bytes,
            resolution.spill_budget_bytes,
        )?;
        let services = services.with_memory_budget_resolution(resolution)?;
        Ok((host, services))
    }

    pub fn default_services(
        managed_budget_bytes: u64,
    ) -> Result<(Arc<Self>, cdf_runtime::ExecutionServices)> {
        Self::default_services_with_spill(
            managed_budget_bytes,
            cdf_memory::DEFAULT_SPILL_BUDGET_BYTES,
        )
    }

    pub fn default_services_with_spill(
        managed_budget_bytes: u64,
        spill_budget_bytes: u64,
    ) -> Result<(Arc<Self>, cdf_runtime::ExecutionServices)> {
        let limit = usize::try_from(managed_budget_bytes)
            .map_err(|_| CdfError::contract("managed memory budget exceeds platform usize"))?;
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = Arc::new(
            datafusion::execution::memory_pool::GreedyMemoryPool::new(limit),
        );
        let discovery = cdf_memory::BudgetTag::new("discovery.metadata")?;
        let memory = Arc::new(crate::DataFusionMemoryCoordinator::new(
            pool,
            BTreeMap::from([(discovery, (128 * 1024 * 1024_u64).min(managed_budget_bytes))]),
        )?);
        let logical = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1)
            .min(usize::from(u16::MAX));
        let host = Arc::new(Self::new_with_spill(
            ExecutionHostCapabilities {
                logical_cpu_slots: u16::try_from(logical).unwrap_or(u16::MAX),
                io_workers: u16::try_from(logical.min(4)).unwrap_or(1),
                blocking_lanes: Vec::new(),
            },
            memory,
            Arc::new(cdf_runtime::FixedSpillBudget::new(spill_budget_bytes)?),
        )?);
        let host_contract: Arc<dyn ExecutionHost> = host.clone();
        let services = cdf_runtime::ExecutionServices::new(host_contract)?;
        Ok((host, services))
    }

    pub fn block_on_root<F: std::future::Future>(&self, future: F) -> F::Output {
        // The composition-root future is deliberately polled outside Tokio so embedded callers
        // never nest runtimes. Production child I/O, CPU, and FFI work enters through the
        // injected execution services below.
        futures_executor::block_on(future)
    }

    pub fn new(
        capabilities: ExecutionHostCapabilities,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Self> {
        Self::new_with_spill(
            capabilities,
            memory,
            Arc::new(cdf_runtime::FixedSpillBudget::new(
                cdf_memory::DEFAULT_SPILL_BUDGET_BYTES,
            )?),
        )
    }

    pub fn new_with_spill(
        capabilities: ExecutionHostCapabilities,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn cdf_runtime::SpillBudgetCoordinator>,
    ) -> Result<Self> {
        capabilities.validate()?;
        let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
        runtime_builder
            .worker_threads(usize::from(capabilities.io_workers))
            .thread_name("cdf-io")
            .on_thread_start(|| CDF_MANAGED_EXECUTION_WORKER.set(true))
            .on_thread_stop(|| CDF_MANAGED_EXECUTION_WORKER.set(false))
            .enable_all();
        let runtime = runtime_builder
            .build()
            .map_err(|error| CdfError::internal(format!("I/O runtime creation failed: {error}")))?;
        let slots = Arc::new(CpuSlots {
            capacity: capabilities.logical_cpu_slots,
            next_work_id: AtomicU64::new(0),
            state: Mutex::new(CpuSlotState {
                available: capabilities.logical_cpu_slots,
                waiting: BTreeMap::new(),
                reservation: None,
            }),
            changed: Condvar::new(),
        });
        let cpu = FixedTaskPool::new("cpu", capabilities.logical_cpu_slots, Arc::clone(&slots))?;
        let lanes = capabilities
            .blocking_lanes
            .iter()
            .map(|lane| {
                if lane.claimed_cpu_slots() > capabilities.logical_cpu_slots {
                    return Err(CdfError::contract(format!(
                        "blocking lane `{}` requires more CPU slots than the host provides",
                        lane.lane_id
                    )));
                }
                Ok((
                    lane.lane_id.clone(),
                    (
                        lane.clone(),
                        FixedTaskPool::new(
                            &lane.lane_id,
                            lane.maximum_concurrency,
                            Arc::clone(&slots),
                        )?,
                    ),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            capabilities: Mutex::new(capabilities),
            runtime: Some(runtime),
            memory,
            spill,
            slots,
            cpu,
            lanes: Mutex::new(lanes),
            monotonic_origin: Instant::now(),
            entropy_counter: AtomicU64::new(0),
        })
    }

    fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("standalone execution runtime is present until host teardown")
    }
}

impl Drop for StandaloneExecutionHost {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        if requires_nonblocking_teardown() {
            // Tokio's ordinary Runtime drop blocks and is forbidden from one
            // of its own async workers. Nonblocking shutdown lets the current
            // task return; fixed pools below use the same managed-worker rule.
            runtime.shutdown_background();
        } else {
            drop(runtime);
        }
    }
}

impl ExecutionHost for StandaloneExecutionHost {
    fn capabilities(&self) -> ExecutionHostCapabilities {
        self.capabilities.lock().unwrap().clone()
    }

    fn memory(&self) -> Arc<dyn MemoryCoordinator> {
        Arc::clone(&self.memory)
    }

    fn spill(&self) -> Arc<dyn cdf_runtime::SpillBudgetCoordinator> {
        Arc::clone(&self.spill)
    }

    fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
        Ok(Box::new(StandaloneTaskScope {
            handle: self.runtime().handle().clone(),
            cancellation: RunCancellation::default(),
            cpu: Arc::clone(&self.cpu),
            lanes: self.lanes.lock().unwrap().clone(),
            io: Vec::new(),
            cpu_tasks: Vec::new(),
            blocking_tasks: Vec::new(),
            report: TaskScopeReport::default(),
            usage: Arc::new(CpuUsageTracker::default()),
        }))
    }

    fn run_io_blocking(&self, task: IoValueTask) -> Result<IoValue> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.runtime().handle().spawn(async move {
            let result = AssertUnwindSafe(task)
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(CdfError::internal("I/O operation panicked")));
            let _ = sender.send(result);
        });
        receiver
            .recv()
            .map_err(|_| CdfError::internal("I/O runtime stopped before the operation completed"))?
    }

    fn delay(
        &self,
        duration: Duration,
        cancellation: RunCancellation,
    ) -> cdf_kernel::BoxFuture<'static, Result<()>> {
        let runtime_handle = self.runtime().handle().clone();
        Box::pin(async move {
            struct AbortOnDrop(tokio::task::AbortHandle);

            impl Drop for AbortOnDrop {
                fn drop(&mut self) {
                    self.0.abort();
                }
            }

            cancellation.check()?;
            let timer = runtime_handle.spawn(async move { tokio::time::sleep(duration).await });
            let _abort_on_drop = AbortOnDrop(timer.abort_handle());
            let cancelled = cancellation.cancelled();
            futures_util::pin_mut!(timer, cancelled);
            match futures_util::future::select(timer, cancelled).await {
                Either::Left((result, _)) => result
                    .map_err(|_| CdfError::internal("execution delay task failed"))
                    .map(|_| ()),
                Either::Right(((), timer)) => {
                    timer.abort();
                    let _ = timer.await;
                    cancellation.check()
                }
            }
        })
    }

    fn monotonic_now(&self) -> Duration {
        self.monotonic_origin.elapsed()
    }

    fn unix_now(&self) -> Duration {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
    }

    fn entropy_u64(&self) -> u64 {
        let counter = self.entropy_counter.fetch_add(1, Ordering::Relaxed);
        std::collections::hash_map::RandomState::new().hash_one(counter)
    }

    fn ensure_blocking_lanes(&self, lanes: &[BlockingLaneSpec]) -> Result<()> {
        let mut registered = self.lanes.lock().unwrap();
        let mut capabilities = self.capabilities.lock().unwrap();
        for lane in lanes {
            lane.validate()?;
            if lane.claimed_cpu_slots() > capabilities.logical_cpu_slots {
                return Err(CdfError::contract(format!(
                    "blocking lane `{}` requires more CPU slots than the host provides",
                    lane.lane_id
                )));
            }
            if let Some((existing, _)) = registered.get(&lane.lane_id) {
                if existing != lane {
                    return Err(CdfError::contract(format!(
                        "blocking lane `{}` conflicts with an existing host declaration",
                        lane.lane_id
                    )));
                }
                continue;
            }
            let pool = FixedTaskPool::new(
                &lane.lane_id,
                lane.maximum_concurrency,
                Arc::clone(&self.slots),
            )?;
            registered.insert(lane.lane_id.clone(), (lane.clone(), pool));
            capabilities.blocking_lanes.push(lane.clone());
        }
        capabilities.validate()
    }

    fn run_blocking_value(&self, lane: &str, task: BlockingValueTask) -> Result<IoValue> {
        let pool = self
            .lanes
            .lock()
            .unwrap()
            .get(lane)
            .map(|(spec, pool)| (spec.clone(), Arc::clone(pool)))
            .ok_or_else(|| CdfError::contract(format!("unknown blocking lane `{lane}`")))?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let released = pool.1.submit_with_release(
            pool.0.claimed_cpu_slots(),
            RunCancellation::default(),
            Box::new(move || {
                let result = task();
                let failed = result.is_err();
                sender.send(result).map_err(|_| {
                    CdfError::internal("blocking result receiver stopped before completion")
                })?;
                if failed {
                    Err(CdfError::internal("blocking value operation failed"))
                } else {
                    Ok(())
                }
            }),
        )?;
        let value = receiver
            .recv()
            .map_err(|_| CdfError::internal("blocking lane stopped before returning its result"));
        released.recv().map_err(|_| {
            CdfError::internal("blocking lane stopped before releasing its CPU slots")
        })?;
        value?
    }
}

struct StandaloneTaskScope {
    handle: tokio::runtime::Handle,
    cancellation: RunCancellation,
    cpu: Arc<FixedTaskPool>,
    lanes: BTreeMap<String, (BlockingLaneSpec, Arc<FixedTaskPool>)>,
    io: Vec<TokioJoinHandle<Result<()>>>,
    cpu_tasks: Vec<oneshot::Receiver<WorkCompletion>>,
    blocking_tasks: Vec<oneshot::Receiver<WorkCompletion>>,
    report: TaskScopeReport,
    usage: Arc<CpuUsageTracker>,
}

impl Drop for StandaloneTaskScope {
    fn drop(&mut self) {
        self.cancellation.cancel();
        for task in &self.io {
            task.abort();
        }
    }
}

impl ExecutionTaskScope for StandaloneTaskScope {
    fn cancellation(&self) -> RunCancellation {
        self.cancellation.clone()
    }

    fn spawn_io(&mut self, task: IoTask) -> Result<()> {
        self.cancellation.check()?;
        self.io.push(self.handle.spawn(task));
        self.report.submitted_io += 1;
        Ok(())
    }

    fn spawn_cpu(&mut self, spec: CpuTaskSpec, task: BlockingTask) -> Result<()> {
        spec.validate()?;
        let cost = spec.claimed_cpu_slots();
        self.cpu_tasks.push(self.cpu.submit(
            cost,
            self.cancellation.clone(),
            task,
            Arc::clone(&self.usage),
        )?);
        self.report.submitted_cpu += 1;
        Ok(())
    }

    fn spawn_cpu_future(&mut self, spec: CpuTaskSpec, task: CpuFutureTask) -> Result<()> {
        spec.validate()?;
        let cost = spec.claimed_cpu_slots();
        self.cancellation.check()?;
        let cancellation = self.cancellation.clone();
        let task_cancellation = cancellation.clone();
        let task = Box::pin(async move {
            let cancelled = task_cancellation.cancelled();
            futures_util::pin_mut!(task, cancelled);
            match futures_util::future::select(task, cancelled).await {
                Either::Left((result, _)) => result,
                Either::Right(((), _)) => task_cancellation.check(),
            }
        });
        let (completion, receiver) = oneshot::channel();
        let state = CpuFutureState::new(
            task,
            Arc::clone(&self.cpu),
            cost,
            cancellation,
            self.handle.clone(),
            Arc::clone(&self.usage),
            completion,
        );
        state.request_poll();
        self.cpu_tasks.push(receiver);
        self.report.submitted_cpu += 1;
        Ok(())
    }

    fn spawn_blocking(&mut self, lane: &str, task: BlockingTask) -> Result<()> {
        let (spec, pool) = self
            .lanes
            .get(lane)
            .ok_or_else(|| CdfError::contract(format!("unknown blocking lane `{lane}`")))?;
        self.blocking_tasks.push(pool.submit(
            spec.claimed_cpu_slots(),
            self.cancellation.clone(),
            task,
            Arc::clone(&self.usage),
        )?);
        self.report.submitted_blocking += 1;
        Ok(())
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn join(mut self: Box<Self>) -> cdf_kernel::BoxFuture<'static, Result<TaskScopeReport>> {
        let runtime = self.handle.clone();
        let join = runtime.spawn(async move {
            enum ScopedCompletion {
                Io(std::result::Result<Result<()>, tokio::task::JoinError>),
                Worker(std::result::Result<WorkCompletion, oneshot::error::RecvError>),
            }

            let mut first_error = None;
            let mut pending = FuturesUnordered::new();
            for task in self.io.drain(..) {
                let cancellation = self.cancellation.clone();
                pending.push(
                    async move {
                        let task = task;
                        let cancelled = cancellation.cancelled();
                        futures_util::pin_mut!(task, cancelled);
                        match futures_util::future::select(task, cancelled).await {
                            Either::Left((result, _)) => ScopedCompletion::Io(result),
                            Either::Right(((), task)) => {
                                task.abort();
                                ScopedCompletion::Io(task.await)
                            }
                        }
                    }
                    .boxed(),
                );
            }
            for completion in self
                .cpu_tasks
                .drain(..)
                .chain(self.blocking_tasks.drain(..))
            {
                pending.push(async move { ScopedCompletion::Worker(completion.await) }.boxed());
            }
            while let Some(completion) = pending.next().await {
                match completion {
                    ScopedCompletion::Io(Ok(Ok(()))) => self.report.completed += 1,
                    ScopedCompletion::Io(Ok(Err(error))) => {
                        self.cancellation.cancel();
                        self.report.failed += 1;
                        first_error.get_or_insert(error);
                    }
                    ScopedCompletion::Io(Err(error)) if error.is_cancelled() => {
                        self.report.cancelled += 1;
                    }
                    ScopedCompletion::Io(Err(_)) => {
                        self.cancellation.cancel();
                        self.report.failed += 1;
                        first_error.get_or_insert_with(|| CdfError::internal("I/O task panicked"));
                    }
                    ScopedCompletion::Worker(Ok(completion)) => {
                        self.report.queue_wait_ns = self
                            .report
                            .queue_wait_ns
                            .saturating_add(completion.queue_wait_ns);
                        match completion.outcome {
                            WorkOutcome::CancelledBeforeAdmission => {
                                self.report.cancelled += 1;
                            }
                            WorkOutcome::Completed(Ok(())) => self.report.completed += 1,
                            WorkOutcome::Completed(Err(error)) => {
                                self.cancellation.cancel();
                                self.report.failed += 1;
                                first_error.get_or_insert(error);
                            }
                        }
                    }
                    ScopedCompletion::Worker(Err(_)) => {
                        self.cancellation.cancel();
                        self.report.failed += 1;
                        first_error.get_or_insert_with(|| {
                            CdfError::internal("execution worker completion channel closed")
                        });
                    }
                }
            }
            let unreleased_cpu_slots = self.usage.current.load(Ordering::Acquire);
            if unreleased_cpu_slots != 0 {
                first_error.get_or_insert_with(|| {
                    CdfError::internal(format!(
                        "execution scope completed before {unreleased_cpu_slots} CPU slots were released"
                    ))
                });
            }
            match first_error {
                Some(error) => Err(error),
                None => {
                    self.report.peak_cpu_slots = self.usage.peak.load(Ordering::Acquire);
                    Ok(std::mem::take(&mut self.report))
                }
            }
        });
        Box::pin(async move {
            join.await
                .map_err(|_| CdfError::internal("execution task-scope supervisor panicked"))?
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    use cdf_memory::{DeterministicMemoryCoordinator, MemoryClass, ReservationRequest};
    use cdf_runtime::{ExecutionHost, ExecutionServices, InterruptionSafety, LaneAffinity};
    use futures_util::TryStreamExt;

    use super::*;

    fn host() -> StandaloneExecutionHost {
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap());
        StandaloneExecutionHost::new(
            ExecutionHostCapabilities {
                logical_cpu_slots: 2,
                io_workers: 2,
                blocking_lanes: vec![BlockingLaneSpec {
                    lane_id: "native".to_owned(),
                    binding: cdf_runtime::BlockingLaneBinding::Static,
                    maximum_concurrency: 1,
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                    affinity: LaneAffinity::Pinned,
                    interruption: InterruptionSafety::CooperativeOnly,
                }],
            },
            memory,
        )
        .unwrap()
    }

    #[test]
    fn default_services_preserve_process_memory_budget_resolution() {
        let resolution = cdf_memory::resolve_unenforced_memory_budget(
            Some(2 * 1024 * 1024 * 1024),
            2 * 1024 * 1024 * 1024,
            64 * 1024 * 1024,
            1024 * 1024 * 1024,
        )
        .unwrap();
        let (_, services) =
            StandaloneExecutionHost::default_services_with_budget_resolution(resolution.clone())
                .unwrap();

        assert_eq!(services.memory_budget_resolution(), Some(&resolution));
        assert_eq!(
            services.memory().snapshot().budget_bytes,
            resolution.managed_pool_bytes
        );
        assert_eq!(
            services.spill().snapshot().budget_bytes,
            resolution.spill_budget_bytes
        );
        assert_eq!(
            services
                .with_run_job_ceiling(2)
                .unwrap()
                .memory_budget_resolution(),
            Some(&resolution)
        );
    }

    #[test]
    fn standalone_scope_separates_io_cpu_and_declared_blocking_lanes() {
        let host = host();
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut scope = host.open_scope("run-host").unwrap();
        scope.spawn_io(Box::pin(async { Ok(()) })).unwrap();
        for _ in 0..2 {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            scope
                .spawn_cpu(
                    CpuTaskSpec {
                        task_kind: "decode".to_owned(),
                        cpu_slot_cost: 2,
                        native_internal_parallelism: 1,
                    },
                    Box::new(move || {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(current, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    }),
                )
                .unwrap();
        }
        let lane_active = Arc::clone(&active);
        let lane_peak = Arc::clone(&peak);
        scope
            .spawn_blocking(
                "native",
                Box::new(move || {
                    let current = lane_active.fetch_add(1, Ordering::SeqCst) + 1;
                    lane_peak.fetch_max(current, Ordering::SeqCst);
                    lane_active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }),
            )
            .unwrap();
        let report = host.runtime().block_on(scope.join()).unwrap();
        assert_eq!(report.completed, 4);
        assert_eq!(report.peak_cpu_slots, 2);
        assert_eq!(peak.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn asynchronous_cpu_work_stays_on_the_bounded_cpu_executor_across_awaits() {
        let host = host();
        let (thread_sender, thread_receiver) = mpsc::sync_channel(2);
        let mut scope = host.open_scope("async-cpu").unwrap();
        scope
            .spawn_cpu_future(
                CpuTaskSpec {
                    task_kind: "format.parquet.decode".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 2,
                },
                Box::pin(async move {
                    thread_sender
                        .send(std::thread::current().name().unwrap_or_default().to_owned())
                        .unwrap();
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    thread_sender
                        .send(std::thread::current().name().unwrap_or_default().to_owned())
                        .unwrap();
                    Ok(())
                }),
            )
            .unwrap();
        let report = host.block_on_root(scope.join()).unwrap();
        let threads = thread_receiver.iter().take(2).collect::<Vec<_>>();

        assert_eq!(threads.len(), 2);
        assert!(threads.iter().all(|name| name.starts_with("cdf-cpu-")));
        assert_eq!(report.submitted_cpu, 1);
        assert_eq!(report.peak_cpu_slots, 2);
    }

    #[test]
    fn pending_cpu_futures_release_workers_and_slots_for_runnable_work() {
        let host = host();
        let mut scope = host.open_scope("async-cpu-work-conservation").unwrap();
        let mut gates = Vec::new();
        for index in 0..2 {
            let (release, wait) = oneshot::channel::<()>();
            gates.push(release);
            scope
                .spawn_cpu_future(
                    CpuTaskSpec {
                        task_kind: format!("pending-{index}"),
                        cpu_slot_cost: 1,
                        native_internal_parallelism: 1,
                    },
                    Box::pin(async move {
                        wait.await.map_err(|_| {
                            CdfError::internal("pending CPU future release was dropped")
                        })?;
                        Ok(())
                    }),
                )
                .unwrap();
        }

        let (ran_sender, ran_receiver) = mpsc::sync_channel(1);
        scope
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "runnable".to_owned(),
                    cpu_slot_cost: 2,
                    native_internal_parallelism: 1,
                },
                Box::new(move || {
                    ran_sender.send(()).unwrap();
                    Ok(())
                }),
            )
            .unwrap();

        ran_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("pending futures must not occupy fixed CPU workers or slots");
        for gate in gates {
            gate.send(()).unwrap();
        }
        let report = host.block_on_root(scope.join()).unwrap();

        assert_eq!(report.submitted_cpu, 3);
        assert_eq!(report.completed, 3);
        assert_eq!(report.peak_cpu_slots, 2);
    }

    #[test]
    fn slot_ineligible_head_does_not_occupy_worker_or_starve_eligible_tail() {
        let host = host();
        let mut scope = host.open_scope("mixed-slot-work-conservation").unwrap();
        let (long_started_sender, long_started_receiver) = mpsc::sync_channel(1);
        let (release_long_sender, release_long_receiver) = mpsc::sync_channel(1);
        scope
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "long-one-slot".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                Box::new(move || {
                    long_started_sender.send(()).unwrap();
                    release_long_receiver.recv().unwrap();
                    Ok(())
                }),
            )
            .unwrap();
        long_started_receiver.recv().unwrap();

        scope
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "waiting-two-slots".to_owned(),
                    cpu_slot_cost: 2,
                    native_internal_parallelism: 1,
                },
                Box::new(|| Ok(())),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(
            host.cpu.state.lock().unwrap().queue.len(),
            1,
            "slot-ineligible work must remain visible to admission instead of occupying a worker"
        );

        let (eligible_sender, eligible_receiver) = mpsc::sync_channel(1);
        scope
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "eligible-one-slot".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                Box::new(move || {
                    eligible_sender.send(()).unwrap();
                    Ok(())
                }),
            )
            .unwrap();
        eligible_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("eligible tail work must use the available worker and slot");

        release_long_sender.send(()).unwrap();
        let report = host.block_on_root(scope.join()).unwrap();
        assert_eq!(report.completed, 3);
        assert_eq!(report.peak_cpu_slots, 2);
    }

    #[test]
    fn mixed_cost_queue_bounds_bypasses_before_reserving_the_expensive_head() {
        let host = host();
        let mut scope = host.open_scope("mixed-slot-bounded-fairness").unwrap();
        let (long_started_sender, long_started_receiver) = mpsc::sync_channel(1);
        let (release_long_sender, release_long_receiver) = mpsc::sync_channel(1);
        scope
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "long-one-slot".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                Box::new(move || {
                    long_started_sender.send(()).unwrap();
                    release_long_receiver.recv().unwrap();
                    Ok(())
                }),
            )
            .unwrap();
        long_started_receiver.recv().unwrap();

        let (event_sender, event_receiver) = mpsc::channel();
        let expensive_sender = event_sender.clone();
        scope
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "waiting-two-slots".to_owned(),
                    cpu_slot_cost: 2,
                    native_internal_parallelism: 1,
                },
                Box::new(move || {
                    expensive_sender.send("expensive").unwrap();
                    Ok(())
                }),
            )
            .unwrap();
        for index in 0..usize::from(MAX_SLOT_BYPASSES + 2) {
            let event_sender = event_sender.clone();
            scope
                .spawn_cpu(
                    CpuTaskSpec {
                        task_kind: format!("small-{index}"),
                        cpu_slot_cost: 1,
                        native_internal_parallelism: 1,
                    },
                    Box::new(move || {
                        event_sender.send("small").unwrap();
                        Ok(())
                    }),
                )
                .unwrap();
        }
        drop(event_sender);

        for _ in 0..MAX_SLOT_BYPASSES {
            assert_eq!(
                event_receiver.recv_timeout(Duration::from_secs(1)).unwrap(),
                "small"
            );
        }
        assert!(
            event_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "the expensive head must reserve the shared slots after the bypass bound"
        );

        release_long_sender.send(()).unwrap();
        assert_eq!(
            event_receiver.recv_timeout(Duration::from_secs(1)).unwrap(),
            "expensive"
        );
        let report = host.block_on_root(scope.join()).unwrap();
        assert_eq!(report.completed, u64::from(MAX_SLOT_BYPASSES) + 4);
        assert_eq!(report.peak_cpu_slots, 2);
    }

    #[test]
    fn shared_slot_reservation_prevents_cross_lane_starvation() {
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap());
        let host = StandaloneExecutionHost::new(
            ExecutionHostCapabilities {
                logical_cpu_slots: 2,
                io_workers: 2,
                blocking_lanes: vec![BlockingLaneSpec {
                    lane_id: "wide-native".to_owned(),
                    binding: cdf_runtime::BlockingLaneBinding::Static,
                    maximum_concurrency: 1,
                    cpu_slot_cost: 2,
                    native_internal_parallelism: 1,
                    affinity: LaneAffinity::Pinned,
                    interruption: InterruptionSafety::CooperativeOnly,
                }],
            },
            memory,
        )
        .unwrap();
        let mut scope = host.open_scope("cross-lane-bounded-fairness").unwrap();
        let (long_started_sender, long_started_receiver) = mpsc::sync_channel(1);
        let (release_long_sender, release_long_receiver) = mpsc::sync_channel(1);
        scope
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "long-one-slot".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                Box::new(move || {
                    long_started_sender.send(()).unwrap();
                    release_long_receiver.recv().unwrap();
                    Ok(())
                }),
            )
            .unwrap();
        long_started_receiver.recv().unwrap();

        let (lane_sender, lane_receiver) = mpsc::sync_channel(1);
        scope
            .spawn_blocking(
                "wide-native",
                Box::new(move || {
                    lane_sender.send(()).unwrap();
                    Ok(())
                }),
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while host.slots.state.lock().unwrap().waiting.is_empty() {
            assert!(
                Instant::now() < deadline,
                "wide lane never registered its wait"
            );
            std::thread::yield_now();
        }

        let (tail_sender, tail_receiver) = mpsc::channel();
        for index in 0..usize::from(MAX_SLOT_BYPASSES + 1) {
            let tail_sender = tail_sender.clone();
            scope
                .spawn_cpu(
                    CpuTaskSpec {
                        task_kind: format!("cpu-tail-{index}"),
                        cpu_slot_cost: 1,
                        native_internal_parallelism: 1,
                    },
                    Box::new(move || {
                        tail_sender.send(()).unwrap();
                        Ok(())
                    }),
                )
                .unwrap();
        }
        drop(tail_sender);
        for _ in 0..MAX_SLOT_BYPASSES {
            tail_receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("bounded bypasses must remain work-conserving across lanes");
        }
        assert!(
            tail_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err()
        );

        release_long_sender.send(()).unwrap();
        lane_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("the reserved wide lane must run before later CPU work");
        tail_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("CPU work must resume after the reserved lane releases");
        let report = host.block_on_root(scope.join()).unwrap();
        assert_eq!(report.completed, u64::from(MAX_SLOT_BYPASSES) + 3);
        assert_eq!(report.peak_cpu_slots, 2);
    }

    #[test]
    fn self_waking_cpu_future_repolls_only_after_release_and_publishes_last() {
        struct SelfWakingFuture {
            remaining_yields: u8,
        }

        impl std::future::Future for SelfWakingFuture {
            type Output = Result<()>;

            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                context: &mut Context<'_>,
            ) -> Poll<Self::Output> {
                if self.remaining_yields == 0 {
                    return Poll::Ready(Ok(()));
                }
                self.remaining_yields -= 1;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }

        let host = host();
        let mut scope = host.open_scope("cpu-future-release-barrier").unwrap();
        scope
            .spawn_cpu_future(
                CpuTaskSpec {
                    task_kind: "self-waking".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                Box::pin(SelfWakingFuture {
                    remaining_yields: 64,
                }),
            )
            .unwrap();

        let report = host.block_on_root(scope.join()).unwrap();
        assert_eq!(report.completed, 1);
        assert_eq!(report.peak_cpu_slots, 1);
        let slots = host.slots.state.lock().unwrap();
        assert_eq!(slots.available, host.slots.capacity);
        assert!(slots.waiting.is_empty());
        assert!(slots.reservation.is_none());
        drop(slots);
        drop(host);
    }

    #[test]
    fn managed_worker_teardown_reaps_cross_pool_peers_without_joining_inline() {
        struct PoolOwner {
            cpu: Arc<FixedTaskPool>,
            lane: Arc<FixedTaskPool>,
        }

        let (test_finished_sender, test_finished_receiver) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let slots = Arc::new(CpuSlots {
                capacity: 2,
                next_work_id: AtomicU64::new(0),
                state: Mutex::new(CpuSlotState {
                    available: 2,
                    waiting: BTreeMap::new(),
                    reservation: None,
                }),
                changed: Condvar::new(),
            });
            let owner = Arc::new(PoolOwner {
                cpu: FixedTaskPool::new("self-drop-cpu", 1, Arc::clone(&slots)).unwrap(),
                lane: FixedTaskPool::new("self-drop-lane", 1, slots).unwrap(),
            });
            let final_worker_owner = Arc::clone(&owner);
            let (cpu_started_sender, cpu_started_receiver) = mpsc::sync_channel(1);
            let (lane_started_sender, lane_started_receiver) = mpsc::sync_channel(1);
            let (release_sender, release_receiver) = mpsc::sync_channel(1);
            let (post_drop_sender, post_drop_receiver) = mpsc::sync_channel(1);
            let cpu_completion = owner
                .cpu
                .submit(
                    1,
                    RunCancellation::default(),
                    Box::new(move || {
                        cpu_started_sender.send(()).unwrap();
                        release_receiver.recv().unwrap();
                        drop(final_worker_owner);
                        post_drop_sender.send(()).unwrap();
                        Ok(())
                    }),
                    Arc::new(CpuUsageTracker::default()),
                )
                .unwrap();
            let lane_completion = owner
                .lane
                .submit(
                    1,
                    RunCancellation::default(),
                    Box::new(move || {
                        lane_started_sender.send(()).unwrap();
                        post_drop_receiver.recv().unwrap();
                        Ok(())
                    }),
                    Arc::new(CpuUsageTracker::default()),
                )
                .unwrap();
            cpu_started_receiver.recv().unwrap();
            lane_started_receiver.recv().unwrap();
            drop(owner);
            release_sender.send(()).unwrap();
            for completion in [cpu_completion, lane_completion] {
                let outcome = futures_executor::block_on(completion).unwrap();
                assert!(matches!(outcome.outcome, WorkOutcome::Completed(Ok(()))));
            }
            test_finished_sender.send(()).unwrap();
        });

        test_finished_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("managed-worker host teardown synchronously joined a dependent pool");
    }

    #[test]
    fn io_worker_can_release_the_last_host_owner_without_dropping_runtime_inline() {
        let host = Arc::new(host());
        let final_io_owner = Arc::clone(&host);
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
        host.runtime().handle().spawn(async move {
            started_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            drop(final_io_owner);
            finished_sender.send(()).unwrap();
        });
        started_receiver.recv().unwrap();
        drop(host);
        release_sender.send(()).unwrap();

        finished_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("I/O worker attempted to synchronously drop its own Tokio runtime");
    }

    #[test]
    fn external_tokio_task_can_release_the_last_host_owner_without_blocking_drop() {
        let external_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        external_runtime.block_on(async {
            let host = Arc::new(host());
            let final_owner = Arc::clone(&host);
            drop(host);
            tokio::spawn(async move {
                drop(final_owner);
            })
            .await
            .expect("external Tokio task panicked while dropping the CDF host");
        });
    }

    #[test]
    fn pending_cpu_future_cancellation_wakes_and_joins_without_leaking_slots() {
        let host = host();
        let mut scope = host.open_scope("async-cpu-cancellation").unwrap();
        scope
            .spawn_cpu_future(
                CpuTaskSpec {
                    task_kind: "pending-cancellation".to_owned(),
                    cpu_slot_cost: 2,
                    native_internal_parallelism: 1,
                },
                Box::pin(async move {
                    std::future::pending::<()>().await;
                    Ok(())
                }),
            )
            .unwrap();
        scope.cancel();

        let report = host.block_on_root(scope.join()).unwrap();

        assert_eq!(report.submitted_cpu, 1);
        assert_eq!(report.cancelled, 1);
        assert_eq!(report.peak_cpu_slots, 2);
    }

    #[test]
    fn asynchronous_cpu_future_panic_is_reported_and_releases_slots() {
        let host = host();
        let mut scope = host.open_scope("async-cpu-panic").unwrap();
        scope
            .spawn_cpu_future(
                CpuTaskSpec {
                    task_kind: "panic".to_owned(),
                    cpu_slot_cost: 2,
                    native_internal_parallelism: 1,
                },
                Box::pin(async move { panic!("intentional async CPU panic") }),
            )
            .unwrap();

        let error = host.block_on_root(scope.join()).unwrap_err();
        assert!(error.message.contains("asynchronous CPU worker panicked"));

        let mut recovery = host.open_scope("async-cpu-panic-recovery").unwrap();
        recovery
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "recovery".to_owned(),
                    cpu_slot_cost: 2,
                    native_internal_parallelism: 1,
                },
                Box::new(|| Ok(())),
            )
            .unwrap();
        let report = host.block_on_root(recovery.join()).unwrap();
        assert_eq!(report.completed, 1);
        assert_eq!(report.peak_cpu_slots, 2);
    }

    #[test]
    fn scoped_io_stream_bridges_tokio_without_materializing_and_joins_errors() {
        let services = ExecutionServices::new(Arc::new(host())).unwrap();
        let stream = services
            .spawn_io_stream("native-format", 1, |mut sender, cancellation| async move {
                cancellation.check()?;
                sender.send(1_u64).await?;
                sender.send(2_u64).await?;
                Ok(())
            })
            .unwrap();
        let values = futures_executor::block_on(stream.try_collect::<Vec<_>>()).unwrap();
        assert_eq!(values, vec![1, 2]);

        let stream = services
            .spawn_io_stream("native-format-error", 1, |mut sender, _| async move {
                sender.send(3_u64).await?;
                Err(CdfError::data("native format failed"))
            })
            .unwrap();
        let error = futures_executor::block_on(stream.try_collect::<Vec<_>>()).unwrap_err();
        assert!(error.message.contains("native format failed"));
    }

    #[test]
    fn dropped_scoped_stream_eagerly_joins_its_producer_without_a_waiter() {
        struct DropSignal(Option<mpsc::SyncSender<()>>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                let _ = self.0.take().expect("drop signal is emitted once").send(());
            }
        }

        let services = ExecutionServices::new(Arc::new(host())).unwrap();
        let (dropped_sender, dropped_receiver) = mpsc::sync_channel(1);
        let producer_guard = DropSignal(Some(dropped_sender));
        let stream = services
            .spawn_io_stream(
                "eager-drop-join",
                1,
                move |_sender: cdf_runtime::TaskStreamSender<()>, cancellation| async move {
                    let _producer_guard = producer_guard;
                    cancellation.cancelled().await;
                    Ok(())
                },
            )
            .unwrap();
        drop(stream);

        dropped_receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("dropping the stream must cancel and join its producer scope eagerly");
    }

    #[test]
    fn dropped_blocking_invocation_cancels_and_joins_its_declared_lane() {
        struct DropSignal(Option<mpsc::SyncSender<()>>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                let _ = self.0.take().expect("drop signal is emitted once").send(());
            }
        }

        let services = ExecutionServices::new(Arc::new(host())).unwrap();
        let (dropped_sender, dropped_receiver) = mpsc::sync_channel(1);
        let guard = DropSignal(Some(dropped_sender));
        let task = services
            .spawn_blocking_value("blocking-open", "native", move |cancellation| {
                let _guard = guard;
                while !cancellation.is_cancelled() {
                    std::thread::yield_now();
                }
                Ok(())
            })
            .unwrap();
        let termination = task.termination();
        drop(task);

        futures_executor::block_on(termination.join()).unwrap();
        dropped_receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("blocking invocation task must be dropped before join returns");
    }

    #[test]
    fn embedding_under_an_existing_tokio_runtime_and_worker_panic_join_cleanly() {
        let host = host();
        let memory = host.memory();
        let request = ReservationRequest::new(
            cdf_memory::ConsumerKey::new("panic-task", MemoryClass::Transform).unwrap(),
            64,
        )
        .unwrap();
        let lease = memory.try_reserve(&request).unwrap().unwrap();
        let mut scope = host.open_scope("embedded").unwrap();
        scope
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "panic".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                Box::new(move || {
                    let _lease = lease;
                    panic!("intentional worker panic");
                }),
            )
            .unwrap();
        let embedding = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let error = embedding.block_on(scope.join()).unwrap_err();
        assert!(error.message.contains("worker panicked"));
        assert_eq!(memory.snapshot().current_bytes, 0);
        drop(embedding);
        drop(host);
    }

    #[test]
    fn first_failure_aborts_blocked_siblings_and_join_releases_their_memory() {
        let host = host();
        let memory = host.memory();
        let request = ReservationRequest::new(
            cdf_memory::ConsumerKey::new("blocked-sibling", MemoryClass::Queue).unwrap(),
            64,
        )
        .unwrap();
        let lease = memory.try_reserve(&request).unwrap().unwrap();
        let mut scope = host.open_scope("first-failure").unwrap();
        scope
            .spawn_io(Box::pin(async move {
                let _lease = lease;
                std::future::pending::<()>().await;
                Ok(())
            }))
            .unwrap();
        scope
            .spawn_io(Box::pin(async {
                Err(CdfError::data("intentional graph stage failure"))
            }))
            .unwrap();

        let error = host.runtime().block_on(scope.join()).unwrap_err();

        assert!(error.message.contains("intentional graph stage failure"));
        assert_eq!(memory.snapshot().current_bytes, 0);
    }

    #[test]
    fn explicit_scope_cancellation_counts_work_cancelled_before_admission() {
        let host = host();
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let mut scope = host.open_scope("cancel-before-admission").unwrap();
        scope
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "occupy-all-slots".to_owned(),
                    cpu_slot_cost: 2,
                    native_internal_parallelism: 1,
                },
                Box::new(move || {
                    started_sender.send(()).unwrap();
                    release_receiver.recv().unwrap();
                    Ok(())
                }),
            )
            .unwrap();
        started_receiver.recv().unwrap();
        scope
            .spawn_cpu(
                CpuTaskSpec {
                    task_kind: "queued-then-cancelled".to_owned(),
                    cpu_slot_cost: 2,
                    native_internal_parallelism: 1,
                },
                Box::new(|| panic!("cancelled work must not execute")),
            )
            .unwrap();

        scope.cancel();
        release_sender.send(()).unwrap();
        let report = host.runtime().block_on(scope.join()).unwrap();

        assert_eq!(report.completed, 1);
        assert_eq!(report.cancelled, 1);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn synchronous_driver_io_uses_host_runtime_under_existing_tokio_runtime() {
        let host = Arc::new(host());
        let services = cdf_runtime::ExecutionServices::new(host.clone()).unwrap();
        let embedding = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        embedding.block_on(async {
            let value = services
                .run_io(async { Ok::<_, CdfError>("host-io".to_owned()) })
                .unwrap();
            assert_eq!(value, "host-io");
        });
    }

    #[test]
    fn execution_delay_uses_the_io_runtime_and_honors_cancellation() {
        let host = Arc::new(host());
        let services = cdf_runtime::ExecutionServices::new(host).unwrap();
        futures_executor::block_on(services.delay(
            std::time::Duration::from_millis(1),
            RunCancellation::default(),
        ))
        .unwrap();

        let cancellation = RunCancellation::default();
        cancellation.cancel();
        let error = futures_executor::block_on(
            services.delay(std::time::Duration::from_secs(60), cancellation),
        )
        .unwrap_err();
        assert!(error.message.contains("cancelled"));
    }

    #[test]
    fn production_runtime_ownership_is_centralized() {
        fn production_source(source: &str) -> String {
            let mut production = String::with_capacity(source.len());
            let mut remaining = source;
            while let Some(test_attribute) = remaining.find("#[cfg(test)]") {
                production.push_str(&remaining[..test_attribute]);
                let test_item = &remaining[test_attribute + "#[cfg(test)]".len()..];
                if test_item.trim_start().starts_with("mod tests") {
                    remaining = "";
                    break;
                }
                let mut braces = 0_usize;
                let mut entered_body = false;
                let mut item_end = test_item.len();
                for (offset, character) in test_item.char_indices() {
                    match character {
                        '{' => {
                            entered_body = true;
                            braces += 1;
                        }
                        '}' if entered_body => {
                            braces = braces.saturating_sub(1);
                            if braces == 0 {
                                item_end = offset + character.len_utf8();
                                break;
                            }
                        }
                        ';' if !entered_body => {
                            item_end = offset + character.len_utf8();
                            break;
                        }
                        _ => {}
                    }
                }
                remaining = &test_item[item_end..];
            }
            production.push_str(remaining);
            production
        }

        fn visit(directory: &Path, violations: &mut Vec<String>) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    if !matches!(
                        path.file_name().and_then(|name| name.to_str()),
                        Some(
                            "cdf-bench-core"
                                | "cdf-bench-measure"
                                | "cdf-benchmarks"
                                | "cdf-conformance"
                        )
                    ) {
                        visit(&path, violations);
                    }
                    continue;
                }
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                if path.extension().and_then(|value| value.to_str()) != Some("rs")
                    || name.contains("test")
                    || path.ends_with("cdf-engine/src/standalone_host.rs")
                {
                    continue;
                }
                let source = std::fs::read_to_string(&path).unwrap();
                let production = production_source(&source);
                for forbidden in [
                    "tokio::runtime::Builder",
                    "RuntimeBuilder::new_",
                    "futures_executor::block_on",
                    ".block_on(",
                    "OnceLock<tokio::runtime::Runtime",
                    "std::thread::spawn",
                    "thread::spawn",
                    "rayon::ThreadPoolBuilder",
                    "rayon::ThreadPool",
                ] {
                    if production.contains(forbidden) {
                        violations.push(format!("{} contains {forbidden}", path.display()));
                    }
                }
            }
        }

        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        let mut violations = Vec::new();
        visit(&workspace.join("crates"), &mut violations);
        assert!(
            violations.is_empty(),
            "production runtimes, pools, and blocking executors must be owned by the standalone host:\n{}",
            violations.join("\n")
        );
    }

    #[test]
    fn adapter_declared_lane_registers_and_executes_without_scheduler_wiring() {
        let host = Arc::new(host());
        let services = cdf_runtime::ExecutionServices::new(host.clone()).unwrap();
        let lane = BlockingLaneSpec {
            lane_id: "mock.adapter".to_owned(),
            binding: cdf_runtime::BlockingLaneBinding::Static,
            maximum_concurrency: 1,
            cpu_slot_cost: 1,
            native_internal_parallelism: 1,
            affinity: LaneAffinity::Pinned,
            interruption: InterruptionSafety::CooperativeOnly,
        };
        services
            .ensure_blocking_lanes(std::slice::from_ref(&lane))
            .unwrap();
        let first = services
            .run_blocking("mock.adapter", || {
                Ok::<_, CdfError>(std::thread::current().id())
            })
            .unwrap();
        let second = services
            .run_blocking("mock.adapter", || {
                Ok::<_, CdfError>(std::thread::current().id())
            })
            .unwrap();
        assert_eq!(first, second);
        assert!(host.capabilities().blocking_lanes.contains(&lane));
        let mut conflict = lane;
        conflict.maximum_concurrency = 2;
        assert!(services.ensure_blocking_lanes(&[conflict]).is_err());
    }

    #[test]
    fn synchronous_lane_panic_releases_slots_before_returning() {
        let host = Arc::new(host());
        let services = cdf_runtime::ExecutionServices::new(host).unwrap();
        let error = services
            .run_blocking::<(), _>("native", || panic!("intentional lane panic"))
            .unwrap_err();
        assert!(error.message.contains("stopped before returning"));
        assert_eq!(
            services
                .run_blocking("native", || Ok::<_, CdfError>(42_u8))
                .unwrap(),
            42
        );
    }

    #[test]
    fn scope_reports_aggregate_concurrent_cpu_slots() {
        let host = host();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut scope = host.open_scope("aggregate-peak").unwrap();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            scope
                .spawn_cpu(
                    CpuTaskSpec {
                        task_kind: "parallel".to_owned(),
                        cpu_slot_cost: 1,
                        native_internal_parallelism: 1,
                    },
                    Box::new(move || {
                        barrier.wait();
                        Ok(())
                    }),
                )
                .unwrap();
        }
        barrier.wait();
        let report = host.block_on_root(scope.join()).unwrap();
        assert_eq!(report.peak_cpu_slots, 2);
    }
}
