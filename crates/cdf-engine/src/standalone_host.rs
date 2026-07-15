use std::{
    collections::{BTreeMap, VecDeque},
    hash::BuildHasher,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU16, AtomicU64, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use cdf_kernel::{CdfError, Result};
use cdf_memory::MemoryCoordinator;
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
    slot_cost: u16,
    enqueued: Instant,
    cancellation: RunCancellation,
    task: BlockingTask,
    completion: oneshot::Sender<WorkCompletion>,
    released: Option<mpsc::SyncSender<()>>,
    usage: Option<Arc<CpuUsageTracker>>,
}

struct PoolState {
    queue: VecDeque<WorkItem>,
    shutdown: bool,
}

struct CpuSlots {
    capacity: u16,
    available: Mutex<u16>,
    changed: Condvar,
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
    state: Arc<(Mutex<PoolState>, Condvar)>,
    slots: Arc<CpuSlots>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

struct CpuFutureState {
    inner: Mutex<CpuFutureInner>,
    pool: Arc<FixedTaskPool>,
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
    notified: bool,
    terminal: bool,
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
                notified: false,
                terminal: false,
                enqueued: None,
                queue_wait_ns: 0,
            }),
            pool,
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
            if inner.polling {
                inner.notified = true;
                return;
            }
            inner.queued = true;
            inner.enqueued = Some(Instant::now());
        }

        let state = Arc::clone(self);
        let submission = self.pool.submit(
            self.slot_cost,
            RunCancellation::default(),
            Box::new(move || {
                state.poll_once();
                Ok(())
            }),
            Arc::clone(&self.usage),
        );
        if let Err(error) = submission {
            self.finish(WorkOutcome::Completed(Err(error)));
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
                let schedule = {
                    let mut inner = self.inner.lock().unwrap();
                    if inner.terminal {
                        return;
                    }
                    inner.queue_wait_ns = inner.queue_wait_ns.saturating_add(queue_wait_ns);
                    inner.polling = false;
                    inner.task = Some(task);
                    std::mem::take(&mut inner.notified)
                };
                if schedule {
                    self.request_poll();
                }
            }
        }
    }

    fn finish(&self, outcome: WorkOutcome) {
        self.finish_with_wait(outcome, 0);
    }

    fn finish_with_wait(&self, outcome: WorkOutcome, queue_wait_ns: u64) {
        let completion = {
            let mut inner = self.inner.lock().unwrap();
            if inner.terminal {
                return;
            }
            inner.queue_wait_ns = inner.queue_wait_ns.saturating_add(queue_wait_ns);
            inner.terminal = true;
            inner.queued = false;
            inner.polling = false;
            inner.task.take();
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
            state: Arc::new((
                Mutex::new(PoolState {
                    queue: VecDeque::new(),
                    shutdown: false,
                }),
                Condvar::new(),
            )),
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
        if slot_cost == 0 || slot_cost > self.capacity {
            return Err(CdfError::contract(format!(
                "task slot cost {slot_cost} exceeds pool capacity {}",
                self.capacity
            )));
        }
        cancellation.check()?;
        let (sender, receiver) = oneshot::channel();
        let (lock, available) = &*self.state;
        let mut state = lock.lock().unwrap();
        if state.shutdown {
            return Err(CdfError::internal("task pool is shutting down"));
        }
        state.queue.push_back(WorkItem {
            slot_cost,
            enqueued: Instant::now(),
            cancellation,
            task,
            completion: sender,
            released: None,
            usage: Some(usage),
        });
        available.notify_all();
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
        let (lock, available) = &*self.state;
        let mut state = lock.lock().unwrap();
        if state.shutdown {
            return Err(CdfError::internal("task pool is shutting down"));
        }
        state.queue.push_back(WorkItem {
            slot_cost,
            enqueued: Instant::now(),
            cancellation,
            task,
            completion,
            released: Some(released),
            usage: None,
        });
        available.notify_all();
        Ok(release_receiver)
    }
}

impl Drop for FixedTaskPool {
    fn drop(&mut self) {
        let (lock, available) = &*self.state;
        if let Ok(mut state) = lock.lock() {
            state.shutdown = true;
            available.notify_all();
        }
        if let Ok(workers) = self.workers.get_mut() {
            for worker in workers.drain(..) {
                let _ = worker.join();
            }
        }
    }
}

fn worker_loop(state: Arc<(Mutex<PoolState>, Condvar)>, slots: Arc<CpuSlots>) {
    loop {
        let item = {
            let (lock, available) = &*state;
            let mut state = lock.lock().unwrap();
            loop {
                if state.shutdown && state.queue.is_empty() {
                    return;
                }
                if let Some(item) = state.queue.pop_front() {
                    break item;
                }
                state = available.wait(state).unwrap();
            }
        };
        {
            let mut available = slots.available.lock().unwrap();
            while *available < item.slot_cost {
                available = slots.changed.wait(available).unwrap();
            }
            *available -= item.slot_cost;
        }
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
        let mut available = slots.available.lock().unwrap();
        *available = available.saturating_add(slot_cost);
        slots.changed.notify_all();
        drop(available);
        if let Some(usage) = &item.usage {
            usage.release(slot_cost);
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

pub struct StandaloneExecutionHost {
    capabilities: Mutex<ExecutionHostCapabilities>,
    runtime: Runtime,
    memory: Arc<dyn MemoryCoordinator>,
    spill: Arc<dyn cdf_runtime::SpillBudgetCoordinator>,
    slots: Arc<CpuSlots>,
    cpu: Arc<FixedTaskPool>,
    lanes: Mutex<BTreeMap<String, (BlockingLaneSpec, Arc<FixedTaskPool>)>>,
    monotonic_origin: Instant,
    entropy_counter: AtomicU64,
}

impl StandaloneExecutionHost {
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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(usize::from(capabilities.io_workers))
            .thread_name("cdf-io")
            .enable_all()
            .build()
            .map_err(|error| CdfError::internal(format!("I/O runtime creation failed: {error}")))?;
        let slots = Arc::new(CpuSlots {
            capacity: capabilities.logical_cpu_slots,
            available: Mutex::new(capabilities.logical_cpu_slots),
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
            runtime,
            memory,
            spill,
            slots,
            cpu,
            lanes: Mutex::new(lanes),
            monotonic_origin: Instant::now(),
            entropy_counter: AtomicU64::new(0),
        })
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
            handle: self.runtime.handle().clone(),
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
        self.runtime.handle().spawn(async move {
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
        let runtime_handle = self.runtime.handle().clone();
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
        let report = host.runtime.block_on(scope.join()).unwrap();
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

        let error = host.runtime.block_on(scope.join()).unwrap_err();

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
        let report = host.runtime.block_on(scope.join()).unwrap();

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
        fn visit(directory: &Path, violations: &mut Vec<String>) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    if !matches!(
                        path.file_name().and_then(|name| name.to_str()),
                        Some("cdf-benchmarks" | "cdf-conformance")
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
                let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
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
