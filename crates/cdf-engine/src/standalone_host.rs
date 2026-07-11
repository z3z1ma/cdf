use std::{
    collections::{BTreeMap, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, mpsc},
    thread::JoinHandle,
    time::Instant,
};

use cdf_kernel::{CdfError, Result};
use cdf_memory::MemoryCoordinator;
use cdf_runtime::{
    BlockingLaneSpec, BlockingTask, CpuTaskSpec, ExecutionHost, ExecutionHostCapabilities,
    ExecutionTaskScope, IoTask, IoValue, IoValueTask, RunCancellation, TaskScopeReport,
};
use futures_util::FutureExt;
use tokio::{runtime::Runtime, sync::oneshot, task::JoinHandle as TokioJoinHandle};

struct WorkCompletion {
    result: Result<()>,
    queue_wait_ns: u64,
    slot_cost: u16,
}

struct WorkItem {
    slot_cost: u16,
    enqueued: Instant,
    cancellation: RunCancellation,
    task: BlockingTask,
    completion: oneshot::Sender<WorkCompletion>,
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

struct FixedTaskPool {
    capacity: u16,
    state: Arc<(Mutex<PoolState>, Condvar)>,
    slots: Arc<CpuSlots>,
    workers: Mutex<Vec<JoinHandle<()>>>,
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
        });
        available.notify_all();
        Ok(receiver)
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
        let queue_wait_ns = u64::try_from(item.enqueued.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let result = if item.cancellation.is_cancelled() {
            Err(CdfError::internal("task cancelled before admission"))
        } else {
            catch_unwind(AssertUnwindSafe(item.task))
                .unwrap_or_else(|_| Err(CdfError::internal("execution worker panicked")))
        };
        let slot_cost = item.slot_cost;
        let _ = item.completion.send(WorkCompletion {
            result,
            queue_wait_ns,
            slot_cost,
        });
        let mut available = slots.available.lock().unwrap();
        *available = available.saturating_add(slot_cost);
        slots.changed.notify_all();
    }
}

pub struct StandaloneExecutionHost {
    capabilities: ExecutionHostCapabilities,
    runtime: Runtime,
    memory: Arc<dyn MemoryCoordinator>,
    cpu: Arc<FixedTaskPool>,
    lanes: BTreeMap<String, (BlockingLaneSpec, Arc<FixedTaskPool>)>,
}

impl StandaloneExecutionHost {
    pub fn default_services(
        managed_budget_bytes: u64,
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
        let host = Arc::new(Self::new(
            ExecutionHostCapabilities {
                logical_cpu_slots: u16::try_from(logical).unwrap_or(u16::MAX),
                io_workers: u16::try_from(logical.min(4)).unwrap_or(1),
                blocking_lanes: Vec::new(),
            },
            memory,
        )?);
        let host_contract: Arc<dyn ExecutionHost> = host.clone();
        let services = cdf_runtime::ExecutionServices::new(host_contract)?;
        Ok((host, services))
    }

    pub fn block_on_root<F: std::future::Future>(&self, future: F) -> F::Output {
        // The orchestration future is deliberately polled outside Tokio. Some
        // compatibility drivers are synchronous and still own private runtimes;
        // entering the host I/O runtime here would make those drivers panic on a
        // nested runtime. Async operators use the host scope/handle explicitly,
        // so runtime ownership remains centralized while drivers migrate.
        futures_executor::block_on(future)
    }

    pub fn new(
        capabilities: ExecutionHostCapabilities,
        memory: Arc<dyn MemoryCoordinator>,
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
                if lane.cpu_slot_cost.max(lane.native_internal_parallelism)
                    > capabilities.logical_cpu_slots
                {
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
            capabilities,
            runtime,
            memory,
            cpu,
            lanes,
        })
    }
}

impl ExecutionHost for StandaloneExecutionHost {
    fn capabilities(&self) -> &ExecutionHostCapabilities {
        &self.capabilities
    }

    fn memory(&self) -> Arc<dyn MemoryCoordinator> {
        Arc::clone(&self.memory)
    }

    fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
        Ok(Box::new(StandaloneTaskScope {
            handle: self.runtime.handle().clone(),
            cancellation: RunCancellation::default(),
            cpu: Arc::clone(&self.cpu),
            lanes: self.lanes.clone(),
            io: Vec::new(),
            cpu_tasks: Vec::new(),
            blocking_tasks: Vec::new(),
            report: TaskScopeReport::default(),
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
        let cost = spec.cpu_slot_cost.max(spec.native_internal_parallelism);
        self.cpu_tasks
            .push(self.cpu.submit(cost, self.cancellation.clone(), task)?);
        self.report.submitted_cpu += 1;
        Ok(())
    }

    fn spawn_blocking(&mut self, lane: &str, task: BlockingTask) -> Result<()> {
        let (spec, pool) = self
            .lanes
            .get(lane)
            .ok_or_else(|| CdfError::contract(format!("unknown blocking lane `{lane}`")))?;
        self.blocking_tasks.push(pool.submit(
            spec.cpu_slot_cost.max(spec.native_internal_parallelism),
            self.cancellation.clone(),
            task,
        )?);
        self.report.submitted_blocking += 1;
        Ok(())
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn join(mut self: Box<Self>) -> cdf_kernel::BoxFuture<'static, Result<TaskScopeReport>> {
        Box::pin(async move {
            let mut first_error = None;
            for task in self.io.drain(..) {
                if self.cancellation.is_cancelled() {
                    task.abort();
                }
                match task.await {
                    Ok(Ok(())) => self.report.completed += 1,
                    Ok(Err(error)) => {
                        self.cancellation.cancel();
                        self.report.failed += 1;
                        first_error.get_or_insert(error);
                    }
                    Err(error) if error.is_cancelled() => self.report.cancelled += 1,
                    Err(_) => {
                        self.cancellation.cancel();
                        self.report.failed += 1;
                        first_error.get_or_insert_with(|| CdfError::internal("I/O task panicked"));
                    }
                }
            }
            for completion in self
                .cpu_tasks
                .drain(..)
                .chain(self.blocking_tasks.drain(..))
            {
                match completion.await {
                    Ok(completion) => {
                        self.report.queue_wait_ns = self
                            .report
                            .queue_wait_ns
                            .saturating_add(completion.queue_wait_ns);
                        self.report.peak_cpu_slots =
                            self.report.peak_cpu_slots.max(completion.slot_cost);
                        match completion.result {
                            Ok(()) => self.report.completed += 1,
                            Err(error) => {
                                self.cancellation.cancel();
                                self.report.failed += 1;
                                first_error.get_or_insert(error);
                            }
                        }
                    }
                    Err(_) => {
                        self.report.failed += 1;
                        first_error.get_or_insert_with(|| {
                            CdfError::internal("execution worker completion channel closed")
                        });
                    }
                }
            }
            match first_error {
                Some(error) => Err(error),
                None => Ok(std::mem::take(&mut self.report)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use cdf_memory::{DeterministicMemoryCoordinator, MemoryClass, ReservationRequest};
    use cdf_runtime::{ExecutionHost, InterruptionSafety, LaneAffinity};

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
            "production runtimes/blocking executors must be owned by the standalone host:\n{}",
            violations.join("\n")
        );
    }
}
