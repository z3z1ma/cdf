use std::{collections::BTreeMap, sync::Arc, time::Duration};

use cdf_kernel::{BoxFuture, Result};
use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
use cdf_runtime::{
    BlockingLaneBinding, BlockingLaneSpec, ExecutionHost, ExecutionHostCapabilities,
    ExecutionServices, ExecutionTaskScope, FixedSpillBudget, InterruptionSafety, IoTask, IoValue,
    IoValueTask, LaneAffinity, RunCancellation, SourceDriverId, SourceEgressScope,
    SpillBudgetCoordinator, TaskScopeReport,
};

pub(crate) fn test_execution_services() -> ExecutionServices {
    ExecutionServices::new(Arc::new(
        TestIoHost::new().expect("object access test execution host"),
    ))
    .expect("valid object access test execution services")
}

pub(crate) fn test_egress_scope() -> SourceEgressScope {
    SourceEgressScope::new(
        SourceDriverId::new("object_access_test").expect("object access test driver id"),
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
}

pub(crate) fn test_local_listing_lane() -> BlockingLaneSpec {
    BlockingLaneSpec {
        lane_id: "object-access-test.local-list".to_owned(),
        binding: BlockingLaneBinding::Static,
        maximum_concurrency: 2,
        cpu_slot_cost: 1,
        native_internal_parallelism: 1,
        affinity: LaneAffinity::Shared,
        interruption: InterruptionSafety::CooperativeOnly,
    }
}

struct TestIoHost {
    runtime: tokio::runtime::Runtime,
    memory: Arc<dyn MemoryCoordinator>,
    spill: Arc<dyn SpillBudgetCoordinator>,
}

impl TestIoHost {
    fn new() -> Result<Self> {
        Ok(Self {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|error| {
                    cdf_kernel::CdfError::internal(format!(
                        "build object access test I/O runtime: {error}"
                    ))
                })?,
            memory: Arc::new(DeterministicMemoryCoordinator::new(
                128 * 1024 * 1024,
                BTreeMap::new(),
            )?),
            spill: Arc::new(FixedSpillBudget::new(128 * 1024 * 1024)?),
        })
    }
}

impl ExecutionHost for TestIoHost {
    fn capabilities(&self) -> ExecutionHostCapabilities {
        ExecutionHostCapabilities {
            logical_cpu_slots: 2,
            io_workers: 2,
            blocking_lanes: Vec::new(),
        }
    }

    fn memory(&self) -> Arc<dyn MemoryCoordinator> {
        Arc::clone(&self.memory)
    }

    fn spill(&self) -> Arc<dyn SpillBudgetCoordinator> {
        Arc::clone(&self.spill)
    }

    fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
        Ok(Box::new(TestIoScope {
            handle: self.runtime.handle().clone(),
            cancellation: RunCancellation::default(),
            tasks: Vec::new(),
            submitted_io: 0,
            submitted_cpu: 0,
            submitted_blocking: 0,
        }))
    }

    fn run_io_blocking(&self, task: IoValueTask) -> Result<IoValue> {
        self.runtime.block_on(task)
    }

    fn delay(
        &self,
        duration: Duration,
        cancellation: RunCancellation,
    ) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            cancellation.check()?;
            tokio::time::sleep(duration).await;
            cancellation.check()
        })
    }

    fn monotonic_now(&self) -> Duration {
        Duration::ZERO
    }

    fn unix_now(&self) -> Duration {
        Duration::ZERO
    }

    fn entropy_u64(&self) -> u64 {
        0
    }

    fn ensure_blocking_lanes(&self, _lanes: &[BlockingLaneSpec]) -> Result<()> {
        Ok(())
    }

    fn run_blocking_value(
        &self,
        _lane: &str,
        task: cdf_runtime::BlockingValueTask,
    ) -> Result<IoValue> {
        task()
    }
}

struct TestIoScope {
    handle: tokio::runtime::Handle,
    cancellation: RunCancellation,
    tasks: Vec<tokio::task::JoinHandle<Result<()>>>,
    submitted_io: u64,
    submitted_cpu: u64,
    submitted_blocking: u64,
}

impl Drop for TestIoScope {
    fn drop(&mut self) {
        self.cancellation.cancel();
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl ExecutionTaskScope for TestIoScope {
    fn cancellation(&self) -> RunCancellation {
        self.cancellation.clone()
    }

    fn spawn_io(&mut self, task: IoTask) -> Result<()> {
        self.tasks.push(self.handle.spawn(task));
        self.submitted_io += 1;
        Ok(())
    }

    fn spawn_cpu(
        &mut self,
        _spec: cdf_runtime::CpuTaskSpec,
        _task: cdf_runtime::BlockingTask,
    ) -> Result<()> {
        Err(cdf_kernel::CdfError::internal(
            "object access test scope does not execute CPU tasks",
        ))
    }

    fn spawn_cpu_future(
        &mut self,
        spec: cdf_runtime::CpuTaskSpec,
        task: cdf_runtime::CpuFutureTask,
    ) -> Result<()> {
        spec.validate()?;
        self.tasks.push(self.handle.spawn(task));
        self.submitted_cpu += 1;
        Ok(())
    }

    fn spawn_blocking(&mut self, _lane: &str, task: cdf_runtime::BlockingTask) -> Result<()> {
        self.tasks.push(self.handle.spawn_blocking(task));
        self.submitted_blocking += 1;
        Ok(())
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn join(mut self: Box<Self>) -> BoxFuture<'static, Result<TaskScopeReport>> {
        Box::pin(async move {
            let mut report = TaskScopeReport {
                submitted_io: self.submitted_io,
                submitted_cpu: self.submitted_cpu,
                submitted_blocking: self.submitted_blocking,
                ..TaskScopeReport::default()
            };
            for task in self.tasks.drain(..) {
                match task.await {
                    Ok(Ok(())) => report.completed += 1,
                    Ok(Err(error)) => return Err(error),
                    Err(error) => {
                        return Err(cdf_kernel::CdfError::internal(format!(
                            "object access test I/O task failed: {error}"
                        )));
                    }
                }
            }
            Ok(report)
        })
    }
}
