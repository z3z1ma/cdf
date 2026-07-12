#![doc = "File and object-store source adapter for cdf."]

use cdf_http::{AuthScheme, EgressAllowlist, SecretUri};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

mod driver;
mod local_byte_source;
mod runtime;
mod transport;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileFormatDeclaration {
    Csv,
    Json,
    Ndjson,
    Parquet,
    ArrowIpc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileCompressionDeclaration {
    Auto,
    None,
    Gzip,
    Zstd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileResourcePlan {
    pub source: String,
    pub root: String,
    pub glob: String,
    pub format: FileFormatDeclaration,
    pub format_declared: bool,
    pub compression: FileCompressionDeclaration,
    pub auth: Option<AuthScheme>,
    pub credentials: Option<SecretUri>,
    pub allowlist: EgressAllowlist,
}

pub use driver::FileSourceDriver;
pub use local_byte_source::LocalByteSource;
pub use runtime::*;
pub use transport::*;

#[cfg(test)]
pub(crate) fn test_execution_services() -> cdf_runtime::ExecutionServices {
    cdf_runtime::ExecutionServices::new(std::sync::Arc::new(
        TestIoHost::new().expect("file source test execution host"),
    ))
    .expect("valid file source test execution services")
}

#[cfg(test)]
pub(crate) fn test_format_registry() -> std::sync::Arc<cdf_runtime::FormatRegistry> {
    static REGISTRY: std::sync::OnceLock<std::sync::Arc<cdf_runtime::FormatRegistry>> =
        std::sync::OnceLock::new();
    REGISTRY
        .get_or_init(|| {
            let mut registry = cdf_runtime::FormatRegistry::default();
            registry
                .register(std::sync::Arc::new(
                    cdf_format_arrow_ipc::ArrowIpcFileFormatDriver::new()
                        .expect("Arrow IPC test format driver"),
                ))
                .expect("Arrow IPC test format registration");
            registry
                .register(std::sync::Arc::new(
                    cdf_format_parquet::ParquetFormatDriver::new()
                        .expect("Parquet test format driver"),
                ))
                .expect("Parquet test format registration");
            std::sync::Arc::new(registry)
        })
        .clone()
}

#[cfg(test)]
struct TestIoHost {
    runtime: tokio::runtime::Runtime,
    memory: std::sync::Arc<dyn cdf_memory::MemoryCoordinator>,
    spill: std::sync::Arc<dyn cdf_runtime::SpillBudgetCoordinator>,
}

#[cfg(test)]
impl TestIoHost {
    fn new() -> cdf_kernel::Result<Self> {
        Ok(Self {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|error| {
                    cdf_kernel::CdfError::internal(format!(
                        "build file source test I/O runtime: {error}"
                    ))
                })?,
            memory: std::sync::Arc::new(cdf_memory::DeterministicMemoryCoordinator::new(
                128 * 1024 * 1024,
                std::collections::BTreeMap::new(),
            )?),
            spill: std::sync::Arc::new(cdf_runtime::FixedSpillBudget::new(128 * 1024 * 1024)?),
        })
    }
}

#[cfg(test)]
impl cdf_runtime::ExecutionHost for TestIoHost {
    fn capabilities(&self) -> cdf_runtime::ExecutionHostCapabilities {
        cdf_runtime::ExecutionHostCapabilities {
            logical_cpu_slots: 2,
            io_workers: 2,
            blocking_lanes: Vec::new(),
        }
    }

    fn memory(&self) -> std::sync::Arc<dyn cdf_memory::MemoryCoordinator> {
        std::sync::Arc::clone(&self.memory)
    }

    fn spill(&self) -> std::sync::Arc<dyn cdf_runtime::SpillBudgetCoordinator> {
        std::sync::Arc::clone(&self.spill)
    }

    fn open_scope(
        &self,
        _run_id: &str,
    ) -> cdf_kernel::Result<Box<dyn cdf_runtime::ExecutionTaskScope>> {
        Ok(Box::new(TestIoScope {
            handle: self.runtime.handle().clone(),
            cancellation: cdf_runtime::RunCancellation::default(),
            tasks: Vec::new(),
        }))
    }

    fn run_io_blocking(
        &self,
        task: cdf_runtime::IoValueTask,
    ) -> cdf_kernel::Result<cdf_runtime::IoValue> {
        self.runtime.block_on(task)
    }

    fn ensure_blocking_lanes(
        &self,
        _lanes: &[cdf_runtime::BlockingLaneSpec],
    ) -> cdf_kernel::Result<()> {
        Ok(())
    }

    fn run_blocking_value(
        &self,
        _lane: &str,
        task: cdf_runtime::BlockingValueTask,
    ) -> cdf_kernel::Result<cdf_runtime::IoValue> {
        task()
    }
}

#[cfg(test)]
struct TestIoScope {
    handle: tokio::runtime::Handle,
    cancellation: cdf_runtime::RunCancellation,
    tasks: Vec<tokio::task::JoinHandle<cdf_kernel::Result<()>>>,
}

#[cfg(test)]
impl Drop for TestIoScope {
    fn drop(&mut self) {
        self.cancellation.cancel();
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[cfg(test)]
impl cdf_runtime::ExecutionTaskScope for TestIoScope {
    fn cancellation(&self) -> cdf_runtime::RunCancellation {
        self.cancellation.clone()
    }

    fn spawn_io(&mut self, task: cdf_runtime::IoTask) -> cdf_kernel::Result<()> {
        self.tasks.push(self.handle.spawn(task));
        Ok(())
    }

    fn spawn_cpu(
        &mut self,
        _spec: cdf_runtime::CpuTaskSpec,
        _task: cdf_runtime::BlockingTask,
    ) -> cdf_kernel::Result<()> {
        Err(cdf_kernel::CdfError::internal(
            "file source test scope does not execute CPU tasks",
        ))
    }

    fn spawn_blocking(
        &mut self,
        _lane: &str,
        _task: cdf_runtime::BlockingTask,
    ) -> cdf_kernel::Result<()> {
        Err(cdf_kernel::CdfError::internal(
            "file source test scope does not execute blocking tasks",
        ))
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn join(
        mut self: Box<Self>,
    ) -> cdf_kernel::BoxFuture<'static, cdf_kernel::Result<cdf_runtime::TaskScopeReport>> {
        Box::pin(async move {
            let mut report = cdf_runtime::TaskScopeReport {
                submitted_io: self.tasks.len() as u64,
                ..cdf_runtime::TaskScopeReport::default()
            };
            for task in self.tasks.drain(..) {
                match task.await {
                    Ok(Ok(())) => report.completed += 1,
                    Ok(Err(error)) => return Err(error),
                    Err(error) => {
                        return Err(cdf_kernel::CdfError::internal(format!(
                            "file source test I/O task failed: {error}"
                        )));
                    }
                }
            }
            Ok(report)
        })
    }
}
