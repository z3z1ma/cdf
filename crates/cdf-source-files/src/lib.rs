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
    static SERVICES: std::sync::OnceLock<cdf_runtime::ExecutionServices> =
        std::sync::OnceLock::new();
    SERVICES
        .get_or_init(|| {
            cdf_runtime::ExecutionServices::new(std::sync::Arc::new(
                TestIoHost::new().expect("file source test execution host"),
            ))
            .expect("valid file source test execution services")
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
        Err(cdf_kernel::CdfError::internal(
            "file source test I/O host does not execute task scopes",
        ))
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
