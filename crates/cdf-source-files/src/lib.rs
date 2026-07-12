#![doc = "File and object-store source adapter for cdf."]

use cdf_http::{AuthScheme, EgressAllowlist, SecretUri};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

mod driver;
mod local_byte_source;
mod runtime;
mod transport;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct FileFormatDeclaration(String);

impl FileFormatDeclaration {
    pub fn named(value: impl Into<String>) -> cdf_kernel::Result<Self> {
        let value = value.into();
        cdf_runtime::FormatId::new(value.clone())?;
        Ok(Self(value))
    }

    pub fn csv() -> Self {
        Self("csv".to_owned())
    }

    pub fn json() -> Self {
        Self("json".to_owned())
    }

    pub fn ndjson() -> Self {
        Self("ndjson".to_owned())
    }

    pub fn parquet() -> Self {
        Self("parquet".to_owned())
    }

    pub fn arrow_ipc() -> Self {
        Self("arrow_ipc".to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn validate(&self) -> cdf_kernel::Result<()> {
        cdf_runtime::FormatId::new(self.0.clone()).map(drop)
    }
}

#[cfg(test)]
mod declaration_tests {
    use super::FileFormatDeclaration;

    #[test]
    fn format_declaration_is_a_registry_id_not_a_closed_vocabulary() {
        let declaration: FileFormatDeclaration =
            serde_json::from_str(r#""external_columnar""#).expect("format declaration");
        declaration.validate().expect("valid registry id");
        assert_eq!(declaration.as_str(), "external_columnar");
        assert_eq!(
            serde_json::to_string(&declaration).expect("serialize declaration"),
            r#""external_columnar""#
        );
        assert!(FileFormatDeclaration::named("External Columnar").is_err());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct FileCompressionDeclaration(String);

impl FileCompressionDeclaration {
    pub fn auto() -> Self {
        Self("auto".to_owned())
    }

    pub fn none() -> Self {
        Self("none".to_owned())
    }

    pub fn named(value: impl Into<String>) -> cdf_kernel::Result<Self> {
        let value = value.into();
        cdf_runtime::ByteTransformId::new(value.clone())?;
        if matches!(value.as_str(), "auto" | "none") {
            return Err(cdf_kernel::CdfError::contract(
                "named byte transform cannot use reserved `auto` or `none`",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_auto(&self) -> bool {
        self.0 == "auto"
    }

    pub fn is_none(&self) -> bool {
        self.0 == "none"
    }

    pub fn validate(&self) -> cdf_kernel::Result<()> {
        if self.is_auto() || self.is_none() {
            Ok(())
        } else {
            cdf_runtime::ByteTransformId::new(self.0.clone()).map(drop)
        }
    }
}

impl Default for FileCompressionDeclaration {
    fn default() -> Self {
        Self::auto()
    }
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
                    cdf_format_delimited::CsvFormatDriver::new().expect("CSV test format driver"),
                ))
                .expect("CSV test format registration");
            registry
                .register(std::sync::Arc::new(
                    cdf_format_parquet::ParquetFormatDriver::new()
                        .expect("Parquet test format driver"),
                ))
                .expect("Parquet test format registration");
            registry
                .register(std::sync::Arc::new(
                    cdf_format_json::NdjsonFormatDriver::new().expect("NDJSON test format driver"),
                ))
                .expect("NDJSON test format registration");
            std::sync::Arc::new(registry)
        })
        .clone()
}

#[cfg(test)]
pub(crate) fn test_transform_registry() -> std::sync::Arc<cdf_runtime::ByteTransformRegistry> {
    static REGISTRY: std::sync::OnceLock<std::sync::Arc<cdf_runtime::ByteTransformRegistry>> =
        std::sync::OnceLock::new();
    REGISTRY
        .get_or_init(|| {
            let mut registry = cdf_runtime::ByteTransformRegistry::default();
            registry
                .register(std::sync::Arc::new(
                    cdf_transform_gzip::GzipTransformDriver::new()
                        .expect("gzip test transform driver"),
                ))
                .expect("gzip test transform registration");
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
