use std::{collections::BTreeMap, sync::Arc};

use cdf_http::{AuthScheme, EgressAllowlist, SecretProvider, SecretUri};
use cdf_kernel::{
    BackpressureSupport, CapabilitySupport, CdfError, EstimateSupport, FilterCapabilities,
    IncrementalShape, PartitioningCapabilities, QueryableResource, ReplaySupport,
    ResourceCapabilities, Result, ScopeKind,
};
use cdf_runtime::{
    CompiledSourcePlan, ExecutionServices, SourceAttestationStrength, SourceCompileRequest,
    SourceDriver, SourceDriverDescriptor, SourceDriverId, SourceExecutionCapabilities,
    SourceExecutorClass, SourceResolutionContext, SourceRetryGranularity, artifact_hash,
};
use serde::{Deserialize, Serialize};

use crate::{
    FileCompressionDeclaration, FileFormatDeclaration, FileResource, FileResourcePlan,
    FileRuntimeDependencies, FileTransport,
};

type TransportFactory = dyn Fn(
        Arc<dyn SecretProvider + Send + Sync>,
        ExecutionServices,
    ) -> Result<Box<dyn FileTransport + Send>>
    + Send
    + Sync
    + 'static;

#[derive(Clone)]
pub struct FileSourceDriver {
    descriptor: SourceDriverDescriptor,
    transport_factory: Arc<TransportFactory>,
}

impl std::fmt::Debug for FileSourceDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileSourceDriver")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl FileSourceDriver {
    pub fn new<F>(transport_factory: F) -> Result<Self>
    where
        F: Fn(
                Arc<dyn SecretProvider + Send + Sync>,
                ExecutionServices,
            ) -> Result<Box<dyn FileTransport + Send>>
            + Send
            + Sync
            + 'static,
    {
        let option_schema = serde_json::json!({
            "source": ["source_name", "root", "auth", "credentials", "egress_allowlist"],
            "resource": ["glob", "format", "format_declared", "compression"]
        });
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new("files")?,
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec!["files".to_owned()],
                schemes: vec![
                    "file".to_owned(),
                    "s3".to_owned(),
                    "gs".to_owned(),
                    "az".to_owned(),
                    "http".to_owned(),
                    "https".to_owned(),
                ],
            },
            transport_factory: Arc::new(transport_factory),
        })
    }
}

impl SourceDriver for FileSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        let source: FileSourceOptions = decode_options("file source", request.source_options)?;
        let resource: FileResourceOptions =
            decode_options("file resource", request.resource_options)?;
        let physical = FilePhysicalPlan { source, resource };
        physical.to_runtime_plan()?;
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            request.descriptor.clone(),
            file_capabilities(&request.descriptor),
            execution_capabilities(),
            request.schema,
            request.type_policy_allowances,
            request.effective_schema_runtime,
            serde_json::to_value(&physical).map_err(serialize_error)?,
            serde_json::to_value(&physical).map_err(serialize_error)?,
        )
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let physical: FilePhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid file source plan: {error}")))?;
        let transport = (self.transport_factory)(
            Arc::clone(context.secret_provider()),
            context.execution().clone(),
        )?;
        Ok(Arc::new(FileResource::new(
            plan.descriptor.clone(),
            Arc::new(plan.schema.clone()),
            plan.resource_capabilities.clone(),
            physical.to_runtime_plan()?,
            plan.type_policy_allowances,
            plan.effective_schema_runtime.clone(),
            FileRuntimeDependencies::from_boxed_transport(transport),
        )?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSourceOptions {
    source_name: String,
    root: String,
    #[serde(default)]
    auth: Option<AuthOptions>,
    #[serde(default)]
    credentials: Option<String>,
    #[serde(default)]
    egress_allowlist: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileResourceOptions {
    glob: String,
    format: FileFormatDeclaration,
    format_declared: bool,
    compression: FileCompressionDeclaration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum AuthOptions {
    Bearer { token: String },
    Header { name: String, value: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePhysicalPlan {
    source: FileSourceOptions,
    resource: FileResourceOptions,
}

impl FilePhysicalPlan {
    fn to_runtime_plan(&self) -> Result<FileResourcePlan> {
        Ok(FileResourcePlan {
            source: self.source.source_name.clone(),
            root: self.source.root.clone(),
            glob: self.resource.glob.clone(),
            format: self.resource.format.clone(),
            format_declared: self.resource.format_declared,
            compression: self.resource.compression.clone(),
            auth: self
                .source
                .auth
                .as_ref()
                .map(AuthOptions::to_runtime)
                .transpose()?,
            credentials: self
                .source
                .credentials
                .as_ref()
                .map(|value| SecretUri::new(value.clone()))
                .transpose()?,
            allowlist: if self.source.egress_allowlist.is_empty() {
                EgressAllowlist::allow_any()
            } else {
                EgressAllowlist::from_hosts(self.source.egress_allowlist.clone())
            },
        })
    }
}

impl AuthOptions {
    fn to_runtime(&self) -> Result<AuthScheme> {
        match self {
            Self::Bearer { token } => Ok(AuthScheme::Bearer {
                token_uri: SecretUri::new(token.clone())?,
            }),
            Self::Header { name, value } => Ok(AuthScheme::Header {
                name: name.clone(),
                value_uri: SecretUri::new(value.clone())?,
            }),
        }
    }
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn serialize_error(error: serde_json::Error) -> CdfError {
    CdfError::internal(format!("serialize file source plan: {error}"))
}

fn file_capabilities(_descriptor: &cdf_kernel::ResourceDescriptor) -> ResourceCapabilities {
    ResourceCapabilities {
        projection: CapabilitySupport::Unsupported,
        filters: FilterCapabilities::default(),
        limits: CapabilitySupport::Unsupported,
        ordering: CapabilitySupport::Unsupported,
        partitioning: PartitioningCapabilities {
            parallel_partitions: true,
            supported_scopes: vec![ScopeKind::File],
        },
        incremental: IncrementalShape::File,
        replay: ReplaySupport::ExactRecordedBatches,
        idempotent_reads: true,
        backpressure: BackpressureSupport::Pausable,
        estimates: EstimateSupport::Bytes,
    }
}

fn execution_capabilities() -> SourceExecutionCapabilities {
    SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: 32 * 1024 * 1024,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: 32 * 1024 * 1024,
        maximum_concurrency: 16,
        useful_concurrency: 16,
        executor_class: SourceExecutorClass::Cpu,
        blocking_lane: None,
        pausable: true,
        spillable: true,
        idempotent_reads: true,
        reopenable: true,
        resumable: true,
        speculative_safe: true,
        retry_granularity: SourceRetryGranularity::Partition,
        retryable_errors: vec![cdf_kernel::ErrorKind::Transient],
        attestation: SourceAttestationStrength::ImmutableContent,
        rate_limit_per_second: None,
        quota_authority: None,
        canonical_order: true,
        bounded: true,
        telemetry_version: "v1".to_owned(),
    }
}
