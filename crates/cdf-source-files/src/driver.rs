use std::{collections::BTreeMap, sync::Arc};

use cdf_http::{AuthScheme, EgressAllowlist, SecretProvider, SecretUri};
use cdf_kernel::{
    BackpressureSupport, CapabilitySupport, CdfError, EstimateSupport, FilterCapabilities,
    IncrementalShape, PartitioningCapabilities, QueryableResource, ReplaySupport,
    ResourceCapabilities, Result, ScopeKind,
};
use cdf_runtime::{
    CompiledFormatBinding, CompiledSourcePlan, ExecutionServices, FormatRegistry,
    SourceAttestationStrength, SourceCompileRequest, SourceDriver, SourceDriverDescriptor,
    SourceDriverId, SourceExecutionCapabilities, SourceExecutorClass, SourceResolutionContext,
    SourceRetryGranularity, artifact_hash,
};
use serde::{Deserialize, Serialize};

use crate::{
    FileCompressionDeclaration, FileFormatDeclaration, FileResource, FileResourceDefinition,
    FileResourcePlan, FileRuntimeDependencies,
};

type RuntimeFactory = dyn Fn(Arc<dyn SecretProvider + Send + Sync>, ExecutionServices) -> Result<FileRuntimeDependencies>
    + Send
    + Sync
    + 'static;

#[derive(Clone)]
pub struct FileSourceDriver {
    descriptor: SourceDriverDescriptor,
    formats: Arc<FormatRegistry>,
    runtime_factory: Arc<RuntimeFactory>,
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
    pub fn new<F>(formats: Arc<FormatRegistry>, runtime_factory: F) -> Result<Self>
    where
        F: Fn(
                Arc<dyn SecretProvider + Send + Sync>,
                ExecutionServices,
            ) -> Result<FileRuntimeDependencies>
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
            formats,
            runtime_factory: Arc::new(runtime_factory),
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
        let compiled_format = CompiledFormatBinding::compile(
            self.formats.as_ref(),
            resource.format.as_str(),
            resource.format_options.clone(),
        )?;
        let physical = FilePhysicalPlan {
            source,
            resource,
            compiled_format,
        };
        physical.to_runtime_plan()?;
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            file_capabilities(&request.descriptor),
            execution_capabilities(),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                redacted_options: serde_json::to_value(&physical).map_err(serialize_error)?,
                physical_plan: serde_json::to_value(&physical).map_err(serialize_error)?,
            },
        )
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let physical: FilePhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid file source plan: {error}")))?;
        let dependencies = (self.runtime_factory)(
            Arc::clone(context.secret_provider()),
            context.execution().clone(),
        )?;
        physical.compiled_format.verify(dependencies.formats())?;
        Ok(Arc::new(FileResource::new(
            FileResourceDefinition {
                descriptor: plan.descriptor.clone(),
                schema: Arc::new(plan.schema.clone()),
                capabilities: plan.resource_capabilities.clone(),
                plan: physical.to_runtime_plan()?,
                type_policy_allowances: plan.type_policy_allowances,
                effective_schema_runtime: plan.effective_schema_runtime.clone(),
                compiled_format: physical.compiled_format,
            },
            dependencies,
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
    #[serde(default = "empty_format_options")]
    format_options: serde_json::Value,
    compression: FileCompressionDeclaration,
}

fn empty_format_options() -> serde_json::Value {
    serde_json::json!({})
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
    compiled_format: CompiledFormatBinding,
}

impl FilePhysicalPlan {
    fn to_runtime_plan(&self) -> Result<FileResourcePlan> {
        self.resource.format.validate()?;
        self.resource.compression.validate()?;
        Ok(FileResourcePlan {
            source: self.source.source_name.clone(),
            root: self.source.root.clone(),
            glob: self.resource.glob.clone(),
            format: self.resource.format.clone(),
            format_declared: self.resource.format_declared,
            format_options: self.resource.format_options.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;
    use cdf_kernel::{
        ResourceDescriptor, ResourceId, SchemaHash, SchemaSource, ScopeKey, TrustLevel,
        WriteDisposition,
    };

    fn compile_request() -> SourceCompileRequest {
        SourceCompileRequest {
            source_kind: "files".to_owned(),
            source_options: BTreeMap::from([
                (
                    "source_name".to_owned(),
                    serde_json::Value::String("events".to_owned()),
                ),
                (
                    "root".to_owned(),
                    serde_json::Value::String("/tmp/events".to_owned()),
                ),
                ("egress_allowlist".to_owned(), serde_json::json!([])),
            ]),
            resource_options: BTreeMap::from([
                (
                    "glob".to_owned(),
                    serde_json::Value::String("*.parquet".to_owned()),
                ),
                (
                    "format".to_owned(),
                    serde_json::Value::String("parquet".to_owned()),
                ),
                ("format_declared".to_owned(), serde_json::Value::Bool(false)),
                ("format_options".to_owned(), serde_json::json!({})),
                (
                    "compression".to_owned(),
                    serde_json::Value::String("auto".to_owned()),
                ),
            ]),
            descriptor: ResourceDescriptor {
                resource_id: ResourceId::new("events.raw").unwrap(),
                schema_source: SchemaSource::Declared {
                    schema_hash: SchemaHash::new(format!("sha256:{}", "a".repeat(64))).unwrap(),
                    source: "test".to_owned(),
                },
                primary_key: Vec::new(),
                merge_key: Vec::new(),
                cursor: None,
                write_disposition: WriteDisposition::Append,
                deduplication: None,
                contract: None,
                state_scope: ScopeKey::Resource,
                freshness: None,
                trust_level: TrustLevel::Governed,
            },
            schema: Schema::empty(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
        }
    }

    #[test]
    fn compiled_file_plan_pins_complete_format_driver_semantics() {
        let formats = crate::test_format_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _| {
            Err(CdfError::internal("compile-only test runtime factory"))
        })
        .unwrap();
        let plan = driver.compile(compile_request()).unwrap();
        let physical: FilePhysicalPlan =
            serde_json::from_value(plan.physical_plan.clone()).unwrap();

        assert_eq!(
            physical.compiled_format.descriptor.format_id.as_str(),
            "parquet"
        );
        assert_eq!(
            physical.compiled_format.descriptor.semantic_version,
            "1.0.0"
        );
        assert_eq!(
            physical.compiled_format.descriptor.decode_unit_policy,
            "row_group"
        );
        assert_eq!(
            physical.compiled_format.descriptor.detection_probe,
            cdf_runtime::FormatDetectionProbe {
                prefix_bytes: 4,
                suffix_bytes: 4,
            }
        );
        assert_eq!(
            physical.compiled_format.canonical_options,
            serde_json::json!({})
        );
        physical.compiled_format.verify(formats.as_ref()).unwrap();

        let mut incompatible = physical.compiled_format;
        incompatible.descriptor.semantic_version = "2.0.0".to_owned();
        let error = match incompatible.verify(formats.as_ref()) {
            Ok(_) => panic!("incompatible compiled format plan must fail verification"),
            Err(error) => error,
        };
        assert!(
            error
                .message
                .contains("does not match the registered driver")
        );
    }
}
