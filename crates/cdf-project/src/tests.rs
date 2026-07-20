use super::*;
use crate::internal::*;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{
    DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use bytes::Bytes;
use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_declarative::SourceDeclaration;
use cdf_engine::{EnginePlan, EnginePlanInput, Planner};
use cdf_http::{
    HttpMethod, HttpRequest, HttpResponse, HttpTransport, SecretProvider, SecretUri, SecretValue,
};
use cdf_kernel::ExecutionExtent;
use cdf_kernel::{
    BoxFuture, CapabilitySupport, CdfError, CheckpointId, ConcurrencyLimit, ContractRef,
    DestinationId, DestinationProtocol, DestinationProtocolCapabilities, DestinationSheet,
    DestinationSheetArtifact, DiscoveryManifestHash, DiscoveryManifestReference,
    IdempotencySupport, IdentifierRules, LeaseOwnerId, PipelineId, QueryableResource, ResourceId,
    RunId, ScanRequest, SchemaHash, SchemaSource, ScopeKey, ScopeLease, ScopeLeaseClock,
    ScopeLeaseStore, SourcePosition, TargetName, TransactionSupport, TypeMapping,
    TypeMappingFidelity, WriteDisposition, source_name,
};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use cdf_object_access::{
    FileIdentityMetadata, FileTransportFacade, FileTransportLocation, FileTransportResource,
    HttpFileRequest, HttpFileResponse, HttpFileTransport,
};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    GenerationStrength, RunCancellation, SequentialReadRequest,
};
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver};
use cdf_state_sqlite::InMemoryScopeLeaseStore;
use flate2::{Compression, write::GzEncoder};
use futures_util::stream;
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath};
use sha2::{Digest, Sha256};

#[test]
fn project_normal_build_graph_has_no_concrete_destination_crates() {
    let manifest: toml::Value = toml::from_str(include_str!("../Cargo.toml")).unwrap();
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .unwrap();
    let concrete = dependencies
        .keys()
        .filter(|name| name.starts_with("cdf-dest-"))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        concrete.is_empty(),
        "cdf-project normal dependencies must remain destination-neutral: {concrete:?}"
    );
}

#[test]
fn resolved_destination_binding_configures_direct_runtime_services() {
    let temp = tempfile::tempdir().unwrap();
    let execution = test_execution_services();
    let spill = execution.spill();
    assert_eq!(spill.snapshot().current_bytes, 0);

    {
        let mut destination = ResolvedProjectDestination::new(
            Box::new(
                cdf_dest_duckdb::DuckDbDestination::new(temp.path().join("direct.duckdb")).unwrap(),
            ),
            TargetName::new("events").unwrap(),
        );
        destination
            .bind_execution_services(execution.clone())
            .unwrap();
        assert!(
            spill.snapshot().current_bytes > 0,
            "binding execution services must let direct runtimes reserve native scratch through the shared spill authority"
        );
    }

    assert_eq!(spill.snapshot().current_bytes, 0);
}

fn test_execution_services() -> cdf_runtime::ExecutionServices {
    cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024)
        .unwrap()
        .1
}

fn test_execution_services_with_slots(
    logical_cpu_slots: u16,
    memory_budget_bytes: u64,
) -> cdf_runtime::ExecutionServices {
    let memory: Arc<dyn MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(memory_budget_bytes, BTreeMap::new())
            .unwrap(),
    );
    let host = Arc::new(
        cdf_engine::StandaloneExecutionHost::new(
            cdf_runtime::ExecutionHostCapabilities {
                logical_cpu_slots,
                io_workers: logical_cpu_slots.min(4),
                blocking_lanes: Vec::new(),
            },
            memory,
        )
        .unwrap(),
    );
    cdf_runtime::ExecutionServices::new(host).unwrap()
}

#[derive(Clone, Debug)]
struct PreparedDiscoveredResource {
    resource: cdf_declarative::CompiledResource,
    discovery: Option<ResourceSchemaDiscovery>,
}

fn test_format_registry() -> Arc<cdf_runtime::FormatRegistry> {
    let mut registry = cdf_runtime::FormatRegistry::default();
    registry
        .register(Arc::new(
            cdf_format_arrow_ipc::ArrowIpcFileFormatDriver::new().unwrap(),
        ))
        .unwrap();
    registry
        .register(Arc::new(
            cdf_format_delimited::CsvFormatDriver::new().unwrap(),
        ))
        .unwrap();
    registry
        .register(Arc::new(
            cdf_format_delimited::DelimitedFormatDriver::tsv().unwrap(),
        ))
        .unwrap();
    registry
        .register(Arc::new(
            cdf_format_delimited::DelimitedFormatDriver::psv().unwrap(),
        ))
        .unwrap();
    registry
        .register(Arc::new(
            cdf_format_delimited::DelimitedFormatDriver::custom().unwrap(),
        ))
        .unwrap();
    registry
        .register(Arc::new(
            cdf_format_delimited::FixedWidthFormatDriver::new().unwrap(),
        ))
        .unwrap();
    registry
        .register(Arc::new(
            cdf_format_parquet::ParquetFormatDriver::new().unwrap(),
        ))
        .unwrap();
    registry
        .register(Arc::new(
            cdf_format_json::NdjsonFormatDriver::new().unwrap(),
        ))
        .unwrap();
    registry
        .register(Arc::new(
            cdf_format_json::JsonDocumentFormatDriver::new().unwrap(),
        ))
        .unwrap();
    Arc::new(registry)
}

fn test_source_registry() -> cdf_runtime::SourceRegistry {
    let formats = test_format_registry();
    let runtime_formats = Arc::clone(&formats);
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry
        .register(
            FileSourceDriver::new(formats, move |secrets, execution, egress| {
                Ok(FileRuntimeDependencies::new(
                    FileTransportFacade::new()
                        .with_shared_secret_provider(secrets)
                        .with_execution_services(execution.clone()),
                    execution,
                    Arc::clone(&runtime_formats),
                    Arc::new(cdf_runtime::ByteTransformRegistry::default()),
                    egress,
                ))
            })
            .unwrap(),
        )
        .unwrap();
    registry
        .register(
            cdf_source_rest::RestSourceDriver::new(|| Ok(Box::new(RecordingTransport::default())))
                .unwrap(),
        )
        .unwrap();
    registry
        .register(cdf_source_postgres::PostgresSourceDriver::new().unwrap())
        .unwrap();
    registry
        .register(ProjectReferenceTestDriver::new())
        .unwrap();
    registry
}

#[derive(Debug)]
struct ProjectReferenceTestDriver {
    descriptor: cdf_runtime::SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl ProjectReferenceTestDriver {
    fn new() -> Self {
        let option_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "properties": {"uri": {"type": "string"}}
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            },
        });
        Self {
            descriptor: cdf_runtime::SourceDriverDescriptor {
                driver_id: cdf_runtime::SourceDriverId::new("python").unwrap(),
                driver_version: "test-v1".to_owned(),
                option_schema_hash: cdf_runtime::artifact_hash(&option_schema).unwrap(),
                kinds: vec!["python".to_owned()],
                schemes: vec!["python".to_owned()],
            },
            option_schema,
        }
    }
}

impl cdf_runtime::SourceDriver for ProjectReferenceTestDriver {
    fn descriptor(&self) -> &cdf_runtime::SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn validate_project_options(&self, options: &serde_json::Value) -> Result<()> {
        let options = options
            .as_object()
            .ok_or_else(|| CdfError::contract("test reference source options must be an object"))?;
        if !options
            .get("interpreter")
            .is_some_and(serde_json::Value::is_string)
            || options
                .get("require_free_threaded")
                .is_some_and(|value| !value.is_boolean())
            || options
                .keys()
                .any(|key| !matches!(key.as_str(), "interpreter" | "require_free_threaded"))
        {
            return Err(CdfError::contract(
                "test reference source options require interpreter and optional require_free_threaded",
            ));
        }
        Ok(())
    }

    fn compile(
        &self,
        _request: cdf_runtime::SourceCompileRequest,
    ) -> Result<cdf_runtime::CompiledSourcePlan> {
        Err(CdfError::internal(
            "project validation fixture does not compile reference sources",
        ))
    }

    fn discovery_session(
        &self,
        _plan: &cdf_runtime::CompiledSourcePlan,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
    ) -> Result<Box<dyn cdf_runtime::SourceDiscoverySession>> {
        Err(CdfError::internal(
            "project validation fixture does not discover reference sources",
        ))
    }

    fn health(
        &self,
        request: cdf_runtime::SourceHealthRequest,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
        output: &mut dyn cdf_runtime::SourceHealthSink,
    ) -> Result<()> {
        for plan in request.compiled_plans {
            output.emit(cdf_runtime::SourceHealthResult {
                probe_id: plan.descriptor.resource_id.as_str().replace('.', "_"),
                status: cdf_runtime::SourceHealthStatus::Unsupported,
                message: "project reference fixture has no health operation".to_owned(),
                details: serde_json::json!({}),
            })?;
        }
        Ok(())
    }

    fn resolve(
        &self,
        _plan: &cdf_runtime::CompiledSourcePlan,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        Err(CdfError::internal(
            "project validation fixture does not resolve reference sources",
        ))
    }
}

#[derive(Debug)]
struct ProjectExternalMockFormat {
    descriptor: cdf_runtime::FormatDriverDescriptor,
}

impl ProjectExternalMockFormat {
    fn new() -> Self {
        Self {
            descriptor: cdf_runtime::FormatDriverDescriptor {
                format_id: cdf_runtime::FormatId::new("project_external_mock").unwrap(),
                semantic_version: "1.0.0".to_owned(),
                aliases: Vec::new(),
                extensions: vec!["mock".to_owned()],
                mime_types: Vec::new(),
                magic: Vec::new(),
                detection_probe: cdf_runtime::FormatDetectionProbe {
                    prefix_bytes: 4,
                    suffix_bytes: 0,
                },
                option_schema: serde_json::json!({
                    "type": "object",
                    "additionalProperties": false
                }),
                projection_pushdown: cdf_kernel::PushdownFidelity::Unsupported,
                predicate_pushdown: cdf_kernel::PushdownFidelity::Unsupported,
                predicate_operators: Vec::new(),
                source_access: cdf_runtime::FormatSourceAccess::Sequential,
                discovery: cdf_runtime::FormatDiscoveryCapabilities::only(
                    cdf_runtime::FormatDiscoveryKind::BoundedContent,
                ),
                decode_unit_policy: "whole_mock_file".to_owned(),
                error_isolation: cdf_runtime::FormatErrorIsolation::DecodeUnit,
                decode_cpu: cdf_runtime::CpuTaskSpec {
                    task_kind: "format.project_external_mock.decode".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                minimum_working_set_bytes: 64,
                maximum_working_set_bytes: 1024 * 1024,
            },
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }
}

impl cdf_runtime::FormatDriver for ProjectExternalMockFormat {
    fn descriptor(&self) -> &cdf_runtime::FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        if options.as_object().is_some_and(serde_json::Map::is_empty) {
            Ok(options)
        } else {
            Err(CdfError::contract(
                "project external mock options must be empty",
            ))
        }
    }

    fn detect(&self, probe: &cdf_runtime::FormatProbe) -> Result<cdf_runtime::FormatDetection> {
        Ok(cdf_runtime::FormatDetection {
            confidence: if probe.prefix.starts_with(b"MOCK") {
                cdf_runtime::FormatDetectionConfidence::Strong
            } else {
                cdf_runtime::FormatDetectionConfidence::None
            },
            reason: "project external mock framing".to_owned(),
        })
    }

    fn discover(
        &self,
        source: Arc<dyn ByteSource>,
        request: cdf_runtime::FormatDiscoveryRequest,
    ) -> BoxFuture<'_, Result<cdf_runtime::PhysicalSchemaObservation>> {
        Box::pin(async move {
            let input = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: 5,
                    cancellation: request.cancellation,
                })
                .await?;
            let mut cursor = cdf_runtime::AccountedByteCursor::new(input);
            if cursor
                .read_exact(5, "project external mock discovery")
                .await?
                != b"MOCK\n"
            {
                return Err(CdfError::data("project external mock framing mismatch"));
            }
            Ok(cdf_runtime::PhysicalSchemaObservation {
                identity: source.identity().clone(),
                arrow_schema: Self::schema(),
                sampled_bytes: 5,
                sampled_records: 1,
                evidence: BTreeMap::from([(
                    "external_driver".to_owned(),
                    "project_fixture".to_owned(),
                )]),
            })
        })
    }

    fn prepare_decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: cdf_runtime::DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Arc<dyn cdf_runtime::FormatDecodeSession>>> {
        Box::pin(async move {
            request.cancellation.check()?;
            Ok(Arc::new(ProjectExternalMockSession {
                source,
                units: vec![cdf_runtime::DecodeUnitPlan {
                    unit_id: "mock-file".to_owned(),
                    ordinal: 0,
                    extent: None,
                    estimated_working_set_bytes: 64,
                    independently_retryable: true,
                }],
            }) as Arc<dyn cdf_runtime::FormatDecodeSession>)
        })
    }
}

struct ProjectExternalMockSession {
    source: Arc<dyn ByteSource>,
    units: Vec<cdf_runtime::DecodeUnitPlan>,
}

impl cdf_runtime::FormatDecodeSession for ProjectExternalMockSession {
    fn units(&self) -> &[cdf_runtime::DecodeUnitPlan] {
        &self.units
    }

    fn decode(
        &self,
        request: cdf_runtime::PhysicalDecodeRequest,
    ) -> BoxFuture<'_, Result<cdf_runtime::PhysicalDecodeStream>> {
        Box::pin(async move {
            self.validate_unit(&request.unit)?;
            let input = self
                .source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: 5,
                    cancellation: request.cancellation,
                })
                .await?;
            let mut cursor = cdf_runtime::AccountedByteCursor::new(input);
            if cursor.read_exact(5, "project external mock decode").await? != b"MOCK\n" {
                return Err(CdfError::data("project external mock framing mismatch"));
            }
            let record_batch = RecordBatch::try_new(
                ProjectExternalMockFormat::schema(),
                vec![Arc::new(Int64Array::from(vec![42]))],
            )?;
            let retained = cdf_memory::record_batch_retained_bytes(&record_batch)?;
            let lease = reserve(
                Arc::clone(&request.memory),
                ReservationRequest::new(
                    ConsumerKey::new("project-external-mock", MemoryClass::Decode)?,
                    retained,
                )?,
            )
            .await?;
            let mut batch = cdf_kernel::Batch::from_record_batch(
                cdf_kernel::BatchId::new(format!(
                    "{}-u{:08}-b00000000",
                    request.batch_id_prefix, request.unit.ordinal
                ))?,
                request.resource_id,
                request.partition_id,
                cdf_kernel::canonical_arrow_schema_hash(
                    ProjectExternalMockFormat::schema().as_ref(),
                )?,
                record_batch,
            )?;
            batch.header.source_position = request.source_position;
            let physical = cdf_runtime::AccountedPhysicalBatch::new(batch, lease)?;
            Ok(
                Box::pin(futures_util::stream::once(async move { Ok(physical) }))
                    as cdf_runtime::PhysicalDecodeStream,
            )
        })
    }
}

fn file_dependencies(transport: FileTransportFacade) -> FileRuntimeDependencies {
    file_dependencies_with_execution(transport, test_execution_services())
}

fn file_dependencies_with_execution(
    transport: FileTransportFacade,
    execution: cdf_runtime::ExecutionServices,
) -> FileRuntimeDependencies {
    let mut transforms = cdf_runtime::ByteTransformRegistry::default();
    transforms
        .register(Arc::new(
            cdf_transform_gzip::GzipTransformDriver::new().unwrap(),
        ))
        .unwrap();
    FileRuntimeDependencies::new(
        transport.with_execution_services(execution.clone()),
        execution,
        test_format_registry(),
        Arc::new(transforms),
        cdf_runtime::SourceEgressScope::new(
            cdf_runtime::SourceDriverId::new("files").unwrap(),
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        ),
    )
}

fn resolve_file_resource_for_test(
    resource: &cdf_declarative::CompiledResource,
    dependencies: FileRuntimeDependencies,
) -> Arc<dyn QueryableResource> {
    let formats = Arc::clone(dependencies.formats());
    let installed = dependencies.clone();
    let prepared_payloads = dependencies.prepared_payloads().clone();
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry
        .register(
            FileSourceDriver::new(formats, move |_secrets, _execution, _egress| {
                Ok(installed.clone())
            })
            .unwrap(),
        )
        .unwrap();
    let execution = test_execution_services();
    let project_root = resource.project_root().unwrap_or_else(|| Path::new("."));
    let resolution = cdf_runtime::SourceResolutionContext::new(
        project_root,
        Arc::new(EnvSecretProvider::from_map(
            std::iter::empty::<(&str, &str)>(),
        )),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_prepared_payloads(prepared_payloads);
    registry
        .resolve(resource.source_plan(), &resolution)
        .unwrap()
}

fn external_mock_source_registry(
    transport: RecordingHttpFileTransport,
) -> cdf_runtime::SourceRegistry {
    let mut formats = cdf_runtime::FormatRegistry::default();
    formats
        .register(Arc::new(ProjectExternalMockFormat::new()))
        .unwrap();
    let formats = Arc::new(formats);
    let runtime_formats = Arc::clone(&formats);
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry
        .register(
            FileSourceDriver::new(formats, move |secrets, execution, egress| {
                Ok(FileRuntimeDependencies::new(
                    FileTransportFacade::new()
                        .with_http_transport(transport.clone())
                        .with_shared_secret_provider(secrets)
                        .with_execution_services(execution.clone()),
                    execution,
                    Arc::clone(&runtime_formats),
                    Arc::new(cdf_runtime::ByteTransformRegistry::default()),
                    egress,
                ))
            })
            .unwrap(),
        )
        .unwrap();
    registry
}

fn discover_file_schema_artifacts_for_test(
    resource: &cdf_declarative::CompiledResource,
    _secret_provider: &dyn SecretProvider,
    dependencies: FileRuntimeDependencies,
    options: SchemaDiscoveryExecutionOptions,
) -> Result<ResourceSchemaDiscoveryArtifacts> {
    let formats = Arc::clone(dependencies.formats());
    let installed_dependencies = dependencies.clone();
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry.register(
        FileSourceDriver::new(formats, move |_secrets, _execution, _egress| {
            Ok(installed_dependencies.clone())
        })
        .unwrap(),
    )?;
    let project_root = resource
        .project_root()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| Path::new(".").to_path_buf());
    let plan = resource.source_plan().clone();
    let execution = test_execution_services();
    let prepared_payloads = dependencies.prepared_payloads().clone();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &project_root,
        Arc::new(EnvSecretProvider::from_map(
            std::iter::empty::<(&str, &str)>(),
        )),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_prepared_payloads(prepared_payloads);
    super::discover_resource_schema_with_source_registry(
        resource,
        &registry,
        &plan,
        &resolution,
        options,
    )
}

fn discover_default_file_schema_artifacts_for_test(
    resource: &cdf_declarative::CompiledResource,
    secret_provider: &dyn SecretProvider,
    options: SchemaDiscoveryExecutionOptions,
) -> Result<ResourceSchemaDiscoveryArtifacts> {
    discover_file_schema_artifacts_for_test(
        resource,
        secret_provider,
        file_dependencies(FileTransportFacade::new()),
        options,
    )
}

fn discover_file_schema_for_test(
    resource: &cdf_declarative::CompiledResource,
    secret_provider: &dyn SecretProvider,
    dependencies: FileRuntimeDependencies,
) -> Result<ResourceSchemaDiscovery> {
    Ok(discover_file_schema_artifacts_for_test(
        resource,
        secret_provider,
        dependencies,
        Default::default(),
    )?
    .discovery)
}

fn prepare_file_discover_resource(
    project_root: &Path,
    resource: &cdf_declarative::CompiledResource,
    _secret_provider: &dyn SecretProvider,
) -> Result<PreparedDiscoveredResource> {
    prepare_file_discover_resource_with_dependencies_for_test(
        project_root,
        resource,
        _secret_provider,
        file_dependencies(FileTransportFacade::new()),
    )
}

fn prepare_file_discover_resource_with_dependencies_for_test(
    project_root: &Path,
    resource: &cdf_declarative::CompiledResource,
    secret_provider: &dyn SecretProvider,
    dependencies: FileRuntimeDependencies,
) -> Result<PreparedDiscoveredResource> {
    if !matches!(
        resource.descriptor().schema_source,
        SchemaSource::Discover | SchemaSource::Hints { snapshot: None, .. }
    ) {
        return Ok(PreparedDiscoveredResource {
            resource: resource.clone(),
            discovery: None,
        });
    }
    let mut artifacts = discover_file_schema_artifacts_for_test(
        resource,
        secret_provider,
        dependencies,
        SchemaDiscoveryExecutionOptions::new()
            .with_observation_cache(ObservationCacheStore::new(project_root)),
    )?;
    let prepared = compile_discovered_schema_artifacts(resource, &mut artifacts)?;
    let discovery = artifacts.discovery.clone();
    write_schema_discovery_artifacts(project_root, &artifacts)?;
    Ok(PreparedDiscoveredResource {
        resource: prepared,
        discovery: Some(discovery),
    })
}

fn discover_rest_schema_artifacts_for_test(
    project_root: &Path,
    resource: &cdf_declarative::CompiledResource,
    transport: RecordingTransport,
    secret_provider: Arc<dyn SecretProvider + Send + Sync>,
    prepared_payloads: cdf_runtime::PreparedSourcePayloads,
) -> Result<ResourceSchemaDiscoveryArtifacts> {
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry.register(cdf_source_rest::RestSourceDriver::new(move || {
        Ok(Box::new(transport.clone()))
    })?)?;
    let plan = resource.source_plan().clone();
    let execution = test_execution_services();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        project_root,
        secret_provider,
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_prepared_payloads(prepared_payloads);
    discover_resource_schema_with_source_registry(
        resource,
        &registry,
        &plan,
        &resolution,
        SchemaDiscoveryExecutionOptions::default(),
    )
}

const BOOK_PROJECT: &str = r#"
[project]
name = "acme_data"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"
retention = { default = "5 runs" }

[environments.prod]
destination = "postgres://secret://env/PROD_DWH"
retention = { default = "90d", financial = "400d" }

[environments.prod.destination_policy.postgres]
merge_dedup = "fail"

[python]
interpreter = ".venv/bin/python"

[defaults]
contract = "governed"

[resources."github.*"]
source = "resources/github.toml"

[resources."events.raw"]
source = "python://src/events.py#raw_events"
trust = "serving"
freshness = { expect_every = "15m", alert_after = "45m" }
"#;

const GITHUB_RESOURCE: &str = r#"
[source.github]
kind = "rest"
base_url = "https://api.github.com"
auth = { kind = "bearer", token = "secret://env/GITHUB_TOKEN" }

[resource.issues]
path = "/repos/{owner}/{repo}/issues"
records = "$"
primary_key = ["id"]
merge_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "5m" }
write_disposition = "merge"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "timestamp_micros", nullable = false, timezone = "UTC" },
] }
"#;

#[test]
fn book_project_shape_parses_into_typed_models() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();

    assert_eq!(config.project.name, "acme_data");
    assert_eq!(
        config.driver_options["python"]["interpreter"],
        ".venv/bin/python"
    );
    assert_eq!(config.defaults.contract.as_deref(), Some("governed"));
    assert_eq!(
        config.resources["events.raw"]
            .freshness
            .as_ref()
            .unwrap()
            .alert_after
            .unwrap()
            .millis(),
        2_700_000
    );
    assert_eq!(
        config.environments["dev"]
            .retention
            .as_ref()
            .unwrap()
            .default,
        Some(RetentionRule::Runs(5))
    );
}

#[test]
fn environment_overlays_inherit_unspecified_settings() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(prod.state, "sqlite://.cdf/state.db");
    assert_eq!(prod.packages, ".cdf/packages");
    assert_eq!(prod.destination, "postgres://secret://env/PROD_DWH");
    assert_eq!(
        prod.retention.as_ref().unwrap().default,
        Some(RetentionRule::Duration(DurationSpec::from_millis(
            90 * 86_400_000
        )))
    );
    assert_eq!(
        prod.retention.as_ref().unwrap().financial,
        Some(RetentionRule::Duration(DurationSpec::from_millis(
            400 * 86_400_000
        )))
    );
    assert_eq!(
        prod.destination_policy
            .postgres
            .as_ref()
            .unwrap()
            .merge_dedup,
        PostgresMergeDedupPolicy::Fail
    );
}

#[test]
fn destination_policy_overlays_from_default_environment() {
    let project = BOOK_PROJECT
        .replace(
            "[environments.prod.destination_policy.postgres]\nmerge_dedup = \"fail\"\n\n",
            "",
        )
        .replace(
            "retention = { default = \"5 runs\" }\n\n",
            "retention = { default = \"5 runs\" }\n\n[environments.dev.destination_policy.postgres]\nmerge_dedup = \"fail\"\n\n",
        );
    let config = parse_cdf_toml(&project).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(
        prod.destination_policy
            .postgres
            .as_ref()
            .unwrap()
            .merge_dedup,
        PostgresMergeDedupPolicy::Fail
    );
}

#[test]
fn validation_resolves_declarative_sources_and_redacts_secret_values() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let provider = DefaultSecretProvider::new(
        EnvSecretProvider::from_map([
            ("GITHUB_TOKEN", "github-token-value"),
            ("PROD_DWH", "postgres-dsn-value"),
        ]),
        FileSecretProvider::without_root(),
    );

    let report = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resolver,
        &provider,
    )
    .unwrap();

    assert_eq!(report.declarative_resources, 1);
    assert_eq!(report.external_resources, 1);
    assert_eq!(report.checked_secrets.len(), 2);
    let debug = format!("{report:?}");
    assert!(!debug.contains("github-token-value"));
    assert!(!debug.contains("postgres-dsn-value"));
    assert!(debug.contains("secret://env/GITHUB_TOKEN"));
}

#[test]
fn validation_checks_missing_secret_without_printing_values() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let provider = EnvSecretProvider::from_map([("GITHUB_TOKEN", "github-token-value")]);

    let error = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resolver,
        &provider,
    )
    .unwrap_err();

    assert!(error.to_string().contains("secret://env/PROD_DWH"));
    assert!(!error.to_string().contains("github-token-value"));
}

#[test]
fn plaintext_secret_values_are_rejected_where_references_are_required() {
    let bad_resource = GITHUB_RESOURCE.replace("secret://env/GITHUB_TOKEN", "plain-token-value");
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", bad_resource);
    let provider = EnvSecretProvider::from_map([("PROD_DWH", "postgres-dsn-value")]);

    let error = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resolver,
        &provider,
    )
    .unwrap_err();

    assert!(error.to_string().contains("secret://"));
    assert!(!error.to_string().contains("plain-token-value"));
}

#[test]
fn file_secret_provider_resolves_without_exposing_contents() {
    let root = env::temp_dir().join(format!("cdf-project-secret-test-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("api-token"), "file-secret-value\n").unwrap();
    let provider = FileSecretProvider::new(&root);
    let uri = SecretUri::new("secret://file/api-token").unwrap();

    let value = provider.resolve(&uri).unwrap();

    assert_eq!(value.as_str().unwrap(), "file-secret-value");
    assert_eq!(format!("{value:?}"), "[REDACTED]");
    assert_eq!(format!("{value}"), "[REDACTED]");
    let _ = fs::remove_file(root.join("api-token"));
    let _ = fs::remove_dir(root);
}

#[test]
fn lockfile_generation_round_trips_and_diffs_semantic_changes() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let resources =
        compile_project_declarative_resources(&test_source_registry(), &config, &resolver).unwrap();
    let sheet = destination_sheet("duckdb", TypeMappingFidelity::Lossless);
    let sheet_artifact =
        DestinationSheetArtifact::new(sheet.clone(), DestinationProtocolCapabilities::default())
            .unwrap();
    let dependency_tuple = DependencyTuple {
        cdf: "0.1.0".to_owned(),
        arrow_rs: "58.3.0".to_owned(),
        datafusion: Some("54.0.0".to_owned()),
        object_store: None,
        duckdb_rs: None,
        rust: None,
    };

    let lock = generate_lockfile_with_destination_artifacts(
        &config,
        &resources,
        dependency_tuple.clone(),
        std::slice::from_ref(&sheet_artifact),
        BTreeMap::new(),
    )
    .unwrap();
    let encoded = lock_to_toml(&lock).unwrap();
    assert!(encoded.contains("protocol_capabilities"));
    assert!(encoded.contains("corrections"));
    let decoded = parse_lock(&encoded).unwrap();
    assert_eq!(decoded, lock);
    assert_eq!(lock_to_toml(&decoded).unwrap(), encoded);
    assert_eq!(lock.normalizer, NORMALIZER_NAMECASE_V1);
    let resource = lock.resources.get("github.issues").unwrap();
    assert!(resource.capability_sheet_hash.starts_with("sha256:"));
    assert_eq!(resource.execution_extent, ExecutionExtent::bounded());
    assert!(resource.execution_extent_hash.is_none());
    assert!(resource.compiled_stream_policy.is_none());
    assert!(!encoded.contains("execution_extent"));
    assert!(!encoded.contains("compiled_stream_policy"));
    let mut tampered_lock = lock.clone();
    tampered_lock
        .resources
        .get_mut("github.issues")
        .unwrap()
        .execution_extent_hash = Some(format!("sha256:{}", "00".repeat(32)));
    assert!(
        lock_to_toml(&tampered_lock)
            .unwrap_err()
            .message
            .contains("execution-extent hash")
    );
    assert!(
        resource
            .schema_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    let contract = resource.contract.as_ref().unwrap();
    assert!(
        contract
            .policy_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        contract
            .validation_program_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(
        lock.destinations["duckdb"].sheet.type_mappings[0].fidelity,
        TypeMappingFidelity::Lossless
    );
    assert_eq!(
        lock.destinations["duckdb"].sheet_hash,
        semantic_hash(&sheet_artifact).unwrap()
    );

    let changed_sheet = destination_sheet(
        "duckdb",
        TypeMappingFidelity::LossyRequiresContractAllowance,
    );
    let changed_artifact =
        DestinationSheetArtifact::new(changed_sheet, DestinationProtocolCapabilities::default())
            .unwrap();
    let changed = generate_lockfile_with_destination_artifacts(
        &config,
        &resources,
        dependency_tuple.clone(),
        &[changed_artifact],
        BTreeMap::new(),
    )
    .unwrap();
    let diffs = diff_lockfiles(&lock, &changed).unwrap();

    assert!(diffs.iter().any(|diff| diff.path.contains("sheet_hash")));
    assert!(diffs.iter().any(|diff| {
        diff.path
            .contains("destinations.duckdb.sheet.type_mappings")
    }));

    let postgres_artifact = cdf_dest_postgres::PostgresDestination::new()
        .sheet_artifact()
        .unwrap();
    let parquet_temp = tempfile::tempdir().unwrap();
    let parquet_artifact = cdf_dest_parquet::ParquetDestination::new_filesystem(
        parquet_temp.path(),
        test_execution_services(),
    )
    .unwrap()
    .sheet_artifact()
    .unwrap();
    let typed_lock = generate_lockfile_with_destination_artifacts(
        &config,
        &resources,
        dependency_tuple,
        &[postgres_artifact.clone(), parquet_artifact.clone()],
        BTreeMap::new(),
    )
    .unwrap();
    let typed_encoded = lock_to_toml(&typed_lock).unwrap();
    assert!(typed_encoded.contains("protocol_capabilities"));
    assert!(typed_encoded.contains("corrections"));
    assert!(typed_encoded.contains("object_key_rules"));
    assert!(typed_encoded.contains("object-key-component-v1"));
    let typed_decoded = parse_lock(&typed_encoded).unwrap();
    assert_eq!(typed_decoded, typed_lock);
    assert_eq!(lock_to_toml(&typed_decoded).unwrap(), typed_encoded);
    assert_eq!(
        typed_lock.destinations["postgres"]
            .sheet_artifact()
            .unwrap(),
        postgres_artifact
    );
    assert_eq!(
        typed_lock.destinations["parquet_object_store"]
            .sheet_artifact()
            .unwrap(),
        parquet_artifact
    );
}

fn schema_lease(store: &InMemoryScopeLeaseStore) -> ScopeLease {
    store
        .acquire(
            ScopeKey::SchemaContract {
                contract: ContractRef::new("orders").unwrap(),
            },
            LeaseOwnerId::new("promotion-executor").unwrap(),
            1_000,
        )
        .unwrap()
}

#[test]
fn lock_file_cas_requires_exact_prior_bytes_hash_and_current_fence() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"version = 1\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    let store = InMemoryScopeLeaseStore::new();
    let lease = schema_lease(&store);

    let report =
        compare_and_swap_lock_file(&path, &expected, b"version = 2\n", &store, &lease).unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"version = 2\n");
    assert_eq!(report.installed, read_lock_file_authority(&path).unwrap());
    #[cfg(unix)]
    assert!(report.parent_directory_synced);

    let error =
        compare_and_swap_lock_file(&path, &expected, b"version = 3\n", &store, &lease).unwrap_err();
    assert!(error.message.contains("prior authority changed"));
    assert_eq!(fs::read(&path).unwrap(), b"version = 2\n");

    let mut inconsistent = read_lock_file_authority(&path).unwrap();
    inconsistent.sha256.push_str("tampered");
    let error = compare_and_swap_lock_file(&path, &inconsistent, b"version = 3\n", &store, &lease)
        .unwrap_err();
    assert!(error.message.contains("supplied bytes hash"));
}

#[test]
fn lock_file_cas_failpoints_model_each_crash_boundary() {
    for (failpoint, expected_bytes) in [
        (LockFileCasFailpoint::BeforeTempSync, b"old\n".as_slice()),
        (LockFileCasFailpoint::BeforeRename, b"old\n".as_slice()),
        (LockFileCasFailpoint::AfterRename, b"new\n".as_slice()),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(LOCK_FILE_NAME);
        fs::write(&path, b"old\n").unwrap();
        let expected = read_lock_file_authority(&path).unwrap();
        let store = InMemoryScopeLeaseStore::new();
        let lease = schema_lease(&store);
        let error = compare_and_swap_lock_file_with_failpoint(
            &path,
            &expected,
            b"new\n",
            &store,
            &lease,
            Some(failpoint),
        )
        .unwrap_err();
        assert!(
            error
                .message
                .contains("injected cdf.lock publication crash")
        );
        assert_eq!(fs::read(&path).unwrap(), expected_bytes);
    }
}

#[test]
fn guarded_lock_writer_atomically_creates_replaces_and_rejects_stale_authority() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    write_lock_file_guarded(&path, None, b"created\n").unwrap();
    let created = read_lock_file_authority(&path).unwrap();
    assert_eq!(created.bytes, b"created\n");

    write_lock_file_guarded(&path, Some(&created), b"replaced\n").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"replaced\n");
    let error = write_lock_file_guarded(&path, Some(&created), b"stale\n").unwrap_err();
    assert!(error.message.contains("prior authority changed"));
    assert_eq!(fs::read(&path).unwrap(), b"replaced\n");

    let temporary_files = fs::read_dir(temp.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".cas.tmp"))
        .count();
    assert_eq!(temporary_files, 0);
    assert!(
        temp.path()
            .join(".cdf/locks/cdf.lock.mutation.lock")
            .is_file()
    );
}

#[test]
fn guarded_cdf_writer_cannot_enter_cas_final_check_rename_window() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"old\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    let writer_expected = expected.clone();
    let store = InMemoryScopeLeaseStore::new();
    let lease = schema_lease(&store);
    let (at_publication_tx, at_publication_rx) = mpsc::channel();
    let (continue_tx, continue_rx) = mpsc::channel();
    let cas_path = path.clone();
    let cas = thread::spawn(move || {
        compare_and_swap_lock_file_with_publication_hook(
            &cas_path,
            &expected,
            b"cas\n",
            &store,
            &lease,
            || {
                at_publication_tx.send(()).unwrap();
                continue_rx.recv().unwrap();
                Ok(())
            },
        )
    });
    at_publication_rx.recv().unwrap();

    let writer_path = path.clone();
    let (attempting_tx, attempting_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        attempting_tx.send(()).unwrap();
        let result =
            write_lock_file_guarded(&writer_path, Some(&writer_expected), b"ordinary-writer\n");
        done_tx.send(result).unwrap();
    });
    attempting_rx.recv().unwrap();
    assert!(
        done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "ordinary CDF writer must block while CAS holds the project mutation guard"
    );

    continue_tx.send(()).unwrap();
    assert_eq!(cas.join().unwrap().unwrap().installed.bytes, b"cas\n");
    let writer_error = done_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap_err();
    assert!(writer_error.message.contains("prior authority changed"));
    writer.join().unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"cas\n");
}

struct StaleAtPublicationStore {
    checks: AtomicUsize,
}

impl ScopeLeaseStore for StaleAtPublicationStore {
    fn authority_domain_id(&self) -> cdf_kernel::LeaseAuthorityDomainId {
        cdf_kernel::LeaseAuthorityDomainId::new("stale-publication-test").unwrap()
    }

    fn acquire(
        &self,
        _scope: ScopeKey,
        _owner: LeaseOwnerId,
        _lease_duration_ms: u64,
    ) -> cdf_kernel::Result<ScopeLease> {
        unreachable!()
    }

    fn renew(
        &self,
        _lease: &ScopeLease,
        _lease_duration_ms: u64,
    ) -> cdf_kernel::Result<ScopeLease> {
        unreachable!()
    }

    fn release(&self, _lease: &ScopeLease) -> cdf_kernel::Result<()> {
        unreachable!()
    }

    fn assert_current(&self, _lease: &ScopeLease) -> cdf_kernel::Result<()> {
        if self.checks.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(())
        } else {
            Err(CdfError::contract("lease superseded before publication"))
        }
    }

    fn prove_expired(
        &self,
        _lease: &ScopeLease,
        _collector: LeaseOwnerId,
        _cleanup_lease_duration_ms: u64,
    ) -> cdf_kernel::Result<Option<cdf_kernel::ExpiredScopeLeaseProof>> {
        Ok(None)
    }
}

#[test]
fn stale_fencing_token_cannot_publish_after_temp_file_sync() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"old\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    let real_store = InMemoryScopeLeaseStore::new();
    let lease = schema_lease(&real_store);
    let store = StaleAtPublicationStore {
        checks: AtomicUsize::new(0),
    };

    let error = compare_and_swap_lock_file(&path, &expected, b"new\n", &store, &lease).unwrap_err();
    assert!(error.message.contains("superseded before publication"));
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
}

#[test]
fn lease_expiring_during_temp_write_cannot_publish() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"old\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    struct ExpiringClock(AtomicUsize);

    impl ScopeLeaseClock for ExpiringClock {
        fn now_ms(&self) -> cdf_kernel::Result<i64> {
            Ok(match self.0.fetch_add(1, Ordering::SeqCst) {
                0 => 1_000,
                1 => 1_099,
                _ => 1_100,
            })
        }
    }

    let store = InMemoryScopeLeaseStore::with_clock(Arc::new(ExpiringClock(AtomicUsize::new(0))));
    let lease = store
        .acquire(
            ScopeKey::SchemaContract {
                contract: ContractRef::new("expiring").unwrap(),
            },
            LeaseOwnerId::new("promotion-executor").unwrap(),
            100,
        )
        .unwrap();

    let error = compare_and_swap_lock_file(&path, &expected, b"new\n", &store, &lease).unwrap_err();
    assert!(error.message.contains("expired, released, or superseded"));
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
}

#[test]
fn lock_file_atomicity_capabilities_state_platform_limits() {
    let capabilities = lock_file_atomicity_capabilities();
    assert!(!capabilities.limitation.is_empty());
    assert!(capabilities.cooperating_cdf_writers_serialized);
    assert!(capabilities.limitation.contains("non-cooperating"));
    #[cfg(unix)]
    {
        assert!(capabilities.atomic_rename_over_existing);
        assert!(capabilities.parent_directory_fsync);
        assert!(capabilities.limitation.contains("same Unix filesystem"));
        assert!(capabilities.limitation.contains("POSIX rename"));
    }
    #[cfg(not(unix))]
    {
        assert!(!capabilities.atomic_rename_over_existing);
        assert!(!capabilities.parent_directory_fsync);
    }
}

#[test]
fn schema_snapshot_artifact_uses_deterministic_hash_and_project_path() {
    let resource_id = ResourceId::new("github.issues").unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "payload",
            DataType::Struct(Fields::from(vec![Field::new(
                "source",
                DataType::Utf8,
                true,
            )])),
            true,
        ),
    ]);
    let metadata = BTreeMap::from([
        (
            "cdf:normalizer".to_owned(),
            NORMALIZER_NAMECASE_V1.to_owned(),
        ),
        ("probe".to_owned(), "parquet-footer".to_owned()),
    ]);

    let artifact = SchemaSnapshotArtifact::new(&resource_id, &schema, metadata.clone()).unwrap();
    let repeated = SchemaSnapshotArtifact::new(&resource_id, &schema, metadata).unwrap();

    assert_eq!(artifact.schema_hash, repeated.schema_hash);
    assert_eq!(artifact.schema.to_arrow().unwrap(), schema);
    assert_eq!(
        artifact.path,
        format!(".cdf/schemas/github.issues@{}.json", artifact.schema_hash)
    );
    assert_eq!(artifact.hash_input["resource_id"], "github.issues");
    assert_eq!(artifact.hash_input["metadata"]["probe"], "parquet-footer");
    assert_eq!(
        artifact.hash_input["schema"]["fields"][2]["data_type"]["kind"],
        "struct"
    );
    assert_eq!(
        artifact.hash_input["schema"]["fields"][2]["data_type"]["fields"][0]["name"],
        "source"
    );

    let temp = tempfile::tempdir().unwrap();
    let store = SchemaSnapshotStore::new(temp.path());
    let path = store.write(&artifact).unwrap();
    assert_eq!(path, temp.path().join(&artifact.path));
    assert_eq!(store.read(&artifact.reference()).unwrap(), artifact);

    let mut tampered = artifact.clone();
    tampered
        .metadata
        .insert("probe".to_owned(), "changed".to_owned());
    assert!(tampered.validate_hash_input().is_err());

    let mut escaped = artifact.reference();
    escaped.path = "../outside.json".to_owned();
    let error = store.read(&escaped).unwrap_err().to_string();
    assert!(error.contains("schema snapshot reference path"));
}

#[test]
fn discovery_executor_budget_defaults_and_rejects_invalid_shapes() {
    let budget = DiscoveryExecutorBudget::default();
    assert_eq!(budget.max_bytes_per_file(), 64 * 1024 * 1024);
    assert_eq!(budget.max_records_per_file(), 1_000);
    assert_eq!(budget.max_total_in_flight_bytes(), 128 * 1024 * 1024);
    assert_eq!(budget.max_concurrent_probes(), 8);
    let options = SchemaDiscoveryExecutionOptions::new();
    assert_eq!(options.budget(), &budget);

    for (bytes_per_file, records_per_file, total, probes, expected) in [
        (0, 1, 1, 1, "max_bytes_per_file"),
        (1, 0, 1, 1, "max_records_per_file"),
        (1, 1, 0, 1, "max_total_in_flight_bytes"),
        (1, 1, 1, 0, "max_concurrent_probes"),
        (2, 1, 1, 1, "cannot exceed"),
        (u64::MAX / 2 + 1, 1, u64::MAX, 2, "overflows"),
    ] {
        let error = DiscoveryExecutorBudget::new(bytes_per_file, records_per_file, total, probes)
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "unexpected error: {error}");
    }

    let invalid_json = r#"{
        "max_bytes_per_file": 0,
        "max_records_per_file": 1,
        "max_total_in_flight_bytes": 1,
        "max_concurrent_probes": 1
    }"#;
    assert!(serde_json::from_str::<DiscoveryExecutorBudget>(invalid_json).is_err());
}

#[test]
fn discovery_manifest_is_canonical_content_addressed_and_fail_closed() {
    let resource_id = ResourceId::new("events.raw").unwrap();
    // Metadata authorities such as SQL catalogs can observe a complete schema
    // without transferring source payload bytes.
    let mut first = observed_discovery_candidate("catalog://warehouse/a", "sha256:a", 0);
    first.identity.size_bytes = None;
    let mut second = observed_discovery_candidate("file:///data/b.parquet", "sha256:b", 64);
    second.metadata_variance = vec![DiscoveryMetadataVariance {
        scope: DiscoveryMetadataScope::Field,
        path: "amount".to_owned(),
        key: "source.logical_type".to_owned(),
        observed_values: vec!["utf8".to_owned(), "decimal".to_owned(), "utf8".to_owned()],
    }];
    let input = DiscoveryManifestInput {
        resource_id: resource_id.as_str().to_owned(),
        baseline_schema_hash: Some(SchemaHash::new("sha256:baseline").unwrap()),
        effective_schema_hash: Some(SchemaHash::new("sha256:effective").unwrap()),
        file_coverage: DiscoveryFileCoverage::AllFiles,
        within_file_coverage: DiscoveryWithinFileCoverage::FormatMetadata,
        selector: None,
        budget: DiscoveryExecutorBudget::default(),
        normalizer_version: "namecase-v1".to_owned(),
        policy_version: "evolve-v1".to_owned(),
        candidates: vec![second, first],
    };
    let artifact = DiscoveryManifestArtifact::new(input.clone()).unwrap();
    let repeated = DiscoveryManifestArtifact::new(input).unwrap();

    assert_eq!(artifact, repeated);
    let same_observation_new_baseline = DiscoveryManifestArtifact::new(DiscoveryManifestInput {
        resource_id: artifact.resource_id.clone(),
        baseline_schema_hash: Some(SchemaHash::new("sha256:next-baseline").unwrap()),
        effective_schema_hash: artifact.effective_schema_hash.clone(),
        file_coverage: artifact.file_coverage.clone(),
        within_file_coverage: artifact.within_file_coverage,
        selector: artifact.selector.clone(),
        budget: artifact.budget.clone(),
        normalizer_version: artifact.normalizer_version.clone(),
        policy_version: artifact.policy_version.clone(),
        candidates: artifact.candidates.clone(),
    })
    .unwrap();
    assert_ne!(
        artifact.manifest_hash,
        same_observation_new_baseline.manifest_hash
    );
    assert!(artifact.has_same_observation(&same_observation_new_baseline));
    assert_eq!(
        artifact
            .candidates
            .iter()
            .map(|candidate| candidate.canonical_location.as_str())
            .collect::<Vec<_>>(),
        vec!["catalog://warehouse/a", "file:///data/b.parquet"]
    );
    assert_eq!(artifact.candidates[0].probe_bytes, Some(0));
    assert_eq!(artifact.candidates[0].identity.size_bytes, None);
    assert_eq!(
        artifact.candidates[1].metadata_variance[0].observed_values,
        vec!["decimal", "utf8"]
    );
    assert_eq!(artifact.hash_input["file_coverage"], "all_files");
    assert_eq!(
        artifact.hash_input["within_file_coverage"],
        "format_metadata"
    );
    assert_eq!(
        artifact.path,
        format!(
            ".cdf/schemas/events.raw@{}.discovery.json",
            artifact.manifest_hash
        )
    );

    let temp = tempfile::tempdir().unwrap();
    let store = DiscoveryManifestStore::new(temp.path());
    assert!(store.write_if_changed(&artifact).unwrap());
    assert!(!store.write_if_changed(&artifact).unwrap());
    assert_eq!(store.read(&artifact.reference()).unwrap(), artifact);
    let schema_dir_entries = std::fs::read_dir(temp.path().join(".cdf/schemas"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        schema_dir_entries
            .iter()
            .all(|name| !name.ends_with(".tmp"))
    );

    let mut unsafe_reference = artifact.reference();
    unsafe_reference.path = "../manifest.json".to_owned();
    assert!(
        store
            .read(&unsafe_reference)
            .unwrap_err()
            .to_string()
            .contains("reference path")
    );

    let missing = DiscoveryManifestReference {
        manifest_hash: DiscoveryManifestHash::new("sha256:missing").unwrap(),
        path: ".cdf/schemas/events.raw@sha256:missing.discovery.json".to_owned(),
    };
    assert!(
        store
            .read(&missing)
            .unwrap_err()
            .to_string()
            .contains("read")
    );

    let wrong_hash = DiscoveryManifestReference {
        manifest_hash: DiscoveryManifestHash::new("sha256:wrong").unwrap(),
        path: artifact.path.clone(),
    };
    assert!(
        store
            .read(&wrong_hash)
            .unwrap_err()
            .to_string()
            .contains("does not match its hash/path reference")
    );

    let path = temp.path().join(&artifact.path);
    let mut tampered = artifact.clone();
    tampered.policy_version = "freeze-v1".to_owned();
    std::fs::write(&path, serde_json::to_vec_pretty(&tampered).unwrap()).unwrap();
    assert!(
        store
            .read(&artifact.reference())
            .unwrap_err()
            .to_string()
            .contains("hash_input")
    );
}

#[test]
fn sampled_discovery_manifest_enforces_truthful_participation() {
    let first = observed_discovery_candidate("file:///data/00.parquet", "sha256:00", 32);
    let middle = unobserved_discovery_candidate("file:///data/01.parquet", "etag:01");
    let last = observed_discovery_candidate("file:///data/02.parquet", "sha256:02", 40);
    let selector_candidates = [&first, &middle, &last]
        .into_iter()
        .map(|candidate| DiscoverySelectorCandidate {
            canonical_location: candidate.canonical_location.clone(),
            identity: candidate.identity.clone(),
        })
        .collect::<Vec<_>>();
    let selector = plan_discovery_selection(
        &ResourceId::new("events.sampled").unwrap(),
        Some(2),
        &selector_candidates,
    )
    .unwrap()
    .selector
    .unwrap();
    let input = DiscoveryManifestInput {
        resource_id: "events.sampled".to_owned(),
        baseline_schema_hash: None,
        effective_schema_hash: Some(SchemaHash::new("sha256:sampled").unwrap()),
        file_coverage: DiscoveryFileCoverage::SampledFiles,
        within_file_coverage: DiscoveryWithinFileCoverage::FormatMetadata,
        selector: Some(selector),
        budget: DiscoveryExecutorBudget::default(),
        normalizer_version: "namecase-v1".to_owned(),
        policy_version: "evolve-v1".to_owned(),
        candidates: vec![last, middle.clone(), first],
    };
    let artifact = DiscoveryManifestArtifact::new(input.clone()).unwrap();
    assert_eq!(artifact.file_coverage, DiscoveryFileCoverage::SampledFiles);
    assert_eq!(
        artifact.candidates[1].participation,
        DiscoveryParticipation::Unobserved
    );
    assert!(artifact.candidates[1].physical_schema_hash.is_none());
    assert!(artifact.candidates[1].probe_bytes.is_none());
    assert!(artifact.candidates[1].schema_verdict.is_none());

    let mut false_score = input.clone();
    false_score.selector.as_mut().unwrap().selected[0].score_sha256 = "0".repeat(64);
    let error = DiscoveryManifestArtifact::new(false_score)
        .unwrap_err()
        .to_string();
    assert!(error.contains("canonical membership, scores, or strata"));

    let mut false_unobserved = input;
    false_unobserved.candidates[1].physical_schema_hash =
        Some(SchemaHash::new("sha256:invented").unwrap());
    let error = DiscoveryManifestArtifact::new(false_unobserved)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unobserved") && error.contains("forbids"));

    let mut false_observed = middle;
    false_observed.participation = DiscoveryParticipation::Observed;
    let error = DiscoveryManifestArtifact::new(DiscoveryManifestInput {
        resource_id: "events.false-observed".to_owned(),
        baseline_schema_hash: None,
        effective_schema_hash: None,
        file_coverage: DiscoveryFileCoverage::AllFiles,
        within_file_coverage: DiscoveryWithinFileCoverage::FormatMetadata,
        selector: None,
        budget: DiscoveryExecutorBudget::default(),
        normalizer_version: "namecase-v1".to_owned(),
        policy_version: "evolve-v1".to_owned(),
        candidates: vec![false_observed],
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("observed") && error.contains("requires"));
}

#[test]
fn schema_snapshot_current_version_covers_schema_and_manifest_and_rejects_old_versions() {
    let resource = ResourceId::new("current.resource").unwrap();
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let schema_only = SchemaSnapshotArtifact::new(&resource, &schema, BTreeMap::new()).unwrap();
    assert_eq!(schema_only.version, SCHEMA_SNAPSHOT_ARTIFACT_VERSION);
    assert_eq!(
        schema_only.schema_hash.as_str(),
        "sha256:7080613cfd096dd56f3081f8867c226ffdbf950890b3c9e034f604e58810c617"
    );
    assert!(
        serde_json::to_value(&schema_only)
            .unwrap()
            .get("discovery_manifest")
            .is_none()
    );
    assert!(
        serde_json::to_value(schema_only.reference())
            .unwrap()
            .get("discovery_manifest")
            .is_none()
    );

    let manifest = DiscoveryManifestArtifact::new(DiscoveryManifestInput {
        resource_id: resource.as_str().to_owned(),
        baseline_schema_hash: None,
        effective_schema_hash: None,
        file_coverage: DiscoveryFileCoverage::AllFiles,
        within_file_coverage: DiscoveryWithinFileCoverage::FormatMetadata,
        selector: None,
        budget: DiscoveryExecutorBudget::default(),
        normalizer_version: "namecase-v1".to_owned(),
        policy_version: "evolve-v1".to_owned(),
        candidates: vec![observed_discovery_candidate(
            "file:///data/current.parquet",
            "sha256:current",
            24,
        )],
    })
    .unwrap();
    let linked = SchemaSnapshotArtifact::new_with_discovery_manifest(
        &resource,
        &schema,
        BTreeMap::new(),
        manifest.reference(),
    )
    .unwrap();
    assert_eq!(linked.version, SCHEMA_SNAPSHOT_ARTIFACT_VERSION);
    assert_ne!(linked.schema_hash, schema_only.schema_hash);
    assert_eq!(
        linked.discovery_manifest_reference().unwrap(),
        Some(manifest.reference())
    );
    assert_eq!(
        linked.reference().discovery_manifest().unwrap(),
        Some(manifest.reference())
    );
    assert_eq!(
        linked.hash_input["discovery_manifest"]["manifest_hash"],
        manifest.manifest_hash.as_str()
    );

    let temp = tempfile::tempdir().unwrap();
    let manifest_store = DiscoveryManifestStore::new(temp.path());
    manifest_store.write(&manifest).unwrap();
    let snapshot_store = SchemaSnapshotStore::new(temp.path());
    snapshot_store.write(&linked).unwrap();
    assert_eq!(snapshot_store.read(&linked.reference()).unwrap(), linked);

    for old_version in 1..SCHEMA_SNAPSHOT_ARTIFACT_VERSION {
        let mut old = linked.clone();
        old.version = old_version;
        std::fs::write(
            temp.path().join(&old.path),
            serde_json::to_vec(&old).unwrap(),
        )
        .unwrap();
        let error = snapshot_store
            .read(&linked.reference())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("unsupported artifact version")
                && error.contains(&SCHEMA_SNAPSHOT_ARTIFACT_VERSION.to_string())
        );
    }
    snapshot_store.write(&linked).unwrap();

    std::fs::remove_file(temp.path().join(&manifest.path)).unwrap();
    let error = snapshot_store
        .read(&linked.reference())
        .unwrap_err()
        .to_string();
    assert!(error.contains("read") && error.contains("discovery"));
}

fn observed_discovery_candidate(
    location: &str,
    physical_schema_seed: &str,
    probe_bytes: u64,
) -> DiscoveryCandidateEvidence {
    let physical_schema = Schema::new(vec![Field::new(physical_schema_seed, DataType::Utf8, true)]);
    let physical_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&physical_schema).unwrap();
    DiscoveryCandidateEvidence {
        transport: "file".to_owned(),
        canonical_location: location.to_owned(),
        identity: DiscoveryBoundedIdentity {
            size_bytes: Some(1024),
            modified_at_ms: Some(1_700_000_000_000),
            value: Some(format!("bounded:{location}")),
            strength: DiscoveryIdentityStrength::BoundedObservation,
        },
        participation: DiscoveryParticipation::Observed,
        metadata_variance: Vec::new(),
        physical_schema_hash: Some(physical_schema_hash),
        physical_schema: Some(
            cdf_kernel::CanonicalArrowSchema::from_arrow(&physical_schema).unwrap(),
        ),
        probe_bytes: Some(probe_bytes),
        probe_records: Some(0),
        schema_verdict: Some(DiscoverySchemaVerdict {
            kind: DiscoverySchemaVerdictKind::Admitted,
            rule: "schema-join-v1".to_owned(),
            details: BTreeMap::new(),
        }),
    }
}

fn unobserved_discovery_candidate(location: &str, identity: &str) -> DiscoveryCandidateEvidence {
    DiscoveryCandidateEvidence {
        transport: "file".to_owned(),
        canonical_location: location.to_owned(),
        identity: DiscoveryBoundedIdentity {
            size_bytes: Some(2048),
            modified_at_ms: None,
            value: Some(identity.to_owned()),
            strength: DiscoveryIdentityStrength::WeakEtag,
        },
        participation: DiscoveryParticipation::Unobserved,
        metadata_variance: Vec::new(),
        physical_schema_hash: None,
        physical_schema: None,
        probe_bytes: None,
        probe_records: None,
        schema_verdict: None,
    }
}

#[test]
fn schema_snapshot_arrow_round_trip_covers_closed_type_vocabulary() {
    let union = UnionFields::try_new(
        [1, 3],
        [
            Field::new("integer", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ],
    )
    .unwrap();
    let fields = vec![
        Field::new("decimal", DataType::Decimal256(76, 9), true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "interval",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        ),
        Field::new("binary_view", DataType::BinaryView, true),
        Field::new("utf8_view", DataType::Utf8View, true),
        Field::new(
            "large_list_view",
            DataType::LargeListView(Field::new("item", DataType::UInt16, true).into()),
            true,
        ),
        Field::new(
            "map",
            DataType::Map(
                Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Float32, true),
                    ])),
                    false,
                )
                .into(),
                false,
            ),
            true,
        ),
        Field::new("union", DataType::Union(union, UnionMode::Dense), true),
        Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::LargeUtf8)),
            true,
        ),
        Field::new(
            "run_end_encoded",
            DataType::RunEndEncoded(
                Field::new("run_ends", DataType::Int32, false).into(),
                Field::new("values", DataType::Utf8, true).into(),
            ),
            true,
        ),
    ];
    let schema = Schema::new(fields);

    assert_eq!(
        SchemaSnapshotSchema::from_arrow(&schema)
            .to_arrow()
            .unwrap(),
        schema
    );
}

#[test]
fn generic_discovery_builds_deterministic_snapshot_without_transport_identity() {
    let resource_id = ResourceId::new("tlc.yellow").unwrap();
    let schema = Schema::new(vec![
        Field::new("VendorID", DataType::Int32, true),
        Field::new(
            "tpep_pickup_datetime",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]);
    let source_identity = BTreeMap::from([
        ("footer_sha256".to_owned(), "sha256:footer".to_owned()),
        ("size_bytes".to_owned(), "123".to_owned()),
        (
            "local_path".to_owned(),
            "/tmp/private/orders.parquet".to_owned(),
        ),
    ]);

    let metadata = BTreeMap::from([
        ("driver".to_owned(), "external_table".to_owned()),
        ("probe".to_owned(), "bounded_metadata".to_owned()),
    ]);
    let artifact = SchemaSnapshotArtifact::new(&resource_id, &schema, metadata.clone()).unwrap();
    let repeated = SchemaSnapshotArtifact::new(&resource_id, &schema, metadata).unwrap();

    assert_eq!(artifact, repeated);
    assert_eq!(artifact.reference(), repeated.reference());
    assert_eq!(artifact.metadata["driver"], "external_table");
    assert_eq!(
        artifact.path,
        format!(".cdf/schemas/tlc.yellow@{}.json", artifact.schema_hash)
    );
    assert_eq!(artifact.hash_input["metadata"]["driver"], "external_table");

    let hash_input = serde_json::to_string(&artifact.hash_input).unwrap();
    assert!(!hash_input.contains("/tmp/private/orders.parquet"));
    assert_eq!(source_identity["local_path"], "/tmp/private/orders.parquet");
    assert!(!hash_input.contains("sha256:footer"));

    let temp = tempfile::tempdir().unwrap();
    let store = SchemaSnapshotStore::new(temp.path());
    store.write(&artifact).unwrap();
    assert_eq!(store.read(&artifact.reference()).unwrap(), artifact);
}

#[test]
fn local_parquet_discover_autopin_writes_normalized_snapshot_and_pins_clone() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/vendors.parquet"));
    let resource = compile_single_project_resource(temp.path());

    let secrets = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());
    let prepared = prepare_file_discover_resource(temp.path(), &resource, &secrets).unwrap();
    let discovery = prepared.discovery.as_ref().unwrap();
    let snapshot_path = temp.path().join(&discovery.snapshot.artifact.path);

    assert!(matches!(
        resource.descriptor().schema_source,
        SchemaSource::Discover
    ));
    assert!(snapshot_path.is_file());
    assert_eq!(
        discovery.snapshot.artifact.metadata["cdf:normalizer"],
        NORMALIZER_NAMECASE_V1
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].name,
        "vendor_id"
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].metadata["cdf:source_name"],
        "VendorID"
    );
    let SchemaSource::Discovered { snapshot } = &prepared.resource.descriptor().schema_source
    else {
        panic!("expected auto-pinned discovered schema source");
    };
    assert_eq!(
        snapshot.schema_hash,
        discovery.snapshot.artifact.schema_hash
    );
    assert_eq!(snapshot.path, discovery.snapshot.artifact.path);
    let schema = prepared.resource.schema();
    let vendor = schema.field_with_name("vendor_id").unwrap();
    assert_eq!(source_name(vendor), Some("VendorID"));

    let repeated = prepare_file_discover_resource(temp.path(), &resource, &secrets).unwrap();
    assert_eq!(
        repeated
            .discovery
            .as_ref()
            .unwrap()
            .snapshot
            .artifact
            .schema_hash,
        discovery.snapshot.artifact.schema_hash
    );
}

#[test]
fn generic_schema_discovery_dispatch_preserves_local_parquet_behavior_without_writes() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/vendors.parquet"));
    let resource = compile_single_project_resource(temp.path());

    let discovery = discover_file_schema_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        file_dependencies(FileTransportFacade::new()),
    )
    .unwrap();

    assert!(!temp.path().join(".cdf/schemas").exists());
    assert_eq!(
        discovery.snapshot.artifact.metadata["probe"],
        "registered-source-discovery"
    );
    assert_eq!(
        discovery.snapshot.source_identity["driver.format"],
        "parquet"
    );
    assert_eq!(
        discovery.snapshot.artifact.metadata["cdf:normalizer"],
        NORMALIZER_NAMECASE_V1
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].name,
        "vendor_id"
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].metadata["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(
        discovery.snapshot.source_identity["path"],
        "vendors.parquet"
    );
    assert!(
        discovery
            .snapshot
            .source_identity
            .contains_key("driver.footer_sha256")
    );
}

#[test]
fn generic_discover_prepare_preserves_local_parquet_autopin_behavior() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/vendors.parquet"));
    let resource = compile_single_project_resource(temp.path());
    let secret_provider = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());

    let prepared =
        prepare_file_discover_resource(temp.path(), &resource, &secret_provider).unwrap();
    let discovery = prepared.discovery.as_ref().unwrap();
    let snapshot_path = temp.path().join(&discovery.snapshot.artifact.path);

    assert!(snapshot_path.is_file());
    assert_eq!(
        discovery.snapshot.artifact.metadata["probe"],
        "registered-source-discovery"
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].name,
        "vendor_id"
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].metadata["cdf:source_name"],
        "VendorID"
    );
    let SchemaSource::Discovered { snapshot } = &prepared.resource.descriptor().schema_source
    else {
        panic!("expected generic auto-pin to set discovered schema source");
    };
    assert_eq!(
        snapshot.schema_hash,
        discovery.snapshot.artifact.schema_hash
    );
}

#[test]
fn project_external_codec_discovers_pins_previews_and_runs_over_remote_provider() {
    let temp = tempfile::tempdir().unwrap();
    write_http_external_mock_project(temp.path());
    let transport = RecordingHttpFileTransport::new(b"MOCK\n".to_vec());
    let registry = external_mock_source_registry(transport.clone());
    let config =
        parse_cdf_toml(&fs::read_to_string(temp.path().join("cdf.toml")).unwrap()).unwrap();
    let resolver = FileResourceSourceResolver::new(temp.path());
    let resource =
        compile_project_declarative_resources_with_root(&registry, &config, &resolver, temp.path())
            .unwrap()
            .remove(0);
    let execution = test_execution_services();
    let secrets = Arc::new(EnvSecretProvider::from_map(
        std::iter::empty::<(&str, &str)>(),
    ));
    let resolution = cdf_runtime::SourceResolutionContext::new(
        temp.path(),
        secrets,
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let source_plan = resource.source_plan().clone();
    let mut artifacts = discover_resource_schema_with_source_registry(
        &resource,
        &registry,
        &source_plan,
        &resolution,
        SchemaDiscoveryExecutionOptions::new()
            .with_observation_cache(ObservationCacheStore::new(temp.path())),
    )
    .unwrap();
    let prepared_resource = compile_discovered_schema_artifacts(&resource, &mut artifacts).unwrap();
    write_schema_discovery_artifacts(temp.path(), &artifacts).unwrap();
    let discovery = &artifacts.discovery;
    assert_eq!(
        discovery.snapshot.artifact.metadata["source_driver"],
        "files"
    );
    assert_eq!(
        discovery.snapshot.source_identity["driver.external_driver"],
        "project_fixture"
    );
    assert_eq!(discovery.snapshot.artifact.schema.fields[0].name, "value");
    assert!(
        temp.path()
            .join(&discovery.snapshot.artifact.path)
            .is_file()
    );
    let SchemaSource::Discovered { snapshot } = &prepared_resource.descriptor().schema_source
    else {
        panic!("external codec cold discovery must pin its schema");
    };
    assert_eq!(
        snapshot.schema_hash,
        discovery.snapshot.artifact.schema_hash
    );

    let source_plan = source_plan
        .bind_schema_authority(
            prepared_resource.descriptor(),
            prepared_resource.schema().as_ref(),
            prepared_resource.effective_schema_runtime().cloned(),
            prepared_resource
                .baseline_observation_schema_catalog()
                .to_vec(),
        )
        .unwrap();
    let runtime = registry.resolve(&source_plan, &resolution).unwrap();
    let plan = live_plan_for_stream(
        runtime.as_ref(),
        &source_plan,
        "pkg-project-external-remote",
    );
    assert_eq!(plan.scan.partitions.len(), 1);
    assert_eq!(
        plan.scan.partitions[0].metadata["format"],
        "project_external_mock"
    );
    let preview =
        futures_executor::block_on(runtime.open(plan.scan.partitions[0].clone())).unwrap();
    let preview_rows = futures_executor::block_on_stream(preview)
        .map(|batch| batch.unwrap().header.row_count)
        .sum::<u64>();
    assert_eq!(preview_rows, 1);

    let report = futures_executor::block_on(run_project(
        ProjectRunRequest {
            resource: ProjectRunSource::new(runtime.as_ref()),
            plan,
            package_root: temp.path().join(".cdf/packages"),
            state_store_path: temp.path().join(".cdf/state.db"),
            pipeline_id: PipelineId::new("pipeline-project-external-remote").unwrap(),
            package_id: "pkg-project-external-remote".to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-project-external-remote").unwrap(),
            destination: ResolvedProjectDestination::duckdb(
                temp.path().join(".cdf/dev.duckdb"),
                TargetName::new("external_events").unwrap(),
            )
            .unwrap(),
            run_id: Some(RunId::new("run-project-external-remote").unwrap()),
            event_sink: None,
            after_receipt_verified: None,
        },
        &execution,
    ))
    .unwrap()
    .into_committed()
    .unwrap();
    assert_eq!(report.row_count, 1);
    assert_eq!(report.segment_count, 1);
    assert!(transport.requests().iter().any(|request| {
        request.method == HttpMethod::Get && !request.headers.contains_key("range")
    }));
}

#[test]
fn http_parquet_schema_discovery_uses_bounded_ranges_without_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = vendor_parquet_bytes_with_rows(10_000);
    assert!(parquet.len() > 16 * 1024);
    write_http_discover_project(temp.path(), "");
    let resource = compile_single_project_resource(temp.path());
    let transport = RecordingHttpFileTransport::new(parquet.clone());
    let dependencies = http_file_dependencies(transport.clone());

    let discovery = discover_file_schema_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies.clone(),
    )
    .unwrap();

    assert!(!temp.path().join(".cdf/schemas").exists());
    assert!(!temp.path().join(".cdf/packages").exists());
    assert!(!temp.path().join(".cdf/state.db").exists());
    assert_eq!(
        discovery.snapshot.artifact.metadata["probe"],
        "registered-source-discovery"
    );
    assert_eq!(
        discovery.snapshot.artifact.metadata["source_driver"],
        "files"
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].name,
        "vendor_id"
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].metadata["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(
        discovery.snapshot.source_identity["path"],
        "https://data.example.test/trip-data/vendors.parquet"
    );
    assert_eq!(
        discovery.snapshot.source_identity["driver.size_bytes"],
        parquet.len().to_string()
    );
    assert_eq!(
        discovery.snapshot.source_identity["driver.etag"],
        "\"fixture-etag\""
    );
    assert_eq!(
        discovery.snapshot.source_identity["driver.row_count"],
        "10000"
    );
    assert!(discovery.snapshot.source_identity["driver.footer_sha256"].starts_with("sha256:"));
    let requests = transport.requests();
    assert_only_bounded_http_file_gets(&requests);
    assert_http_file_gets_download_less_than_fixture(&requests, parquet.len());
}

#[test]
fn remote_observation_cache_exact_hit_avoids_schema_io_and_generation_change_misses() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = vendor_parquet_bytes_with_rows(10_000);
    write_http_discover_project(temp.path(), "");
    let resource = compile_single_project_resource(temp.path());
    let transport = RecordingHttpFileTransport::new(parquet);
    let dependencies = http_file_dependencies(transport.clone());
    let secret_provider = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());
    let cache = ObservationCacheStore::new(temp.path());

    let first = discover_file_schema_artifacts_for_test(
        &resource,
        &secret_provider,
        dependencies.clone(),
        SchemaDiscoveryExecutionOptions::new().with_observation_cache(cache.clone()),
    )
    .unwrap();
    assert_eq!(
        first.discovery.snapshot.source_identity["observation_cache_hits"],
        "0"
    );
    assert_eq!(
        first.discovery.snapshot.source_identity["observation_cache_misses"],
        "1"
    );
    let first_request_count = transport.requests().len();

    let second = discover_file_schema_artifacts_for_test(
        &resource,
        &secret_provider,
        dependencies.clone(),
        SchemaDiscoveryExecutionOptions::new().with_observation_cache(cache.clone()),
    )
    .unwrap();
    let hit_requests = transport
        .requests()
        .into_iter()
        .skip(first_request_count)
        .collect::<Vec<_>>();
    assert!(
        hit_requests
            .iter()
            .all(|request| request.method != HttpMethod::Get)
    );
    assert_eq!(
        second.discovery.snapshot.source_identity["observation_cache_hits"],
        "1"
    );
    assert_eq!(
        second.discovery.snapshot.source_identity["discovery_source_bytes_read"],
        "0"
    );
    assert_eq!(
        second.discovery.snapshot.artifact.schema_hash,
        first.discovery.snapshot.artifact.schema_hash
    );
    assert_eq!(
        second.discovery_manifest.as_ref().unwrap().manifest_hash,
        first.discovery_manifest.as_ref().unwrap().manifest_hash
    );

    transport.set_etag("\"fixture-etag-v2\"");
    let request_count_before_generation_change = transport.requests().len();
    let changed = discover_file_schema_artifacts_for_test(
        &resource,
        &secret_provider,
        dependencies,
        SchemaDiscoveryExecutionOptions::new().with_observation_cache(cache),
    )
    .unwrap();
    let changed_requests = transport
        .requests()
        .into_iter()
        .skip(request_count_before_generation_change)
        .collect::<Vec<_>>();
    assert!(
        changed_requests
            .iter()
            .any(|request| request.method == HttpMethod::Get)
    );
    assert_eq!(
        changed.discovery.snapshot.source_identity["observation_cache_hits"],
        "0"
    );
    assert_eq!(
        changed.discovery.snapshot.source_identity["observation_cache_misses"],
        "1"
    );
}

#[test]
fn object_store_multi_file_parquet_discovery_pins_one_reconciled_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    write_object_store_discover_project(temp.path());
    let store = Arc::new(InMemory::new());
    let first = vendor_parquet_bytes();
    let second_schema = Arc::new(Schema::new(vec![
        Field::new("VendorID", DataType::Int32, false),
        Field::new("fare_amount", DataType::Int64, true),
    ]));
    let second_batch = RecordBatch::try_new(
        second_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(10), None])) as ArrayRef,
        ],
    )
    .unwrap();
    let second = cdf_package::transcode_record_batches_to_parquet_bytes(&[second_batch]).unwrap();
    for (path, bytes) in [
        ("trip-data/2024/01.parquet", first),
        ("trip-data/2024/02.parquet", second),
    ] {
        futures_executor::block_on(store.put(&ObjectPath::from(path), PutPayload::from(bytes)))
            .unwrap();
    }
    let dependencies = file_dependencies(
        FileTransportFacade::new()
            .with_object_store("s3://tlc", store)
            .with_execution_services(test_execution_services()),
    );
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies.clone(),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();

    let field_names = artifacts
        .discovery
        .normalized_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(field_names, vec!["vendor_id", "fare_amount"]);
    assert_eq!(
        artifacts.discovery.snapshot.source_identity["transport"],
        "files"
    );
    assert_eq!(
        artifacts.discovery.snapshot.source_identity["matched_files"],
        "2"
    );
    let manifest = artifacts.discovery_manifest.as_ref().unwrap();
    assert_eq!(manifest.candidates.len(), 2);
    assert!(manifest.candidates.iter().all(|candidate| {
        candidate.participation == DiscoveryParticipation::Observed
            && candidate
                .canonical_location
                .starts_with("s3://tlc/trip-data/2024/")
    }));

    write_schema_discovery_artifacts(temp.path(), &artifacts).unwrap();
    let pinned = apply_discovered_schema(&resource, artifacts.discovery.clone());
    let prepared = prepare_pinned_resource_schema_artifacts(temp.path(), &pinned).unwrap();
    assert_eq!(prepared.discovery_manifest().unwrap().candidates.len(), 2);
    assert!(prepared.resource().effective_schema_runtime().is_none());
    assert_eq!(prepared.resource().schema(), pinned.schema());
}

#[test]
fn declared_multi_file_parquet_defers_physical_admission_to_the_stream() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    fs::write(
        temp.path().join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "VendorID", type = "int64", nullable = false },
] }
"#,
    )
    .unwrap();
    write_vendor_parquet(&temp.path().join("data/01.parquet"));
    write_vendor_parquet(&temp.path().join("data/02.parquet"));
    let resource = compile_single_project_resource(temp.path());
    let dependencies = file_dependencies(FileTransportFacade::new());

    assert!(resource.effective_schema_runtime().is_none());
    let runtime = resolve_file_resource_for_test(&resource, dependencies);
    let plan = live_plan_for_stream(
        runtime.as_ref(),
        resource.source_plan(),
        "pkg-declared-multi-file",
    );
    assert_eq!(plan.scan.partitions.len(), 2);
    assert!(plan.effective_schema_evidence().is_none());
    assert!(!temp.path().join(".cdf").exists());
}

#[test]
fn object_store_gzip_ndjson_discovers_pins_and_executes_through_one_transport() {
    let temp = tempfile::tempdir().unwrap();
    write_object_store_ndjson_discover_project(temp.path());
    let mut source = Vec::new();
    for id in 0..10_000_u64 {
        source.extend_from_slice(format!("{{\"id\":{id},\"kind\":\"k{id}\"}}\n").as_bytes());
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    std::io::Write::write_all(&mut encoder, &source).unwrap();
    let encoded = encoder.finish().unwrap();
    let store = Arc::new(InMemory::new());
    futures_executor::block_on(store.put(
        &ObjectPath::from("prod/2026/07/events.ndjson.gz"),
        PutPayload::from(encoded.clone()),
    ))
    .unwrap();
    let dependencies = file_dependencies(
        FileTransportFacade::new()
            .with_object_store("s3://acme-events", store)
            .with_execution_services(test_execution_services()),
    );
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies.clone(),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();
    assert_eq!(
        artifacts
            .discovery
            .normalized_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["id", "kind"]
    );
    let manifest = artifacts.discovery_manifest.as_ref().unwrap();
    assert_eq!(manifest.candidates.len(), 1);
    assert_eq!(
        manifest.candidates[0].participation,
        DiscoveryParticipation::Observed
    );
    assert!(manifest.candidates[0].probe_bytes.unwrap() <= 8 * 1024 * 1024);

    let prepared = apply_discovered_schema(&resource, artifacts.discovery.clone());
    let runtime = resolve_file_resource_for_test(&prepared, dependencies);
    let plan = live_plan_for_stream(runtime.as_ref(), prepared.source_plan(), "pkg-cloud-ndjson");
    assert_eq!(plan.scan.partitions.len(), 1);
    let preview = futures_executor::block_on(cdf_engine::preview_resource(
        &plan,
        runtime.as_ref(),
        cdf_engine::EnginePreviewLimits::default(),
    ))
    .unwrap();
    assert_eq!(preview.row_count, 500);
    assert_eq!(preview.fields, vec!["id", "kind", "_cdf_variant"]);
    assert_eq!(preview.planned_partition_count, 1);
    assert_eq!(preview.payload_opened_partition_count, 1);
    let stream = futures_executor::block_on(runtime.open(plan.scan.partitions[0].clone())).unwrap();
    let batches = futures_executor::block_on_stream(stream)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        10_000
    );
    let SourcePosition::FileManifest(position) =
        batches[0].header.source_position.as_ref().unwrap()
    else {
        panic!("expected cloud file manifest position")
    };
    assert_eq!(
        position.files[0].path,
        "s3://acme-events/prod/2026/07/events.ndjson.gz"
    );
}

#[test]
fn http_gzip_ndjson_backpressures_and_cancels_before_download_completion() {
    let temp = tempfile::tempdir().unwrap();
    write_http_discover_project(temp.path(), "");
    fs::write(
        temp.path().join("resources/files.toml"),
        r#"
[source.remote]
kind = "files"
root = "https://data.example.test/events"

[resource.events]
glob = "events.ndjson.gz"
format = "ndjson"
compression = "gzip"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "payload", type = "utf8", nullable = false },
] }
"#,
    )
    .unwrap();

    // Sixteen native batches exceed every bounded frontier between transport and this consumer.
    // Compression level zero keeps the fixture cheap while still exercising the production gzip
    // transform and leaving enough source bytes to observe two distinct backpressure plateaus.
    let row_count = 16 * cdf_runtime::DEFAULT_FORMAT_BATCH_ROWS + 128;
    let mut source = Vec::with_capacity(row_count * 48);
    for id in 0..row_count {
        source
            .extend_from_slice(format!(r#"{{"id":{id},"payload":"payload-{id:08x}"}}"#).as_bytes());
        source.push(b'\n');
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::none());
    std::io::Write::write_all(&mut encoder, &source).unwrap();
    let encoded = encoder.finish().unwrap();
    assert!(encoded.len() > 4 * 1024 * 1024);
    let encoded_bytes = u64::try_from(encoded.len()).unwrap();

    let transport = RecordingHttpFileTransport::new(encoded.clone());
    let execution = cdf_engine::StandaloneExecutionHost::default_services(512 * 1024 * 1024)
        .unwrap()
        .1;
    let dependencies = file_dependencies_with_execution(
        FileTransportFacade::new().with_http_transport(transport.clone()),
        execution,
    );
    let resource = compile_single_project_resource(temp.path());
    let runtime = resolve_file_resource_for_test(&resource, dependencies.clone());
    let plan = live_plan_for_stream(runtime.as_ref(), resource.source_plan(), "pkg-http-gzip");
    let mut opened = futures_executor::block_on(runtime.open(plan.scan.partitions[0].clone()))
        .expect("open recorded HTTP gzip partition");
    let first = futures_executor::block_on(futures_util::StreamExt::next(&mut opened))
        .expect("first bounded batch")
        .expect("decode first bounded batch");
    assert_eq!(
        first.header.row_count,
        u64::try_from(cdf_runtime::DEFAULT_FORMAT_BATCH_ROWS).unwrap()
    );

    let wait_for_stable_partial_progress = |minimum_bytes: u64| {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut stable_observations = 0;
        let mut previous = transport.sequential_progress();
        loop {
            thread::sleep(Duration::from_millis(20));
            let current = transport.sequential_progress();
            if current == previous
                && current.bytes_emitted > minimum_bytes
                && current.bytes_emitted < encoded_bytes
            {
                stable_observations += 1;
                if stable_observations == 5 {
                    break current;
                }
            } else {
                stable_observations = 0;
            }
            previous = current;
            assert!(
                std::time::Instant::now() < deadline,
                "recorded HTTP source did not reach a stable partial-transfer plateau: {current:?}"
            );
        }
    };
    let stalled = wait_for_stable_partial_progress(encoded_bytes / 4);
    assert_eq!(stalled.streams_closed, 0);
    assert_eq!(stalled.streams_completed, 0);

    // Drain only the already-bounded output frontier until demand propagates through every nested
    // stage and resumes the transport. A stable plateau followed by progress caused only by
    // downstream polls distinguishes backpressure from an incidental decode pause.
    let mut resumed_batches = Vec::new();
    let mut resumed = None;
    for _ in 0..8 {
        resumed_batches.push(
            futures_executor::block_on(futures_util::StreamExt::next(&mut opened))
                .expect("bounded batch after transport plateau")
                .expect("decode bounded batch after transport plateau"),
        );
        let poll_deadline = std::time::Instant::now() + Duration::from_millis(250);
        loop {
            let progress = transport.sequential_progress();
            if progress.bytes_emitted > stalled.bytes_emitted {
                resumed = Some(progress);
                break;
            }
            if std::time::Instant::now() >= poll_deadline {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        if resumed.is_some() {
            break;
        }
    }
    let resumed = resumed.expect(
        "bounded downstream demand did not propagate through the source frontier to the transport",
    );
    assert!(resumed.bytes_emitted < encoded_bytes);
    let stalled_again = wait_for_stable_partial_progress(stalled.bytes_emitted);
    assert!(stalled_again.bytes_emitted >= resumed.bytes_emitted);
    assert_eq!(stalled_again.streams_closed, 0);
    assert_eq!(stalled_again.streams_completed, 0);

    let (termination_tx, termination_rx) = mpsc::sync_channel(1);
    let termination = thread::spawn(move || {
        let result = futures_executor::block_on(opened.terminate_and_join());
        termination_tx.send(result).unwrap();
    });
    termination_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocked source invocation must cancel and join within one second")
        .expect("blocked source invocation must cancel and join cleanly");
    termination.join().unwrap();
    drop((first, resumed_batches));
    let cleanup_deadline = std::time::Instant::now() + Duration::from_secs(1);
    let stopped = loop {
        let progress = transport.sequential_progress();
        assert_eq!(progress.bytes_emitted, stalled_again.bytes_emitted);
        if progress.streams_closed == 1 && transport.current_memory_bytes() == 0 {
            break progress;
        }
        assert!(
            std::time::Instant::now() < cleanup_deadline,
            "cancelled stream did not drop its transport state: {progress:?}; memory={} bytes",
            transport.current_memory_bytes()
        );
        thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(stopped.streams_completed, 0);
}

#[test]
fn recorded_http_multifile_packages_are_jobs_invariant() {
    let temp = tempfile::tempdir().unwrap();
    write_http_discover_project(temp.path(), "");
    fs::write(
        temp.path().join("resources/files.toml"),
        r#"
[source.remote]
kind = "files"
root = "https://data.example.test/events"

[resource.events]
glob = "part-{01..04}.ndjson.gz"
format = "ndjson"
compression = "gzip"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "payload", type = "utf8", nullable = false },
] }
"#,
    )
    .unwrap();
    let rows_per_file = 8_192;
    let mut source = Vec::with_capacity(rows_per_file * 80);
    for id in 0..rows_per_file {
        source.extend_from_slice(
            format!(
                r#"{{"id":{id},"payload":"payload-{id:08x}-0123456789abcdef0123456789abcdef"}}"#
            )
            .as_bytes(),
        );
        source.push(b'\n');
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::none());
    std::io::Write::write_all(&mut encoder, &source).unwrap();
    let encoded = encoder.finish().unwrap();
    let resource = compile_single_project_resource(temp.path());

    let run = |jobs: u16| {
        let execution = test_execution_services_with_slots(4, 512 * 1024 * 1024)
            .with_run_job_ceiling(jobs)
            .unwrap();
        let transport = RecordingHttpFileTransport::new(encoded.clone());
        let dependencies = file_dependencies_with_execution(
            FileTransportFacade::new().with_http_transport(transport.clone()),
            execution.clone(),
        );
        let runtime = resolve_file_resource_for_test(&resource, dependencies);
        let plan = live_plan_for_stream(runtime.as_ref(), resource.source_plan(), "pkg-http-jobs")
            .bind_operator_graph(
                resource.source_plan(),
                &cdf_runtime::DestinationRuntimeCapabilities::default(),
            )
            .unwrap();
        assert_eq!(plan.scan.partitions.len(), 4);
        let source_execution = plan.compiled_source_execution.as_ref().unwrap();
        let scheduler = cdf_runtime::resolve_runtime_scheduler(
            plan.scan.partitions.len(),
            source_execution.execution_capabilities(),
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
            &execution,
            Some(jobs),
        )
        .unwrap();
        assert_eq!(scheduler.effective_jobs.jobs, jobs);
        let run_root = temp.path().join(format!("jobs-{jobs}"));
        let pre_finalize =
            |_: &cdf_package::PackageBuilder, _: cdf_engine::EnginePackageDraft<'_>| Ok(());
        let output = futures_executor::block_on(
            cdf_engine::execute_to_package_with_segment_positions_and_pre_finalize(
                &plan,
                runtime.as_ref(),
                run_root.join("package"),
                &pre_finalize,
                cdf_engine::EngineExecutionOptions::default()
                    .with_execution_services(execution)
                    .with_scheduler_resolution(scheduler),
            ),
        )
        .unwrap();
        let progress = transport.sequential_progress();
        assert_eq!(progress.streams_completed, 4);
        assert_eq!(progress.streams_closed, 4);
        assert!(progress.peak_active_streams <= jobs);
        assert_eq!(transport.current_memory_bytes(), 0);
        (output, progress)
    };

    let (serial, serial_progress) = run(1);
    let (parallel, parallel_progress) = run(4);
    assert_eq!(serial_progress.peak_active_streams, 1);
    assert!(parallel_progress.peak_active_streams >= 2);
    assert_eq!(
        parallel.output.profile.output_rows,
        u64::try_from(4 * rows_per_file).unwrap()
    );
    assert_eq!(
        parallel.output.manifest.package_hash,
        serial.output.manifest.package_hash
    );
    assert_eq!(parallel.output.segments, serial.output.segments);
    assert_eq!(parallel.output.profile, serial.output.profile);
    assert_eq!(parallel.output.lineage, serial.output.lineage);
    assert_eq!(parallel.segment_positions, serial.segment_positions);
    assert_eq!(
        parallel.output.terminal_schema_quarantines,
        serial.output.terminal_schema_quarantines
    );
}

#[test]
fn http_numeric_template_discovers_and_plans_every_file() {
    let temp = tempfile::tempdir().unwrap();
    write_http_discover_project(temp.path(), "");
    let resource_path = temp.path().join("resources/files.toml");
    let resource_toml = fs::read_to_string(&resource_path)
        .unwrap()
        .replace("vendors.parquet", "yellow_tripdata_2024-{01..03}.parquet");
    fs::write(resource_path, resource_toml).unwrap();
    let parquet = vendor_parquet_bytes();
    let transport = RecordingHttpFileTransport::new(parquet);
    let dependencies = http_file_dependencies(transport.clone());
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies.clone(),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();
    assert_eq!(artifacts.discovery_manifest.unwrap().candidates.len(), 3);

    let runtime = resolve_file_resource_for_test(&resource, dependencies);
    let partitions = runtime
        .plan_partitions(&ScanRequest {
            resource_id: resource.descriptor().resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: resource.descriptor().state_scope.clone(),
        })
        .unwrap();
    assert_eq!(partitions.len(), 3);
    assert_eq!(
        partitions
            .iter()
            .map(|partition| { partition.planned_file().unwrap().unwrap().path.as_str() })
            .collect::<Vec<_>>(),
        vec![
            "https://data.example.test/trip-data/yellow_tripdata_2024-01.parquet",
            "https://data.example.test/trip-data/yellow_tripdata_2024-02.parquet",
            "https://data.example.test/trip-data/yellow_tripdata_2024-03.parquet",
        ]
    );
    assert_eq!(
        transport
            .requests()
            .iter()
            .filter(|request| request.method == HttpMethod::Head)
            .count(),
        9
    );
}

#[test]
fn http_year_month_glob_skips_absent_candidates_without_hiding_other_failures() {
    let temp = tempfile::tempdir().unwrap();
    write_http_discover_project(temp.path(), "");
    let resource_path = temp.path().join("resources/files.toml");
    let resource_toml = fs::read_to_string(&resource_path)
        .unwrap()
        .replace("vendors.parquet", "yellow_tripdata_2024-*.parquet");
    fs::write(resource_path, resource_toml).unwrap();
    let missing = (3..=12).map(|month| {
        format!("https://data.example.test/trip-data/yellow_tripdata_2024-{month:02}.parquet")
    });
    let transport = RecordingHttpFileTransport::with_missing(vendor_parquet_bytes(), missing);
    let dependencies = http_file_dependencies(transport.clone());
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies.clone(),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();
    assert_eq!(artifacts.discovery_manifest.unwrap().candidates.len(), 2);
    let runtime = resolve_file_resource_for_test(&resource, dependencies);
    let partitions = runtime
        .plan_partitions(&ScanRequest {
            resource_id: resource.descriptor().resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: resource.descriptor().state_scope.clone(),
        })
        .unwrap();
    assert_eq!(partitions.len(), 2);
    assert!(
        partitions[0]
            .planned_file()
            .unwrap()
            .unwrap()
            .path
            .ends_with("2024-01.parquet")
    );
    assert!(
        partitions[1]
            .planned_file()
            .unwrap()
            .unwrap()
            .path
            .ends_with("2024-02.parquet")
    );
}

#[test]
fn http_parquet_auto_pin_plan_preview_and_run_use_file_runtime() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = vendor_parquet_bytes();
    write_http_discover_project(temp.path(), "");
    let resource = compile_single_project_resource(temp.path());
    let reference_transport = RecordingHttpFileTransport::new(parquet.clone());
    let reference_dependencies = http_file_dependencies(reference_transport.clone());
    discover_file_schema_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        reference_dependencies,
    )
    .unwrap();
    let expected_discovery_requests = reference_transport.requests();
    let transport = RecordingHttpFileTransport::new(parquet.clone());
    let dependencies = http_file_dependencies(transport.clone());
    let secret_provider = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());

    let prepared = prepare_file_discover_resource_with_dependencies_for_test(
        temp.path(),
        &resource,
        &secret_provider,
        dependencies.clone(),
    )
    .unwrap();
    let discovery = prepared.discovery.as_ref().unwrap();
    assert_eq!(
        transport.requests(),
        expected_discovery_requests,
        "auto-pin must perform exactly one cold discovery lifecycle"
    );
    assert!(
        temp.path()
            .join(&discovery.snapshot.artifact.path)
            .is_file()
    );
    let SchemaSource::Discovered { snapshot } = &prepared.resource.descriptor().schema_source
    else {
        panic!("expected HTTP Parquet auto-pin to set discovered schema source");
    };
    assert_eq!(
        snapshot.schema_hash,
        discovery.snapshot.artifact.schema_hash
    );

    let file_resource = resolve_file_resource_for_test(&prepared.resource, dependencies.clone());
    let plan = live_plan_for_stream(
        file_resource.as_ref(),
        prepared.resource.source_plan(),
        "pkg-http-parquet-runtime",
    );
    assert_eq!(
        transport.requests(),
        expected_discovery_requests,
        "planning must consume the cold-discovery inventory without a second transport inventory"
    );
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 0);
    assert_eq!(plan.scan.partitions.len(), 1);
    let partition = plan.scan.partitions[0].clone();
    let planned_file = partition.planned_file().unwrap().unwrap();
    assert_eq!(
        planned_file.path,
        "https://data.example.test/trip-data/vendors.parquet"
    );
    assert_eq!(planned_file.size_bytes, parquet.len() as u64);
    assert_eq!(planned_file.etag.as_deref(), Some("\"fixture-etag\""));
    for legacy_key in [
        "path",
        "bytes",
        "etag",
        "version",
        "sha256",
        "source_generation",
    ] {
        assert!(!partition.metadata.contains_key(legacy_key));
    }
    assert!(!partition.metadata.contains_key("bytes_loaded"));

    let preview_stream =
        futures_executor::block_on(file_resource.as_ref().open(partition)).unwrap();
    let preview_rows = futures_executor::block_on_stream(preview_stream)
        .map(|batch| batch.unwrap().header.row_count)
        .sum::<u64>();
    assert_eq!(preview_rows, 2);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let report = futures_executor::block_on(run_project(
        ProjectRunRequest {
            resource: ProjectRunSource::new(file_resource.as_ref()),
            plan,
            package_root: temp.path().join(".cdf/packages"),
            state_store_path: temp.path().join(".cdf/state.db"),
            pipeline_id: PipelineId::new("pipeline-http").unwrap(),
            package_id: "pkg-http-parquet-runtime".to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-http-parquet-runtime").unwrap(),
            destination: ResolvedProjectDestination::duckdb(
                duckdb_path,
                TargetName::new("events").unwrap(),
            )
            .unwrap(),
            run_id: Some(RunId::new("run-http-parquet-runtime").unwrap()),
            event_sink: None,
            after_receipt_verified: None,
        },
        &test_execution_services(),
    ))
    .unwrap()
    .into_committed()
    .unwrap();

    assert_eq!(report.row_count, 2);
    assert_eq!(report.segment_count, 1);
    let SourcePosition::FileManifest(manifest) = &report.checkpoint.delta.output_position else {
        panic!("checkpoint output position should be a file manifest");
    };
    assert_eq!(manifest.files.len(), 1);
    assert_eq!(
        manifest.files[0].path,
        "https://data.example.test/trip-data/vendors.parquet"
    );
    assert_eq!(manifest.files[0].size_bytes, parquet.len() as u64);
    assert_eq!(manifest.files[0].etag.as_deref(), Some("\"fixture-etag\""));
    assert_eq!(manifest.files[0].sha256, None);
    let requests = transport.requests();
    let sequential_gets = requests
        .iter()
        .filter(|request| {
            request.method == HttpMethod::Get && !request.headers.contains_key("range")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sequential_gets.len(),
        2,
        "preview and run must each use one sequential HTTP GET: {requests:?}"
    );
    assert!(sequential_gets.iter().all(|request| {
        request.headers.get("if-match").map(String::as_str) == Some("\"fixture-etag\"")
    }));
    let ranged_gets = requests
        .iter()
        .filter(|request| {
            request.method == HttpMethod::Get && request.headers.contains_key("range")
        })
        .collect::<Vec<_>>();
    assert!(
        !ranged_gets.is_empty(),
        "discovery must retain bounded Parquet range reads"
    );

    let request_count_before_pinned_prepare = requests.len();
    let compiled_again = compile_single_project_resource(temp.path());
    let pinned_compiled = compiled_again.with_schema_source_and_schema(
        prepared.resource.descriptor().schema_source.clone(),
        prepared.resource.schema(),
    );
    assert!(pinned_compiled.effective_schema_runtime().is_none());
    let pinned = prepare_pinned_resource_schema_artifacts(temp.path(), &pinned_compiled).unwrap();
    assert_eq!(
        transport.requests().len(),
        request_count_before_pinned_prepare,
        "pinned preparation must not contact the source"
    );
    let (pinned_resource, _) = pinned.into_parts();
    let pinned_file_resource =
        resolve_file_resource_for_test(&pinned_resource, dependencies.clone());
    let pinned_plan = live_plan_for_stream(
        pinned_file_resource.as_ref(),
        pinned_resource.source_plan(),
        "pkg-http-parquet-pinned-runtime",
    );
    let pinned_report = futures_executor::block_on(run_project(
        ProjectRunRequest {
            resource: ProjectRunSource::new(pinned_file_resource.as_ref()),
            plan: pinned_plan,
            package_root: temp.path().join(".cdf/packages"),
            state_store_path: temp.path().join(".cdf/state-pinned.db"),
            pipeline_id: PipelineId::new("pipeline-http-pinned").unwrap(),
            package_id: "pkg-http-parquet-pinned-runtime".to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-http-parquet-pinned-runtime").unwrap(),
            destination: ResolvedProjectDestination::duckdb(
                temp.path().join(".cdf/dev-pinned.duckdb"),
                TargetName::new("events_pinned").unwrap(),
            )
            .unwrap(),
            run_id: Some(RunId::new("run-http-parquet-pinned-runtime").unwrap()),
            event_sink: None,
            after_receipt_verified: None,
        },
        &test_execution_services(),
    ))
    .unwrap()
    .into_committed()
    .unwrap();
    assert_eq!(pinned_report.row_count, 2);
    let pinned_package = cdf_package::PackageReader::open(&pinned_report.package_dir).unwrap();
    assert!(
        pinned_package
            .manifest()
            .identity
            .files
            .iter()
            .any(|entry| entry.path == "plan/schema-admission.json")
    );
    assert!(
        pinned_package
            .manifest()
            .identity
            .files
            .iter()
            .any(|entry| entry.path == "schema/stream-admission-evidence.json")
    );
    let pinned_execution_requests = transport
        .requests()
        .into_iter()
        .skip(request_count_before_pinned_prepare)
        .collect::<Vec<_>>();
    assert_eq!(
        pinned_execution_requests
            .iter()
            .filter(|request| request.method == HttpMethod::Get
                && !request.headers.contains_key("range"))
            .count(),
        1,
        "pinned execution must transfer the extraction payload once: {pinned_execution_requests:?}"
    );
    let pinned_ranges = pinned_execution_requests
        .iter()
        .filter(|request| {
            request.method == HttpMethod::Get && request.headers.contains_key("range")
        })
        .collect::<Vec<_>>();
    let expected_tail = format!("-{}", parquet.len().saturating_sub(1));
    assert!(
        pinned_ranges.iter().all(|request| {
            request.headers.get("if-match").map(String::as_str) == Some("\"fixture-etag\"")
                && request.headers["range"].ends_with(&expected_tail)
        }),
        "pinned extraction may overlap its sequential spool only with generation-bound Parquet tail reads: {pinned_execution_requests:?}"
    );
}

#[test]
fn unversioned_http_parquet_runs_and_commits_terminal_content_identity() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = vendor_parquet_bytes();
    write_http_discover_project(temp.path(), "");
    let resource = compile_single_project_resource(temp.path());
    let transport = RecordingHttpFileTransport::new(parquet.clone());
    transport.clear_etag();
    let dependencies = http_file_dependencies(transport.clone());
    let prepared = prepare_file_discover_resource_with_dependencies_for_test(
        temp.path(),
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies.clone(),
    )
    .unwrap();
    let file_resource = resolve_file_resource_for_test(&prepared.resource, dependencies.clone());
    let plan = live_plan_for_stream(
        file_resource.as_ref(),
        prepared.resource.source_plan(),
        "pkg-http-unversioned",
    );
    let partition = &plan.scan.partitions[0];
    assert_eq!(partition.metadata["identity_strength"], "weak");
    let planned_file = partition.planned_file().unwrap().unwrap();
    assert_eq!(planned_file.etag, None);
    assert_eq!(planned_file.source_generation, None);
    assert_eq!(planned_file.sha256, None);
    let request_count_before_run = transport.requests().len();
    let discovery_payload_gets = transport
        .requests()
        .into_iter()
        .filter(|request| {
            request.method == HttpMethod::Get && !request.headers.contains_key("range")
        })
        .count();
    assert_eq!(
        discovery_payload_gets, 1,
        "cold discovery must materialize the unversioned payload exactly once"
    );
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 1);
    let report = futures_executor::block_on(run_project(
        ProjectRunRequest {
            resource: ProjectRunSource::new(file_resource.as_ref()),
            plan,
            package_root: temp.path().join(".cdf/packages"),
            state_store_path: temp.path().join(".cdf/state.db"),
            pipeline_id: PipelineId::new("pipeline-http-unversioned").unwrap(),
            package_id: "pkg-http-unversioned".to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-http-unversioned").unwrap(),
            destination: ResolvedProjectDestination::duckdb(
                temp.path().join(".cdf/dev.duckdb"),
                TargetName::new("events").unwrap(),
            )
            .unwrap(),
            run_id: Some(RunId::new("run-http-unversioned").unwrap()),
            event_sink: None,
            after_receipt_verified: None,
        },
        &test_execution_services(),
    ))
    .unwrap()
    .into_committed()
    .unwrap();

    assert_eq!(report.row_count, 2);
    let expected_sha256 = format!("sha256:{}", hex::encode(Sha256::digest(&parquet)));
    let SourcePosition::FileManifest(manifest) = &report.checkpoint.delta.output_position else {
        panic!("unversioned HTTP checkpoint must commit a file manifest");
    };
    assert_eq!(manifest.files.len(), 1);
    assert_eq!(
        manifest.files[0].sha256.as_deref(),
        Some(expected_sha256.as_str())
    );
    assert_eq!(manifest.files[0].etag, None);
    assert_eq!(manifest.files[0].source_generation, None);
    assert_eq!(report.checkpoint.delta.segments.len(), 1);
    let SourcePosition::FileManifest(segment_manifest) =
        &report.checkpoint.delta.segments[0].output_position
    else {
        panic!("unversioned HTTP segment must retain a file manifest");
    };
    assert_eq!(segment_manifest.files.len(), 1);
    assert_eq!(
        segment_manifest.files[0].sha256.as_deref(),
        Some(expected_sha256.as_str())
    );
    let sequential_gets = transport
        .requests()
        .into_iter()
        .skip(request_count_before_run)
        .filter(|request| {
            request.method == HttpMethod::Get && !request.headers.contains_key("range")
        })
        .count();
    assert_eq!(
        sequential_gets, 0,
        "same-command execution must consume the retained discovery spool without another transfer"
    );
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 0);
    assert_eq!(
        transport
            .requests()
            .iter()
            .filter(|request| request.method == HttpMethod::Get
                && !request.headers.contains_key("range"))
            .count(),
        1,
        "cold discovery plus execution must transfer one unversioned payload generation once"
    );
}

#[test]
fn http_file_discovery_egress_and_auth_fail_before_transport_use() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = vendor_parquet_bytes();
    write_http_discover_project(temp.path(), r#"egress_allowlist = ["other.example.test"]"#);
    let resource = compile_single_project_resource(temp.path());
    let transport = RecordingHttpFileTransport::new(parquet.clone());
    let dependencies = http_file_dependencies(transport.clone());

    let error = discover_file_schema_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies,
    )
    .unwrap_err();

    assert!(error.to_string().contains("egress"));
    assert!(transport.requests().is_empty());

    let auth_temp = tempfile::tempdir().unwrap();
    write_http_discover_project(
        auth_temp.path(),
        r#"
auth = { kind = "bearer", token = "secret://env/HTTP_TOKEN" }
egress_allowlist = ["data.example.test"]
"#,
    );
    let auth_resource = compile_single_project_resource(auth_temp.path());
    let auth_transport = RecordingHttpFileTransport::new(parquet);
    let auth_dependencies = file_dependencies(
        FileTransportFacade::new()
            .with_http_transport(auth_transport.clone())
            .with_secret_provider(StaticSecretProvider::new([(
                "secret://env/HTTP_TOKEN",
                "super-secret-http-token",
            )])),
    );

    let auth_error = discover_file_schema_for_test(
        &auth_resource,
        &StaticSecretProvider::new([("secret://env/HTTP_TOKEN", "super-secret-http-token")]),
        auth_dependencies,
    )
    .unwrap_err();
    let message = auth_error.to_string();
    assert!(message.contains("credential resolution is not implemented"));
    assert!(!message.contains("super-secret-http-token"));
    assert!(auth_transport.requests().is_empty());
}

#[test]
fn local_parquet_discover_autopin_leaves_declared_resources_unobserved() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.missing");
    let declared = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.missing"
format = "parquet"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "VendorID", type = "int32", nullable = false },
] }
"#;
    fs::write(temp.path().join("resources/files.toml"), declared).unwrap();
    let resource = compile_single_project_resource(temp.path());

    let prepared = prepare_file_discover_resource(
        temp.path(),
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
    )
    .unwrap();

    assert!(prepared.discovery.is_none());
    assert!(matches!(
        prepared.resource.descriptor().schema_source,
        SchemaSource::Declared { .. }
    ));
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn local_ndjson_discovery_is_bounded_and_writes_nothing_until_pin() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "ndjson", "*.ndjson");
    fs::write(
        temp.path().join("data/events.ndjson"),
        b"{\"VendorID\":1}\n{\"VendorID\":2}\n",
    )
    .unwrap();
    let resource = compile_single_project_resource(temp.path());
    let cache = ObservationCacheStore::new(temp.path());

    let artifacts = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::new().with_observation_cache(cache.clone()),
    )
    .unwrap();

    assert_eq!(
        artifacts.discovery.normalized_schema.field(0).name(),
        "vendor_id"
    );
    assert_eq!(
        artifacts.discovery.snapshot.source_identity["file_coverage"],
        "all_files"
    );
    let manifest = artifacts.discovery_manifest.unwrap();
    assert_eq!(
        manifest.within_file_coverage,
        DiscoveryWithinFileCoverage::BoundedContent
    );
    assert_eq!(manifest.candidates.len(), 1);
    assert_eq!(manifest.candidates[0].probe_records, Some(2));
    assert!(
        manifest.candidates[0]
            .probe_bytes
            .is_some_and(|bytes| bytes > 0)
    );
    assert_eq!(
        artifacts.discovery.snapshot.source_identity["observation_cache_bypasses"],
        "1"
    );
    assert!(!cache.root().exists());
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn local_csv_discovery_uses_the_registered_driver_manifest_path() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "csv", "*.csv");
    fs::write(
        temp.path().join("data/events.csv"),
        b"VendorID,fare_amount\n1,10.5\n2,20.25\n",
    )
    .unwrap();
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();

    assert_eq!(
        artifacts
            .discovery
            .normalized_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["vendor_id", "fare_amount"]
    );
    assert_eq!(
        artifacts.discovery.snapshot.artifact.metadata["probe"],
        "registered-source-discovery"
    );
    let manifest = artifacts.discovery_manifest.unwrap();
    assert_eq!(
        manifest.within_file_coverage,
        DiscoveryWithinFileCoverage::BoundedContent
    );
    assert_eq!(manifest.candidates.len(), 1);
    assert_eq!(manifest.candidates[0].probe_records, Some(2));
}

#[test]
fn local_json_document_discovery_uses_the_registered_driver_manifest_path() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "json", "*.json");
    fs::write(
        temp.path().join("data/events.json"),
        br#"[{"VendorID":1,"active":true},{"VendorID":2,"active":false}]"#,
    )
    .unwrap();
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();

    assert_eq!(
        artifacts
            .discovery
            .normalized_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["vendor_id", "active"]
    );
    assert_eq!(
        artifacts.discovery.snapshot.artifact.metadata["probe"],
        "registered-source-discovery"
    );
    let manifest = artifacts.discovery_manifest.unwrap();
    assert_eq!(
        manifest.within_file_coverage,
        DiscoveryWithinFileCoverage::BoundedContent
    );
    assert_eq!(manifest.candidates.len(), 1);
    assert_eq!(manifest.candidates[0].probe_records, Some(2));
}

#[test]
fn local_parquet_discover_autopin_persists_all_file_metadata_manifest() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/a.parquet"));
    write_vendor_parquet(&temp.path().join("data/b.parquet"));
    let resource = compile_single_project_resource(temp.path());

    let prepared = prepare_file_discover_resource(
        temp.path(),
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
    )
    .unwrap();
    let discovery = prepared.discovery.unwrap();
    assert_eq!(
        discovery.snapshot.source_identity["file_coverage"],
        "all_files"
    );
    assert_eq!(
        discovery.snapshot.source_identity["within_file_coverage"],
        "format_metadata"
    );
    assert_eq!(discovery.snapshot.source_identity["matched_files"], "2");
    assert_eq!(discovery.snapshot.source_identity["selected_files"], "2");
    let reference = discovery
        .snapshot
        .reference
        .discovery_manifest()
        .unwrap()
        .unwrap();
    let manifest = DiscoveryManifestStore::new(temp.path())
        .read(&reference)
        .unwrap();
    assert_eq!(manifest.file_coverage, DiscoveryFileCoverage::AllFiles);
    assert_eq!(
        manifest.within_file_coverage,
        DiscoveryWithinFileCoverage::FormatMetadata
    );
    assert!(manifest.selector.is_none());
    assert_eq!(manifest.budget.max_concurrent_probes(), 8);
    assert_eq!(manifest.budget.max_bytes_per_file(), 64 * 1024 * 1024);
    assert_eq!(manifest.budget.max_records_per_file(), 1_000);
    assert_eq!(
        manifest.budget.max_total_in_flight_bytes(),
        128 * 1024 * 1024
    );
    assert_eq!(manifest.candidates.len(), 2);
    assert!(manifest.candidates.iter().all(|candidate| {
        candidate.participation == DiscoveryParticipation::Observed
            && candidate.physical_schema_hash.is_some()
            && candidate.probe_bytes.is_some()
            && candidate.probe_records == Some(0)
            && candidate.schema_verdict.is_some()
    }));
    assert_eq!(
        manifest
            .candidates
            .iter()
            .map(|candidate| candidate.canonical_location.as_str())
            .collect::<Vec<_>>(),
        vec!["a.parquet", "b.parquet"]
    );
    assert!(temp.path().join(discovery.snapshot.artifact.path).is_file());
}

#[test]
fn explicit_sampled_parquet_pin_records_exact_participation_and_is_deterministic() {
    let temp = tempfile::tempdir().unwrap();
    write_sampled_discover_project(temp.path(), "parquet", "*.parquet", 3);
    for index in 0..9 {
        write_vendor_parquet(&temp.path().join(format!("data/{index:02}.parquet")));
    }
    let resource = compile_single_project_resource(temp.path());
    let first = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();
    let manifest = first.discovery_manifest.as_ref().unwrap();
    assert_eq!(manifest.file_coverage, DiscoveryFileCoverage::SampledFiles);
    let selector = manifest.selector.as_ref().unwrap();
    assert_eq!(selector.selector, STRATIFIED_HASH_SELECTOR_V1);
    assert_eq!(selector.sample_files, 3);
    assert_eq!(selector.matched_count, 9);
    assert_eq!(selector.selected.len(), 3);
    assert_eq!(manifest.candidates.len(), 9);
    assert_eq!(
        manifest
            .candidates
            .iter()
            .filter(|candidate| candidate.participation == DiscoveryParticipation::Observed)
            .count(),
        3
    );
    assert_eq!(
        manifest
            .candidates
            .iter()
            .filter(|candidate| candidate.participation == DiscoveryParticipation::Unobserved)
            .count(),
        6
    );
    assert!(manifest.candidates.iter().all(|candidate| {
        candidate.participation == DiscoveryParticipation::Observed
            || (candidate.physical_schema_hash.is_none()
                && candidate.probe_bytes.is_none()
                && candidate.schema_verdict.is_none())
    }));
    assert_eq!(
        first.discovery.snapshot.source_identity["file_coverage"],
        "sampled_files"
    );
    assert_eq!(
        first.discovery.snapshot.source_identity["matched_files"],
        "9"
    );
    assert_eq!(
        first.discovery.snapshot.source_identity["selected_files"],
        "3"
    );
    assert_eq!(
        first.discovery.snapshot.source_identity["unobserved_files"],
        "6"
    );

    let repeated = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_vec(manifest).unwrap(),
        serde_json::to_vec(repeated.discovery_manifest.as_ref().unwrap()).unwrap()
    );
}

#[test]
fn explicit_sample_larger_than_set_preserves_all_files_manifest_bytes() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/a.parquet"));
    write_vendor_parquet(&temp.path().join("data/b.parquet"));
    let all_files_resource = compile_single_project_resource(temp.path());
    let all_files = discover_default_file_schema_artifacts_for_test(
        &all_files_resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();

    write_sampled_discover_project(temp.path(), "parquet", "*.parquet", 2);
    let configured_resource = compile_single_project_resource(temp.path());
    let configured = discover_default_file_schema_artifacts_for_test(
        &configured_resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        configured
            .discovery_manifest
            .as_ref()
            .unwrap()
            .file_coverage,
        DiscoveryFileCoverage::AllFiles
    );
    assert_eq!(
        serde_json::to_vec(all_files.discovery_manifest.as_ref().unwrap()).unwrap(),
        serde_json::to_vec(configured.discovery_manifest.as_ref().unwrap()).unwrap()
    );
    assert_eq!(
        all_files.discovery.snapshot.artifact,
        configured.discovery.snapshot.artifact
    );
}

#[test]
fn explicit_sampled_arrow_ipc_uses_the_same_format_neutral_selector() {
    let temp = tempfile::tempdir().unwrap();
    write_sampled_discover_project(temp.path(), "arrow_ipc", "*.arrow", 2);
    for index in 0..5 {
        write_arrow_ipc_fixture(
            &temp.path().join(format!("data/{index:02}.arrow")),
            vec![Field::new("VendorID", DataType::Int32, false)],
            vec![Arc::new(Int32Array::from(vec![index]))],
        );
    }
    let resource = compile_single_project_resource(temp.path());
    let artifacts = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();
    let manifest = artifacts.discovery_manifest.unwrap();
    assert_eq!(manifest.file_coverage, DiscoveryFileCoverage::SampledFiles);
    assert_eq!(manifest.selector.unwrap().sample_files, 2);
    assert_eq!(
        manifest
            .candidates
            .iter()
            .filter(|candidate| candidate.participation == DiscoveryParticipation::Observed)
            .count(),
        2
    );
}

#[test]
fn pinned_schema_preparation_reuses_snapshot_without_observing_runtime_files() {
    let temp = tempfile::tempdir().unwrap();
    write_sampled_discover_project(temp.path(), "parquet", "*.parquet", 2);
    write_parquet_fixture(
        &temp.path().join("data/a.parquet"),
        vec![Field::new("value", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![1]))],
    );
    write_parquet_fixture(
        &temp.path().join("data/middle.parquet"),
        vec![Field::new("value", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["drift"]))],
    );
    write_parquet_fixture(
        &temp.path().join("data/z.parquet"),
        vec![Field::new("value", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![2]))],
    );
    let resource = compile_single_project_resource(temp.path());
    let initial = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();
    let initial_manifest = initial.discovery_manifest.as_ref().unwrap();
    assert_eq!(
        initial_manifest.file_coverage,
        DiscoveryFileCoverage::SampledFiles
    );
    assert_eq!(
        initial_manifest.candidates[1].participation,
        DiscoveryParticipation::Unobserved
    );
    write_schema_discovery_artifacts(temp.path(), &initial).unwrap();
    let pinned = resource.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: initial.discovery.snapshot.reference.clone(),
        },
        Arc::clone(&initial.discovery.normalized_schema),
    );

    fs::remove_dir_all(temp.path().join("data")).unwrap();
    let prepared = prepare_pinned_resource_schema_artifacts(temp.path(), &pinned).unwrap();
    assert_eq!(
        prepared.discovery_manifest().unwrap().reference(),
        initial_manifest.reference()
    );
    assert_eq!(prepared.resource().schema(), pinned.schema());
    assert!(prepared.resource().effective_schema_runtime().is_none());
    let expected_baseline_hashes = initial_manifest
        .candidates
        .iter()
        .filter_map(|candidate| candidate.physical_schema_hash.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        prepared
            .resource()
            .baseline_observation_schema_catalog()
            .iter()
            .map(|entry| entry.physical_schema_hash.clone())
            .collect::<BTreeSet<_>>(),
        expected_baseline_hashes
    );
}

#[test]
fn sampled_probe_budget_failure_does_not_substitute_an_unselected_candidate() {
    let temp = tempfile::tempdir().unwrap();
    write_sampled_discover_project(temp.path(), "parquet", "*.parquet", 1);
    for index in 0..5 {
        write_vendor_parquet(&temp.path().join(format!("data/{index:02}.parquet")));
    }
    let resource = compile_single_project_resource(temp.path());
    let error = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::new()
            .with_budget(DiscoveryExecutorBudget::new(8, 1_000, 8, 1).unwrap()),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("sampled_files + format_metadata files discovery failed"));
    assert!(error.contains("without substitution"));
    assert_eq!(error.matches(": failed:").count(), 1);
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn sampled_initial_pin_reports_every_selected_incompatibility_without_writes() {
    let temp = tempfile::tempdir().unwrap();
    write_sampled_discover_project(temp.path(), "parquet", "*.parquet", 2);
    write_parquet_fixture(
        &temp.path().join("data/a.parquet"),
        vec![Field::new("value", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![1]))],
    );
    write_vendor_parquet(&temp.path().join("data/middle.parquet"));
    write_parquet_fixture(
        &temp.path().join("data/z.parquet"),
        vec![Field::new("value", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["incompatible"]))],
    );
    let resource = compile_single_project_resource(temp.path());
    let error = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("initial sampled schema pin"));
    assert!(error.contains("a.parquet"));
    assert!(error.contains("z.parquet"));
    assert!(error.contains("candidate verdicts"));
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn all_files_discovery_uses_exact_verified_baseline_and_schema_only_effective_hash() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/a.parquet"));
    let resource = compile_single_project_resource(temp.path());
    let authority_temp = tempfile::tempdir().unwrap();
    let baseline_artifact = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        resource.schema().as_ref(),
        BTreeMap::new(),
    )
    .unwrap();
    let baseline_store = SchemaSnapshotStore::new(authority_temp.path());
    baseline_store.write(&baseline_artifact).unwrap();
    let (_, verified_baseline) = baseline_store
        .read_with_verified_baseline(&baseline_artifact.reference())
        .unwrap();
    assert_eq!(
        verified_baseline.resource_id(),
        &resource.descriptor().resource_id
    );
    let verified_baseline_hash = verified_baseline.schema_hash().clone();

    let artifacts = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::new().with_verified_baseline(verified_baseline),
    )
    .unwrap();
    let manifest = artifacts.discovery_manifest.as_ref().unwrap();
    assert_eq!(manifest.baseline_schema_hash, Some(verified_baseline_hash));

    let mut schema_only_metadata = artifacts.discovery.snapshot.artifact.metadata.clone();
    schema_only_metadata.remove("cdf:discovery_manifest_hash");
    schema_only_metadata.remove("cdf:discovery_manifest_path");
    let schema_only = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        artifacts.discovery.normalized_schema.as_ref(),
        schema_only_metadata,
    )
    .unwrap();
    assert_eq!(
        manifest.effective_schema_hash.as_ref(),
        Some(&schema_only.schema_hash)
    );
    assert_ne!(
        manifest.effective_schema_hash.as_ref(),
        Some(&artifacts.discovery.snapshot.artifact.schema_hash)
    );

    let other_resource_id = ResourceId::new("other.resource").unwrap();
    let other_artifact = SchemaSnapshotArtifact::new(
        &other_resource_id,
        resource.schema().as_ref(),
        BTreeMap::new(),
    )
    .unwrap();
    baseline_store.write(&other_artifact).unwrap();
    let (_, wrong_baseline) = baseline_store
        .read_with_verified_baseline(&other_artifact.reference())
        .unwrap();
    let wrong_resource = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::new().with_verified_baseline(wrong_baseline),
    )
    .unwrap_err()
    .to_string();
    assert!(wrong_resource.contains("belongs to resource `other.resource`"));
    assert!(wrong_resource.contains("discovery is for `local.events`"));
}

#[test]
fn all_files_local_parquet_discovery_aggregates_widening_missing_metadata_and_set_identity() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_parquet_fixture(
        &temp.path().join("data/a.parquet"),
        vec![
            Field::new("VendorID", DataType::Int32, false)
                .with_metadata(HashMap::from([("source-tag".to_owned(), "a".to_owned())])),
        ],
        vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32]))],
    );
    write_parquet_fixture(
        &temp.path().join("data/b.parquet"),
        vec![
            Field::new("VendorID", DataType::Int64, false)
                .with_metadata(HashMap::from([("source-tag".to_owned(), "b".to_owned())])),
            Field::new("Note", DataType::Utf8, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![3_i64, 4_i64])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    );
    let resource = compile_single_project_resource(temp.path());
    let options = SchemaDiscoveryExecutionOptions::new();
    let first = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        options.clone(),
    )
    .unwrap();
    assert!(!temp.path().join(".cdf/schemas").exists());
    assert_eq!(
        first.discovery.normalized_schema.field(0).data_type(),
        &DataType::Int64
    );
    assert_eq!(first.discovery.normalized_schema.field(1).name(), "note");
    assert!(first.discovery.normalized_schema.field(1).is_nullable());
    let first_manifest = first.discovery_manifest.as_ref().unwrap();
    assert_eq!(first_manifest.candidates.len(), 2);
    assert!(
        first_manifest.candidates[0]
            .metadata_variance
            .iter()
            .any(|variance| variance.key == "source-tag")
    );
    assert!(
        first_manifest.candidates[0]
            .schema_verdict
            .as_ref()
            .unwrap()
            .details["field_verdicts"]
            .contains("widened")
    );
    assert!(
        first_manifest.candidates[0]
            .schema_verdict
            .as_ref()
            .unwrap()
            .details["field_verdicts"]
            .contains("missing_null")
    );
    let repeated = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        options.clone(),
    )
    .unwrap();
    assert_eq!(
        repeated.discovery_manifest.as_ref().unwrap().manifest_hash,
        first_manifest.manifest_hash
    );

    write_parquet_fixture(
        &temp.path().join("data/c.parquet"),
        vec![Field::new("VendorID", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![5_i64]))],
    );
    let added = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        options.clone(),
    )
    .unwrap();
    assert_ne!(
        added.discovery_manifest.as_ref().unwrap().manifest_hash,
        first_manifest.manifest_hash
    );
    fs::remove_file(temp.path().join("data/c.parquet")).unwrap();
    fs::remove_file(temp.path().join("data/a.parquet")).unwrap();
    let removed = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        options.clone(),
    )
    .unwrap();
    assert_ne!(
        removed.discovery_manifest.as_ref().unwrap().manifest_hash,
        first_manifest.manifest_hash
    );
    write_parquet_fixture(
        &temp.path().join("data/a.parquet"),
        vec![Field::new("VendorID", DataType::Int32, false)],
        vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32, 3_i32]))],
    );
    let changed = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        options,
    )
    .unwrap();
    assert_ne!(
        changed.discovery_manifest.as_ref().unwrap().manifest_hash,
        first_manifest.manifest_hash
    );
}

#[test]
fn all_files_gzip_parquet_discovery_joins_every_transformed_candidate() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet.gz");
    for (name, field, values) in [
        (
            "a",
            Field::new("VendorID", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![1_i32, 2_i32])) as ArrayRef,
        ),
        (
            "b",
            Field::new("VendorID", DataType::Int64, false),
            Arc::new(Int64Array::from(vec![3_i64, 4_i64])) as ArrayRef,
        ),
    ] {
        let raw = temp.path().join(format!("data/{name}.parquet"));
        write_parquet_fixture(&raw, vec![field], vec![values]);
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        std::io::Write::write_all(&mut encoder, &fs::read(&raw).unwrap()).unwrap();
        fs::write(
            temp.path().join(format!("data/{name}.parquet.gz")),
            encoder.finish().unwrap(),
        )
        .unwrap();
        fs::remove_file(raw).unwrap();
    }
    let resource = compile_single_project_resource(temp.path());
    let artifacts = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();

    assert_eq!(
        artifacts.discovery.normalized_schema.field(0).data_type(),
        &DataType::Int64
    );
    let manifest = artifacts.discovery_manifest.unwrap();
    assert_eq!(manifest.candidates.len(), 2);
    assert!(manifest.candidates.iter().all(|candidate| {
        candidate.participation == DiscoveryParticipation::Observed
            && candidate.canonical_location.ends_with(".parquet.gz")
    }));
}

#[test]
fn all_files_local_parquet_discovery_budget_and_incompatibility_fail_without_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/a.parquet"));
    write_parquet_fixture(
        &temp.path().join("data/b.parquet"),
        vec![Field::new("VendorID", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["one", "two"]))],
    );
    let resource = compile_single_project_resource(temp.path());
    let incompatible = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(incompatible.contains("candidate verdicts"));
    assert!(incompatible.contains("a.parquet"));
    assert!(incompatible.contains("b.parquet"));
    assert!(!temp.path().join(".cdf/schemas").exists());

    write_vendor_parquet(&temp.path().join("data/b.parquet"));
    let corrupt_path = temp.path().join("data/b.parquet");
    let mut corrupt = fs::read(&corrupt_path).unwrap();
    let footer_length = corrupt.len() - 8;
    corrupt[footer_length..footer_length + 4].fill(0xff);
    fs::write(&corrupt_path, corrupt).unwrap();
    let malformed = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(malformed.contains("a.parquet: observed"));
    assert!(malformed.contains("b.parquet: failed"));
    assert!(!temp.path().join(".cdf/schemas").exists());

    fs::remove_file(temp.path().join("data/b.parquet")).unwrap();
    let budget_error = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::new()
            .with_budget(DiscoveryExecutorBudget::new(8, 1_000, 8, 1).unwrap()),
    )
    .unwrap_err()
    .to_string();
    assert!(
        budget_error.contains("format confirmation"),
        "{budget_error}"
    );
    assert!(
        budget_error.contains("requires 482 bytes"),
        "{budget_error}"
    );
    assert!(
        budget_error.contains("configured 8-byte discovery budget"),
        "{budget_error}"
    );
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn all_files_local_binary_discovery_detects_normalizer_collision_before_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_parquet_fixture(
        &temp.path().join("data/a.parquet"),
        vec![Field::new("VendorID", DataType::Int32, false)],
        vec![Arc::new(Int32Array::from(vec![1_i32]))],
    );
    write_parquet_fixture(
        &temp.path().join("data/b.parquet"),
        vec![Field::new("vendor_id", DataType::Int32, false)],
        vec![Arc::new(Int32Array::from(vec![2_i32]))],
    );
    let resource = compile_single_project_resource(temp.path());
    let error = discover_default_file_schema_artifacts_for_test(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("collision"));
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn generic_schema_discovery_dispatch_samples_rest_without_snapshot_write() {
    let temp = tempfile::tempdir().unwrap();
    let project = r#"
[project]
name = "api"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."api.*"]
source = "resources/api.toml"
"#;
    let rest = r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"
auth = { kind = "bearer", token = "secret://env/API_TOKEN" }
egress_allowlist = ["api.example.test"]

[resource.items]
path = "/items"
records = "$.items"
cursor = { field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }
write_disposition = "append"
trust = "governed"
"#;
    let config = parse_cdf_toml(project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new().with_toml("resources/api.toml", rest);
    let mut resources =
        compile_project_declarative_resources(&test_source_registry(), &config, &resolver).unwrap();
    let resource = resources.remove(0);
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "VendorID": 1, "updated_at": 10, "active": true, "score": 4.5 },
            { "VendorID": 2, "updated_at": 20, "active": false, "score": null },
            { "VendorID": 3, "updated_at": 30, "active": true }
        ] }"#,
    )]);
    let discovery = discover_rest_schema_artifacts_for_test(
        temp.path(),
        &resource,
        transport.clone(),
        Arc::new(StaticSecretProvider::new([(
            "secret://env/API_TOKEN",
            "rest-discover-secret",
        )])),
        cdf_runtime::PreparedSourcePayloads::default(),
    )
    .unwrap()
    .discovery;

    assert!(!temp.path().join(".cdf/schemas").exists());
    assert_eq!(
        discovery.snapshot.artifact.metadata["probe"],
        "registered-source-discovery"
    );
    assert_eq!(
        discovery.snapshot.artifact.metadata["source_driver"],
        "rest"
    );
    assert_eq!(
        discovery.snapshot.artifact.metadata["cdf:normalizer"],
        NORMALIZER_NAMECASE_V1
    );
    assert!(
        discovery
            .snapshot
            .artifact
            .schema
            .fields
            .iter()
            .any(|field| field.name == "active")
    );
    let score = discovery
        .snapshot
        .artifact
        .schema
        .fields
        .iter()
        .find(|field| field.name == "score")
        .unwrap();
    assert!(score.nullable);
    assert!(
        discovery
            .snapshot
            .artifact
            .schema
            .fields
            .iter()
            .any(|field| field.name == "updated_at")
    );
    let vendor = discovery
        .snapshot
        .artifact
        .schema
        .fields
        .iter()
        .find(|field| field.name == "vendor_id")
        .unwrap();
    assert_eq!(vendor.metadata["cdf:source_name"], "VendorID");
    assert_eq!(
        discovery.snapshot.source_identity["driver.source_kind"],
        "rest"
    );
    assert_eq!(discovery.snapshot.source_identity["driver.path"], "/items");
    assert_eq!(
        discovery.snapshot.source_identity["driver.sample_pages"],
        "1"
    );
    assert_eq!(
        discovery.snapshot.source_identity["driver.sample_records"],
        "3"
    );

    let requests = transport.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url, "https://api.example.test/items");
    assert_eq!(
        requests[0].headers.get("authorization").map(String::as_str),
        Some("Bearer rest-discover-secret")
    );
    let artifact_text = serde_json::to_string(&discovery.snapshot.artifact).unwrap();
    assert!(!artifact_text.contains("rest-discover-secret"));
}

#[test]
fn generic_discover_prepare_autopins_rest_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let project = r#"
[project]
name = "api"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."api.*"]
source = "resources/api.toml"
"#;
    let rest = r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"

[resource.items]
path = "/items"
records = "$.items"
cursor = { field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }
write_disposition = "append"
trust = "governed"
"#;
    let config = parse_cdf_toml(project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new().with_toml("resources/api.toml", rest);
    let mut resources =
        compile_project_declarative_resources(&test_source_registry(), &config, &resolver).unwrap();
    let resource = resources.remove(0);
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "VendorID": 1, "updated_at": 10 },
            { "VendorID": 2, "updated_at": 20 }
        ] }"#,
    )]);
    let prepared_payloads = cdf_runtime::PreparedSourcePayloads::default();
    let mut artifacts = discover_rest_schema_artifacts_for_test(
        temp.path(),
        &resource,
        transport.clone(),
        Arc::new(EnvSecretProvider::from_map(
            std::iter::empty::<(&str, &str)>(),
        )),
        prepared_payloads.clone(),
    )
    .unwrap();
    let prepared_resource = compile_discovered_schema_artifacts(&resource, &mut artifacts).unwrap();
    write_schema_discovery_artifacts(temp.path(), &artifacts).unwrap();
    let prepared = PreparedDiscoveredResource {
        resource: prepared_resource,
        discovery: Some(artifacts.discovery),
    };

    let discovery = prepared.discovery.as_ref().unwrap();
    let snapshot_path = temp.path().join(&discovery.snapshot.artifact.path);
    assert!(snapshot_path.is_file());
    let SchemaSource::Discovered { snapshot } = &prepared.resource.descriptor().schema_source
    else {
        panic!("expected REST auto-pin to set discovered schema source");
    };
    assert_eq!(
        snapshot.schema_hash,
        discovery.snapshot.artifact.schema_hash
    );
    assert_eq!(
        prepared
            .resource
            .schema()
            .field_with_name("vendor_id")
            .unwrap()
            .metadata()["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(transport.requests().len(), 1);
    assert_eq!(prepared_payloads.pending_count().unwrap(), 1);

    let execution = test_execution_services();
    let runtime_transport = transport.clone();
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry
        .register(
            cdf_source_rest::RestSourceDriver::new(move || Ok(Box::new(runtime_transport.clone())))
                .unwrap(),
        )
        .unwrap();
    let source_plan = prepared.resource.source_plan().clone();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        temp.path(),
        Arc::new(EnvSecretProvider::from_map(
            std::iter::empty::<(&str, &str)>(),
        )),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_prepared_payloads(prepared_payloads.clone());
    let runtime = registry.resolve(&source_plan, &resolution).unwrap();
    let plan = live_plan_for_stream(runtime.as_ref(), &source_plan, "pkg-rest-discovery-handoff");
    let stream = futures_executor::block_on(runtime.open(plan.scan.partitions[0].clone())).unwrap();
    let batches = futures_executor::block_on_stream(stream)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        2
    );
    assert_eq!(transport.requests().len(), 1);
    assert_eq!(prepared_payloads.pending_count().unwrap(), 0);
    drop(batches);
    assert_eq!(execution.memory().snapshot().current_bytes, 0);
}

#[test]
fn pinned_schema_preparation_requires_verified_snapshot_before_source_contact() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "json", "*.missing");
    let resource = compile_single_project_resource(temp.path());
    let snapshot = cdf_kernel::SchemaSnapshotReference {
        schema_hash: SchemaHash::new(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap(),
        path: ".cdf/schemas/missing.json".to_owned(),
        metadata: BTreeMap::new(),
    };
    let pinned = resource.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: snapshot.clone(),
        },
        resource.schema(),
    );

    let error = prepare_pinned_resource_schema(temp.path(), &pinned).unwrap_err();

    assert!(
        error.message.contains(".cdf/schemas/missing.json"),
        "{}",
        error.message
    );
    assert!(!temp.path().join(".cdf").exists());
}

#[test]
fn generic_schema_discovery_dispatch_fails_closed_for_non_postgres_sql_dialect() {
    let project = r#"
[project]
name = "warehouse"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."warehouse.*"]
source = "resources/sql.toml"
"#;
    let sql = r#"
[source.warehouse]
kind = "sql"
connection = "secret://env/WAREHOUSE_URL"
dialect = "mysql"

[resource.orders]
table = "orders"
write_disposition = "append"
trust = "governed"
"#;
    let config = parse_cdf_toml(project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new().with_toml("resources/sql.toml", sql);
    let error = compile_project_declarative_resources(&test_source_registry(), &config, &resolver)
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("dialect must be `postgres`"), "{message}");
}

struct RecordingResponse {
    response: HttpResponse,
    body: Vec<u8>,
}

fn json_response(body: &str) -> RecordingResponse {
    RecordingResponse {
        response: HttpResponse::new(200),
        body: body.as_bytes().to_vec(),
    }
}

#[derive(Clone, Default)]
struct RecordingTransport {
    state: Arc<Mutex<RecordingTransportState>>,
}

#[derive(Default)]
struct RecordingTransportState {
    requests: Vec<HttpRequest>,
    responses: VecDeque<RecordingResponse>,
}

impl RecordingTransport {
    fn new<I>(responses: I) -> Self
    where
        I: IntoIterator<Item = RecordingResponse>,
    {
        Self {
            state: Arc::new(Mutex::new(RecordingTransportState {
                requests: Vec::new(),
                responses: responses.into_iter().collect(),
            })),
        }
    }

    fn requests(&self) -> Vec<HttpRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl HttpTransport for RecordingTransport {
    fn send(
        &self,
        request: HttpRequest,
        budget: cdf_http::HttpResponseBudget,
    ) -> cdf_kernel::BoxFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let template = {
                let mut state = self.state.lock().unwrap();
                state.requests.push(request);
                state
                    .responses
                    .pop_front()
                    .ok_or_else(|| CdfError::internal("test transport exhausted responses"))?
            };
            Ok(template
                .response
                .with_body(budget.account_body(template.body).await?))
        })
    }
}

#[derive(Clone)]
struct RecordingHttpFileTransport {
    state: Arc<Mutex<RecordingHttpFileTransportState>>,
}

struct RecordingHttpFileTransportState {
    requests: Vec<HttpFileRequest>,
    body: Arc<Vec<u8>>,
    etag: Option<String>,
    missing: BTreeSet<String>,
    sequential_chunks_emitted: u64,
    sequential_bytes_emitted: u64,
    sequential_streams_active: u16,
    sequential_streams_peak: u16,
    sequential_streams_closed: u64,
    sequential_streams_completed: u64,
    memory: Option<Arc<dyn MemoryCoordinator>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RecordingSequentialProgress {
    chunks_emitted: u64,
    bytes_emitted: u64,
    active_streams: u16,
    peak_active_streams: u16,
    streams_closed: u64,
    streams_completed: u64,
}

struct RecordingHttpByteSource {
    state: Arc<Mutex<RecordingHttpFileTransportState>>,
    url: String,
    etag: Option<String>,
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    memory: Arc<dyn MemoryCoordinator>,
}

impl RecordingHttpByteSource {
    fn new(
        state: Arc<Mutex<RecordingHttpFileTransportState>>,
        resource: &FileTransportResource,
        expected: &FileIdentityMetadata,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Self> {
        let FileTransportLocation::HttpUrl { url } = &resource.location else {
            return Err(CdfError::contract(
                "recording HTTP byte source requires an HTTP(S) resource",
            ));
        };
        let strong = expected.etag.is_some();
        let identity = ContentIdentity {
            stable_id: url.clone(),
            size_bytes: expected.size_bytes,
            generation: expected.etag.clone().or_else(|| {
                expected
                    .size_bytes
                    .map(|size| format!("unversioned-size:{size}"))
            }),
            checksum: expected.sha256().map(str::to_owned),
            strength: if expected.sha256().is_some() {
                GenerationStrength::ContentAddressed
            } else if strong {
                GenerationStrength::Strong
            } else {
                GenerationStrength::Weak
            },
        };
        identity.validate()?;
        let capabilities = ByteSourceCapabilities {
            known_length: true,
            reopenable: true,
            seekable: strong,
            exact_ranges: strong,
            useful_range_concurrency: if strong { 4 } else { 0 },
            minimum_chunk_bytes: 1,
            maximum_chunk_bytes: 32 * 1024 * 1024,
        };
        capabilities.validate()?;
        state.lock().unwrap().memory = Some(Arc::clone(&memory));
        Ok(Self {
            state,
            url: url.clone(),
            etag: expected.etag.clone(),
            identity,
            capabilities,
            memory,
        })
    }
}

impl ByteSource for RecordingHttpByteSource {
    fn identity(&self) -> &ContentIdentity {
        &self.identity
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        &self.capabilities
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        Box::pin(async move {
            request.cancellation.check()?;
            if request.preferred_chunk_bytes < self.capabilities.minimum_chunk_bytes
                || request.preferred_chunk_bytes > self.capabilities.maximum_chunk_bytes
            {
                return Err(CdfError::contract(
                    "recording HTTP sequential chunk target is outside source capabilities",
                ));
            }
            let body = {
                let mut state = self.state.lock().unwrap();
                let mut request = HttpFileRequest::new(HttpMethod::Get, self.url.clone());
                if let Some(etag) = &self.etag {
                    request.headers.insert("if-match".to_owned(), etag.clone());
                }
                state.requests.push(request);
                state.sequential_streams_active += 1;
                state.sequential_streams_peak = state
                    .sequential_streams_peak
                    .max(state.sequential_streams_active);
                Arc::clone(&state.body)
            };
            let state = RecordingSequentialState {
                body,
                offset: 0,
                chunk_bytes: usize::try_from(request.preferred_chunk_bytes)
                    .map_err(|_| CdfError::data("test chunk size exceeds usize"))?,
                memory: Arc::clone(&self.memory),
                cancellation: request.cancellation,
                transport_state: Arc::clone(&self.state),
            };
            Ok(Box::pin(stream::try_unfold(state, |mut state| async move {
                state.cancellation.check()?;
                if state.offset == state.body.len() {
                    state
                        .transport_state
                        .lock()
                        .unwrap()
                        .sequential_streams_completed += 1;
                    return Ok(None);
                }
                let end = state
                    .offset
                    .saturating_add(state.chunk_bytes)
                    .min(state.body.len());
                let byte_count = u64::try_from(end.saturating_sub(state.offset))
                    .map_err(|_| CdfError::data("test byte length exceeds u64"))?;
                let reservation = ReservationRequest::new(
                    ConsumerKey::new("project-http-fixture", MemoryClass::Source)?,
                    byte_count,
                )?;
                let lease = reserve(Arc::clone(&state.memory), reservation).await?;
                let bytes = Bytes::copy_from_slice(&state.body[state.offset..end]);
                state.offset = end;
                {
                    let mut transport = state.transport_state.lock().unwrap();
                    transport.sequential_chunks_emitted += 1;
                    transport.sequential_bytes_emitted += byte_count;
                }
                Ok(Some((AccountedBytes::new(bytes, lease)?, state)))
            })) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<AccountedBytes>> {
        Box::pin(async move {
            cancellation.check()?;
            let end = extent
                .start
                .checked_add(extent.length)
                .ok_or_else(|| CdfError::data("test byte range overflow"))?;
            let start = usize::try_from(extent.start)
                .map_err(|_| CdfError::data("test byte range start exceeds usize"))?;
            let end = usize::try_from(end)
                .map_err(|_| CdfError::data("test byte range end exceeds usize"))?;
            let bytes = {
                let mut state = self.state.lock().unwrap();
                if end > state.body.len() {
                    return Err(CdfError::data("test byte range exceeds fixture"));
                }
                let mut request = HttpFileRequest::new(HttpMethod::Get, self.url.clone());
                request.headers.insert(
                    "range".to_owned(),
                    format!("bytes={}-{}", extent.start, end.saturating_sub(1)),
                );
                if let Some(etag) = &self.etag {
                    request.headers.insert("if-match".to_owned(), etag.clone());
                }
                state.requests.push(request);
                Bytes::copy_from_slice(&state.body[start..end])
            };
            let reservation = ReservationRequest::new(
                ConsumerKey::new("project-http-fixture-range", MemoryClass::Source)?,
                extent.length,
            )?;
            let lease = reserve(Arc::clone(&self.memory), reservation).await?;
            AccountedBytes::new(bytes, lease)
        })
    }
}

struct RecordingSequentialState {
    body: Arc<Vec<u8>>,
    offset: usize,
    chunk_bytes: usize,
    memory: Arc<dyn MemoryCoordinator>,
    cancellation: RunCancellation,
    transport_state: Arc<Mutex<RecordingHttpFileTransportState>>,
}

impl Drop for RecordingSequentialState {
    fn drop(&mut self) {
        let mut state = self.transport_state.lock().unwrap();
        state.sequential_streams_active -= 1;
        state.sequential_streams_closed += 1;
    }
}

impl RecordingHttpFileTransport {
    fn new(body: Vec<u8>) -> Self {
        Self {
            state: Arc::new(Mutex::new(RecordingHttpFileTransportState {
                requests: Vec::new(),
                body: Arc::new(body),
                etag: Some("\"fixture-etag\"".to_owned()),
                missing: BTreeSet::new(),
                sequential_chunks_emitted: 0,
                sequential_bytes_emitted: 0,
                sequential_streams_active: 0,
                sequential_streams_peak: 0,
                sequential_streams_closed: 0,
                sequential_streams_completed: 0,
                memory: None,
            })),
        }
    }

    fn with_missing(body: Vec<u8>, missing: impl IntoIterator<Item = String>) -> Self {
        let transport = Self::new(body);
        transport.state.lock().unwrap().missing.extend(missing);
        transport
    }

    fn requests(&self) -> Vec<HttpFileRequest> {
        self.state.lock().unwrap().requests.clone()
    }

    fn set_etag(&self, etag: &str) {
        self.state.lock().unwrap().etag = Some(etag.to_owned());
    }

    fn clear_etag(&self) {
        self.state.lock().unwrap().etag = None;
    }

    fn sequential_progress(&self) -> RecordingSequentialProgress {
        let state = self.state.lock().unwrap();
        RecordingSequentialProgress {
            chunks_emitted: state.sequential_chunks_emitted,
            bytes_emitted: state.sequential_bytes_emitted,
            active_streams: state.sequential_streams_active,
            peak_active_streams: state.sequential_streams_peak,
            streams_closed: state.sequential_streams_closed,
            streams_completed: state.sequential_streams_completed,
        }
    }

    fn current_memory_bytes(&self) -> u64 {
        self.state
            .lock()
            .unwrap()
            .memory
            .as_ref()
            .map_or(0, |memory| memory.snapshot().current_bytes)
    }
}

impl HttpFileTransport for RecordingHttpFileTransport {
    fn send_headers(
        &self,
        request: HttpFileRequest,
    ) -> BoxFuture<'static, Result<HttpFileResponse>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let mut state = state.lock().unwrap();
            state.requests.push(request.clone());
            match request.method {
                HttpMethod::Head if state.missing.contains(&request.url) => {
                    Ok(HttpFileResponse::new(404))
                }
                HttpMethod::Head => {
                    let mut response = HttpFileResponse::new(200)
                        .with_header("Content-Length", state.body.len().to_string());
                    if let Some(etag) = &state.etag {
                        response = response.with_header("ETag", etag.clone());
                    }
                    Ok(response)
                }
                HttpMethod::Get => {
                    let range = request.headers.get("range").ok_or_else(|| {
                        CdfError::data("test HTTP file transport requires ranged GET")
                    })?;
                    let (start, end) = parse_http_fixture_range(range, state.body.len())?;
                    Ok(HttpFileResponse::new(206).with_header(
                        "Content-Range",
                        format!("bytes {start}-{end}/{}", state.body.len()),
                    ))
                }
                _ => Ok(HttpFileResponse::new(405)),
            }
        })
    }

    fn open_byte_source(
        &self,
        resource: &FileTransportResource,
        expected: &FileIdentityMetadata,
        _auth: Option<cdf_object_access::ResolvedHttpAuth>,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Arc<dyn ByteSource>> {
        Ok(Arc::new(RecordingHttpByteSource::new(
            Arc::clone(&self.state),
            resource,
            expected,
            memory,
        )?))
    }
}

fn parse_http_fixture_range(range: &str, len: usize) -> Result<(usize, usize)> {
    let raw = range
        .strip_prefix("bytes=")
        .ok_or_else(|| CdfError::data(format!("invalid test range header `{range}`")))?;
    let (start, end) = raw
        .split_once('-')
        .ok_or_else(|| CdfError::data(format!("invalid test range header `{range}`")))?;
    let start = start
        .parse::<usize>()
        .map_err(|error| CdfError::data(format!("invalid range start: {error}")))?;
    let end = end
        .parse::<usize>()
        .map_err(|error| CdfError::data(format!("invalid range end: {error}")))?;
    if start > end || end >= len {
        return Err(CdfError::data(format!(
            "test range {start}-{end} exceeds fixture length {len}"
        )));
    }
    Ok((start, end))
}

fn vendor_parquet_bytes() -> Vec<u8> {
    vendor_parquet_bytes_with_rows(2)
}

fn vendor_parquet_bytes_with_rows(row_count: i32) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "VendorID",
        DataType::Int32,
        false,
    )]));
    let values = (0..row_count).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap();
    cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap()
}

fn write_http_discover_project(root: &Path, source_extra: &str) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "http_files"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."remote.*"]
source = "resources/files.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/files.toml"),
        format!(
            r#"
[source.remote]
kind = "files"
root = "https://data.example.test/trip-data"
{source_extra}

[resource.events]
glob = "vendors.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
}

fn write_http_external_mock_project(root: &Path) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "external_remote"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."external.*"]
source = "resources/files.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/files.toml"),
        r#"
[source.external]
kind = "files"
root = "https://data.example.test/custom"

[resource.events]
glob = "events.mock"
format = "project_external_mock"
write_disposition = "append"
trust = "governed"
"#,
    )
    .unwrap();
}

fn write_object_store_discover_project(root: &Path) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "cloud_files"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."remote.*"]
source = "resources/files.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/files.toml"),
        r#"
[source.remote]
kind = "files"
root = "s3://tlc/trip-data"

[resource.events]
glob = "2024/**/*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#,
    )
    .unwrap();
}

fn write_object_store_ndjson_discover_project(root: &Path) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "cloud_events"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."events.*"]
source = "resources/files.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/files.toml"),
        r#"
[source.events]
kind = "files"
root = "s3://acme-events/prod"

[resource.raw]
glob = "2026/**/*.ndjson.gz"
format = "ndjson"
write_disposition = "append"
trust = "governed"
"#,
    )
    .unwrap();
}

fn http_file_dependencies(transport: RecordingHttpFileTransport) -> FileRuntimeDependencies {
    file_dependencies(FileTransportFacade::new().with_http_transport(transport))
}

fn live_plan_for_stream(
    resource: &dyn QueryableResource,
    source_plan: &cdf_runtime::CompiledSourcePlan,
    package_id: &str,
) -> EnginePlan {
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let destination = ResolvedProjectDestination::duckdb(
        "/tmp/cdf-project-plan-policy-only.duckdb",
        TargetName::new("events").unwrap(),
    )
    .unwrap();
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = destination.column_identifier_policy().unwrap().unwrap();
    let validation_program = compile_validation_program(&policy, &observed_schema).unwrap();
    Planner::new()
        .plan_tier_b(
            resource,
            EnginePlanInput {
                request: ScanRequest {
                    resource_id: resource.descriptor().resource_id.clone(),
                    projection: None,
                    filters: Vec::new(),
                    limit: None,
                    order_by: Vec::new(),
                    scope: resource.descriptor().state_scope.clone(),
                },
                validation_program,
                execution_extent: ExecutionExtent::bounded(),
                package_id: package_id.to_owned(),
            },
        )
        .unwrap()
        .bind_compiled_source(source_plan)
        .unwrap()
}

fn assert_only_bounded_http_file_gets(requests: &[HttpFileRequest]) {
    assert!(
        requests
            .iter()
            .any(|request| request.method == HttpMethod::Head),
        "expected an HTTP HEAD metadata request, got {requests:?}"
    );
    let get_requests = requests
        .iter()
        .filter(|request| request.method == HttpMethod::Get)
        .collect::<Vec<_>>();
    assert!(
        !get_requests.is_empty(),
        "expected bounded HTTP GET requests, got {requests:?}"
    );
    for request in get_requests {
        let range = request
            .headers
            .get("range")
            .expect("HTTP file GET should carry Range header");
        assert!(
            range.starts_with("bytes="),
            "HTTP file GET should use byte range, got {range}"
        );
    }
}

fn assert_http_file_gets_download_less_than_fixture(
    requests: &[HttpFileRequest],
    fixture_len: usize,
) {
    let downloaded = requests
        .iter()
        .filter(|request| request.method == HttpMethod::Get)
        .map(|request| {
            let range = request
                .headers
                .get("range")
                .expect("HTTP file GET should carry Range header");
            let (start, end) = parse_http_fixture_range(range, fixture_len).unwrap();
            end - start + 1
        })
        .sum::<usize>();
    assert!(
        downloaded < fixture_len,
        "expected discovery to use partial ranged reads, downloaded {downloaded} of {fixture_len} bytes"
    );
}

struct StaticSecretProvider {
    values: BTreeMap<String, String>,
}

impl StaticSecretProvider {
    fn new<I, K, V>(values: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            values: values
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}

impl SecretProvider for StaticSecretProvider {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        self.values
            .get(uri.as_str())
            .map(|value| SecretValue::new(value.clone()))
            .ok_or_else(|| CdfError::auth(format!("missing test secret `{uri}`")))
    }
}

fn write_discover_project(root: &Path, format: &str, glob: &str) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "files"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."local.*"]
source = "resources/files.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/files.toml"),
        format!(
            r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "{glob}"
format = "{format}"
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
}

fn write_sampled_discover_project(root: &Path, format: &str, glob: &str, sample_files: u64) {
    write_discover_project(root, format, glob);
    let path = root.join("resources/files.toml");
    let input = fs::read_to_string(&path).unwrap();
    fs::write(
        path,
        input.replace(
            &format!("glob = \"{glob}\""),
            &format!("glob = \"{glob}\"\nsample_files = {sample_files}"),
        ),
    )
    .unwrap();
}

fn compile_single_project_resource(root: &Path) -> cdf_declarative::CompiledResource {
    let config = parse_cdf_toml(&fs::read_to_string(root.join("cdf.toml")).unwrap()).unwrap();
    let resolver = FileResourceSourceResolver::new(root);
    let mut resources = compile_project_declarative_resources_with_root(
        &test_source_registry(),
        &config,
        &resolver,
        root,
    )
    .unwrap();
    assert_eq!(resources.len(), 1);
    resources.remove(0)
}

fn write_vendor_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "VendorID",
        DataType::Int32,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32]))]).unwrap();
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    fs::write(path, bytes).unwrap();
}

fn write_parquet_fixture(path: &Path, fields: Vec<Field>, columns: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    fs::write(path, bytes).unwrap();
}

fn write_arrow_ipc_fixture(path: &Path, fields: Vec<Field>, columns: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let file = fs::File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, batch.schema().as_ref()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
}

#[test]
fn contract_freeze_preserves_existing_dependency_and_destination_data() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let resources =
        compile_project_declarative_resources(&test_source_registry(), &config, &resolver).unwrap();
    let sheet = destination_sheet("duckdb", TypeMappingFidelity::Lossless);
    let sheet_artifact =
        DestinationSheetArtifact::new(sheet, DestinationProtocolCapabilities::default()).unwrap();
    let dependency_tuple = DependencyTuple {
        cdf: "0.1.0-old".to_owned(),
        arrow_rs: "58.3.0-old".to_owned(),
        datafusion: Some("pinned-datafusion".to_owned()),
        object_store: Some("pinned-object-store".to_owned()),
        duckdb_rs: Some("pinned-duckdb".to_owned()),
        rust: Some("pinned-rust".to_owned()),
    };
    let existing = generate_lockfile_with_destination_artifacts(
        &config,
        &resources,
        dependency_tuple.clone(),
        std::slice::from_ref(&sheet_artifact),
        BTreeMap::new(),
    )
    .unwrap();

    let (lock, report) = freeze_contract_snapshots(
        &config,
        &resources,
        Some(&existing),
        &[],
        Some("github.issues"),
    )
    .unwrap();

    assert_eq!(lock.dependency_tuple, dependency_tuple);
    assert_eq!(lock.destinations, existing.destinations);
    assert_eq!(report.resource_ids, vec!["github.issues"]);
    let snapshot = lock.resources["github.issues"].contract.as_ref().unwrap();
    assert!(
        snapshot
            .policy_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        snapshot
            .validation_program_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
}

#[test]
fn contract_test_reports_field_level_snapshot_drift() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let resources =
        compile_project_declarative_resources(&test_source_registry(), &config, &resolver).unwrap();
    let artifact = cdf_kernel::DestinationSheetArtifact::new(
        destination_sheet("duckdb", TypeMappingFidelity::Lossless),
        cdf_kernel::DestinationProtocolCapabilities::default(),
    )
    .unwrap();
    let (lock, _) =
        freeze_contract_snapshots(&config, &resources, None, &[artifact], None).unwrap();
    let changed_resource = GITHUB_RESOURCE.replace(
        "  { name = \"updated_at\", type = \"timestamp_micros\", nullable = false, timezone = \"UTC\" },",
        concat!(
            "  { name = \"updated_at\", type = \"timestamp_micros\", nullable = false, timezone = \"UTC\" },\n",
            "  { name = \"ingested_at\", type = \"int64\", nullable = true },"
        ),
    );
    let changed_resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", &changed_resource);
    let changed_resources =
        compile_project_declarative_resources(&test_source_registry(), &config, &changed_resolver)
            .unwrap();

    let report = test_contract_snapshots(&lock, &changed_resources, Some("github.issues")).unwrap();

    assert_eq!(report.counts.passed, 0);
    assert_eq!(report.counts.drifted, 1);
    let fields = report
        .drift_details
        .iter()
        .map(|detail| detail.field.as_str())
        .collect::<Vec<_>>();
    assert!(fields.contains(&"schema_hash"));
    assert!(fields.contains(&"validation_program_hash"));
}

fn destination_sheet(name: &str, fidelity: TypeMappingFidelity) -> DestinationSheet {
    DestinationSheet {
        destination: DestinationId::new(name).unwrap(),
        supported_dispositions: vec![WriteDisposition::Append, WriteDisposition::Merge],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: vec![TypeMapping {
            arrow_type: "utf8".to_owned(),
            destination_type: "text".to_owned(),
            fidelity,
        }],
        identifier_rules: IdentifierRules {
            normalizer: NORMALIZER_NAMECASE_V1.to_owned(),
            max_length: Some(63),
            allowed_pattern: Some("[a-z_][a-z0-9_]*".to_owned()),
        },
        migration_support: CapabilitySupport::Supported,
        quarantine_tables: CapabilitySupport::Supported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    }
}

#[test]
fn inline_uri_credentials_are_rejected() {
    let input = BOOK_PROJECT.replace(
        "destination = \"duckdb://.cdf/dev.duckdb\"",
        "destination = \"postgres://user:password@example.com/db\"",
    );
    let config = parse_cdf_toml(&input).unwrap();

    let error = config.effective_environment("dev").and_then(|env| {
        validate_environment_uri_fields(&env)?;
        Ok(())
    });

    assert!(
        error
            .unwrap_err()
            .to_string()
            .contains("inline credentials")
    );
}

#[test]
fn secret_ref_requires_provider_and_key() {
    assert!(SecretRef::new("secret://env/TOKEN").is_ok());
    assert!(SecretRef::new("env:TOKEN").is_err());
    assert!(SecretRef::new("secret://env").is_err());
}

#[test]
fn declarative_resource_compilation_hook_uses_cdf_declarative() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);

    let resources =
        compile_project_declarative_resources(&test_source_registry(), &config, &resolver).unwrap();

    assert_eq!(resources.len(), 1);
    assert_eq!(
        resources[0].descriptor().resource_id.as_str(),
        "github.issues"
    );
}

#[test]
fn declarative_resource_mapping_pattern_must_match_compiled_id() {
    let project = r#"
[project]
name = "tlc"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."yellow"]
source = "resources/tlc.toml"
"#;
    let resource = r#"
[source.tlc]
kind = "files"
root = "data"

[resource.yellow]
glob = "*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#;
    let config = parse_cdf_toml(project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new().with_toml("resources/tlc.toml", resource);

    let error = compile_project_declarative_resources(&test_source_registry(), &config, &resolver)
        .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("resource mapping pattern `yellow`"));
    assert!(message.contains("tlc.yellow"));
    assert!(message.contains("`<source>.<resource>`"));
    assert!(message.contains("[resources.\"tlc.yellow\"]"));
}

#[test]
fn declarative_file_roots_resolve_under_project_root_for_runtime_compile() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir_all(temp.path().join("resources")).unwrap();
    let project = r#"
[project]
name = "files"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."local.*"]
source = "resources/files.toml"
"#;
    let resource = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
"#;
    fs::write(temp.path().join("resources/files.toml"), resource).unwrap();
    let config = parse_cdf_toml(project).unwrap();
    let resolver = FileResourceSourceResolver::new(temp.path());

    let resources = compile_project_declarative_resources_with_root(
        &test_source_registry(),
        &config,
        &resolver,
        temp.path(),
    )
    .unwrap();

    assert_eq!(
        resources[0].source_plan().physical_plan["source"]["root"],
        "data"
    );
    assert_eq!(resources[0].project_root(), Some(temp.path()));
}

#[test]
fn local_project_scaffold_writes_valid_project_without_runtime_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("fresh-project");

    let report = write_local_project_scaffold(ProjectScaffoldOptions {
        root: root.clone(),
        project_name: None,
        force: false,
    })
    .unwrap();

    assert_eq!(report.project_name, "fresh-project");
    assert_eq!(
        report.created,
        vec![
            "cdf.toml",
            "README.md",
            "resources",
            "resources/files.toml",
            "data"
        ]
    );
    assert!(root.join("cdf.toml").is_file());
    assert!(root.join("README.md").is_file());
    assert!(root.join("resources/files.toml").is_file());
    assert!(root.join("data").is_dir());
    assert!(fs::read_dir(root.join("data")).unwrap().next().is_none());
    assert!(!root.join(".cdf").exists());
    assert!(!root.join("cdf.lock").exists());

    let config = parse_cdf_toml(&fs::read_to_string(root.join("cdf.toml")).unwrap()).unwrap();
    let readme = fs::read_to_string(root.join("README.md")).unwrap();
    let resource = fs::read_to_string(root.join("resources/files.toml")).unwrap();
    assert!(readme.contains("docs/quickstart.md"));
    assert!(readme.contains("cdf validate"));
    assert!(readme.contains("cdf plan local.events"));
    assert!(readme.contains("cdf run local.events"));
    assert!(!readme.contains("secret://"));
    assert!(!readme.contains(root.to_str().unwrap()));
    assert!(!resource.contains("primary_key"));
    assert!(!resource.contains("merge_key"));
    let resolver = FileResourceSourceResolver::new(&root);
    let provider = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());
    let validation = validate_project(
        &test_source_registry(),
        &config,
        Some("dev"),
        &resolver,
        &provider,
    )
    .unwrap();

    assert_eq!(validation.declarative_resources, 1);
    assert!(validation.checked_secrets.is_empty());
}

#[test]
fn declarative_sql_secret_is_collected_for_validation() {
    let project = BOOK_PROJECT.replace(
        "[resources.\"github.*\"]\nsource = \"resources/github.toml\"",
        "[resources.\"warehouse.*\"]\nsource = \"resources/sql.toml\"",
    );
    let sql_resource = r#"
[source.warehouse]
kind = "sql"
connection = "secret://env/POSTGRES_URL"

[resource.orders]
table = "public.orders"
primary_key = ["id"]
merge_key = ["id"]
write_disposition = "merge"
trust = "governed"
"#;
    let config = parse_cdf_toml(&project).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/sql.toml", sql_resource);
    let provider = EnvSecretProvider::from_map([
        ("POSTGRES_URL", "postgres-url-value"),
        ("PROD_DWH", "postgres-dsn-value"),
    ]);

    let report = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resolver,
        &provider,
    )
    .unwrap();

    assert!(
        report
            .checked_secrets
            .iter()
            .any(|check| check.uri.as_str() == "secret://env/POSTGRES_URL")
    );
    assert!(!format!("{report:?}").contains("postgres-url-value"));
}

#[test]
fn unsupported_keychain_provider_is_explicit_not_guessy() {
    let provider = DefaultSecretProvider::default();
    let uri = SecretUri::new("secret://keychain/prod-token").unwrap();
    let error = provider.resolve(&uri).unwrap_err();

    assert!(error.to_string().contains("not available"));
    assert!(!error.to_string().contains("prod-token-value"));
}

#[test]
fn source_declaration_is_registry_open_and_preserves_secret_references() {
    let source = SourceDeclaration {
        kind: "external_api".to_owned(),
        options: BTreeMap::from([(
            "token".to_owned(),
            serde_json::Value::String("secret://env/TOKEN".to_owned()),
        )]),
    };

    assert_eq!(source.kind, "external_api");
    assert_eq!(source.options["token"], "secret://env/TOKEN");
}
