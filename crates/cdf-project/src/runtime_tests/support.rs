use super::{
    Arc, Array, ArrayRef, AtomicBool, AtomicU64, BTreeMap, BackpressureSupport,
    CHECKPOINT_STATE_VERSION, CapabilitySupport, CdfError, CheckpointId, Client, Command,
    CommitCounts, CommitPlan, CommitSession, CompiledStreamAdmissionEvidence, ConcurrencyLimit,
    ContractPolicy, CursorOrderingClaim, CursorPosition, CursorSpec, CursorValue, DataType,
    DeliveryGuarantee, DestinationCommitPlanPreimage, DestinationCommitPlanningInputs,
    DestinationCommitPlanningOutcome, DestinationCommitRequest, DestinationId, DestinationProtocol,
    DestinationReceiptReportingPolicy, DestinationSheet, DuckDbDestination, EnginePlan,
    EnginePlanInput, EstimateSupport, ExecutionExtent, Field, FileRuntimeDependencies,
    FileSourceDriver, FileTransportFacade, FilterCapabilities, HttpRequest, HttpResponse,
    HttpTransport, IdempotencySupport, IdentifierRules, InMemoryScopeLeaseStore, IncrementalShape,
    Int64Array, LATE_DATA_CARRYOVER_VERSION, LateDataCarryoverRef, LineageInputObservation,
    LineageSummary, MigrationRecord, Mutex, NoTls, ObservedSchema, Ordering,
    PROCESSED_OBSERVATIONS_FILE, PackageBuilder, PackageHash, PackageManifest, PackageReader,
    PackageReplayInputs, PackageStatus, PartitionId, Path, PathBuf, PipelineId, PlanId, Planner,
    PostgresTarget, PreparedDestinationCommit, ProcessedObservationEvidenceArtifact,
    ProcessedObservationOutcome, ProcessedObservationPosition, ProjectDestinationDescription,
    ProjectDestinationRuntime, ProjectRunOutcome, ProjectRunReport, ProjectRunRequest,
    ProjectRunSource, PushdownFidelity, QueryableResource, Receipt, ReceiptId, ReceiptVerification,
    RecordBatch, ReplaySupport, ResolvedProjectDestination, ResourceCapabilities,
    ResourceDescriptor, ResourceId, ResourceStream, RestSourceDriver, Result, RunId,
    RunTelemetryConfig, ScanRequest, Schema, SchemaHash, SchemaSource, ScopeKey, SecretProvider,
    SecretUri, SecretValue, SegmentAck, SegmentEntry, SegmentId, SourcePosition, StateDelta,
    StateDeltaPreimage, StateSegment, StreamAdmissionObservationEvidence, StringArray, TargetName,
    TcpListener, TempDir, TransactionSupport, TrustLevel, TypeMapping, TypeMappingFidelity,
    VecDeque, VerifyClause, WriteDisposition, canonical_json_bytes, compile_validation_program,
    env, fs, negotiate_scan_plan, run_project_with_execution_services_and_telemetry,
};

pub(super) fn test_execution_services() -> cdf_runtime::ExecutionServices {
    let services = cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024)
        .unwrap()
        .1;
    let scopes: Arc<dyn cdf_kernel::ScopeLeaseStore> = Arc::new(InMemoryScopeLeaseStore::new());
    services
        .with_staging_lease_authority(Arc::new(cdf_runtime::ScopeStagingLeaseAuthority::new(
            scopes,
        )))
        .unwrap()
        .with_content_reachability_store(Arc::new(
            cdf_state_sqlite::SqliteContentReachabilityStore::open_in_memory().unwrap(),
        ))
}

pub(super) async fn run_project(request: ProjectRunRequest<'_>) -> Result<ProjectRunReport> {
    run_project_outcome(request).await?.into_committed()
}

pub(super) async fn run_project_outcome(
    request: ProjectRunRequest<'_>,
) -> Result<ProjectRunOutcome> {
    let services = test_execution_services();
    Box::pin(run_project_outcome_fixture(
        request,
        &services,
        RunTelemetryConfig::disabled(),
    ))
    .await
}

pub(super) async fn run_project_fixture<'a>(
    request: ProjectRunRequest<'a>,
    services: &cdf_runtime::ExecutionServices,
    telemetry: RunTelemetryConfig,
) -> Result<ProjectRunReport> {
    run_project_outcome_fixture(request, services, telemetry)
        .await?
        .into_committed()
}

pub(super) async fn run_project_outcome_fixture<'a>(
    mut request: ProjectRunRequest<'a>,
    services: &cdf_runtime::ExecutionServices,
    telemetry: RunTelemetryConfig,
) -> Result<ProjectRunOutcome> {
    if request.plan.compiled_source_execution.is_some() {
        return run_project_with_execution_services_and_telemetry(request, services, telemetry)
            .await;
    }
    let resource = request.resource.queryable();
    let source = compiled_test_source_plan(resource);
    let compiled_source_plan_hash = source.compiled_source_plan_hash()?;
    request.plan = request.plan.bind_compiled_source(&source)?;
    let bound = BoundTestResource {
        inner: resource,
        compiled_source_plan_hash,
        replay_retention: None,
    };
    request.resource = ProjectRunSource::new(&bound);
    run_project_with_execution_services_and_telemetry(request, services, telemetry).await
}

pub(super) struct BoundTestResource<'a> {
    pub(super) inner: &'a dyn QueryableResource,
    pub(super) compiled_source_plan_hash: cdf_kernel::CompiledSourcePlanHash,
    pub(super) replay_retention: Option<&'a dyn cdf_kernel::SourceReplayRetention>,
}

impl ResourceStream for BoundTestResource<'_> {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.inner.descriptor()
    }

    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        Some(&self.compiled_source_plan_hash)
    }

    fn validate_runtime_dependencies(&self) -> Result<()> {
        self.inner.validate_runtime_dependencies()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<cdf_kernel::PartitionPlan>> {
        self.inner.plan_partitions(request)
    }

    fn planned_partition_reader(
        &self,
        reference: &cdf_kernel::PlannedTaskSetReference,
    ) -> Result<Box<dyn cdf_kernel::PlannedPartitionReader>> {
        self.inner.planned_partition_reader(reference)
    }

    fn rebind_scan_for_resume(
        &self,
        scan: cdf_kernel::ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<cdf_kernel::ScanPlan> {
        self.inner.rebind_scan_for_resume(scan, committed_frontier)
    }

    fn open(&self, partition: cdf_kernel::PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.inner.open(partition)
    }

    fn open_executable(
        &self,
        partition: cdf_kernel::ExecutablePartition,
    ) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.inner.open_executable(partition)
    }

    fn attest_partition(
        &self,
        partition: cdf_kernel::PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.inner.attest_partition(partition)
    }

    fn attest_executable(
        &self,
        partition: cdf_kernel::ExecutablePartition,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.inner.attest_executable(partition)
    }

    fn effective_schema_runtime(&self) -> Option<&cdf_kernel::EffectiveSchemaRuntime> {
        self.inner.effective_schema_runtime()
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.inner.type_policy_allowances()
    }

    fn replay_retention(&self) -> Option<&dyn cdf_kernel::SourceReplayRetention> {
        self.replay_retention
    }
}

pub(super) fn test_file_runtime_dependencies() -> FileRuntimeDependencies {
    let execution = test_execution_services();
    let mut formats = cdf_builtin_drivers::new_builtin_format_registry().unwrap();
    formats
        .register(Arc::new(
            cdf_format_avro::AvroOcfFormatDriver::new().unwrap(),
        ))
        .unwrap();
    formats
        .register(Arc::new(
            cdf_format_avro::AvroSingleObjectFormatDriver::new().unwrap(),
        ))
        .unwrap();
    let transforms = cdf_builtin_drivers::new_builtin_transform_registry().unwrap();
    FileRuntimeDependencies::new(
        FileTransportFacade::new().with_execution_services(execution.clone()),
        execution,
        Arc::new(formats),
        Arc::new(transforms),
        cdf_runtime::SourceEgressScope::new(
            cdf_runtime::SourceDriverId::new("files").unwrap(),
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        ),
    )
}

pub(super) struct OwnedTestResource {
    pub(super) inner: Arc<dyn QueryableResource>,
    pub(super) source_plan: cdf_runtime::CompiledSourcePlan,
}

impl ResourceStream for OwnedTestResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.inner.descriptor()
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        self.inner.compiled_source_plan_hash()
    }

    fn validate_runtime_dependencies(&self) -> Result<()> {
        self.inner.validate_runtime_dependencies()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<cdf_kernel::PartitionPlan>> {
        self.inner.plan_partitions(request)
    }

    fn planned_partition_reader(
        &self,
        reference: &cdf_kernel::PlannedTaskSetReference,
    ) -> Result<Box<dyn cdf_kernel::PlannedPartitionReader>> {
        self.inner.planned_partition_reader(reference)
    }

    fn rebind_scan_for_resume(
        &self,
        scan: cdf_kernel::ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<cdf_kernel::ScanPlan> {
        self.inner.rebind_scan_for_resume(scan, committed_frontier)
    }

    fn open(&self, partition: cdf_kernel::PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.inner.open(partition)
    }

    fn open_executable(
        &self,
        partition: cdf_kernel::ExecutablePartition,
    ) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.inner.open_executable(partition)
    }

    fn attest_partition(
        &self,
        partition: cdf_kernel::PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.inner.attest_partition(partition)
    }

    fn attest_executable(
        &self,
        partition: cdf_kernel::ExecutablePartition,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.inner.attest_executable(partition)
    }

    fn effective_schema_runtime(&self) -> Option<&cdf_kernel::EffectiveSchemaRuntime> {
        self.inner.effective_schema_runtime()
    }

    fn baseline_observation_schema_catalog(&self) -> &[cdf_kernel::EffectiveSchemaCatalogEntry] {
        self.inner.baseline_observation_schema_catalog()
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.inner.type_policy_allowances()
    }
}

impl QueryableResource for OwnedTestResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<cdf_kernel::ScanPlan> {
        self.inner.negotiate(request)
    }
}

pub(super) fn compile_test_file_resource(root: &Path, document: &str) -> OwnedTestResource {
    let document = cdf_declarative::parse_toml(document).unwrap();
    let dependencies = test_file_runtime_dependencies();
    let formats = Arc::clone(dependencies.formats());
    let installed = dependencies.clone();
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry
        .register(
            FileSourceDriver::new(formats, move |_secrets, _execution, _egress| {
                Ok(installed.clone())
            })
            .unwrap(),
        )
        .unwrap();
    let resource = cdf_declarative::compile_document_with_project_root(&registry, &document, root)
        .unwrap()
        .remove(0);
    let execution = test_execution_services();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        root,
        Arc::new(StaticSecretProvider::new(std::iter::empty::<(&str, &str)>())),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    OwnedTestResource {
        source_plan: resource.source_plan().clone(),
        inner: registry
            .resolve(resource.source_plan(), &resolution)
            .unwrap(),
    }
}

pub(super) const SCHEMA_HASH: &str =
    "sha256:f3e5592a1a5159773a70d3dfc1255d47a98be505b2ce6e57218e5c879c4eaeef";
pub(super) const LIVE_FILE_RESOURCE: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "int64", nullable = false },
] }
"#;
pub(super) const SIMPLE_FILE_RESOURCE_APPEND: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
] }
"#;
pub(super) const MULTI_FILE_RESOURCE_APPEND: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events-*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
] }
"#;
pub(super) struct BackfillMockResource {
    pub(super) descriptor: ResourceDescriptor,
    pub(super) capabilities: ResourceCapabilities,
    pub(super) schema: Arc<Schema>,
    pub(super) open_count: AtomicU64,
}

impl BackfillMockResource {
    pub(super) fn cursor() -> Self {
        Self::new(IncrementalShape::Cursor, Some(CursorOrderingClaim::Exact))
    }

    pub(super) fn file_incremental() -> Self {
        Self::new(IncrementalShape::File, Some(CursorOrderingClaim::Exact))
    }

    pub(super) fn postgres_unallowed_lossy_schema() -> Self {
        let mut resource = Self::new(IncrementalShape::Cursor, Some(CursorOrderingClaim::Exact));
        resource.descriptor.resource_id = ResourceId::new("mock.unsupported_postgres").unwrap();
        resource
            .descriptor
            .cursor
            .as_mut()
            .expect("cursor fixture")
            .field = "id".to_owned();
        resource.schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "seen_at",
                DataType::Duration(arrow_schema::TimeUnit::Nanosecond),
                false,
            ),
        ]));
        resource
    }

    pub(super) fn new(
        incremental: IncrementalShape,
        ordering: Option<CursorOrderingClaim>,
    ) -> Self {
        let schema_hash = SchemaHash::new("schema-backfill-mock").unwrap();
        Self {
            descriptor: ResourceDescriptor {
                resource_id: ResourceId::new("mock.events").unwrap(),
                schema_source: SchemaSource::Declared {
                    schema_hash,
                    source: "mock".to_owned(),
                },
                primary_key: vec!["id".to_owned()],
                merge_key: vec!["id".to_owned()],
                cursor: ordering.map(|ordering| CursorSpec {
                    field: "updated_at".to_owned(),
                    ordering,
                    lag_tolerance_ms: 0,
                }),
                write_disposition: WriteDisposition::Append,
                deduplication: None,
                contract: None,
                state_scope: ScopeKey::Resource,
                freshness: None,
                trust_level: TrustLevel::Governed,
            },
            capabilities: ResourceCapabilities {
                projection: CapabilitySupport::Unsupported,
                filters: FilterCapabilities {
                    default_fidelity: PushdownFidelity::Exact,
                    supported_operators: vec![">=".to_owned(), "<".to_owned()],
                },
                limits: CapabilitySupport::Unsupported,
                ordering: CapabilitySupport::Unsupported,
                partitioning: Default::default(),
                incremental,
                replay: ReplaySupport::FromPosition,
                idempotent_reads: true,
                backpressure: BackpressureSupport::Pausable,
                estimates: EstimateSupport::None,
            },
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("updated_at", DataType::Int64, false),
            ])),
            open_count: AtomicU64::new(0),
        }
    }
}

impl ResourceStream for BackfillMockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<cdf_kernel::PartitionPlan>> {
        let mut metadata = BTreeMap::new();
        metadata.insert("kind".to_owned(), "mock".to_owned());
        Ok(vec![cdf_kernel::PartitionPlan {
            partition_id: PartitionId::new("mock").unwrap(),
            scope: request.scope.clone(),
            planned_position: None,
            start_position: None,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata,
        }])
    }

    fn open(&self, _partition: cdf_kernel::PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.open_count.fetch_add(1, Ordering::SeqCst);
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
            Err(CdfError::internal(
                "mock backfill source should not be opened",
            ))
        }))
    }
}

impl QueryableResource for BackfillMockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<cdf_kernel::ScanPlan> {
        negotiate_scan_plan(
            self.descriptor.resource_id.clone(),
            request.clone(),
            &self.capabilities,
            self.plan_partitions(request)?,
            None,
            None,
            DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        )
    }
}

pub(super) const POSTGRES_RUNTIME_RESOURCE: &str = r#"
[source.warehouse]
kind = "postgres"
connection = "secret://env/POSTGRES_URL"
dialect = "postgres"

[resource.orders]
table = "public.orders"
primary_key = ["id"]
cursor = { field = "updated_at", ordering = "exact", lag = "0ms" }
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "int64", nullable = false },
] }
"#;

pub(super) static LIVE_POSTGRES_SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);
pub(super) static LOCAL_POSTGRES_START: Mutex<()> = Mutex::new(());

pub(super) fn sample_batch(ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = std::sync::Arc::new(Int64Array::from(ids));
    let name: ArrayRef = std::sync::Arc::new(StringArray::from(names));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

pub(super) fn package_id_name_rows(reader: &PackageReader) -> Vec<(i64, Option<String>)> {
    let mut rows = Vec::new();
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
    );
    let segments = reader
        .verified_segment_stream(memory, 64 * 1024 * 1024)
        .unwrap();
    for segment in segments {
        for batch in segment.unwrap().batches {
            let ids = batch
                .column(batch.schema().index_of("id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let names = batch
                .column(batch.schema().index_of("name").unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let name = (!names.is_null(row)).then(|| names.value(row).to_owned());
                rows.push((ids.value(row), name));
            }
        }
    }
    rows
}

pub(super) fn build_package_with_carryover(
    package_dir: &Path,
    package_id: &str,
) -> (PackageManifest, LateDataCarryoverRef) {
    let builder = PackageBuilder::create(
        package_dir,
        package_id,
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    builder.update_status(PackageStatus::Extracting).unwrap();
    let carryover_batch = sample_batch(vec![7], vec![Some("late")]);
    builder
        .write_runtime_arrow_schema(carryover_batch.schema().as_ref())
        .unwrap();
    builder
        .write_json_artifact(
            "schema/output.arrow.json",
            &BTreeMap::from([("schema_hash", SCHEMA_HASH)]),
        )
        .unwrap();
    let carryover_file = builder
        .write_ipc_identity_batches(
            "carryover/late-data-000.arrow",
            std::slice::from_ref(&carryover_batch),
        )
        .unwrap();
    let batches = cdf_package_contract::append_package_row_ord(
        vec![sample_batch(
            vec![1, 2, 3],
            vec![Some("current-1"), Some("current-2"), Some("current-3")],
        )],
        0,
    )
    .unwrap();
    let segment = builder
        .write_segment(
            cdf_kernel::SegmentId::new("seg-000001").unwrap(),
            0,
            &batches,
        )
        .unwrap();
    builder
        .write_lineage_artifact(
            "lineage.json",
            &canonical_json_bytes(&LineageSummary {
                input_rows: 3,
                input_observations: vec![LineageInputObservation {
                    observation_id: "artifact-fixture".to_owned(),
                    partition_id: PartitionId::new("artifact-fixture").unwrap(),
                    partition_binding: artifact_fixture_partition_binding(),
                    observed_rows: 3,
                    output_position: Some(position(3)),
                }],
            })
            .unwrap(),
        )
        .unwrap();
    let reference = LateDataCarryoverRef {
        version: LATE_DATA_CARRYOVER_VERSION,
        package_id: package_id.to_owned(),
        relative_path: carryover_file.path,
        byte_count: carryover_file.byte_count,
        sha256: carryover_file.sha256,
        row_count: 1,
        memory_bound_bytes: u64::try_from(carryover_batch.get_array_memory_size()).unwrap(),
        output_position: position(1),
    };
    reference.validate().unwrap();
    write_state_commit_artifacts(
        &builder,
        &segment,
        WriteDisposition::Append,
        "checkpoint-carryover-artifact",
        vec![reference.clone()],
    );
    write_compiled_expression_artifacts(&builder, false, true, None, false);
    builder.finish().unwrap();
    let manifest = cdf_package::read_manifest(package_dir).unwrap();
    (manifest, reference)
}

pub(super) fn write_compiled_expression_artifacts(
    builder: &PackageBuilder,
    stale: bool,
    write_stream_evidence: bool,
    quarantine: Option<(
        &cdf_kernel::TerminalSchemaObservationQuarantine,
        cdf_engine::PhysicalObservationEvidence,
    )>,
    duplicate_scan_observation: bool,
) {
    let schema = sample_batch(vec![], vec![]).schema();
    let mut plan = artifact_expression_plan();
    if stale {
        plan.validation_program
            .compiled_expression_plan
            .as_mut()
            .unwrap()
            .native_filter_lowering_version = "stale-test-version".to_owned();
    }
    if duplicate_scan_observation {
        let mut duplicate = plan.scan.inline_partitions().unwrap()[0].clone();
        duplicate.partition_id = PartitionId::new("artifact-fixture-duplicate").unwrap();
        duplicate.scope = ScopeKey::Partition {
            partition_id: duplicate.partition_id.clone(),
        };
        duplicate.metadata.insert(
            cdf_kernel::PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
            "artifact-fixture".to_owned(),
        );
        plan.scan.inline_partitions_mut().unwrap().push(duplicate);
    }
    builder
        .write_json_artifact("plan/validation-program.json", &plan.validation_program)
        .unwrap();
    builder
        .write_json_artifact(
            cdf_package_contract::SCHEMA_ADMISSION_PROGRAM_FILE,
            &plan.schema_admission_program,
        )
        .unwrap();
    builder
        .write_json_artifact("plan/scan.json", &plan.scan)
        .unwrap();
    builder
        .write_json_artifact(
            "plan/schema-admission.json",
            &plan.compiled_schema_admission,
        )
        .unwrap();
    let physical_schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap();
    let coercion_plan = plan
        .compiled_schema_admission
        .instantiate(schema.as_ref(), &physical_schema_hash)
        .unwrap();
    if write_stream_evidence {
        let (physical_observation_catalog, observations) = if quarantine.is_some() {
            (BTreeMap::new(), Vec::new())
        } else {
            let physical_observation =
                cdf_engine::PhysicalObservationEvidence::arrow_schema(schema.as_ref()).unwrap();
            let physical_observation_hash = physical_observation.identity_hash().unwrap();
            (
                BTreeMap::from([(physical_observation_hash.to_string(), physical_observation)]),
                vec![
                    StreamAdmissionObservationEvidence::new(
                        "artifact-fixture",
                        physical_observation_hash,
                        coercion_plan,
                        cdf_engine::StreamAdmissionCompletion::Complete {
                            source_position: position(3),
                            partition_binding: artifact_fixture_partition_binding(),
                        },
                    )
                    .unwrap(),
                ],
            )
        };
        builder
            .write_json_artifact(
                "schema/stream-admission-evidence.json",
                &CompiledStreamAdmissionEvidence::new(
                    &plan.compiled_schema_admission,
                    physical_observation_catalog,
                    observations,
                )
                .unwrap(),
            )
            .unwrap();
    }
    if let Some((quarantine, physical_observation)) = quarantine {
        let physical_observation_hash = physical_observation.identity_hash().unwrap();
        builder
            .write_json_artifact(
                "quarantine/schema-admission-evidence.json",
                &cdf_engine::CompiledSchemaQuarantineEvidence::new(
                    &plan.compiled_schema_admission,
                    BTreeMap::from([(physical_observation_hash.to_string(), physical_observation)]),
                    vec![
                        cdf_engine::SchemaQuarantineObservationEvidence::new(
                            quarantine,
                            physical_observation_hash,
                        )
                        .unwrap(),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
    }
}

pub(super) fn artifact_expression_plan() -> EnginePlan {
    let schema = sample_batch(vec![], vec![]).schema();
    let mut program = compile_validation_program(
        &ContractPolicy::evolve(),
        &ObservedSchema::from_arrow(schema.as_ref()),
    )
    .unwrap();
    program.row_rules.clear();
    program.transforms.clear();
    let resource = ArtifactPlanResource::new(Arc::clone(&schema));
    Planner::new()
        .plan_tier_a(
            &resource,
            EnginePlanInput {
                request: ScanRequest {
                    resource_id: ResourceId::new("orders").unwrap(),
                    projection: None,
                    filters: Vec::new(),
                    limit: None,
                    order_by: Vec::new(),
                    scope: ScopeKey::Resource,
                },
                validation_program: program,
                execution_extent: ExecutionExtent::bounded(),
                segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
                package_id: "artifact-test-package".to_owned(),
                relational_expression_plan: None,
                committed_frontier: None,
            },
        )
        .unwrap()
}

pub(super) fn artifact_fixture_partition_binding() -> cdf_kernel::SchemaObservationBinding {
    let plan = artifact_expression_plan();
    cdf_kernel::partition_schema_observation_binding(&plan.scan.inline_partitions().unwrap()[0])
        .unwrap()
}

pub(super) struct ArtifactPlanResource {
    pub(super) descriptor: ResourceDescriptor,
    pub(super) schema: Arc<Schema>,
}

impl ArtifactPlanResource {
    pub(super) fn new(schema: Arc<Schema>) -> Self {
        let schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap();
        Self {
            descriptor: ResourceDescriptor {
                resource_id: ResourceId::new("orders").unwrap(),
                schema_source: SchemaSource::Declared {
                    schema_hash,
                    source: "artifact-fixture".to_owned(),
                },
                primary_key: Vec::new(),
                merge_key: Vec::new(),
                cursor: None,
                write_disposition: WriteDisposition::Append,
                deduplication: None,
                contract: None,
                state_scope: ScopeKey::Resource,
                freshness: None,
                trust_level: TrustLevel::Experimental,
            },
            schema,
        }
    }
}

impl ResourceStream for ArtifactPlanResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<cdf_kernel::PartitionPlan>> {
        let partition_id = PartitionId::new("artifact-fixture")?;
        Ok(vec![cdf_kernel::PartitionPlan {
            partition_id: partition_id.clone(),
            scope: ScopeKey::Partition { partition_id },
            planned_position: None,
            start_position: None,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::new(),
        }])
    }

    fn open(&self, _partition: cdf_kernel::PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
            Err(CdfError::internal("artifact fixture has no payload"))
        }))
    }
}

pub(super) fn write_state_commit_artifacts(
    builder: &PackageBuilder,
    segment: &SegmentEntry,
    disposition: WriteDisposition,
    checkpoint_id: &str,
    late_data_carryover: Vec<LateDataCarryoverRef>,
) {
    let scope = scope();
    let output_position = position(3);
    let segments = vec![StateSegment {
        segment_id: segment.segment_id.clone(),
        scope: scope.clone(),
        output_position: output_position.clone(),
        row_count: segment.row_count,
        byte_count: segment.byte_count,
    }];
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new(checkpoint_id).unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position,
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover,
        source_continuation: None,
        schema_hash: SchemaHash::new(SCHEMA_HASH).unwrap(),
        segments,
    };
    let processed = ProcessedObservationPosition::new(
        "artifact-fixture",
        ProcessedObservationOutcome::Admitted,
        state_delta.output_position.clone(),
    )
    .unwrap();
    builder
        .write_json_artifact(
            PROCESSED_OBSERVATIONS_FILE,
            &ProcessedObservationEvidenceArtifact::new(
                None,
                disposition.clone(),
                vec![processed],
                state_delta.output_position.clone(),
            )
            .unwrap(),
        )
        .unwrap();
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders").unwrap(),
        disposition,
        Vec::new(),
        SchemaHash::new(SCHEMA_HASH).unwrap(),
    );
    builder.write_input_checkpoint_artifact(&None).unwrap();
    builder
        .write_state_delta_preimage_artifact(&state_delta)
        .unwrap();
    builder
        .write_commit_plan_preimage_artifact(&commit_plan)
        .unwrap();
}

pub(super) fn scope() -> ScopeKey {
    ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    }
}

pub(super) fn position(value: i64) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(value),
    })
}

pub(super) fn delta(manifest: &PackageManifest, checkpoint_id: &str) -> StateDelta {
    let scope = scope();
    let output_position = position(3);
    StateDelta {
        checkpoint_id: CheckpointId::new(checkpoint_id).unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope: scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: output_position.clone(),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        schema_hash: SchemaHash::new(SCHEMA_HASH).unwrap(),
        segments: manifest
            .identity
            .segments
            .iter()
            .map(|segment| StateSegment {
                segment_id: segment.segment_id.clone(),
                scope: scope.clone(),
                output_position: output_position.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect(),
    }
}

pub(super) fn destination(path: &Path) -> DuckDbDestination {
    DuckDbDestination::new(path).unwrap()
}

pub(super) fn resolved_duckdb_destination(
    destination: &DuckDbDestination,
    target: TargetName,
) -> ResolvedProjectDestination {
    let execution = test_execution_services();
    ResolvedProjectDestination::new(Box::new(destination.clone()), target)
        .with_bound_execution_services(execution)
        .unwrap()
}

#[derive(Clone)]
pub(super) struct MockDestination {
    pub(super) sheet: DestinationSheet,
    pub(super) receipts: Arc<Mutex<Vec<Receipt>>>,
    pub(super) writes: Arc<Mutex<Vec<SegmentId>>>,
    pub(super) aborts: Arc<AtomicU64>,
    pub(super) stage_threads: Arc<Mutex<Vec<std::thread::ThreadId>>>,
    pub(super) fail_begin: Arc<AtomicBool>,
}

impl MockDestination {
    pub(super) fn new() -> Self {
        Self {
            sheet: DestinationSheet {
                destination: DestinationId::new("mock").unwrap(),
                supported_dispositions: vec![WriteDisposition::Append],
                transactions: TransactionSupport::AtomicPackage,
                idempotency: IdempotencySupport::PackageToken,
                type_mappings: vec![
                    TypeMapping {
                        arrow_type: "Int64".to_owned(),
                        destination_type: "BIGINT".to_owned(),
                        fidelity: TypeMappingFidelity::Lossless,
                    },
                    TypeMapping {
                        arrow_type: "Utf8".to_owned(),
                        destination_type: "TEXT".to_owned(),
                        fidelity: TypeMappingFidelity::Lossless,
                    },
                ],
                identifier_rules: IdentifierRules {
                    normalizer: "namecase-v1".to_owned(),
                    max_length: Some(63),
                    allowed_pattern: None,
                },
                migration_support: CapabilitySupport::Supported,
                quarantine_tables: CapabilitySupport::Unsupported,
                concurrency: ConcurrencyLimit {
                    max_writers: Some(1),
                },
            },
            receipts: Arc::new(Mutex::new(Vec::new())),
            writes: Arc::new(Mutex::new(Vec::new())),
            aborts: Arc::new(AtomicU64::new(0)),
            stage_threads: Arc::new(Mutex::new(Vec::new())),
            fail_begin: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) fn with_begin_failure(self) -> Self {
        self.fail_begin.store(true, Ordering::SeqCst);
        self
    }

    pub(super) fn write_count(&self) -> usize {
        self.writes.lock().unwrap().len()
    }

    pub(super) fn abort_count(&self) -> u64 {
        self.aborts.load(Ordering::SeqCst)
    }

    pub(super) fn stage_threads(&self) -> Vec<std::thread::ThreadId> {
        self.stage_threads.lock().unwrap().clone()
    }
}

impl DestinationProtocol for MockDestination {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        Ok(CommitPlan {
            plan_id: PlanId::new(format!(
                "mock-plan:{}:{}",
                request.target.as_str(),
                request.idempotency_token.as_str()
            ))?,
            target: request.target.clone(),
            disposition: request.disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: vec![MigrationRecord {
                migration_id: "mock.migration".to_owned(),
                description: "mock migration".to_owned(),
            }],
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
        })
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        let verified = self
            .receipts
            .lock()
            .unwrap()
            .iter()
            .any(|stored| stored == receipt);
        Ok(ReceiptVerification {
            verified,
            receipt_id: receipt.receipt_id.clone(),
            reason: (!verified).then(|| "mock receipt not recorded".to_owned()),
        })
    }
}

impl MockDestination {
    pub(super) fn begin(
        &self,
        request: DestinationCommitRequest,
        plan: CommitPlan,
    ) -> Result<Box<dyn CommitSession + '_>> {
        if self.fail_begin.load(Ordering::SeqCst) {
            return Err(CdfError::destination("injected primary replay failure"));
        }
        Ok(Box::new(MockCommitSession {
            destination: self,
            request,
            plan,
            migrations_applied: false,
            acks: Vec::new(),
        }))
    }
}

pub(super) struct MockCommitSession<'a> {
    pub(super) destination: &'a MockDestination,
    pub(super) request: DestinationCommitRequest,
    pub(super) plan: CommitPlan,
    pub(super) migrations_applied: bool,
    pub(super) acks: Vec<SegmentAck>,
}

impl CommitSession for MockCommitSession<'_> {
    fn apply_migrations(&mut self) -> Result<()> {
        self.migrations_applied = true;
        Ok(())
    }

    fn write_segments(
        &mut self,
        segments: cdf_kernel::CommitSegmentIterator,
    ) -> Result<Vec<SegmentAck>> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "mock destination migrations must be applied before writing",
            ));
        }
        let mut acknowledgements = Vec::new();
        for segment in segments {
            let segment = segment?;
            let expected = self
                .request
                .segments
                .iter()
                .find(|state| state.segment_id == segment.state.segment_id)
                .ok_or_else(|| CdfError::data("unexpected mock segment"))?;
            if expected.row_count != segment.state.row_count
                || expected.byte_count != segment.state.byte_count
            {
                return Err(CdfError::data("mock segment state mismatch"));
            }
            let ack = SegmentAck {
                segment_id: expected.segment_id.clone(),
                row_count: expected.row_count,
                byte_count: expected.byte_count,
            };
            self.destination
                .writes
                .lock()
                .unwrap()
                .push(ack.segment_id.clone());
            self.acks.push(ack.clone());
            acknowledgements.push(ack);
        }
        Ok(acknowledgements)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        if self.acks.len() != self.request.segments.len() {
            return Err(CdfError::destination(
                "mock destination did not receive every segment",
            ));
        }
        let rows_written = self.acks.iter().map(|ack| ack.row_count).sum();
        let mut parameters = BTreeMap::new();
        parameters.insert("target".to_owned(), self.request.target.as_str().to_owned());
        parameters.insert(
            "package_hash".to_owned(),
            self.request.package_hash.as_str().to_owned(),
        );
        let receipt = Receipt {
            receipt_id: ReceiptId::new(format!(
                "mock-receipt:{}",
                self.request.package_hash.as_str()
            ))?,
            destination: self.destination.sheet.destination.clone(),
            target: self.request.target.clone(),
            package_hash: self.request.package_hash.clone(),
            segment_acks: self.acks,
            disposition: self.request.disposition.clone(),
            idempotency_token: self.request.idempotency_token.clone(),
            transaction: None,
            counts: CommitCounts {
                rows_written,
                rows_inserted: Some(rows_written),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: SchemaHash::new(SCHEMA_HASH).unwrap(),
            migrations: self.plan.migrations.clone(),
            committed_at_ms: 1_700_000_000_000,
            verify: VerifyClause {
                kind: "mock".to_owned(),
                statement: "mock durable receipt".to_owned(),
                parameters,
            },
        };
        self.destination
            .receipts
            .lock()
            .unwrap()
            .push(receipt.clone());
        Ok(receipt)
    }

    fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct MockDestinationCounters {
    pub(super) resolves: Arc<AtomicU64>,
    pub(super) prepares: Arc<AtomicU64>,
    pub(super) binds: Arc<AtomicU64>,
}

impl MockDestinationCounters {
    pub(super) fn new() -> Self {
        Self {
            resolves: Arc::new(AtomicU64::new(0)),
            prepares: Arc::new(AtomicU64::new(0)),
            binds: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(super) fn resolve_count(&self) -> usize {
        self.resolves.load(Ordering::SeqCst) as usize
    }

    pub(super) fn prepare_count(&self) -> usize {
        self.prepares.load(Ordering::SeqCst) as usize
    }

    pub(super) fn bind_count(&self) -> usize {
        self.binds.load(Ordering::SeqCst) as usize
    }
}

pub(super) struct MockProjectDestinationRuntime {
    pub(super) destination: MockDestination,
    pub(super) counters: MockDestinationCounters,
    pub(super) sheet_drift_on_plan: bool,
}

pub(super) fn mock_bulk_path(
    path_id: &str,
    ingress_mode: cdf_runtime::DestinationIngressMode,
    writer_model: cdf_runtime::DestinationWriterModel,
    blocking_lane: Option<&str>,
) -> cdf_runtime::BulkPathDescriptor {
    cdf_runtime::BulkPathDescriptor {
        path_id: path_id.to_owned(),
        version: 1,
        ingress_mode,
        writer_model,
        ordering: cdf_runtime::BulkOrdering::ManifestOrder,
        rows: cdf_runtime::BulkSizeRange {
            minimum: 1,
            preferred: 8_192,
            maximum: 65_536,
        },
        bytes: cdf_runtime::BulkSizeRange {
            minimum: 1,
            preferred: 1024 * 1024,
            maximum: 64 * 1024 * 1024,
        },
        max_useful_writers: 1,
        blocking_lane: blocking_lane.map(str::to_owned),
        native_internal_parallelism: 1,
        external_staging: false,
        fallback: cdf_runtime::BulkFallbackMode::Forbidden,
        schema_preflight_version: "mock-v1".to_owned(),
        measured_evidence_version: Some("mock-v1".to_owned()),
    }
}

impl MockProjectDestinationRuntime {
    pub(super) fn with_destination(
        destination: MockDestination,
        counters: MockDestinationCounters,
    ) -> Self {
        Self {
            destination,
            counters,
            sheet_drift_on_plan: false,
        }
    }

    pub(super) fn with_sheet_drift(destination: MockDestination) -> Self {
        Self {
            destination,
            counters: MockDestinationCounters::new(),
            sheet_drift_on_plan: true,
        }
    }
}

impl ProjectDestinationRuntime for MockProjectDestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.destination
    }

    fn ingress(&mut self) -> cdf_runtime::DestinationIngress<'_> {
        cdf_runtime::DestinationIngress::FinalizedPackage(self)
    }

    fn describe(&self) -> ProjectDestinationDescription {
        ProjectDestinationDescription::new(
            self.destination.sheet.destination.clone(),
            &["mock"],
            "mock",
        )
    }

    fn runtime_capabilities(&self) -> cdf_runtime::DestinationRuntimeCapabilities {
        let path = mock_bulk_path(
            "mock-finalized",
            cdf_runtime::DestinationIngressMode::FinalizedPackageOnly,
            cdf_runtime::DestinationWriterModel::SingleWriter,
            None,
        );
        cdf_runtime::DestinationRuntimeCapabilities {
            commit_payload_mode: cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming,
            max_in_flight_segments: Some(1),
            max_in_flight_bytes: Some(64 * 1024 * 1024),
            bulk_paths: vec![path],
            bulk_path: Some("mock-finalized".to_owned()),
            bulk_evidence_version: Some("mock-v1".to_owned()),
            ..Default::default()
        }
    }

    fn validate_run_preflight(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        _schema_hash: &SchemaHash,
    ) -> Result<()> {
        Ok(())
    }

    fn plan_resource_commit(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let plan = self.destination.plan_commit(&inputs.destination_commit)?;
        let mut sheet = self.destination.sheet.clone();
        if self.sheet_drift_on_plan {
            sheet.concurrency.max_writers = Some(2);
        }
        Ok(DestinationCommitPlanningOutcome::new(sheet, plan))
    }

    fn secret_redaction(&self) -> Option<&str> {
        Some("quasar-secret")
    }
}

impl cdf_runtime::FinalizedPackageIngress for MockProjectDestinationRuntime {
    fn prepare_package_commit(
        &mut self,
        inputs: &PackageReplayInputs,
        context: &crate::DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        self.counters.prepares.fetch_add(1, Ordering::SeqCst);
        let plan = self.destination.plan_commit(&inputs.destination_commit)?;
        PreparedDestinationCommit::from_verified_inputs(
            inputs,
            plan,
            context.bulk_path.clone(),
            DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
        )
    }

    fn begin_prepared_commit(
        &mut self,
        prepared: &mut PreparedDestinationCommit,
    ) -> Result<Box<dyn CommitSession + '_>> {
        if prepared.has_pending_context() {
            return Err(CdfError::internal(
                "mock destination received unexpected pending context",
            ));
        }
        self.counters.binds.fetch_add(1, Ordering::SeqCst);
        self.destination
            .begin(prepared.commit().clone(), prepared.plan().clone())
    }
}

pub(super) fn package_status(package_dir: &Path) -> PackageStatus {
    PackageReader::open(package_dir)
        .unwrap()
        .manifest()
        .lifecycle
        .status
        .clone()
}

pub(super) fn package_receipts(package_dir: &Path) -> Vec<Receipt> {
    let reader = PackageReader::open(package_dir).unwrap();
    let mut receipts = Vec::new();
    reader
        .for_each_receipt(&mut |receipt| {
            receipts.push(receipt);
            Ok(())
        })
        .unwrap();
    receipts
}

pub(super) fn live_file_resource(root: &Path) -> OwnedTestResource {
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("data/events.ndjson"),
        "{\"id\":1,\"updated_at\":1783296000000000}\n\
         {\"id\":2,\"updated_at\":1783296060000000}\n",
    )
    .unwrap();
    compile_test_file_resource(root, LIVE_FILE_RESOURCE)
}

pub(super) fn simple_file_resource(root: &Path, document: &str) -> OwnedTestResource {
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("data/events.ndjson"),
        "{\"id\":1,\"name\":\"ada\"}\n\
         {\"id\":2,\"name\":\"grace\"}\n",
    )
    .unwrap();
    compile_test_file_resource(root, document)
}

pub(super) fn multi_file_resource(root: &Path) -> OwnedTestResource {
    multi_file_resource_with_document(root, MULTI_FILE_RESOURCE_APPEND)
}

pub(super) fn multi_file_resource_with_document(root: &Path, document: &str) -> OwnedTestResource {
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("data/events-a.ndjson"),
        "{\"id\":1,\"name\":\"ada\"}\n",
    )
    .unwrap();
    fs::write(
        root.join("data/events-b.ndjson"),
        "{\"id\":2,\"name\":\"grace\"}\n",
    )
    .unwrap();
    compile_test_file_resource(root, document)
}

pub(super) fn compiled_test_source_plan(
    resource: &dyn QueryableResource,
) -> cdf_runtime::CompiledSourcePlan {
    cdf_runtime::CompiledSourcePlan::new(
        cdf_runtime::SourceDriverDescriptor {
            driver_id: cdf_runtime::SourceDriverId::new("project_test").unwrap(),
            driver_version: "1.0.0".to_owned(),
            option_schema_hash: cdf_runtime::artifact_hash(&serde_json::json!({})).unwrap(),
            kinds: vec!["project_test".to_owned()],
            schemes: Vec::new(),
        },
        resource.capabilities().clone(),
        cdf_runtime::SourceExecutionCapabilities {
            minimum_poll_bytes: 1,
            maximum_poll_bytes: 1024,
            minimum_decode_bytes: 1,
            maximum_decode_bytes: 4096,
            maximum_emitted_batch_bytes: 4096,
            maximum_concurrency: 2,
            useful_concurrency: 2,
            executor_class: cdf_runtime::SourceExecutorClass::Io,
            blocking_lane: None,
            pausable: true,
            spillable: false,
            idempotent_reads: true,
            reopenable: true,
            resumable: false,
            speculative_safe: false,
            retry_granularity: cdf_runtime::SourceRetryGranularity::None,
            retryable_errors: Vec::new(),
            retry_policy: None,
            attestation: cdf_runtime::SourceAttestationStrength::None,
            rate_limit: None,
            quota_authority: None,
            canonical_order: true,
            bounded: true,
            batch_memory: if resource.capabilities().incremental == IncrementalShape::File {
                cdf_runtime::SourceBatchMemoryContract::Preaccounted
            } else {
                cdf_runtime::SourceBatchMemoryContract::FrontierReserved
            },
            telemetry_version: "project-test-v1".to_owned(),
        },
        cdf_runtime::CompiledSourcePlanInput {
            descriptor: resource.descriptor().clone(),
            schema: resource.schema().as_ref().clone(),
            type_policy_allowances: resource.type_policy_allowances(),
            effective_schema_runtime: resource.effective_schema_runtime().cloned(),
            baseline_observation_schema_catalog: resource
                .baseline_observation_schema_catalog()
                .to_vec(),
            redacted_options: serde_json::json!({}),
            physical_plan: serde_json::json!({"partitions": 2}),
        },
    )
    .unwrap()
}

pub(super) fn rest_compile_registry() -> cdf_runtime::SourceRegistry {
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry
        .register(RestSourceDriver::new(|| Ok(Box::new(RecordingTransport::default()))).unwrap())
        .unwrap();
    registry
}

pub(super) fn postgres_compile_registry() -> cdf_runtime::SourceRegistry {
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry
        .register(cdf_source_postgres::PostgresSourceDriver::new().unwrap())
        .unwrap();
    registry
}

pub(super) fn resolve_rest_resource(
    compiled: &cdf_declarative::CompiledResource,
    transport: RecordingTransport,
    secret_provider: Arc<dyn SecretProvider + Send + Sync>,
    execution: &cdf_runtime::ExecutionServices,
) -> OwnedTestResource {
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry
        .register(RestSourceDriver::new(move || Ok(Box::new(transport.clone()))).unwrap())
        .unwrap();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        Path::new("."),
        secret_provider,
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    OwnedTestResource {
        source_plan: compiled.source_plan().clone(),
        inner: registry
            .resolve(compiled.source_plan(), &resolution)
            .unwrap(),
    }
}

pub(super) fn resolve_postgres_resource(
    compiled: &cdf_declarative::CompiledResource,
    database_url: &str,
    execution: &cdf_runtime::ExecutionServices,
) -> OwnedTestResource {
    let registry = postgres_compile_registry();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        Path::new("."),
        Arc::new(StaticSecretProvider::new([(
            "secret://env/POSTGRES_URL",
            database_url,
        )])),
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    OwnedTestResource {
        source_plan: compiled.source_plan().clone(),
        inner: registry
            .resolve(compiled.source_plan(), &resolution)
            .unwrap(),
    }
}

pub(super) fn postgres_runtime_resource(table: &str) -> cdf_declarative::CompiledResource {
    let document = cdf_declarative::parse_toml(&POSTGRES_RUNTIME_RESOURCE.replace(
        r#"table = "public.orders""#,
        &format!(r#"table = "{table}""#),
    ))
    .unwrap();
    cdf_declarative::compile_document(&postgres_compile_registry(), &document)
        .unwrap()
        .remove(0)
}

pub(super) fn live_plan(resource: &dyn QueryableResource, package_id: &str) -> EnginePlan {
    let destination = crate::test_destinations::duckdb(
        "/tmp/cdf-plan-policy-only.duckdb",
        TargetName::new("events").unwrap(),
    )
    .unwrap();
    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = identifier_policy;
    live_plan_for_queryable_with_exact_policy(resource, package_id, &policy)
}

pub(super) fn live_plan_for_queryable(
    resource: &dyn QueryableResource,
    package_id: &str,
) -> EnginePlan {
    let destination = crate::test_destinations::duckdb(
        "/tmp/cdf-plan-policy-only.duckdb",
        TargetName::new("events").unwrap(),
    )
    .unwrap();
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = destination.column_identifier_policy().unwrap().unwrap();
    let validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
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
                segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
                package_id: package_id.to_owned(),
                relational_expression_plan: None,
                committed_frontier: None,
            },
        )
        .unwrap()
}

pub(super) fn live_plan_for_queryable_with_exact_policy(
    resource: &dyn QueryableResource,
    package_id: &str,
    policy: &ContractPolicy,
) -> EnginePlan {
    let validation_program = compile_validation_program(
        policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
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
                segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
                package_id: package_id.to_owned(),
                relational_expression_plan: None,
                committed_frontier: None,
            },
        )
        .unwrap()
}

pub(super) fn live_plan_with_policy(
    resource: &dyn QueryableResource,
    package_id: &str,
    policy: &ContractPolicy,
) -> EnginePlan {
    let destination = crate::test_destinations::duckdb(
        "/tmp/cdf-plan-policy-only.duckdb",
        TargetName::new("events").unwrap(),
    )
    .unwrap();
    let mut policy = policy.clone();
    policy.normalization.identifier = destination.column_identifier_policy().unwrap().unwrap();
    live_plan_for_queryable_with_exact_policy(resource, package_id, &policy)
}

pub(super) fn live_plan_with_exact_policy(
    resource: &dyn QueryableResource,
    package_id: &str,
    policy: &ContractPolicy,
) -> EnginePlan {
    live_plan_for_queryable_with_exact_policy(resource, package_id, policy)
}

pub(super) fn project_run_request<'a>(
    resource: &'a dyn QueryableResource,
    package_id: &str,
    package_root: &Path,
    duckdb_path: &Path,
    state_path: &Path,
    run_id: &str,
) -> ProjectRunRequest<'a> {
    ProjectRunRequest {
        resource: ProjectRunSource::new(resource),
        plan: live_plan_for_queryable(resource, package_id),
        package_root: package_root.to_path_buf(),
        state_store_path: state_path.to_path_buf(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        destination: crate::test_destinations::duckdb(
            duckdb_path,
            TargetName::new("events").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new(run_id).unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }
}

pub(super) fn parquet_project_run_request<'a>(
    resource: &'a dyn QueryableResource,
    package_id: &str,
    package_root: &Path,
    parquet_root: &Path,
    state_path: &Path,
    run_id: &str,
) -> ProjectRunRequest<'a> {
    let destination = crate::test_destinations::parquet_filesystem(
        parquet_root,
        TargetName::new("events").unwrap(),
    )
    .unwrap();
    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = identifier_policy;
    ProjectRunRequest {
        resource: ProjectRunSource::new(resource),
        plan: live_plan_for_queryable_with_exact_policy(resource, package_id, &policy),
        package_root: package_root.to_path_buf(),
        state_store_path: state_path.to_path_buf(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        destination,
        run_id: Some(RunId::new(run_id).unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }
}

pub(super) fn postgres_project_run_request<'a>(
    resource: &'a dyn QueryableResource,
    package_id: &str,
    package_root: &Path,
    database_url: &str,
    target: PostgresTarget,
    state_path: &Path,
    run_id: &str,
) -> ProjectRunRequest<'a> {
    let destination =
        crate::test_destinations::postgres(database_url.to_owned(), target, None).unwrap();
    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = identifier_policy;
    ProjectRunRequest {
        resource: ProjectRunSource::new(resource),
        plan: live_plan_for_queryable_with_exact_policy(resource, package_id, &policy),
        package_root: package_root.to_path_buf(),
        state_store_path: state_path.to_path_buf(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        destination,
        run_id: Some(RunId::new(run_id).unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }
}

pub(super) struct RecordingResponse {
    pub(super) response: HttpResponse,
    pub(super) body: Vec<u8>,
}

#[derive(Clone, Default)]
pub(super) struct RecordingTransport {
    pub(super) state: Arc<Mutex<RecordingTransportState>>,
}

#[derive(Default)]
pub(super) struct RecordingTransportState {
    pub(super) requests: Vec<HttpRequest>,
    pub(super) responses: VecDeque<RecordingResponse>,
}

impl RecordingTransport {
    pub(super) fn new<I>(responses: I) -> Self
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

    pub(super) fn requests(&self) -> Vec<HttpRequest> {
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

pub(super) struct StaticSecretProvider {
    pub(super) values: BTreeMap<String, String>,
}

impl StaticSecretProvider {
    pub(super) fn new<I, K, V>(values: I) -> Self
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

pub(super) struct LivePostgres {
    pub(super) url: String,
    pub(super) schema: String,
    pub(super) _server: Option<LocalPostgres>,
}

pub(super) struct LocalPostgres {
    pub(super) data_dir: TempDir,
    pub(super) _socket_dir: TempDir,
    pub(super) pg_ctl: PathBuf,
}

impl LivePostgres {
    pub(super) fn start() -> Option<Self> {
        let (url, server) = match env::var("TEST_DATABASE_URL") {
            Ok(url) if !url.trim().is_empty() => (url, None),
            _ => {
                let Some(server) = LocalPostgres::start() else {
                    eprintln!(
                        "skipping live Postgres test: set TEST_DATABASE_URL or install postgres/initdb/pg_ctl"
                    );
                    return None;
                };
                (server.url(), Some(server))
            }
        };
        let schema = format!(
            "cdf_project_live_{}_{}",
            std::process::id(),
            LIVE_POSTGRES_SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let mut client = Client::connect(&url, NoTls).unwrap();
        client
            .batch_execute(&format!("CREATE SCHEMA {}", quote_identifier(&schema)))
            .unwrap();
        Some(Self {
            url,
            schema,
            _server: server,
        })
    }

    pub(super) fn client(&self) -> Client {
        Client::connect(&self.url, NoTls).unwrap()
    }

    pub(super) fn table(&self, table: &str) -> String {
        format!("{}.{}", self.schema, table)
    }
}

impl Drop for LivePostgres {
    fn drop(&mut self) {
        if let Ok(mut client) = Client::connect(&self.url, NoTls) {
            let _ = client.batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_identifier(&self.schema)
            ));
        }
    }
}

impl LocalPostgres {
    pub(super) fn start() -> Option<Self> {
        let _guard = LOCAL_POSTGRES_START.lock().unwrap();
        let initdb = find_binary("initdb")?;
        let pg_ctl = find_binary("pg_ctl")?;
        let data_dir = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let port = free_port();

        let init_status = Command::new(&initdb)
            .args(["-D", data_dir.path().to_str().unwrap()])
            .args(["-A", "trust"])
            .args(["-U", "cdf"])
            .arg("--no-sync")
            .status()
            .unwrap();
        assert!(init_status.success(), "initdb failed");

        let options = format!("-h 127.0.0.1 -p {port} -k {}", socket_dir.path().display());
        let log_path = data_dir.path().join("postgres.log");
        let start_status = Command::new(&pg_ctl)
            .args(["-D", data_dir.path().to_str().unwrap()])
            .args(["-l", log_path.to_str().unwrap()])
            .args(["-o", &options])
            .args(["-w", "start"])
            .status()
            .unwrap();
        assert!(start_status.success(), "pg_ctl start failed");

        Some(Self {
            data_dir,
            _socket_dir: socket_dir,
            pg_ctl,
        })
    }

    pub(super) fn url(&self) -> String {
        let port = fs::read_to_string(self.data_dir.path().join("postmaster.pid"))
            .unwrap()
            .lines()
            .nth(3)
            .unwrap()
            .to_owned();
        format!("postgresql://cdf@127.0.0.1:{port}/postgres")
    }
}

impl Drop for LocalPostgres {
    fn drop(&mut self) {
        let _ = Command::new(&self.pg_ctl)
            .args(["-D", self.data_dir.path().to_str().unwrap()])
            .args(["-m", "fast"])
            .args(["-w", "stop"])
            .status();
    }
}

pub(super) fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(super) fn find_binary(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|path| path.join(name))
            .find(|candidate| candidate.is_file())
    })
}

pub(super) fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
