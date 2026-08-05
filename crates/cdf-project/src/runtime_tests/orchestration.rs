use super::{
    Arc, ArrayRef, AtomicBool, AtomicU64, Attributes, BTreeMap, BackfillPlanRequest,
    BackpressureSupport, CapabilitySupport, CdfError, Checkpoint, CheckpointId, CheckpointStatus,
    CheckpointStore, CompositePosition, CursorPosition, CursorValue, DataType, DeliveryGuarantee,
    DestinationId, DuckDbDestination, EngineRunOutput, EngineRunOutputWithSegmentPositions,
    EngineSegmentPosition, EstimateSupport, Event, ExecutionExtent, ExecutionProfile, Field,
    FileManifest, FileManifestRunSummary, FilePosition, FilterCapabilities, Id, IncrementalShape,
    Int64Array, LineageSummary, Metadata, Mutex, Ordering, PackageBuilder, PackageHash,
    PackageReader, PackageStatus, PageToken, PartitionId, Path, PathBuf, PipelineId, PlanId,
    ProcessedObservationOutcome, ProcessedObservationPosition, ProjectRunNoOpReason,
    ProjectRunOutcome, ProjectRunReport, ProjectRunRequest, ProjectRunSource, QueryableResource,
    Receipt, Record, RecordBatch, ReplaySupport, ResourceCapabilities, ResourceDescriptor,
    ResourceId, ResourceStream, Result, RunEvent, RunEventDetails, RunEventKind, RunEventSink,
    RunEventSinkResult, RunEventValue, RunId, RunPhase, RunPhaseMetric, RunPhaseStatus,
    RunTelemetryConfig, ScanRequest, Schema, SchemaHash, SchemaSource, ScopeKey, SecretReference,
    SegmentId, SourcePosition, SqliteCheckpointStore, SqliteRunLedger, StateDelta,
    StateDeltaTestRequest, Subscriber, TargetName, TracingField, TracingRunEventSink, TrustLevel,
    Visit, WriteDisposition, backfill_pipeline_id, fmt, fs, negotiate_scan_plan, plan_backfill,
    postgres_log_position, state_delta_from_run,
    support::{
        BackfillMockResource, BoundTestResource, MULTI_FILE_RESOURCE_APPEND, OwnedTestResource,
        RecordingTransport, SCHEMA_HASH, StaticSecretProvider, build_package_with_carryover,
        compile_test_file_resource, compiled_test_source_plan, delta, destination,
        live_file_resource, live_plan, live_plan_for_queryable, multi_file_resource,
        multi_file_resource_with_document, package_id_name_rows, package_receipts, package_status,
        project_run_request, resolve_rest_resource, rest_compile_registry, run_project,
        run_project_fixture, run_project_outcome, sample_batch, test_execution_services,
    },
};

pub(super) async fn run_project_with_telemetry(
    request: ProjectRunRequest<'_>,
    telemetry: RunTelemetryConfig,
) -> Result<ProjectRunReport> {
    let services = test_execution_services();
    run_project_fixture(request, &services, telemetry).await
}

pub(super) const MULTI_FILE_RESOURCE_REPLACE: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events-*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "replace"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
] }
"#;

pub(super) fn cursor_position(field: &str, value: CursorValue) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: field.to_owned(),
        value,
    })
}

pub(super) struct CheckpointBoundReplayRetention {
    pub(super) state_path: PathBuf,
    pub(super) pipeline_id: PipelineId,
    pub(super) resource_id: ResourceId,
    pub(super) scope: ScopeKey,
    pub(super) committed: Mutex<Vec<SourcePosition>>,
}

impl cdf_kernel::SourceReplayRetention for CheckpointBoundReplayRetention {
    fn status(&self) -> Result<cdf_kernel::SourceReplayRetentionStatus> {
        Ok(cdf_kernel::SourceReplayRetentionStatus {
            maximum_bytes: 1024,
            maximum_age_milliseconds: 1_000,
            maximum_units: 16,
            retained_bytes: 0,
            retained_units: 0,
            committed_low_watermark: self.committed.lock().unwrap().last().cloned(),
        })
    }

    fn validate_checkpoint_frontier(&self, frontier: &SourcePosition) -> Result<()> {
        frontier.validate()
    }

    fn reconcile_committed_frontier(&self, frontier: &SourcePosition) -> Result<()> {
        self.commit_checkpoint_frontier(frontier)
    }

    fn commit_checkpoint_frontier(&self, frontier: &SourcePosition) -> Result<()> {
        let store = SqliteCheckpointStore::open(&self.state_path)?;
        let head = store
            .head(&self.pipeline_id, &self.resource_id, &self.scope)?
            .ok_or_else(|| {
                CdfError::internal(
                    "replay retention advanced before the checkpoint head was committed",
                )
            })?;
        if &head.delta.output_position != frontier {
            return Err(CdfError::internal(
                "replay retention frontier differs from the committed checkpoint head",
            ));
        }
        let mut committed = self.committed.lock().unwrap();
        if committed.last() != Some(frontier) {
            committed.push(frontier.clone());
        }
        Ok(())
    }
}

impl QueryableResource for BoundTestResource<'_> {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<cdf_kernel::ScanPlan> {
        self.inner.negotiate(request)
    }
}

pub(super) struct EmptyDrainResource {
    pub(super) inner: BackfillMockResource,
}

impl EmptyDrainResource {
    pub(super) fn new() -> Self {
        Self {
            inner: BackfillMockResource::cursor(),
        }
    }
}

impl ResourceStream for EmptyDrainResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.inner.descriptor()
    }

    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<cdf_kernel::PartitionPlan>> {
        self.inner.plan_partitions(request)
    }

    fn rebind_scan_for_resume(
        &self,
        scan: cdf_kernel::ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<cdf_kernel::ScanPlan> {
        self.inner.rebind_scan_for_resume(scan, committed_frontier)
    }

    fn open(&self, _partition: cdf_kernel::PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
            let batches = futures_util::stream::empty::<Result<cdf_kernel::Batch>>();
            Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                batches,
            )))
        }))
    }
}

impl QueryableResource for EmptyDrainResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<cdf_kernel::ScanPlan> {
        self.inner.negotiate(request)
    }
}

pub(super) struct OneBatchThenEmptyDrainResource {
    pub(super) inner: BackfillMockResource,
    pub(super) open_count: AtomicU64,
}

impl OneBatchThenEmptyDrainResource {
    pub(super) fn new() -> Self {
        Self {
            inner: BackfillMockResource::cursor(),
            open_count: AtomicU64::new(0),
        }
    }
}

impl ResourceStream for OneBatchThenEmptyDrainResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.inner.descriptor()
    }

    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<cdf_kernel::PartitionPlan>> {
        self.inner.plan_partitions(request)
    }

    fn open(&self, partition: cdf_kernel::PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        let first_open = self.open_count.fetch_add(1, Ordering::SeqCst) == 0;
        let schema = self.schema();
        let resource_id = self.descriptor().resource_id.clone();
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            if !first_open {
                let batches = futures_util::stream::empty::<Result<cdf_kernel::Batch>>();
                return Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                    batches,
                )));
            }
            let batch = one_row_cursor_batch(
                "batch-one-then-empty",
                1,
                schema,
                resource_id,
                partition.partition_id,
            )?;
            let batches = futures_util::stream::once(async move { Ok(batch) });
            Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                batches,
            )))
        }))
    }
}

impl QueryableResource for OneBatchThenEmptyDrainResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<cdf_kernel::ScanPlan> {
        self.inner.negotiate(request)
    }
}

pub(super) struct OneBatchThenErrorOnceDrainResource {
    pub(super) inner: BackfillMockResource,
    pub(super) open_count: AtomicU64,
}

impl OneBatchThenErrorOnceDrainResource {
    pub(super) fn new() -> Self {
        Self {
            inner: BackfillMockResource::cursor(),
            open_count: AtomicU64::new(0),
        }
    }
}

impl ResourceStream for OneBatchThenErrorOnceDrainResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.inner.descriptor()
    }

    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<cdf_kernel::PartitionPlan>> {
        self.inner.plan_partitions(request)
    }

    fn open(&self, partition: cdf_kernel::PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        let first_open = self.open_count.fetch_add(1, Ordering::SeqCst) == 0;
        let schema = self.schema();
        let resource_id = self.descriptor().resource_id.clone();
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let batch = one_row_cursor_batch(
                if first_open {
                    "batch-before-error"
                } else {
                    "batch-after-retry"
                },
                1,
                schema,
                resource_id,
                partition.partition_id,
            )?;
            let mut batches = vec![Ok(batch)];
            if first_open {
                batches.push(Err(CdfError::internal(
                    "injected extraction failure after one durable segment",
                )));
            }
            Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                futures_util::stream::iter(batches),
            )))
        }))
    }
}

impl QueryableResource for OneBatchThenErrorOnceDrainResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<cdf_kernel::ScanPlan> {
        self.inner.negotiate(request)
    }
}

pub(super) struct DurableMultiPartitionDrainResource {
    pub(super) inner: BackfillMockResource,
    pub(super) fail_first_resume: AtomicBool,
    pub(super) opens: Mutex<Vec<(String, Option<SourcePosition>)>>,
}

impl DurableMultiPartitionDrainResource {
    pub(super) fn new() -> Self {
        Self {
            inner: BackfillMockResource::cursor(),
            fail_first_resume: AtomicBool::new(true),
            opens: Mutex::new(Vec::new()),
        }
    }
}

impl ResourceStream for DurableMultiPartitionDrainResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.inner.descriptor()
    }

    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<cdf_kernel::PartitionPlan>> {
        ["part-a", "part-b"]
            .into_iter()
            .map(|partition_id| {
                Ok(cdf_kernel::PartitionPlan {
                    partition_id: PartitionId::new(partition_id)?,
                    scope: request.scope.clone(),
                    planned_position: None,
                    start_position: None,
                    scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
                    retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
                    metadata: BTreeMap::new(),
                })
            })
            .collect()
    }

    fn open(&self, partition: cdf_kernel::PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        let partition_id = partition.partition_id.as_str().to_owned();
        let start_position = partition.start_position.clone();
        self.opens
            .lock()
            .unwrap()
            .push((partition_id.clone(), start_position.clone()));
        let schema = self.schema();
        let resource_id = self.descriptor().resource_id.clone();
        let fail_resume = partition_id == "part-b"
            && start_position.is_some()
            && self.fail_first_resume.swap(false, Ordering::SeqCst);
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            if fail_resume {
                return Err(CdfError::internal(
                    "injected failure after the first multi-partition checkpoint",
                ));
            }
            let values = match (partition_id.as_str(), start_position) {
                ("part-a", None) => vec![100],
                ("part-a", Some(_)) => Vec::new(),
                ("part-b", None) => vec![1, 2],
                ("part-b", Some(SourcePosition::Cursor(position)))
                    if position.value == CursorValue::I64(1) =>
                {
                    vec![2]
                }
                (partition, start) => {
                    return Err(CdfError::data(format!(
                        "unexpected drain resume for {partition}: {start:?}"
                    )));
                }
            };
            let batches = values
                .into_iter()
                .map(|value| {
                    one_row_cursor_batch(
                        &format!("batch-{partition_id}-{value}"),
                        value,
                        Arc::clone(&schema),
                        resource_id.clone(),
                        PartitionId::new(partition_id.clone())?,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                futures_util::stream::iter(batches.into_iter().map(Ok)),
            )))
        }))
    }
}

impl QueryableResource for DurableMultiPartitionDrainResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<cdf_kernel::ScanPlan> {
        negotiate_scan_plan(
            self.descriptor().resource_id.clone(),
            request.clone(),
            self.capabilities(),
            self.plan_partitions(request)?,
            None,
            None,
            DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        )
    }
}

pub(super) fn one_row_cursor_batch(
    batch_id: &str,
    value: i64,
    schema: Arc<Schema>,
    resource_id: ResourceId,
    partition_id: PartitionId,
) -> Result<cdf_kernel::Batch> {
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![value])) as ArrayRef,
            Arc::new(Int64Array::from(vec![value])) as ArrayRef,
        ],
    )?;
    let schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?;
    let mut batch = cdf_kernel::Batch::from_record_batch(
        cdf_kernel::BatchId::new(batch_id)?,
        resource_id,
        partition_id,
        schema_hash,
        record_batch,
    )?;
    batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "updated_at".to_owned(),
        value: CursorValue::I64(value),
    }));
    Ok(batch)
}

pub(super) struct RecordingRunEventSink {
    pub(super) capacity: Option<usize>,
    pub(super) persisted_state_path: Option<PathBuf>,
    pub(super) events: Mutex<Vec<RunEvent>>,
    pub(super) drops: AtomicU64,
}

impl RecordingRunEventSink {
    pub(super) fn unbounded() -> Self {
        Self {
            capacity: None,
            persisted_state_path: None,
            events: Mutex::new(Vec::new()),
            drops: AtomicU64::new(0),
        }
    }

    pub(super) fn bounded(capacity: usize) -> Self {
        Self {
            capacity: Some(capacity),
            persisted_state_path: None,
            events: Mutex::new(Vec::new()),
            drops: AtomicU64::new(0),
        }
    }

    pub(super) fn unbounded_with_persistence_check(state_path: &Path) -> Self {
        Self {
            persisted_state_path: Some(state_path.to_path_buf()),
            ..Self::unbounded()
        }
    }

    pub(super) fn events(&self) -> Vec<RunEvent> {
        self.events.lock().unwrap().clone()
    }

    pub(super) fn drop_count(&self) -> u64 {
        self.drops.load(Ordering::SeqCst)
    }
}

impl RunEventSink for RecordingRunEventSink {
    fn try_emit(&self, event: &RunEvent) -> RunEventSinkResult {
        let mut events = self.events.lock().unwrap();
        if self
            .capacity
            .is_some_and(|capacity| events.len() >= capacity)
        {
            self.drops.fetch_add(1, Ordering::SeqCst);
            return RunEventSinkResult::Dropped;
        }
        if let Some(state_path) = &self.persisted_state_path {
            let ledger = SqliteRunLedger::open(state_path).unwrap();
            let persisted = ledger.events(&event.run_id).unwrap();
            assert_eq!(persisted.last(), Some(event));
        }
        events.push(event.clone());
        RunEventSinkResult::Accepted
    }
}

#[derive(Clone, Default)]
pub(super) struct CapturingTracingSubscriber {
    pub(super) next_id: Arc<AtomicU64>,
    pub(super) events: Arc<Mutex<Vec<CapturedTracingEvent>>>,
}

impl CapturingTracingSubscriber {
    pub(super) fn captured_events(&self) -> Vec<CapturedTracingEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl Subscriber for CapturingTracingSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _attrs: &Attributes<'_>) -> Id {
        Id::from_u64(self.next_id.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = TracingFieldVisitor::default();
        event.record(&mut visitor);
        self.events.lock().unwrap().push(CapturedTracingEvent {
            target: event.metadata().target().to_owned(),
            fields: visitor.fields,
        });
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[derive(Clone, Debug)]
pub(super) struct CapturedTracingEvent {
    pub(super) target: String,
    pub(super) fields: BTreeMap<String, String>,
}

#[derive(Default)]
pub(super) struct TracingFieldVisitor {
    pub(super) fields: BTreeMap<String, String>,
}

impl Visit for TracingFieldVisitor {
    fn record_str(&mut self, field: &TracingField, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }

    fn record_bool(&mut self, field: &TracingField, value: bool) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_i64(&mut self, field: &TracingField, value: i64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_u64(&mut self, field: &TracingField, value: u64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_debug(&mut self, field: &TracingField, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }
}

pub(super) fn expected_runtime_trace_fields(event: &RunEvent) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("run_id".to_owned(), event.run_id.as_str().to_owned()),
        (
            "resource_id".to_owned(),
            optional_trace_field(event.resource_id.as_ref()),
        ),
        (
            "scope".to_owned(),
            event
                .scope
                .as_ref()
                .and_then(|scope| serde_json::to_string(scope).ok())
                .unwrap_or_default(),
        ),
        (
            "partition_id".to_owned(),
            optional_trace_field(event.partition_id.as_ref()),
        ),
        (
            "package_id".to_owned(),
            optional_trace_field(event.package_id.as_ref()),
        ),
        (
            "package_hash".to_owned(),
            optional_trace_field(event.package_hash.as_ref()),
        ),
        (
            "package_path".to_owned(),
            optional_trace_field(event.package_path.as_ref()),
        ),
        (
            "destination_id".to_owned(),
            optional_trace_field(event.destination_id.as_ref()),
        ),
        (
            "plan_id".to_owned(),
            optional_trace_field(event.plan_id.as_ref()),
        ),
        (
            "checkpoint_id".to_owned(),
            optional_trace_field(event.checkpoint_id.as_ref()),
        ),
        (
            "receipt_id".to_owned(),
            optional_trace_field(event.receipt_id.as_ref()),
        ),
        ("event_kind".to_owned(), event.kind.as_str().to_owned()),
        ("sequence".to_owned(), event.sequence.to_string()),
        ("timestamp_ms".to_owned(), event.timestamp_ms.to_string()),
        (
            "details".to_owned(),
            serde_json::to_string(&event.details.attributes).unwrap(),
        ),
    ])
}

pub(super) fn optional_trace_field<T: AsRef<str>>(value: Option<&T>) -> String {
    value.map(|value| value.as_ref()).unwrap_or("").to_owned()
}

pub(super) fn runtime_trace_events(
    subscriber: &CapturingTracingSubscriber,
) -> Vec<CapturedTracingEvent> {
    subscriber
        .captured_events()
        .into_iter()
        .filter(|event| event.target == "cdf_project.runtime.run_event")
        .collect()
}

pub(super) fn run_event_for_tracing_details(details: RunEventDetails) -> RunEvent {
    RunEvent {
        run_id: RunId::new("run-tracing-redaction").unwrap(),
        sequence: 1,
        timestamp_ms: 1_800_000_000_000,
        kind: RunEventKind::RunStarted,
        resource_id: Some(ResourceId::new("local.events").unwrap()),
        scope: Some(ScopeKey::Resource),
        partition_id: None,
        package_id: Some("pkg-tracing-redaction".to_owned()),
        package_hash: None,
        package_path: Some("pkg-tracing-redaction".to_owned()),
        checkpoint_id: None,
        receipt_id: None,
        destination_id: Some(DestinationId::new("duckdb").unwrap()),
        plan_id: Some(PlanId::new("plan-tracing-redaction").unwrap()),
        details,
    }
}

pub(super) fn compiled_backfill_source(
    resource: &BackfillMockResource,
) -> cdf_runtime::CompiledSourcePlan {
    cdf_runtime::CompiledSourcePlan::new(
        cdf_runtime::SourceDriverDescriptor {
            driver_id: cdf_runtime::SourceDriverId::new("backfill_mock").unwrap(),
            driver_version: "1.0.0".to_owned(),
            option_schema_hash: cdf_runtime::artifact_hash(&serde_json::json!({})).unwrap(),
            kinds: vec!["mock".to_owned()],
            schemes: Vec::new(),
        },
        resource.capabilities.clone(),
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
            telemetry_version: "backfill-mock-v1".to_owned(),
        },
        cdf_runtime::CompiledSourcePlanInput {
            descriptor: resource.descriptor.clone(),
            schema: resource.schema.as_ref().clone(),
            type_policy_allowances: resource.type_policy_allowances(),
            effective_schema_runtime: resource.effective_schema_runtime().cloned(),
            baseline_observation_schema_catalog: resource
                .baseline_observation_schema_catalog()
                .to_vec(),
            redacted_options: serde_json::json!({}),
            physical_plan: serde_json::json!({"partitions": 1}),
        },
    )
    .unwrap()
}

pub(super) struct TableSnapshotMockResource {
    pub(super) descriptor: ResourceDescriptor,
    pub(super) capabilities: ResourceCapabilities,
    pub(super) schema: Arc<Schema>,
    pub(super) snapshot: cdf_kernel::TableSnapshotPosition,
    pub(super) open_count: AtomicU64,
}

impl TableSnapshotMockResource {
    pub(super) fn new() -> Self {
        Self {
            descriptor: ResourceDescriptor {
                resource_id: ResourceId::new("mock.snapshot_events").unwrap(),
                schema_source: SchemaSource::Declared {
                    schema_hash: SchemaHash::new("schema-table-snapshot-mock").unwrap(),
                    source: "mock://snapshot-events".to_owned(),
                },
                primary_key: vec!["id".to_owned()],
                merge_key: Vec::new(),
                cursor: None,
                write_disposition: WriteDisposition::Append,
                deduplication: None,
                contract: None,
                state_scope: ScopeKey::Resource,
                freshness: None,
                trust_level: TrustLevel::Governed,
            },
            capabilities: ResourceCapabilities {
                projection: CapabilitySupport::Unsupported,
                filters: FilterCapabilities::default(),
                limits: CapabilitySupport::Unsupported,
                ordering: CapabilitySupport::Unsupported,
                partitioning: Default::default(),
                incremental: IncrementalShape::TableSnapshot,
                replay: ReplaySupport::FromPosition,
                idempotent_reads: true,
                backpressure: BackpressureSupport::Pausable,
                estimates: EstimateSupport::RowsAndBytes,
            },
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("updated_at", DataType::Int64, false),
            ])),
            snapshot: cdf_kernel::TableSnapshotPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                protocol: "iceberg".to_owned(),
                catalog: "mock:catalog".to_owned(),
                namespace: vec!["analytics".to_owned()],
                table: "events".to_owned(),
                selector: cdf_kernel::TableSnapshotSelector::Current,
                snapshot_id: 41,
                sequence_number: 7,
                parent_snapshot_id: Some(40),
                metadata_location: "mock://catalog/analytics/events/metadata/v7.json".to_owned(),
                metadata_generation: "version-id:v7".to_owned(),
            },
            open_count: AtomicU64::new(0),
        }
    }

    pub(super) fn position(&self) -> SourcePosition {
        SourcePosition::TableSnapshot(Box::new(self.snapshot.clone()))
    }
}

impl ResourceStream for TableSnapshotMockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<cdf_kernel::PartitionPlan>> {
        Ok(vec![cdf_kernel::PartitionPlan {
            partition_id: PartitionId::new("snapshot-task-0").unwrap(),
            scope: request.scope.clone(),
            planned_position: Some(self.position()),
            start_position: None,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::new(),
        }])
    }

    fn rebind_scan_for_resume(
        &self,
        scan: cdf_kernel::ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<cdf_kernel::ScanPlan> {
        if committed_frontier == &self.position() {
            return scan.try_map_partition_authority(|authority| match authority {
                cdf_kernel::PartitionAuthority::Inline(_) => {
                    Ok(cdf_kernel::PartitionAuthority::Inline(Vec::new()))
                }
                cdf_kernel::PartitionAuthority::External(_) => Err(CdfError::contract(
                    "mock table-snapshot resource requires inline partition authority",
                )),
            });
        }
        Err(CdfError::data(
            "mock table-snapshot resource received an unexpected committed frontier",
        ))
    }

    fn open(&self, partition: cdf_kernel::PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.open_count.fetch_add(1, Ordering::SeqCst);
        let schema = self.schema();
        let resource_id = self.descriptor.resource_id.clone();
        let snapshot = self.position();
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let record_batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                ],
            )?;
            let schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?;
            let mut batch = cdf_kernel::Batch::from_record_batch(
                cdf_kernel::BatchId::new("batch-table-snapshot")?,
                resource_id,
                partition.partition_id,
                schema_hash,
                record_batch,
            )?;
            batch.header.source_position = Some(snapshot);
            Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                futures_util::stream::once(async move { Ok(batch) }),
            )))
        }))
    }
}

impl QueryableResource for TableSnapshotMockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<cdf_kernel::ScanPlan> {
        negotiate_scan_plan(
            self.descriptor.resource_id.clone(),
            request.clone(),
            &self.capabilities,
            self.plan_partitions(request)?,
            Some(1),
            Some(16),
            DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        )
    }
}

pub(super) fn compiled_drain_test_source_plan(
    resource: &dyn QueryableResource,
) -> cdf_runtime::CompiledSourcePlan {
    let mut source = compiled_test_source_plan(resource);
    source.execution_capabilities.bounded = false;
    source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: true,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Drop,
        watermark: None,
        safe_frontiers: vec![cdf_kernel::SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::FileManifest],
        idleness_capabilities: Vec::new(),
    });
    source.validate().unwrap();
    source
}

pub(super) fn replace_multi_file_resource(root: &Path) -> OwnedTestResource {
    multi_file_resource_with_document(root, MULTI_FILE_RESOURCE_REPLACE)
}

pub(super) fn rest_cursor_runtime_resource(
    cursor_field: &str,
    cursor_field_decl: &str,
    ordering: &str,
    lag: &str,
) -> OwnedTestResource {
    let input = format!(
        r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"
auth = {{ kind = "bearer", token = "secret://env/API_TOKEN" }}
egress_allowlist = ["api.example.test"]

[resource.items]
path = "/items"
paginate = {{ kind = "cursor_param", query_param = "cursor", response_field = "next_cursor" }}
records = "$.items"
primary_key = ["id"]
cursor = {{ field = "{cursor_field}", param = "since", ordering = "{ordering}", lag = "{lag}" }}
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
  {cursor_field_decl},
] }}
"#
    );
    let document = cdf_declarative::parse_toml(&input).unwrap();
    let compiled = cdf_declarative::compile_document(&rest_compile_registry(), &document)
        .unwrap()
        .remove(0);
    let execution = test_execution_services();
    resolve_rest_resource(
        &compiled,
        RecordingTransport::default(),
        Arc::new(StaticSecretProvider::new([(
            "secret://env/API_TOKEN",
            "token",
        )])),
        &execution,
    )
}

pub(super) fn state_delta_request<'a>(
    resource: &'a dyn QueryableResource,
    package_id: &str,
) -> StateDeltaTestRequest<'a> {
    StateDeltaTestRequest {
        resource,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        target: TargetName::new("items").unwrap(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
    }
}

pub(super) fn engine_output_with_positions(
    package_dir: &Path,
    package_id: &str,
    positions: Vec<SourcePosition>,
) -> EngineRunOutputWithSegmentPositions {
    engine_output_with_positions_and_checkpoint_eligibility(
        package_dir,
        package_id,
        positions,
        true,
    )
}

pub(super) fn engine_output_with_positions_and_checkpoint_eligibility(
    package_dir: &Path,
    package_id: &str,
    positions: Vec<SourcePosition>,
    checkpoint_eligible: bool,
) -> EngineRunOutputWithSegmentPositions {
    let builder = PackageBuilder::create(
        package_dir,
        package_id,
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    let mut segments = Vec::with_capacity(positions.len());
    let mut package_row_ord_start = 0_u64;
    for index in 0..positions.len() {
        let batches = cdf_package_contract::append_package_row_ord(
            vec![sample_batch(
                vec![1, 2, 3],
                vec![Some("ada"), Some("grace"), None],
            )],
            package_row_ord_start,
        )
        .unwrap();
        let segment = builder
            .write_segment(
                SegmentId::new(format!("seg-{:06}", index + 1)).unwrap(),
                package_row_ord_start,
                &batches,
            )
            .unwrap();
        package_row_ord_start = package_row_ord_start.checked_add(3).unwrap();
        segments.push(segment);
    }
    let (manifest, verification) = builder.finish_verified().unwrap();
    let processed_observations = positions
        .iter()
        .enumerate()
        .map(|(index, position)| {
            ProcessedObservationPosition::new(
                format!("fixture-observation-{index}"),
                ProcessedObservationOutcome::Admitted,
                position.clone(),
            )
            .unwrap()
        })
        .collect();
    let segment_positions = segments
        .iter()
        .zip(positions)
        .map(|(segment, position)| EngineSegmentPosition {
            segment_id: segment.segment_id.clone(),
            partition_ordinal: 0,
            output_position: Some(position),
        })
        .collect();
    let execution_evidence = cdf_engine::EngineExecutionEvidence::new(
        processed_observations,
        Vec::new(),
        None,
        checkpoint_eligible,
    )
    .unwrap();
    EngineRunOutputWithSegmentPositions::new(
        EngineRunOutput {
            manifest,
            verification,
            profile: ExecutionProfile::default(),
            lineage: LineageSummary::default(),
            terminal_schema_quarantines: Vec::new(),
        },
        segment_positions,
        execution_evidence,
    )
}

pub(super) fn state_delta_for_positions(
    resource: &dyn QueryableResource,
    root: &Path,
    package_id: &str,
    positions: Vec<SourcePosition>,
) -> Result<StateDelta> {
    let output = engine_output_with_positions(&root.join(package_id), package_id, positions);
    let request = state_delta_request(resource, package_id);
    state_delta_from_run(
        &request,
        &output,
        &SchemaHash::new(SCHEMA_HASH).unwrap(),
        &resource.descriptor().state_scope,
        None,
    )
}

pub(super) fn assert_run_artifact_identity_unchanged(report: &ProjectRunReport) {
    let reader = PackageReader::open(&report.package_dir).unwrap();
    assert_eq!(
        PackageHash::new(reader.manifest().package_hash.clone()).unwrap(),
        report.package_hash
    );
    assert_eq!(report.receipt.package_hash, report.package_hash);
    assert_eq!(report.checkpoint.delta.package_hash, report.package_hash);
    assert_eq!(reader.manifest().lifecycle.status, report.package_status);
}

pub(super) fn output_manifest(report: &ProjectRunReport) -> &FileManifest {
    let SourcePosition::FileManifest(manifest) = &report.checkpoint.delta.output_position else {
        panic!("checkpoint output position should be a file manifest");
    };
    manifest
}

pub(super) fn output_manifest_paths(report: &ProjectRunReport) -> Vec<String> {
    output_manifest(report)
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect()
}

pub(super) fn output_manifest_file<'a>(
    report: &'a ProjectRunReport,
    path: &str,
) -> &'a FilePosition {
    output_manifest(report)
        .files
        .iter()
        .find(|file| file.path == path)
        .unwrap_or_else(|| panic!("manifest omitted {path}"))
}

pub(super) fn single_segment_manifest_path(report: &ProjectRunReport) -> String {
    assert_eq!(report.checkpoint.delta.segments.len(), 1);
    let SourcePosition::FileManifest(manifest) =
        &report.checkpoint.delta.segments[0].output_position
    else {
        panic!("state segment should retain file manifest evidence");
    };
    assert_eq!(manifest.files.len(), 1);
    manifest.files[0].path.clone()
}

pub(super) fn file_position(path: &str) -> SourcePosition {
    file_position_with_identity(path, 42, Some(format!("sha256:{}", "00".repeat(32))))
}

pub(super) fn file_position_with_identity(
    path: &str,
    size_bytes: u64,
    sha256: Option<String>,
) -> SourcePosition {
    SourcePosition::FileManifest(FileManifest {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        files: vec![FilePosition {
            path: path.to_owned(),
            size_bytes,
            source_generation: None,
            etag: None,
            object_version: None,
            sha256,
        }],
    })
}

#[test]
fn backfill_planner_splits_numeric_windows_with_window_scopes_and_ids() {
    let resource = BackfillMockResource::cursor();
    let source = compiled_backfill_source(&resource);

    let plan = plan_backfill(
        &resource,
        &source,
        BackfillPlanRequest {
            target: TargetName::new("events").unwrap(),
            from: "0".to_owned(),
            to: "25".to_owned(),
            slice_size: Some(10),
            segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
        },
    )
    .unwrap();

    assert_eq!(plan.resource_id, "mock.events");
    assert_eq!(plan.target, "events");
    assert_eq!(
        plan.pipeline_id,
        backfill_pipeline_id().unwrap().to_string()
    );
    assert_eq!(plan.slices.len(), 3);
    assert_eq!(
        plan.slices
            .iter()
            .map(|slice| (slice.start.as_str(), slice.end.as_str()))
            .collect::<Vec<_>>(),
        vec![("0", "10"), ("10", "20"), ("20", "25")]
    );
    for slice in &plan.slices {
        assert_eq!(
            slice.scope,
            ScopeKey::Window {
                start: slice.start.clone(),
                end: slice.end.clone()
            }
        );
        assert_eq!(
            slice.engine_plan.scan.request.scope,
            ScopeKey::Window {
                start: slice.start.clone(),
                end: slice.end.clone()
            }
        );
        assert!(slice.package_id.starts_with("cdf-backfill-pkg-"));
        assert!(slice.checkpoint_id.starts_with("cdf-backfill-cp-"));
        assert_eq!(
            slice.filters,
            vec![
                format!("updated_at >= {}", slice.start),
                format!("updated_at < {}", slice.end),
            ]
        );
        assert!(slice.engine_plan.residual_predicates.is_empty());
    }
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn backfill_planner_binds_every_slice_to_the_compiled_source_artifact() {
    let resource = BackfillMockResource::cursor();
    let source = compiled_backfill_source(&resource);
    let expected_hash = source.compiled_source_plan_hash().unwrap();

    let plan = plan_backfill(
        &resource,
        &source,
        BackfillPlanRequest {
            target: TargetName::new("events").unwrap(),
            from: "0".to_owned(),
            to: "20".to_owned(),
            slice_size: Some(10),
            segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
        },
    )
    .unwrap();

    assert_eq!(plan.slices.len(), 2);
    for slice in &plan.slices {
        let execution = slice
            .engine_plan
            .compiled_source_execution
            .as_ref()
            .expect("every executable backfill slice retains compiler source authority");
        assert_eq!(execution.compiled_source_plan_hash(), &expected_hash);
        assert_eq!(
            slice
                .engine_plan
                .partition_schedule
                .as_ref()
                .expect("source-bound backfill slice has a canonical schedule")
                .inline_partitions()
                .expect("backfill slices retain inline partition authority")[0]
                .partition
                .scope,
            slice.scope
        );
    }
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn backfill_planner_rejects_file_incremental_resource_without_opening_source() {
    let resource = BackfillMockResource::file_incremental();
    let source = compiled_backfill_source(&resource);

    let error = plan_backfill(
        &resource,
        &source,
        BackfillPlanRequest {
            target: TargetName::new("events").unwrap(),
            from: "0".to_owned(),
            to: "10".to_owned(),
            slice_size: None,
            segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("cursor-backed queryable"));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn backfill_planner_rejects_inverted_numeric_bounds_without_opening_source() {
    let resource = BackfillMockResource::cursor();
    let source = compiled_backfill_source(&resource);

    let error = plan_backfill(
        &resource,
        &source,
        BackfillPlanRequest {
            target: TargetName::new("events").unwrap(),
            from: "10".to_owned(),
            to: "10".to_owned(),
            slice_size: None,
            segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("--from < --to"));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn general_project_run_records_ledger_events_in_commit_gate_order() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-general-ledger-order";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut request = project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-general-ledger-order",
    );
    request.resource = ProjectRunSource::new(&resource);
    request.plan = live_plan_for_queryable(&resource, package_id);

    let report = futures_executor::block_on(run_project(request)).unwrap();

    let kinds = report
        .ledger_snapshot
        .events
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            RunEventKind::RunStarted,
            RunEventKind::PlanRecorded,
            RunEventKind::PackageStarted,
            RunEventKind::PackageSegmentRecorded,
            RunEventKind::PackageFinalized,
            RunEventKind::ValidationDepthTransitionRecorded,
            RunEventKind::CheckpointProposed,
            RunEventKind::DestinationCommitStarted,
            RunEventKind::DestinationSegmentAcknowledged,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::CheckpointCommitted,
            RunEventKind::PackageStatusUpdated,
            RunEventKind::RunSucceeded,
        ]
    );
    for (index, event) in report.ledger_snapshot.events.iter().enumerate() {
        assert_eq!(event.sequence, u64::try_from(index + 1).unwrap());
        assert_eq!(event.run_id, report.run_id);
    }
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(report.row_count, 2);
    assert_eq!(
        report.ledger_snapshot.events[4].package_hash,
        Some(report.package_hash.clone())
    );
    assert_eq!(
        report.ledger_snapshot.events[9].receipt_id,
        Some(report.receipt.receipt_id.clone())
    );
    assert_eq!(
        report.ledger_snapshot.events[3]
            .details
            .attributes
            .get("row_count"),
        Some(&RunEventValue::U64(2))
    );
    assert_eq!(
        report.ledger_snapshot.events[4]
            .details
            .attributes
            .get("batch_count"),
        Some(&RunEventValue::U64(1))
    );
    assert_eq!(
        report.ledger_snapshot.events[4]
            .details
            .attributes
            .get("quarantine_record_count"),
        Some(&RunEventValue::U64(0))
    );
    assert_eq!(
        report.ledger_snapshot.events[8]
            .details
            .attributes
            .get("byte_count"),
        report.ledger_snapshot.events[3]
            .details
            .attributes
            .get("byte_count")
    );
    let bulk = &report.ledger_snapshot.events[7].details.attributes;
    assert_eq!(
        bulk.get("bulk_path_id"),
        Some(&RunEventValue::String("canonical_segment_scan".to_owned()))
    );
    assert_eq!(bulk.get("bulk_path_version"), Some(&RunEventValue::U64(1)));
    assert_eq!(
        bulk.get("bulk_evidence_version"),
        Some(&RunEventValue::String(
            "p3-d14-stock-scan-2026-07-19-v1".to_owned()
        ))
    );
    assert!(matches!(
        bulk.get("bulk_rows_per_batch"),
        Some(RunEventValue::U64(value)) if *value > 0
    ));
    assert!(matches!(
        bulk.get("bulk_bytes_per_batch"),
        Some(RunEventValue::U64(value)) if *value > 0
    ));
    assert_eq!(bulk.get("bulk_writers"), Some(&RunEventValue::U64(1)));
    assert!(
        report.ledger_snapshot.events[12]
            .details
            .attributes
            .contains_key("elapsed_ms")
    );
}

#[test]
fn committed_head_reopens_only_its_verified_late_data_carryover() {
    let temp = tempfile::tempdir().unwrap();
    let package_root = temp.path().join("packages");
    let package_id = "pkg-carryover-loader";
    let package_dir = package_root.join(package_id);
    let (manifest, reference) = build_package_with_carryover(&package_dir, package_id);
    let mut state = delta(&manifest, "checkpoint-carryover-loader");
    state.late_data_carryover = vec![reference];
    let head = Checkpoint {
        delta: state,
        status: CheckpointStatus::Committed,
        receipt: None,
        is_head: true,
        created_at_ms: 1_700_000_000_000,
        committed_at_ms: Some(1_700_000_000_001),
        rewind_target_checkpoint_id: None,
    };

    assert!(
        crate::runtime::load_late_data_carryover(&package_root, None)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        crate::runtime::load_late_data_carryover(&package_root, Some(&head))
            .unwrap()
            .len(),
        1
    );

    let mut wrong_head = head;
    wrong_head.delta.package_hash = PackageHash::new("different-package-hash").unwrap();
    let error = match crate::runtime::load_late_data_carryover(&package_root, Some(&wrong_head)) {
        Ok(_) => panic!("mismatched checkpoint authority must fail closed"),
        Err(error) => error,
    };
    assert!(error.message.contains("committed checkpoint head"));
}

#[test]
fn drain_project_settles_each_frontier_before_committing_the_next_epoch() {
    let temp = tempfile::tempdir().unwrap();
    let resource = multi_file_resource(temp.path());
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let package_id = "pkg-drain-epochs";
    let source = compiled_drain_test_source_plan(&resource);
    let replay_retention = CheckpointBoundReplayRetention {
        state_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-drain").unwrap(),
        resource_id: resource.descriptor().resource_id.clone(),
        scope: resource.descriptor().state_scope.clone(),
        committed: Mutex::new(Vec::new()),
    };
    let bound = BoundTestResource {
        inner: &resource,
        compiled_source_plan_hash: source.compiled_source_plan_hash().unwrap(),
        replay_retention: Some(&replay_retention),
    };
    let extent = ExecutionExtent::Drain {
        version: cdf_kernel::EXECUTION_EXTENT_VERSION,
        policy: cdf_kernel::StreamEpochPolicy {
            version: cdf_kernel::STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: cdf_kernel::EpochClosureTrigger::Rows { count: 1 },
            package_rotation: cdf_kernel::EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: cdf_kernel::WatermarkPolicy::Disabled,
            late_data: cdf_kernel::LateDataAction::Quarantine,
            safe_frontier: cdf_kernel::SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: cdf_kernel::DrainTermination::Records { count: 2 },
    };
    let mut plan = live_plan_for_queryable(&resource, package_id);
    plan.execution_extent = extent.clone();
    plan.explain.execution_extent = extent;
    let resolved_destination =
        crate::test_destinations::duckdb(&duckdb_path, TargetName::new("events").unwrap()).unwrap();
    let plan = plan
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(&source, &resolved_destination.runtime_capabilities())
        .unwrap();
    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-drain").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-drain").unwrap(),
        destination: resolved_destination,
        run_id: Some(RunId::new("run-drain").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    let drain = report.drain.as_ref().expect("drain summary");
    assert_eq!(drain.epoch_count, 2);
    assert_eq!(drain.total_row_count, 2);
    assert_eq!(drain.total_segment_count, 2);
    assert_eq!(drain.first_run_id.as_str(), "run-drain");
    assert_eq!(drain.last_epoch.epoch_ordinal, 1);
    assert_eq!(
        drain.last_epoch.package_id,
        "pkg-drain-epochs-epoch-00000000000000000001"
    );
    assert!(matches!(
        drain.last_epoch.closure.cause,
        cdf_kernel::EpochClosureCause::DrainTermination { .. }
    ));
    assert!(
        package_root
            .join(package_id)
            .join("plan/epoch-closure.json")
            .is_file()
    );
    assert!(
        package_root
            .join(&drain.last_epoch.package_id)
            .join("plan/epoch-closure.json")
            .is_file()
    );

    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let history = store
        .history(
            &PipelineId::new("pipeline-drain").unwrap(),
            &resource.descriptor().resource_id,
            &resource.descriptor().state_scope,
        )
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(
        history[1].delta.parent_checkpoint_id,
        Some(history[0].delta.checkpoint_id.clone())
    );
    assert_eq!(report.checkpoint, history[1]);
    assert_eq!(
        *replay_retention.committed.lock().unwrap(),
        history
            .iter()
            .map(|checkpoint| checkpoint.delta.output_position.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        output_manifest_paths(&report),
        vec!["events-a.ndjson", "events-b.ndjson"]
    );
}

#[test]
fn cold_empty_drain_returns_no_op_without_package_destination_or_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let resource = EmptyDrainResource::new();
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let package_id = "pkg-empty-drain";
    let pipeline_id = PipelineId::new("pipeline-empty-drain").unwrap();
    let source = compiled_drain_test_source_plan(&resource);
    let bound = BoundTestResource {
        inner: &resource,
        compiled_source_plan_hash: source.compiled_source_plan_hash().unwrap(),
        replay_retention: None,
    };
    let extent = ExecutionExtent::Drain {
        version: cdf_kernel::EXECUTION_EXTENT_VERSION,
        policy: cdf_kernel::StreamEpochPolicy {
            version: cdf_kernel::STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: cdf_kernel::EpochClosureTrigger::Rows { count: 1 },
            package_rotation: cdf_kernel::EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: cdf_kernel::WatermarkPolicy::Disabled,
            late_data: cdf_kernel::LateDataAction::Quarantine,
            safe_frontier: cdf_kernel::SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: cdf_kernel::DrainTermination::Duration {
            milliseconds: 60_000,
        },
    };
    let mut plan = live_plan_for_queryable(&resource, package_id);
    plan.execution_extent = extent.clone();
    plan.explain.execution_extent = extent;
    let destination =
        crate::test_destinations::duckdb(&duckdb_path, TargetName::new("events").unwrap()).unwrap();
    let plan = plan
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(&source, &destination.runtime_capabilities())
        .unwrap();

    let outcome = futures_executor::block_on(run_project_outcome(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: pipeline_id.clone(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-empty-drain").unwrap(),
        destination,
        run_id: Some(RunId::new("run-empty-drain").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    let ProjectRunOutcome::NoOp(report) = outcome else {
        panic!("cold empty drain should return an explicit no-op outcome");
    };
    assert_eq!(report.reason, ProjectRunNoOpReason::SourceExhausted);
    assert!(report.current_checkpoint.is_none());
    assert!(!package_root.join(package_id).exists());
    assert!(report.ledger_snapshot.events.iter().all(|event| !matches!(
        event.kind,
        RunEventKind::PackageFinalized
            | RunEventKind::DestinationReceiptRecorded
            | RunEventKind::CheckpointCommitted
    )));
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    assert!(
        store
            .head(
                &pipeline_id,
                &resource.descriptor().resource_id,
                &resource.descriptor().state_scope,
            )
            .unwrap()
            .is_none()
    );
    assert!(!duckdb_path.exists());
}

#[test]
fn drain_preserves_committed_summary_when_the_following_epoch_is_empty() {
    let temp = tempfile::tempdir().unwrap();
    let resource = OneBatchThenEmptyDrainResource::new();
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let package_id = "pkg-drain-final-noop";
    let pipeline_id = PipelineId::new("pipeline-drain-final-noop").unwrap();
    let mut source = compiled_drain_test_source_plan(&resource);
    source.execution_capabilities.maximum_concurrency = 1;
    source.execution_capabilities.useful_concurrency = 1;
    source.validate().unwrap();
    let bound = BoundTestResource {
        inner: &resource,
        compiled_source_plan_hash: source.compiled_source_plan_hash().unwrap(),
        replay_retention: None,
    };
    let extent = ExecutionExtent::Drain {
        version: cdf_kernel::EXECUTION_EXTENT_VERSION,
        policy: cdf_kernel::StreamEpochPolicy {
            version: cdf_kernel::STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: cdf_kernel::EpochClosureTrigger::Rows { count: 1 },
            package_rotation: cdf_kernel::EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: cdf_kernel::WatermarkPolicy::Disabled,
            late_data: cdf_kernel::LateDataAction::Quarantine,
            safe_frontier: cdf_kernel::SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: cdf_kernel::DrainTermination::Records { count: 2 },
    };
    let mut plan = live_plan_for_queryable(&resource, package_id);
    plan.execution_extent = extent.clone();
    plan.explain.execution_extent = extent;
    let destination =
        crate::test_destinations::duckdb(&duckdb_path, TargetName::new("events").unwrap()).unwrap();
    let plan = plan
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(&source, &destination.runtime_capabilities())
        .unwrap();

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: pipeline_id.clone(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-drain-final-noop").unwrap(),
        destination,
        run_id: Some(RunId::new("run-drain-final-noop").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    let drain = report.drain.as_ref().expect("drain summary");
    assert_eq!(drain.epoch_count, 1);
    assert_eq!(drain.total_row_count, 1);
    assert_eq!(drain.total_segment_count, 1);
    assert_eq!(report.row_count, 1);
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    let second_epoch = 1_u64;
    assert!(
        !package_root
            .join(format!("{package_id}-epoch-{second_epoch:020}"))
            .exists()
    );
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    assert_eq!(
        store
            .history(
                &pipeline_id,
                &resource.descriptor().resource_id,
                &resource.descriptor().state_scope,
            )
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn drain_retry_discards_only_incomplete_construction_after_staging_abort() {
    let temp = tempfile::tempdir().unwrap();
    let resource = OneBatchThenErrorOnceDrainResource::new();
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let package_id = "pkg-drain-incomplete-retry";
    let pipeline_id = PipelineId::new("pipeline-drain-incomplete-retry").unwrap();
    let source = compiled_drain_test_source_plan(&resource);
    let bound = BoundTestResource {
        inner: &resource,
        compiled_source_plan_hash: source.compiled_source_plan_hash().unwrap(),
        replay_retention: None,
    };
    let extent = ExecutionExtent::Drain {
        version: cdf_kernel::EXECUTION_EXTENT_VERSION,
        policy: cdf_kernel::StreamEpochPolicy {
            version: cdf_kernel::STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: cdf_kernel::EpochClosureTrigger::Rows { count: 2 },
            package_rotation: cdf_kernel::EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: cdf_kernel::WatermarkPolicy::Disabled,
            late_data: cdf_kernel::LateDataAction::Quarantine,
            safe_frontier: cdf_kernel::SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: cdf_kernel::DrainTermination::Records { count: 2 },
    };
    let mut plan = live_plan_for_queryable(&resource, package_id);
    plan.execution_extent = extent.clone();
    plan.explain.execution_extent = extent;
    let destination =
        crate::test_destinations::duckdb(&duckdb_path, TargetName::new("events").unwrap()).unwrap();
    let plan = plan
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(&source, &destination.runtime_capabilities())
        .unwrap();
    let retry_plan = plan.clone();

    let first = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: pipeline_id.clone(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-drain-incomplete-retry").unwrap(),
        destination,
        run_id: Some(RunId::new("run-drain-incomplete-retry-first").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();
    assert!(first.to_string().contains("injected extraction failure"));
    assert_eq!(
        package_status(&package_root.join(package_id)),
        PackageStatus::Extracting
    );
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    assert!(
        store
            .head(
                &pipeline_id,
                &resource.descriptor().resource_id,
                &resource.descriptor().state_scope,
            )
            .unwrap()
            .is_none()
    );

    let destination =
        crate::test_destinations::duckdb(&duckdb_path, TargetName::new("events").unwrap()).unwrap();
    let resumed = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan: retry_plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: pipeline_id.clone(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-drain-incomplete-retry").unwrap(),
        destination,
        run_id: Some(RunId::new("run-drain-incomplete-retry-second").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    assert_eq!(resumed.row_count, 1);
    assert_eq!(resumed.package_status, PackageStatus::Checkpointed);
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    let history = SqliteCheckpointStore::open(&state_path)
        .unwrap()
        .history(
            &pipeline_id,
            &resource.descriptor().resource_id,
            &resource.descriptor().state_scope,
        )
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Committed);
}

#[test]
fn multi_partition_drain_restart_uses_persisted_partition_continuation() {
    let temp = tempfile::tempdir().unwrap();
    let resource = DurableMultiPartitionDrainResource::new();
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let package_id = "pkg-drain-durable-continuation";
    let pipeline_id = PipelineId::new("pipeline-drain-durable-continuation").unwrap();
    let mut source = compiled_drain_test_source_plan(&resource);
    source.execution_capabilities.maximum_concurrency = 1;
    source.execution_capabilities.useful_concurrency = 1;
    source.validate().unwrap();
    let bound = BoundTestResource {
        inner: &resource,
        compiled_source_plan_hash: source.compiled_source_plan_hash().unwrap(),
        replay_retention: None,
    };
    let extent = ExecutionExtent::Drain {
        version: cdf_kernel::EXECUTION_EXTENT_VERSION,
        policy: cdf_kernel::StreamEpochPolicy {
            version: cdf_kernel::STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: cdf_kernel::EpochClosureTrigger::Rows { count: 2 },
            package_rotation: cdf_kernel::EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: cdf_kernel::WatermarkPolicy::Disabled,
            late_data: cdf_kernel::LateDataAction::Quarantine,
            safe_frontier: cdf_kernel::SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: cdf_kernel::DrainTermination::Records { count: 3 },
    };
    let mut plan = live_plan_for_queryable(&resource, package_id);
    plan.execution_extent = extent.clone();
    plan.explain.execution_extent = extent;
    let destination =
        crate::test_destinations::duckdb(&duckdb_path, TargetName::new("events").unwrap()).unwrap();
    let plan = plan
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(&source, &destination.runtime_capabilities())
        .unwrap();
    let retry_plan = plan.clone();

    let first = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: pipeline_id.clone(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-drain-durable-continuation").unwrap(),
        destination,
        run_id: Some(RunId::new("run-drain-durable-continuation-first").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();
    assert!(
        first
            .to_string()
            .contains("injected failure after the first multi-partition checkpoint"),
        "{first}"
    );

    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let first_head = store
        .head(
            &pipeline_id,
            &resource.descriptor().resource_id,
            &resource.descriptor().state_scope,
        )
        .unwrap()
        .expect("first epoch checkpoint");
    assert_eq!(
        first_head.delta.output_position,
        SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(100),
        })
    );
    let SourcePosition::Composite(first_continuation) = first_head
        .delta
        .source_continuation
        .as_ref()
        .expect("durable source continuation")
    else {
        panic!("multi-partition restart authority must remain partition-keyed");
    };
    assert_eq!(
        first_continuation.positions.get("part-b"),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(1),
        }))
    );

    let destination =
        crate::test_destinations::duckdb(&duckdb_path, TargetName::new("events").unwrap()).unwrap();
    let resumed = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan: retry_plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: pipeline_id.clone(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-drain-durable-continuation").unwrap(),
        destination,
        run_id: Some(RunId::new("run-drain-durable-continuation-second").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    assert_eq!(resumed.row_count, 1);
    let history = SqliteCheckpointStore::open(&state_path)
        .unwrap()
        .history(
            &pipeline_id,
            &resource.descriptor().resource_id,
            &resource.descriptor().state_scope,
        )
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(
        history[1].delta.parent_checkpoint_id,
        Some(history[0].delta.checkpoint_id.clone())
    );
    let SourcePosition::Composite(final_continuation) = history[1]
        .delta
        .source_continuation
        .as_ref()
        .expect("final durable source continuation")
    else {
        panic!("final multi-partition restart authority must remain partition-keyed");
    };
    assert_eq!(
        final_continuation.positions.get("part-a"),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(100),
        }))
    );
    assert_eq!(
        final_continuation.positions.get("part-b"),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(2),
        }))
    );
    let committed_rows = history
        .iter()
        .map(|checkpoint| {
            checkpoint
                .receipt
                .as_ref()
                .expect("committed checkpoint receipt")
                .counts
                .rows_written
        })
        .sum::<u64>();
    assert_eq!(committed_rows, 3);
    let opens = resource.opens.lock().unwrap();
    assert!(opens.contains(&(
        "part-a".to_owned(),
        Some(SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(100),
        })),
    )));
    assert!(opens.contains(&(
        "part-b".to_owned(),
        Some(SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(1),
        })),
    )));
}

#[test]
fn drain_project_does_not_publish_a_later_epoch_before_checkpoint_settlement() {
    let temp = tempfile::tempdir().unwrap();
    let resource = multi_file_resource(temp.path());
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let package_id = "pkg-drain-settlement-failure";
    let pipeline_id = PipelineId::new("pipeline-drain-settlement-failure").unwrap();
    let source = compiled_drain_test_source_plan(&resource);
    let bound = BoundTestResource {
        inner: &resource,
        compiled_source_plan_hash: source.compiled_source_plan_hash().unwrap(),
        replay_retention: None,
    };
    let extent = ExecutionExtent::Drain {
        version: cdf_kernel::EXECUTION_EXTENT_VERSION,
        policy: cdf_kernel::StreamEpochPolicy {
            version: cdf_kernel::STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: cdf_kernel::EpochClosureTrigger::Rows { count: 1 },
            package_rotation: cdf_kernel::EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: cdf_kernel::WatermarkPolicy::Disabled,
            late_data: cdf_kernel::LateDataAction::Quarantine,
            safe_frontier: cdf_kernel::SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: cdf_kernel::DrainTermination::Records { count: 2 },
    };
    let mut plan = live_plan_for_queryable(&resource, package_id);
    plan.execution_extent = extent.clone();
    plan.explain.execution_extent = extent;
    let resolved_destination =
        crate::test_destinations::duckdb(&duckdb_path, TargetName::new("events").unwrap()).unwrap();
    let plan = plan
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(&source, &resolved_destination.runtime_capabilities())
        .unwrap();
    let resume_plan = plan.clone();
    let hook = |_receipt: &Receipt| {
        Err(CdfError::internal(
            "injected drain checkpoint settlement failure",
        ))
    };

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: pipeline_id.clone(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-drain-settlement-failure").unwrap(),
        destination: resolved_destination,
        run_id: Some(RunId::new("run-drain-settlement-failure").unwrap()),
        event_sink: None,
        after_receipt_verified: Some(&hook),
    }))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("injected drain checkpoint settlement failure"),
        "{error}"
    );
    let first_package = package_root.join(package_id);
    assert_eq!(package_status(&first_package), PackageStatus::Loading);
    let receipts = package_receipts(&first_package);
    assert_eq!(receipts.len(), 1);
    assert!(
        destination(&duckdb_path)
            .verify_receipt(&receipts[0])
            .unwrap()
            .verified
    );
    assert!(
        !package_root
            .join("pkg-drain-settlement-failure-epoch-00000000000000000001")
            .exists()
    );

    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let scope = resource.descriptor().state_scope.clone();
    assert!(
        store
            .head(&pipeline_id, &resource.descriptor().resource_id, &scope)
            .unwrap()
            .is_none()
    );
    let history = store
        .history(&pipeline_id, &resource.descriptor().resource_id, &scope)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Proposed);

    let resumed = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan: resume_plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: pipeline_id.clone(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-drain-settlement-failure").unwrap(),
        destination: crate::test_destinations::duckdb(
            &duckdb_path,
            TargetName::new("events").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-drain-settlement-resumed").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();
    assert_eq!(package_status(&first_package), PackageStatus::Checkpointed);
    assert_eq!(resumed.row_count, 1);
    assert_eq!(resumed.drain.as_ref().unwrap().epoch_count, 1);
    assert_eq!(
        output_manifest_paths(&resumed),
        vec!["events-a.ndjson", "events-b.ndjson"]
    );
    assert_eq!(single_segment_manifest_path(&resumed), "events-b.ndjson");
    let resumed_history = store
        .history(&pipeline_id, &resource.descriptor().resource_id, &scope)
        .unwrap();
    assert_eq!(resumed_history.len(), 2);
    assert!(
        resumed_history
            .iter()
            .all(|checkpoint| checkpoint.status == CheckpointStatus::Committed)
    );
    assert_eq!(
        resumed_history[1].delta.input_position,
        Some(resumed_history[0].delta.output_position.clone())
    );
}

#[test]
fn general_project_run_records_bounded_complete_phase_telemetry() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let report = futures_executor::block_on(run_project_with_telemetry(
        project_run_request(
            &resource,
            "pkg-general-phase-telemetry",
            &temp.path().join(".cdf/packages"),
            &temp.path().join(".cdf/dev.duckdb"),
            &temp.path().join(".cdf/state.db"),
            "run-general-phase-telemetry",
        ),
        RunTelemetryConfig::phase_metrics(),
    ))
    .unwrap();

    let metrics = report
        .ledger_snapshot
        .events
        .iter()
        .filter_map(|event| match event.details.attributes.get("metric") {
            Some(RunEventValue::PhaseMetric(metric)) => Some(metric),
            _ => None,
        })
        .collect::<Vec<&RunPhaseMetric>>();
    assert!(!metrics.is_empty());
    assert!(metrics.len() <= usize::from(RunTelemetryConfig::phase_metrics().max_phase_events));
    assert!(metrics.iter().all(|metric| {
        metric.status == RunPhaseStatus::Completed
            && metric.duration_ns > 0
            && metric.operations > 0
    }));
    for phase in [
        RunPhase::PackageExecution,
        RunPhase::SourceRead,
        RunPhase::Decode,
        RunPhase::ValidationNormalization,
        RunPhase::SegmentEncode,
        RunPhase::PersistHash,
        RunPhase::PackageFinalize,
        RunPhase::DestinationIngress,
        RunPhase::DestinationWriteReceipt,
        RunPhase::CheckpointGate,
    ] {
        assert!(
            metrics.iter().any(|metric| metric.phase == phase),
            "missing {phase:?}"
        );
    }
    let source_read = metrics
        .iter()
        .find(|metric| metric.phase == RunPhase::SourceRead)
        .expect("file run omitted source I/O telemetry");
    assert!(matches!(
        source_read.context,
        Some(cdf_kernel::RunPhaseContext::SourceRead { .. })
    ));
    assert!(source_read.input_bytes > 0);
    assert!(source_read.output_bytes > 0);
    assert!(source_read.operations > 0);
    assert!(
        metrics
            .iter()
            .any(|metric| metric.input_bytes > 0 || metric.output_bytes > 0)
    );
}

#[test]
fn general_project_run_commits_multi_file_resource_manifest_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let resource = multi_file_resource(temp.path());
    let package_id = "pkg-general-multi-file-manifest";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let report = futures_executor::block_on(run_project(project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-general-multi-file-manifest",
    )))
    .unwrap();

    assert_eq!(report.row_count, 2);
    assert_eq!(report.segment_count, 2);
    let SourcePosition::FileManifest(manifest) = &report.checkpoint.delta.output_position else {
        panic!("checkpoint output position should be a file manifest");
    };
    let manifest_paths = manifest
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    assert_eq!(manifest_paths, vec!["events-a.ndjson", "events-b.ndjson"]);
    assert!(manifest.files.iter().all(|file| file.size_bytes > 0));
    assert!(manifest.files.iter().all(|file| file.sha256.is_some()));

    let mut segment_paths = report
        .checkpoint
        .delta
        .segments
        .iter()
        .map(|segment| match &segment.output_position {
            SourcePosition::FileManifest(manifest) => {
                assert_eq!(manifest.files.len(), 1);
                assert!(manifest.files[0].sha256.is_some());
                manifest.files[0].path.clone()
            }
            other => panic!("state segment should retain file manifest evidence: {other:?}"),
        })
        .collect::<Vec<_>>();
    segment_paths.sort();
    assert_eq!(segment_paths, manifest_paths);
}

#[test]
fn file_manifest_append_run_skips_unchanged_files_and_loads_only_changes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = multi_file_resource(temp.path());
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let first = futures_executor::block_on(run_project(project_run_request(
        &resource,
        "pkg-file-manifest-incremental-1",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-file-manifest-incremental-1",
    )))
    .unwrap();
    assert_eq!(first.row_count, 2);
    assert_eq!(first.segment_count, 2);
    assert_eq!(
        first.file_manifest,
        Some(FileManifestRunSummary {
            total_file_count: 2,
            changed_file_count: 2,
            unchanged_file_count: 0,
        })
    );
    assert_eq!(
        output_manifest_paths(&first),
        vec!["events-a.ndjson", "events-b.ndjson"]
    );
    let first_b_sha = output_manifest_file(&first, "events-b.ndjson")
        .sha256
        .clone();

    let unchanged = futures_executor::block_on(run_project_outcome(project_run_request(
        &resource,
        "pkg-file-manifest-incremental-2",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-file-manifest-incremental-2",
    )))
    .unwrap();
    let ProjectRunOutcome::NoOp(unchanged) = unchanged else {
        panic!("unchanged file manifest should produce an explicit no-op outcome");
    };
    assert_eq!(
        unchanged.reason,
        ProjectRunNoOpReason::FileManifestUnchanged
    );
    assert_eq!(
        unchanged.file_manifest,
        Some(FileManifestRunSummary {
            total_file_count: 2,
            changed_file_count: 0,
            unchanged_file_count: 2,
        })
    );
    assert_eq!(unchanged.current_checkpoint, Some(first.checkpoint.clone()));
    assert!(
        !package_root
            .join("pkg-file-manifest-incremental-2")
            .exists()
    );
    assert_eq!(
        unchanged.ledger_snapshot.events.len(),
        3,
        "no-op run should not emit package, destination, or checkpoint events"
    );
    assert_eq!(
        unchanged.ledger_snapshot.events[1]
            .details
            .attributes
            .get("planned_packages"),
        Some(&RunEventValue::U64(0))
    );

    fs::write(
        temp.path().join("data/events-c.ndjson"),
        "{\"id\":3,\"name\":\"katherine\"}\n",
    )
    .unwrap();
    let resource = compile_test_file_resource(temp.path(), MULTI_FILE_RESOURCE_APPEND);
    let added = futures_executor::block_on(run_project(project_run_request(
        &resource,
        "pkg-file-manifest-incremental-3",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-file-manifest-incremental-3",
    )))
    .unwrap();
    assert_eq!(added.row_count, 1);
    assert_eq!(added.segment_count, 1);
    assert_eq!(single_segment_manifest_path(&added), "events-c.ndjson");
    assert_eq!(
        added.file_manifest,
        Some(FileManifestRunSummary {
            total_file_count: 3,
            changed_file_count: 1,
            unchanged_file_count: 2,
        })
    );
    assert_eq!(
        output_manifest_paths(&added),
        vec!["events-a.ndjson", "events-b.ndjson", "events-c.ndjson"]
    );
    assert_eq!(
        added.checkpoint.delta.parent_checkpoint_id.as_ref(),
        Some(&first.checkpoint.delta.checkpoint_id)
    );
    let reader = PackageReader::open(&added.package_dir).unwrap();
    assert_eq!(
        package_id_name_rows(&reader),
        vec![(3, Some("katherine".to_owned()))]
    );

    fs::write(
        temp.path().join("data/events-b.ndjson"),
        "{\"id\":4,\"name\":\"grace\"}\n",
    )
    .unwrap();
    let changed = futures_executor::block_on(run_project(project_run_request(
        &resource,
        "pkg-file-manifest-incremental-4",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-file-manifest-incremental-4",
    )))
    .unwrap();
    assert_eq!(changed.row_count, 1);
    assert_eq!(changed.segment_count, 1);
    assert_eq!(single_segment_manifest_path(&changed), "events-b.ndjson");
    assert_eq!(
        changed.file_manifest,
        Some(FileManifestRunSummary {
            total_file_count: 3,
            changed_file_count: 1,
            unchanged_file_count: 2,
        })
    );
    assert_eq!(
        output_manifest_paths(&changed),
        vec!["events-a.ndjson", "events-b.ndjson", "events-c.ndjson"]
    );
    assert_ne!(
        output_manifest_file(&changed, "events-b.ndjson").sha256,
        first_b_sha
    );
    let reader = PackageReader::open(&changed.package_dir).unwrap();
    assert_eq!(
        package_id_name_rows(&reader),
        vec![(4, Some("grace".to_owned()))]
    );
}

#[test]
fn bounded_table_snapshot_run_rebinds_unchanged_frontier_to_no_op() {
    let temp = tempfile::tempdir().unwrap();
    let resource = TableSnapshotMockResource::new();
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let first = futures_executor::block_on(run_project(project_run_request(
        &resource,
        "pkg-table-snapshot-1",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-table-snapshot-1",
    )))
    .unwrap();
    assert_eq!(first.row_count, 1);
    assert_eq!(first.checkpoint.delta.output_position, resource.position());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 1);

    let unchanged = futures_executor::block_on(run_project_outcome(project_run_request(
        &resource,
        "pkg-table-snapshot-2",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-table-snapshot-2",
    )))
    .unwrap();
    let ProjectRunOutcome::NoOp(unchanged) = unchanged else {
        panic!("unchanged table snapshot should produce an explicit no-op outcome");
    };
    assert_eq!(
        unchanged.reason,
        ProjectRunNoOpReason::SourcePositionUnchanged
    );
    assert_eq!(unchanged.current_checkpoint, Some(first.checkpoint));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 1);
    assert!(!package_root.join("pkg-table-snapshot-2").exists());
    assert_eq!(
        unchanged.ledger_snapshot.events.len(),
        3,
        "snapshot no-op must not emit package, destination, or checkpoint events"
    );
}

#[test]
fn file_manifest_noop_rejects_source_binding_and_schedule_tampering_before_subsetting() {
    let temp = tempfile::tempdir().unwrap();
    let resource = multi_file_resource(temp.path());
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    futures_executor::block_on(run_project(project_run_request(
        &resource,
        "pkg-file-manifest-authority-1",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-file-manifest-authority-1",
    )))
    .unwrap();

    let source_plan = compiled_test_source_plan(&resource);
    let mut source_tamper = project_run_request(
        &resource,
        "pkg-file-manifest-authority-2",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-file-manifest-authority-2",
    );
    source_tamper.plan = source_tamper
        .plan
        .bind_compiled_source(&source_plan)
        .unwrap();
    let error = futures_executor::block_on(run_project(source_tamper)).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("resolved source does not match the compiler source artifact"),
        "{error}"
    );
    assert!(!package_root.join("pkg-file-manifest-authority-2").exists());

    let mut schedule_tamper = project_run_request(
        &resource,
        "pkg-file-manifest-authority-3",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-file-manifest-authority-3",
    );
    schedule_tamper.plan = schedule_tamper
        .plan
        .bind_compiled_source(&resource.source_plan)
        .unwrap();
    schedule_tamper.plan.explain.partition_schedule = None;
    let error = futures_executor::block_on(run_project(schedule_tamper)).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("engine partition schedule does not match its recorded explain schedule"),
        "{error}"
    );
    assert!(!package_root.join("pkg-file-manifest-authority-3").exists());
}

#[test]
fn file_manifest_replace_run_keeps_planning_all_files() {
    let temp = tempfile::tempdir().unwrap();
    let resource = replace_multi_file_resource(temp.path());
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let first = futures_executor::block_on(run_project(project_run_request(
        &resource,
        "pkg-file-manifest-replace-1",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-file-manifest-replace-1",
    )))
    .unwrap();
    let second = futures_executor::block_on(run_project(project_run_request(
        &resource,
        "pkg-file-manifest-replace-2",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-file-manifest-replace-2",
    )))
    .unwrap();

    assert_eq!(first.row_count, 2);
    assert_eq!(first.segment_count, 2);
    assert_eq!(second.row_count, 2);
    assert_eq!(second.segment_count, 2);
    assert_eq!(
        second.file_manifest,
        Some(FileManifestRunSummary {
            total_file_count: 2,
            changed_file_count: 2,
            unchanged_file_count: 0,
        })
    );
    assert_eq!(
        output_manifest_paths(&second),
        vec!["events-a.ndjson", "events-b.ndjson"]
    );
}

#[test]
fn general_project_run_live_sink_events_match_persisted_ledger_order() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-general-live-sink-order";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let sink = RecordingRunEventSink::unbounded_with_persistence_check(&state_path);
    let mut request = project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-general-live-sink-order",
    );
    request.event_sink = Some(&sink);

    let report = futures_executor::block_on(run_project(request)).unwrap();

    let live_events = sink.events();
    assert_eq!(live_events, report.ledger_snapshot.events);
    assert_eq!(
        live_events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        report
            .ledger_snapshot
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>()
    );
    let ledger = SqliteRunLedger::open(&state_path).unwrap();
    assert_eq!(ledger.events(&report.run_id).unwrap(), live_events);
    assert_run_artifact_identity_unchanged(&report);
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert!(
        DuckDbDestination::new(&duckdb_path)
            .unwrap()
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
}

#[test]
fn general_project_run_live_sink_drops_do_not_fail_run_or_truncate_ledger() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-general-live-sink-drop";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let sink = RecordingRunEventSink::bounded(0);
    let mut request = project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-general-live-sink-drop",
    );
    request.event_sink = Some(&sink);

    let report = futures_executor::block_on(run_project(request)).unwrap();

    let live_events = sink.events();
    assert!(live_events.is_empty());
    assert_eq!(
        sink.drop_count(),
        u64::try_from(report.ledger_snapshot.events.len()).unwrap()
    );
    let ledger = SqliteRunLedger::open(&state_path).unwrap();
    assert_eq!(
        ledger.events(&report.run_id).unwrap(),
        report.ledger_snapshot.events
    );
    assert_eq!(
        report
            .ledger_snapshot
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![
            RunEventKind::RunStarted,
            RunEventKind::PlanRecorded,
            RunEventKind::PackageStarted,
            RunEventKind::PackageSegmentRecorded,
            RunEventKind::PackageFinalized,
            RunEventKind::ValidationDepthTransitionRecorded,
            RunEventKind::CheckpointProposed,
            RunEventKind::DestinationCommitStarted,
            RunEventKind::DestinationSegmentAcknowledged,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::CheckpointCommitted,
            RunEventKind::PackageStatusUpdated,
            RunEventKind::RunSucceeded,
        ]
    );
    assert_run_artifact_identity_unchanged(&report);
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(report.row_count, 2);
}

#[test]
fn general_project_run_tracing_bridge_emits_structured_runtime_events() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-general-tracing-bridge";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let tracing_sink = TracingRunEventSink::new();
    let subscriber = CapturingTracingSubscriber::default();
    let mut request = project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-general-tracing-bridge",
    );
    request.event_sink = Some(&tracing_sink);

    let report = tracing::subscriber::with_default(subscriber.clone(), || {
        futures_executor::block_on(run_project(request))
    })
    .unwrap();

    let traced_events = runtime_trace_events(&subscriber);
    assert_eq!(traced_events.len(), report.ledger_snapshot.events.len());
    for (traced, persisted) in traced_events.iter().zip(&report.ledger_snapshot.events) {
        assert_eq!(traced.fields, expected_runtime_trace_fields(persisted));
    }
    let finalized = traced_events
        .iter()
        .find(|event| {
            event.fields.get("event_kind").map(String::as_str) == Some("package_finalized")
        })
        .expect("package_finalized trace event");
    assert_eq!(
        finalized.fields.get("package_hash").map(String::as_str),
        Some(report.package_hash.as_str())
    );
    let checkpoint = traced_events
        .iter()
        .find(|event| {
            event.fields.get("event_kind").map(String::as_str) == Some("checkpoint_committed")
        })
        .expect("checkpoint_committed trace event");
    assert_eq!(
        checkpoint.fields.get("checkpoint_id").map(String::as_str),
        Some(report.checkpoint.delta.checkpoint_id.as_str())
    );
    let receipt = traced_events
        .iter()
        .find(|event| {
            event.fields.get("event_kind").map(String::as_str)
                == Some("destination_receipt_recorded")
        })
        .expect("destination_receipt_recorded trace event");
    assert_eq!(
        receipt.fields.get("receipt_id").map(String::as_str),
        Some(report.receipt.receipt_id.as_str())
    );
    let ledger = SqliteRunLedger::open(&state_path).unwrap();
    assert_eq!(
        ledger.events(&report.run_id).unwrap(),
        report.ledger_snapshot.events
    );
    assert_run_artifact_identity_unchanged(&report);
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
}

#[test]
fn runtime_tracing_bridge_drops_unredacted_details_before_emit() {
    let tracing_sink = TracingRunEventSink::new();
    let subscriber = CapturingTracingSubscriber::default();
    let raw_secret = run_event_for_tracing_details(RunEventDetails::new([(
        "api_token",
        RunEventValue::String("super-secret-token".to_owned()),
    )]));

    let result = tracing::subscriber::with_default(subscriber.clone(), || {
        tracing_sink.try_emit(&raw_secret)
    });

    assert_eq!(result, RunEventSinkResult::Dropped);
    assert!(runtime_trace_events(&subscriber).is_empty());

    let typed_secret = run_event_for_tracing_details(RunEventDetails::new([(
        "api_token",
        RunEventValue::SecretRef(SecretReference::new("secret://env/API_TOKEN").unwrap()),
    )]));
    let result = tracing::subscriber::with_default(subscriber.clone(), || {
        tracing_sink.try_emit(&typed_secret)
    });

    assert_eq!(result, RunEventSinkResult::Accepted);
    let traced_events = runtime_trace_events(&subscriber);
    assert_eq!(traced_events.len(), 1);
    assert_eq!(
        traced_events[0].fields,
        expected_runtime_trace_fields(&typed_secret)
    );
    let details = traced_events[0].fields.get("details").unwrap();
    assert!(details.contains("secret://env/API_TOKEN"));
    assert!(!details.contains("super-secret-token"));
}

#[test]
fn stale_normalizer_version_fails_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-stale-normalizer-version";
    let package_root = temp.path().join(".cdf/packages");
    let destination_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut plan = live_plan(&resource, package_id);
    plan.validation_program.normalizer_version = "namecase-v0-stale".to_owned();

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&resource),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-stale-normalizer-version").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-stale-normalizer-version").unwrap(),
        destination: crate::test_destinations::duckdb(
            &destination_path,
            TargetName::new("events").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-stale-normalizer-version").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("normalization program is stale"));
    assert!(message.contains("normalizer_version"));
    assert!(message.contains("rebuild the plan for the selected destination"));
    assert!(!package_root.join(package_id).exists());
    assert!(!destination_path.exists());
    assert!(!state_path.exists());
}

#[test]
fn project_run_rejects_plan_package_id_mismatch_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut request = project_run_request(
        &resource,
        "pkg-live-request-id",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-live-plan-id",
    );
    request.plan = live_plan(&resource, "pkg-live-plan-id");
    let error = futures_executor::block_on(run_project(request)).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not match explicit package id")
    );
    assert!(!package_root.join("pkg-live-request-id").exists());
    assert!(!package_root.join("pkg-live-plan-id").exists());
    assert!(!duckdb_path.exists());
    assert!(!state_path.exists());
}

#[test]
fn state_delta_rejects_partial_execution_even_with_an_earlier_complete_observation() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let output = engine_output_with_positions_and_checkpoint_eligibility(
        &temp.path().join("pkg-partial-state"),
        "pkg-partial-state",
        vec![file_position("/tmp/cdf/partial.ndjson")],
        false,
    );
    let request = state_delta_request(&resource, "pkg-partial-state");

    let error = state_delta_from_run(
        &request,
        &output,
        &SchemaHash::new(SCHEMA_HASH).unwrap(),
        &resource.descriptor().state_scope,
        None,
    )
    .unwrap_err();

    assert!(
        error
            .message
            .contains("partial or limited source execution")
    );
}

#[test]
fn state_delta_aggregates_file_manifest_positions_deterministically() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());

    let delta = state_delta_for_positions(
        &resource,
        temp.path(),
        "pkg-state-delta-file-manifest-aggregate",
        vec![
            file_position("/tmp/cdf/z.ndjson"),
            file_position("/tmp/cdf/a.ndjson"),
            file_position("/tmp/cdf/a.ndjson"),
        ],
    )
    .unwrap();

    let SourcePosition::FileManifest(manifest) = &delta.output_position else {
        panic!("output position should be a file manifest");
    };
    assert_eq!(
        manifest
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/tmp/cdf/a.ndjson", "/tmp/cdf/z.ndjson"]
    );
    assert_eq!(delta.segments.len(), 3);
    assert_eq!(
        delta.segments[0].output_position,
        file_position("/tmp/cdf/z.ndjson")
    );
    assert_eq!(
        delta.segments[1].output_position,
        file_position("/tmp/cdf/a.ndjson")
    );
}

#[test]
fn state_delta_merges_append_file_manifest_output_with_head() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let previous = state_delta_for_positions(
        &resource,
        temp.path(),
        "pkg-state-delta-file-manifest-previous",
        vec![
            file_position_with_identity("events-a.ndjson", 11, Some("sha256:a".to_owned())),
            file_position_with_identity("events-b.ndjson", 12, Some("sha256:b-old".to_owned())),
        ],
    )
    .unwrap();
    let head = Checkpoint {
        delta: previous,
        status: CheckpointStatus::Committed,
        receipt: None,
        is_head: true,
        created_at_ms: 1,
        committed_at_ms: Some(1),
        rewind_target_checkpoint_id: None,
    };
    let package_id = "pkg-state-delta-file-manifest-merge-head";
    let output = engine_output_with_positions(
        &temp.path().join(package_id),
        package_id,
        vec![
            file_position_with_identity("events-b.ndjson", 99, Some("sha256:b-new".to_owned())),
            file_position_with_identity("events-c.ndjson", 13, Some("sha256:c".to_owned())),
        ],
    );
    let request = state_delta_request(&resource, package_id);

    let delta = state_delta_from_run(
        &request,
        &output,
        &SchemaHash::new(SCHEMA_HASH).unwrap(),
        &resource.descriptor().state_scope,
        Some(&head),
    )
    .unwrap();

    let SourcePosition::FileManifest(manifest) = &delta.output_position else {
        panic!("output position should be a file manifest");
    };
    assert_eq!(
        manifest
            .files
            .iter()
            .map(|file| (file.path.clone(), file.size_bytes, file.sha256.clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                "events-a.ndjson".to_owned(),
                11,
                Some("sha256:a".to_owned())
            ),
            (
                "events-b.ndjson".to_owned(),
                99,
                Some("sha256:b-new".to_owned()),
            ),
            (
                "events-c.ndjson".to_owned(),
                13,
                Some("sha256:c".to_owned())
            ),
        ]
    );
    assert_eq!(delta.segments.len(), 2);
    assert_eq!(delta.parent_checkpoint_id, Some(head.delta.checkpoint_id));
}

#[test]
fn state_delta_rejects_conflicting_duplicate_file_manifest_entries() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());

    let error = state_delta_for_positions(
        &resource,
        temp.path(),
        "pkg-state-delta-file-manifest-conflict",
        vec![
            file_position_with_identity("/tmp/cdf/a.ndjson", 42, Some("sha256:first".to_owned())),
            file_position_with_identity("/tmp/cdf/a.ndjson", 42, Some("sha256:second".to_owned())),
        ],
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("conflicting file manifest evidence")
    );
}

#[test]
fn state_delta_rejects_mixed_file_and_non_file_source_positions() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());

    let error = state_delta_for_positions(
        &resource,
        temp.path(),
        "pkg-state-delta-mixed-file-log",
        vec![
            file_position("/tmp/cdf/a.ndjson"),
            postgres_log_position("orders", 11),
        ],
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("mixed, divergent, or opaque unordered positions"),
        "{error}"
    );
}

#[test]
fn state_delta_preserves_engine_canonical_file_manifest_entries() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-state-delta-file-scope-normalize";
    let output = engine_output_with_positions(
        &temp.path().join(package_id),
        package_id,
        vec![file_position("events-a.ndjson")],
    );
    let request = state_delta_request(&resource, package_id);
    let scope = ScopeKey::File {
        path: "events-a.ndjson".to_owned(),
    };

    let delta = state_delta_from_run(
        &request,
        &output,
        &SchemaHash::new(SCHEMA_HASH).unwrap(),
        &scope,
        None,
    )
    .unwrap();

    let SourcePosition::FileManifest(output_manifest) = &delta.output_position else {
        panic!("output position should be a file manifest");
    };
    assert_eq!(output_manifest.files[0].path, "events-a.ndjson");
    assert_eq!(
        output_manifest.files[0].sha256.as_deref(),
        Some("sha256:0000000000000000000000000000000000000000000000000000000000000000")
    );
    let SourcePosition::FileManifest(segment_manifest) = &delta.segments[0].output_position else {
        panic!("state segment should retain file manifest evidence");
    };
    assert_eq!(segment_manifest.files[0].path, "events-a.ndjson");
}

#[test]
fn state_delta_joins_already_closed_timestamp_cursor_positions_without_second_lag() {
    let temp = tempfile::tempdir().unwrap();
    let resource = rest_cursor_runtime_resource(
        "updated_at",
        r#"{ name = "updated_at", type = "timestamp_micros", nullable = false, timezone = "UTC" }"#,
        "best_effort",
        "5m",
    );

    let delta = state_delta_for_positions(
        &resource,
        temp.path(),
        "pkg-state-delta-window-close-timestamp",
        vec![
            cursor_position(
                "updated_at",
                CursorValue::TimestampMicros {
                    micros: 60_000_000,
                    timezone: Some("UTC".to_owned()),
                },
            ),
            cursor_position(
                "updated_at",
                CursorValue::TimestampMicros {
                    micros: 600_000_000,
                    timezone: Some("UTC".to_owned()),
                },
            ),
        ],
    )
    .unwrap();

    assert_eq!(
        delta.output_position,
        cursor_position(
            "updated_at",
            CursorValue::TimestampMicros {
                micros: 600_000_000,
                timezone: Some("UTC".to_owned()),
            },
        )
    );
    assert_eq!(
        delta.segments[0].output_position,
        cursor_position(
            "updated_at",
            CursorValue::TimestampMicros {
                micros: 60_000_000,
                timezone: Some("UTC".to_owned()),
            },
        )
    );
    assert_eq!(
        delta.segments[1].output_position,
        cursor_position(
            "updated_at",
            CursorValue::TimestampMicros {
                micros: 600_000_000,
                timezone: Some("UTC".to_owned()),
            },
        )
    );
}

#[test]
fn state_delta_joins_already_closed_date_cursor_positions_without_second_lag() {
    let temp = tempfile::tempdir().unwrap();
    let resource = rest_cursor_runtime_resource(
        "event_day",
        r#"{ name = "event_day", type = "date32", nullable = false }"#,
        "best_effort",
        "2d",
    );

    let delta = state_delta_for_positions(
        &resource,
        temp.path(),
        "pkg-state-delta-window-close-date",
        vec![
            cursor_position("event_day", CursorValue::I64(3)),
            cursor_position("event_day", CursorValue::I64(9)),
        ],
    )
    .unwrap();

    assert_eq!(
        delta.output_position,
        cursor_position("event_day", CursorValue::I64(9))
    );
    assert_eq!(
        delta.segments[0].output_position,
        cursor_position("event_day", CursorValue::I64(3))
    );
    assert_eq!(
        delta.segments[1].output_position,
        cursor_position("event_day", CursorValue::I64(9))
    );
}

#[test]
fn state_delta_rejects_page_token_only_and_mixed_cursor_positions() {
    let temp = tempfile::tempdir().unwrap();
    let resource = rest_cursor_runtime_resource(
        "updated_at",
        r#"{ name = "updated_at", type = "int64", nullable = false }"#,
        "best_effort",
        "5ms",
    );

    let page_token_error = state_delta_for_positions(
        &resource,
        temp.path(),
        "pkg-state-delta-page-token-only",
        vec![SourcePosition::PageToken(PageToken {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            token: "next-page".to_owned(),
        })],
    )
    .unwrap_err();
    assert!(page_token_error.to_string().contains("page-token-only"));

    let mixed_position = SourcePosition::Composite(CompositePosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        positions: BTreeMap::from([
            (
                "cursor".to_owned(),
                cursor_position("updated_at", CursorValue::I64(10)),
            ),
            (
                "page".to_owned(),
                SourcePosition::PageToken(PageToken {
                    version: cdf_kernel::SOURCE_POSITION_VERSION,
                    token: "next-page".to_owned(),
                }),
            ),
        ]),
    });
    let mixed_error = state_delta_for_positions(
        &resource,
        temp.path(),
        "pkg-state-delta-mixed-cursor-page-token",
        vec![mixed_position],
    )
    .unwrap_err();
    assert!(mixed_error.to_string().contains("mixed cursor/page-token"));
}

#[test]
fn state_delta_rejects_divergent_non_file_source_position_variants() {
    let temp = tempfile::tempdir().unwrap();
    let resource = rest_cursor_runtime_resource(
        "updated_at",
        r#"{ name = "updated_at", type = "int64", nullable = false }"#,
        "best_effort",
        "5ms",
    );

    let error = state_delta_for_positions(
        &resource,
        temp.path(),
        "pkg-state-delta-divergent-non-file-variants",
        vec![
            cursor_position("updated_at", CursorValue::I64(10)),
            postgres_log_position("orders", 11),
        ],
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("divergent source-position variants")
    );
}

#[test]
fn state_delta_rejects_incompatible_cursor_fields_and_values_but_never_reapplies_lag() {
    let temp = tempfile::tempdir().unwrap();
    let numeric_resource = rest_cursor_runtime_resource(
        "updated_at",
        r#"{ name = "updated_at", type = "int64", nullable = false }"#,
        "best_effort",
        "5ms",
    );
    let field_error = state_delta_for_positions(
        &numeric_resource,
        temp.path(),
        "pkg-state-delta-incompatible-cursor-field",
        vec![cursor_position("other", CursorValue::I64(10))],
    )
    .unwrap_err();
    assert!(
        field_error
            .to_string()
            .contains("does not match resource cursor field")
    );

    let string_resource = rest_cursor_runtime_resource(
        "name",
        r#"{ name = "name", type = "string", nullable = false }"#,
        "best_effort",
        "0ms",
    );
    let value_error = state_delta_for_positions(
        &string_resource,
        temp.path(),
        "pkg-state-delta-unsupported-cursor-value",
        vec![cursor_position(
            "name",
            CursorValue::String("unsupported".to_owned()),
        )],
    )
    .unwrap_err();
    assert!(
        value_error
            .to_string()
            .contains("unsupported cursor value kind")
    );

    let unsigned_resource = rest_cursor_runtime_resource(
        "updated_at",
        r#"{ name = "updated_at", type = "u_int64", nullable = false }"#,
        "best_effort",
        "5ms",
    );
    let delta = state_delta_for_positions(
        &unsigned_resource,
        temp.path(),
        "pkg-state-delta-closed-unsigned-cursor",
        vec![cursor_position("updated_at", CursorValue::U64(3))],
    )
    .unwrap();
    assert_eq!(
        delta.output_position,
        cursor_position("updated_at", CursorValue::U64(3))
    );
}
