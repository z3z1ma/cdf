use std::{
    collections::{BTreeMap, VecDeque},
    env, fmt, fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{
    AnomalyFact, ContractPolicy, DedupKeep, ObservedSchema, RowRule, compile_validation_program,
};
use cdf_dest_duckdb::DuckDbDestination;
use cdf_dest_parquet::ParquetDestination;
use cdf_dest_postgres::{MergeDedupPolicy, PostgresDestination, PostgresTarget};
use cdf_engine::{
    EnginePlan, EnginePlanInput, EngineRunOutput, EngineRunOutputWithSegmentPositions,
    EngineSegmentPosition, ExecutionProfile, LineageSummary, PlanBoundedness, Planner,
    negotiate_scan_plan,
};
use cdf_http::{HttpRequest, HttpResponse, HttpTransport, SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{
    BackpressureSupport, BatchStream, CHECKPOINT_STATE_VERSION, CapabilitySupport, CdfError,
    Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore, CommitCounts, CommitPlan,
    CommitSegment, CommitSession, CompositePosition, ConcurrencyLimit, CursorOrderingClaim,
    CursorPosition, CursorSpec, CursorValue, DeliveryGuarantee, DestinationCommitRequest,
    DestinationId, DestinationProtocol, DestinationSheet, EstimateSupport, FileManifest,
    FilePosition, FilterCapabilities, IdempotencySupport, IdempotencyToken, IdentifierRules,
    IncrementalShape, LogPosition, MigrationRecord, PackageHash, PageToken, PartitionId,
    PipelineId, PlanId, ProcessedObservationOutcome, ProcessedObservationPosition,
    PushdownFidelity, QueryableResource, Receipt, ReceiptId, ReceiptVerification, ReplaySupport,
    ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream, Result, RewindReport,
    RewindRequest, RunEvent, RunEventSink, RunEventSinkResult, RunId, RunPhase, RunPhaseMetric,
    RunPhaseStatus, ScanRequest, SchemaHash, SchemaSource, ScopeKey, SegmentAck, SegmentId,
    SourcePosition, StateDelta, StateSegment, TargetName, TransactionSupport, TrustLevel,
    VerifyClause, WriteDisposition,
};
use cdf_package::{
    DESTINATION_COMMIT_PLAN_FILE, DestinationCommitPlanPreimage, MANIFEST_FILE, PackageBuilder,
    PackageManifest, PackageReader, PackageReplayInputs, PackageStatus,
    ProcessedObservationEvidenceArtifact, RECEIPTS_FILE, STATE_INPUT_CHECKPOINT_FILE,
    STATE_PROPOSED_DELTA_FILE, StateDeltaPreimage, canonical_json_bytes,
};
use cdf_state_sqlite::{
    RunEventDetails, RunEventKind, RunEventValue, SecretReference, SqliteCheckpointStore,
    SqliteRunLedger,
};
use postgres::{Client, NoTls};
use tempfile::TempDir;
use tracing::{
    Event, Id, Metadata, Subscriber,
    field::{Field as TracingField, Visit},
    span::{Attributes, Record},
};

use crate::{
    BackfillPlanRequest, DestinationReceiptReportingPolicy, FileManifestRunSummary,
    LocalFileDuckDbRunRequest, PackageArtifactRecoveryRequest, PackageArtifactReplayRequest,
    PackageReplayHooks, PackageReplayStage, PreparedDestinationCommit,
    PreparedPackageRecoveryRequest, PreparedPackageReplayRequest, ProjectDestinationDescription,
    ProjectDestinationDriver, ProjectDestinationRegistry, ProjectDestinationRuntime,
    ProjectReceiptSource, ProjectResolutionContext, ProjectRunReport, ProjectRunRequest,
    ProjectRunSource, ResolvedProjectDestination, RunTelemetryConfig, RuntimeStage,
    TracingRunEventSink, backfill_pipeline_id, plan_backfill, recover_package_from_artifacts,
    recover_package_with_runtime, recover_prepared_package, replay_package_from_artifacts,
    replay_package_with_runtime, replay_prepared_package, replay_prepared_package_with_stage_hook,
    run_local_file_to_duckdb_checkpoint, run_project, run_project_with_telemetry,
    runtime::state_delta_from_run,
};

const SCHEMA_HASH: &str = "schema-v1";
const LIVE_FILE_RESOURCE: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.events"
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
const SIMPLE_FILE_RESOURCE_APPEND: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.events"
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
const MULTI_FILE_RESOURCE_APPEND: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.events"
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
const MULTI_FILE_RESOURCE_REPLACE: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.events"
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

struct RecordingRunEventSink {
    capacity: Option<usize>,
    persisted_state_path: Option<PathBuf>,
    events: Mutex<Vec<RunEvent>>,
    drops: AtomicU64,
}

impl RecordingRunEventSink {
    fn unbounded() -> Self {
        Self {
            capacity: None,
            persisted_state_path: None,
            events: Mutex::new(Vec::new()),
            drops: AtomicU64::new(0),
        }
    }

    fn bounded(capacity: usize) -> Self {
        Self {
            capacity: Some(capacity),
            persisted_state_path: None,
            events: Mutex::new(Vec::new()),
            drops: AtomicU64::new(0),
        }
    }

    fn unbounded_with_persistence_check(state_path: &Path) -> Self {
        Self {
            persisted_state_path: Some(state_path.to_path_buf()),
            ..Self::unbounded()
        }
    }

    fn events(&self) -> Vec<RunEvent> {
        self.events.lock().unwrap().clone()
    }

    fn drop_count(&self) -> u64 {
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
struct CapturingTracingSubscriber {
    next_id: Arc<AtomicU64>,
    events: Arc<Mutex<Vec<CapturedTracingEvent>>>,
}

impl CapturingTracingSubscriber {
    fn captured_events(&self) -> Vec<CapturedTracingEvent> {
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
struct CapturedTracingEvent {
    target: String,
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct TracingFieldVisitor {
    fields: BTreeMap<String, String>,
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

fn expected_runtime_trace_fields(event: &RunEvent) -> BTreeMap<String, String> {
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

fn optional_trace_field<T: AsRef<str>>(value: Option<&T>) -> String {
    value.map(|value| value.as_ref()).unwrap_or("").to_owned()
}

fn runtime_trace_events(subscriber: &CapturingTracingSubscriber) -> Vec<CapturedTracingEvent> {
    subscriber
        .captured_events()
        .into_iter()
        .filter(|event| event.target == "cdf_project.runtime.run_event")
        .collect()
}

fn run_event_for_tracing_details(details: RunEventDetails) -> RunEvent {
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

#[test]
fn backfill_planner_splits_numeric_windows_with_window_scopes_and_ids() {
    let resource = BackfillMockResource::cursor();

    let plan = plan_backfill(
        &resource,
        BackfillPlanRequest {
            target: TargetName::new("events").unwrap(),
            from: "0".to_owned(),
            to: "25".to_owned(),
            slice_size: Some(10),
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
fn backfill_planner_rejects_file_incremental_resource_without_opening_source() {
    let resource = BackfillMockResource::file_incremental();

    let error = plan_backfill(
        &resource,
        BackfillPlanRequest {
            target: TargetName::new("events").unwrap(),
            from: "0".to_owned(),
            to: "10".to_owned(),
            slice_size: None,
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("cursor-backed queryable"));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn backfill_planner_rejects_inverted_numeric_bounds_without_opening_source() {
    let resource = BackfillMockResource::cursor();

    let error = plan_backfill(
        &resource,
        BackfillPlanRequest {
            target: TargetName::new("events").unwrap(),
            from: "10".to_owned(),
            to: "10".to_owned(),
            slice_size: None,
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("--from < --to"));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

struct BackfillMockResource {
    descriptor: ResourceDescriptor,
    capabilities: ResourceCapabilities,
    schema: Arc<Schema>,
    open_count: AtomicU64,
}

impl BackfillMockResource {
    fn cursor() -> Self {
        Self::new(IncrementalShape::Cursor, Some(CursorOrderingClaim::Exact))
    }

    fn file_incremental() -> Self {
        Self::new(IncrementalShape::File, Some(CursorOrderingClaim::Exact))
    }

    fn new(incremental: IncrementalShape, ordering: Option<CursorOrderingClaim>) -> Self {
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
            start_position: None,
            metadata,
        }])
    }

    fn open(
        &self,
        _partition: cdf_kernel::PartitionPlan,
    ) -> cdf_kernel::BoxFuture<'_, Result<BatchStream>> {
        self.open_count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(CdfError::internal(
                "mock backfill source should not be opened",
            ))
        })
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
const SIMPLE_FILE_RESOURCE_APPEND_DRIFT: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.events"
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
  { name = "note", type = "string", nullable = true },
] }
"#;
const SIMPLE_FILE_RESOURCE_MERGE: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.events"
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
merge_key = ["id"]
write_disposition = "merge"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
] }
"#;
const POSTGRES_UNSUPPORTED_FILE_RESOURCE: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.events"
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "seen_at", type = "timestamp_millis", nullable = false, timezone = "UTC" },
] }
"#;

const REST_RESOURCE: &str = r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"

[resource.items]
id = "api.items"
path = "/items"
records = "$"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
] }
"#;
const REST_RUNTIME_RESOURCE: &str = r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"
auth = { kind = "bearer", token = "secret://env/API_TOKEN" }
egress_allowlist = ["api.example.test"]

[resource.items]
id = "api.items"
path = "/items"
records = "$.items"
primary_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "int64", nullable = false },
] }
"#;
const SQL_RUNTIME_RESOURCE: &str = r#"
[source.warehouse]
kind = "sql"
connection = "secret://env/POSTGRES_URL"
dialect = "postgres"

[resource.orders]
id = "postgres.orders"
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

static LIVE_POSTGRES_SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);
static LOCAL_POSTGRES_START: Mutex<()> = Mutex::new(());

fn sample_batch(ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = std::sync::Arc::new(Int64Array::from(ids));
    let name: ArrayRef = std::sync::Arc::new(StringArray::from(names));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

fn package_id_name_rows(reader: &PackageReader) -> Vec<(i64, Option<String>)> {
    let mut rows = Vec::new();
    for (_segment, batches) in reader.read_all_segments().unwrap() {
        for batch in batches {
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

fn build_package(package_dir: &Path, package_id: &str, status: PackageStatus) -> PackageManifest {
    let mut builder = PackageBuilder::create(package_dir, package_id).unwrap();
    builder.update_status(PackageStatus::Extracting).unwrap();
    builder
        .write_json_artifact(
            "schema/output.arrow.json",
            &BTreeMap::from([("schema_hash", SCHEMA_HASH)]),
        )
        .unwrap();
    let segment = builder
        .write_segment(
            cdf_kernel::SegmentId::new("seg-000001").unwrap(),
            &[sample_batch(
                vec![1, 2, 3],
                vec![Some("ada"), Some("grace"), None],
            )],
        )
        .unwrap();
    write_state_commit_artifacts(&builder, &segment);
    builder.finish_with_status(status).unwrap()
}

fn build_zero_segment_processed_package(package_dir: &Path, package_id: &str) -> PackageManifest {
    let builder = PackageBuilder::create(package_dir, package_id).unwrap();
    builder.update_status(PackageStatus::Extracting).unwrap();
    let output_position = SourcePosition::FileManifest(FileManifest {
        version: CHECKPOINT_STATE_VERSION,
        files: vec![FilePosition {
            path: "month-07.parquet".to_owned(),
            size_bytes: 41,
            etag: Some("etag-07".to_owned()),
            sha256: Some("sha256-07".to_owned()),
        }],
    });
    let processed = ProcessedObservationPosition::new(
        "month-07.parquet",
        ProcessedObservationOutcome::Quarantined,
        output_position.clone(),
    )
    .unwrap();
    let scope = ScopeKey::Resource;
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new("checkpoint-zero-artifact").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: output_position.clone(),
        schema_hash: SchemaHash::new(SCHEMA_HASH).unwrap(),
        segments: Vec::new(),
    };
    builder
        .write_json_artifact(
            cdf_package::PROCESSED_OBSERVATIONS_FILE,
            &ProcessedObservationEvidenceArtifact::new(
                None,
                WriteDisposition::Append,
                vec![processed],
                output_position,
            )
            .unwrap(),
        )
        .unwrap();
    builder.write_input_checkpoint_artifact(&None).unwrap();
    builder
        .write_state_delta_preimage_artifact(&state_delta)
        .unwrap();
    builder
        .write_commit_plan_preimage_artifact(&DestinationCommitPlanPreimage::package_hash_token(
            TargetName::new("orders").unwrap(),
            WriteDisposition::Append,
            Vec::new(),
            SchemaHash::new(SCHEMA_HASH).unwrap(),
            Vec::new(),
        ))
        .unwrap();
    builder.finish().unwrap()
}

fn write_state_commit_artifacts(builder: &PackageBuilder, segment: &cdf_package::SegmentEntry) {
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
        checkpoint_id: CheckpointId::new("checkpoint-artifact").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position,
        schema_hash: SchemaHash::new(SCHEMA_HASH).unwrap(),
        segments: segments.clone(),
    };
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        Vec::new(),
        SchemaHash::new(SCHEMA_HASH).unwrap(),
        segments,
    );
    builder.write_input_checkpoint_artifact(&None).unwrap();
    builder
        .write_state_delta_preimage_artifact(&state_delta)
        .unwrap();
    builder
        .write_commit_plan_preimage_artifact(&commit_plan)
        .unwrap();
}

fn scope() -> ScopeKey {
    ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    }
}

fn position(value: i64) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "id".to_owned(),
        value: CursorValue::I64(value),
    })
}

fn cursor_position(field: &str, value: CursorValue) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: field.to_owned(),
        value,
    })
}

fn delta(manifest: &PackageManifest, checkpoint_id: &str) -> StateDelta {
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

fn destination(path: &Path) -> DuckDbDestination {
    DuckDbDestination::new(path).unwrap()
}

fn replay_request<'a, Store: CheckpointStore + ?Sized>(
    package_dir: &Path,
    destination: &'a DuckDbDestination,
    checkpoint_store: &'a Store,
    delta: StateDelta,
) -> PreparedPackageReplayRequest<'a, Store> {
    PreparedPackageReplayRequest {
        package_dir: package_dir.to_path_buf(),
        destination: resolved_duckdb_destination(destination, TargetName::new("orders").unwrap()),
        checkpoint_store,
        inputs: replay_inputs_from_delta(delta, TargetName::new("orders").unwrap()),
        after_receipt_verified: None,
    }
}

fn artifact_replay_request<'a, Store: CheckpointStore + ?Sized>(
    package_dir: &Path,
    destination: &'a DuckDbDestination,
    checkpoint_store: &'a Store,
) -> PackageArtifactReplayRequest<'a, Store> {
    PackageArtifactReplayRequest {
        package_dir: package_dir.to_path_buf(),
        destination: resolved_duckdb_destination(destination, TargetName::new("orders").unwrap()),
        checkpoint_store,
        after_receipt_verified: None,
    }
}

fn recovery_request<'a, Store: CheckpointStore + ?Sized>(
    package_dir: &Path,
    destination: &'a DuckDbDestination,
    checkpoint_store: &'a Store,
    delta: StateDelta,
    receipt: Receipt,
) -> PreparedPackageRecoveryRequest<'a, Store> {
    PreparedPackageRecoveryRequest {
        package_dir: package_dir.to_path_buf(),
        destination: resolved_duckdb_destination(destination, TargetName::new("orders").unwrap()),
        checkpoint_store,
        inputs: replay_inputs_from_delta(delta, TargetName::new("orders").unwrap()),
        receipt,
        after_receipt_verified: None,
    }
}

fn replay_inputs_from_delta(delta: StateDelta, target: TargetName) -> PackageReplayInputs {
    PackageReplayInputs {
        input_checkpoint: None,
        destination_commit: DestinationCommitRequest {
            package_hash: delta.package_hash.clone(),
            target,
            disposition: WriteDisposition::Append,
            segments: delta.segments.clone(),
            idempotency_token: IdempotencyToken::new(delta.package_hash.as_str()).unwrap(),
        },
        merge_keys: Vec::new(),
        schema_hash: SchemaHash::new(SCHEMA_HASH).unwrap(),
        state_delta: delta,
    }
}

fn resolved_duckdb_destination(
    destination: &DuckDbDestination,
    target: TargetName,
) -> ResolvedProjectDestination {
    ResolvedProjectDestination::new(Box::new(destination.clone()), target)
}

#[derive(Clone)]
struct MockDestination {
    sheet: DestinationSheet,
    receipts: Arc<Mutex<Vec<Receipt>>>,
    writes: Arc<Mutex<Vec<SegmentId>>>,
}

impl MockDestination {
    fn new() -> Self {
        Self {
            sheet: DestinationSheet {
                destination: DestinationId::new("mock").unwrap(),
                supported_dispositions: vec![WriteDisposition::Append],
                transactions: TransactionSupport::AtomicPackage,
                idempotency: IdempotencySupport::PackageToken,
                type_mappings: Vec::new(),
                identifier_rules: IdentifierRules {
                    normalizer: "mock".to_owned(),
                    max_length: None,
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
        }
    }

    fn write_count(&self) -> usize {
        self.writes.lock().unwrap().len()
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

    fn begin(
        &self,
        request: DestinationCommitRequest,
        plan: CommitPlan,
    ) -> Result<Box<dyn CommitSession + '_>> {
        Ok(Box::new(MockCommitSession {
            destination: self,
            request,
            plan,
            migrations_applied: false,
            acks: Vec::new(),
        }))
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

struct MockCommitSession<'a> {
    destination: &'a MockDestination,
    request: DestinationCommitRequest,
    plan: CommitPlan,
    migrations_applied: bool,
    acks: Vec<SegmentAck>,
}

impl CommitSession for MockCommitSession<'_> {
    fn apply_migrations(&mut self) -> Result<()> {
        self.migrations_applied = true;
        Ok(())
    }

    fn write_segment(&mut self, segment: CommitSegment) -> Result<SegmentAck> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "mock destination migrations must be applied before writing",
            ));
        }
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
        Ok(ack)
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
struct MockDestinationCounters {
    resolves: Arc<AtomicU64>,
    prepares: Arc<AtomicU64>,
    binds: Arc<AtomicU64>,
}

impl MockDestinationCounters {
    fn new() -> Self {
        Self {
            resolves: Arc::new(AtomicU64::new(0)),
            prepares: Arc::new(AtomicU64::new(0)),
            binds: Arc::new(AtomicU64::new(0)),
        }
    }

    fn resolve_count(&self) -> usize {
        self.resolves.load(Ordering::SeqCst) as usize
    }

    fn prepare_count(&self) -> usize {
        self.prepares.load(Ordering::SeqCst) as usize
    }

    fn bind_count(&self) -> usize {
        self.binds.load(Ordering::SeqCst) as usize
    }
}

struct MockProjectDestinationDriver {
    destination: MockDestination,
    counters: MockDestinationCounters,
}

impl MockProjectDestinationDriver {
    fn new(destination: MockDestination, counters: MockDestinationCounters) -> Self {
        Self {
            destination,
            counters,
        }
    }
}

impl ProjectDestinationDriver for MockProjectDestinationDriver {
    fn schemes(&self) -> &'static [&'static str] {
        &["mock"]
    }

    fn resolve(
        &self,
        uri: &str,
        _context: &ProjectResolutionContext<'_>,
    ) -> Result<Box<dyn ProjectDestinationRuntime>> {
        if !uri.starts_with("mock:") {
            return Err(CdfError::contract(format!(
                "mock destination driver cannot resolve `{uri}`"
            )));
        }
        self.counters.resolves.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(MockProjectDestinationRuntime::with_destination(
            self.destination.clone(),
            self.counters.clone(),
        )))
    }
}

struct MockProjectDestinationRuntime {
    destination: MockDestination,
    counters: MockDestinationCounters,
}

impl MockProjectDestinationRuntime {
    fn with_destination(destination: MockDestination, counters: MockDestinationCounters) -> Self {
        Self {
            destination,
            counters,
        }
    }
}

impl ProjectDestinationRuntime for MockProjectDestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.destination
    }

    fn describe(&self) -> ProjectDestinationDescription {
        ProjectDestinationDescription::new(
            self.destination.sheet.destination.clone(),
            &["mock"],
            "mock",
        )
    }

    fn validate_run_preflight(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        _schema_hash: &SchemaHash,
    ) -> Result<()> {
        Ok(())
    }

    fn prepare_package_commit(
        &mut self,
        _package_dir: &Path,
        _reader: &PackageReader,
        inputs: &cdf_package::PackageReplayInputs,
        _context: &crate::DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        self.counters.prepares.fetch_add(1, Ordering::SeqCst);
        let plan = self.destination.plan_commit(&inputs.destination_commit)?;
        Ok(PreparedDestinationCommit::new(
            inputs.destination_commit.clone(),
            plan,
            DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
        ))
    }

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()> {
        if prepared.has_pending_context() {
            return Err(CdfError::internal(
                "mock destination received unexpected pending context",
            ));
        }
        self.counters.binds.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn package_replay_stage_name(stage: PackageReplayStage<'_>) -> &'static str {
    match stage {
        PackageReplayStage::PackageReplayVerified => "package_replay_verified",
        PackageReplayStage::CheckpointProposed { .. } => "checkpoint_proposed",
        PackageReplayStage::DestinationWriteReady => "destination_write_ready",
        PackageReplayStage::DestinationCommitStarted { .. } => "destination_commit_started",
        PackageReplayStage::DestinationSegmentAcknowledged { .. } => {
            "destination_segment_acknowledged"
        }
        PackageReplayStage::DestinationReceiptRecorded { .. } => "destination_receipt_recorded",
        PackageReplayStage::CheckpointCommitted { .. } => "checkpoint_committed",
        PackageReplayStage::PackageStatusUpdated { .. } => "package_status_updated",
    }
}

fn assert_no_head<Store: CheckpointStore>(store: &Store, delta: &StateDelta) {
    assert!(
        store
            .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_none()
    );
}

fn assert_head<Store: CheckpointStore>(store: &Store, delta: &StateDelta) -> Checkpoint {
    store
        .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
        .unwrap()
        .expect("checkpoint head")
}

fn package_status(package_dir: &Path) -> PackageStatus {
    PackageReader::open(package_dir)
        .unwrap()
        .manifest()
        .lifecycle
        .status
        .clone()
}

fn package_receipts(package_dir: &Path) -> Vec<Receipt> {
    PackageReader::open(package_dir)
        .unwrap()
        .receipts()
        .unwrap()
}

fn remove_package_receipts(package_dir: &Path) {
    let path = package_dir.join(RECEIPTS_FILE);
    if path.exists() {
        fs::remove_file(path).unwrap();
    }
}

fn live_file_resource(root: &Path) -> cdf_declarative::CompiledResource {
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("data/events.ndjson"),
        "{\"id\":1,\"updated_at\":1783296000000000}\n\
         {\"id\":2,\"updated_at\":1783296060000000}\n",
    )
    .unwrap();
    let document = cdf_declarative::parse_toml(LIVE_FILE_RESOURCE).unwrap();
    cdf_declarative::compile_document_with_project_root(&document, root)
        .unwrap()
        .remove(0)
}

fn simple_file_resource(root: &Path, document: &str) -> cdf_declarative::CompiledResource {
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("data/events.ndjson"),
        "{\"id\":1,\"name\":\"ada\"}\n\
         {\"id\":2,\"name\":\"grace\"}\n",
    )
    .unwrap();
    let document = cdf_declarative::parse_toml(document).unwrap();
    cdf_declarative::compile_document_with_project_root(&document, root)
        .unwrap()
        .remove(0)
}

fn long_identifier_file_resource(
    root: &Path,
    source_name: &str,
) -> cdf_declarative::CompiledResource {
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("data/events.ndjson"),
        format!("{{\"VendorID\":1,\"{source_name}\":10}}\n"),
    )
    .unwrap();
    let document = cdf_declarative::parse_toml(&format!(
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.events"
glob = "events.ndjson"
format = "ndjson"
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
  {{ name = "VendorID", type = "int64", nullable = false }},
  {{ name = "{source_name}", type = "int64", nullable = false }},
] }}
"#,
    ))
    .unwrap();
    cdf_declarative::compile_document_with_project_root(&document, root)
        .unwrap()
        .remove(0)
}

fn multi_file_resource(root: &Path) -> cdf_declarative::CompiledResource {
    multi_file_resource_with_document(root, MULTI_FILE_RESOURCE_APPEND)
}

fn replace_multi_file_resource(root: &Path) -> cdf_declarative::CompiledResource {
    multi_file_resource_with_document(root, MULTI_FILE_RESOURCE_REPLACE)
}

fn multi_file_resource_with_document(
    root: &Path,
    document: &str,
) -> cdf_declarative::CompiledResource {
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
    let document = cdf_declarative::parse_toml(document).unwrap();
    cdf_declarative::compile_document_with_project_root(&document, root)
        .unwrap()
        .remove(0)
}

fn rest_resource() -> cdf_declarative::CompiledResource {
    let document = cdf_declarative::parse_toml(REST_RESOURCE).unwrap();
    cdf_declarative::compile_document(&document)
        .unwrap()
        .remove(0)
}

fn rest_runtime_resource() -> cdf_declarative::CompiledResource {
    let document = cdf_declarative::parse_toml(REST_RUNTIME_RESOURCE).unwrap();
    cdf_declarative::compile_document(&document)
        .unwrap()
        .remove(0)
}

fn rest_cursor_runtime_resource(
    cursor_field: &str,
    cursor_field_decl: &str,
    ordering: &str,
    lag: &str,
) -> cdf_declarative::CompiledResource {
    let input = format!(
        r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"
auth = {{ kind = "bearer", token = "secret://env/API_TOKEN" }}
egress_allowlist = ["api.example.test"]

[resource.items]
id = "api.items"
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
    cdf_declarative::compile_document(&document)
        .unwrap()
        .remove(0)
}

fn sql_runtime_resource(table: &str) -> cdf_declarative::CompiledResource {
    let document = cdf_declarative::parse_toml(&SQL_RUNTIME_RESOURCE.replace(
        r#"table = "public.orders""#,
        &format!(r#"table = "{table}""#),
    ))
    .unwrap();
    cdf_declarative::compile_document(&document)
        .unwrap()
        .remove(0)
}

fn live_plan(resource: &cdf_declarative::CompiledResource, package_id: &str) -> EnginePlan {
    let destination = ResolvedProjectDestination::duckdb(
        "/tmp/cdf-plan-policy-only.duckdb",
        TargetName::new("events").unwrap(),
    )
    .unwrap();
    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = identifier_policy;
    live_plan_with_policy(resource, package_id, &policy)
}

fn default_live_plan(resource: &cdf_declarative::CompiledResource, package_id: &str) -> EnginePlan {
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
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
                boundedness: PlanBoundedness::Bounded,
                package_id: package_id.to_owned(),
            },
        )
        .unwrap()
}

fn live_plan_with_policy(
    resource: &cdf_declarative::CompiledResource,
    package_id: &str,
    policy: &ContractPolicy,
) -> EnginePlan {
    let destination = ResolvedProjectDestination::duckdb(
        "/tmp/cdf-plan-policy-only.duckdb",
        TargetName::new("events").unwrap(),
    )
    .unwrap();
    let mut policy = policy.clone();
    policy.normalization.identifier = destination.column_identifier_policy().unwrap().unwrap();
    live_plan_with_exact_policy(resource, package_id, &policy)
}

fn live_plan_with_exact_policy(
    resource: &cdf_declarative::CompiledResource,
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
                boundedness: PlanBoundedness::Bounded,
                package_id: package_id.to_owned(),
            },
        )
        .unwrap()
}

fn state_delta_request<'a>(
    resource: &'a cdf_declarative::CompiledResource,
    package_id: &str,
    root: &Path,
) -> LocalFileDuckDbRunRequest<'a> {
    LocalFileDuckDbRunRequest {
        resource,
        plan: default_live_plan(resource, package_id),
        package_root: root.to_path_buf(),
        destination_path: root.join("dev.duckdb"),
        state_store_path: root.join("state.db"),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        target: TargetName::new("items").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        after_receipt_verified: None,
    }
}

fn engine_output_with_positions(
    package_dir: &Path,
    package_id: &str,
    positions: Vec<SourcePosition>,
) -> EngineRunOutputWithSegmentPositions {
    let mut manifest = build_package(package_dir, package_id, PackageStatus::Packaged);
    let template = manifest.identity.segments[0].clone();
    let segments = positions
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let mut segment = template.clone();
            segment.segment_id = SegmentId::new(format!("seg-{:06}", index + 1)).unwrap();
            segment.path = format!("data/seg-{:06}.arrow", index + 1);
            segment
        })
        .collect::<Vec<_>>();
    let segment_positions = segments
        .iter()
        .zip(positions)
        .map(|(segment, position)| EngineSegmentPosition {
            segment_id: segment.segment_id.clone(),
            output_position: Some(position),
        })
        .collect();
    manifest.identity.segments = segments.clone();
    EngineRunOutputWithSegmentPositions::new(
        EngineRunOutput {
            manifest,
            segments,
            profile: ExecutionProfile::default(),
            lineage: LineageSummary::default(),
        },
        segment_positions,
    )
}

fn state_delta_for_positions(
    resource: &cdf_declarative::CompiledResource,
    root: &Path,
    package_id: &str,
    positions: Vec<SourcePosition>,
) -> Result<StateDelta> {
    let output = engine_output_with_positions(&root.join(package_id), package_id, positions);
    let request = state_delta_request(resource, package_id, root);
    state_delta_from_run(
        &request,
        &output,
        &SchemaHash::new(SCHEMA_HASH).unwrap(),
        &resource.descriptor().state_scope,
        None,
    )
}

#[test]
fn destination_planning_facade_previews_duckdb_schema_commit_without_writes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let database_path = temp.path().join("planned.duckdb");
    let mut destination =
        ResolvedProjectDestination::duckdb(&database_path, TargetName::new("events").unwrap())
            .unwrap();

    let engine_plan = live_plan(&resource, "pkg-plan-preview-duckdb");
    let plan = destination
        .plan_resource_commit(&resource, &engine_plan)
        .unwrap();

    assert_eq!(plan.description.destination_id.as_str(), "duckdb");
    assert_eq!(plan.target.as_str(), "events");
    assert_eq!(
        plan.commit_plan.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerPackage
    );
    assert_eq!(
        plan.commit_plan.idempotency,
        IdempotencySupport::PackageToken
    );
    assert_eq!(plan.synthetic.package_hash.as_str(), "sha256:plan-preview");
    assert_eq!(plan.synthetic.segment_ids.len(), 1);
    assert!(
        plan.commit_plan
            .migrations
            .iter()
            .any(|migration| migration.description.contains("CREATE TABLE"))
    );
    assert!(
        !database_path.exists(),
        "DuckDB plan preview must not create destination data"
    );
}

#[test]
fn destination_planning_facade_rejects_parquet_merge_without_writes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_MERGE);
    let parquet_root = temp.path().join("parquet");
    let mut destination = ResolvedProjectDestination::parquet_filesystem(
        &parquet_root,
        TargetName::new("events").unwrap(),
    )
    .unwrap();

    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    assert_eq!(identifier_policy.version, "namecase-v1");
    assert_eq!(identifier_policy.max_length, None);
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = identifier_policy;
    let engine_plan = live_plan_with_exact_policy(&resource, "pkg-plan-preview-parquet", &policy);
    let error = destination
        .plan_resource_commit(&resource, &engine_plan)
        .unwrap_err();

    assert!(error.to_string().contains("Parquet destination"));
    assert!(
        !parquet_root.exists(),
        "Parquet plan preview must not create destination root"
    );
}

fn project_run_request<'a>(
    resource: &'a cdf_declarative::CompiledResource,
    package_id: &str,
    package_root: &Path,
    duckdb_path: &Path,
    state_path: &Path,
    run_id: &str,
) -> ProjectRunRequest<'a> {
    ProjectRunRequest {
        resource: ProjectRunSource::local_file(resource),
        plan: live_plan(resource, package_id),
        package_root: package_root.to_path_buf(),
        state_store_path: state_path.to_path_buf(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path,
            TargetName::new("events").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new(run_id).unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }
}

fn project_run_request_with_policy<'a>(
    resource: &'a cdf_declarative::CompiledResource,
    package_id: &str,
    package_root: &Path,
    duckdb_path: &Path,
    state_path: &Path,
    run_id: &str,
    policy: &ContractPolicy,
) -> ProjectRunRequest<'a> {
    let mut request = project_run_request(
        resource,
        package_id,
        package_root,
        duckdb_path,
        state_path,
        run_id,
    );
    request.plan = live_plan_with_policy(resource, package_id, policy);
    request
}

fn assert_run_artifact_identity_unchanged(report: &ProjectRunReport) {
    let reader = PackageReader::open(&report.package_dir).unwrap();
    assert_eq!(
        PackageHash::new(reader.manifest().package_hash.clone()).unwrap(),
        report.package_hash
    );
    assert_eq!(report.receipt.package_hash, report.package_hash);
    assert_eq!(report.checkpoint.delta.package_hash, report.package_hash);
    assert_eq!(reader.manifest().lifecycle.status, report.package_status);
}

fn output_manifest(report: &ProjectRunReport) -> &FileManifest {
    let SourcePosition::FileManifest(manifest) = &report.checkpoint.delta.output_position else {
        panic!("checkpoint output position should be a file manifest");
    };
    manifest
}

fn output_manifest_paths(report: &ProjectRunReport) -> Vec<String> {
    output_manifest(report)
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect()
}

fn output_manifest_file<'a>(report: &'a ProjectRunReport, path: &str) -> &'a FilePosition {
    output_manifest(report)
        .files
        .iter()
        .find(|file| file.path == path)
        .unwrap_or_else(|| panic!("manifest omitted {path}"))
}

fn single_segment_manifest_path(report: &ProjectRunReport) -> String {
    assert_eq!(report.checkpoint.delta.segments.len(), 1);
    let SourcePosition::FileManifest(manifest) =
        &report.checkpoint.delta.segments[0].output_position
    else {
        panic!("state segment should retain file manifest evidence");
    };
    assert_eq!(manifest.files.len(), 1);
    manifest.files[0].path.clone()
}

fn parquet_project_run_request<'a>(
    resource: &'a cdf_declarative::CompiledResource,
    package_id: &str,
    package_root: &Path,
    parquet_root: &Path,
    state_path: &Path,
    run_id: &str,
) -> ProjectRunRequest<'a> {
    let destination = ResolvedProjectDestination::parquet_filesystem(
        parquet_root,
        TargetName::new("events").unwrap(),
    )
    .unwrap();
    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = identifier_policy;
    ProjectRunRequest {
        resource: ProjectRunSource::local_file(resource),
        plan: live_plan_with_exact_policy(resource, package_id, &policy),
        package_root: package_root.to_path_buf(),
        state_store_path: state_path.to_path_buf(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        destination,
        run_id: Some(RunId::new(run_id).unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }
}

fn postgres_project_run_request<'a>(
    resource: &'a cdf_declarative::CompiledResource,
    package_id: &str,
    package_root: &Path,
    database_url: &str,
    target: PostgresTarget,
    state_path: &Path,
    run_id: &str,
) -> ProjectRunRequest<'a> {
    let destination = ResolvedProjectDestination::postgres(
        database_url.to_owned(),
        target,
        MergeDedupPolicy::Last,
        None,
    )
    .unwrap();
    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = identifier_policy;
    ProjectRunRequest {
        resource: ProjectRunSource::local_file(resource),
        plan: live_plan_with_exact_policy(resource, package_id, &policy),
        package_root: package_root.to_path_buf(),
        state_store_path: state_path.to_path_buf(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        destination,
        run_id: Some(RunId::new(run_id).unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }
}

fn file_position(path: &str) -> SourcePosition {
    file_position_with_identity(path, 42, Some(format!("sha256:{path}")))
}

fn file_position_with_identity(
    path: &str,
    size_bytes: u64,
    sha256: Option<String>,
) -> SourcePosition {
    SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: path.to_owned(),
            size_bytes,
            etag: None,
            sha256,
        }],
    })
}

fn json_response(body: &str) -> HttpResponse {
    HttpResponse::new(200).with_body(body.as_bytes().to_vec())
}

#[derive(Clone, Default)]
struct RecordingTransport {
    state: Arc<Mutex<RecordingTransportState>>,
}

#[derive(Default)]
struct RecordingTransportState {
    requests: Vec<HttpRequest>,
    responses: VecDeque<HttpResponse>,
}

impl RecordingTransport {
    fn new<I>(responses: I) -> Self
    where
        I: IntoIterator<Item = HttpResponse>,
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
    fn send(&mut self, request: HttpRequest) -> Result<HttpResponse> {
        let mut state = self.state.lock().unwrap();
        state.requests.push(request);
        state
            .responses
            .pop_front()
            .ok_or_else(|| CdfError::internal("test transport exhausted responses"))
    }
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

struct LivePostgres {
    url: String,
    schema: String,
    _server: Option<LocalPostgres>,
}

struct LocalPostgres {
    data_dir: TempDir,
    _socket_dir: TempDir,
    pg_ctl: PathBuf,
}

impl LivePostgres {
    fn start() -> Option<Self> {
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

    fn client(&self) -> Client {
        Client::connect(&self.url, NoTls).unwrap()
    }

    fn table(&self, table: &str) -> String {
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
    fn start() -> Option<Self> {
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

    fn url(&self) -> String {
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

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn reset_postgres_schema(postgres: &LivePostgres) {
    let schema = quote_identifier(&postgres.schema);
    postgres
        .client()
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
        ))
        .unwrap();
}

fn postgres_table_exists(postgres: &LivePostgres, table: &str) -> bool {
    postgres
        .client()
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&postgres.schema, &table],
        )
        .unwrap()
        .get(0)
}

fn find_binary(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|path| path.join(name))
            .find(|candidate| candidate.is_file())
    })
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn stage_successful_replay(
    package_dir: &Path,
    db_path: &Path,
    checkpoint_id: &str,
) -> (DuckDbDestination, StateDelta, Receipt) {
    let manifest = build_package(package_dir, "pkg-stage", PackageStatus::Packaged);
    let delta = delta(&manifest, checkpoint_id);
    let destination = destination(db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let report = replay_prepared_package(replay_request(
        package_dir,
        &destination,
        &store,
        delta.clone(),
    ))
    .unwrap();
    (destination, delta, report.receipt)
}

fn assert_bad_reuse_head_rejected(
    package_id: &str,
    checkpoint_id: &str,
    mutate_head: impl FnOnce(&mut Checkpoint),
) {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join(package_id);
    let db_path = temp.path().join("local.duckdb");
    let (destination, delta, receipt) =
        stage_successful_replay(&package_dir, &db_path, checkpoint_id);
    let mut head = Checkpoint {
        delta: delta.clone(),
        status: CheckpointStatus::Committed,
        receipt: Some(receipt.clone()),
        is_head: true,
        created_at_ms: receipt.committed_at_ms,
        committed_at_ms: Some(receipt.committed_at_ms),
        rewind_target_checkpoint_id: None,
    };
    mutate_head(&mut head);
    let store = HeadOnlyCommitFailingStore { head };

    let error = recover_prepared_package(recovery_request(
        &package_dir,
        &destination,
        &store,
        delta,
        receipt,
    ))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("injected checkpoint commit failure")
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
}

fn run_rest_project(root: &Path, run_id: &str) -> (ProjectRunReport, RecordingTransport) {
    let compiled = rest_runtime_resource();
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": 20 }
        ] }"#,
    )]);
    let resource = compiled
        .to_rest_resource(
            cdf_declarative::RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    let package_id = "pkg-general-rest-runtime";
    let package_root = root.join(".cdf/packages");
    let state_path = root.join(".cdf/state.db");
    let duckdb_path = root.join(".cdf/dev.duckdb");

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root,
        state_store_path: state_path,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-runtime").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path,
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new(run_id).unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();
    (report, transport)
}

#[test]
fn live_file_run_post_receipt_failure_keeps_checkpoint_uncommitted_and_receipt_recoverable() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-live-hook-failure";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let pipeline_id = PipelineId::new("pipeline-live").unwrap();
    let hook = |_receipt: &Receipt| Err(CdfError::internal("injected live checkpoint failure"));

    let error = futures_executor::block_on(run_local_file_to_duckdb_checkpoint(
        LocalFileDuckDbRunRequest {
            resource: &resource,
            plan: live_plan(&resource, package_id),
            package_root: package_root.clone(),
            destination_path: duckdb_path.clone(),
            state_store_path: state_path.clone(),
            pipeline_id: pipeline_id.clone(),
            target: TargetName::new("events").unwrap(),
            package_id: package_id.to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-live-hook-failure").unwrap(),
            after_receipt_verified: Some(&hook),
        },
    ))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("injected live checkpoint failure"),
        "{error}"
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let destination = destination(&duckdb_path);
    assert!(destination.verify_receipt(&receipts[0]).unwrap().verified);

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
    assert!(matches!(
        history[0].delta.output_position,
        SourcePosition::FileManifest(_)
    ));
}

#[test]
fn general_project_run_records_ledger_events_in_commit_gate_order() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-general-ledger-order";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let report = futures_executor::block_on(run_project(project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-general-ledger-order",
    )))
    .unwrap();

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
    assert!(
        report.ledger_snapshot.events[12]
            .details
            .attributes
            .contains_key("elapsed_ms")
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
        RunPhase::Decode,
        RunPhase::ValidationNormalization,
        RunPhase::SegmentEncode,
        RunPhase::PersistHash,
        RunPhase::PackageFinalize,
        RunPhase::DestinationWriteReceipt,
        RunPhase::CheckpointGate,
    ] {
        assert!(
            metrics.iter().any(|metric| metric.phase == phase),
            "missing {phase:?}"
        );
    }
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

    let unchanged = futures_executor::block_on(run_project(project_run_request(
        &resource,
        "pkg-file-manifest-incremental-2",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-file-manifest-incremental-2",
    )))
    .unwrap();
    assert_eq!(unchanged.row_count, 0);
    assert_eq!(unchanged.segment_count, 0);
    assert_eq!(
        unchanged.receipt_source,
        ProjectReceiptSource::FileManifestNoChangedFiles
    );
    assert_eq!(
        unchanged.file_manifest,
        Some(FileManifestRunSummary {
            total_file_count: 2,
            changed_file_count: 0,
            unchanged_file_count: 2,
        })
    );
    assert_eq!(unchanged.checkpoint, first.checkpoint);
    assert_eq!(unchanged.receipt, first.receipt);
    assert!(!unchanged.package_dir.exists());
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
fn trust_ring_clean_stable_runs_gate_sampled_fast_path_promotion() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 2;

    let first = project_run_request_with_policy(
        &resource,
        "pkg-trust-promotion-1",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-promotion-1",
        &policy,
    );
    let first_report = futures_executor::block_on(run_project(first)).unwrap();
    let first_transitions = first_report
        .ledger_snapshot
        .events
        .iter()
        .filter(|event| event.kind == RunEventKind::ValidationDepthTransitionRecorded)
        .collect::<Vec<_>>();
    assert_eq!(first_transitions.len(), 1);
    assert_eq!(
        first_transitions[0].details.attributes.get("trigger"),
        Some(&RunEventValue::String("new_resource".to_owned()))
    );

    fs::write(
        temp.path().join("data/events.ndjson"),
        "{\"id\":3,\"name\":\"katherine\"}\n\
         {\"id\":4,\"name\":\"dorothy\"}\n",
    )
    .unwrap();
    let second = project_run_request_with_policy(
        &resource,
        "pkg-trust-promotion-2",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-promotion-2",
        &policy,
    );
    let second_report = futures_executor::block_on(run_project(second)).unwrap();
    let transition = second_report
        .ledger_snapshot
        .events
        .iter()
        .find(|event| event.kind == RunEventKind::ValidationDepthTransitionRecorded)
        .expect("promotion transition event");

    assert_eq!(
        transition.package_hash,
        Some(second_report.package_hash.clone())
    );
    assert_eq!(
        transition.details.attributes.get("from_depth"),
        Some(&RunEventValue::String("full".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("to_depth"),
        Some(&RunEventValue::String("sampled_fast_path".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("trigger"),
        Some(&RunEventValue::String("clean_stable_runs".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("clean_run_count"),
        Some(&RunEventValue::U64(2))
    );
    assert_eq!(
        transition.details.attributes.get("clean_runs_required"),
        Some(&RunEventValue::U64(2))
    );
    assert_eq!(
        transition.details.attributes.get("schema_hash"),
        Some(&RunEventValue::String(
            second_report.receipt.schema_hash.as_str().to_owned()
        ))
    );
    assert_eq!(second_report.package_status, PackageStatus::Checkpointed);
    assert_eq!(second_report.checkpoint.status, CheckpointStatus::Committed);
}

#[test]
fn trust_ring_schema_drift_demotes_sampled_fast_path() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_root = temp.path().join(".cdf/packages");
    let parquet_root = temp.path().join(".cdf/lake");
    let state_path = temp.path().join(".cdf/state.db");
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 1;
    policy.promotion.demote_on_drift = true;

    let mut clean = parquet_project_run_request(
        &resource,
        "pkg-trust-drift-clean",
        &package_root,
        &parquet_root,
        &state_path,
        "run-trust-drift-clean",
    );
    clean.plan = live_plan_with_policy(&resource, "pkg-trust-drift-clean", &policy);
    let clean_report = futures_executor::block_on(run_project(clean)).unwrap();
    assert!(
        clean_report.ledger_snapshot.events.iter().any(|event| event
            .details
            .attributes
            .get("trigger")
            == Some(&RunEventValue::String("clean_stable_runs".to_owned())))
    );

    let drift_resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND_DRIFT);
    fs::write(
        temp.path().join("data/events.ndjson"),
        "{\"id\":3,\"name\":\"katherine\",\"note\":\"schema drift\"}\n\
         {\"id\":4,\"name\":\"dorothy\",\"note\":\"schema drift\"}\n",
    )
    .unwrap();
    let mut drift = parquet_project_run_request(
        &drift_resource,
        "pkg-trust-drift-schema",
        &package_root,
        &parquet_root,
        &state_path,
        "run-trust-drift-schema",
    );
    drift.plan = live_plan_with_policy(&drift_resource, "pkg-trust-drift-schema", &policy);
    let report = futures_executor::block_on(run_project(drift)).unwrap();
    let transition = report
        .ledger_snapshot
        .events
        .iter()
        .find(|event| {
            event.kind == RunEventKind::ValidationDepthTransitionRecorded
                && event.details.attributes.get("trigger")
                    == Some(&RunEventValue::String("drift".to_owned()))
        })
        .expect("drift demotion transition event");

    assert_eq!(
        transition.details.attributes.get("from_depth"),
        Some(&RunEventValue::String("sampled_fast_path".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("to_depth"),
        Some(&RunEventValue::String("full".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("schema_hash"),
        Some(&RunEventValue::String(
            report.receipt.schema_hash.as_str().to_owned()
        ))
    );
    assert_eq!(
        transition.details.attributes.get("previous_schema_hash"),
        Some(&RunEventValue::String(
            clean_report.receipt.schema_hash.as_str().to_owned()
        ))
    );
    assert_eq!(
        transition.checkpoint_id,
        Some(report.checkpoint.delta.checkpoint_id.clone())
    );
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
}

#[test]
fn trust_ring_quarantine_demotes_sampled_fast_path_without_checkpoint_bypass() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 1;
    policy.promotion.demote_on_quarantine = true;
    policy.rows.rules = vec![RowRule::Domain {
        column: "name".to_owned(),
        allowed: vec!["ada".to_owned(), "grace".to_owned()],
    }];

    let clean = project_run_request_with_policy(
        &resource,
        "pkg-trust-demotion-clean",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-demotion-clean",
        &policy,
    );
    futures_executor::block_on(run_project(clean)).unwrap();

    fs::write(
        temp.path().join("data/events.ndjson"),
        "{\"id\":1,\"name\":\"ada\"}\n\
         {\"id\":2,\"name\":\"raw-secret\"}\n",
    )
    .unwrap();
    let quarantine = project_run_request_with_policy(
        &resource,
        "pkg-trust-demotion-quarantine",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-demotion-quarantine",
        &policy,
    );
    let report = futures_executor::block_on(run_project(quarantine)).unwrap();
    let transition_index = report
        .ledger_snapshot
        .events
        .iter()
        .position(|event| event.kind == RunEventKind::ValidationDepthTransitionRecorded)
        .expect("demotion transition event");
    let transition = &report.ledger_snapshot.events[transition_index];

    assert_eq!(transition.package_hash, Some(report.package_hash.clone()));
    assert_eq!(
        transition.details.attributes.get("from_depth"),
        Some(&RunEventValue::String("sampled_fast_path".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("to_depth"),
        Some(&RunEventValue::String("full".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("trigger"),
        Some(&RunEventValue::String("quarantine_event".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("schema_hash"),
        Some(&RunEventValue::String(
            report.receipt.schema_hash.as_str().to_owned()
        ))
    );
    let transition_json = serde_json::to_string(transition).unwrap();
    assert!(!transition_json.contains("raw-secret"));
    assert!(!transition_json.contains("secret://"));

    let kinds = report
        .ledger_snapshot
        .events
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(
        kinds
            .iter()
            .position(|kind| *kind == RunEventKind::PackageFinalized)
            .unwrap()
            < transition_index
    );
    assert!(
        transition_index
            < kinds
                .iter()
                .position(|kind| *kind == RunEventKind::CheckpointProposed)
                .unwrap()
    );
    assert!(kinds.contains(&RunEventKind::CheckpointCommitted));
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-live").unwrap(),
            &resource.descriptor().resource_id,
            &resource.descriptor().state_scope,
        )
        .unwrap()
        .expect("checkpoint head");
    assert_eq!(
        head.delta.checkpoint_id,
        report.checkpoint.delta.checkpoint_id
    );
}

#[test]
fn trust_ring_explicit_anomaly_fact_demotes_sampled_fast_path() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 1;
    policy.promotion.demote_on_anomaly = true;

    let clean = project_run_request_with_policy(
        &resource,
        "pkg-trust-anomaly-clean",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-anomaly-clean",
        &policy,
    );
    futures_executor::block_on(run_project(clean)).unwrap();

    fs::write(
        temp.path().join("data/events.ndjson"),
        "{\"id\":3,\"name\":\"katherine\"}\n\
         {\"id\":4,\"name\":\"dorothy\"}\n",
    )
    .unwrap();
    let mut anomaly = project_run_request_with_policy(
        &resource,
        "pkg-trust-anomaly-spike",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-anomaly-spike",
        &policy,
    );
    anomaly
        .plan
        .validation_program
        .explicit_anomalies
        .push(AnomalyFact {
            metric: "profile.value_distribution_zscore".to_owned(),
            observed: "12.4".to_owned(),
            threshold: "3.0".to_owned(),
            window: "last_5_committed_packages".to_owned(),
        });
    let report = futures_executor::block_on(run_project(anomaly)).unwrap();
    let transition = report
        .ledger_snapshot
        .events
        .iter()
        .find(|event| {
            event.kind == RunEventKind::ValidationDepthTransitionRecorded
                && event.details.attributes.get("trigger")
                    == Some(&RunEventValue::String("anomaly_spike".to_owned()))
        })
        .expect("anomaly demotion transition event");

    assert_eq!(transition.package_hash, Some(report.package_hash.clone()));
    assert_eq!(
        transition.details.attributes.get("from_depth"),
        Some(&RunEventValue::String("sampled_fast_path".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("to_depth"),
        Some(&RunEventValue::String("full".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("schema_hash"),
        Some(&RunEventValue::String(
            report.receipt.schema_hash.as_str().to_owned()
        ))
    );
    assert_eq!(
        transition.details.attributes.get("metric"),
        Some(&RunEventValue::String(
            "profile.value_distribution_zscore".to_owned()
        ))
    );
    assert_eq!(
        transition.details.attributes.get("observed"),
        Some(&RunEventValue::String("12.4".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("threshold"),
        Some(&RunEventValue::String("3.0".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("window"),
        Some(&RunEventValue::String(
            "last_5_committed_packages".to_owned()
        ))
    );
    assert_eq!(
        transition.checkpoint_id,
        Some(report.checkpoint.delta.checkpoint_id.clone())
    );
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
}

#[test]
fn trust_ring_anomaly_demotion_requires_explicit_fact() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 1;
    policy.promotion.demote_on_anomaly = true;

    let clean = project_run_request_with_policy(
        &resource,
        "pkg-trust-no-anomaly-clean",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-no-anomaly-clean",
        &policy,
    );
    futures_executor::block_on(run_project(clean)).unwrap();

    let no_anomaly = project_run_request_with_policy(
        &resource,
        "pkg-trust-no-anomaly-current",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-no-anomaly-current",
        &policy,
    );
    let report = futures_executor::block_on(run_project(no_anomaly)).unwrap();

    assert!(!report.ledger_snapshot.events.iter().any(|event| {
        event.kind == RunEventKind::ValidationDepthTransitionRecorded
            && event.details.attributes.get("trigger")
                == Some(&RunEventValue::String("anomaly_spike".to_owned()))
    }));
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
}

#[test]
fn merge_dedup_live_run_records_deduped_package_replay_identity_and_duplicate_redrive() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_MERGE);
    let source_path = temp.path().join("data/events.ndjson");
    fs::write(
        &source_path,
        "{\"id\":1,\"name\":\"one-first\"}\n\
         {\"id\":2,\"name\":\"two\"}\n\
         {\"id\":1,\"name\":\"one-last\"}\n",
    )
    .unwrap();
    let package_id = "pkg-merge-dedup-live-replay";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut plan = live_plan(&resource, package_id);
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Last,
    }];
    plan.validation_program =
        live_plan_with_policy(&resource, package_id, &policy).validation_program;
    let mut request = project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-merge-dedup-live-replay",
    );
    request.plan = plan;

    let report = futures_executor::block_on(run_project(request)).unwrap();

    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(report.row_count, 2);
    assert_eq!(report.segment_count, 1);
    assert_eq!(report.receipt.disposition, WriteDisposition::Merge);
    assert_eq!(report.receipt.counts.rows_written, 2);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: true
        }
    );

    let reader = PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    assert_eq!(reader.manifest().identity.segments.len(), 1);
    assert_eq!(reader.manifest().identity.segments[0].row_count, 2);
    assert_eq!(
        package_id_name_rows(&reader),
        vec![
            (2, Some("two".to_owned())),
            (1, Some("one-last".to_owned()))
        ]
    );
    assert!(
        reader
            .manifest()
            .identity
            .files
            .iter()
            .any(|file| file.path == cdf_package::DEDUP_SUMMARY_FILE)
    );
    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["rule_id"], "row-rule-0000-dedup");
    assert_eq!(summary["keys"], serde_json::json!(["id"]));
    assert_eq!(summary["keep"], "last");
    assert_eq!(summary["input_rows"], 3);
    assert_eq!(summary["output_rows"], 2);
    assert_eq!(summary["duplicate_key_count"], 1);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(summary["dropped_rows"][0]["package_row_ordinal"], 0);
    assert_eq!(summary["dropped_rows"][0]["kept_package_row_ordinal"], 2);
    let replay_inputs = reader.replay_inputs().unwrap();
    assert_eq!(
        replay_inputs.destination_commit.disposition,
        WriteDisposition::Merge
    );
    assert_eq!(replay_inputs.merge_keys, vec!["id".to_owned()]);
    assert_eq!(
        replay_inputs
            .destination_commit
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .sum::<u64>(),
        2
    );

    fs::remove_file(&source_path).unwrap();
    let replay_duckdb_path = temp.path().join(".cdf/replay.duckdb");
    let replay_destination = destination(&replay_duckdb_path);
    let replay_store =
        SqliteCheckpointStore::open(temp.path().join(".cdf/replay-state.db")).unwrap();
    let replay = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: resolved_duckdb_destination(
            &replay_destination,
            replay_inputs.destination_commit.target.clone(),
        ),
        checkpoint_store: &replay_store,
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(replay.checkpoint.delta, report.checkpoint.delta);
    assert_eq!(replay.receipt.disposition, WriteDisposition::Merge);
    assert_eq!(replay.receipt.counts.rows_written, 2);
    assert_eq!(
        replay
            .receipt
            .segment_acks
            .iter()
            .map(|ack| ack.row_count)
            .sum::<u64>(),
        2
    );
    assert!(matches!(
        replay.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            ..
        }
    ));
    let replay_snapshot = replay_destination.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(replay_snapshot.loads.len(), 1);
    assert_eq!(replay_snapshot.state.len(), 1);
    assert_eq!(replay_snapshot.state[0].row_count, 2);

    let duplicate_store =
        SqliteCheckpointStore::open(temp.path().join(".cdf/replay-duplicate-state.db")).unwrap();
    let duplicate = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: resolved_duckdb_destination(
            &replay_destination,
            replay_inputs.destination_commit.target,
        ),
        checkpoint_store: &duplicate_store,
        after_receipt_verified: None,
    })
    .unwrap();
    let duplicate_snapshot = replay_destination.read_mirror_snapshot_read_only().unwrap();

    assert_eq!(duplicate_snapshot, replay_snapshot);
    assert_eq!(duplicate.checkpoint.delta, report.checkpoint.delta);
    assert_eq!(duplicate.receipt, replay.receipt);
    assert_eq!(
        duplicate.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: true,
            package_receipt_recorded: false
        }
    );
}

#[test]
fn project_run_records_non_mirror_outcome_for_unsupported_quarantine_sheet() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-quarantine-duckdb-unsupported";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut plan = live_plan(&resource, package_id);
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.rows.rules = vec![RowRule::Range {
        column: "id".to_owned(),
        min: None,
        max: Some("1".to_owned()),
    }];
    plan.validation_program =
        live_plan_with_policy(&resource, package_id, &policy).validation_program;

    let report = futures_executor::block_on(run_local_file_to_duckdb_checkpoint(
        LocalFileDuckDbRunRequest {
            resource: &resource,
            plan,
            package_root: package_root.clone(),
            destination_path: duckdb_path,
            state_store_path: state_path,
            pipeline_id: PipelineId::new("pipeline-live").unwrap(),
            target: TargetName::new("events").unwrap(),
            package_id: package_id.to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-quarantine-duckdb-unsupported").unwrap(),
            after_receipt_verified: None,
        },
    ))
    .unwrap();

    assert_eq!(report.row_count, 1);
    assert_eq!(report.segment_count, 1);
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    let reader = PackageReader::open(&package_dir).unwrap();
    assert_eq!(reader.read_quarantine_records().unwrap().len(), 1);
    assert!(
        reader
            .manifest()
            .identity
            .files
            .iter()
            .any(|file| file.path == "destination/quarantine-mirror.json")
    );
    let mirror_outcome: serde_json::Value = serde_json::from_slice(
        &fs::read(package_dir.join("destination/quarantine-mirror.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(mirror_outcome["destination_id"], "duckdb");
    assert_eq!(mirror_outcome["quarantine_table_support"], "unsupported");
    assert_eq!(mirror_outcome["outcome"], "not_mirrored");
    assert_eq!(
        mirror_outcome["quarantine_artifacts"][0],
        "quarantine/part-000001.parquet"
    );
}

#[test]
fn general_project_run_commits_file_resource_to_parquet_with_ledger_order() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-parquet";
    let package_root = temp.path().join(".cdf/packages");
    let parquet_root = temp.path().join(".cdf/lake");
    let state_path = temp.path().join(".cdf/state.db");

    let report = futures_executor::block_on(run_project(parquet_project_run_request(
        &resource,
        package_id,
        &package_root,
        &parquet_root,
        &state_path,
        "run-general-parquet",
    )))
    .unwrap();

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
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(report.row_count, 2);
    assert_eq!(report.receipt.destination.as_str(), "parquet_object_store");
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: true
        }
    );
    let destination = ParquetDestination::new_filesystem(&parquet_root).unwrap();
    assert!(
        destination
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
}

#[test]
fn general_project_run_commits_file_resource_to_postgres_with_ledger_order() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-postgres";
    let package_root = temp.path().join(".cdf/packages");
    let state_path = temp.path().join(".cdf/state.db");
    let target = PostgresTarget::new(Some(&postgres.schema), "events").unwrap();

    let report = futures_executor::block_on(run_project(postgres_project_run_request(
        &resource,
        package_id,
        &package_root,
        &postgres.url,
        target.clone(),
        &state_path,
        "run-general-postgres",
    )))
    .unwrap();

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
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(report.row_count, 2);
    assert_eq!(report.receipt.destination.as_str(), "postgres");
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommitReceiptOnly {
            package_receipt_recorded: true
        }
    );
    let destination = PostgresDestination::connect(postgres.url.clone()).unwrap();
    assert!(
        destination
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
    let mut client = postgres.client();
    let rows: i64 = client
        .query_one(
            &format!("SELECT COUNT(*)::bigint FROM {}", postgres.table("events")),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 2);
}

#[test]
fn postgres_destination_policy_truncates_package_and_committed_column_identically() {
    const LONG_SOURCE: &str =
        "this_is_a_very_long_vendor_identifier_column_name_that_exceeds_sixty_three_bytes_total";
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = long_identifier_file_resource(temp.path(), LONG_SOURCE);
    let package_id = "pkg-postgres-destination-normalization";
    let package_root = temp.path().join(".cdf/packages");
    let state_path = temp.path().join(".cdf/state.db");
    let target = PostgresTarget::new(Some(&postgres.schema), "normalized_events").unwrap();
    let destination = ResolvedProjectDestination::postgres(
        postgres.url.clone(),
        target,
        MergeDedupPolicy::Last,
        None,
    )
    .unwrap();
    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    let expected = cdf_contract::normalize_identifier(LONG_SOURCE, &identifier_policy).unwrap();
    assert_eq!(expected.len(), 63);
    assert_eq!(
        expected,
        cdf_contract::normalize_identifier(LONG_SOURCE, &identifier_policy).unwrap()
    );
    let mut contract = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    contract.normalization.identifier = identifier_policy.clone();

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::local_file(&resource),
        plan: live_plan_with_exact_policy(&resource, package_id, &contract),
        package_root,
        state_store_path: state_path,
        pipeline_id: PipelineId::new("pipeline-postgres-destination-normalization").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-postgres-destination-normalization").unwrap(),
        destination,
        run_id: Some(RunId::new("run-postgres-destination-normalization").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    let validation: serde_json::Value = serde_json::from_slice(
        &fs::read(report.package_dir.join("plan/validation-program.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(validation["identifier_policy"]["max_length"], 63);
    let output: serde_json::Value =
        serde_json::from_slice(&fs::read(report.package_dir.join("schema/output.json")).unwrap())
            .unwrap();
    assert_eq!(output["fields"][0]["name"], "vendor_id");
    assert_eq!(output["fields"][1]["name"], expected);
    assert_eq!(
        output["fields"][1]["metadata"]["cdf:source_name"],
        LONG_SOURCE
    );

    let mut client = postgres.client();
    let columns = client
        .query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'normalized_events' ORDER BY ordinal_position",
            &[&postgres.schema],
        )
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    assert_eq!(&columns[..2], &["vendor_id".to_owned(), expected]);
}

#[test]
fn stale_long_name_column_program_cannot_spoof_destination_policy_before_writes() {
    const LONG_SOURCE: &str =
        "this_is_a_very_long_vendor_identifier_column_name_that_exceeds_sixty_three_bytes_total";
    let temp = tempfile::tempdir().unwrap();
    let resource = long_identifier_file_resource(temp.path(), LONG_SOURCE);
    let package_id = "pkg-stale-long-name-normalization";
    let package_root = temp.path().join(".cdf/packages");
    let destination_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let destination =
        ResolvedProjectDestination::duckdb(&destination_path, TargetName::new("events").unwrap())
            .unwrap();
    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    let mut plan = default_live_plan(&resource, package_id);
    assert_ne!(
        plan.validation_program.column_programs[1].output_name,
        cdf_contract::normalize_identifier(LONG_SOURCE, &identifier_policy).unwrap()
    );
    plan.validation_program.normalizer_version = identifier_policy.version.clone();
    plan.validation_program.identifier_policy = identifier_policy;

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::local_file(&resource),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-stale-long-name-normalization").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-stale-long-name-normalization").unwrap(),
        destination,
        run_id: Some(RunId::new("run-stale-long-name-normalization").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("normalization program is stale at column 1"));
    assert!(message.contains(LONG_SOURCE));
    assert!(message.contains("rebuild the plan for the selected destination"));
    assert!(!package_root.join(package_id).exists());
    assert!(!destination_path.exists());
    assert!(!state_path.exists());
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
        resource: ProjectRunSource::local_file(&resource),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-stale-normalizer-version").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-stale-normalizer-version").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
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
fn general_project_run_executes_deterministic_rest_resource_stream() {
    let first_root = tempfile::tempdir().unwrap();
    let second_root = tempfile::tempdir().unwrap();

    let (first, first_transport) = run_rest_project(first_root.path(), "run-general-rest-runtime");
    let (second, second_transport) =
        run_rest_project(second_root.path(), "run-general-rest-runtime");

    assert_eq!(first.row_count, 2);
    assert_eq!(first.segment_count, 1);
    assert_eq!(first.package_status, PackageStatus::Checkpointed);
    assert_eq!(first.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(first.package_hash, second.package_hash);
    assert_eq!(first_transport.requests().len(), 1);
    assert_eq!(second_transport.requests().len(), 1);
    let SourcePosition::Cursor(cursor) = &first.checkpoint.delta.output_position else {
        panic!("expected REST run to checkpoint a cursor position");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

#[test]
fn general_project_run_executes_rest_with_discovered_snapshot_hash() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = rest_runtime_resource();
    let schema = compiled.schema();
    let schema_hash = SchemaHash::new("sha256:rest-discovered-runtime").unwrap();
    let compiled = compiled.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: cdf_kernel::SchemaSnapshotReference {
                schema_hash: schema_hash.clone(),
                path: ".cdf/schemas/api.items@sha256:rest-discovered-runtime.json".to_owned(),
                metadata: BTreeMap::from([("probe".to_owned(), "rest-sample-page".to_owned())]),
            },
        },
        schema,
    );
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": 20 }
        ] }"#,
    )]);
    let resource = compiled
        .to_rest_resource(
            cdf_declarative::RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    let package_id = "pkg-general-rest-discovered-runtime";
    let state_path = temp.path().join(".cdf/state.db");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: temp.path().join(".cdf/packages"),
        state_store_path: state_path,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-discovered-runtime").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path,
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-rest-discovered-runtime").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    assert_eq!(report.row_count, 2);
    assert_eq!(report.checkpoint.delta.schema_hash, schema_hash);
    assert_eq!(report.receipt.schema_hash, schema_hash);
    assert_eq!(transport.requests().len(), 1);
}

#[test]
fn general_project_run_rejects_unsupported_parquet_disposition_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_MERGE);
    let package_id = "pkg-general-parquet-merge-rejected";
    let package_root = temp.path().join(".cdf/packages");
    let parquet_root = temp.path().join(".cdf/lake");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project(parquet_project_run_request(
        &resource,
        package_id,
        &package_root,
        &parquet_root,
        &state_path,
        "run-general-parquet-merge-rejected",
    )))
    .unwrap_err();

    assert!(error.to_string().contains("Parquet destination"));
    assert!(!package_root.join(package_id).exists());
    assert!(!parquet_root.exists());
    assert!(!state_path.exists());
}

#[test]
fn general_project_run_rejects_unsupported_postgres_schema_before_writes() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), POSTGRES_UNSUPPORTED_FILE_RESOURCE);
    let package_id = "pkg-general-postgres-unsupported-schema";
    let package_root = temp.path().join(".cdf/packages");
    let state_path = temp.path().join(".cdf/state.db");
    let target = PostgresTarget::new(Some(&postgres.schema), "events_unsupported").unwrap();

    let error = futures_executor::block_on(run_project(postgres_project_run_request(
        &resource,
        package_id,
        &package_root,
        &postgres.url,
        target.clone(),
        &state_path,
        "run-general-postgres-unsupported-schema",
    )))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("Postgres destination does not support Arrow type Timestamp(Millisecond"),
        "{error}"
    );
    assert!(!package_root.join(package_id).exists());
    assert!(!state_path.exists());
    let mut client = postgres.client();
    let target_exists: Option<String> = client
        .query_one(
            "SELECT to_regclass($1)::text",
            &[&format!("{}.events_unsupported", postgres.schema)],
        )
        .unwrap()
        .get(0);
    let loads_exists: Option<String> = client
        .query_one(
            "SELECT to_regclass($1)::text",
            &[&format!("{}._cdf_loads", postgres.schema)],
        )
        .unwrap()
        .get(0);
    assert!(target_exists.is_none());
    assert!(loads_exists.is_none());
}

#[test]
fn parquet_artifact_recovery_after_general_run_failure_does_not_need_source() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-parquet-recovery";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let parquet_root = temp.path().join(".cdf/lake");
    let state_path = temp.path().join(".cdf/state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before parquet checkpoint"));
    let mut request = parquet_project_run_request(
        &resource,
        package_id,
        &package_root,
        &parquet_root,
        &state_path,
        "run-general-parquet-recovery",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();

    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let target = receipts[0].target.clone();
    let report = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: ResolvedProjectDestination::parquet_filesystem(&parquet_root, target).unwrap(),
        checkpoint_store: &store,
        receipt: receipts[0].clone(),
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
}

#[test]
fn parquet_artifact_replay_after_source_loss_without_receipt_commits_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-parquet-artifact-replay";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let parquet_root = temp.path().join(".cdf/lake");
    let replay_root = temp.path().join(".cdf/replay-lake");
    let state_path = temp.path().join(".cdf/state.db");
    let replay_state_path = temp.path().join(".cdf/replay-state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before parquet checkpoint"));
    let mut request = parquet_project_run_request(
        &resource,
        package_id,
        &package_root,
        &parquet_root,
        &state_path,
        "run-general-parquet-artifact-replay",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();
    fs::remove_file(temp.path().join("data/events.ndjson")).unwrap();
    remove_package_receipts(&package_dir);
    assert!(package_receipts(&package_dir).is_empty());

    let store = SqliteCheckpointStore::open(&replay_state_path).unwrap();
    let target = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .destination_commit
        .target;
    let report = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: ResolvedProjectDestination::parquet_filesystem(&replay_root, target).unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: true
        }
    );
    assert_eq!(package_receipts(&package_dir), vec![report.receipt.clone()]);
    assert!(
        ParquetDestination::new_filesystem(&replay_root)
            .unwrap()
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
    assert_eq!(
        assert_head(&store, &report.checkpoint.delta)
            .delta
            .checkpoint_id,
        report.checkpoint.delta.checkpoint_id
    );
}

#[test]
fn postgres_artifact_recovery_after_durable_receipt_commits_without_source_contact() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-postgres-recovery";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let state_path = temp.path().join(".cdf/state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before postgres checkpoint"));
    let target = PostgresTarget::new(Some(&postgres.schema), "events_recovery").unwrap();
    let mut request = postgres_project_run_request(
        &resource,
        package_id,
        &package_root,
        &postgres.url,
        target.clone(),
        &state_path,
        "run-general-postgres-recovery",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();
    fs::remove_file(temp.path().join("data/events.ndjson")).unwrap();

    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let report = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: ResolvedProjectDestination::postgres(
            postgres.url.clone(),
            target,
            MergeDedupPolicy::Last,
            None,
        )
        .unwrap(),
        checkpoint_store: &store,
        receipt: receipts[0].clone(),
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
    let mut client = postgres.client();
    let rows: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                postgres.table("events_recovery")
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 2);
}

#[test]
fn postgres_artifact_replay_after_source_loss_without_receipt_commits_checkpoint() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-postgres-artifact-replay";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let state_path = temp.path().join(".cdf/state.db");
    let replay_state_path = temp.path().join(".cdf/replay-state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before postgres checkpoint"));
    let target = PostgresTarget::new(Some(&postgres.schema), "events_artifact_replay").unwrap();
    let mut request = postgres_project_run_request(
        &resource,
        package_id,
        &package_root,
        &postgres.url,
        target.clone(),
        &state_path,
        "run-general-postgres-artifact-replay",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();
    fs::remove_file(temp.path().join("data/events.ndjson")).unwrap();
    remove_package_receipts(&package_dir);
    reset_postgres_schema(&postgres);
    assert!(package_receipts(&package_dir).is_empty());

    let store = SqliteCheckpointStore::open(&replay_state_path).unwrap();
    let report = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: ResolvedProjectDestination::postgres(
            postgres.url.clone(),
            target,
            MergeDedupPolicy::Last,
            None,
        )
        .unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommitReceiptOnly {
            package_receipt_recorded: true
        }
    );
    assert_eq!(package_receipts(&package_dir), vec![report.receipt.clone()]);
    assert!(
        PostgresDestination::connect(postgres.url.clone())
            .unwrap()
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
    assert_eq!(
        assert_head(&store, &report.checkpoint.delta)
            .delta
            .checkpoint_id,
        report.checkpoint.delta.checkpoint_id
    );
    let mut client = postgres.client();
    let rows: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                postgres.table("events_artifact_replay")
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 2);
}

#[test]
fn postgres_artifact_replay_rejects_mismatched_explicit_target_before_mutation() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-postgres-target-mismatch";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let state_path = temp.path().join(".cdf/state.db");
    let replay_state_path = temp.path().join(".cdf/replay-state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before postgres checkpoint"));
    let target = PostgresTarget::new(Some(&postgres.schema), "events_target_match").unwrap();
    let mut request = postgres_project_run_request(
        &resource,
        package_id,
        &package_root,
        &postgres.url,
        target,
        &state_path,
        "run-general-postgres-target-mismatch",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();
    fs::remove_file(temp.path().join("data/events.ndjson")).unwrap();
    remove_package_receipts(&package_dir);
    reset_postgres_schema(&postgres);
    let delta = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta;

    let store = SqliteCheckpointStore::open(&replay_state_path).unwrap();
    let wrong_target = PostgresTarget::new(Some(&postgres.schema), "events_target_wrong").unwrap();
    let error = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: ResolvedProjectDestination::postgres(
            postgres.url.clone(),
            wrong_target,
            MergeDedupPolicy::Last,
            None,
        )
        .unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: None,
    })
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not match package destination commit target"),
        "{error}"
    );
    assert!(package_receipts(&package_dir).is_empty());
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    assert!(
        store
            .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_empty()
    );
    assert!(!postgres_table_exists(&postgres, "events_target_match"));
    assert!(!postgres_table_exists(&postgres, "events_target_wrong"));
}

#[test]
fn general_project_run_rejects_raw_compiled_rest_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let rest_resource = rest_resource();
    let package_id = "pkg-general-rest-rejected";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::local_file(&rest_resource),
        plan: live_plan(&rest_resource, package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-rejected").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path.clone(),
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-rest-rejected").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();

    assert!(error.to_string().contains("local file resources"));
    assert!(!package_root.join(package_id).exists());
    assert!(!duckdb_path.exists());
    assert!(!state_path.exists());
}

#[test]
fn general_project_run_rejects_rest_missing_secret_provider_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = rest_runtime_resource();
    let transport = RecordingTransport::new([json_response(r#"{ "items": [] }"#)]);
    let resource = compiled
        .to_rest_resource(cdf_declarative::RestRuntimeDependencies::new(
            transport.clone(),
        ))
        .unwrap();
    let package_id = "pkg-general-rest-missing-secret";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-missing-secret").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path.clone(),
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-rest-missing-secret").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();

    assert!(error.to_string().contains("SecretProvider"));
    assert_eq!(transport.requests().len(), 0);
    assert!(!package_root.join(package_id).exists());
    assert!(!duckdb_path.exists());
    assert!(!state_path.exists());
}

#[test]
fn general_project_run_rejects_rest_missing_secret_value_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = rest_runtime_resource();
    let transport = RecordingTransport::new([json_response(r#"{ "items": [] }"#)]);
    let resource = compiled
        .to_rest_resource(
            cdf_declarative::RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new(std::iter::empty::<(&str, &str)>()),
            ),
        )
        .unwrap();
    let package_id = "pkg-general-rest-missing-secret-value";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-missing-secret-value").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path.clone(),
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-rest-missing-secret-value").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();

    assert!(error.to_string().contains("missing test secret"));
    assert_eq!(transport.requests().len(), 0);
    assert!(!package_root.join(package_id).exists());
    assert!(!duckdb_path.exists());
    assert!(!state_path.exists());
}

#[test]
fn general_project_run_rejects_rest_without_cursor_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = rest_resource();
    let transport = RecordingTransport::new([json_response(r#"[{ "id": 1 }]"#)]);
    let resource = compiled
        .to_rest_resource(cdf_declarative::RestRuntimeDependencies::new(
            transport.clone(),
        ))
        .unwrap();
    let package_id = "pkg-general-rest-no-cursor";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-no-cursor").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path.clone(),
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-rest-no-cursor").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();

    assert!(error.to_string().contains("ordered cursor"));
    assert_eq!(transport.requests().len(), 0);
    assert!(!package_root.join(package_id).exists());
    assert!(!duckdb_path.exists());
    assert!(!state_path.exists());
}

#[test]
fn general_project_run_window_closes_inexact_numeric_rest_cursor() {
    let temp = tempfile::tempdir().unwrap();
    let document = cdf_declarative::parse_toml(
        &REST_RUNTIME_RESOURCE
            .replace(r#"ordering = "exact""#, r#"ordering = "best_effort""#)
            .replace(r#"lag = "0ms""#, r#"lag = "5ms""#),
    )
    .unwrap();
    let compiled = cdf_declarative::compile_document(&document)
        .unwrap()
        .remove(0);
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": 20 }
        ] }"#,
    )]);
    let resource = compiled
        .to_rest_resource(
            cdf_declarative::RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    let package_id = "pkg-general-rest-window-close-numeric";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-window-close-numeric").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path.clone(),
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-rest-window-close-numeric").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    assert_eq!(transport.requests().len(), 1);
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("expected REST run to checkpoint a cursor position");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(15));
}

#[test]
fn general_project_run_rejects_sql_missing_secret_provider_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = sql_runtime_resource("public.orders");
    let resource = compiled
        .to_sql_resource(cdf_declarative::SqlRuntimeDependencies::new())
        .unwrap();
    let package_id = "pkg-general-sql-missing-secret";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::sql(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-sql-missing-secret").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path.clone(),
            TargetName::new("orders").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-sql-missing-secret").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();

    assert!(error.to_string().contains("SecretProvider"));
    assert!(!package_root.join(package_id).exists());
    assert!(!duckdb_path.exists());
    assert!(!state_path.exists());
}

#[test]
fn general_project_run_rejects_sql_empty_secret_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = sql_runtime_resource("public.orders");
    let resource = compiled
        .to_sql_resource(
            cdf_declarative::SqlRuntimeDependencies::new().with_secret_provider(
                StaticSecretProvider::new([("secret://env/POSTGRES_URL", "")]),
            ),
        )
        .unwrap();
    let package_id = "pkg-general-sql-empty-secret";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::sql(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-sql-empty-secret").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path.clone(),
            TargetName::new("orders").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-sql-empty-secret").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();

    assert!(error.to_string().contains("empty value"));
    assert!(!package_root.join(package_id).exists());
    assert!(!duckdb_path.exists());
    assert!(!state_path.exists());
}

#[test]
fn general_project_run_executes_table_backed_postgres_sql_resource_stream() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("source_orders");
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"id\" BIGINT NOT NULL,
                \"updated_at\" BIGINT NOT NULL
            );
            INSERT INTO {} (\"id\", \"updated_at\") VALUES (1, 10), (2, 20)",
            table, table
        ))
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let compiled = sql_runtime_resource(&table);
    let resource = compiled
        .to_sql_resource(
            cdf_declarative::SqlRuntimeDependencies::new().with_secret_provider(
                StaticSecretProvider::new([("secret://env/POSTGRES_URL", postgres.url.clone())]),
            ),
        )
        .unwrap();
    let package_id = "pkg-general-sql-runtime";
    let package_root = temp.path().join(".cdf/packages");
    let state_path = temp.path().join(".cdf/state.db");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::sql(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root,
        state_store_path: state_path,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-sql-runtime").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path,
            TargetName::new("orders").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-sql-runtime").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    assert_eq!(report.row_count, 2);
    assert_eq!(report.segment_count, 1);
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("expected SQL run to checkpoint a cursor position");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

#[test]
fn general_project_run_records_failure_after_durable_receipt_without_advancing_state() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-general-run-failed";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("injected general failure"));
    let mut request = project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-general-failed",
    );
    request.after_receipt_verified = Some(&hook);

    let error = futures_executor::block_on(run_project(request)).unwrap_err();

    assert!(error.to_string().contains("injected general failure"));
    let ledger = SqliteRunLedger::open(&state_path).unwrap();
    let snapshot = ledger
        .snapshot(&RunId::new("run-general-failed").unwrap())
        .unwrap()
        .unwrap();
    let kinds = snapshot
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
            RunEventKind::RunFailed,
        ]
    );
    assert!(
        snapshot
            .events
            .last()
            .unwrap()
            .details
            .attributes
            .contains_key("elapsed_ms")
    );
    assert_eq!(
        snapshot
            .events
            .last()
            .unwrap()
            .details
            .attributes
            .get("error_kind"),
        Some(&RunEventValue::String("internal".to_owned()))
    );

    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let scope = resource.descriptor().state_scope.clone();
    assert!(
        store
            .head(
                &PipelineId::new("pipeline-live").unwrap(),
                &resource.descriptor().resource_id,
                &scope
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let destination = destination(&duckdb_path);
    assert!(destination.verify_receipt(&receipts[0]).unwrap().verified);
}

#[test]
fn package_artifact_recovery_after_general_run_failure_does_not_need_source() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-general-recovery";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before checkpoint"));
    let mut request = project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-general-recovery",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();

    let destination = destination(&duckdb_path);
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let receipts = package_receipts(&package_dir);
    let report = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: resolved_duckdb_destination(&destination, receipts[0].target.clone()),
        checkpoint_store: &store,
        receipt: receipts[0].clone(),
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
}

#[test]
fn live_file_run_rejects_non_file_resource_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let file_resource = live_file_resource(temp.path());
    let rest_resource = rest_resource();
    let package_id = "pkg-live-rest-rejected";
    let package_root = temp.path().join(".cdf/packages");
    let error = futures_executor::block_on(run_local_file_to_duckdb_checkpoint(
        LocalFileDuckDbRunRequest {
            resource: &rest_resource,
            plan: live_plan(&file_resource, package_id),
            package_root: package_root.clone(),
            destination_path: temp.path().join(".cdf/dev.duckdb"),
            state_store_path: temp.path().join(".cdf/state.db"),
            pipeline_id: PipelineId::new("pipeline-live").unwrap(),
            target: TargetName::new("items").unwrap(),
            package_id: package_id.to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-live-rest-rejected").unwrap(),
            after_receipt_verified: None,
        },
    ))
    .unwrap_err();

    assert!(error.to_string().contains("local file resources"));
    assert!(!package_root.join(package_id).exists());
    assert!(!temp.path().join(".cdf/dev.duckdb").exists());
    assert!(!temp.path().join(".cdf/state.db").exists());
}

#[test]
fn live_file_run_rejects_plan_package_id_mismatch_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_root = temp.path().join(".cdf/packages");
    let error = futures_executor::block_on(run_local_file_to_duckdb_checkpoint(
        LocalFileDuckDbRunRequest {
            resource: &resource,
            plan: live_plan(&resource, "pkg-live-plan-id"),
            package_root: package_root.clone(),
            destination_path: temp.path().join(".cdf/dev.duckdb"),
            state_store_path: temp.path().join(".cdf/state.db"),
            pipeline_id: PipelineId::new("pipeline-live").unwrap(),
            target: TargetName::new("events").unwrap(),
            package_id: "pkg-live-request-id".to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-live-plan-id").unwrap(),
            after_receipt_verified: None,
        },
    ))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not match explicit package id")
    );
    assert!(!package_root.join("pkg-live-request-id").exists());
    assert!(!package_root.join("pkg-live-plan-id").exists());
    assert!(!temp.path().join(".cdf/dev.duckdb").exists());
    assert!(!temp.path().join(".cdf/state.db").exists());
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
    let request = state_delta_request(&resource, package_id, temp.path());

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
            SourcePosition::Log(LogPosition {
                version: 1,
                log: "orders".to_owned(),
                offset: 11,
                sequence: None,
            }),
        ],
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("divergent segment source positions")
    );
}

#[test]
fn state_delta_normalizes_file_manifest_entries_for_file_scope() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-state-delta-file-scope-normalize";
    let output = engine_output_with_positions(
        &temp.path().join(package_id),
        package_id,
        vec![file_position("/tmp/cdf/events-a.ndjson")],
    );
    let request = state_delta_request(&resource, package_id, temp.path());
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
        Some("sha256:/tmp/cdf/events-a.ndjson")
    );
    let SourcePosition::FileManifest(segment_manifest) = &delta.segments[0].output_position else {
        panic!("state segment should retain file manifest evidence");
    };
    assert_eq!(segment_manifest.files[0].path, "events-a.ndjson");
}

#[test]
fn state_delta_window_closes_timestamp_cursor_positions() {
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
                micros: 300_000_000,
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
fn state_delta_window_closes_date_cursor_positions() {
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
        cursor_position("event_day", CursorValue::I64(7))
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
            version: 1,
            token: "next-page".to_owned(),
        })],
    )
    .unwrap_err();
    assert!(page_token_error.to_string().contains("page-token-only"));

    let mixed_position = SourcePosition::Composite(CompositePosition {
        version: 1,
        positions: BTreeMap::from([
            (
                "cursor".to_owned(),
                cursor_position("updated_at", CursorValue::I64(10)),
            ),
            (
                "page".to_owned(),
                SourcePosition::PageToken(PageToken {
                    version: 1,
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
            SourcePosition::Log(LogPosition {
                version: 1,
                log: "orders".to_owned(),
                offset: 11,
                sequence: None,
            }),
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
fn state_delta_rejects_incompatible_cursor_fields_values_and_lag() {
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
    let lag_error = state_delta_for_positions(
        &unsigned_resource,
        temp.path(),
        "pkg-state-delta-incompatible-cursor-lag",
        vec![cursor_position("updated_at", CursorValue::U64(3))],
    )
    .unwrap_err();
    assert!(
        lag_error.to_string().contains("incompatible cursor lag"),
        "{lag_error}"
    );
}

struct CommitFailingStore {
    inner: SqliteCheckpointStore,
    fail_commit: AtomicBool,
}

impl CommitFailingStore {
    fn new() -> Self {
        Self {
            inner: SqliteCheckpointStore::open_in_memory().unwrap(),
            fail_commit: AtomicBool::new(true),
        }
    }

    fn allow_commit(&self) {
        self.fail_commit.store(false, Ordering::SeqCst);
    }
}

impl CheckpointStore for CommitFailingStore {
    fn propose(&self, delta: StateDelta) -> Result<Checkpoint> {
        self.inner.propose(delta)
    }

    fn commit(&self, checkpoint_id: &CheckpointId, receipt: Receipt) -> Result<Checkpoint> {
        if self.fail_commit.load(Ordering::SeqCst) {
            return Err(CdfError::internal("injected checkpoint commit failure"));
        }
        self.inner.commit(checkpoint_id, receipt)
    }

    fn abandon(&self, checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        self.inner.abandon(checkpoint_id)
    }

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        self.inner.head(pipeline_id, resource_id, scope)
    }

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        self.inner.history(pipeline_id, resource_id, scope)
    }

    fn rewind(&self, request: RewindRequest) -> Result<RewindReport> {
        self.inner.rewind(request)
    }
}

struct HeadOnlyCommitFailingStore {
    head: Checkpoint,
}

impl CheckpointStore for HeadOnlyCommitFailingStore {
    fn propose(&self, _delta: StateDelta) -> Result<Checkpoint> {
        Err(CdfError::internal("unexpected propose"))
    }

    fn commit(&self, _checkpoint_id: &CheckpointId, _receipt: Receipt) -> Result<Checkpoint> {
        Err(CdfError::internal("injected checkpoint commit failure"))
    }

    fn abandon(&self, _checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        Err(CdfError::internal("unexpected abandon"))
    }

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        if &self.head.delta.pipeline_id == pipeline_id
            && &self.head.delta.resource_id == resource_id
            && &self.head.delta.scope == scope
        {
            Ok(Some(self.head.clone()))
        } else {
            Ok(None)
        }
    }

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        Ok(self
            .head(pipeline_id, resource_id, scope)?
            .into_iter()
            .collect())
    }

    fn rewind(&self, _request: RewindRequest) -> Result<RewindReport> {
        Err(CdfError::internal("unexpected rewind"))
    }
}

#[test]
fn generic_package_replay_and_recovery_drive_mock_runtime_without_destination_branch() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-generic-mock");
    build_package(&package_dir, "pkg-generic-mock", PackageStatus::Packaged);
    let reader = PackageReader::open(&package_dir).unwrap();
    let inputs = reader.replay_inputs().unwrap();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let destination = MockDestination::new();
    let counters = MockDestinationCounters::new();
    let mut registry = ProjectDestinationRegistry::new();
    registry
        .register(MockProjectDestinationDriver::new(
            destination.clone(),
            counters.clone(),
        ))
        .unwrap();
    let context = ProjectResolutionContext::new();
    let mut replay_runtime = registry.resolve("mock://registered", &context).unwrap();
    let replay_stages = Arc::new(Mutex::new(Vec::new()));
    let replay_stages_hook = Arc::clone(&replay_stages);
    let stage_hook = move |stage: PackageReplayStage<'_>| {
        replay_stages_hook
            .lock()
            .unwrap()
            .push(package_replay_stage_name(stage));
        Ok(())
    };

    let report = replay_package_with_runtime(
        reader,
        package_dir.clone(),
        replay_runtime.as_mut(),
        &store,
        inputs.clone(),
        PackageReplayHooks {
            stage: Some(&stage_hook),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(counters.resolve_count(), 1);
    assert_eq!(counters.prepare_count(), 1);
    assert_eq!(counters.bind_count(), 1);
    assert_eq!(destination.write_count(), 1);
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: false
        }
    );
    assert!(report.receipt.covers_state_delta(&inputs.state_delta));
    assert_eq!(
        *replay_stages.lock().unwrap(),
        vec![
            "package_replay_verified",
            "checkpoint_proposed",
            "destination_write_ready",
            "destination_commit_started",
            "destination_segment_acknowledged",
            "destination_receipt_recorded",
            "checkpoint_committed",
            "package_status_updated",
        ]
    );

    let writes_before_recovery = destination.write_count();
    let mut recovery_runtime = registry.resolve("mock://registered", &context).unwrap();
    let recovery_stages = Arc::new(Mutex::new(Vec::new()));
    let recovery_stages_hook = Arc::clone(&recovery_stages);
    let recovery_stage_hook = move |stage: PackageReplayStage<'_>| {
        recovery_stages_hook
            .lock()
            .unwrap()
            .push(package_replay_stage_name(stage));
        Ok(())
    };
    let recovery = recover_package_with_runtime(
        PackageReader::open(&package_dir).unwrap(),
        recovery_runtime.as_mut(),
        &store,
        inputs.clone(),
        report.receipt.clone(),
        PackageReplayHooks {
            stage: Some(&recovery_stage_hook),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(counters.resolve_count(), 2);
    assert_eq!(counters.prepare_count(), 1);
    assert_eq!(counters.bind_count(), 1);
    assert_eq!(destination.write_count(), writes_before_recovery);
    assert_eq!(
        recovery.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
    assert_eq!(recovery.checkpoint, report.checkpoint);
    assert_eq!(
        *recovery_stages.lock().unwrap(),
        vec![
            "destination_receipt_recorded",
            "checkpoint_committed",
            "package_status_updated",
        ]
    );
}

#[test]
fn generic_stage_hook_stops_mock_replay_before_destination_write() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-generic-mock-failpoint");
    build_package(
        &package_dir,
        "pkg-generic-mock-failpoint",
        PackageStatus::Packaged,
    );
    let reader = PackageReader::open(&package_dir).unwrap();
    let inputs = reader.replay_inputs().unwrap();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let destination = MockDestination::new();
    let counters = MockDestinationCounters::new();
    let mut registry = ProjectDestinationRegistry::new();
    registry
        .register(MockProjectDestinationDriver::new(
            destination.clone(),
            counters.clone(),
        ))
        .unwrap();
    let context = ProjectResolutionContext::new();
    let mut runtime = registry
        .resolve("mock://registered-failpoint", &context)
        .unwrap();
    let stage_hook = |stage: PackageReplayStage<'_>| {
        if matches!(stage, PackageReplayStage::DestinationWriteReady) {
            return Err(CdfError::internal("stop at generic destination write hook"));
        }
        Ok(())
    };

    let error = replay_package_with_runtime(
        reader,
        package_dir.clone(),
        runtime.as_mut(),
        &store,
        inputs.clone(),
        PackageReplayHooks {
            stage: Some(&stage_hook),
            ..Default::default()
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("generic destination write"));
    assert_eq!(counters.resolve_count(), 1);
    assert_eq!(counters.prepare_count(), 0);
    assert_eq!(counters.bind_count(), 0);
    assert_eq!(destination.write_count(), 0);
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let history = store
        .history(
            &inputs.state_delta.pipeline_id,
            &inputs.state_delta.resource_id,
            &inputs.state_delta.scope,
        )
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Proposed);
}

#[test]
fn replay_commits_duckdb_receipt_then_checkpoint_and_marks_package_checkpointed() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-success");
    let manifest = build_package(&package_dir, "pkg-success", PackageStatus::Packaged);
    let delta = delta(&manifest, "checkpoint-success");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let report = replay_prepared_package(replay_request(
        &package_dir,
        &destination,
        &store,
        delta.clone(),
    ))
    .unwrap();

    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(
        assert_head(&store, &delta).delta.checkpoint_id,
        delta.checkpoint_id
    );
    assert_eq!(report.receipt.package_hash, delta.package_hash);
    assert_eq!(
        report.receipt.idempotency_token.as_str(),
        delta.package_hash.as_str()
    );
    assert_eq!(
        report.receipt.segment_acks[0].byte_count,
        delta.segments[0].byte_count
    );
    assert!(
        destination
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
    assert_eq!(package_receipts(&package_dir), vec![report.receipt.clone()]);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: true
        }
    );
}

#[test]
fn artifact_replay_reconstructs_delta_and_commit_request_from_package_files() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-artifact-success");
    let manifest = build_package(
        &package_dir,
        "pkg-artifact-success",
        PackageStatus::Packaged,
    );
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let report =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap();

    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(
        report.checkpoint.delta.checkpoint_id.as_str(),
        "checkpoint-artifact"
    );
    assert_eq!(
        report.checkpoint.delta.package_hash.as_str(),
        manifest.package_hash
    );
    assert_eq!(
        report.receipt.idempotency_token.as_str(),
        manifest.package_hash
    );
    assert_head(&store, &report.checkpoint.delta);
    assert_eq!(package_receipts(&package_dir), vec![report.receipt.clone()]);
}

#[test]
fn artifact_replay_rejects_corrupted_or_missing_preimages_before_mutation() {
    for path in [
        STATE_INPUT_CHECKPOINT_FILE,
        STATE_PROPOSED_DELTA_FILE,
        DESTINATION_COMMIT_PLAN_FILE,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let package_dir = temp
            .path()
            .join(format!("pkg-artifact-tampered-{}", path.replace('/', "-")));
        build_package(
            &package_dir,
            "pkg-artifact-tampered",
            PackageStatus::Packaged,
        );
        fs::write(package_dir.join(path), b"{\"tampered\":true}").unwrap();
        let db_path = temp.path().join("local.duckdb");
        let duckdb = destination(&db_path);
        let store = SqliteCheckpointStore::open_in_memory().unwrap();

        let error =
            replay_package_from_artifacts(artifact_replay_request(&package_dir, &duckdb, &store))
                .unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&format!("tampered identity file {path}")),
            "{path}: {error}"
        );
        assert!(
            store
                .history(
                    &PipelineId::new("pipeline-1").unwrap(),
                    &ResourceId::new("orders").unwrap(),
                    &scope()
                )
                .unwrap()
                .is_empty()
        );
        assert!(!db_path.exists());

        let temp = tempfile::tempdir().unwrap();
        let package_dir = temp
            .path()
            .join(format!("pkg-artifact-missing-{}", path.replace('/', "-")));
        build_package(
            &package_dir,
            "pkg-artifact-missing",
            PackageStatus::Packaged,
        );
        fs::remove_file(package_dir.join(path)).unwrap();
        let db_path = temp.path().join("local.duckdb");
        let duckdb = destination(&db_path);
        let store = SqliteCheckpointStore::open_in_memory().unwrap();

        let error =
            replay_package_from_artifacts(artifact_replay_request(&package_dir, &duckdb, &store))
                .unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&format!("missing identity file {path}")),
            "{path}: {error}"
        );
        assert!(
            store
                .history(
                    &PipelineId::new("pipeline-1").unwrap(),
                    &ResourceId::new("orders").unwrap(),
                    &scope()
                )
                .unwrap()
                .is_empty()
        );
        assert!(!db_path.exists());
    }
}

#[test]
fn artifact_replay_rejects_manifest_package_hash_mismatch_before_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-artifact-hash-mismatch");
    build_package(
        &package_dir,
        "pkg-artifact-hash-mismatch",
        PackageStatus::Packaged,
    );
    let mut manifest = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .clone();
    manifest.package_hash = "sha256:wrong-package".to_owned();
    manifest.signature.signing_input = manifest.package_hash.clone();
    fs::write(
        package_dir.join(MANIFEST_FILE),
        canonical_json_bytes(&manifest).unwrap(),
    )
    .unwrap();
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let error =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("manifest identity hash mismatch")
    );
    assert!(
        store
            .history(
                &PipelineId::new("pipeline-1").unwrap(),
                &ResourceId::new("orders").unwrap(),
                &scope()
            )
            .unwrap()
            .is_empty()
    );
    assert!(!db_path.exists());
}

#[test]
fn duplicate_destination_replay_returns_duplicate_receipt_and_commits_new_store_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-duplicate");
    let db_path = temp.path().join("local.duckdb");
    let (destination, first_delta, first_receipt) =
        stage_successful_replay(&package_dir, &db_path, "checkpoint-first");
    let mut second_delta = first_delta.clone();
    second_delta.checkpoint_id = CheckpointId::new("checkpoint-second").unwrap();
    let second_store = SqliteCheckpointStore::open_in_memory().unwrap();

    let report = replay_prepared_package(replay_request(
        &package_dir,
        &destination,
        &second_store,
        second_delta.clone(),
    ))
    .unwrap();

    assert_eq!(report.receipt.receipt_id, first_receipt.receipt_id);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: true,
            package_receipt_recorded: false
        }
    );
    assert_eq!(
        assert_head(&second_store, &second_delta)
            .delta
            .checkpoint_id,
        second_delta.checkpoint_id
    );
    let snapshot = destination.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(snapshot.loads.len(), 1);
    assert_eq!(snapshot.state.len(), 1);
}

#[test]
fn recovery_verifies_durable_receipt_and_commits_without_new_destination_write() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-recovery");
    let manifest = build_package(&package_dir, "pkg-recovery", PackageStatus::Packaged);
    let delta = delta(&manifest, "checkpoint-recovery");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before checkpoint commit"));
    let mut request = replay_request(&package_dir, &destination, &store, delta.clone());
    request.after_receipt_verified = Some(&hook);

    let error = replay_prepared_package(request).unwrap_err();
    assert!(error.to_string().contains("stop before checkpoint commit"));
    assert_no_head(&store, &delta);
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let loads_before = destination
        .read_mirror_snapshot_read_only()
        .unwrap()
        .loads
        .len();

    let report = recover_prepared_package(recovery_request(
        &package_dir,
        &destination,
        &store,
        delta.clone(),
        receipts[0].clone(),
    ))
    .unwrap();

    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        destination
            .read_mirror_snapshot_read_only()
            .unwrap()
            .loads
            .len(),
        loads_before
    );
}

#[test]
fn zero_segment_processed_package_recovers_after_receipt_without_source_or_data_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-zero-recovery");
    build_zero_segment_processed_package(&package_dir, "pkg-zero-recovery");
    let db_path = temp.path().join("local.duckdb");
    let state_path = temp.path().join("state.db");
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before zero checkpoint"));

    let error = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: ResolvedProjectDestination::duckdb(
            &db_path,
            TargetName::new("orders").unwrap(),
        )
        .unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: Some(&hook),
    })
    .unwrap_err();
    assert!(error.to_string().contains("stop before zero checkpoint"));
    let reader = PackageReader::open(&package_dir).unwrap();
    let inputs = reader.replay_inputs().unwrap();
    assert!(inputs.state_delta.segments.is_empty());
    let receipts = reader.receipts().unwrap();
    assert_eq!(receipts.len(), 1);
    assert!(receipts[0].segment_acks.is_empty());
    assert!(
        store
            .head(
                &inputs.state_delta.pipeline_id,
                &inputs.state_delta.resource_id,
                &inputs.state_delta.scope,
            )
            .unwrap()
            .is_none()
    );

    let recovered = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: ResolvedProjectDestination::duckdb(
            &db_path,
            TargetName::new("orders").unwrap(),
        )
        .unwrap(),
        checkpoint_store: &store,
        receipt: receipts[0].clone(),
        after_receipt_verified: None,
    })
    .unwrap();
    assert_eq!(recovered.checkpoint.status, CheckpointStatus::Committed);
    assert!(recovered.checkpoint.delta.segments.is_empty());
    assert_eq!(
        recovered.checkpoint.delta.output_position,
        inputs.state_delta.output_position
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);

    let mirrors = DuckDbDestination::new(&db_path)
        .unwrap()
        .read_mirror_snapshot_read_only()
        .unwrap();
    assert_eq!(mirrors.loads.len(), 1);
    assert!(mirrors.state.is_empty());
}

#[test]
fn named_failpoint_after_checkpoint_proposal_stops_before_destination_write() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-after-proposal");
    let manifest = build_package(&package_dir, "pkg-after-proposal", PackageStatus::Packaged);
    let delta = delta(&manifest, "checkpoint-after-proposal");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let hook = |stage: RuntimeStage<'_>| {
        if matches!(stage, RuntimeStage::DestinationWriteReady) {
            return Err(CdfError::internal("stop after checkpoint proposal"));
        }
        Ok(())
    };

    let error = replay_prepared_package_with_stage_hook(
        replay_request(&package_dir, &destination, &store, delta.clone()),
        Some(&hook),
    )
    .unwrap_err();

    assert!(error.to_string().contains("stop after checkpoint proposal"));
    assert!(!db_path.exists());
    assert!(package_receipts(&package_dir).is_empty());
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    assert_no_head(&store, &delta);
    let history = store
        .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Proposed);
}

#[test]
fn named_failpoint_after_checkpoint_commit_allows_status_only_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-after-checkpoint");
    let manifest = build_package(
        &package_dir,
        "pkg-after-checkpoint",
        PackageStatus::Packaged,
    );
    let delta = delta(&manifest, "checkpoint-after-checkpoint");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let hook = |stage: RuntimeStage<'_>| {
        if let RuntimeStage::CheckpointCommitted { checkpoint } = stage {
            assert!(checkpoint.receipt.is_some());
            return Err(CdfError::internal("stop after checkpoint commit"));
        }
        Ok(())
    };

    let error = replay_prepared_package_with_stage_hook(
        replay_request(&package_dir, &destination, &store, delta.clone()),
        Some(&hook),
    )
    .unwrap_err();

    assert!(error.to_string().contains("stop after checkpoint commit"));
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let head = assert_head(&store, &delta);
    assert_eq!(head.status, CheckpointStatus::Committed);
    assert_eq!(head.delta, delta);
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    assert!(destination.verify_receipt(&receipts[0]).unwrap().verified);
    let snapshot_before = destination.read_mirror_snapshot_read_only().unwrap();

    let report = recover_prepared_package(recovery_request(
        &package_dir,
        &destination,
        &store,
        delta.clone(),
        receipts[0].clone(),
    ))
    .unwrap();

    assert_eq!(report.checkpoint, head);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        destination.read_mirror_snapshot_read_only().unwrap(),
        snapshot_before
    );
}

#[test]
fn recovery_reuses_only_exact_committed_checkpoint_head() {
    assert_bad_reuse_head_rejected(
        "pkg-reuse-proposed-head",
        "checkpoint-reuse-proposed-head",
        |head| {
            head.status = CheckpointStatus::Proposed;
        },
    );
    assert_bad_reuse_head_rejected("pkg-reuse-non-head", "checkpoint-reuse-non-head", |head| {
        head.is_head = false;
    });
    assert_bad_reuse_head_rejected(
        "pkg-reuse-wrong-delta",
        "checkpoint-reuse-wrong-delta",
        |head| {
            head.delta.checkpoint_id = CheckpointId::new("checkpoint-other-head").unwrap();
        },
    );
    assert_bad_reuse_head_rejected(
        "pkg-reuse-missing-receipt",
        "checkpoint-reuse-missing-receipt",
        |head| {
            head.receipt = None;
        },
    );
}

#[test]
fn recovery_rejects_receipt_verification_failure_without_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-verification-failure");
    let db_path = temp.path().join("local.duckdb");
    let (destination, staged_delta, mut receipt) =
        stage_successful_replay(&package_dir, &db_path, "checkpoint-staged");
    receipt.committed_at_ms += 1;
    let mut recovery_delta = staged_delta.clone();
    recovery_delta.checkpoint_id = CheckpointId::new("checkpoint-verify-failure").unwrap();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    store.propose(recovery_delta.clone()).unwrap();

    let error = recover_prepared_package(recovery_request(
        &package_dir,
        &destination,
        &store,
        recovery_delta.clone(),
        receipt,
    ))
    .unwrap_err();

    assert!(error.to_string().contains("did not verify"));
    assert_no_head(&store, &recovery_delta);
}

#[test]
fn recovery_rejects_bad_receipt_identity_without_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-bad-identity");
    let db_path = temp.path().join("local.duckdb");
    let (destination, staged_delta, mut receipt) =
        stage_successful_replay(&package_dir, &db_path, "checkpoint-staged");
    receipt.idempotency_token = IdempotencyToken::new("different-token").unwrap();
    let mut recovery_delta = staged_delta.clone();
    recovery_delta.checkpoint_id = CheckpointId::new("checkpoint-bad-identity").unwrap();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    store.propose(recovery_delta.clone()).unwrap();

    let error = recover_prepared_package(recovery_request(
        &package_dir,
        &destination,
        &store,
        recovery_delta.clone(),
        receipt,
    ))
    .unwrap_err();

    assert!(error.to_string().contains("idempotency token"));
    assert_no_head(&store, &recovery_delta);
}

#[test]
fn recovery_rejects_missing_segment_ack_without_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-missing-ack");
    let db_path = temp.path().join("local.duckdb");
    let (destination, staged_delta, mut receipt) =
        stage_successful_replay(&package_dir, &db_path, "checkpoint-staged");
    receipt.segment_acks.clear();
    let mut recovery_delta = staged_delta.clone();
    recovery_delta.checkpoint_id = CheckpointId::new("checkpoint-missing-ack").unwrap();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    store.propose(recovery_delta.clone()).unwrap();

    let error = recover_prepared_package(recovery_request(
        &package_dir,
        &destination,
        &store,
        recovery_delta.clone(),
        receipt,
    ))
    .unwrap_err();

    assert!(error.to_string().contains("acknowledges 0 segment"));
    assert_no_head(&store, &recovery_delta);
}

#[test]
fn replay_rejects_non_replayable_package_before_checkpoint_or_destination_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-not-replayable");
    let manifest = build_package(&package_dir, "pkg-not-replayable", PackageStatus::Validated);
    let delta = delta(&manifest, "checkpoint-not-replayable");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let error = replay_prepared_package(replay_request(
        &package_dir,
        &destination,
        &store,
        delta.clone(),
    ))
    .unwrap_err();

    assert!(error.to_string().contains("not replayable"));
    assert_eq!(package_status(&package_dir), PackageStatus::Validated);
    assert!(
        store
            .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_empty()
    );
    assert!(!db_path.exists());
}

#[test]
fn replay_rejects_bad_package_hash_and_segment_mismatch_before_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-mismatch");
    let manifest = build_package(&package_dir, "pkg-mismatch", PackageStatus::Packaged);
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);

    let bad_hash_store = SqliteCheckpointStore::open_in_memory().unwrap();
    let mut bad_hash_delta = delta(&manifest, "checkpoint-bad-hash");
    bad_hash_delta.package_hash = PackageHash::new("sha256:wrong-package").unwrap();
    let error = replay_prepared_package(replay_request(
        &package_dir,
        &destination,
        &bad_hash_store,
        bad_hash_delta.clone(),
    ))
    .unwrap_err();
    assert!(error.to_string().contains("package hash"));
    assert!(
        bad_hash_store
            .history(
                &bad_hash_delta.pipeline_id,
                &bad_hash_delta.resource_id,
                &bad_hash_delta.scope
            )
            .unwrap()
            .is_empty()
    );

    let bad_segment_store = SqliteCheckpointStore::open_in_memory().unwrap();
    let mut bad_segment_delta = delta(&manifest, "checkpoint-bad-segment");
    bad_segment_delta.segments[0].byte_count += 1;
    let error = replay_prepared_package(replay_request(
        &package_dir,
        &destination,
        &bad_segment_store,
        bad_segment_delta.clone(),
    ))
    .unwrap_err();
    assert!(error.to_string().contains("StateDelta segment"));
    assert!(
        bad_segment_store
            .history(
                &bad_segment_delta.pipeline_id,
                &bad_segment_delta.resource_id,
                &bad_segment_delta.scope
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Packaged);
    assert!(!db_path.exists());
}

#[test]
fn destination_failure_before_receipt_abandons_proposed_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-destination-failure");
    let manifest = build_package(
        &package_dir,
        "pkg-destination-failure",
        PackageStatus::Packaged,
    );
    let delta = delta(&manifest, "checkpoint-destination-failure");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let mut request = replay_request(&package_dir, &destination, &store, delta.clone());
    request.inputs.destination_commit.disposition = WriteDisposition::CdcApply;

    let error = replay_prepared_package(request).unwrap_err();

    assert!(
        error.to_string().contains("does not support cdc_apply"),
        "{error}"
    );
    let history = store
        .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Abandoned);
    assert_no_head(&store, &delta);
    assert!(package_receipts(&package_dir).is_empty());
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
}

#[test]
fn checkpoint_failure_after_receipt_keeps_receipt_recoverable_and_state_unadvanced() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-checkpoint-failure");
    let manifest = build_package(
        &package_dir,
        "pkg-checkpoint-failure",
        PackageStatus::Packaged,
    );
    let delta = delta(&manifest, "checkpoint-fails-once");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = CommitFailingStore::new();

    let error = replay_prepared_package(replay_request(
        &package_dir,
        &destination,
        &store,
        delta.clone(),
    ))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("injected checkpoint commit failure")
    );
    assert_no_head(&store, &delta);
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    assert!(destination.verify_receipt(&receipts[0]).unwrap().verified);
    assert!(matches!(
        store
            .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()[0]
            .status,
        CheckpointStatus::Proposed
    ));

    store.allow_commit();
    let report = recover_prepared_package(recovery_request(
        &package_dir,
        &destination,
        &store,
        delta.clone(),
        receipts[0].clone(),
    ))
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(
        assert_head(&store, &delta).delta.checkpoint_id,
        delta.checkpoint_id
    );
}

#[test]
fn recovery_refuses_receipts_not_covering_state_delta_counts() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-wrong-counts");
    let db_path = temp.path().join("local.duckdb");
    let (destination, staged_delta, mut receipt) =
        stage_successful_replay(&package_dir, &db_path, "checkpoint-staged");
    receipt.segment_acks[0].row_count += 1;
    let mut recovery_delta = staged_delta.clone();
    recovery_delta.checkpoint_id = CheckpointId::new("checkpoint-wrong-counts").unwrap();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    store.propose(recovery_delta.clone()).unwrap();

    let error = recover_prepared_package(recovery_request(
        &package_dir,
        &destination,
        &store,
        recovery_delta.clone(),
        receipt,
    ))
    .unwrap_err();

    assert!(error.to_string().contains("StateDelta has"));
    assert_no_head(&store, &recovery_delta);
}
