use std::{
    collections::{BTreeMap, VecDeque},
    env, fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_dest_duckdb::DuckDbDestination;
use cdf_dest_parquet::ParquetDestination;
use cdf_dest_postgres::{MergeDedupPolicy, PostgresDestination, PostgresTarget};
use cdf_engine::{
    EnginePlan, EnginePlanInput, EngineRunOutput, EngineRunOutputWithSegmentPositions,
    EngineSegmentPosition, ExecutionProfile, LineageSummary, PlanBoundedness, Planner,
};
use cdf_http::{HttpRequest, HttpResponse, HttpTransport, SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, Checkpoint, CheckpointId, CheckpointStatus,
    CheckpointStore, CompositePosition, CursorPosition, CursorValue, FileManifest, FilePosition,
    IdempotencyToken, LogPosition, PackageHash, PageToken, PartitionId, PipelineId, Receipt,
    ResourceId, ResourceStream, Result, RewindReport, RewindRequest, RunId, ScanRequest,
    SchemaHash, ScopeKey, SegmentId, SourcePosition, StateDelta, StateSegment, TargetName,
    WriteDisposition,
};
use cdf_package::{
    DESTINATION_COMMIT_PLAN_FILE, DestinationCommitPlanPreimage, MANIFEST_FILE, PackageBuilder,
    PackageManifest, PackageReader, PackageStatus, RECEIPTS_FILE, STATE_INPUT_CHECKPOINT_FILE,
    STATE_PROPOSED_DELTA_FILE, StateDeltaPreimage, canonical_json_bytes,
};
use cdf_state_sqlite::{RunEventKind, SqliteCheckpointStore, SqliteRunLedger};
use postgres::{Client, NoTls};
use tempfile::TempDir;

use crate::{
    LocalDuckDbLifecycleFailpoint, LocalFileDuckDbRunRequest, PackageArtifactDuckDbRecoveryRequest,
    PackageArtifactDuckDbReplayRequest, PackageArtifactParquetRecoveryRequest,
    PackageArtifactParquetReplayRequest, PackageArtifactPostgresRecoveryRequest,
    PackageArtifactPostgresReplayRequest, PreparedDuckDbRecoveryRequest,
    PreparedDuckDbReplayRequest, PreparedReceiptSource, ProjectReceiptSource,
    ProjectRunDestination, ProjectRunReport, ProjectRunRequest, ProjectRunResource,
    recover_duckdb_package_from_artifacts, recover_parquet_package_from_artifacts,
    recover_postgres_package_from_artifacts, recover_prepared_duckdb_package,
    replay_duckdb_package_from_artifacts, replay_parquet_package_from_artifacts,
    replay_postgres_package_from_artifacts, replay_prepared_duckdb_package,
    replay_prepared_duckdb_package_with_failpoint, run_local_file_to_duckdb_checkpoint,
    run_project, runtime::state_delta_from_run,
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
  { name = "updated_at", type = "timestamp_micros", nullable = false, timezone = "UTC" },
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
const SIMPLE_FILE_RESOURCE_MERGE: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.events"
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
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
) -> PreparedDuckDbReplayRequest<'a, Store> {
    PreparedDuckDbReplayRequest {
        package_dir: package_dir.to_path_buf(),
        destination,
        checkpoint_store,
        delta,
        target: TargetName::new("orders").unwrap(),
        disposition: WriteDisposition::Append,
        merge_keys: Vec::new(),
        schema_hash: SchemaHash::new(SCHEMA_HASH).unwrap(),
        after_receipt_verified: None,
    }
}

fn artifact_replay_request<'a, Store: CheckpointStore + ?Sized>(
    package_dir: &Path,
    destination: &'a DuckDbDestination,
    checkpoint_store: &'a Store,
) -> PackageArtifactDuckDbReplayRequest<'a, Store> {
    PackageArtifactDuckDbReplayRequest {
        package_dir: package_dir.to_path_buf(),
        destination,
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
) -> PreparedDuckDbRecoveryRequest<'a, Store> {
    PreparedDuckDbRecoveryRequest {
        package_dir: package_dir.to_path_buf(),
        destination,
        checkpoint_store,
        delta,
        target: TargetName::new("orders").unwrap(),
        disposition: WriteDisposition::Append,
        schema_hash: SchemaHash::new(SCHEMA_HASH).unwrap(),
        receipt,
        after_receipt_verified: None,
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
        "{\"id\":1,\"updated_at\":\"2026-07-06T00:00:00Z\"}\n\
         {\"id\":2,\"updated_at\":\"2026-07-06T00:01:00Z\"}\n",
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

fn state_delta_request<'a>(
    resource: &'a cdf_declarative::CompiledResource,
    package_id: &str,
    root: &Path,
) -> LocalFileDuckDbRunRequest<'a> {
    LocalFileDuckDbRunRequest {
        resource,
        plan: live_plan(resource, package_id),
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
    EngineRunOutputWithSegmentPositions {
        output: EngineRunOutput {
            manifest,
            segments,
            profile: ExecutionProfile::default(),
            lineage: LineageSummary::default(),
        },
        segment_positions,
    }
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

fn project_run_request<'a>(
    resource: &'a cdf_declarative::CompiledResource,
    package_id: &str,
    package_root: &Path,
    duckdb_path: &Path,
    state_path: &Path,
    run_id: &str,
) -> ProjectRunRequest<'a> {
    ProjectRunRequest {
        resource: ProjectRunResource::LocalFile(resource),
        plan: live_plan(resource, package_id),
        package_root: package_root.to_path_buf(),
        state_store_path: state_path.to_path_buf(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        destination: ProjectRunDestination::DuckDb {
            database_path: duckdb_path.to_path_buf(),
            target: TargetName::new("events").unwrap(),
        },
        run_id: Some(RunId::new(run_id).unwrap()),
        after_receipt_verified: None,
    }
}

fn parquet_project_run_request<'a>(
    resource: &'a cdf_declarative::CompiledResource,
    package_id: &str,
    package_root: &Path,
    parquet_root: &Path,
    state_path: &Path,
    run_id: &str,
) -> ProjectRunRequest<'a> {
    ProjectRunRequest {
        resource: ProjectRunResource::LocalFile(resource),
        plan: live_plan(resource, package_id),
        package_root: package_root.to_path_buf(),
        state_store_path: state_path.to_path_buf(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        destination: ProjectRunDestination::ParquetFilesystem {
            root: parquet_root.to_path_buf(),
            target: TargetName::new("events").unwrap(),
        },
        run_id: Some(RunId::new(run_id).unwrap()),
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
    ProjectRunRequest {
        resource: ProjectRunResource::LocalFile(resource),
        plan: live_plan(resource, package_id),
        package_root: package_root.to_path_buf(),
        state_store_path: state_path.to_path_buf(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        destination: ProjectRunDestination::Postgres {
            database_url: database_url.to_owned(),
            target,
            dedup: MergeDedupPolicy::Last,
            existing_table: None,
        },
        run_id: Some(RunId::new(run_id).unwrap()),
        after_receipt_verified: None,
    }
}

fn file_position(path: &str) -> SourcePosition {
    SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: path.to_owned(),
            size_bytes: 42,
            etag: None,
            sha256: Some(format!("sha256:{path}")),
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
    let report = replay_prepared_duckdb_package(replay_request(
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

    let error = recover_prepared_duckdb_package(recovery_request(
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
        resource: ProjectRunResource::Rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root,
        state_store_path: state_path,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-runtime").unwrap(),
        destination: ProjectRunDestination::DuckDb {
            database_path: duckdb_path,
            target: TargetName::new("items").unwrap(),
        },
        run_id: Some(RunId::new(run_id).unwrap()),
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
            RunEventKind::PackageFinalized,
            RunEventKind::CheckpointProposed,
            RunEventKind::DestinationCommitStarted,
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
        report.ledger_snapshot.events[3].package_hash,
        Some(report.package_hash.clone())
    );
    assert_eq!(
        report.ledger_snapshot.events[6].receipt_id,
        Some(report.receipt.receipt_id.clone())
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
            RunEventKind::PackageFinalized,
            RunEventKind::CheckpointProposed,
            RunEventKind::DestinationCommitStarted,
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
        target,
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
            RunEventKind::PackageFinalized,
            RunEventKind::CheckpointProposed,
            RunEventKind::DestinationCommitStarted,
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
        target,
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

    let destination = ParquetDestination::new_filesystem(&parquet_root).unwrap();
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let report = recover_parquet_package_from_artifacts(PackageArtifactParquetRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: &destination,
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

    let destination = ParquetDestination::new_filesystem(&replay_root).unwrap();
    let store = SqliteCheckpointStore::open(&replay_state_path).unwrap();
    let report = replay_parquet_package_from_artifacts(PackageArtifactParquetReplayRequest {
        package_dir: package_dir.clone(),
        destination: &destination,
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
        destination
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
        target,
        &state_path,
        "run-general-postgres-recovery",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();
    fs::remove_file(temp.path().join("data/events.ndjson")).unwrap();

    let destination = PostgresDestination::connect(postgres.url.clone()).unwrap();
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let report = recover_postgres_package_from_artifacts(PackageArtifactPostgresRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: &destination,
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

    let destination = PostgresDestination::connect(postgres.url.clone()).unwrap();
    let store = SqliteCheckpointStore::open(&replay_state_path).unwrap();
    let report = replay_postgres_package_from_artifacts(PackageArtifactPostgresReplayRequest {
        package_dir: package_dir.clone(),
        destination: &destination,
        checkpoint_store: &store,
        target,
        dedup: MergeDedupPolicy::Last,
        existing_table: None,
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
        destination
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

    let destination = PostgresDestination::connect(postgres.url.clone()).unwrap();
    let store = SqliteCheckpointStore::open(&replay_state_path).unwrap();
    let wrong_target = PostgresTarget::new(Some(&postgres.schema), "events_target_wrong").unwrap();
    let error = replay_postgres_package_from_artifacts(PackageArtifactPostgresReplayRequest {
        package_dir: package_dir.clone(),
        destination: &destination,
        checkpoint_store: &store,
        target: wrong_target,
        dedup: MergeDedupPolicy::Last,
        existing_table: None,
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
        resource: ProjectRunResource::LocalFile(&rest_resource),
        plan: live_plan(&rest_resource, package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-rejected").unwrap(),
        destination: ProjectRunDestination::DuckDb {
            database_path: duckdb_path.clone(),
            target: TargetName::new("items").unwrap(),
        },
        run_id: Some(RunId::new("run-general-rest-rejected").unwrap()),
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
        resource: ProjectRunResource::Rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-missing-secret").unwrap(),
        destination: ProjectRunDestination::DuckDb {
            database_path: duckdb_path.clone(),
            target: TargetName::new("items").unwrap(),
        },
        run_id: Some(RunId::new("run-general-rest-missing-secret").unwrap()),
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
        resource: ProjectRunResource::Rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-missing-secret-value").unwrap(),
        destination: ProjectRunDestination::DuckDb {
            database_path: duckdb_path.clone(),
            target: TargetName::new("items").unwrap(),
        },
        run_id: Some(RunId::new("run-general-rest-missing-secret-value").unwrap()),
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
        resource: ProjectRunResource::Rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-no-cursor").unwrap(),
        destination: ProjectRunDestination::DuckDb {
            database_path: duckdb_path.clone(),
            target: TargetName::new("items").unwrap(),
        },
        run_id: Some(RunId::new("run-general-rest-no-cursor").unwrap()),
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
        resource: ProjectRunResource::Rest(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-window-close-numeric").unwrap(),
        destination: ProjectRunDestination::DuckDb {
            database_path: duckdb_path.clone(),
            target: TargetName::new("items").unwrap(),
        },
        run_id: Some(RunId::new("run-general-rest-window-close-numeric").unwrap()),
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
        resource: ProjectRunResource::Sql(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-sql-missing-secret").unwrap(),
        destination: ProjectRunDestination::DuckDb {
            database_path: duckdb_path.clone(),
            target: TargetName::new("orders").unwrap(),
        },
        run_id: Some(RunId::new("run-general-sql-missing-secret").unwrap()),
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
        resource: ProjectRunResource::Sql(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-sql-empty-secret").unwrap(),
        destination: ProjectRunDestination::DuckDb {
            database_path: duckdb_path.clone(),
            target: TargetName::new("orders").unwrap(),
        },
        run_id: Some(RunId::new("run-general-sql-empty-secret").unwrap()),
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
        resource: ProjectRunResource::Sql(&resource),
        plan: live_plan(resource.compiled(), package_id),
        package_root,
        state_store_path: state_path,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-sql-runtime").unwrap(),
        destination: ProjectRunDestination::DuckDb {
            database_path: duckdb_path,
            target: TargetName::new("orders").unwrap(),
        },
        run_id: Some(RunId::new("run-general-sql-runtime").unwrap()),
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
            RunEventKind::PackageFinalized,
            RunEventKind::CheckpointProposed,
            RunEventKind::DestinationCommitStarted,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::RunFailed,
        ]
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
    let report = recover_duckdb_package_from_artifacts(PackageArtifactDuckDbRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: &destination,
        checkpoint_store: &store,
        receipt: receipts[0].clone(),
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        PreparedReceiptSource::SuppliedDurableReceipt
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
fn state_delta_rejects_divergent_segment_source_positions() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_dir = temp.path().join("pkg-state-delta-divergent");
    let manifest = build_package(
        &package_dir,
        "pkg-state-delta-divergent",
        PackageStatus::Packaged,
    );
    let first = manifest.identity.segments[0].clone();
    let mut second = first.clone();
    second.segment_id = SegmentId::new("seg-000002").unwrap();
    second.path = "data/seg-000002.arrow".to_owned();
    let mut manifest = manifest;
    manifest.identity.segments = vec![first.clone(), second.clone()];
    let output = EngineRunOutputWithSegmentPositions {
        output: EngineRunOutput {
            manifest,
            segments: vec![first.clone(), second.clone()],
            profile: ExecutionProfile::default(),
            lineage: LineageSummary::default(),
        },
        segment_positions: vec![
            EngineSegmentPosition {
                segment_id: first.segment_id.clone(),
                output_position: Some(file_position("/tmp/cdf/a.ndjson")),
            },
            EngineSegmentPosition {
                segment_id: second.segment_id.clone(),
                output_position: Some(file_position("/tmp/cdf/b.ndjson")),
            },
        ],
    };
    let request = LocalFileDuckDbRunRequest {
        resource: &resource,
        plan: live_plan(&resource, "pkg-state-delta-divergent"),
        package_root: temp.path().to_path_buf(),
        destination_path: temp.path().join("dev.duckdb"),
        state_store_path: temp.path().join("state.db"),
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        target: TargetName::new("events").unwrap(),
        package_id: "pkg-state-delta-divergent".to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-state-delta-divergent").unwrap(),
        after_receipt_verified: None,
    };

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
            .to_string()
            .contains("divergent segment source positions")
    );
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
    assert!(lag_error.to_string().contains("incompatible cursor lag"));
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
fn replay_commits_duckdb_receipt_then_checkpoint_and_marks_package_checkpointed() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-success");
    let manifest = build_package(&package_dir, "pkg-success", PackageStatus::Packaged);
    let delta = delta(&manifest, "checkpoint-success");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let report = replay_prepared_duckdb_package(replay_request(
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
        PreparedReceiptSource::DuckDbCommit {
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

    let report = replay_duckdb_package_from_artifacts(artifact_replay_request(
        &package_dir,
        &destination,
        &store,
    ))
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

        let error = replay_duckdb_package_from_artifacts(artifact_replay_request(
            &package_dir,
            &duckdb,
            &store,
        ))
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

        let error = replay_duckdb_package_from_artifacts(artifact_replay_request(
            &package_dir,
            &duckdb,
            &store,
        ))
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

    let error = replay_duckdb_package_from_artifacts(artifact_replay_request(
        &package_dir,
        &destination,
        &store,
    ))
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

    let report = replay_prepared_duckdb_package(replay_request(
        &package_dir,
        &destination,
        &second_store,
        second_delta.clone(),
    ))
    .unwrap();

    assert_eq!(report.receipt.receipt_id, first_receipt.receipt_id);
    assert_eq!(
        report.receipt_source,
        PreparedReceiptSource::DuckDbCommit {
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

    let error = replay_prepared_duckdb_package(request).unwrap_err();
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

    let report = recover_prepared_duckdb_package(recovery_request(
        &package_dir,
        &destination,
        &store,
        delta.clone(),
        receipts[0].clone(),
    ))
    .unwrap();

    assert_eq!(
        report.receipt_source,
        PreparedReceiptSource::SuppliedDurableReceipt
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
fn named_failpoint_after_checkpoint_proposal_stops_before_destination_write() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-after-proposal");
    let manifest = build_package(&package_dir, "pkg-after-proposal", PackageStatus::Packaged);
    let delta = delta(&manifest, "checkpoint-after-proposal");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let hook = |failpoint: LocalDuckDbLifecycleFailpoint, receipt: Option<&Receipt>| {
        assert!(receipt.is_none());
        if failpoint == LocalDuckDbLifecycleFailpoint::AfterCheckpointProposalBeforeDestinationWrite
        {
            return Err(CdfError::internal("stop after checkpoint proposal"));
        }
        Ok(())
    };

    let error = replay_prepared_duckdb_package_with_failpoint(
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
    let hook = |failpoint: LocalDuckDbLifecycleFailpoint, receipt: Option<&Receipt>| {
        if failpoint
            == LocalDuckDbLifecycleFailpoint::AfterCheckpointCommitBeforePackageStatusCheckpointed
        {
            assert!(receipt.is_some());
            return Err(CdfError::internal("stop after checkpoint commit"));
        }
        Ok(())
    };

    let error = replay_prepared_duckdb_package_with_failpoint(
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

    let report = recover_prepared_duckdb_package(recovery_request(
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
        PreparedReceiptSource::SuppliedDurableReceipt
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

    let error = recover_prepared_duckdb_package(recovery_request(
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

    let error = recover_prepared_duckdb_package(recovery_request(
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

    let error = recover_prepared_duckdb_package(recovery_request(
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

    let error = replay_prepared_duckdb_package(replay_request(
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
    let error = replay_prepared_duckdb_package(replay_request(
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
    let error = replay_prepared_duckdb_package(replay_request(
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
    request.disposition = WriteDisposition::CdcApply;

    let error = replay_prepared_duckdb_package(request).unwrap_err();

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

    let error = replay_prepared_duckdb_package(replay_request(
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
    let report = recover_prepared_duckdb_package(recovery_request(
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

    let error = recover_prepared_duckdb_package(recovery_request(
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
