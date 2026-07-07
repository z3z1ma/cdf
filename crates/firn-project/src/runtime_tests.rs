use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use firn_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use firn_dest_duckdb::DuckDbDestination;
use firn_engine::{
    EnginePlan, EnginePlanInput, EngineRunOutput, EngineRunOutputWithSegmentPositions,
    EngineSegmentPosition, ExecutionProfile, LineageSummary, PlanBoundedness, Planner,
};
use firn_kernel::{
    CHECKPOINT_STATE_VERSION, Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore,
    CursorPosition, CursorValue, FileManifest, FilePosition, FirnError, IdempotencyToken,
    PackageHash, PartitionId, PipelineId, Receipt, ResourceId, ResourceStream, Result,
    RewindReport, RewindRequest, ScanRequest, SchemaHash, ScopeKey, SegmentId, SourcePosition,
    StateDelta, StateSegment, TargetName, WriteDisposition,
};
use firn_package::{PackageBuilder, PackageManifest, PackageReader, PackageStatus};
use firn_state_sqlite::SqliteCheckpointStore;

use crate::{
    LocalFileDuckDbRunRequest, PreparedDuckDbRecoveryRequest, PreparedDuckDbReplayRequest,
    PreparedReceiptSource, recover_prepared_duckdb_package, replay_prepared_duckdb_package,
    run_local_file_to_duckdb_checkpoint, runtime::state_delta_from_run,
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
    builder
        .write_json_artifact(
            "destination/commit_plan.json",
            &BTreeMap::from([("target", "orders"), ("disposition", "append")]),
        )
        .unwrap();
    builder
        .write_segment(
            firn_kernel::SegmentId::new("seg-000001").unwrap(),
            &[sample_batch(
                vec![1, 2, 3],
                vec![Some("ada"), Some("grace"), None],
            )],
        )
        .unwrap();
    builder.finish_with_status(status).unwrap()
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

fn live_file_resource(root: &Path) -> firn_declarative::CompiledResource {
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("data/events.ndjson"),
        "{\"id\":1,\"updated_at\":\"2026-07-06T00:00:00Z\"}\n\
         {\"id\":2,\"updated_at\":\"2026-07-06T00:01:00Z\"}\n",
    )
    .unwrap();
    let document = firn_declarative::parse_toml(LIVE_FILE_RESOURCE).unwrap();
    firn_declarative::compile_document_with_project_root(&document, root)
        .unwrap()
        .remove(0)
}

fn rest_resource() -> firn_declarative::CompiledResource {
    let document = firn_declarative::parse_toml(REST_RESOURCE).unwrap();
    firn_declarative::compile_document(&document)
        .unwrap()
        .remove(0)
}

fn live_plan(resource: &firn_declarative::CompiledResource, package_id: &str) -> EnginePlan {
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

#[test]
fn live_file_run_post_receipt_failure_keeps_checkpoint_uncommitted_and_receipt_recoverable() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-live-hook-failure";
    let package_root = temp.path().join(".firn/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".firn/dev.duckdb");
    let state_path = temp.path().join(".firn/state.db");
    let pipeline_id = PipelineId::new("pipeline-live").unwrap();
    let hook = |_receipt: &Receipt| Err(FirnError::internal("injected live checkpoint failure"));

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
            .contains("injected live checkpoint failure")
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
fn live_file_run_rejects_non_file_resource_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let file_resource = live_file_resource(temp.path());
    let rest_resource = rest_resource();
    let package_id = "pkg-live-rest-rejected";
    let package_root = temp.path().join(".firn/packages");
    let error = futures_executor::block_on(run_local_file_to_duckdb_checkpoint(
        LocalFileDuckDbRunRequest {
            resource: &rest_resource,
            plan: live_plan(&file_resource, package_id),
            package_root: package_root.clone(),
            destination_path: temp.path().join(".firn/dev.duckdb"),
            state_store_path: temp.path().join(".firn/state.db"),
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
    assert!(!temp.path().join(".firn/dev.duckdb").exists());
    assert!(!temp.path().join(".firn/state.db").exists());
}

#[test]
fn live_file_run_rejects_plan_package_id_mismatch_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_root = temp.path().join(".firn/packages");
    let error = futures_executor::block_on(run_local_file_to_duckdb_checkpoint(
        LocalFileDuckDbRunRequest {
            resource: &resource,
            plan: live_plan(&resource, "pkg-live-plan-id"),
            package_root: package_root.clone(),
            destination_path: temp.path().join(".firn/dev.duckdb"),
            state_store_path: temp.path().join(".firn/state.db"),
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
    assert!(!temp.path().join(".firn/dev.duckdb").exists());
    assert!(!temp.path().join(".firn/state.db").exists());
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
                output_position: Some(file_position("/tmp/firn/a.ndjson")),
            },
            EngineSegmentPosition {
                segment_id: second.segment_id.clone(),
                output_position: Some(file_position("/tmp/firn/b.ndjson")),
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
            return Err(FirnError::internal("injected checkpoint commit failure"));
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
    let hook = |_receipt: &Receipt| Err(FirnError::internal("stop before checkpoint commit"));
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
