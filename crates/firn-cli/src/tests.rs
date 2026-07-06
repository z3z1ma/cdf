use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use firn_kernel::{
    CHECKPOINT_STATE_VERSION, CheckpointId, CheckpointStore, CommitCounts, CursorPosition,
    CursorValue, DestinationId, IdempotencyToken, PackageHash, PipelineId, Receipt, ReceiptId,
    ResourceId, SchemaHash, ScopeKey, SegmentAck, SegmentId, SourcePosition, StateDelta,
    StateSegment, TargetName, VerifyClause, WriteDisposition,
};
use firn_package::{PackageBuilder, PackageReader, PackageStatus};
use firn_state_sqlite::SqliteCheckpointStore;
use serde_json::Value;
use serde_json::json;

use crate::invoke;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

const PROJECT: &str = r#"
[project]
name = "cli_test"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.firn/state.db"
packages = ".firn/packages"
destination = "duckdb://.firn/dev.duckdb"

[resources."local.*"]
source = "resources/files.toml"
"#;

const RESOURCE: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "timestamp_micros", nullable = false, timezone = "UTC" },
] }
"#;

#[test]
fn help_lists_required_command_surface() {
    let result = run(["firn", "--help"]);

    assert_eq!(result.exit_code, 0);
    for command in [
        "init",
        "validate",
        "plan",
        "explain",
        "run",
        "preview",
        "sql",
        "inspect",
        "diff schema",
        "contract freeze|show|test",
        "state show|history",
        "resume",
        "replay package",
        "backfill",
        "package ls",
        "doctor",
        "status",
    ] {
        assert!(result.stdout.contains(command), "missing {command}");
    }
}

#[test]
fn validate_json_reports_project_shape() {
    let project = TestProject::new();
    let result = run([
        "firn",
        "--json",
        "--project",
        project.root_str(),
        "validate",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["ok"], true);
    assert_eq!(json["command"], "validate");
    assert_eq!(json["result"]["environment"]["name"], "dev");
    assert_eq!(json["result"]["declarative_resources"], 1);
}

#[test]
fn plan_json_exposes_pushdown_ddl_guarantee_and_state_advancement() {
    let project = TestProject::new();
    let result = run([
        "firn",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--select",
        "id,updated_at",
        "--filter",
        "id > 10",
        "--limit",
        "5",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let result = &json["result"];
    assert_eq!(result["resource_id"], "local.events");
    assert_eq!(result["will_fetch"]["limit"], 5);
    assert_eq!(
        result["pushdown"]["unsupported"][0]["fidelity"],
        "unsupported"
    );
    assert_eq!(result["ddl_preview"]["supported"], false);
    assert!(
        result["delivery_guarantee"]
            .as_str()
            .unwrap()
            .contains("AtLeast")
    );
    assert_eq!(
        result["state_advancement"]["advances_after"],
        "destination receipt is recorded and CheckpointStore::commit verifies coverage"
    );
}

#[test]
fn preview_returns_explicit_unsupported_without_creating_package_root() {
    let project = TestProject::new();
    let package_root = project.root.join(".firn/packages");
    let result = run([
        "firn",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 78);
    assert!(
        !package_root.exists(),
        "preview must not create the package root"
    );
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["ok"], false);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("resource runtime open implementation")
    );
}

#[test]
fn run_returns_unsupported_instead_of_faking_writes() {
    let project = TestProject::new();
    let result = run([
        "firn",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 78);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("CheckpointStore::commit")
    );
}

#[test]
fn sql_mounts_checkpoint_package_and_receipt_tables_as_json_rows() {
    let project = TestProject::new();
    let fixture = create_system_sql_fixture(&project);
    let result = run([
        "firn",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select p.package_hash, p.status, s.segment_id, c.checkpoint_id, c.status as checkpoint_status, r.receipt_id from packages p join package_segments s using (package_hash) join checkpoints c using (package_hash) join package_receipts r using (package_hash) order by p.package_id",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "sql");
    let result = json["result"].as_object().unwrap();
    assert_eq!(result.len(), 3);
    assert!(result.contains_key("columns"));
    assert!(result.contains_key("rows"));
    assert!(result.contains_key("tables"));
    assert_eq!(
        json["result"]["columns"],
        json!([
            "package_hash",
            "status",
            "segment_id",
            "checkpoint_id",
            "checkpoint_status",
            "receipt_id"
        ])
    );
    assert_eq!(json["result"]["rows"].as_array().unwrap().len(), 1);
    let row = &json["result"]["rows"][0];
    assert_eq!(row[0], fixture.package_hash);
    assert_eq!(row[1], "checkpointed");
    assert_eq!(row[2], "seg-000001");
    assert_eq!(row[3], "checkpoint-sql-1");
    assert_eq!(row[4], "committed");
    assert_eq!(row[5], "receipt-sql-1");
    assert!(
        json["result"]["tables"]
            .as_array()
            .unwrap()
            .iter()
            .any(|table| table == "package_files")
    );
}

#[test]
fn sql_human_output_is_concise_for_scheduler_logs() {
    let project = TestProject::new();
    let result = run([
        "firn",
        "--project",
        project.root_str(),
        "sql",
        "select count(*) as package_count from packages",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_eq!(
        result.stdout,
        "sql returned 1 row(s) from local system history\n"
    );
}

#[test]
fn sql_read_only_query_does_not_create_local_artifacts() {
    let project = TestProject::new();
    let state_path = project.root.join(".firn/state.db");
    let package_root = project.root.join(".firn/packages");
    let result = run([
        "firn",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select count(*) as checkpoint_count from checkpoints",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["columns"], json!(["checkpoint_count"]));
    assert_eq!(json["result"]["rows"][0][0], 0);
    assert!(!state_path.exists(), "sql must not create the state DB");
    assert!(
        !package_root.exists(),
        "sql must not create the package root"
    );
}

#[test]
fn sql_rejects_non_readonly_before_artifact_access() {
    let project = TestProject::new();
    let state_path = project.root.join(".firn/state.db");
    let package_root = project.root.join(".firn/packages");
    let result = run([
        "firn",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "delete from packages",
    ]);

    assert_eq!(result.exit_code, 2);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("read-only")
    );
    assert!(
        !state_path.exists(),
        "rejected sql must not create state DB"
    );
    assert!(
        !package_root.exists(),
        "rejected sql must not create package root"
    );
}

#[test]
fn package_verify_uses_lower_package_reader() {
    let temp = TempDir::new("firn-cli-package");
    let package_dir = temp.path().join("pkg");
    let builder = PackageBuilder::create(&package_dir, "pkg-1").unwrap();
    builder.finish_with_status(PackageStatus::Packaged).unwrap();

    let result = run([
        "firn",
        "--json",
        "package",
        "verify",
        package_dir.to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "package verify");
    assert!(
        json["result"]["package_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
}

struct SystemSqlFixture {
    package_hash: String,
}

#[test]
fn state_show_uses_sqlite_store_and_reports_missing_head() {
    let project = TestProject::new();
    let result = run([
        "firn",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "show",
        "--pipeline",
        "pipeline-1",
        "--resource",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "state show");
    assert!(json["result"]["head"].is_null());
}

#[test]
fn unknown_command_returns_usage_exit_code() {
    let result = run(["firn", "--json", "bogus"]);

    assert_eq!(result.exit_code, 2);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert_eq!(json["error"]["exit_code"], 2);
}

struct TestProject {
    _temp: TempDir,
    root: PathBuf,
    root_string: String,
}

impl TestProject {
    fn new() -> Self {
        let temp = TempDir::new("firn-cli-project");
        let root = temp.path().to_path_buf();
        fs::create_dir_all(root.join("resources")).unwrap();
        fs::create_dir_all(root.join(".firn")).unwrap();
        fs::write(root.join("firn.toml"), PROJECT).unwrap();
        fs::write(root.join("resources/files.toml"), RESOURCE).unwrap();
        let root_string = root.to_str().unwrap().to_owned();
        Self {
            _temp: temp,
            root,
            root_string,
        }
    }

    fn root_str(&self) -> &str {
        &self.root_string
    }
}

fn create_system_sql_fixture(project: &TestProject) -> SystemSqlFixture {
    let package_root = project.root.join(".firn/packages");
    fs::create_dir_all(&package_root).unwrap();
    let package_dir = package_root.join("pkg-sql-1");
    let mut builder = PackageBuilder::create(&package_dir, "pkg-sql-1").unwrap();
    builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), &[sample_sql_batch()])
        .unwrap();
    let manifest = builder
        .finish_with_status(PackageStatus::Checkpointed)
        .unwrap();
    let receipt = sample_sql_receipt(&manifest.package_hash);
    PackageReader::open(&package_dir)
        .unwrap()
        .append_receipt(receipt.clone())
        .unwrap();

    let store = SqliteCheckpointStore::open(project.root.join(".firn/state.db")).unwrap();
    let delta = sample_sql_delta(&manifest.package_hash);
    let checkpoint_id = delta.checkpoint_id.clone();
    store.propose(delta).unwrap();
    store.commit(&checkpoint_id, receipt).unwrap();

    SystemSqlFixture {
        package_hash: manifest.package_hash,
    }
}

fn sample_sql_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let name: ArrayRef = Arc::new(StringArray::from(vec![
        Some("ada"),
        Some("grace"),
        Some("margaret"),
    ]));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

fn sample_sql_delta(package_hash: &str) -> StateDelta {
    let output_position = SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "updated_at".to_owned(),
        value: CursorValue::I64(42),
    });
    StateDelta {
        checkpoint_id: CheckpointId::new("checkpoint-sql-1").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("local.events").unwrap(),
        scope: ScopeKey::Resource,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: output_position.clone(),
        package_hash: PackageHash::new(package_hash).unwrap(),
        schema_hash: SchemaHash::new("schema-sql-1").unwrap(),
        segments: vec![StateSegment {
            segment_id: SegmentId::new("seg-000001").unwrap(),
            scope: ScopeKey::Resource,
            output_position,
            row_count: 3,
            byte_count: 30,
        }],
    }
}

fn sample_sql_receipt(package_hash: &str) -> Receipt {
    Receipt {
        receipt_id: ReceiptId::new("receipt-sql-1").unwrap(),
        destination: DestinationId::new("local-test").unwrap(),
        target: TargetName::new("events").unwrap(),
        package_hash: PackageHash::new(package_hash).unwrap(),
        segment_acks: vec![SegmentAck {
            segment_id: SegmentId::new("seg-000001").unwrap(),
            row_count: 3,
            byte_count: 30,
        }],
        disposition: WriteDisposition::Append,
        idempotency_token: IdempotencyToken::new(package_hash).unwrap(),
        transaction: None,
        counts: CommitCounts {
            rows_written: 3,
            rows_inserted: Some(3),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: SchemaHash::new("schema-sql-1").unwrap(),
        migrations: Vec::new(),
        committed_at_ms: 1_700_000_000_000,
        verify: VerifyClause {
            kind: "sql".to_owned(),
            statement: "select count(*) from events where _firn_package = ?".to_owned(),
            parameters: BTreeMap::new(),
        },
    }
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let parent = PathBuf::from("target").join("firn-cli-tests");
        let path = parent.join(format!(
            "{prefix}-{}-{counter}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&parent).unwrap();
        fs::create_dir(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn run<const N: usize>(args: [&str; N]) -> crate::InvocationResult {
    invoke(args.into_iter().map(OsString::from))
}

fn stderr_or_stdout_json(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}
