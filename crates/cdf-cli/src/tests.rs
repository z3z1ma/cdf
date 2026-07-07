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
use cdf_dest_duckdb::{DuckDbCommitRequest, DuckDbDestination};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CheckpointId, CheckpointStore, CommitCounts, CursorPosition,
    CursorValue, DestinationCommitRequest, DestinationId, IdempotencyToken, PackageHash,
    PartitionId, PipelineId, Receipt, ReceiptId, ResourceId, SchemaHash, ScopeKey, SegmentAck,
    SegmentId, SourcePosition, StateDelta, StateSegment, TargetName, VerifyClause,
    WriteDisposition,
};
use cdf_package::{PackageBuilder, PackageReader, PackageStatus};
use cdf_state_sqlite::SqliteCheckpointStore;
use duckdb::Connection as DuckConnection;
use rusqlite::Connection;
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
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

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

const PYTHON_RESOURCE_PROJECT: &str = r#"
[project]
name = "cli_test"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."events.raw"]
source = "python://src/events.py#raw_events"
"#;

#[test]
fn help_lists_required_command_surface() {
    let result = run(["cdf", "--help"]);

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
        "package archive",
        "doctor",
        "status",
    ] {
        assert!(result.stdout.contains(command), "missing {command}");
    }
}

#[test]
fn validate_json_reports_project_shape() {
    let project = TestProject::new();
    let result = run(["cdf", "--json", "--project", project.root_str(), "validate"]);

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
        "cdf",
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
fn preview_reads_single_ndjson_file_without_creating_runtime_artifacts() {
    let project = TestProject::new();
    let package_root = project.root.join(".cdf/packages");
    let state_path = project.root.join(".cdf/state.db");
    let duckdb_path = project.root.join(".cdf/dev.duckdb");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        !package_root.exists(),
        "preview must not create the package root"
    );
    assert!(!state_path.exists(), "preview must not create state");
    assert!(
        !duckdb_path.exists(),
        "preview must not create destination data"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["ok"], true);
    assert_eq!(json["command"], "preview");
    assert_eq!(json["result"]["resource_id"], "local.events");
    assert_eq!(json["result"]["partition_id"], "files");
    assert_eq!(json["result"]["row_count"], 2);
    assert!(
        json["result"]["batch_id"]
            .as_str()
            .unwrap()
            .starts_with("local-events-files-")
    );
    assert!(json["result"]["byte_count"].as_u64().unwrap() > 0);
    assert_eq!(json["result"]["writes"]["package"], false);
    assert_eq!(json["result"]["writes"]["destination"], false);
    assert_eq!(json["result"]["writes"]["checkpoint"], false);
}

#[test]
fn preview_succeeds_for_csv_json_and_parquet_file_resources() {
    for format in ["csv", "json", "parquet"] {
        let project = TestProject::new();
        write_format_fixture(&project, format);

        let result = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "preview",
            "local.events",
        ]);

        assert_eq!(
            result.exit_code, 0,
            "format {format} stderr: {}",
            result.stderr
        );
        let json = stderr_or_stdout_json(&result.stdout);
        assert_eq!(json["result"]["resource_id"], "local.events");
        assert_eq!(json["result"]["row_count"], 2, "format {format}");
        assert!(!project.root.join(".cdf/packages").exists());
        assert!(!project.root.join(".cdf/state.db").exists());
        assert!(!project.root.join(".cdf/dev.duckdb").exists());
    }
}

#[test]
fn preview_zero_match_file_glob_fails_closed_without_writes() {
    let project = TestProject::new();
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "data");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("matched no files")
    );
}

#[test]
fn preview_missing_file_source_root_fails_as_zero_match_without_writes() {
    let project = TestProject::new();
    fs::remove_dir_all(project.root.join("data")).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "data");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("matched no files")
    );
}

#[test]
fn preview_missing_intermediate_literal_directory_fails_as_zero_match_without_writes() {
    let project = TestProject::new();
    write_resource_glob(&project, "missing/events.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "data");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("matched no files")
    );
}

#[test]
fn preview_multi_match_file_glob_fails_closed_without_writes() {
    let project = TestProject::new();
    fs::write(
        project.root.join("data/events-extra.ndjson"),
        "{\"id\":3,\"updated_at\":\"2026-07-06T00:02:00Z\"}\n",
    )
    .unwrap();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "data");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("matched 2 files")
    );
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("multi-file scan semantics are not supported")
    );
}

#[test]
fn preview_wildcard_directory_glob_requires_component_match() {
    let project = TestProject::new();
    fs::create_dir_all(project.root.join("data/match-a")).unwrap();
    fs::create_dir_all(project.root.join("data/other")).unwrap();
    fs::write(
        project.root.join("data/match-a/events.ndjson"),
        "{\"id\":1,\"updated_at\":\"2026-07-06T00:00:00Z\"}\n",
    )
    .unwrap();
    fs::write(
        project.root.join("data/other/events.ndjson"),
        "{\"id\":2,\"updated_at\":\"2026-07-06T00:01:00Z\"}\n",
    )
    .unwrap();
    write_resource_glob(&project, "match-*/events.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 1);
    assert_no_preview_writes(&project);
}

#[test]
fn preview_question_mark_glob_matches_exactly_one_character() {
    let project = TestProject::new();
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    fs::write(
        project.root.join("data/event1.ndjson"),
        "{\"id\":1,\"updated_at\":\"2026-07-06T00:00:00Z\"}\n",
    )
    .unwrap();
    fs::write(
        project.root.join("data/event12.ndjson"),
        "{\"id\":2,\"updated_at\":\"2026-07-06T00:01:00Z\"}\n",
    )
    .unwrap();
    write_resource_glob(&project, "event?.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 1);
    assert_no_preview_writes(&project);
}

#[test]
fn preview_double_star_glob_descends_into_physical_nested_directories() {
    let project = TestProject::new();
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    fs::create_dir_all(project.root.join("data/nested")).unwrap();
    fs::write(
        project.root.join("data/nested/events.ndjson"),
        "{\"id\":1,\"updated_at\":\"2026-07-06T00:00:00Z\"}\n",
    )
    .unwrap();
    write_resource_glob(&project, "**/*.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 1);
    assert_no_preview_writes(&project);
}

#[cfg(unix)]
#[test]
fn preview_double_star_glob_ignores_symlink_directory_loops() {
    let project = TestProject::new();
    std::os::unix::fs::symlink(project.root.join("data"), project.root.join("data/loop")).unwrap();
    write_resource_glob(&project, "**/*.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 2);
    assert_no_preview_writes(&project);
}

#[cfg(unix)]
#[test]
fn preview_wildcard_directory_glob_ignores_symlink_directories() {
    let project = TestProject::new();
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    fs::create_dir_all(project.root.join("data/real")).unwrap();
    fs::write(
        project.root.join("data/real/events.ndjson"),
        "{\"id\":1,\"updated_at\":\"2026-07-06T00:00:00Z\"}\n",
    )
    .unwrap();
    std::os::unix::fs::symlink(
        project.root.join("data/real"),
        project.root.join("data/alias"),
    )
    .unwrap();
    write_resource_glob(&project, "*/events.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 1);
    assert_no_preview_writes(&project);
}

#[cfg(unix)]
#[test]
fn preview_unreadable_glob_directory_reports_directory_read_error() {
    use std::os::unix::fs::PermissionsExt;

    let project = TestProject::new();
    let private = project.root.join("data/private");
    fs::create_dir_all(&private).unwrap();
    fs::set_permissions(&private, fs::Permissions::from_mode(0o000)).unwrap();
    write_resource_glob(&project, "private/*.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("read file source directory")
    );
}

#[cfg(unix)]
#[test]
fn preview_inaccessible_literal_child_reports_path_inspection_error() {
    use std::os::unix::fs::PermissionsExt;

    let project = TestProject::new();
    let private = project.root.join("data/private");
    fs::create_dir_all(&private).unwrap();
    fs::set_permissions(&private, fs::Permissions::from_mode(0o000)).unwrap();
    write_resource_glob(&project, "private/child/*.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("inspect file source path")
    );
}

#[test]
fn run_local_file_to_duckdb_commits_package_rows_mirrors_and_checkpoint() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "--resource",
        "local.events",
        "--pipeline",
        "pipeline-run",
        "--target",
        "events",
        "--package-id",
        "pkg-run-success",
        "--checkpoint-id",
        "checkpoint-run-success",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "run");
    assert_eq!(report["command"], "run");
    assert!(!report["run_id"].as_str().unwrap().is_empty());
    assert_eq!(report["resource_id"], "local.events");
    assert_eq!(report["pipeline_id"], "pipeline-run");
    assert_eq!(report["target"], "events");
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["destination"]["destination_id"], "duckdb");
    assert!(
        report["destination"]["database_path"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/dev.duckdb")
    );
    assert_eq!(report["package_id"], "pkg-run-success");
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(report["checkpoint_id"], "checkpoint-run-success");
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["checkpoint"]["committed"], true);
    assert_eq!(report["checkpoint"]["is_head"], true);
    assert_eq!(report["receipt"]["destination_id"], "duckdb");
    assert_eq!(report["receipt"]["target"], "events");
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(report["receipt_source"]["kind"], "duck_db_commit");
    assert_eq!(report["receipt_source"]["duplicate"], false);
    assert_eq!(report["receipt_source"]["no_op"], false);
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["segment_count"], 1);
    assert_eq!(report["ledger_events"]["event_count"], 10);
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    assert_eq!(report["ledger_events"]["events"][0]["kind"], "run_started");
    assert_eq!(
        report["ledger_events"]["events"][9]["kind"],
        "run_succeeded"
    );
    assert_eq!(report["writes"]["package"], true);
    assert_eq!(report["writes"]["destination"], true);
    assert_eq!(report["writes"]["checkpoint"], true);

    let package_dir = project.root.join(".cdf/packages/pkg-run-success");
    let manifest = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .clone();
    assert_eq!(manifest.lifecycle.status, PackageStatus::Checkpointed);
    assert_eq!(report["package_hash"], manifest.package_hash);

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);

    let destination = DuckDbDestination::new(project.root.join(".cdf/dev.duckdb")).unwrap();
    let mirrors = destination.read_mirror_snapshot_read_only().unwrap();
    assert!(mirrors.loads_table_present);
    assert!(mirrors.state_table_present);
    assert_eq!(mirrors.loads.len(), 1);
    assert_eq!(mirrors.state.len(), 1);
    assert_eq!(mirrors.loads[0].package_hash, manifest.package_hash);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed run head");
    assert_eq!(head.delta.checkpoint_id.as_str(), "checkpoint-run-success");
    assert_eq!(head.delta.package_hash.as_str(), manifest.package_hash);
    assert!(head.delta.schema_hash.as_str().starts_with("sha256:"));
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
    assert_eq!(head.delta.segments.len(), 1);
    assert!(matches!(
        head.delta.output_position,
        SourcePosition::FileManifest(_)
    ));
}

#[test]
fn run_human_output_mentions_receipt_verified_commit_gate() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "run",
        "--resource",
        "local.events",
        "--pipeline",
        "pipeline-run",
        "--target",
        "events",
        "--package-id",
        "pkg-run-human",
        "--checkpoint-id",
        "checkpoint-run-human",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(result.stdout.contains("resource local.events"));
    assert!(result.stdout.contains("target events"));
    assert!(result.stdout.contains("checkpoint checkpoint-run-human"));
    assert!(result.stdout.contains("receipt verification"));
    assert!(result.stdout.contains("commit gate"));
}

#[test]
fn run_missing_explicit_inputs_fails_before_writes() {
    for (name, args) in [
        (
            "resource",
            vec![
                "cdf",
                "--json",
                "run",
                "--pipeline",
                "pipeline-run",
                "--target",
                "events",
                "--package-id",
                "pkg-run-missing",
                "--checkpoint-id",
                "checkpoint-run-missing",
            ],
        ),
        (
            "pipeline",
            vec![
                "cdf",
                "--json",
                "run",
                "--resource",
                "local.events",
                "--target",
                "events",
                "--package-id",
                "pkg-run-missing",
                "--checkpoint-id",
                "checkpoint-run-missing",
            ],
        ),
        (
            "target",
            vec![
                "cdf",
                "--json",
                "run",
                "--resource",
                "local.events",
                "--pipeline",
                "pipeline-run",
                "--package-id",
                "pkg-run-missing",
                "--checkpoint-id",
                "checkpoint-run-missing",
            ],
        ),
        (
            "package",
            vec![
                "cdf",
                "--json",
                "run",
                "--resource",
                "local.events",
                "--pipeline",
                "pipeline-run",
                "--target",
                "events",
                "--checkpoint-id",
                "checkpoint-run-missing",
            ],
        ),
        (
            "checkpoint",
            vec![
                "cdf",
                "--json",
                "run",
                "--resource",
                "local.events",
                "--pipeline",
                "pipeline-run",
                "--target",
                "events",
                "--package-id",
                "pkg-run-missing",
            ],
        ),
    ] {
        let project = TestProject::new();
        let mut command = vec![
            "cdf".to_owned(),
            "--json".to_owned(),
            "--project".to_owned(),
            project.root_str().to_owned(),
        ];
        command.extend(args.into_iter().skip(2).map(str::to_owned));

        let result = run_dynamic(command);

        assert_eq!(result.exit_code, 2, "case {name}: {}", result.stderr);
        assert_no_run_writes(&project, "pkg-run-missing");
        let json = stderr_or_stdout_json(&result.stderr);
        assert_eq!(json["error"]["kind"], "contract");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("run requires")
        );
    }
}

#[test]
fn run_path_package_id_fails_before_writes() {
    let project = TestProject::new();

    let result = run_valid_run_args(&project, "../pkg-run-escape", "checkpoint-run-escape");

    assert_eq!(result.exit_code, 3);
    assert!(!project.root.join(".cdf/pkg-run-escape").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("one path component")
    );
}

#[test]
fn run_postgres_destination_fails_closed_until_policy_config_is_ratified() {
    let project = TestProject::new();
    write_project_destination(&project, "postgres://secret://env/WAREHOUSE");

    let result = run_valid_run_args(&project, "pkg-run-postgres", "checkpoint-run-postgres");

    assert_eq!(result.exit_code, 78);
    assert_no_run_writes(&project, "pkg-run-postgres");
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("existing-table policy and merge dedup policy")
    );
}

#[test]
fn run_rest_resource_fails_before_package_or_destination_writes() {
    let project = TestProject::new();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        Some("secret://env/CDF_CLI_TOKEN"),
        None,
    );

    let result =
        run_valid_run_resource(&project, "api.items", "pkg-run-rest", "checkpoint-run-rest");

    assert_eq!(result.exit_code, 78);
    assert_no_run_writes(&project, "pkg-run-rest");
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("HttpTransport")
    );
}

#[test]
fn run_sql_resource_missing_secret_fails_before_package_or_destination_writes() {
    let project = TestProject::new();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://env/CDF_CLI_SQL"),
    );

    let result = run_valid_run_resource(
        &project,
        "warehouse.orders",
        "pkg-run-sql",
        "checkpoint-run-sql",
    );

    assert_eq!(result.exit_code, 4);
    assert_no_run_writes(&project, "pkg-run-sql");
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], false);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("secret://env/CDF_CLI_SQL")
    );
}

#[test]
fn run_sql_resource_resolves_secret_without_leaking_before_cursor_blocker() {
    let project = TestProject::new();
    fs::write(
        project.root.join("sql-dsn"),
        "postgres://user:sql-secret@localhost/db\n",
    )
    .unwrap();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/sql-dsn"),
    );

    let result = run_valid_run_resource(
        &project,
        "warehouse.orders",
        "pkg-run-sql-resolved",
        "checkpoint-run-sql-resolved",
    );

    assert_eq!(result.exit_code, 3);
    assert_no_run_writes(&project, "pkg-run-sql-resolved");
    assert_secret_absent(&result, "sql-secret");
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("declare an ordered cursor")
    );
}

#[test]
fn run_parquet_destination_writes_filesystem_root() {
    let project = TestProject::new();
    write_project_destination(&project, "parquet://.cdf/parquet");

    let result = run_valid_run_args(&project, "pkg-run-parquet", "checkpoint-run-parquet");

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["destination"]["kind"], "parquet");
    assert_eq!(
        report["destination"]["destination_id"],
        "parquet_object_store"
    );
    assert!(
        report["destination"]["root"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/parquet")
    );
    assert_eq!(report["target"], "events");
    assert_eq!(report["receipt"]["destination_id"], "parquet_object_store");
    assert_eq!(report["receipt"]["target"], "events");
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(report["receipt_source"]["kind"], "destination_commit");
    assert_eq!(report["receipt_source"]["duplicate"], false);
    assert_eq!(report["receipt_source"]["no_op"], false);
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    assert!(project.root.join(".cdf/parquet").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed Parquet run head");
    assert_eq!(head.delta.checkpoint_id.as_str(), "checkpoint-run-parquet");
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
}

#[test]
fn run_parquet_malformed_uri_fails_before_writes() {
    for uri in ["parquet://", "parquet://s3://bucket"] {
        let project = TestProject::new();
        write_project_destination(&project, uri);

        let result = run_valid_run_args(&project, "pkg-run-parquet-bad", "checkpoint-run-bad");

        assert_eq!(result.exit_code, 78, "uri {uri}: {}", result.stderr);
        assert_no_run_writes(&project, "pkg-run-parquet-bad");
        let json = stderr_or_stdout_json(&result.stderr);
        assert_eq!(json["error"]["not_supported"], true);
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("malformed or non-local")
        );
    }
}

#[test]
fn run_postgres_destination_secret_is_not_resolved_before_policy_config_blocker() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "postgres://user:destination-secret@localhost/db\n",
    )
    .unwrap();
    write_project_destination(&project, "postgres://secret://file/destination-dsn");

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "run".to_owned(),
        "--resource".to_owned(),
        "local.events".to_owned(),
        "--pipeline".to_owned(),
        "pipeline-run".to_owned(),
        "--target".to_owned(),
        "events".to_owned(),
        "--package-id".to_owned(),
        "pkg-run-postgres-redacted".to_owned(),
        "--checkpoint-id".to_owned(),
        "checkpoint-run-postgres-redacted".to_owned(),
    ]);

    assert_eq!(result.exit_code, 78);
    assert_no_run_writes(&project, "pkg-run-postgres-redacted");
    assert_secret_absent(&result, "destination-secret");
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Postgres destination execution requires explicit")
    );
}

#[test]
fn run_existing_package_directory_is_refused_before_destination_or_checkpoint_writes() {
    let project = TestProject::new();
    fs::create_dir_all(project.root.join(".cdf/packages/pkg-run-existing")).unwrap();

    let result = run_valid_run_args(&project, "pkg-run-existing", "checkpoint-run-existing");

    assert_eq!(result.exit_code, 5);
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(
        !project
            .root
            .join(".cdf/packages/pkg-run-existing/manifest.json")
            .exists()
    );
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("package directory already exists")
    );
}

#[test]
fn run_discovered_schema_resource_fails_before_writes() {
    let project = TestProject::new();
    write_discovered_schema_resource(&project);

    let result = run_valid_run_args(&project, "pkg-run-discovered", "checkpoint-run-discovered");

    assert_eq!(result.exit_code, 3);
    assert_no_run_writes(&project, "pkg-run-discovered");
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("declared schema")
    );
}

#[test]
fn run_loop_remains_unsupported_without_writes() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "--loop",
    ]);

    assert_eq!(result.exit_code, 78);
    assert_no_run_writes(&project, "pkg-run-loop");
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(json["error"]["message"].as_str().unwrap().contains("loop"));
}

#[test]
fn replay_package_missing_to_rejects_before_writes() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "replay",
        "package",
        "missing-package",
    ]);

    assert_eq!(result.exit_code, 2);
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("requires --to")
    );
}

#[test]
fn replay_package_missing_package_rejects_before_duckdb_parent_creation() {
    let project = TestProject::new();
    let package_dir = project.root.join(".cdf/packages/missing-package");
    let destination_parent = project.root.join(".cdf/new-replay-parent");
    let result = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/new-replay-parent/replay.duckdb",
    );

    assert_ne!(result.exit_code, 0);
    assert!(
        !destination_parent.exists(),
        "missing package replay must not create destination parent"
    );
    assert!(
        !project.root.join(".cdf/state.db").exists(),
        "missing package replay must not create checkpoint state"
    );
}

#[test]
fn replay_package_duckdb_replays_from_artifacts_without_source_contact() {
    let project = TestProject::new();
    let package_dir =
        create_replay_package_fixture(&project, "pkg-replay-duckdb", "checkpoint-replay-duckdb");
    let manifest = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .clone();

    let result = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-success.duckdb",
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "replay package");
    assert_eq!(report["command"], "replay package");
    assert!(!report["run_id"].as_str().unwrap().is_empty());
    assert_eq!(report["package_id"], "pkg-replay-duckdb");
    assert_eq!(report["package_hash"], manifest.package_hash);
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["destination"]["destination_id"], "duckdb");
    assert!(
        report["destination"]["database_path"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/replay-success.duckdb")
    );
    assert_eq!(report["target"], "events");
    assert_eq!(report["receipt"]["destination_id"], "duckdb");
    assert_eq!(report["receipt"]["target"], "events");
    assert_eq!(report["receipt"]["package_hash"], manifest.package_hash);
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert!(!report["receipt_id"].as_str().unwrap().is_empty());
    assert_eq!(report["checkpoint_id"], "checkpoint-replay-duckdb");
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["checkpoint"]["committed"], true);
    assert_eq!(report["checkpoint"]["is_head"], true);
    assert_eq!(report["receipt_source"]["kind"], "duck_db_commit");
    assert_eq!(report["receipt_source"]["duplicate"], false);
    assert_eq!(report["receipt_source"]["no_op"], false);
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(report["ledger_events"]["event_count"], 1);
    assert_eq!(report["ledger_events"]["terminal_kind"], "replay_recorded");
    assert_eq!(report["ledger_events"]["kinds"]["replay_recorded"], 1);
    assert_eq!(report["writes"]["package"], true);
    assert_eq!(report["writes"]["destination"], true);
    assert_eq!(report["writes"]["checkpoint"], true);

    let conn = DuckConnection::open(project.root.join(".cdf/replay-success.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("replay checkpoint head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        "checkpoint-replay-duckdb"
    );
    assert_eq!(head.delta.package_hash.as_str(), manifest.package_hash);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
}

#[test]
fn replay_package_duckdb_duplicate_reports_no_op() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-replay-duplicate",
        "checkpoint-replay-duplicate",
    );
    let first = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-duplicate.duckdb",
    );
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);

    remove_state_store(&project);
    let second = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-duplicate.duckdb",
    );

    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    let json = stderr_or_stdout_json(&second.stdout);
    let report = &json["result"];
    assert_eq!(report["receipt_source"]["kind"], "duck_db_commit");
    assert_eq!(report["receipt_source"]["duplicate"], true);
    assert_eq!(report["receipt_source"]["no_op"], true);
    assert_eq!(report["package_status"], "checkpointed");

    let conn = DuckConnection::open(project.root.join(".cdf/replay-duplicate.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);
    let destination =
        DuckDbDestination::new(project.root.join(".cdf/replay-duplicate.duckdb")).unwrap();
    let mirrors = destination.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(mirrors.loads.len(), 1);
    assert_eq!(mirrors.state.len(), 1);
}

#[test]
fn replay_package_postgres_destination_fails_closed_before_mutation() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "postgres://user:destination-secret@localhost/db\n",
    )
    .unwrap();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-replay-postgres",
        "checkpoint-replay-postgres",
    );
    let receipts = package_receipt_count(&package_dir);
    let status = package_status(&package_dir);

    let result = replay_package_command(
        &project,
        &package_dir,
        "postgres://secret://file/destination-dsn",
    );

    assert_eq!(result.exit_code, 78);
    assert_secret_absent(&result, "destination-secret");
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("target, merge dedup policy")
    );
}

#[test]
fn replay_package_parquet_replays_from_artifacts_without_source_contact() {
    let project = TestProject::new();
    let package_dir =
        create_replay_package_fixture(&project, "pkg-replay-parquet", "checkpoint-replay-parquet");
    let manifest = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .clone();
    let receipts_before = package_receipt_count(&package_dir);
    let parquet_root = project.root.join(".cdf/replay-parquet");

    let result = replay_package_command(&project, &package_dir, "parquet://.cdf/replay-parquet");

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "replay package");
    assert_eq!(report["command"], "replay package");
    assert_eq!(report["package_id"], "pkg-replay-parquet");
    assert_eq!(report["package_hash"], manifest.package_hash);
    assert_eq!(report["destination"]["kind"], "parquet");
    assert_eq!(
        report["destination"]["destination_id"],
        "parquet_object_store"
    );
    assert!(
        report["destination"]["root"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/replay-parquet")
    );
    assert_eq!(report["target"], "events");
    assert_eq!(report["receipt"]["destination_id"], "parquet_object_store");
    assert_eq!(report["receipt"]["target"], "events");
    assert_eq!(report["receipt"]["package_hash"], manifest.package_hash);
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(report["receipt_source"]["kind"], "destination_commit");
    assert_eq!(report["receipt_source"]["duplicate"], false);
    assert_eq!(report["receipt_source"]["no_op"], false);
    assert_eq!(report["checkpoint_id"], "checkpoint-replay-parquet");
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(report["ledger_events"]["event_count"], 1);
    assert_eq!(report["ledger_events"]["terminal_kind"], "replay_recorded");
    assert_eq!(report["ledger_events"]["kinds"]["replay_recorded"], 1);
    assert!(parquet_root.exists());
    assert_eq!(package_receipt_count(&package_dir), receipts_before + 1);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("replay checkpoint head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        "checkpoint-replay-parquet"
    );
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
}

#[test]
fn replay_package_parquet_malformed_uri_fails_before_mutation() {
    for uri in ["parquet://", "parquet://s3://bucket"] {
        let project = TestProject::new();
        let package_dir = create_replay_package_fixture(
            &project,
            "pkg-replay-parquet-bad",
            "checkpoint-replay-parquet-bad",
        );
        let receipts = package_receipt_count(&package_dir);
        let status = package_status(&package_dir);

        let result = replay_package_command(&project, &package_dir, uri);

        assert_eq!(result.exit_code, 78, "uri {uri}: {}", result.stderr);
        assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
        let json = stderr_or_stdout_json(&result.stderr);
        assert_eq!(json["error"]["not_supported"], true);
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("malformed or non-local")
        );
    }
}

#[test]
fn replay_package_unknown_destination_scheme_fails_closed_before_mutation() {
    let project = TestProject::new();
    let package_dir =
        create_replay_package_fixture(&project, "pkg-replay-s3", "checkpoint-replay-s3");
    let receipts = package_receipt_count(&package_dir);
    let status = package_status(&package_dir);

    let result = replay_package_command(&project, &package_dir, "s3://bucket/replay");

    assert_eq!(result.exit_code, 78);
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("only local duckdb://path")
    );
}

#[test]
fn status_ignores_non_serving_freshness_resources() {
    let project = TestProject::new();
    write_status_resource(&project, "governed", "1h");
    let state_path = project.root.join(".cdf/state.db");
    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        !state_path.exists(),
        "status must not create state DB when nothing is evaluable"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "status");
    assert_eq!(json["result"]["summary"]["total"], 0);
    assert!(
        json["result"]["freshness_resources"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let human = run(["cdf", "--project", project.root_str(), "status"]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert_eq!(human.stdout, "no freshness SLO resources to evaluate\n");
}

#[test]
fn status_reports_fresh_committed_head() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-fresh",
        "package-status-fresh",
        "receipt-status-fresh",
        now_ms_for_test(),
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["fresh"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["resource_id"], "local.events");
    assert_eq!(resource["trust_level"], "serving");
    assert_eq!(resource["state_scope"], json!({ "kind": "resource" }));
    assert_eq!(resource["max_age_ms"], 3_600_000);
    assert_eq!(resource["freshness_state"], "fresh");
    assert_eq!(
        resource["checkpoint"]["checkpoint_id"],
        "checkpoint-status-fresh"
    );
    assert_eq!(resource["checkpoint"]["pipeline_id"], "pipeline-1");
    assert!(resource["age_ms"].as_u64().unwrap() <= 3_600_000);
    let human = run(["cdf", "--project", project.root_str(), "status"]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert_eq!(human.stdout, "freshness SLO status fresh: 1 resource(s)\n");
}

#[test]
fn status_reports_stale_committed_head() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1ms");
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-stale",
        "package-status-stale",
        "receipt-status-stale",
        1,
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["stale"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "stale");
    assert!(resource["age_ms"].as_u64().unwrap() > 1);
    let human = run(["cdf", "--project", project.root_str(), "status"]);
    assert_eq!(human.exit_code, 1, "stderr: {}", human.stderr);
    assert_eq!(
        human.stdout,
        "freshness SLO breach: 1 stale, 0 fresh, 0 non-evaluable\n"
    );
}

#[test]
fn status_clamps_future_committed_head_age_to_zero() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1ms");
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-future",
        "package-status-future",
        "receipt-status-future",
        now_ms_for_test() + 3_600_000,
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "fresh");
    assert_eq!(resource["age_ms"], 0);
}

#[test]
fn status_reports_elapsed_age_from_committed_timestamp() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-age",
        "package-status-age",
        "receipt-status-age",
        now_ms_for_test() - 120_000,
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let age_ms = json["result"]["freshness_resources"][0]["age_ms"]
        .as_u64()
        .unwrap();
    assert!(
        (120_000..180_000).contains(&age_ms),
        "unexpected age_ms: {age_ms}"
    );
}

#[test]
fn status_reports_missing_state_as_non_evaluable() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    let state_path = project.root.join(".cdf/state.db");
    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 78, "stderr: {}", result.stderr);
    assert!(
        !state_path.exists(),
        "status must not create missing state DB"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["non_evaluable"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "non_evaluable");
    assert_eq!(resource["non_evaluable_reason"], "state_database_missing");
    let human = run(["cdf", "--project", project.root_str(), "status"]);
    assert_eq!(human.exit_code, 78, "stderr: {}", human.stderr);
    assert_eq!(
        human.stdout,
        "freshness SLO status non-evaluable: 1 resource(s), 0 fresh\n"
    );
}

#[test]
fn status_reports_missing_checkpoint_table_as_non_evaluable() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    fs::create_dir_all(project.root.join(".cdf")).unwrap();
    Connection::open(project.root.join(".cdf/state.db")).unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 78, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["non_evaluable"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "non_evaluable");
    assert_eq!(resource["non_evaluable_reason"], "checkpoint_table_missing");
}

#[test]
fn status_reports_ambiguous_multiple_pipeline_heads_as_non_evaluable() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    let committed_at_ms = now_ms_for_test();
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-ambiguous-1",
        "package-status-ambiguous-1",
        "receipt-status-ambiguous-1",
        committed_at_ms,
    );
    commit_status_head(
        &project,
        "pipeline-2",
        "checkpoint-status-ambiguous-2",
        "package-status-ambiguous-2",
        "receipt-status-ambiguous-2",
        committed_at_ms,
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 78, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["non_evaluable"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "non_evaluable");
    assert_eq!(
        resource["non_evaluable_reason"],
        "ambiguous_committed_heads"
    );
    assert_eq!(resource["matching_committed_heads"], 2);
}

#[test]
fn sql_mounts_checkpoint_package_and_receipt_tables_as_json_rows() {
    let project = TestProject::new();
    let fixture = create_system_sql_fixture(&project);
    let result = run([
        "cdf",
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
        "cdf",
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
    let state_path = project.root.join(".cdf/state.db");
    let package_root = project.root.join(".cdf/packages");
    let result = run([
        "cdf",
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
    let state_path = project.root.join(".cdf/state.db");
    let package_root = project.root.join(".cdf/packages");
    let result = run([
        "cdf",
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
fn doctor_skips_duckdb_drift_without_creating_missing_databases() {
    let project = TestProject::new();
    let state_path = project.root.join(".cdf/state.db");
    let duckdb_path = project.root.join(".cdf/dev.duckdb");
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!state_path.exists(), "doctor must not create state DB");
    assert!(
        !duckdb_path.exists(),
        "doctor drift probe must not create DuckDB DB"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    let project_file = named_check(&json, "project_file");
    assert_eq!(project_file["details"]["project_root"], project.root_str());
    assert_eq!(project_file["details"]["selected_environment"], "dev");
    assert_eq!(project_file["details"]["compiled_resources"], 1);
    assert_eq!(project_file["details"]["lockfile_present"], false);
    let icu = named_check(&json, "duckdb_icu");
    assert_eq!(icu["status"], "skipped");
    assert_eq!(icu["details"]["database_exists"], false);
    assert_eq!(icu["details"]["probe"], "icu_sort_key");
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "skipped");
    assert!(
        drift["message"]
            .as_str()
            .unwrap()
            .contains("SQLite state database is absent")
    );
}

#[test]
fn doctor_reports_lockfile_presence_when_lock_exists() {
    let project = TestProject::new();
    write_minimal_lockfile(&project);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let project_file = named_check(&json, "project_file");
    assert_eq!(project_file["details"]["lockfile_present"], true);
}

#[test]
fn doctor_reports_resolved_secret_references_without_values() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "resolved-destination-dsn-value\n",
    )
    .unwrap();
    fs::write(
        project.root.join("auth-token"),
        "resolved-auth-token-value\n",
    )
    .unwrap();
    fs::write(project.root.join("sql-dsn"), "resolved-file-secret-value\n").unwrap();
    write_secret_project(
        &project,
        "postgres://secret://file/destination-dsn",
        Some("secret://file/auth-token"),
        Some("secret://file/sql-dsn"),
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "resolved-destination-dsn-value");
    assert_secret_absent(&result, "resolved-auth-token-value");
    assert_secret_absent(&result, "resolved-file-secret-value");
    let json = stderr_or_stdout_json(&result.stdout);
    let secrets = named_check(&json, "secrets");
    assert_eq!(secrets["status"], "passed");
    assert_eq!(secrets["details"]["count"], 3);
    let references = secrets["details"]["references"].as_array().unwrap();
    for reference in [
        "secret://file/destination-dsn".to_owned(),
        "secret://file/auth-token".to_owned(),
        "secret://file/sql-dsn".to_owned(),
    ] {
        assert!(
            references.iter().any(|value| value == &reference),
            "missing secret reference {reference}"
        );
    }
}

#[test]
fn doctor_later_secret_failure_does_not_leak_already_resolved_secrets() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-before-failure"),
        "already-resolved-destination-value\n",
    )
    .unwrap();
    fs::write(
        project.root.join("token-before-failure"),
        "already-resolved-token-value\n",
    )
    .unwrap();
    fs::write(
        project.root.join("resolved-file-secret"),
        "already-resolved-file-value\n",
    )
    .unwrap();
    write_secret_project(
        &project,
        "postgres://secret://file/destination-before-failure",
        Some("secret://file/token-before-failure"),
        Some("secret://env/CDF_CLI_MISSING_SQL_AFTER_RESOLVED"),
    );
    let project_file = project.root.join("cdf.toml");
    let project_text = fs::read_to_string(&project_file).unwrap().replace(
        "packages = \".cdf/packages\"",
        "packages = \"secret://file/resolved-file-secret\"",
    );
    fs::write(project_file, project_text).unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    assert_secret_absent(&result, "already-resolved-destination-value");
    assert_secret_absent(&result, "already-resolved-token-value");
    assert_secret_absent(&result, "already-resolved-file-value");
    let json = stderr_or_stdout_json(&result.stdout);
    let secrets = named_check(&json, "secrets");
    assert_eq!(secrets["status"], "failed");
    assert!(
        secrets["message"]
            .as_str()
            .unwrap()
            .contains("secret://env/CDF_CLI_MISSING_SQL_AFTER_RESOLVED")
    );
}

#[test]
fn doctor_fails_missing_and_unavailable_secrets_without_leaking_values() {
    for case in [
        SecretFailureCase::EnvironmentDestination,
        SecretFailureCase::File,
        SecretFailureCase::DeclarativeAuthToken,
        SecretFailureCase::DeclarativeSqlConnection,
        SecretFailureCase::UnavailableProvider,
    ] {
        let project = TestProject::new();
        write_secret_failure_project(&project, case);

        let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

        assert_eq!(result.exit_code, 1, "case {case:?}");
        assert_secret_absent(&result, "would-be-token-value");
        assert_secret_absent(&result, "would-be-file-value");
        let json = stderr_or_stdout_json(&result.stdout);
        let secrets = named_check(&json, "secrets");
        assert_eq!(secrets["status"], "failed", "case {case:?}");
        assert!(secrets.as_object().unwrap().get("details").is_none());
    }
}

#[test]
fn doctor_runs_duckdb_icu_probe_for_existing_database_with_safe_details() {
    let project = TestProject::new();
    let duckdb_path = project.root.join(".cdf/dev.duckdb");
    DuckDbDestination::new(&duckdb_path)
        .unwrap()
        .probe_icu()
        .unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert!(duckdb_path.exists(), "fixture should create the DuckDB DB");
    let json = stderr_or_stdout_json(&result.stdout);
    let icu = named_check(&json, "duckdb_icu");
    assert!(
        matches!(icu["status"].as_str(), Some("passed" | "failed")),
        "unexpected ICU status: {icu}"
    );
    assert_eq!(icu["details"]["database_exists"], true);
    assert_eq!(icu["details"]["probe"], "icu_sort_key");
    assert_eq!(
        icu["details"]["available"],
        icu["status"].as_str().unwrap() == "passed"
    );
    assert!(!icu.to_string().contains("resolved-api-token-value"));
}

#[test]
fn doctor_skips_python_without_interpreter_or_python_resources() {
    let project = TestProject::new();
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "skipped");
    assert!(python.as_object().unwrap().get("details").is_none());
}

#[test]
fn doctor_fails_python_resource_without_interpreter() {
    let project = TestProject::new();
    fs::write(project.root.join("cdf.toml"), PYTHON_RESOURCE_PROJECT).unwrap();
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("python.interpreter")
    );
    assert_eq!(python["details"]["python_resources"], 1);
}

#[test]
fn doctor_uses_fixed_python_probe_not_python_resource_code() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_probe_validating_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 12, 7, true, false),
    );
    write_python_resource_config_project(&project, "fake-python");

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "passed");
    assert_eq!(python["details"]["version"], "3.12.7");
}

#[test]
fn doctor_passes_gil_enabled_python_interpreter_with_details() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 12, 7, true, false),
    );
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "passed");
    assert_eq!(
        python["details"]["executable"],
        interpreter.canonicalize().unwrap().display().to_string()
    );
    assert_eq!(python["details"]["version"], "3.12.7");
    assert_eq!(python["details"]["implementation"], "CPython");
    assert_eq!(python["details"]["gil_enabled"], true);
    assert_eq!(python["details"]["free_threaded_build"], false);
    assert_eq!(python["details"]["can_parallelize_python"], false);
    assert_eq!(python["details"]["require_free_threaded"], false);
}

#[test]
fn doctor_passes_when_free_threaded_required_and_gil_disabled() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 13, 1, false, true),
    );
    write_python_config_project(&project, "fake-python", true);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "passed");
    assert_eq!(python["details"]["gil_enabled"], false);
    assert_eq!(python["details"]["free_threaded_build"], true);
    assert_eq!(python["details"]["can_parallelize_python"], true);
    assert_eq!(python["details"]["require_free_threaded"], true);
}

#[test]
fn doctor_fails_when_free_threaded_required_but_gil_enabled() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 12, 7, true, false),
    );
    write_python_config_project(&project, "fake-python", true);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("free-threaded")
    );
    assert_eq!(python["details"]["require_free_threaded"], true);
    assert_eq!(python["details"]["can_parallelize_python"], false);
}

#[test]
fn doctor_fails_when_free_threaded_build_still_has_gil_enabled() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 13, 1, true, true),
    );
    write_python_config_project(&project, "fake-python", true);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "failed");
    assert_eq!(python["details"]["gil_enabled"], true);
    assert_eq!(python["details"]["free_threaded_build"], true);
    assert_eq!(python["details"]["can_parallelize_python"], false);
    assert_eq!(python["details"]["require_free_threaded"], true);
}

#[test]
fn doctor_fails_missing_python_interpreter() {
    let project = TestProject::new();
    write_python_config_project(&project, "absent-python", true);
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("configured interpreter is missing")
    );
    assert!(
        python["details"]["executable"]
            .as_str()
            .unwrap()
            .ends_with("absent-python")
    );
    assert_eq!(python["details"]["require_free_threaded"], true);
}

#[cfg(unix)]
#[test]
fn doctor_fails_non_executable_python_interpreter() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    fs::write(&interpreter, "#!/bin/sh\nexit 0\n").unwrap();
    set_mode(&interpreter, 0o644);
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("not executable")
    );
}

#[test]
fn doctor_fails_unsuccessful_python_probe_without_echoing_output() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_failing_interpreter(&interpreter);
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    assert!(!result.stdout.contains("SUPER_SECRET"));
    assert!(!result.stderr.contains("SUPER_SECRET"));
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("exited unsuccessfully")
    );
}

#[test]
fn doctor_fails_invalid_python_probe_json_without_echoing_output() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(&interpreter, "not-json SUPER_SECRET");
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    assert!(!result.stdout.contains("SUPER_SECRET"));
    assert!(!result.stderr.contains("SUPER_SECRET"));
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("valid inspection JSON")
    );
}

#[test]
fn doctor_fails_probe_json_with_inconsistent_version_metadata() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json_from(FakePythonProbe {
            executable: &interpreter,
            version: "3.12.8",
            major: 3,
            minor: 12,
            micro: 7,
            gil_enabled: true,
            free_threaded_build: false,
            can_parallelize_python: false,
        }),
    );
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("inconsistent version metadata")
    );
}

#[test]
fn doctor_fails_probe_json_with_inconsistent_gil_metadata() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json_from(FakePythonProbe {
            executable: &interpreter,
            version: "3.12.7",
            major: 3,
            minor: 12,
            micro: 7,
            gil_enabled: false,
            free_threaded_build: true,
            can_parallelize_python: false,
        }),
    );
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("inconsistent GIL metadata")
    );
}

#[test]
fn doctor_fails_old_python_interpreter_version() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 11, 9, true, false),
    );
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "python");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("older than required 3.12")
    );
    assert_eq!(python["details"]["version"], "3.11.9");
}

#[test]
fn doctor_passes_clean_duckdb_ledger_mirror_drift_check() {
    let project = TestProject::new();
    create_duckdb_doctor_fixture(&project, DoctorDriftFixtureMode::Clean);
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "passed");
    assert_eq!(drift["details"]["counts"]["ledger_heads"], 1);
    assert_eq!(drift["details"]["counts"]["expected_loads"], 1);
    assert_eq!(drift["details"]["counts"]["expected_state_rows"], 1);
    assert_eq!(drift["details"]["counts"]["mirror_loads"], 1);
    assert_eq!(drift["details"]["counts"]["mirror_state_rows"], 1);
    assert_eq!(drift["details"]["examples"].as_array().unwrap().len(), 0);
}

#[test]
fn doctor_fails_on_duckdb_state_mirror_drift() {
    let project = TestProject::new();
    create_duckdb_doctor_fixture(&project, DoctorDriftFixtureMode::StatePositionDrift);
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "failed");
    assert_eq!(drift["details"]["counts"]["mismatched_state_rows"], 1);
    assert_eq!(drift["details"]["examples"][0]["kind"], "mismatched_state");
    assert_eq!(
        drift["details"]["examples"][0]["field"],
        "output_position_json"
    );
}

#[test]
fn doctor_fails_on_missing_and_extra_duckdb_mirror_rows() {
    let project = TestProject::new();
    create_duckdb_doctor_fixture(&project, DoctorDriftFixtureMode::TargetDrift);
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "failed");
    assert_eq!(drift["details"]["counts"]["missing_loads"], 1);
    assert_eq!(drift["details"]["counts"]["extra_loads"], 1);
    assert_eq!(drift["details"]["counts"]["missing_state_rows"], 1);
    assert_eq!(drift["details"]["counts"]["extra_state_rows"], 1);
}

#[test]
fn package_verify_uses_lower_package_reader() {
    let temp = TempDir::new("cdf-cli-package");
    let package_dir = temp.path().join("pkg");
    let builder = PackageBuilder::create(&package_dir, "pkg-1").unwrap();
    builder.finish_with_status(PackageStatus::Packaged).unwrap();

    let result = run([
        "cdf",
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

#[test]
fn package_archive_writes_parquet_archive_and_reports_json() {
    let temp = TempDir::new("cdf-cli-package-archive-json");
    let package_dir = build_archive_cli_package(temp.path(), "pkg-archive-cli-json");

    let result = run([
        "cdf",
        "--json",
        "package",
        "archive",
        package_dir.to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "package archive");
    assert_eq!(json["result"]["command"], "package archive");
    assert_eq!(json["result"]["format"], "parquet");
    assert_eq!(json["result"]["status"], "written");
    assert_eq!(
        json["result"]["fidelity_report_path"],
        "archive/parquet/fidelity.json"
    );
    assert_eq!(
        json["result"]["segments"][0]["archive_path"],
        "archive/parquet/data/seg-000001.parquet"
    );
    assert!(
        package_dir
            .join("archive/parquet/data/seg-000001.parquet")
            .is_file()
    );
    assert!(package_dir.join("archive/parquet/fidelity.json").is_file());
}

#[test]
fn package_archive_supports_local_json_flag_and_human_output() {
    let json_temp = TempDir::new("cdf-cli-package-archive-local-json");
    let json_package = build_archive_cli_package(json_temp.path(), "pkg-archive-cli-local-json");
    let json_result = run([
        "cdf",
        "package",
        "archive",
        json_package.to_str().unwrap(),
        "--json",
    ]);

    assert_eq!(json_result.exit_code, 0, "stderr: {}", json_result.stderr);
    let json = stderr_or_stdout_json(&json_result.stdout);
    assert_eq!(json["command"], "package archive");
    assert_eq!(json["result"]["status"], "written");

    let human_temp = TempDir::new("cdf-cli-package-archive-human");
    let human_package = build_archive_cli_package(human_temp.path(), "pkg-archive-cli-human");
    let human_result = run(["cdf", "package", "archive", human_package.to_str().unwrap()]);

    assert_eq!(human_result.exit_code, 0, "stderr: {}", human_result.stderr);
    assert!(human_result.stdout.contains("archived package sha256:"));
    assert!(human_result.stdout.contains("status written"));
    assert!(human_result.stdout.contains("1 segment(s)"));
    assert!(
        human_result
            .stdout
            .contains("fidelity archive/parquet/fidelity.json")
    );
}

#[test]
fn package_archive_rejects_unsupported_format_before_writes() {
    let temp = TempDir::new("cdf-cli-package-archive-format");
    let package_dir = build_archive_cli_package(temp.path(), "pkg-archive-cli-format");

    let result = run([
        "cdf",
        "--json",
        "package",
        "archive",
        package_dir.to_str().unwrap(),
        "--format",
        "orc",
    ]);

    assert_eq!(result.exit_code, 2);
    assert!(!package_dir.join("archive").exists());
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unsupported package archive format `orc`")
    );
}

struct SystemSqlFixture {
    package_hash: String,
}

#[derive(Clone, Copy)]
enum DoctorDriftFixtureMode {
    Clean,
    StatePositionDrift,
    TargetDrift,
}

#[test]
fn state_show_uses_sqlite_store_and_reports_missing_head() {
    let project = TestProject::new();
    let result = run([
        "cdf",
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
    let result = run(["cdf", "--json", "bogus"]);

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
        let temp = TempDir::new("cdf-cli-project");
        let root = temp.path().to_path_buf();
        fs::create_dir_all(root.join("resources")).unwrap();
        fs::create_dir_all(root.join("data")).unwrap();
        fs::create_dir_all(root.join(".cdf")).unwrap();
        fs::write(root.join("cdf.toml"), PROJECT).unwrap();
        fs::write(root.join("resources/files.toml"), RESOURCE).unwrap();
        fs::write(
            root.join("data/events.ndjson"),
            concat!(
                "{\"id\":1,\"updated_at\":\"2026-07-06T00:00:00Z\"}\n",
                "{\"id\":2,\"updated_at\":\"2026-07-06T00:01:00Z\"}\n"
            ),
        )
        .unwrap();
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

fn assert_no_preview_writes(project: &TestProject) {
    assert!(
        !project.root.join(".cdf/packages").exists(),
        "preview must not create package root"
    );
    assert!(
        !project.root.join(".cdf/state.db").exists(),
        "preview must not create checkpoint state"
    );
    assert!(
        !project.root.join(".cdf/dev.duckdb").exists(),
        "preview must not create destination DB"
    );
}

fn assert_no_run_writes(project: &TestProject, package_id: &str) {
    assert!(
        !project.root.join(".cdf/packages").join(package_id).exists(),
        "rejected run must not create package directory {package_id}"
    );
    assert!(
        !project.root.join(".cdf/state.db").exists(),
        "rejected run must not create checkpoint state"
    );
    assert!(
        !project.root.join(".cdf/dev.duckdb").exists(),
        "rejected run must not create destination DB"
    );
}

fn run_valid_run_args(
    project: &TestProject,
    package_id: &str,
    checkpoint_id: &str,
) -> crate::InvocationResult {
    run_valid_run_resource(project, "local.events", package_id, checkpoint_id)
}

fn run_valid_run_resource(
    project: &TestProject,
    resource_id: &str,
    package_id: &str,
    checkpoint_id: &str,
) -> crate::InvocationResult {
    run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "run".to_owned(),
        "--resource".to_owned(),
        resource_id.to_owned(),
        "--pipeline".to_owned(),
        "pipeline-run".to_owned(),
        "--target".to_owned(),
        "events".to_owned(),
        "--package-id".to_owned(),
        package_id.to_owned(),
        "--checkpoint-id".to_owned(),
        checkpoint_id.to_owned(),
    ])
}

fn create_replay_package_fixture(
    project: &TestProject,
    package_id: &str,
    checkpoint_id: &str,
) -> PathBuf {
    let result = run_valid_run_args(project, package_id, checkpoint_id);
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    remove_state_store(project);
    project.root.join(".cdf/packages").join(package_id)
}

fn replay_package_command(
    project: &TestProject,
    package_dir: &Path,
    destination_uri: &str,
) -> crate::InvocationResult {
    run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "replay".to_owned(),
        "package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        destination_uri.to_owned(),
    ])
}

fn remove_state_store(project: &TestProject) {
    for suffix in ["", "-wal", "-shm"] {
        let path = project.root.join(format!(".cdf/state.db{suffix}"));
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("remove {}: {error}", path.display()),
        }
    }
}

fn package_receipt_count(package_dir: &Path) -> usize {
    PackageReader::open(package_dir)
        .unwrap()
        .receipts()
        .unwrap()
        .len()
}

fn package_status(package_dir: &Path) -> PackageStatus {
    PackageReader::open(package_dir)
        .unwrap()
        .manifest()
        .lifecycle
        .status
        .clone()
}

fn assert_no_replay_mutation(
    project: &TestProject,
    package_dir: &Path,
    receipt_count: usize,
    status: PackageStatus,
    local_destination_path: Option<&Path>,
) {
    assert!(
        !project.root.join(".cdf/state.db").exists(),
        "rejected replay must not create checkpoint state"
    );
    assert_eq!(package_receipt_count(package_dir), receipt_count);
    assert_eq!(package_status(package_dir), status);
    if let Some(path) = local_destination_path {
        assert!(
            !path.exists(),
            "rejected replay must not create {}",
            path.display()
        );
    }
}

fn write_project_destination(project: &TestProject, destination: &str) {
    fs::write(
        project.root.join("cdf.toml"),
        PROJECT.replace(
            "destination = \"duckdb://.cdf/dev.duckdb\"",
            &format!("destination = \"{destination}\""),
        ),
    )
    .unwrap();
}

fn write_discovered_schema_resource(project: &TestProject) {
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
"#,
    )
    .unwrap();
}

fn write_resource_glob(project: &TestProject, glob: &str) {
    fs::write(
        project.root.join("resources/files.toml"),
        RESOURCE.replace("glob = \"*.ndjson\"", &format!("glob = \"{glob}\"")),
    )
    .unwrap();
}

fn write_format_fixture(project: &TestProject, format: &str) {
    for entry in fs::read_dir(project.root.join("data")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }

    let glob = format!("events.{format}");
    let resource = RESOURCE
        .replace("glob = \"*.ndjson\"", &format!("glob = \"{glob}\""))
        .replace("format = \"ndjson\"", &format!("format = \"{format}\""));
    fs::write(project.root.join("resources/files.toml"), resource).unwrap();

    match format {
        "csv" => fs::write(
            project.root.join("data/events.csv"),
            "id,updated_at\n1,2026-07-06T00:00:00Z\n2,2026-07-06T00:01:00Z\n",
        )
        .unwrap(),
        "json" => fs::write(
            project.root.join("data/events.json"),
            r#"[{"id":1,"updated_at":"2026-07-06T00:00:00Z"},{"id":2,"updated_at":"2026-07-06T00:01:00Z"}]"#,
        )
        .unwrap(),
        "parquet" => write_parquet_preview_fixture(project),
        other => panic!("unsupported format fixture {other}"),
    }
}

fn write_parquet_preview_fixture(project: &TestProject) {
    let temp = TempDir::new("cdf-cli-preview-parquet-source");
    let package_dir = build_archive_cli_package(temp.path(), "pkg-preview-parquet-source");
    cdf_package::persist_package_parquet_archive(&package_dir, false).unwrap();
    fs::copy(
        package_dir.join("archive/parquet/data/seg-000001.parquet"),
        project.root.join("data/events.parquet"),
    )
    .unwrap();
}

fn write_status_resource(project: &TestProject, trust: &str, max_age: &str) {
    let status_resource = RESOURCE.replace(
        "trust = \"governed\"",
        &format!("trust = \"{trust}\"\nfreshness = {{ max_age = \"{max_age}\" }}"),
    );
    fs::write(project.root.join("resources/files.toml"), status_resource).unwrap();
}

fn commit_status_head(
    project: &TestProject,
    pipeline_id: &str,
    checkpoint_id: &str,
    package_hash: &str,
    receipt_id: &str,
    committed_at_ms: i64,
) {
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let delta = status_delta(pipeline_id, checkpoint_id, package_hash);
    let checkpoint_id = delta.checkpoint_id.clone();
    store.propose(delta).unwrap();
    store
        .commit(
            &checkpoint_id,
            status_receipt(package_hash, receipt_id, committed_at_ms),
        )
        .unwrap();
}

fn status_delta(pipeline_id: &str, checkpoint_id: &str, package_hash: &str) -> StateDelta {
    let output_position = SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "updated_at".to_owned(),
        value: CursorValue::I64(42),
    });
    StateDelta {
        checkpoint_id: CheckpointId::new(checkpoint_id).unwrap(),
        pipeline_id: PipelineId::new(pipeline_id).unwrap(),
        resource_id: ResourceId::new("local.events").unwrap(),
        scope: ScopeKey::Resource,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: output_position.clone(),
        package_hash: PackageHash::new(package_hash).unwrap(),
        schema_hash: SchemaHash::new("schema-status-1").unwrap(),
        segments: vec![StateSegment {
            segment_id: SegmentId::new("seg-status-1").unwrap(),
            scope: ScopeKey::Resource,
            output_position,
            row_count: 1,
            byte_count: 8,
        }],
    }
}

fn status_receipt(package_hash: &str, receipt_id: &str, committed_at_ms: i64) -> Receipt {
    Receipt {
        receipt_id: ReceiptId::new(receipt_id).unwrap(),
        destination: DestinationId::new("local-test").unwrap(),
        target: TargetName::new("events").unwrap(),
        package_hash: PackageHash::new(package_hash).unwrap(),
        segment_acks: vec![SegmentAck {
            segment_id: SegmentId::new("seg-status-1").unwrap(),
            row_count: 1,
            byte_count: 8,
        }],
        disposition: WriteDisposition::Append,
        idempotency_token: IdempotencyToken::new(package_hash).unwrap(),
        transaction: None,
        counts: CommitCounts {
            rows_written: 1,
            rows_inserted: Some(1),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: SchemaHash::new("schema-status-1").unwrap(),
        migrations: Vec::new(),
        committed_at_ms,
        verify: VerifyClause {
            kind: "status".to_owned(),
            statement: "select 1".to_owned(),
            parameters: BTreeMap::new(),
        },
    }
}

fn now_ms_for_test() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

#[derive(Clone, Copy, Debug)]
enum SecretFailureCase {
    EnvironmentDestination,
    File,
    DeclarativeAuthToken,
    DeclarativeSqlConnection,
    UnavailableProvider,
}

fn write_secret_failure_project(project: &TestProject, case: SecretFailureCase) {
    match case {
        SecretFailureCase::EnvironmentDestination => write_secret_project(
            project,
            "postgres://secret://env/CDF_CLI_MISSING_DESTINATION_SECRET",
            None,
            None,
        ),
        SecretFailureCase::File => write_secret_project(
            project,
            "duckdb://.cdf/dev.duckdb",
            None,
            Some("secret://file/missing-sql-dsn"),
        ),
        SecretFailureCase::DeclarativeAuthToken => write_secret_project(
            project,
            "duckdb://.cdf/dev.duckdb",
            Some("secret://env/CDF_CLI_MISSING_AUTH_TOKEN"),
            None,
        ),
        SecretFailureCase::DeclarativeSqlConnection => write_secret_project(
            project,
            "duckdb://.cdf/dev.duckdb",
            None,
            Some("secret://env/CDF_CLI_MISSING_SQL_CONNECTION"),
        ),
        SecretFailureCase::UnavailableProvider => write_secret_project(
            project,
            "postgres://secret://keychain/prod-token",
            None,
            None,
        ),
    }
}

fn write_secret_project(
    project: &TestProject,
    destination: &str,
    rest_token: Option<&str>,
    sql_connection: Option<&str>,
) {
    let mut resources = String::new();
    if rest_token.is_some() {
        resources.push_str("\n[resources.\"api.*\"]\nsource = \"resources/api.toml\"\n");
    }
    if sql_connection.is_some() {
        resources.push_str("\n[resources.\"warehouse.*\"]\nsource = \"resources/sql.toml\"\n");
    }
    if rest_token.is_none() && sql_connection.is_none() {
        resources.push_str("\n[resources.\"local.*\"]\nsource = \"resources/files.toml\"\n");
    }

    fs::write(
        project.root.join("cdf.toml"),
        format!(
            r#"
[project]
name = "cli_test"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "{destination}"
{resources}
"#
        ),
    )
    .unwrap();

    if let Some(token) = rest_token {
        fs::write(
            project.root.join("resources/api.toml"),
            rest_resource(token),
        )
        .unwrap();
    }
    if let Some(connection) = sql_connection {
        fs::write(
            project.root.join("resources/sql.toml"),
            sql_resource(connection),
        )
        .unwrap();
    }
}

fn rest_resource(token: &str) -> String {
    format!(
        r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"
auth = {{ kind = "bearer", token = "{token}" }}

[resource.items]
path = "/items"
records = "$"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
] }}
"#
    )
}

fn sql_resource(connection: &str) -> String {
    format!(
        r#"
[source.warehouse]
kind = "sql"
connection = "{connection}"

[resource.orders]
table = "orders"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
] }}
"#
    )
}

fn assert_secret_absent(result: &crate::InvocationResult, secret: &str) {
    assert!(!result.stdout.contains(secret), "stdout leaked {secret}");
    assert!(!result.stderr.contains(secret), "stderr leaked {secret}");
}

fn write_minimal_lockfile(project: &TestProject) {
    fs::write(
        project.root.join("cdf.lock"),
        r#"
version = 1
normalizer = "namecase-v1"

[project]
name = "cli_test"
default_environment = "dev"

[dependency_tuple]
cdf = "0.1.0"
arrow_rs = "59.1.0"
"#,
    )
    .unwrap();
}

fn create_system_sql_fixture(project: &TestProject) -> SystemSqlFixture {
    let package_root = project.root.join(".cdf/packages");
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

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let delta = sample_sql_delta(&manifest.package_hash);
    let checkpoint_id = delta.checkpoint_id.clone();
    store.propose(delta).unwrap();
    store.commit(&checkpoint_id, receipt).unwrap();

    SystemSqlFixture {
        package_hash: manifest.package_hash,
    }
}

fn create_duckdb_doctor_fixture(project: &TestProject, mode: DoctorDriftFixtureMode) {
    let package_root = project.root.join(".cdf/packages");
    fs::create_dir_all(&package_root).unwrap();
    let package_dir = package_root.join("pkg-doctor-1");
    let mut builder = PackageBuilder::create(&package_dir, "pkg-doctor-1").unwrap();
    builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), &[sample_sql_batch()])
        .unwrap();
    let manifest = builder.finish().unwrap();
    let package_hash = PackageHash::new(manifest.package_hash.clone()).unwrap();
    let output_position = doctor_output_position(42);
    let segment = doctor_state_segment(output_position.clone());

    let destination = DuckDbDestination::new(project.root.join(".cdf/dev.duckdb")).unwrap();
    let outcome = destination
        .commit_package(DuckDbCommitRequest {
            package_dir: package_dir.clone(),
            commit: DestinationCommitRequest {
                package_hash: package_hash.clone(),
                target: TargetName::new("events").unwrap(),
                disposition: WriteDisposition::Append,
                segments: vec![segment.clone()],
                idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
            },
            schema_hash: SchemaHash::new("schema-doctor-1").unwrap(),
            merge_keys: Vec::new(),
        })
        .unwrap();

    let ledger_output_position = match mode {
        DoctorDriftFixtureMode::Clean => output_position,
        DoctorDriftFixtureMode::StatePositionDrift => doctor_output_position(43),
        DoctorDriftFixtureMode::TargetDrift => output_position,
    };
    let delta = doctor_delta(&package_hash, ledger_output_position);
    let checkpoint_id = delta.checkpoint_id.clone();
    let mut receipt = outcome.receipt;
    if matches!(mode, DoctorDriftFixtureMode::TargetDrift) {
        receipt.target = TargetName::new("other_events").unwrap();
    }
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    store.propose(delta).unwrap();
    store.commit(&checkpoint_id, receipt).unwrap();
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

fn doctor_output_position(value: i64) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "id".to_owned(),
        value: CursorValue::I64(value),
    })
}

fn doctor_state_segment(output_position: SourcePosition) -> StateSegment {
    StateSegment {
        segment_id: SegmentId::new("seg-000001").unwrap(),
        scope: ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        },
        output_position,
        row_count: 3,
        byte_count: 48,
    }
}

fn doctor_delta(package_hash: &PackageHash, output_position: SourcePosition) -> StateDelta {
    StateDelta {
        checkpoint_id: CheckpointId::new("checkpoint-doctor-1").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("local.events").unwrap(),
        scope: ScopeKey::Resource,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: output_position.clone(),
        package_hash: package_hash.clone(),
        schema_hash: SchemaHash::new("schema-doctor-1").unwrap(),
        segments: vec![doctor_state_segment(output_position)],
    }
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
            statement: "select count(*) from events where _cdf_package = ?".to_owned(),
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
        let parent = PathBuf::from("target").join("cdf-cli-tests");
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

fn run_dynamic(args: Vec<String>) -> crate::InvocationResult {
    invoke(args.into_iter().map(OsString::from))
}

fn build_archive_cli_package(root: &Path, package_id: &str) -> PathBuf {
    let package_dir = root.join(package_id);
    let mut builder = PackageBuilder::create(&package_dir, package_id).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
            Arc::new(StringArray::from(vec![Some("ada"), None])),
        ],
    )
    .unwrap();
    builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), &[batch])
        .unwrap();
    builder.finish_with_status(PackageStatus::Packaged).unwrap();
    package_dir
}

fn stderr_or_stdout_json(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

fn named_check<'a>(json: &'a Value, name: &str) -> &'a Value {
    json["result"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == name)
        .unwrap()
}

fn write_python_config_project(
    project: &TestProject,
    interpreter: &str,
    require_free_threaded: bool,
) {
    let mut text = PROJECT.to_owned();
    text.push_str("\n[python]\ninterpreter = ");
    text.push_str(&serde_json::to_string(interpreter).unwrap());
    text.push('\n');
    if require_free_threaded {
        text.push_str("require_free_threaded = true\n");
    }
    fs::write(project.root.join("cdf.toml"), text).unwrap();
}

fn write_python_resource_config_project(project: &TestProject, interpreter: &str) {
    let mut text = PYTHON_RESOURCE_PROJECT.to_owned();
    text.push_str("\n[python]\ninterpreter = ");
    text.push_str(&serde_json::to_string(interpreter).unwrap());
    text.push('\n');
    fs::write(project.root.join("cdf.toml"), text).unwrap();
}

fn write_fake_interpreter(path: &Path, stdout: &str) {
    fs::write(
        path,
        format!("#!/bin/sh\ncat <<'CDF_FAKE_PYTHON_JSON'\n{stdout}\nCDF_FAKE_PYTHON_JSON\n"),
    )
    .unwrap();
    make_executable(path);
}

fn write_probe_validating_interpreter(path: &Path, stdout: &str) {
    fs::write(
        path,
        format!(
            r#"#!/bin/sh
if [ "$#" -ne 3 ]; then exit 10; fi
if [ "$1" != "-I" ]; then exit 11; fi
if [ "$2" != "-c" ]; then exit 12; fi

case "$3" in
  *"sysconfig.get_config_var"*) ;;
  *) exit 13 ;;
esac

case "$3" in
  *"_is_gil_enabled"*) ;;
  *) exit 14 ;;
esac

case "$3" in
  *"src/events.py"*|*"raw_events"*|*"python://"*) exit 15 ;;
esac

cat <<'CDF_FAKE_PYTHON_JSON'
{stdout}
CDF_FAKE_PYTHON_JSON
"#
        ),
    )
    .unwrap();
    make_executable(path);
}

fn write_failing_interpreter(path: &Path) {
    fs::write(
        path,
        "#!/bin/sh\necho SUPER_SECRET_STDOUT\necho SUPER_SECRET_STDERR >&2\nexit 42\n",
    )
    .unwrap();
    make_executable(path);
}

fn python_probe_json(
    executable: &Path,
    major: u16,
    minor: u16,
    micro: u16,
    gil_enabled: bool,
    free_threaded_build: bool,
) -> String {
    let version = format!("{major}.{minor}.{micro}");
    python_probe_json_from(FakePythonProbe {
        executable,
        version: &version,
        major,
        minor,
        micro,
        gil_enabled,
        free_threaded_build,
        can_parallelize_python: free_threaded_build && !gil_enabled,
    })
}

struct FakePythonProbe<'a> {
    executable: &'a Path,
    version: &'a str,
    major: u16,
    minor: u16,
    micro: u16,
    gil_enabled: bool,
    free_threaded_build: bool,
    can_parallelize_python: bool,
}

fn python_probe_json_from(probe: FakePythonProbe<'_>) -> String {
    json!({
        "executable": probe.executable.display().to_string(),
        "version": probe.version,
        "major": probe.major,
        "minor": probe.minor,
        "micro": probe.micro,
        "implementation": "CPython",
        "gil_enabled": probe.gil_enabled,
        "free_threaded_build": probe.free_threaded_build,
        "can_parallelize_python": probe.can_parallelize_python,
    })
    .to_string()
}

fn make_executable(path: &Path) {
    #[cfg(unix)]
    set_mode(path, 0o755);
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(mode);
    fs::set_permissions(path, permissions).unwrap();
}
