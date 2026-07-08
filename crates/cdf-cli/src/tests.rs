use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    sync::Mutex,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_dest_duckdb::{DuckDbCommitRequest, DuckDbDestination};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, CheckpointId, CheckpointStatus, CheckpointStore,
    CommitCounts, CursorPosition, CursorValue, DestinationCommitRequest, DestinationId,
    IdempotencyToken, PackageHash, PartitionId, PipelineId, Receipt, ReceiptId, ResourceId, RunId,
    SchemaHash, ScopeKey, SegmentAck, SegmentId, SourcePosition, StateDelta, StateSegment,
    TargetName, VerifyClause, WriteDisposition,
};
use cdf_package::{PackageBuilder, PackageReader, PackageStatus};
use cdf_project::{
    PackageArtifactReplayRequest, ResolvedProjectDestination, replay_package_from_artifacts,
};
use cdf_state_sqlite::{
    RunEventAppend, RunEventDetails, RunEventKind, RunEventValue, SecretReference,
    SqliteCheckpointStore, SqliteRunLedger,
};
use duckdb::Connection as DuckConnection;
use postgres::{Client, NoTls};
use rusqlite::Connection;
use serde_json::Value;
use serde_json::json;

use crate::invoke;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
static LIVE_POSTGRES_SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);
static LOCAL_POSTGRES_START: Mutex<()> = Mutex::new(());

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
  { name = "updated_at", type = "int64", nullable = false },
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
        "help", "version", "init", "validate", "plan", "explain", "run", "preview", "sql",
        "inspect", "diff", "contract", "state", "resume", "replay", "backfill", "package",
        "doctor", "status",
    ] {
        assert!(result.stdout.contains(command), "missing {command}");
    }
}

#[test]
fn parser_provides_subcommand_help_at_nested_layers() {
    let plan = run(["cdf", "plan", "--help"]);

    assert_eq!(plan.exit_code, 0);
    assert!(plan.stdout.contains("Usage: cdf plan"));
    assert!(plan.stdout.contains("--resource <RESOURCE>"));
    assert!(plan.stdout.contains("--to <DEST>"));
    assert!(plan.stdout.contains("--target <TARGET>"));

    let rewind = run(["cdf", "state", "rewind", "--help"]);

    assert_eq!(rewind.exit_code, 0);
    assert!(rewind.stdout.contains("Usage: cdf state rewind"));
    assert!(rewind.stdout.contains("--scope <KEY=VALUE>"));
    assert!(rewind.stdout.contains("[aliases: --to]"));
    assert!(rewind.stdout.contains("--target-checkpoint <CHECKPOINT>"));
    assert!(rewind.stdout.contains("--marker-checkpoint <CHECKPOINT>"));
}

#[test]
fn parser_help_command_renders_requested_command_path() {
    let result = run(["cdf", "help", "state", "rewind"]);

    assert_eq!(result.exit_code, 0);
    assert!(result.stdout.contains("Usage: cdf state rewind"));
    assert!(result.stdout.contains("--scope <KEY=VALUE>"));
    assert!(result.stdout.contains("--scope-json <JSON>"));
}

#[test]
fn parser_preserves_json_anywhere_for_help_envelope() {
    let result = run(["cdf", "plan", "--help", "--json"]);

    assert_eq!(result.exit_code, 0);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "help");
    assert!(
        json["result"]["help"]
            .as_str()
            .unwrap()
            .contains("Usage: cdf plan")
    );
}

#[cfg(feature = "cli-artifacts")]
#[test]
fn cli_generated_artifacts_match_committed_snapshots() {
    crate::cli_artifacts::check_cli_artifacts(&crate::cli_artifacts::default_artifact_dir())
        .unwrap();
}

#[test]
fn renderer_migration_gate_rejects_raw_human_output_bypasses() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_files(&src, &mut files);
    let mut violations = Vec::new();
    for file in files {
        let relative = file.strip_prefix(&src).unwrap();
        if matches!(
            relative.to_str(),
            Some("commands.rs" | "output.rs" | "tests.rs")
        ) {
            continue;
        }
        let text = fs::read_to_string(&file).unwrap();
        for (pattern, reason) in [
            (
                "HumanOutput::Plain",
                "plain human output bypasses the renderer",
            ),
            (
                "CommandOutput {",
                "commands must construct output through renderer helpers",
            ),
            (
                "commands::output",
                "command modules must return RenderDocument output directly",
            ),
            (
                "commands::report_output",
                "command modules must return RenderDocument output directly",
            ),
            (
                "commands::{output",
                "command modules must not import the raw output shim",
            ),
            (
                "commands::{report_output",
                "command modules must not import the raw output shim",
            ),
            (
                "report_output(",
                "command modules must not call the raw output shim",
            ),
            (
                "human_message(",
                "legacy human message helpers bypass renderer documents",
            ),
        ] {
            if text.contains(pattern) {
                violations.push(format!(
                    "{} contains `{pattern}`: {reason}",
                    relative.display()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "renderer migration gate failed:\n{}",
        violations.join("\n")
    );
}

fn collect_rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

#[test]
fn parser_preserves_global_project_env_and_json_anywhere() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "validate",
        "--project",
        project.root_str(),
        "--env",
        "dev",
        "--json",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "validate");
}

#[test]
fn inspect_human_outputs_use_renderer_for_project_inventory() {
    let project = TestProject::new();

    let project_result = run(["cdf", "--project", project.root_str(), "inspect", "project"]);
    assert_eq!(
        project_result.exit_code, 0,
        "stderr: {}",
        project_result.stderr
    );
    assert!(
        project_result
            .stdout
            .contains("OK project cli_test env dev")
    );
    assert!(project_result.stdout.contains("Project"));
    assert!(
        project_result
            .stdout
            .contains("destination  duckdb://.cdf/dev.duckdb")
    );
    assert!(project_result.stdout.contains("-> cdf inspect resources"));

    let resources = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "resources",
    ]);
    assert_eq!(resources.exit_code, 0, "stderr: {}", resources.stderr);
    assert!(resources.stdout.contains("OK 1 compiled resource(s)"));
    assert!(resources.stdout.contains("| resource     | trust"));
    assert!(resources.stdout.contains("| local.events | governed"));

    let resource = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "resource",
        "local.events",
    ]);
    assert_eq!(resource.exit_code, 0, "stderr: {}", resource.stderr);
    assert!(resource.stdout.contains("OK resource local.events"));
    assert!(resource.stdout.contains("Resource"));
    assert!(resource.stdout.contains("-> cdf plan local.events"));

    let destinations = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "destinations",
    ]);
    assert_eq!(destinations.exit_code, 0, "stderr: {}", destinations.stderr);
    assert!(
        destinations
            .stdout
            .contains("OK inspected destination capabilities")
    );
    assert!(destinations.stdout.contains("Destination"));
    assert!(
        destinations
            .stdout
            .contains("environment  duckdb://.cdf/dev.duckdb")
    );
    assert!(destinations.stdout.contains("-> cdf plan"));
}

#[test]
fn parser_accepts_no_color_anywhere_without_changing_json_envelope() {
    let result = run(["cdf", "version", "--no-color", "--json"]);

    assert_eq!(result.exit_code, 0);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "version");
    assert_eq!(json["result"]["version"], env!("CARGO_PKG_VERSION"));
}

#[test]
fn init_default_directory_creates_scaffold_and_validate_passes() {
    let temp = TempDir::new("cdf-cli-init");
    let target = temp.path().join("fresh-project");
    let target_string = target.to_str().unwrap().to_owned();

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "init".to_owned(),
        target_string.clone(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["ok"], true);
    assert_eq!(json["command"], "init");
    assert_eq!(json["result"]["project_name"], "fresh-project");
    assert_eq!(
        json["result"]["created"],
        json!([
            "cdf.toml",
            "README.md",
            "resources",
            "resources/files.toml",
            "data"
        ])
    );
    assert_eq!(json["result"]["replaced"], json!([]));
    assert_eq!(json["result"]["skipped"], json!([]));
    assert!(target.join("cdf.toml").is_file());
    assert!(target.join("README.md").is_file());
    assert!(target.join("resources/files.toml").is_file());
    assert!(target.join("data").is_dir());
    assert!(fs::read_dir(target.join("data")).unwrap().next().is_none());
    assert!(!target.join(".cdf").exists());
    assert!(!target.join("cdf.lock").exists());
    assert!(!target.join(".cdf/packages").exists());
    assert!(!target.join(".cdf/state.db").exists());
    assert!(!target.join(".cdf/dev.duckdb").exists());

    let project_text = fs::read_to_string(target.join("cdf.toml")).unwrap();
    let readme_text = fs::read_to_string(target.join("README.md")).unwrap();
    let resource_text = fs::read_to_string(target.join("resources/files.toml")).unwrap();
    assert!(project_text.contains("default_environment = \"dev\""));
    assert!(project_text.contains("[resources.\"local.*\"]"));
    assert!(readme_text.contains("docs/quickstart.md"));
    assert!(readme_text.contains("cdf validate"));
    assert!(readme_text.contains("cdf plan local.events --target local_events"));
    assert!(readme_text.contains("cdf run --resource local.events"));
    assert!(resource_text.contains("[resource.events]"));
    assert!(!project_text.contains("secret://"));
    assert!(!readme_text.contains("secret://"));
    assert!(!readme_text.contains(&target_string));
    assert!(!readme_text.contains(".cdf/"));
    assert!(!resource_text.contains("secret://"));

    let validate = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        target_string,
        "validate".to_owned(),
    ]);
    assert_eq!(validate.exit_code, 0, "stderr: {}", validate.stderr);
    let validate_json = stderr_or_stdout_json(&validate.stdout);
    assert_eq!(validate_json["result"]["declarative_resources"], 1);
}

#[test]
fn init_name_sets_project_name_and_json_fields() {
    let temp = TempDir::new("cdf-cli-init-name");
    let target = temp.path().join("named-project");
    let target_string = target.to_str().unwrap().to_owned();

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "init".to_owned(),
        target_string.clone(),
        "--name".to_owned(),
        "warehouse-core".to_owned(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["root"], target_string);
    assert_eq!(json["result"]["project_name"], "warehouse-core");
    assert_eq!(json["result"]["force"], false);
    assert_eq!(
        fs::read_to_string(target.join("cdf.toml")).unwrap(),
        concat!(
            "[project]\n",
            "name = \"warehouse-core\"\n",
            "default_environment = \"dev\"\n",
            "normalizer = \"namecase-v1\"\n",
            "\n",
            "[environments.dev]\n",
            "state = \"sqlite://.cdf/state.db\"\n",
            "packages = \".cdf/packages\"\n",
            "destination = \"duckdb://.cdf/dev.duckdb\"\n",
            "\n",
            "[resources.\"local.*\"]\n",
            "source = \"resources/files.toml\"\n",
        )
    );
}

#[test]
fn init_refuses_existing_scaffold_paths_without_force_and_preserves_contents() {
    let temp = TempDir::new("cdf-cli-init-refuse");
    let root = temp.path();
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(root.join("cdf.toml"), "keep project").unwrap();
    fs::write(root.join("README.md"), "keep readme").unwrap();
    fs::write(root.join("resources/files.toml"), "keep resource").unwrap();
    fs::write(root.join("data/events.ndjson"), "keep data").unwrap();

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "init".to_owned(),
        root.to_str().unwrap().to_owned(),
    ]);

    assert_ne!(result.exit_code, 0);
    let json = assert_json_error_code(&result, "CDF-PROJECT-CONTRACT");
    assert_eq!(json["error"]["kind"], "contract");
    let message = json["error"]["message"].as_str().unwrap();
    assert!(message.contains("cdf.toml"));
    assert!(message.contains("README.md"));
    assert!(message.contains("resources/files.toml"));
    assert!(message.contains("data"));
    assert_eq!(
        fs::read_to_string(root.join("cdf.toml")).unwrap(),
        "keep project"
    );
    assert_eq!(
        fs::read_to_string(root.join("README.md")).unwrap(),
        "keep readme"
    );
    assert_eq!(
        fs::read_to_string(root.join("resources/files.toml")).unwrap(),
        "keep resource"
    );
    assert_eq!(
        fs::read_to_string(root.join("data/events.ndjson")).unwrap(),
        "keep data"
    );
}

#[test]
fn init_force_replaces_scaffold_files_and_preserves_unrelated_runtime_paths() {
    let temp = TempDir::new("cdf-cli-init-force");
    let root = temp.path();
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::create_dir_all(root.join(".cdf/packages")).unwrap();
    fs::write(root.join("cdf.toml"), "old project").unwrap();
    fs::write(root.join("resources/files.toml"), "old resource").unwrap();
    fs::write(root.join("data/existing.ndjson"), "keep input").unwrap();
    fs::write(root.join("README.md"), "keep unrelated").unwrap();
    fs::write(root.join(".cdf/state.db"), "keep state").unwrap();
    fs::write(root.join("cdf.lock"), "keep lock").unwrap();

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "init".to_owned(),
        root.to_str().unwrap().to_owned(),
        "--name".to_owned(),
        "forced-project".to_owned(),
        "--force".to_owned(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["replaced"],
        json!(["cdf.toml", "README.md", "resources/files.toml"])
    );
    assert_eq!(json["result"]["created"], json!([]));
    assert_eq!(json["result"]["skipped"], json!(["resources", "data"]));
    assert_eq!(json["result"]["force"], true);
    assert!(
        fs::read_to_string(root.join("cdf.toml"))
            .unwrap()
            .contains("name = \"forced-project\"")
    );
    assert!(
        fs::read_to_string(root.join("resources/files.toml"))
            .unwrap()
            .contains("[resource.events]")
    );
    assert_eq!(
        fs::read_to_string(root.join("data/existing.ndjson")).unwrap(),
        "keep input"
    );
    let readme_text = fs::read_to_string(root.join("README.md")).unwrap();
    assert!(readme_text.contains("docs/quickstart.md"));
    assert!(readme_text.contains("cdf validate"));
    assert!(readme_text.contains("cdf plan local.events --target local_events"));
    assert!(readme_text.contains("cdf run --resource local.events"));
    assert!(!readme_text.contains("secret://"));
    assert!(!readme_text.contains(root.to_str().unwrap()));
    assert!(!readme_text.contains(".cdf/"));
    assert_eq!(
        fs::read_to_string(root.join(".cdf/state.db")).unwrap(),
        "keep state"
    );
    assert_eq!(
        fs::read_to_string(root.join("cdf.lock")).unwrap(),
        "keep lock"
    );
    assert!(!root.join(".cdf/dev.duckdb").exists());
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
fn contract_show_remains_project_free() {
    let result = run(["cdf", "--json", "contract", "show", "--trust", "governed"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "contract show");
    assert_eq!(json["result"]["policy"], "governed");
    assert_eq!(
        json["result"]["contract"]["schema"]["review_artifact_required"],
        true
    );
}

#[test]
fn contract_freeze_writes_lock_and_contract_test_passes() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "freeze",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(project.root.join("cdf.lock").is_file());
    assert!(
        !project.root.join(".cdf/dev.duckdb").exists(),
        "contract freeze must not create destination data"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource_ids"], json!(["local.events"]));
    assert_eq!(json["result"]["counts"]["frozen"], 1);
    let snapshot = &json["result"]["snapshots"]["local.events"];
    assert!(
        snapshot["schema_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        snapshot["policy_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        snapshot["validation_program_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );

    let test = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "test",
    ]);

    assert_eq!(test.exit_code, 0, "stderr: {}", test.stderr);
    let json = stderr_or_stdout_json(&test.stdout);
    assert_eq!(json["result"]["counts"]["passed"], 1);
    assert_eq!(json["result"]["counts"]["drifted"], 0);
    assert_eq!(json["result"]["drift_details"], json!([]));

    let diff = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "diff",
        "schema",
    ]);

    assert_eq!(diff.exit_code, 0, "stderr: {}", diff.stderr);
    let json = stderr_or_stdout_json(&diff.stdout);
    assert_eq!(json["result"]["diffs"], json!([]));
}

#[test]
fn contract_test_fails_closed_when_lock_is_missing() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "test",
    ]);

    assert_eq!(result.exit_code, 3);
    let json = assert_json_error_code(&result, "CDF-CONTRACT-LOCKFILE");
    let message = json["error"]["message"].as_str().unwrap();
    assert!(message.contains("cdf.lock"));
    assert!(message.contains("cdf contract freeze"));
}

#[test]
fn contract_test_reports_schema_and_program_drift() {
    let project = TestProject::new();
    let freeze = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "freeze",
        "local.events",
    ]);
    assert_eq!(freeze.exit_code, 0, "stderr: {}", freeze.stderr);
    write_resource_with_extra_contract_field(&project);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "test",
        "--contract",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource_ids"], json!(["local.events"]));
    assert_eq!(json["result"]["counts"]["passed"], 0);
    assert_eq!(json["result"]["counts"]["drifted"], 1);
    let fields = json["result"]["drift_details"]
        .as_array()
        .unwrap()
        .iter()
        .map(|detail| detail["field"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(fields.contains(&"schema_hash"));
    assert!(fields.contains(&"validation_program_hash"));
}

#[test]
fn contract_test_fails_closed_when_selected_snapshot_is_missing() {
    let project = TestProject::new();
    write_minimal_lockfile(&project);
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "test",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 3);
    let json = assert_json_error_code(&result, "CDF-PROJECT-CONTRACT");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("no frozen contract snapshot")
    );
}

#[test]
fn plan_json_exposes_pushdown_ddl_guarantee_and_state_advancement() {
    let project = TestProject::new();
    let package_root = project.root.join(".cdf/packages");
    let state_path = project.root.join(".cdf/state.db");
    let duckdb_path = project.root.join(".cdf/dev.duckdb");
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
    assert!(!package_root.exists(), "plan must not create package root");
    assert!(!state_path.exists(), "plan must not create state store");
    assert!(
        !duckdb_path.exists(),
        "plan must not create destination data"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    let result = &json["result"];
    assert_eq!(result["resource_id"], "local.events");
    assert!(
        result["resource_schema"]["schema_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(result["resource_schema"]["fields"][0]["name"], "id");
    assert_eq!(result["will_fetch"]["limit"], 5);
    assert_eq!(
        result["pushdown"]["unsupported"][0]["fidelity"],
        "unsupported"
    );
    assert_eq!(result["destination"]["destination_id"], "duckdb");
    assert_eq!(result["destination"]["target"], "events");
    assert_eq!(result["destination"]["disposition"], "append");
    assert_eq!(result["destination"]["idempotency"], "package_token");
    assert_eq!(result["ddl_preview"]["supported"], true);
    assert_eq!(result["ddl_preview"]["migration_support"], "supported");
    assert!(
        result["ddl_preview"]["migrations"][0]["description"]
            .as_str()
            .unwrap()
            .contains("CREATE TABLE")
    );
    assert_eq!(result["delivery_guarantee"], "effectively_once_per_package");
    assert_eq!(
        result["delivery_guarantee_detail"]["qualifier"],
        "per_package"
    );
    assert_eq!(
        result["state_advancement"]["advances_after"],
        "destination receipt is recorded and CheckpointStore::commit verifies coverage"
    );
}

#[test]
fn plan_human_headless_render_uses_operator_panels() {
    let project = TestProject::new();
    let result = run([
        "cdf",
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
        "--package-id",
        "pkg-plan-render",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        "OK plan local.events -> events",
        "Fetch",
        "Pushdown",
        "Destination",
        "Guarantee",
        "Contract",
        "Migration",
        "unsupported  1",
        "guarantee  effectively_once_per_package",
        "items      1",
        "-> cdf run local.events",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn plan_human_rich_render_uses_glyphs_color_and_operator_panels() {
    let project = TestProject::new();
    let cli = test_cli(&project);
    let output = crate::scan_command::plan_or_explain(
        &cli,
        crate::args::ScanArgs {
            resource_id: "local.events".to_owned(),
            destination_uri: None,
            target: None,
            projection: Some(vec!["id".to_owned(), "updated_at".to_owned()]),
            filters: vec!["id > 10".to_owned()],
            limit: Some(5),
            order_by: Vec::new(),
            package_id: Some("pkg-plan-rich".to_owned()),
        },
        "plan",
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "\u{1b}[32m✓\u{1b}[0m plan local.events -> events",
        "\u{1b}[36mPushdown\u{1b}[0m",
        "\u{1b}[36mDestination\u{1b}[0m",
        "\u{1b}[36mGuarantee\u{1b}[0m",
        "\u{1b}[36mContract\u{1b}[0m",
        "\u{1b}[36mMigration\u{1b}[0m",
        "\u{1b}[36m→\u{1b}[0m cdf run local.events",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn plan_human_next_command_preserves_explicit_destination_and_target() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--target",
        "custom_events",
        "--to",
        "duckdb://.cdf/plan-explicit.duckdb",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        result.stdout.contains(
            "-> cdf run local.events --target custom_events --to duckdb://.cdf/plan-explicit.duckdb"
        ),
        "stdout:\n{}",
        result.stdout
    );
    assert!(!result.stdout.contains("--package-id"));
    assert!(!result.stdout.contains("--checkpoint-id"));
}

#[test]
fn explain_json_exposes_destination_plan_without_writes() {
    let project = TestProject::new();
    let override_path = project.root.join(".cdf/explain.duckdb");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "explain",
        "local.events",
        "--to",
        "duckdb://.cdf/explain.duckdb",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert!(!override_path.exists());
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "explain");
    let report = &json["result"];
    assert_eq!(report["destination"]["target"], "events");
    assert!(
        report["destination"]["label"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/explain.duckdb")
    );
    assert_eq!(report["ddl_preview"]["supported"], true);
    assert_eq!(report["delivery_guarantee"], "effectively_once_per_package");
}

#[test]
fn explain_human_headless_render_uses_operator_panels() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "explain",
        "local.events",
        "--to",
        "duckdb://.cdf/explain-render.duckdb",
        "--package-id",
        "pkg-explain-render",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        "OK explain local.events -> events",
        "Pushdown",
        "Destination",
        "Guarantee",
        "Contract",
        "Migration",
        ".cdf/explain-render.duckdb",
        "-> cdf run local.events --to duckdb://.cdf/explain-render.duckdb",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn backfill_dry_plan_splits_sql_cursor_windows_without_writes() {
    let project = TestProject::new();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/sql-dsn"),
    );
    fs::write(
        project.root.join("resources/sql.toml"),
        sql_resource_with_ordered_cursor("secret://file/sql-dsn", "orders"),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "backfill",
        "warehouse.orders",
        "--from",
        "0",
        "--to",
        "25",
        "--target",
        "orders",
        "--slice-size",
        "10",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "backfill");
    let report = &json["result"];
    assert_eq!(report["mode"], "dry_plan");
    assert_eq!(report["resource_id"], "warehouse.orders");
    assert_eq!(report["target"], "orders");
    assert_eq!(report["requested"]["from"], "0");
    assert_eq!(report["requested"]["to"], "25");
    assert_eq!(report["requested"]["slice_size"], 10);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert_eq!(report["slices"].as_array().unwrap().len(), 3);
    assert_eq!(report["slices"][0]["start"], "0");
    assert_eq!(report["slices"][0]["end"], "10");
    assert_eq!(
        report["slices"][0]["filters"],
        json!(["updated_at >= 0", "updated_at < 10"])
    );
    assert_eq!(report["slices"][0]["scope"]["kind"], "window");
    assert_eq!(report["slices"][0]["status"], "planned");
    assert_eq!(report["slices"][0]["reason"], "dry_plan_only");
    assert!(
        report["slices"][0]["package_id"]
            .as_str()
            .unwrap()
            .starts_with("cdf-backfill-pkg-")
    );

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "backfill",
        "warehouse.orders",
        "--from",
        "0",
        "--to",
        "25",
        "--target",
        "orders",
        "--slice-size",
        "10",
    ]);

    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(!human.stdout.contains("\u{1b}["));
    for expected in [
        "OK planned backfill warehouse.orders -> orders",
        "Backfill",
        "Writes",
        "dry plan only; no package, destination, checkpoint, or run-ledger writes",
        "| slice | window | status",
        "-> cdf backfill warehouse.orders --from 0 --to 25 --target orders --execute",
    ] {
        assert!(
            human.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human.stdout
        );
    }
}

#[test]
fn backfill_human_rich_render_uses_plan_panels_and_slice_table() {
    let project = TestProject::new();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/sql-dsn"),
    );
    fs::write(
        project.root.join("resources/sql.toml"),
        sql_resource_with_ordered_cursor("secret://file/sql-dsn", "orders"),
    )
    .unwrap();

    let output = crate::backfill_command::backfill(
        &test_cli(&project),
        crate::args::BackfillArgs {
            resource_id: "warehouse.orders".to_owned(),
            from: "0".to_owned(),
            to: "20".to_owned(),
            target: Some("orders".to_owned()),
            execute: false,
            slice_size: Some(10),
        },
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "\u{1b}[32m✓\u{1b}[0m planned backfill warehouse.orders -> orders",
        "\u{1b}[36mBackfill\u{1b}[0m",
        "\u{1b}[36mWrites\u{1b}[0m",
        "dry plan only; no package, destination, checkpoint, or run-ledger writes",
        "│ slice │ window │ status",
        "\u{1b}[36m→\u{1b}[0m cdf backfill warehouse.orders --from 0 --to 20 --target orders --execute",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn backfill_rejects_resource_alias_mismatch_before_project_load() {
    let result = run([
        "cdf",
        "--json",
        "backfill",
        "local.events",
        "--resource",
        "other.events",
        "--from",
        "0",
        "--to",
        "10",
        "--target",
        "events",
    ]);

    assert_eq!(result.exit_code, 2);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("must match")
    );
}

#[test]
fn backfill_rejects_file_resource_without_runtime_writes() {
    let project = TestProject::new();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "backfill",
        "local.events",
        "--from",
        "0",
        "--to",
        "10",
    ]);

    assert_eq!(result.exit_code, 3);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("no cursor")
    );
}

#[test]
fn backfill_execute_sql_cursor_window_commits_window_scope() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("backfill_source_orders");
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"id\" BIGINT NOT NULL,
                \"updated_at\" BIGINT NOT NULL
            );
            INSERT INTO {} (\"id\", \"updated_at\") VALUES (1, 5), (2, 15), (3, 25)",
            table, table
        ))
        .unwrap();

    let project = TestProject::new();
    let source_dsn = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:source-backfill-secret@",
        1,
    );
    fs::write(project.root.join("sql-dsn"), format!("{source_dsn}\n")).unwrap();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/sql-dsn"),
    );
    fs::write(
        project.root.join("resources/sql.toml"),
        sql_resource_with_ordered_cursor("secret://file/sql-dsn", &table),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "backfill",
        "warehouse.orders",
        "--from",
        "0",
        "--to",
        "20",
        "--target",
        "orders",
        "--execute",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert_secret_absent(&result, "source-backfill-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["mode"], "execute");
    assert_eq!(report["writes"]["package"], true);
    assert_eq!(report["writes"]["destination"], true);
    assert_eq!(report["writes"]["checkpoint"], true);
    assert_eq!(report["slices"].as_array().unwrap().len(), 1);
    let slice = &report["slices"][0];
    assert_eq!(
        slice["scope"],
        json!({ "kind": "window", "start": "0", "end": "20" })
    );
    assert_eq!(slice["status"], "succeeded");
    assert_eq!(slice["executed"]["row_count"], 2);
    assert_eq!(slice["executed"]["destination"]["destination_id"], "duckdb");

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let window_scope = ScopeKey::Window {
        start: "0".to_owned(),
        end: "20".to_owned(),
    };
    let window_head = store
        .head(
            &PipelineId::new("cdf-backfill").unwrap(),
            &ResourceId::new("warehouse.orders").unwrap(),
            &window_scope,
        )
        .unwrap()
        .expect("backfill window checkpoint head");
    assert_eq!(
        window_head.delta.checkpoint_id.as_str(),
        slice["checkpoint_id"].as_str().unwrap()
    );
    assert!(
        store
            .head(
                &PipelineId::new("cdf-backfill").unwrap(),
                &ResourceId::new("warehouse.orders").unwrap(),
                &ScopeKey::Resource,
            )
            .unwrap()
            .is_none(),
        "backfill must not advance the resource-scope head"
    );
}

#[test]
fn plan_json_derives_merge_guarantee_per_key() {
    let project = TestProject::new();
    write_resource_disposition(&project, "merge");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--target",
        "events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["destination"]["disposition"], "merge");
    assert_eq!(report["delivery_guarantee"], "effectively_once_per_key");
    assert_eq!(report["delivery_guarantee_detail"]["qualifier"], "per_key");
    assert!(
        !project.root.join(".cdf/packages").exists(),
        "merge plan must not create package root"
    );
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
}

#[test]
fn plan_unsupported_destination_disposition_fails_closed_without_writes() {
    let project = TestProject::new();
    write_project_destination(&project, "parquet://.cdf/parquet");
    write_resource_disposition(&project, "merge");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--target",
        "events",
    ]);

    assert_ne!(result.exit_code, 0);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Parquet")
    );
    assert!(
        !result.stdout.contains("effectively_once"),
        "unsupported plan must not pretend a delivery guarantee"
    );
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert!(
        !project.root.join(".cdf/parquet").exists(),
        "Parquet no-write planning must not create the destination root"
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
    assert_eq!(json["result"]["resource"], "local.events");
    assert_eq!(json["result"]["partition"], "files");
    assert_eq!(json["result"]["resource_id"], "local.events");
    assert_eq!(json["result"]["partition_id"], "files");
    assert_eq!(json["result"]["row_count"], 2);
    assert!(
        json["result"]["batch"]
            .as_str()
            .unwrap()
            .starts_with("local-events-files-")
    );
    assert_eq!(json["result"]["batch"], json["result"]["batch_id"]);
    assert!(
        json["result"]["batch_id"]
            .as_str()
            .unwrap()
            .starts_with("local-events-files-")
    );
    assert!(json["result"]["byte_count"].as_u64().unwrap() > 0);
    assert_eq!(json["result"]["write_effects"]["package"], false);
    assert_eq!(json["result"]["write_effects"]["destination"], false);
    assert_eq!(json["result"]["write_effects"]["checkpoint"], false);
    assert_eq!(json["result"]["writes"]["package"], false);
    assert_eq!(json["result"]["writes"]["destination"], false);
    assert_eq!(json["result"]["writes"]["checkpoint"], false);

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(human.stdout.contains("OK previewed resource local.events"));
    assert!(human.stdout.contains("Preview"));
    assert!(human.stdout.contains("Writes"));
    assert!(human.stdout.contains("-> cdf plan local.events"));
    assert_no_preview_writes(&project);
}

#[test]
fn preview_succeeds_for_csv_json_parquet_and_arrow_ipc_file_resources() {
    for format in ["csv", "json", "parquet", "arrow_ipc"] {
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
        assert_eq!(json["result"]["resource"], "local.events");
        assert_eq!(json["result"]["partition"], "files");
        assert_eq!(json["result"]["resource_id"], "local.events");
        assert_eq!(json["result"]["row_count"], 2, "format {format}");
        assert_no_preview_writes(&project);
    }
}

#[test]
fn preview_rest_resource_uses_local_http_runtime_without_writes() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "rest-preview-token\n").unwrap();
    let (base_url, request) = serve_json_once_capturing_request(
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": 20 }
        ] }"#,
    );
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    fs::write(
        project.root.join("resources/api.toml"),
        rest_resource_with_exact_cursor_base_url(&base_url, "secret://file/rest-token"),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "api.items",
        "--filter",
        "updated_at >= 10",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "rest-preview-token");
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource"], "api.items");
    assert_eq!(json["result"]["partition"], "rest");
    assert_eq!(json["result"]["row_count"], 2);
    let request = request.lock().unwrap().clone().unwrap();
    assert!(request.starts_with("GET /items?since=10 HTTP/1.1"));
}

#[test]
fn preview_sql_table_resource_uses_postgres_runtime_without_writes() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("preview_source_orders");
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

    let project = TestProject::new();
    let source_dsn = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:source-sql-preview-secret@",
        1,
    );
    fs::write(project.root.join("sql-dsn"), format!("{source_dsn}\n")).unwrap();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/sql-dsn"),
    );
    fs::write(
        project.root.join("resources/sql.toml"),
        sql_resource_with_ordered_cursor("secret://file/sql-dsn", &table),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "warehouse.orders",
        "--filter",
        "id > 1",
        "--limit",
        "1",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert_secret_absent(&result, "source-sql-preview-secret");
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource"], "warehouse.orders");
    assert_eq!(json["result"]["partition"], "sql");
    assert_eq!(json["result"]["row_count"], 1);
}

#[test]
fn preview_sql_query_resource_fails_closed_without_writes() {
    let project = TestProject::new();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/sql-dsn"),
    );
    fs::write(
        project.root.join("resources/sql.toml"),
        r#"
[source.warehouse]
kind = "sql"
connection = "secret://file/sql-dsn"
dialect = "postgres"

[resource.orders]
query = "SELECT * FROM public.orders"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
] }
"#,
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "warehouse.orders",
    ]);

    assert_ne!(result.exit_code, 0);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("query resources are not supported")
    );
}

#[test]
fn preview_file_filter_fails_closed_without_writes() {
    let project = TestProject::new();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
        "--filter",
        "id > 1",
    ]);

    assert_eq!(result.exit_code, 3);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("cannot apply residual predicates")
    );
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
fn preview_multi_match_file_glob_reads_first_sorted_match_without_writes() {
    let project = TestProject::new();
    fs::write(
        project.root.join("data/zzz-events.ndjson"),
        "{\"id\":3,\"updated_at\":1783296120000000}\n",
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

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource"], "local.events");
    assert_eq!(json["result"]["partition"], "files");
    assert_eq!(json["result"]["row_count"], 2);
}

#[test]
fn preview_wildcard_directory_glob_requires_component_match() {
    let project = TestProject::new();
    fs::create_dir_all(project.root.join("data/match-a")).unwrap();
    fs::create_dir_all(project.root.join("data/other")).unwrap();
    fs::write(
        project.root.join("data/match-a/events.ndjson"),
        "{\"id\":1,\"updated_at\":1783296000000000}\n",
    )
    .unwrap();
    fs::write(
        project.root.join("data/other/events.ndjson"),
        "{\"id\":2,\"updated_at\":1783296060000000}\n",
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
        "{\"id\":1,\"updated_at\":1783296000000000}\n",
    )
    .unwrap();
    fs::write(
        project.root.join("data/event12.ndjson"),
        "{\"id\":2,\"updated_at\":1783296060000000}\n",
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
        "{\"id\":1,\"updated_at\":1783296000000000}\n",
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
        "{\"id\":1,\"updated_at\":1783296000000000}\n",
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
    assert_eq!(report["ledger_events"]["event_count"], 13);
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    assert_eq!(report["ledger_events"]["events"][0]["kind"], "run_started");
    assert_eq!(
        report["ledger_events"]["events"][12]["kind"],
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
fn run_short_form_uses_product_defaults_and_destination_alias() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
        "--to",
        "duckdb://.cdf/short-form.duckdb",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "local.events");
    assert_eq!(report["pipeline_id"], "cdf-run");
    assert_eq!(report["target"], "events");
    assert!(
        report["package_id"]
            .as_str()
            .unwrap()
            .starts_with("pkg-local-events-")
    );
    assert!(
        report["checkpoint_id"]
            .as_str()
            .unwrap()
            .starts_with("checkpoint-local-events-")
    );
    assert!(
        report["destination"]["database_path"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/short-form.duckdb")
    );
    assert!(project.root.join(".cdf/short-form.duckdb").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let package_dir = project
        .root
        .join(".cdf/packages")
        .join(report["package_id"].as_str().unwrap());
    assert!(package_dir.exists());
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
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        "[plan] running run started",
        "[gate] succeeded run succeeded",
        "OK run ",
        "Run",
        "Package",
        "Rows",
        "Verdicts",
        "Receipt",
        "Gate",
        "resource     local.events",
        "target       events",
        "checkpoint           checkpoint-run-human",
        "condition            destination receipt verified before checkpoint commit",
        "-> cdf inspect run ",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn run_human_rich_render_uses_checkpoint_gate_panel() {
    let project = TestProject::new();
    let cli = test_cli(&project);
    let output = crate::run_command::run(
        &cli,
        crate::args::RunArgs {
            resource_id: Some("local.events".to_owned()),
            pipeline_id: Some("pipeline-run-rich".to_owned()),
            destination_uri: None,
            target: Some("events".to_owned()),
            package_id: Some("pkg-run-rich".to_owned()),
            checkpoint_id: Some("checkpoint-run-rich".to_owned()),
            loop_mode: false,
        },
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "Run progress",
        "\u{1b}[32m✓\u{1b}[0m run ",
        "\u{1b}[36mRun\u{1b}[0m",
        "\u{1b}[36mPackage\u{1b}[0m",
        "\u{1b}[36mRows\u{1b}[0m",
        "\u{1b}[36mVerdicts\u{1b}[0m",
        "\u{1b}[36mReceipt\u{1b}[0m",
        "\u{1b}[36mGate\u{1b}[0m",
        "checkpoint           checkpoint-run-rich",
        "destination receipt verified before checkpoint commit",
        "\u{1b}[36m→\u{1b}[0m cdf inspect run ",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn inspect_run_parser_rejects_missing_and_extra_args() {
    for args in [
        vec!["cdf", "--json", "inspect", "run"],
        vec!["cdf", "--json", "inspect", "run", "run-1", "extra"],
    ] {
        let result = run_dynamic(args.into_iter().map(str::to_owned).collect());

        assert_eq!(result.exit_code, 2, "stderr: {}", result.stderr);
        let json = assert_json_error_code(&result, "CDF-CLI-USAGE");
        assert_eq!(json["error"]["kind"], "contract");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("inspect run")
        );
    }
}

#[test]
fn inspect_run_reports_completed_run_json_and_human() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(&project, "pkg-inspect-run", "checkpoint-inspect-run");
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_id = run_json["result"]["run_id"].as_str().unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "inspect run");
    assert_eq!(report["command"], "inspect run");
    assert_eq!(report["run_id"], run_id);
    assert_eq!(report["terminal_state"], "succeeded");
    assert_eq!(report["terminal_kind"], "run_succeeded");
    assert_eq!(report["recovery"]["action"], "no_op");
    assert_eq!(report["recovery"]["mutation_required"], false);
    assert_eq!(report["recovery"]["source_contact"], false);
    assert_eq!(report["pointers"]["resource_ids"], json!(["local.events"]));
    assert_eq!(
        report["pointers"]["package_ids"],
        json!(["pkg-inspect-run"])
    );
    assert_eq!(
        report["pointers"]["checkpoint_ids"],
        json!(["checkpoint-inspect-run"])
    );
    assert_eq!(report["events"].as_array().unwrap().len(), 13);
    assert_eq!(report["events"][0]["sequence"], 1);
    assert_eq!(report["events"][0]["kind"], "run_started");
    assert_eq!(report["events"][12]["kind"], "run_succeeded");
    assert_eq!(report["artifacts"]["package_status"], "checkpointed");
    assert_eq!(
        report["artifacts"]["packages"][0]["status"], "available",
        "package report: {}",
        report["artifacts"]["packages"][0]
    );
    assert_eq!(
        report["artifacts"]["packages"][0]["lifecycle_status"],
        "checkpointed"
    );
    assert_eq!(report["artifacts"]["receipt"]["status"], "available");
    assert_eq!(
        report["artifacts"]["receipt"]["package_receipt_ids"][0],
        run_json["result"]["receipt_id"]
    );
    assert_eq!(report["artifacts"]["checkpoint"]["status"], "committed");
    assert_eq!(report["duplicate"]["status"], "unknown");
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id,
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(!human.stdout.contains("\u{1b}["));
    for expected in [
        &format!("OK run {run_id} terminal succeeded"),
        "Recovery",
        "Artifacts",
        "Pointers",
        "Duplicate",
        "Package artifacts",
        "action             no_op",
        "checkpoint status     committed",
        "-> cdf inspect run ",
    ] {
        assert!(
            human.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human.stdout
        );
    }
}

#[test]
fn inspect_run_marks_missing_package_artifact() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(
        &project,
        "pkg-inspect-run-missing",
        "checkpoint-inspect-run-missing",
    );
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_id = run_json["result"]["run_id"].as_str().unwrap();
    fs::remove_dir_all(project.root.join(".cdf/packages/pkg-inspect-run-missing")).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let package = &json["result"]["artifacts"]["packages"][0];
    assert_eq!(package["status"], "missing");
    assert_eq!(package["receipt_artifact_status"], "unavailable");
    assert!(
        package["reason"]
            .as_str()
            .unwrap()
            .contains("does not exist")
    );
    assert_eq!(
        json["result"]["artifacts"]["receipt"]["status"],
        "unavailable"
    );

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id,
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    for expected in [
        "Package artifacts",
        "missing",
        "package path recorded in the run ledger does not exist",
        "missing packages      1",
        "receipt status        unavailable",
    ] {
        assert!(
            human.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human.stdout
        );
    }
}

#[test]
fn inspect_run_human_rich_render_uses_recovery_and_artifact_panels() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(
        &project,
        "pkg-inspect-run-rich",
        "checkpoint-inspect-run-rich",
    );
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_id = run_json["result"]["run_id"].as_str().unwrap();
    let context = crate::context::ProjectContext::load(Some(&project.root), None).unwrap();

    let output = crate::inspect_run_command::inspect_run(&context, run_id.to_owned()).unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "\u{1b}[32m✓\u{1b}[0m run ",
        "\u{1b}[36mRecovery\u{1b}[0m",
        "\u{1b}[36mArtifacts\u{1b}[0m",
        "\u{1b}[36mPointers\u{1b}[0m",
        "action             no_op",
        "checkpoint status     committed",
        "│ seq │ kind",
        "\u{1b}[36m→\u{1b}[0m cdf inspect run ",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn inspect_run_missing_ledger_or_run_fails_without_creating_state() {
    let project = TestProject::new();
    let state_path = project.root.join(".cdf/state.db");

    let missing_state = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        "run-absent",
    ]);

    assert_eq!(missing_state.exit_code, 5);
    assert!(
        !state_path.exists(),
        "inspect run must not create missing state"
    );
    let json = stderr_or_stdout_json(&missing_state.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("state database")
    );

    let run_result = run_valid_run_args(
        &project,
        "pkg-inspect-run-present",
        "checkpoint-inspect-run-present",
    );
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let missing_run = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        "run-not-in-ledger",
    ]);

    assert_eq!(missing_run.exit_code, 5);
    let json = stderr_or_stdout_json(&missing_run.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not present")
    );
}

#[test]
fn inspect_run_redacts_secret_ref_details_without_resolving_project_secret() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "postgres://user:resolved-inspect-secret@localhost/db\n",
    )
    .unwrap();
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let run_id = RunId::new("run-redaction").unwrap();
    let run_record = ledger.create_run(Some(run_id.clone())).unwrap();
    let mut event = RunEventAppend::new(RunEventKind::RunStarted);
    event.destination_id = Some(DestinationId::new("postgres").unwrap());
    event.details = RunEventDetails::new([(
        "destination_secret",
        RunEventValue::SecretRef(SecretReference::new("secret://file/destination-dsn").unwrap()),
    )]);
    ledger.append_event(&run_record.run_id, event).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id.as_str(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "resolved-inspect-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    let detail = &json["result"]["events"][0]["details"]["attributes"]["destination_secret"];
    assert_eq!(detail["type"], "secret_ref");
    assert_eq!(detail["value"], "secret://file/destination-dsn");
}

#[test]
fn inspect_run_human_render_redacts_uri_userinfo_in_artifact_paths() {
    let project = TestProject::new();
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let run_id = RunId::new("run-inspect-render-redaction").unwrap();
    let run_record = ledger.create_run(Some(run_id.clone())).unwrap();
    let mut event = RunEventAppend::new(RunEventKind::PackageFinalized);
    event.package_id = Some("pkg-inspect-render-redaction".to_owned());
    event.package_path = Some("postgres://user:inspect-render-secret@localhost/db".to_owned());
    ledger.append_event(&run_record.run_id, event).unwrap();

    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id.as_str(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "inspect-render-secret");
    assert!(result.stdout.contains("postgres://[redacted]@localhost/db"));
    assert!(result.stdout.contains("missing"));
}

#[test]
fn inspect_run_reports_duplicate_replay_status() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-inspect-run-duplicate",
        "checkpoint-inspect-run-duplicate",
    );
    let first = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/inspect-run-duplicate.duckdb",
    );
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    remove_state_store(&project);

    let second = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/inspect-run-duplicate.duckdb",
    );
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    let second_json = stderr_or_stdout_json(&second.stdout);
    let run_id = second_json["result"]["run_id"].as_str().unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["terminal_state"], "replay_recorded");
    assert_eq!(json["result"]["duplicate"]["status"], "duplicate");
    assert_eq!(json["result"]["duplicate"]["duplicate"], true);
    assert_eq!(json["result"]["duplicate"]["no_op"], true);
    let replay_event = json["result"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["kind"] == "replay_recorded")
        .unwrap();
    assert_eq!(
        replay_event["details"]["attributes"]["receipt_source"]["value"],
        "duck_db_commit"
    );
}

#[test]
fn resume_bare_noops_when_no_interrupted_runs_and_accepts_positional_terminal_noop() {
    let project = TestProject::new();
    SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();

    let bare = run(["cdf", "--json", "--project", project.root_str(), "resume"]);

    assert_eq!(bare.exit_code, 0, "stderr: {}", bare.stderr);
    let bare_json = stderr_or_stdout_json(&bare.stdout);
    assert_eq!(bare_json["command"], "resume");
    assert_eq!(bare_json["result"]["state"], "no_interrupted_runs");
    assert_eq!(bare_json["result"]["writes"]["package"], false);
    assert_eq!(bare_json["result"]["writes"]["destination"], false);
    assert_eq!(bare_json["result"]["writes"]["checkpoint"], false);

    let run_result = run_valid_run_args(&project, "pkg-resume-noop", "checkpoint-resume-noop");
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_id = run_json["result"]["run_id"].as_str().unwrap();

    let result = resume_command(&project, run_id, false);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "resume");
    assert_eq!(json["result"]["state"], "terminal_success");
    assert_eq!(json["result"]["action"], "no_op");
    assert_eq!(json["result"]["source_contact"], false);
    assert_eq!(json["result"]["mutation_required"], false);
    assert_eq!(json["result"]["mutated"], false);
}

#[test]
fn resume_missing_state_path_error_has_code_and_project_path_context() {
    let project = TestProject::new();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "resume",
        "run-missing-state",
    ]);

    assert_eq!(result.exit_code, 5);
    let json = assert_json_error_code(&result, "CDF-STATE-RESUME-LEDGER");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains(".cdf/state.db")
    );
}

#[test]
fn resume_bare_selects_single_interrupted_run_and_fails_closed() {
    let project = TestProject::new();
    create_resume_run_with_events(
        &project,
        "run-resume-bare-single",
        &[RunEventKind::RunStarted, RunEventKind::RunFailed],
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "resume"]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["run_id"], "run-resume-bare-single");
    assert_eq!(json["result"]["state"], "no_finalized_package");
    assert_eq!(
        json["result"]["action"],
        "rerun_extraction_from_last_committed_checkpoint"
    );
}

#[test]
fn resume_bare_multiple_interrupted_runs_fails_closed_without_mutation() {
    let project = TestProject::new();
    for run_id in ["run-resume-bare-first", "run-resume-bare-second"] {
        create_resume_run_with_events(&project, run_id, &[RunEventKind::RunStarted]);
    }

    let result = run(["cdf", "--json", "--project", project.root_str(), "resume"]);

    assert_eq!(result.exit_code, 78);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("bare resume found 2 interrupted runs")
    );
}

#[test]
fn resume_no_finalized_package_fails_closed_with_guidance() {
    let project = TestProject::new();
    let run_id = create_resume_run_with_events(
        &project,
        "run-resume-no-package",
        &[RunEventKind::RunStarted, RunEventKind::RunFailed],
    );

    let result = resume_command(&project, run_id.as_str(), true);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["state"], "no_finalized_package");
    assert_eq!(
        json["result"]["action"],
        "rerun_extraction_from_last_committed_checkpoint"
    );
    assert_eq!(json["result"]["recovery"]["result"], "failed_closed");
    assert!(
        json["result"]["recovery"]["guidance"]
            .as_str()
            .unwrap()
            .contains("no finalized package")
    );
}

#[test]
fn resume_human_headless_render_uses_recovery_panels_and_redacts_destination_uri() {
    let project = TestProject::new();
    write_project_destination(
        &project,
        "postgres://user:resume-render-secret@localhost/db",
    );
    let run_id = create_resume_run_with_events(
        &project,
        "run-resume-human-no-package",
        &[RunEventKind::RunStarted, RunEventKind::RunFailed],
    );

    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "resume",
        "--run",
        run_id.as_str(),
    ]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    assert_secret_absent(&result, "resume-render-secret");
    for expected in [
        "[plan] running run started",
        "[plan] failed run failed",
        "ERR resume run run-resume-human-no-package failed closed",
        "Recovery",
        "Durable artifacts",
        "State",
        "Run ledger",
        "failed phase        no_finalized_package",
        "mutation performed  no",
        "postgres://[redacted]@localhost/db",
        "-> cdf run <resource>",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn resume_human_rich_render_uses_recovery_and_artifact_panels() {
    let project = TestProject::new();
    let run_id = create_resume_run_with_events(
        &project,
        "run-resume-rich-no-package",
        &[RunEventKind::RunStarted, RunEventKind::RunFailed],
    );
    let output = crate::resume_command::resume(
        &test_cli(&project),
        crate::args::ResumeArgs {
            run_id: Some(run_id.to_string()),
        },
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    for expected in [
        "Run progress",
        "\u{1b}[31m✗\u{1b}[0m resume run run-resume-rich-no-package failed closed",
        "\u{1b}[36mRecovery\u{1b}[0m",
        "\u{1b}[36mDurable artifacts\u{1b}[0m",
        "\u{1b}[36mState\u{1b}[0m",
        "\u{1b}[36mRun ledger\u{1b}[0m",
        "failed phase        no_finalized_package",
        "mutation performed  no",
        "\u{1b}[36m→\u{1b}[0m cdf run <resource>",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn resume_finalized_package_without_receipt_replays_without_source_contact() {
    let project = TestProject::new();
    let package_dir =
        create_replay_package_fixture(&project, "pkg-resume-replay", "checkpoint-resume-replay");
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-replay",
        &package_dir,
        &[RunEventKind::PackageFinalized, RunEventKind::RunFailed],
    );

    let result = resume_command(&project, run_id.as_str(), true);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["state"], "package_finalized_without_receipt");
    assert_eq!(json["result"]["action"], "replay_package");
    assert_eq!(json["result"]["source_contact"], false);
    assert_eq!(json["result"]["mutation_required"], true);
    assert_eq!(json["result"]["mutated"], true);
    assert_eq!(json["result"]["package"]["status"], "checkpointed");
    assert_eq!(json["result"]["checkpoint"]["status"], "committed");
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(package_receipt_count(&package_dir), 1);
}

#[test]
fn resume_finalized_package_human_progress_replays_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-resume-progress",
        "checkpoint-resume-progress",
    );
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-progress",
        &package_dir,
        &[RunEventKind::PackageFinalized, RunEventKind::RunFailed],
    );

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "resume".to_owned(),
        "--run".to_owned(),
        run_id.to_string(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join("data/events.ndjson").exists());
    for expected in [
        "[package] running package finalized",
        "[package] failed run failed",
        "[verify] running destination receipt recorded",
        "[gate] succeeded run resumed",
        "OK resume run run-resume-progress completed",
        "source_contact=false",
        "source contact",
        "mutation performed  yes",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(package_receipt_count(&package_dir), 1);
}

#[test]
fn resume_finalized_postgres_package_without_receipt_replays_without_source_contact() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        format!("{}\n", postgres.url),
    )
    .unwrap();
    let target = format!("{}.events_cli_resume", postgres.schema);
    let package_dir = create_replay_package_fixture_with_target(
        &project,
        "pkg-resume-postgres-replay",
        "checkpoint-resume-postgres-replay",
        &target,
    );
    write_project_destination_with_postgres_policy(
        &project,
        "postgres://secret://file/destination-dsn",
        "fail",
    );
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-postgres-replay",
        &package_dir,
        &[RunEventKind::PackageFinalized, RunEventKind::RunFailed],
    );

    let result = resume_command(&project, run_id.as_str(), true);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &postgres.url);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["state"], "package_finalized_without_receipt");
    assert_eq!(report["action"], "replay_package");
    assert_eq!(report["source_contact"], false);
    assert_eq!(report["mutated"], true);
    assert_eq!(report["package"]["status"], "checkpointed");
    assert_eq!(report["package"]["receipt_count"], 1);
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["receipt"]["destination_id"], "postgres");
    assert_eq!(report["receipt"]["target"], target);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(package_receipt_count(&package_dir), 1);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("resume Postgres checkpoint head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        "checkpoint-resume-postgres-replay"
    );
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt"]["receipt_id"].as_str().unwrap()
    );

    let mut client = postgres.client();
    let rows: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                postgres.table("events_cli_resume")
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 2);
}

#[test]
fn resume_durable_receipt_commits_uncommitted_checkpoint_without_source_contact() {
    let project = TestProject::new();
    let package_dir =
        create_replay_package_fixture(&project, "pkg-resume-receipt", "checkpoint-resume-receipt");
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id =
        seed_resume_receipt_before_checkpoint(&project, &package_dir, "run-resume-receipt");

    let result = resume_command(&project, run_id.as_str(), true);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["state"],
        "receipt_recorded_without_checkpoint_commit"
    );
    assert_eq!(
        json["result"]["action"],
        "verify_receipt_then_commit_checkpoint"
    );
    assert_eq!(json["result"]["source_contact"], false);
    assert_eq!(json["result"]["mutated"], true);
    assert_eq!(json["result"]["checkpoint"]["status"], "committed");
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
}

#[test]
fn resume_committed_checkpoint_updates_stale_package_status_only() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(
        &project,
        "pkg-resume-stale-status",
        "checkpoint-resume-stale-status",
    );
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let package_dir = project.root.join(".cdf/packages/pkg-resume-stale-status");
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Loading).unwrap();
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-stale-status",
        &package_dir,
        &[
            RunEventKind::PackageFinalized,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::CheckpointCommitted,
            RunEventKind::RunFailed,
        ],
    );

    let result = resume_command(&project, run_id.as_str(), true);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["state"],
        "checkpoint_committed_with_stale_package_status"
    );
    assert_eq!(json["result"]["action"], "update_package_status");
    assert_eq!(json["result"]["mutated"], true);
    assert_eq!(json["result"]["package"]["status"], "checkpointed");
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
}

#[test]
fn resume_stale_package_status_fails_closed_when_current_head_is_different() {
    let project = TestProject::new();
    let first = run_valid_run_args(
        &project,
        "pkg-resume-wrong-head-old",
        "checkpoint-resume-wrong-head-old",
    );
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let second = run_valid_run_args(
        &project,
        "pkg-resume-wrong-head-current",
        "checkpoint-resume-wrong-head-current",
    );
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let package_dir = project.root.join(".cdf/packages/pkg-resume-wrong-head-old");
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Loading).unwrap();
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-wrong-head",
        &package_dir,
        &[
            RunEventKind::PackageFinalized,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::CheckpointCommitted,
            RunEventKind::RunFailed,
        ],
    );

    let result = resume_command(&project, run_id.as_str(), true);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["state"],
        "checkpoint_committed_head_not_exact"
    );
    assert_eq!(json["result"]["action"], "inspect_missing_artifacts");
    assert_eq!(json["result"]["mutated"], false);
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
}

#[test]
fn resume_stale_package_status_fails_closed_when_selected_receipt_differs_from_head() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(
        &project,
        "pkg-resume-wrong-receipt",
        "checkpoint-resume-wrong-receipt",
    );
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let package_dir = project.root.join(".cdf/packages/pkg-resume-wrong-receipt");
    let mut reader = PackageReader::open(&package_dir).unwrap();
    let mut wrong_receipt = reader.receipts().unwrap()[0].clone();
    wrong_receipt.receipt_id = ReceiptId::new("receipt-resume-wrong").unwrap();
    reader.append_receipt(wrong_receipt).unwrap();
    reader.update_status(PackageStatus::Loading).unwrap();
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-wrong-receipt",
        &package_dir,
        &[
            RunEventKind::PackageFinalized,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::CheckpointCommitted,
            RunEventKind::RunFailed,
        ],
    );

    let result = resume_command(&project, run_id.as_str(), true);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["state"],
        "checkpoint_committed_head_not_exact"
    );
    assert_eq!(json["result"]["mutated"], false);
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
}

#[test]
fn resume_missing_package_artifact_fails_closed_with_guidance() {
    let project = TestProject::new();
    let missing_package = project.root.join(".cdf/packages/pkg-resume-missing");
    let run_id = create_resume_run_with_missing_package(
        &project,
        "run-resume-missing-package",
        &missing_package,
    );

    let result = resume_command(&project, run_id.as_str(), true);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["state"], "missing_package_artifact");
    assert_eq!(json["result"]["action"], "inspect_missing_artifacts");
    assert_eq!(json["result"]["recovery"]["result"], "failed_closed");
    assert!(
        json["result"]["recovery"]["guidance"]
            .as_str()
            .unwrap()
            .contains("does not exist")
    );
}

#[test]
fn run_missing_resource_still_fails_before_writes() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "--pipeline",
        "pipeline-run",
        "--target",
        "events",
        "--package-id",
        "pkg-run-missing",
        "--checkpoint-id",
        "checkpoint-run-missing",
    ]);

    assert_eq!(result.exit_code, 2, "stderr: {}", result.stderr);
    assert_no_run_writes(&project, "pkg-run-missing");
    let json = assert_json_error_code(&result, "CDF-RUN-ARGUMENT");
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("run requires RESOURCE or --resource")
    );
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
fn run_postgres_destination_missing_policy_fails_closed_before_writes() {
    let project = TestProject::new();
    write_project_destination(&project, "postgres://secret://env/WAREHOUSE");

    let result = run_valid_run_args(&project, "pkg-run-postgres", "checkpoint-run-postgres");

    assert_eq!(result.exit_code, 3);
    assert_no_run_writes(&project, "pkg-run-postgres");
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], false);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("destination_policy.postgres")
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

    assert_eq!(result.exit_code, 4);
    assert_no_run_writes(&project, "pkg-run-rest");
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], false);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("secret://env/CDF_CLI_TOKEN")
    );
}

#[test]
fn run_rest_resource_uses_http_transport_and_commits_checkpoint() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "rest-token-secret\n").unwrap();
    let base_url = serve_json_once(
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": 20 }
        ] }"#,
    );
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );

    let result = run_valid_run_resource_target(
        &project,
        "api.items",
        "pkg-run-rest-success",
        "checkpoint-run-rest-success",
        "items",
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "rest-token-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "api.items");
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["target"], "items");
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-run").unwrap(),
            &ResourceId::new("api.items").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed REST run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        "checkpoint-run-rest-success"
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
fn run_sql_resource_with_ordered_cursor_commits_checkpoint() {
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

    let project = TestProject::new();
    let source_dsn = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:source-sql-secret@",
        1,
    );
    fs::write(project.root.join("sql-dsn"), format!("{source_dsn}\n")).unwrap();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/sql-dsn"),
    );
    fs::write(
        project.root.join("resources/sql.toml"),
        sql_resource_with_ordered_cursor("secret://file/sql-dsn", &table),
    )
    .unwrap();

    let result = run_valid_run_resource_target(
        &project,
        "warehouse.orders",
        "pkg-run-sql-success",
        "checkpoint-run-sql-success",
        "orders",
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert_secret_absent(&result, "source-sql-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "warehouse.orders");
    assert_eq!(report["target"], "orders");
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["destination"]["destination_id"], "duckdb");
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["segment_count"], 1);
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(report["receipt"]["segment_ack_count"], 1);
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["checkpoint"]["committed"], true);
    assert_eq!(report["checkpoint"]["is_head"], true);
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    assert_eq!(
        report["ledger_events"]["events"][12]["kind"],
        "run_succeeded"
    );

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let mut statement = conn
        .prepare("SELECT id, updated_at FROM orders ORDER BY id")
        .unwrap();
    let rows = statement
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-run").unwrap(),
            &ResourceId::new("warehouse.orders").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed SQL run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        "checkpoint-run-sql-success"
    );
    let SourcePosition::Cursor(cursor) = &head.delta.output_position else {
        panic!("expected SQL run checkpoint head to use a cursor position");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
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
fn run_postgres_destination_secret_is_not_resolved_before_missing_policy_blocker() {
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

    assert_eq!(result.exit_code, 3);
    assert_no_run_writes(&project, "pkg-run-postgres-redacted");
    assert_secret_absent(&result, "destination-secret");
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("destination_policy.postgres")
    );
}

#[test]
fn run_postgres_destination_unsupported_policy_fails_before_secret_resolution() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "postgres://user:destination-secret@localhost/db\n",
    )
    .unwrap();
    write_project_destination_with_postgres_policy(
        &project,
        "postgres://secret://file/destination-dsn",
        "last",
    );

    let result = run_valid_run_target(
        &project,
        "pkg-run-postgres-policy-unsupported",
        "checkpoint-run-postgres-policy-unsupported",
        "public.events",
    );

    assert_eq!(result.exit_code, 3);
    assert_no_run_writes(&project, "pkg-run-postgres-policy-unsupported");
    assert_secret_absent(&result, "destination-secret");
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("expected `fail`")
    );
}

#[test]
fn run_postgres_destination_resolves_secret_and_commits_checkpoint() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        format!("{}\n", postgres.url),
    )
    .unwrap();
    write_project_destination_with_postgres_policy(
        &project,
        "postgres://secret://file/destination-dsn",
        "fail",
    );
    let target = format!("{}.events_cli_run", postgres.schema);

    let result = run_valid_run_target(
        &project,
        "pkg-run-postgres-success",
        "checkpoint-run-postgres-success",
        &target,
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &postgres.url);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["destination"]["kind"], "postgres");
    assert_eq!(report["destination"]["destination_id"], "postgres");
    assert_eq!(report["destination"]["target"], target);
    assert_eq!(report["target"], target);
    assert_eq!(report["receipt"]["destination_id"], "postgres");
    assert_eq!(report["receipt"]["target"], target);
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(
        report["receipt_source"]["kind"],
        "destination_commit_receipt_only"
    );
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed Postgres run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        "checkpoint-run-postgres-success"
    );

    let mut client = postgres.client();
    let rows: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                postgres.table("events_cli_run")
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 2);
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
fn replay_package_without_to_uses_environment_destination_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-replay-default-destination",
        "checkpoint-replay-default-destination",
    );
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "replay",
        "package",
        package_dir.to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["package_id"], "pkg-replay-default-destination");
    assert_eq!(report["target"], "events");
    assert!(
        report["destination"]["database_path"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/dev.duckdb")
    );
    assert_eq!(duckdb_event_count(project.root.join(".cdf/dev.duckdb")), 2);
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
    assert_eq!(report["ledger_events"]["event_count"], 8);
    assert_eq!(report["ledger_events"]["terminal_kind"], "replay_recorded");
    assert_eq!(report["ledger_events"]["kinds"]["package_finalized"], 1);
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
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
fn replay_package_failure_records_progress_events_without_json_progress_output() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-replay-progress-failure",
        "checkpoint-replay-progress-failure",
    );
    let first = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-progress-failure.duckdb",
    );
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);

    let second = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-progress-failure-again.duckdb",
    );

    assert_ne!(second.exit_code, 0);
    assert!(second.stdout.is_empty());
    assert!(!second.stderr.contains("Run progress"));
    assert!(!second.stderr.contains("package finalized"));

    let conn = Connection::open(project.root.join(".cdf/state.db")).unwrap();
    let latest_run_id: String = conn
        .query_row(
            "SELECT run_id FROM cdf_runs ORDER BY sequence DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let events = ledger
        .events(&RunId::new(latest_run_id).unwrap())
        .unwrap()
        .into_iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        events,
        vec![RunEventKind::PackageFinalized, RunEventKind::RunFailed]
    );
}

#[test]
fn replay_package_failure_human_stderr_includes_progress_context() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-replay-progress-human-failure",
        "checkpoint-replay-progress-human-failure",
    );
    let first = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-progress-human-failure.duckdb",
    );
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);

    let second = run_dynamic(vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "replay".to_owned(),
        "package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        "duckdb://.cdf/replay-progress-human-failure-again.duckdb".to_owned(),
    ]);

    assert_ne!(second.exit_code, 0);
    assert!(second.stdout.is_empty());
    assert!(!second.stderr.contains("\u{1b}["));
    for expected in [
        "[package] running package finalized",
        "[package] failed run failed",
        "error:",
        "checkpoint-replay-progress-human-failure",
    ] {
        assert!(
            second.stderr.contains(expected),
            "missing {expected:?} in:\n{}",
            second.stderr
        );
    }
}

#[test]
fn replay_package_human_headless_render_reports_receipt_checkpoint_and_duplicate_facts() {
    let project = TestProject::new();
    let package_dir =
        create_replay_package_fixture(&project, "pkg-replay-human", "checkpoint-replay-human");
    let first = run_dynamic(vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "replay".to_owned(),
        "package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        "duckdb://.cdf/replay-human.duckdb".to_owned(),
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);

    remove_state_store(&project);
    let second = run_dynamic(vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "replay".to_owned(),
        "package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        "duckdb://.cdf/replay-human.duckdb".to_owned(),
    ]);

    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    assert!(!second.stdout.contains("\u{1b}["));
    for expected in [
        "[commit] succeeded replay recorded",
        "duplicate=true",
        "no_op=true",
        "OK replay package pkg-replay-human completed",
        "Replay",
        "Destination",
        "Duplicate",
        "Receipt",
        "Checkpoint",
        "duplicate  yes",
        "no-op      yes",
        "checkpoint       checkpoint-replay-human",
        "ledger terminal  replay_recorded",
        "-> cdf inspect run ",
    ] {
        assert!(
            second.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            second.stdout
        );
    }
}

#[test]
fn replay_package_human_rich_render_uses_duplicate_receipt_checkpoint_panels() {
    let project = TestProject::new();
    let package_dir =
        create_replay_package_fixture(&project, "pkg-replay-rich", "checkpoint-replay-rich");
    let cli = test_cli(&project);
    let output = crate::replay_command::replay_package(
        &cli,
        crate::args::ReplayPackageArgs {
            package_dir,
            destination_uri: Some("duckdb://.cdf/replay-rich.duckdb".to_owned()),
            target: None,
            merge_dedup: None,
        },
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "Run progress",
        "\u{1b}[32m✓\u{1b}[0m replay package pkg-replay-rich completed",
        "\u{1b}[36mReplay\u{1b}[0m",
        "\u{1b}[36mDestination\u{1b}[0m",
        "\u{1b}[36mDuplicate\u{1b}[0m",
        "\u{1b}[36mReceipt\u{1b}[0m",
        "\u{1b}[36mCheckpoint\u{1b}[0m",
        "duplicate  no",
        "no-op      no",
        "checkpoint       checkpoint-replay-rich",
        "\u{1b}[36m→\u{1b}[0m cdf inspect run ",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
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

    assert_eq!(result.exit_code, 2);
    assert_secret_absent(&result, "destination-secret");
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("requires --target schema.table")
    );
}

#[test]
fn replay_package_postgres_missing_merge_dedup_fails_closed_before_mutation() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "postgres://user:destination-secret@localhost/db\n",
    )
    .unwrap();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-replay-postgres-missing-dedup",
        "checkpoint-replay-postgres-missing-dedup",
    );
    let receipts = package_receipt_count(&package_dir);
    let status = package_status(&package_dir);

    let result = replay_package_command_with_postgres_options(
        &project,
        &package_dir,
        "postgres://secret://file/destination-dsn",
        Some("public.events"),
        None,
    );

    assert_eq!(result.exit_code, 2);
    assert_secret_absent(&result, "destination-secret");
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("requires --merge-dedup fail")
    );
}

#[test]
fn replay_package_postgres_unsupported_merge_dedup_fails_closed_before_mutation() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "postgres://user:destination-secret@localhost/db\n",
    )
    .unwrap();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-replay-postgres-unsupported-dedup",
        "checkpoint-replay-postgres-unsupported-dedup",
    );
    let receipts = package_receipt_count(&package_dir);
    let status = package_status(&package_dir);

    let result = replay_package_command_with_postgres_options(
        &project,
        &package_dir,
        "postgres://secret://file/destination-dsn",
        Some("public.events"),
        Some("last"),
    );

    assert_eq!(result.exit_code, 2);
    assert_secret_absent(&result, "destination-secret");
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("supported value is `fail`")
    );
}

#[test]
fn replay_package_postgres_target_mismatch_fails_closed_before_state_creation() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-replay-postgres-target-mismatch",
        "checkpoint-replay-postgres-target-mismatch",
    );
    let receipts = package_receipt_count(&package_dir);
    let status = package_status(&package_dir);

    let result = replay_package_command_with_postgres_options(
        &project,
        &package_dir,
        "postgres://localhost/cdf",
        Some("public.events"),
        Some("fail"),
    );

    assert_eq!(result.exit_code, 3, "stderr: {}", result.stderr);
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("does not match package destination commit target")
    );
}

#[test]
fn replay_package_postgres_secret_backed_uri_redacts_resolved_dsn_on_target_mismatch() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "postgres://user:destination-secret@localhost/db\n",
    )
    .unwrap();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-replay-postgres-secret-target-mismatch",
        "checkpoint-replay-postgres-secret-target-mismatch",
    );
    let receipts = package_receipt_count(&package_dir);
    let status = package_status(&package_dir);

    let result = replay_package_command_with_postgres_options(
        &project,
        &package_dir,
        "postgres://secret://file/destination-dsn",
        Some("public.events"),
        Some("fail"),
    );

    assert_eq!(result.exit_code, 3, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "destination-secret");
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("does not match package destination commit target")
    );
}

#[test]
fn replay_package_postgres_replays_from_artifacts_without_source_contact() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        format!("{}\n", postgres.url),
    )
    .unwrap();
    let target = format!("{}.events_cli_replay", postgres.schema);
    let package_dir = create_replay_package_fixture_with_target(
        &project,
        "pkg-replay-postgres-success",
        "checkpoint-replay-postgres-success",
        &target,
    );
    let manifest = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .clone();
    let receipts_before = package_receipt_count(&package_dir);

    let result = replay_package_command_with_postgres_options(
        &project,
        &package_dir,
        "postgres://secret://file/destination-dsn",
        Some(&target),
        Some("fail"),
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &postgres.url);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "replay package");
    assert_eq!(report["command"], "replay package");
    assert_eq!(report["package_id"], "pkg-replay-postgres-success");
    assert_eq!(report["package_hash"], manifest.package_hash);
    assert_eq!(report["destination"]["kind"], "postgres");
    assert_eq!(report["destination"]["destination_id"], "postgres");
    assert_eq!(report["destination"]["target"], target);
    assert_eq!(report["target"], target);
    assert_eq!(report["receipt"]["destination_id"], "postgres");
    assert_eq!(report["receipt"]["target"], target);
    assert_eq!(report["receipt"]["package_hash"], manifest.package_hash);
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(
        report["receipt_source"]["kind"],
        "destination_commit_receipt_only"
    );
    assert_eq!(report["receipt_source"]["package_receipt_recorded"], true);
    assert_eq!(
        report["checkpoint_id"],
        "checkpoint-replay-postgres-success"
    );
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(report["ledger_events"]["event_count"], 8);
    assert_eq!(report["ledger_events"]["terminal_kind"], "replay_recorded");
    assert_eq!(report["ledger_events"]["kinds"]["package_finalized"], 1);
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    assert_eq!(report["ledger_events"]["kinds"]["replay_recorded"], 1);
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
        "checkpoint-replay-postgres-success"
    );
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );

    let mut client = postgres.client();
    let rows: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                postgres.table("events_cli_replay")
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 2);
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
    assert_eq!(report["ledger_events"]["event_count"], 8);
    assert_eq!(report["ledger_events"]["terminal_kind"], "replay_recorded");
    assert_eq!(report["ledger_events"]["kinds"]["package_finalized"], 1);
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
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
            .contains("supported destinations are duckdb://path, parquet://root, and postgres://")
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
    assert!(
        human
            .stdout
            .contains("OK no freshness SLO resources to evaluate")
    );
    assert!(human.stdout.contains("Freshness"));
    assert!(human.stdout.contains("total          0"));
    assert!(human.stdout.contains("-> cdf doctor"));
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
    assert_eq!(resource["receipt_freshness"]["state"], "missing_run_ledger");
    assert_eq!(
        resource["receipt_freshness"]["source"],
        "checkpoint_committed_head"
    );
    assert_eq!(
        resource["checkpoint"]["checkpoint_id"],
        "checkpoint-status-fresh"
    );
    assert_eq!(resource["checkpoint"]["pipeline_id"], "pipeline-1");
    assert!(resource["age_ms"].as_u64().unwrap() <= 3_600_000);
    let human = run(["cdf", "--project", project.root_str(), "status"]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(
        human
            .stdout
            .contains("OK freshness SLO status fresh: 1 resource(s)")
    );
    assert!(human.stdout.contains("| resource     | state | age"));
    assert!(human.stdout.contains("| local.events | fresh"));
    assert!(human.stdout.contains("-> cdf doctor"));
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
    assert!(
        human
            .stdout
            .contains("ERR freshness SLO breach: 1 stale, 0 fresh, 0 non-evaluable")
    );
    assert!(human.stdout.contains("| local.events | stale"));
    assert!(human.stdout.contains("-> cdf doctor"));
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
    assert!(
        human
            .stdout
            .contains("WARN freshness SLO status non-evaluable: 1 resource(s), 0 fresh")
    );
    assert!(human.stdout.contains("| local.events | non-evaluable"));
    assert!(human.stdout.contains("state_database_missing"));
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
fn status_reports_missing_run_ledger_as_non_evaluable_without_committed_head() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    initialize_status_state(&project);

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 78, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["non_evaluable"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "non_evaluable");
    assert_eq!(resource["non_evaluable_reason"], "run_ledger_missing");
    assert_eq!(resource["receipt_freshness"]["state"], "missing_run_ledger");
    assert_eq!(resource["receipt_freshness"]["source"], "run_ledger");
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
fn status_reports_fresh_receipt_only_runtime_fact() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    initialize_status_state(&project);
    let committed_at_ms = now_ms_for_test();
    let (package_dir, package_hash) = write_status_package_receipt(
        &project,
        "pkg-status-receipt-fresh",
        "receipt-status-runtime-fresh",
        committed_at_ms,
    );
    record_status_receipt_event(
        &project,
        "run-status-receipt-fresh",
        &package_dir,
        &package_hash,
        "receipt-status-runtime-fresh",
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(
        result.exit_code, 0,
        "stdout: {} stderr: {}",
        result.stdout, result.stderr
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["fresh"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "fresh");
    assert!(resource["checkpoint"].is_null());
    assert_eq!(resource["receipt_freshness"]["state"], "fresh_receipt");
    assert_eq!(resource["receipt_freshness"]["source"], "package_receipt");
    assert_eq!(
        resource["receipt_freshness"]["receipt_id"],
        "receipt-status-runtime-fresh"
    );
    assert!(resource["age_ms"].as_u64().unwrap() <= 3_600_000);
}

#[test]
fn status_reports_stale_receipt_only_runtime_fact() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1ms");
    initialize_status_state(&project);
    let (package_dir, package_hash) =
        write_status_package_receipt(&project, "pkg-status-receipt-stale", "receipt-stale", 1);
    record_status_receipt_event(
        &project,
        "run-status-receipt-stale",
        &package_dir,
        &package_hash,
        "receipt-stale",
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(
        result.exit_code, 1,
        "stdout: {} stderr: {}",
        result.stdout, result.stderr
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["stale"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "stale");
    assert_eq!(resource["receipt_freshness"]["state"], "stale_receipt");
    assert_eq!(resource["receipt_freshness"]["source"], "package_receipt");
}

#[test]
fn status_reports_missing_receipt_artifact_as_non_evaluable() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    initialize_status_state(&project);
    let (package_dir, package_hash) = write_status_package(&project, "pkg-status-missing-receipt");
    record_status_receipt_event(
        &project,
        "run-status-missing-receipt",
        &package_dir,
        &package_hash,
        "receipt-status-missing",
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 78, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["non_evaluable"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "non_evaluable");
    assert_eq!(resource["non_evaluable_reason"], "receipt_missing");
    assert_eq!(resource["receipt_freshness"]["state"], "missing_receipt");
    assert_eq!(
        resource["receipt_freshness"]["source"],
        "run_ledger_receipt"
    );
}

#[test]
fn status_committed_head_timestamp_takes_precedence_over_package_receipt() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    let checkpoint_committed_at_ms = now_ms_for_test();
    let (package_dir, package_hash) = write_status_package_receipt(
        &project,
        "pkg-status-precedence",
        "receipt-status-precedence",
        1,
    );
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-precedence",
        &package_hash,
        "receipt-status-precedence",
        checkpoint_committed_at_ms,
    );
    record_status_receipt_event(
        &project,
        "run-status-precedence",
        &package_dir,
        &package_hash,
        "receipt-status-precedence",
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "fresh");
    assert!(resource["age_ms"].as_u64().unwrap() <= 3_600_000);
    assert_eq!(resource["receipt_freshness"]["state"], "corrupt_receipt");
    assert_eq!(resource["receipt_freshness"]["source"], "package_receipt");
    assert_eq!(
        resource["receipt_freshness"]["observed_at_ms"],
        checkpoint_committed_at_ms
    );
    assert_eq!(
        resource["receipt_freshness"]["package_receipt_committed_at_ms"],
        1
    );
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
    assert!(
        result
            .stdout
            .contains("OK sql returned 1 row(s) from local system history")
    );
    assert!(result.stdout.contains("System SQL"));
    assert!(result.stdout.contains("| package_count |"));
    assert!(result.stdout.contains("| 0             |"));
    assert!(
        result
            .stdout
            .contains("-> cdf sql \"select * from packages limit 5\"")
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
    let json = assert_json_error_code(&result, "CDF-SQL-QUERY");
    assert_eq!(json["error"]["kind"], "contract");
    assert!(!result.stderr.contains("delete from packages"));
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
fn package_ls_json_remains_array_while_human_uses_renderer() {
    let temp = TempDir::new("cdf-cli-package-ls");
    let package_dir = build_archive_cli_package(temp.path(), "pkg-ls-json-array");

    let json_result = run([
        "cdf",
        "--json",
        "package",
        "ls",
        temp.path().to_str().unwrap(),
    ]);

    assert_eq!(json_result.exit_code, 0, "stderr: {}", json_result.stderr);
    let json = stderr_or_stdout_json(&json_result.stdout);
    assert_eq!(json["command"], "package ls");
    assert!(json["result"].as_array().is_some());
    assert_eq!(json["result"][0]["path"], package_dir.display().to_string());

    let human = run(["cdf", "package", "ls", temp.path().to_str().unwrap()]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(human.stdout.contains("OK 1 package(s)"));
    assert!(human.stdout.contains("Packages"));
    assert!(human.stdout.contains("| path"));
    assert!(human.stdout.contains("-> cdf package verify <package>"));
}

#[test]
fn package_gc_plans_retention_from_packages_and_checkpoint_history() {
    let project = TestProject::new();
    let package_root = project.root.join(".cdf/packages");
    fs::create_dir_all(&package_root).unwrap();

    let protected_dir = build_archive_cli_package(&package_root, "pkg-gc-protected");
    let protected_manifest = cdf_package::read_manifest(&protected_dir).unwrap();
    commit_status_head(
        &project,
        "pipeline-gc",
        "checkpoint-gc-protected",
        &protected_manifest.package_hash,
        "receipt-gc-protected",
        1_783_296_000_000,
    );
    commit_status_head(
        &project,
        "pipeline-gc-missing",
        "checkpoint-gc-missing",
        "sha256:missing-gc-package",
        "receipt-gc-missing",
        1_783_296_000_001,
    );

    let collectible_dir = package_root.join("pkg-gc-collectible");
    let collectible_builder =
        PackageBuilder::create(&collectible_dir, "pkg-gc-collectible").unwrap();
    let collectible_manifest = collectible_builder
        .finish_with_status(PackageStatus::Validated)
        .unwrap();

    let retained_dir = build_archive_cli_package(&package_root, "pkg-gc-retained");
    let retained_manifest = cdf_package::read_manifest(&retained_dir).unwrap();

    let corrupt_dir = build_archive_cli_package(&package_root, "pkg-gc-corrupt");
    let corrupt_manifest = cdf_package::read_manifest(&corrupt_dir).unwrap();
    fs::write(corrupt_dir.join("data/seg-000001.arrow"), "tampered").unwrap();

    let tombstone_dir = build_archive_cli_package(&package_root, "pkg-gc-tombstone");
    let tombstone_manifest = cdf_package::read_manifest(&tombstone_dir).unwrap();
    cdf_package::tombstone_package(&tombstone_dir).unwrap();

    fs::create_dir_all(package_root.join("pkg-gc-partial")).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "package",
        "gc",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "package gc");
    assert_eq!(json["result"]["command"], "package gc");
    assert_eq!(json["result"]["mode"], "dry_run");
    assert_eq!(json["result"]["counts"]["protected"], 2);
    assert_eq!(json["result"]["counts"]["collectible"], 1);
    assert_eq!(json["result"]["counts"]["retained"], 1);
    assert_eq!(json["result"]["counts"]["corrupt"], 2);
    assert_eq!(json["result"]["counts"]["missing"], 1);

    assert_gc_artifact(
        &json,
        Some(&protected_manifest.package_hash),
        "protected",
        "committed_checkpoint",
        "retain",
    );
    assert_gc_artifact(
        &json,
        Some(&collectible_manifest.package_hash),
        "collectible",
        "pre_packaged_artifact",
        "would_collect",
    );
    assert_gc_artifact(
        &json,
        Some(&retained_manifest.package_hash),
        "retained",
        "replay_or_recovery_artifact",
        "retain",
    );
    assert_gc_artifact(
        &json,
        Some(&corrupt_manifest.package_hash),
        "corrupt",
        "verification_failed",
        "retain",
    );
    assert_gc_artifact(
        &json,
        Some(&tombstone_manifest.package_hash),
        "protected",
        "retention_tombstone",
        "retain",
    );
    assert_gc_artifact(
        &json,
        Some("sha256:missing-gc-package"),
        "missing",
        "committed_checkpoint_missing_artifact",
        "restore_required",
    );
    assert_gc_artifact(&json, None, "corrupt", "manifest_missing", "retain");
}

#[test]
fn package_gc_explicit_directory_is_dry_run_without_deleting_collectible_artifacts() {
    let temp = TempDir::new("cdf-cli-package-gc-dry-run");
    let package_dir = temp.path().join("pkg-validated");
    let builder = PackageBuilder::create(&package_dir, "pkg-validated").unwrap();
    let manifest = builder
        .finish_with_status(PackageStatus::Validated)
        .unwrap();

    let result = run([
        "cdf",
        "--json",
        "package",
        "gc",
        temp.path().to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(package_dir.join("manifest.json").is_file());
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["counts"]["collectible"], 1);
    assert_gc_artifact(
        &json,
        Some(&manifest.package_hash),
        "collectible",
        "pre_packaged_artifact",
        "would_collect",
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
    assert!(human_result.stdout.contains("OK archived package sha256:"));
    assert!(human_result.stdout.contains("Archive"));
    assert!(human_result.stdout.contains("status     written"));
    assert!(human_result.stdout.contains("segments   1"));
    assert!(
        human_result
            .stdout
            .contains("archive/parquet/fidelity.json")
    );
    assert!(
        human_result
            .stdout
            .contains("-> cdf package verify <package>")
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

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "state",
        "show",
        "--pipeline",
        "pipeline-1",
        "--resource",
        "local.events",
    ]);

    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(!human.stdout.contains("\u{1b}["));
    for expected in [
        "WARN no committed state head",
        "Scope",
        "Head",
        "pipeline",
        "pipeline-1",
        "checkpoint",
        "none",
        "mutation performed",
        "-> cdf state history local.events --pipeline pipeline-1",
    ] {
        assert!(
            human.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human.stdout
        );
    }
}

#[test]
fn state_followup_commands_render_scope_pairs_for_scope_json_objects() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--scope-json",
        r#"{"kind":"window","start":"0","end":"10"}"#,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "-> cdf state history local.events",
        "--scope kind=window",
        "--scope start=0",
        "--scope end=10",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
    assert!(
        !result.stdout.contains("--scope-json"),
        "follow-up command should teach --scope pairs:\n{}",
        result.stdout
    );
}

#[test]
fn state_product_grammar_uses_default_pipeline_scope_pairs_and_rewind_marker() {
    let project = TestProject::new();
    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
        "--package-id",
        "pkg-state-product-first",
        "--checkpoint-id",
        "checkpoint-state-product-first",
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
        "--package-id",
        "pkg-state-product-second",
        "--checkpoint-id",
        "checkpoint-state-product-second",
    ]);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);

    let show = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--scope",
        "kind=resource",
    ]);

    assert_eq!(show.exit_code, 0, "stderr: {}", show.stderr);
    let show_json = stderr_or_stdout_json(&show.stdout);
    assert_eq!(show_json["result"]["scope"]["kind"], "resource");
    assert_eq!(
        show_json["result"]["head"]["delta"]["pipeline_id"],
        "cdf-run"
    );
    assert_eq!(
        show_json["result"]["head"]["delta"]["checkpoint_id"],
        "checkpoint-state-product-second"
    );

    let history = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "history",
        "local.events",
        "--scope",
        "kind=resource",
    ]);

    assert_eq!(history.exit_code, 0, "stderr: {}", history.stderr);
    let history_json = stderr_or_stdout_json(&history.stdout);
    assert_eq!(
        history_json["result"]["history"].as_array().unwrap().len(),
        2
    );

    let human_show = run([
        "cdf",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--scope",
        "kind=resource",
    ]);
    assert_eq!(human_show.exit_code, 0, "stderr: {}", human_show.stderr);
    for expected in [
        "OK state head found",
        "Scope",
        "Head",
        "checkpoint-state-product-second",
        "-> cdf state history local.events --scope kind=resource",
    ] {
        assert!(
            human_show.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human_show.stdout
        );
    }

    let human_history = run([
        "cdf",
        "--project",
        project.root_str(),
        "state",
        "history",
        "local.events",
        "--scope",
        "kind=resource",
    ]);
    assert_eq!(
        human_history.exit_code, 0,
        "stderr: {}",
        human_history.stderr
    );
    for expected in [
        "OK 2 checkpoint(s)",
        "| checkpoint",
        "checkpoint-state-product-first",
        "checkpoint-state-product-second",
        "-> cdf state show local.events --scope kind=resource",
    ] {
        assert!(
            human_history.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human_history.stdout
        );
    }

    let rewind = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "rewind",
        "local.events",
        "--scope",
        "kind=resource",
        "--to",
        "checkpoint-state-product-first",
    ]);

    assert_eq!(rewind.exit_code, 0, "stderr: {}", rewind.stderr);
    let rewind_json = stderr_or_stdout_json(&rewind.stdout);
    assert!(
        rewind_json["result"]["marker"]["delta"]["checkpoint_id"]
            .as_str()
            .unwrap()
            .starts_with("rewind-marker-")
    );
    assert_eq!(
        rewind_json["result"]["head"]["delta"]["checkpoint_id"],
        "checkpoint-state-product-first"
    );
    assert_eq!(
        rewind_json["result"]["packages_ahead"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn state_rewind_human_headless_render_reports_marker_and_packages_ahead() {
    let project = TestProject::new();
    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
        "--package-id",
        "pkg-state-human-first",
        "--checkpoint-id",
        "checkpoint-state-human-first",
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
        "--package-id",
        "pkg-state-human-second",
        "--checkpoint-id",
        "checkpoint-state-human-second",
    ]);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);

    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "state",
        "rewind",
        "local.events",
        "--scope",
        "kind=resource",
        "--to",
        "checkpoint-state-human-first",
        "--marker-checkpoint",
        "rewind-marker-human",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        "OK rewound to checkpoint-state-human-first",
        "Rewind",
        "marker              rewind-marker-human",
        "packages ahead      1",
        "rewind marker checkpoint appended",
        "| package ahead of state",
        "-> cdf state show local.events --scope kind=resource",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn state_migrate_initializes_sqlite_components_and_is_idempotent() {
    let project = TestProject::new();
    remove_state_store(&project);

    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "migrate",
    ]);

    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let first_json = stderr_or_stdout_json(&first.stdout);
    assert_eq!(first_json["command"], "state migrate");
    assert_eq!(first_json["result"]["applied_count"], 2);
    assert!(
        first_json["result"]["state_store_path"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/state.db")
    );
    let first_components = first_json["result"]["components"].as_array().unwrap();
    assert_eq!(first_components[0]["component"], "checkpoint_store");
    assert_eq!(first_components[0]["before_version"], Value::Null);
    assert_eq!(first_components[0]["after_version"], 1);
    assert_eq!(first_components[0]["target_version"], 1);
    assert_eq!(first_components[0]["applied"], true);
    assert_eq!(first_components[0]["action"], "initialized");
    assert_eq!(first_components[1]["component"], "run_ledger");
    assert_eq!(first_components[1]["before_version"], Value::Null);
    assert_eq!(first_components[1]["after_version"], 3);
    assert_eq!(first_components[1]["target_version"], 3);
    assert_eq!(first_components[1]["applied"], true);
    assert_eq!(first_components[1]["action"], "initialized");

    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "migrate",
    ]);

    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    let second_json = stderr_or_stdout_json(&second.stdout);
    assert_eq!(second_json["result"]["applied_count"], 0);
    let second_components = second_json["result"]["components"].as_array().unwrap();
    assert_eq!(second_components[0]["action"], "current");
    assert_eq!(second_components[0]["applied"], false);
    assert_eq!(second_components[1]["action"], "current");
    assert_eq!(second_components[1]["applied"], false);

    let human = run(["cdf", "--project", project.root_str(), "state", "migrate"]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    for expected in [
        "OK state migration checked 2 component(s)",
        "State store",
        "mutation performed  none; all SQLite state components were current",
        "| component",
        "checkpoint_store",
        "run_ledger",
    ] {
        assert!(
            human.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human.stdout
        );
    }
}

#[test]
fn state_show_human_rich_render_uses_scope_and_head_panels() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(
        &project,
        "pkg-state-show-rich",
        "checkpoint-state-show-rich",
    );
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);

    let output = crate::state_command::state(
        &test_cli(&project),
        crate::args::StateCommand::Show(crate::args::StateScopeArgs {
            pipeline_id: Some("pipeline-run".to_owned()),
            resource_id: "local.events".to_owned(),
            scope_json: None,
            scope: vec!["kind=resource".to_owned()],
        }),
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "\u{1b}[32m✓\u{1b}[0m state head found",
        "\u{1b}[36mScope\u{1b}[0m",
        "\u{1b}[36mHead\u{1b}[0m",
        "checkpoint  checkpoint-state-show-rich",
        "\u{1b}[36m→\u{1b}[0m cdf state history local.events --pipeline pipeline-run --scope kind=resource",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn state_recover_commits_verified_package_receipt_without_destination_rows() {
    let project = TestProject::new();
    let package_dir =
        create_replay_package_fixture(&project, "pkg-state-recover", "checkpoint-state-recover");
    let reader = PackageReader::open(&package_dir).unwrap();
    let package_hash = reader.manifest().package_hash.clone();
    let receipt_id = reader.receipts().unwrap()[0].receipt_id.to_string();
    let destination_path = project.root.join(".cdf/dev.duckdb");
    let rows_before = duckdb_event_count(&destination_path);

    let result = state_recover_command(
        &project,
        &package_dir,
        "duckdb://.cdf/dev.duckdb",
        None,
        None,
        None,
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "state recover");
    assert_eq!(report["command"], "state recover");
    assert_eq!(report["package_id"], "pkg-state-recover");
    assert_eq!(report["package_hash"], package_hash);
    assert_eq!(report["selected_receipt_id"], receipt_id);
    assert_eq!(report["receipt_selection"], "single_durable_receipt");
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["destination"]["destination_id"], "duckdb");
    assert_eq!(report["checkpoint_id"], "checkpoint-state-recover");
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["checkpoint"]["is_head"], true);
    assert_eq!(report["receipt_source"], "supplied_durable_receipt");
    assert_eq!(report["writes"]["destination_rows"], false);
    assert_eq!(report["writes"]["checkpoint"], true);
    assert!(
        report["evidence_limits"]
            .as_array()
            .unwrap()
            .iter()
            .any(|limit| limit.as_str().unwrap().contains("quarantine lineage"))
    );
    assert_eq!(duckdb_event_count(&destination_path), rows_before);
    assert_eq!(package_receipt_count(&package_dir), 1);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("state recover checkpoint head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        "checkpoint-state-recover"
    );
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.to_string(),
        receipt_id
    );
}

#[test]
fn state_recover_human_headless_render_reports_receipt_checkpoint_and_limits() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-state-recover-human",
        "checkpoint-state-recover-human",
    );

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "state".to_owned(),
        "recover".to_owned(),
        "--package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        "duckdb://.cdf/dev.duckdb".to_owned(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        "OK recovered checkpoint checkpoint-state-recover-human",
        "Recovery",
        "Checkpoint",
        "Writes",
        "destination rows  no",
        "verified receipt only; destination rows were not written",
        "| evidence limit",
        "does not reconstruct quarantine lineage",
        "-> cdf inspect package ",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn state_recover_explicit_receipt_disambiguates_multiple_package_receipts() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-state-recover-explicit",
        "checkpoint-state-recover-explicit",
    );
    let reader = PackageReader::open(&package_dir).unwrap();
    let mut receipts = reader.receipts().unwrap();
    let selected_receipt_id = receipts[0].receipt_id.to_string();
    receipts[0].receipt_id = ReceiptId::new("receipt-state-recover-extra").unwrap();
    reader.append_receipt(receipts[0].clone()).unwrap();
    let rows_before = duckdb_event_count(project.root.join(".cdf/dev.duckdb"));

    let result = state_recover_command(
        &project,
        &package_dir,
        "duckdb://.cdf/dev.duckdb",
        Some(&selected_receipt_id),
        None,
        None,
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["receipt_selection"], "explicit");
    assert_eq!(report["selected_receipt_id"], selected_receipt_id);
    assert_eq!(report["receipt_id"], selected_receipt_id);
    assert_eq!(
        duckdb_event_count(project.root.join(".cdf/dev.duckdb")),
        rows_before
    );
}

#[test]
fn state_recover_fails_closed_on_zero_or_ambiguous_package_receipts() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-state-recover-missing-receipt",
        "checkpoint-state-recover-missing-receipt",
    );
    remove_package_receipts(&package_dir);

    let missing = state_recover_command(
        &project,
        &package_dir,
        "duckdb://.cdf/dev.duckdb",
        None,
        None,
        None,
    );

    assert_eq!(missing.exit_code, 3);
    assert!(
        !project.root.join(".cdf/state.db").exists(),
        "missing receipt recovery must not create checkpoint state"
    );
    let missing_json = stderr_or_stdout_json(&missing.stderr);
    assert!(
        missing_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("found zero")
    );

    let ambiguous_project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &ambiguous_project,
        "pkg-state-recover-ambiguous-receipt",
        "checkpoint-state-recover-ambiguous-receipt",
    );
    let reader = PackageReader::open(&package_dir).unwrap();
    let mut duplicate = reader.receipts().unwrap()[0].clone();
    duplicate.receipt_id = ReceiptId::new("receipt-state-recover-ambiguous-extra").unwrap();
    reader.append_receipt(duplicate).unwrap();

    let ambiguous = state_recover_command(
        &ambiguous_project,
        &package_dir,
        "duckdb://.cdf/dev.duckdb",
        None,
        None,
        None,
    );

    assert_eq!(ambiguous.exit_code, 3);
    assert!(
        !ambiguous_project.root.join(".cdf/state.db").exists(),
        "ambiguous receipt recovery must not create checkpoint state"
    );
    let ambiguous_json = stderr_or_stdout_json(&ambiguous.stderr);
    assert!(
        ambiguous_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("pass --receipt")
    );
}

#[test]
fn migrated_command_family_errors_include_code_and_remediation() {
    let init = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "init".to_owned(),
        "--name".to_owned(),
        String::new(),
    ]);
    assert_json_error_code(&init, "CDF-PROJECT-INIT-ARGUMENT");

    let project = TestProject::new();
    let scan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--order-by",
        "id:sideways",
    ]);
    assert_json_error_code(&scan, "CDF-RUN-SCAN-ARGUMENT");

    let run_result = run(["cdf", "--json", "run"]);
    assert_json_error_code(&run_result, "CDF-RUN-ARGUMENT");

    let run_loop = run(["cdf", "--json", "run", "local.events", "--loop"]);
    let run_loop_json = assert_json_error_code(&run_loop, "CDF-RUN-LOOP-NOT-SUPPORTED");
    assert_eq!(run_loop_json["error"]["not_supported"], true);

    let replay_project = TestProject::new();
    let package_dir = create_replay_package_fixture(
        &replay_project,
        "pkg-replay-error-code",
        "checkpoint-replay-error-code",
    );
    let replay = replay_package_command_with_postgres_options(
        &replay_project,
        &package_dir,
        "postgres://localhost/db",
        Some("public.events"),
        Some("later"),
    );
    assert_json_error_code(&replay, "CDF-PACKAGE-REPLAY-ARGUMENT");

    let package = run([
        "cdf", "--json", "package", "archive", ".", "--format", "json",
    ]);
    assert_json_error_code(&package, "CDF-PACKAGE-ARGUMENT");

    let state = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--scope",
        "bad",
    ]);
    assert_json_error_code(&state, "CDF-STATE-SCOPE-ARGUMENT");

    let sql = run(["cdf", "--json", "sql", "delete from packages"]);
    assert_json_error_code(&sql, "CDF-SQL-QUERY");
}

#[test]
fn unknown_command_returns_usage_exit_code() {
    let result = run(["cdf", "--json", "bogus"]);

    assert_eq!(result.exit_code, 2);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(json["error"]["message"].as_str().unwrap().contains("bogus"));
    assert_eq!(json["error"]["exit_code"], 2);
    assert_eq!(json["error"]["not_supported"], false);
    assert_eq!(json["error"]["code"], "CDF-CLI-USAGE");
    assert_eq!(
        json["error"]["remediation"]["summary"],
        "Correct the command arguments and run the command again."
    );
    assert!(json["error"]["remediation"]["steps"].is_array());
    assert!(json["error"]["suggestions"].is_null());
}

#[test]
fn unknown_command_and_subcommand_json_suggest_high_confidence_matches() {
    let command = run(["cdf", "--json", "staus"]);

    assert_eq!(command.exit_code, 2);
    let json = assert_json_error_code(&command, "CDF-CLI-USAGE");
    assert_eq!(json["error"]["suggestions"], json!(["cdf status"]));

    let subcommand = run(["cdf", "--json", "inspect", "resorce"]);

    assert_eq!(subcommand.exit_code, 2);
    let json = assert_json_error_code(&subcommand, "CDF-CLI-USAGE");
    assert_eq!(
        json["error"]["suggestions"],
        json!(["cdf inspect resource"])
    );
}

#[test]
fn unknown_resource_json_suggests_nearest_configured_resource_id() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "resource",
        "local.eventz",
    ]);

    assert_eq!(result.exit_code, 3, "stderr: {}", result.stderr);
    let json = assert_json_error_code(&result, "CDF-PROJECT-CONTRACT");
    assert_eq!(json["error"]["suggestions"], json!(["local.events"]));
}

#[test]
fn unknown_resource_json_omits_suggestions_without_inventory() {
    let project = TestProject::new();
    fs::write(
        project.root.join("cdf.toml"),
        r#"
[project]
name = "cli_test"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"
"#,
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "resource",
        "local.eventz",
    ]);

    assert_eq!(result.exit_code, 3, "stderr: {}", result.stderr);
    let json = assert_json_error_code(&result, "CDF-PROJECT-CONTRACT");
    assert!(json["error"]["suggestions"].is_null());
}

#[test]
fn unknown_destination_json_suggests_environment_or_uri_shape_without_secrets() {
    let project = TestProject::new();
    fs::write(
        project.root.join("cdf.toml"),
        r#"
[project]
name = "cli_test"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[environments.prod]
destination = "duckdb://.cdf/prod.duckdb"

[resources."local.*"]
source = "resources/files.toml"
"#,
    )
    .unwrap();

    let typo = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--to",
        "prd",
    ]);

    assert_eq!(typo.exit_code, 78, "stderr: {}", typo.stderr);
    let json = assert_json_error_code(&typo, "CDF-DEST-NOT-SUPPORTED");
    assert_eq!(
        json["error"]["suggestions"],
        json!(["--env prod", "duckdb://path", "parquet://root"])
    );

    let package_dir = create_replay_package_fixture(
        &project,
        "pkg-replay-redacted-destination-suggestion",
        "checkpoint-replay-redacted-destination-suggestion",
    );
    let redacted = replay_package_command(
        &project,
        &package_dir,
        "dckdb://user:destination-secret@localhost/db",
    );

    assert_eq!(redacted.exit_code, 78, "stderr: {}", redacted.stderr);
    assert_secret_absent(&redacted, "destination-secret");
    let json = assert_json_error_code(&redacted, "CDF-DEST-NOT-SUPPORTED");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("dckdb://[redacted]@localhost/db")
    );
    assert_eq!(
        json["error"]["suggestions"],
        json!([
            "duckdb://path",
            "parquet://root",
            "postgres://secret://env/NAME"
        ])
    );
}

#[test]
fn usage_error_human_output_keeps_message_and_adds_remediation() {
    let result = run(["cdf", "sql"]);

    assert_eq!(result.exit_code, 2);
    assert!(result.stderr.contains("error: sql requires a query string"));
    assert!(result.stderr.contains("remediation:"));
}

#[test]
fn not_supported_error_preserves_exit_code_and_json_compatibility() {
    let error =
        crate::output::CliError::not_supported("preview", "query resources", "native scan runtime");
    let result = crate::output::InvocationResult::from_error(true, error);

    assert_eq!(result.exit_code, 78);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["ok"], false);
    assert_eq!(json["error"]["kind"], "internal");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("preview")
    );
    assert_eq!(json["error"]["exit_code"], 78);
    assert_eq!(json["error"]["not_supported"], true);
    assert_eq!(json["error"]["code"], "CDF-CLI-NOT-SUPPORTED");
    assert_eq!(
        json["error"]["remediation"]["summary"],
        "Use a currently supported path or wait for the named lower layer to land."
    );
}

#[test]
fn generic_lower_layer_conversion_uses_documented_mapping() {
    let error = crate::output::CliError::from(CdfError::destination("destination refused commit"));
    let result = crate::output::InvocationResult::from_error(true, error);

    assert_eq!(result.exit_code, 6);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "destination");
    assert_eq!(json["error"]["message"], "destination refused commit");
    assert_eq!(json["error"]["exit_code"], 6);
    assert_eq!(json["error"]["not_supported"], false);
    assert_eq!(json["error"]["code"], "CDF-DEST-ERROR");
    assert_eq!(
        json["error"]["remediation"]["summary"],
        "Inspect the destination URI, target, policy, and destination health."
    );
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
                "{\"id\":1,\"updated_at\":1783296000000000}\n",
                "{\"id\":2,\"updated_at\":1783296060000000}\n"
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
    for suffix in ["", "-wal", "-shm"] {
        assert!(
            !project.root.join(format!(".cdf/state.db{suffix}")).exists(),
            "preview must not create checkpoint/run-ledger state{}",
            suffix
        );
    }
    assert!(
        !project.root.join(".cdf/dev.duckdb").exists(),
        "preview must not create destination DB"
    );
    assert!(
        !project.root.join(".cdf/parquet").exists(),
        "preview must not create destination root"
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
    run_valid_run_resource_target(project, resource_id, package_id, checkpoint_id, "events")
}

fn run_valid_run_resource_target(
    project: &TestProject,
    resource_id: &str,
    package_id: &str,
    checkpoint_id: &str,
    target: &str,
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
        target.to_owned(),
        "--package-id".to_owned(),
        package_id.to_owned(),
        "--checkpoint-id".to_owned(),
        checkpoint_id.to_owned(),
    ])
}

fn run_valid_run_target(
    project: &TestProject,
    package_id: &str,
    checkpoint_id: &str,
    target: &str,
) -> crate::InvocationResult {
    run_dynamic(vec![
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
        target.to_owned(),
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

fn create_replay_package_fixture_with_target(
    project: &TestProject,
    package_id: &str,
    checkpoint_id: &str,
    target: &str,
) -> PathBuf {
    let result = run_valid_run_target(project, package_id, checkpoint_id, target);
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
    replay_package_command_with_postgres_options(project, package_dir, destination_uri, None, None)
}

fn replay_package_command_with_postgres_options(
    project: &TestProject,
    package_dir: &Path,
    destination_uri: &str,
    target: Option<&str>,
    merge_dedup: Option<&str>,
) -> crate::InvocationResult {
    let mut command = vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "replay".to_owned(),
        "package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        destination_uri.to_owned(),
    ];
    if let Some(target) = target {
        command.push("--target".to_owned());
        command.push(target.to_owned());
    }
    if let Some(merge_dedup) = merge_dedup {
        command.push("--merge-dedup".to_owned());
        command.push(merge_dedup.to_owned());
    }
    run_dynamic(command)
}

fn state_recover_command(
    project: &TestProject,
    package_dir: &Path,
    destination_uri: &str,
    receipt_id: Option<&str>,
    target: Option<&str>,
    merge_dedup: Option<&str>,
) -> crate::InvocationResult {
    let mut command = vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "state".to_owned(),
        "recover".to_owned(),
        "--package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        destination_uri.to_owned(),
    ];
    if let Some(receipt_id) = receipt_id {
        command.push("--receipt".to_owned());
        command.push(receipt_id.to_owned());
    }
    if let Some(target) = target {
        command.push("--target".to_owned());
        command.push(target.to_owned());
    }
    if let Some(merge_dedup) = merge_dedup {
        command.push("--merge-dedup".to_owned());
        command.push(merge_dedup.to_owned());
    }
    run_dynamic(command)
}

fn duckdb_event_count(path: impl AsRef<Path>) -> i64 {
    let conn = DuckConnection::open(path).unwrap();
    conn.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap()
}

fn resume_command(
    project: &TestProject,
    run_id: &str,
    use_run_flag: bool,
) -> crate::InvocationResult {
    let mut command = vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "resume".to_owned(),
    ];
    if use_run_flag {
        command.push("--run".to_owned());
    }
    command.push(run_id.to_owned());
    run_dynamic(command)
}

fn create_resume_run_with_events(
    project: &TestProject,
    run_id: &str,
    kinds: &[RunEventKind],
) -> RunId {
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let run_id = RunId::new(run_id).unwrap();
    ledger.create_run(Some(run_id.clone())).unwrap();
    for kind in kinds {
        ledger
            .append_event(&run_id, RunEventAppend::new(*kind))
            .unwrap();
    }
    run_id
}

fn create_resume_run_with_package(
    project: &TestProject,
    run_id: &str,
    package_dir: &Path,
    kinds: &[RunEventKind],
) -> RunId {
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let run_id = RunId::new(run_id).unwrap();
    ledger.create_run(Some(run_id.clone())).unwrap();
    for kind in kinds {
        let event = resume_package_event(*kind, package_dir);
        ledger.append_event(&run_id, event).unwrap();
    }
    run_id
}

fn create_resume_run_with_missing_package(
    project: &TestProject,
    run_id: &str,
    package_dir: &Path,
) -> RunId {
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let run_id = RunId::new(run_id).unwrap();
    ledger.create_run(Some(run_id.clone())).unwrap();
    let mut event = RunEventAppend::new(RunEventKind::PackageFinalized);
    event.package_id = Some("pkg-resume-missing".to_owned());
    event.package_path = Some(package_dir.display().to_string());
    ledger.append_event(&run_id, event).unwrap();
    run_id
}

fn seed_resume_receipt_before_checkpoint(
    project: &TestProject,
    package_dir: &Path,
    run_id: &str,
) -> RunId {
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let run_id = RunId::new(run_id).unwrap();
    ledger.create_run(Some(run_id.clone())).unwrap();
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let destination = DuckDbDestination::new(project.root.join(".cdf/dev.duckdb")).unwrap();
    let target = PackageReader::open(package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .destination_commit
        .target;
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before resume checkpoint"));
    let error = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.to_path_buf(),
        destination: ResolvedProjectDestination::new(Box::new(destination), target),
        checkpoint_store: &store,
        after_receipt_verified: Some(&hook),
    })
    .unwrap_err();
    assert!(error.to_string().contains("stop before resume checkpoint"));
    let reader = PackageReader::open(package_dir).unwrap();
    assert_eq!(reader.receipts().unwrap().len(), 1);
    assert_eq!(reader.manifest().lifecycle.status, PackageStatus::Loading);
    let inputs = reader.replay_inputs().unwrap();
    let history = store
        .history(
            &inputs.state_delta.pipeline_id,
            &inputs.state_delta.resource_id,
            &inputs.state_delta.scope,
        )
        .unwrap();
    assert!(history.iter().any(|checkpoint| {
        checkpoint.delta.checkpoint_id == inputs.state_delta.checkpoint_id
            && checkpoint.status == CheckpointStatus::Proposed
    }));
    for kind in [
        RunEventKind::PackageFinalized,
        RunEventKind::CheckpointProposed,
        RunEventKind::DestinationReceiptRecorded,
        RunEventKind::RunFailed,
    ] {
        let event = resume_package_event(kind, package_dir);
        ledger.append_event(&run_id, event).unwrap();
    }
    run_id
}

fn resume_package_event(kind: RunEventKind, package_dir: &Path) -> RunEventAppend {
    let reader = PackageReader::open(package_dir).unwrap();
    let inputs = reader.replay_inputs().unwrap();
    let receipts = reader.receipts().unwrap();
    let receipt = receipts.last();
    let mut event = RunEventAppend::new(kind);
    event.resource_id = Some(inputs.state_delta.resource_id.clone());
    event.scope = Some(inputs.state_delta.scope.clone());
    event.package_id = Some(reader.manifest().identity.package_id.clone());
    event.package_hash = Some(PackageHash::new(reader.manifest().package_hash.clone()).unwrap());
    event.package_path = Some(package_dir.display().to_string());
    event.checkpoint_id = Some(inputs.state_delta.checkpoint_id.clone());
    if matches!(
        kind,
        RunEventKind::DestinationReceiptRecorded | RunEventKind::CheckpointCommitted
    ) && let Some(receipt) = receipt
    {
        event.receipt_id = Some(receipt.receipt_id.clone());
        event.destination_id = Some(receipt.destination.clone());
    }
    event
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

fn remove_package_receipts(package_dir: &Path) {
    let path = package_dir.join(cdf_package::RECEIPTS_FILE);
    if path.exists() {
        fs::remove_file(path).unwrap();
    }
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
            "cdf_cli_live_{}_{}",
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
        let data_dir = TempDir::new("cdf-cli-postgres-data");
        let socket_dir = TempDir::new_short("cdfpgs");
        let port = free_port();

        let init_status = Command::new(&initdb)
            .args(["-D", data_dir.path().to_str().unwrap()])
            .args(["-A", "trust"])
            .args(["-U", "cdf"])
            .arg("--no-sync")
            .status()
            .unwrap();
        assert!(init_status.success(), "initdb failed");

        let socket_path = socket_dir.path().canonicalize().unwrap();
        let options = format!("-h 127.0.0.1 -p {port} -k {}", socket_path.display());
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

fn write_project_destination_with_postgres_policy(
    project: &TestProject,
    destination: &str,
    merge_dedup: &str,
) {
    let project_text = PROJECT.replace(
        "destination = \"duckdb://.cdf/dev.duckdb\"",
        &format!(
            "destination = \"{destination}\"\n\n[environments.dev.destination_policy.postgres]\nmerge_dedup = \"{merge_dedup}\""
        ),
    );
    fs::write(project.root.join("cdf.toml"), project_text).unwrap();
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

fn write_resource_disposition(project: &TestProject, disposition: &str) {
    fs::write(
        project.root.join("resources/files.toml"),
        RESOURCE.replace(
            "write_disposition = \"append\"",
            &format!("write_disposition = \"{disposition}\""),
        ),
    )
    .unwrap();
}

fn write_resource_with_extra_contract_field(project: &TestProject) {
    fs::write(
        project.root.join("resources/files.toml"),
        RESOURCE.replace(
            "  { name = \"updated_at\", type = \"int64\", nullable = false },",
            concat!(
                "  { name = \"updated_at\", type = \"int64\", nullable = false },\n",
                "  { name = \"ingested_at\", type = \"int64\", nullable = true },"
            ),
        ),
    )
    .unwrap();
}

fn write_format_fixture(project: &TestProject, format: &str) {
    for entry in fs::read_dir(project.root.join("data")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }

    let extension = match format {
        "arrow_ipc" => "arrow",
        other => other,
    };
    let glob = format!("events.{extension}");
    let resource = RESOURCE
        .replace("glob = \"*.ndjson\"", &format!("glob = \"{glob}\""))
        .replace("format = \"ndjson\"", &format!("format = \"{format}\""));
    fs::write(project.root.join("resources/files.toml"), resource).unwrap();

    match format {
        "csv" => fs::write(
            project.root.join("data/events.csv"),
            "id,updated_at\n1,1783296000000000\n2,1783296060000000\n",
        )
        .unwrap(),
        "json" => fs::write(
            project.root.join("data/events.json"),
            r#"[{"id":1,"updated_at":1783296000000000},{"id":2,"updated_at":1783296060000000}]"#,
        )
        .unwrap(),
        "parquet" => write_parquet_preview_fixture(project),
        "arrow_ipc" => write_arrow_ipc_preview_fixture(project),
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

fn write_arrow_ipc_preview_fixture(project: &TestProject) {
    let temp = TempDir::new("cdf-cli-preview-arrow-ipc-source");
    let package_dir = temp.path().join("pkg-preview-arrow-ipc-source");
    let mut builder = PackageBuilder::create(&package_dir, "pkg-preview-arrow-ipc-source").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
            Arc::new(Int64Array::from(vec![
                1_783_296_000_000_000_i64,
                1_783_296_060_000_000_i64,
            ])),
        ],
    )
    .unwrap();
    builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), &[batch])
        .unwrap();
    builder.finish_with_status(PackageStatus::Packaged).unwrap();
    fs::copy(
        package_dir.join("data/seg-000001.arrow"),
        project.root.join("data/events.arrow"),
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

fn initialize_status_state(project: &TestProject) {
    SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
}

fn write_status_package(project: &TestProject, package_id: &str) -> (PathBuf, String) {
    let package_dir = project.root.join(".cdf/packages").join(package_id);
    fs::create_dir_all(project.root.join(".cdf/packages")).unwrap();
    let builder = PackageBuilder::create(&package_dir, package_id).unwrap();
    let manifest = builder
        .finish_with_status(PackageStatus::Checkpointed)
        .unwrap();
    (package_dir, manifest.package_hash)
}

fn write_status_package_receipt(
    project: &TestProject,
    package_id: &str,
    receipt_id: &str,
    committed_at_ms: i64,
) -> (PathBuf, String) {
    let (package_dir, package_hash) = write_status_package(project, package_id);
    PackageReader::open(&package_dir)
        .unwrap()
        .append_receipt(status_receipt(&package_hash, receipt_id, committed_at_ms))
        .unwrap();
    (package_dir, package_hash)
}

fn record_status_receipt_event(
    project: &TestProject,
    run_id: &str,
    package_dir: &Path,
    package_hash: &str,
    receipt_id: &str,
) {
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let run_id = RunId::new(run_id).unwrap();
    ledger.create_run(Some(run_id.clone())).unwrap();
    let mut event = RunEventAppend::new(RunEventKind::DestinationReceiptRecorded);
    event.resource_id = Some(ResourceId::new("local.events").unwrap());
    event.scope = Some(ScopeKey::Resource);
    event.package_id = package_dir
        .file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned);
    event.package_hash = Some(PackageHash::new(package_hash).unwrap());
    event.package_path = Some(package_dir.display().to_string());
    event.receipt_id = Some(ReceiptId::new(receipt_id).unwrap());
    event.destination_id = Some(DestinationId::new("local-test").unwrap());
    ledger.append_event(&run_id, event).unwrap();
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

fn write_rest_project(project: &TestProject, destination: &str, base_url: &str, token: &str) {
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

[resources."api.*"]
source = "resources/api.toml"
"#
        ),
    )
    .unwrap();
    fs::write(
        project.root.join("resources/api.toml"),
        rest_resource_with_base_url(base_url, token),
    )
    .unwrap();
}

fn rest_resource(token: &str) -> String {
    rest_resource_with_base_url("https://api.example.test", token)
}

fn rest_resource_with_base_url(base_url: &str, token: &str) -> String {
    format!(
        r#"
[source.api]
kind = "rest"
base_url = "{base_url}"
auth = {{ kind = "bearer", token = "{token}" }}

[resource.items]
path = "/items"
records = "$.items"
primary_key = ["id"]
cursor = {{ field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }}
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
  {{ name = "updated_at", type = "int64", nullable = false }},
] }}
"#
    )
}

fn rest_resource_with_exact_cursor_base_url(base_url: &str, token: &str) -> String {
    rest_resource_with_base_url(base_url, token).replace(
        r#"cursor = { field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }"#,
        r#"cursor = { field = "updated_at", param = "since", ordering = "exact", lag = "0ms", filter_fidelity = "exact" }"#,
    )
}

fn serve_json_once(body: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let body = body.to_owned();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).unwrap();
        stream.flush().unwrap();
    });
    format!("http://{address}")
}

fn serve_json_once_capturing_request(body: &str) -> (String, Arc<Mutex<Option<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let request_text = Arc::new(Mutex::new(None));
    let request_for_thread = Arc::clone(&request_text);
    let body = body.to_owned();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 4096];
        let bytes_read = stream.read(&mut request).unwrap_or(0);
        *request_for_thread.lock().unwrap() =
            Some(String::from_utf8_lossy(&request[..bytes_read]).into_owned());
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).unwrap();
        stream.flush().unwrap();
    });
    (format!("http://{address}"), request_text)
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

fn sql_resource_with_ordered_cursor(connection: &str, table: &str) -> String {
    format!(
        r#"
[source.warehouse]
kind = "sql"
connection = "{connection}"
dialect = "postgres"

[resource.orders]
table = "{table}"
primary_key = ["id"]
cursor = {{ field = "updated_at", ordering = "exact", lag = "0ms" }}
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
  {{ name = "updated_at", type = "int64", nullable = false }},
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

    fn new_short(prefix: &str) -> Self {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let parent = PathBuf::from("/tmp");
        let path = parent.join(format!(
            "{prefix}-{}-{counter}-{unique}",
            std::process::id()
        ));
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

fn render_rich(output: crate::output::CommandOutput) -> crate::InvocationResult {
    crate::output::InvocationResult::from_output(false, &rich_render_config(), output)
}

fn rich_render_config() -> crate::render::RenderConfig {
    crate::render::RenderConfig::new(
        crate::render::config::DisplayMode::Tty,
        96,
        crate::render::config::RenderEnv {
            no_color: false,
            clicolor_force: false,
        },
        false,
    )
}

fn test_cli(project: &TestProject) -> crate::args::Cli {
    crate::args::Cli {
        json: false,
        no_color: false,
        project: Some(project.root.clone()),
        env: None,
        command: crate::args::Command::Version,
    }
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

fn assert_json_error_code(result: &crate::InvocationResult, code: &str) -> Value {
    assert_ne!(result.exit_code, 0, "expected error result");
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["ok"], false);
    assert_eq!(json["error"]["code"], code);
    assert!(
        json["error"]["remediation"]["summary"]
            .as_str()
            .is_some_and(|summary| !summary.is_empty()),
        "missing remediation summary for {code}: {}",
        result.stderr
    );
    json
}

fn assert_gc_artifact(
    json: &Value,
    package_hash: Option<&str>,
    classification: &str,
    retention_reason: &str,
    planned_action: &str,
) {
    let artifact = json["result"]["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|artifact| {
            artifact["package_hash"].as_str() == package_hash
                && artifact["classification"] == classification
                && artifact["retention_reason"] == retention_reason
        })
        .unwrap_or_else(|| {
            panic!(
                "missing gc artifact hash={package_hash:?} classification={classification} reason={retention_reason}: {}",
                json["result"]["artifacts"]
            )
        });
    assert_eq!(artifact["planned_action"], planned_action);
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
