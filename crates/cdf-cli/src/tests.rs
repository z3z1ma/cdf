use std::{
    collections::{BTreeMap, HashMap},
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

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::{FileWriter, StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{
    RESIDUAL_ENCODING_METADATA_KEY, RESIDUAL_ENCODING_NAME, VARIANT_COLUMN_NAME,
    VARIANT_SEMANTIC_TAG,
};
use cdf_dest_duckdb::DuckDbDestination;
use cdf_dest_parquet::ParquetDestination;
use cdf_engine::{
    CompiledStreamAdmissionEvidence, EnginePlanInput, Planner, StreamAdmissionObservationEvidence,
};
use cdf_kernel::ExecutionExtent;
use cdf_kernel::{
    BatchStream, CHECKPOINT_STATE_VERSION, CdfError, CheckpointId, CheckpointStatus,
    CheckpointStore, CommitCounts, CursorPosition, CursorValue, DestinationId, FileManifest,
    FilePosition, IdempotencyToken, LeaseOwnerId, PackageHash, PartitionId, PartitionPlan,
    PipelineId, PromotionSettlementStore, Receipt, ReceiptId, ResourceDescriptor, ResourceId,
    ResourceStream, RunId, ScanRequest, SchemaHash, SchemaSnapshotReference, SchemaSource,
    ScopeKey, SegmentAck, SegmentId, SourcePosition, StateDelta, StateSegment,
    TableSnapshotPosition, TableSnapshotSelector, TargetName, TrustLevel, VerifyClause,
    WriteDisposition, with_semantic,
};
use cdf_package::{PackageBuilder, PackageReader};
use cdf_package_contract::{
    DestinationCommitPlanPreimage, MANIFEST_FILE, PackageStatus, RECEIPTS_FILE, SegmentEntry,
    StateDeltaPreimage,
};
use cdf_project::{
    DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS, PackageArtifactReplayRequest,
    ResolvedProjectDestination, STRATIFIED_HASH_SELECTOR_V1, SchemaPromotionExecutionFailpoint,
    SchemaPromotionExecutionPhase, SchemaPromotionExecutionRequest, SchemaPromotionPlanReport,
    execute_schema_promotion, load_schema_promotion_recovery_status, parse_lock,
    replay_package_from_artifacts,
};
use cdf_state_sqlite::{
    RunEventAppend, RunEventDetails, RunEventKind, RunEventValue, SecretReference,
    SqliteCheckpointStore, SqlitePromotionSettlementStore, SqliteRunLedger,
};
use duckdb::Connection as DuckConnection;
use flate2::{Compression, write::GzEncoder};
use postgres::{Client, NoTls};
use rusqlite::Connection;
use serde_json::Value;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::invoke;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
static LIVE_POSTGRES_SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);
static LOCAL_POSTGRES_START: Mutex<()> = Mutex::new(());

macro_rules! package_builder {
    ($path:expr, $package_id:expr $(,)?) => {
        PackageBuilder::create(
            $path,
            $package_id,
            cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
                .unwrap(),
        )
    };
}

fn collect_package_receipts(reader: &PackageReader) -> Vec<Receipt> {
    let mut receipts = Vec::new();
    reader
        .for_each_receipt(&mut |receipt| {
            receipts.push(receipt);
            Ok(())
        })
        .unwrap();
    receipts
}

fn test_execution_services() -> cdf_runtime::ExecutionServices {
    let services = cdf_engine::StandaloneExecutionHost::default_services(512 * 1024 * 1024)
        .unwrap()
        .1;
    let scopes: Arc<dyn cdf_kernel::ScopeLeaseStore> =
        Arc::new(cdf_state_sqlite::InMemoryScopeLeaseStore::new());
    let services = services
        .with_staging_lease_authority(Arc::new(cdf_runtime::ScopeStagingLeaseAuthority::new(
            scopes,
        )))
        .unwrap();
    services.with_content_reachability_store(Arc::new(
        cdf_state_sqlite::SqliteContentReachabilityStore::open_in_memory().unwrap(),
    ))
}

fn test_destination_registry() -> cdf_runtime::DestinationRegistry {
    crate::destination_registry::builtin_destination_registry().unwrap()
}

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
        "help", "version", "init", "add", "validate", "plan", "explain", "run", "preview", "sql",
        "inspect", "diff", "schema", "contract", "state", "resume", "replay", "backfill",
        "package", "doctor", "status",
    ] {
        assert!(result.stdout.contains(command), "missing {command}");
    }
    for required in [
        "--progress <WHEN>",
        "--unicode <WHEN>",
        "Environment:",
        "Examples:",
    ] {
        assert!(result.stdout.contains(required), "missing {required}");
    }
}

#[test]
fn cx1_short_and_long_help_are_distinct_complete_and_placeholder_free() {
    let short = run(["cdf", "-h"]);
    assert_eq!(short.exit_code, 0);
    assert!(short.stdout.contains("--progress"));
    assert!(short.stdout.contains("--unicode"));
    assert!(!short.stdout.contains("Environment:"));

    let long = run(["cdf", "help"]);
    assert_eq!(long.exit_code, 0);
    for required in ["--progress", "--unicode", "Environment:", "Examples:"] {
        assert!(long.stdout.contains(required), "missing {required}");
    }

    let recover = run(["cdf", "state", "recover", "--help"]);
    for required in [
        "Package directory",
        "Receipt identifier",
        "Merge deduplication policy",
    ] {
        assert!(recover.stdout.contains(required), "missing {required}");
    }
    assert!(!recover.stdout.contains("Command option"));
    assert!(!recover.stdout.contains("Command value"));
}

#[test]
fn parser_provides_subcommand_help_at_nested_layers() {
    let validate = run(["cdf", "validate", "--help"]);

    assert_eq!(validate.exit_code, 0);
    assert!(validate.stdout.contains("Usage: cdf validate"));
    assert!(validate.stdout.contains("--deep"));

    let add = run(["cdf", "add", "--help"]);

    assert_eq!(add.exit_code, 0);
    assert!(add.stdout.contains("Usage: cdf add"));
    assert!(add.stdout.contains("RESOURCE_ID"));
    assert!(add.stdout.contains("URL_OR_PATH"));
    assert!(add.stdout.contains("--dry-run"));

    let plan = run(["cdf", "plan", "--help"]);

    assert_eq!(plan.exit_code, 0);
    assert!(plan.stdout.contains("Usage: cdf plan"));
    assert!(plan.stdout.contains("[RESOURCE]"));
    assert!(plan.stdout.contains("--to <DEST>"));
    assert!(!plan.stdout.contains("--resource"));
    assert!(!plan.stdout.contains("--target"));

    let schema = run(["cdf", "schema", "discover", "--help"]);

    assert_eq!(schema.exit_code, 0);
    assert!(schema.stdout.contains("Usage: cdf schema discover"));
    assert!(schema.stdout.contains("[RESOURCE]"));
    assert!(!schema.stdout.contains("--resource"));

    for subcommand in ["pin", "show", "diff", "promote"] {
        let result = run(["cdf", "schema", subcommand, "--help"]);

        assert_eq!(result.exit_code, 0);
        assert!(
            result
                .stdout
                .contains(&format!("Usage: cdf schema {subcommand}"))
        );
        assert!(result.stdout.contains("[RESOURCE]"));
        assert!(!result.stdout.contains("--resource"));
        if subcommand == "promote" {
            assert!(result.stdout.contains("--type <JSON_POINTER=ARROW_TYPE>"));
            assert!(result.stdout.contains("--execute"));
        }
    }

    let rewind = run(["cdf", "state", "rewind", "--help"]);

    assert_eq!(rewind.exit_code, 0);
    assert!(rewind.stdout.contains("Usage: cdf state rewind"));
    assert!(rewind.stdout.contains("--scope <KEY=VALUE>"));
    assert!(rewind.stdout.contains("--to <CHECKPOINT>"));
    assert!(!rewind.stdout.contains("--target-checkpoint"));
    assert!(!rewind.stdout.contains("--marker-checkpoint"));

    let state = run(["cdf", "state", "--help"]);

    assert_eq!(state.exit_code, 0);
    assert!(state.stdout.contains("show"));
    assert!(state.stdout.contains("recover"));
    assert!(!state.stdout.contains("migrate"));
}

#[test]
fn state_migrate_is_absent_until_a_supported_predecessor_exists() {
    let result = run(["cdf", "--json", "state", "migrate"]);

    assert_eq!(result.exit_code, 2);
    let json = assert_json_error_code(&result, "CDF-CLI-USAGE");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unrecognized subcommand 'migrate'")
    );
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

#[test]
fn progress_enabled_human_commands_route_through_progress_renderer() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let expectations: &[(&str, &[&str])] = &[
        (
            "run_command.rs",
            &[
                "let progress = human_progress_sink(cli.json, &cli.terminal);",
                "let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);",
                "event_sink,",
                "error.with_progress(progress.snapshot())",
                "Some(progress) => CommandOutput::rendered_with_progress(",
            ],
        ),
        (
            "replay_command.rs",
            &[
                "let progress = human_progress_sink(cli.json, &cli.terminal);",
                "let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);",
                "ReplayProgressRecorder::new(",
                "error.with_progress(progress.snapshot())",
                "CommandOutput::rendered_with_progress(",
            ],
        ),
        (
            "resume_command.rs",
            &[
                "let progress = human_progress_sink(json_mode, terminal);",
                "let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);",
                "sink.try_emit(event)",
                "ResumeAttempt::new(",
                "destinations,",
                "finish_resume_report(report, progress.map(|progress| progress.snapshot()))",
            ],
        ),
        (
            "resume_command/report.rs",
            &["CommandOutput::rendered_with_progress_and_exit_code("],
        ),
        (
            "backfill_command.rs",
            &[
                "let progress = human_progress_sink(cli.json, &cli.terminal);",
                "let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);",
                "BackfillSliceExecutor {",
                "executor.execute(slice)",
                "destinations,",
                "progress.as_ref().map(|progress| progress.snapshot())",
                "CommandOutput::rendered_with_progress(",
            ],
        ),
    ];

    for (relative, patterns) in expectations {
        let text = fs::read_to_string(src.join(relative)).unwrap();
        for pattern in *patterns {
            assert!(
                text.contains(pattern),
                "{relative} no longer routes human progress through `{pattern}`"
            );
        }
    }
}

#[test]
fn destination_registry_composition_is_confined_to_the_cli_root() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = manifest_dir.join("src");
    let mut files = Vec::new();
    collect_rust_files(&src, &mut files);
    let mut violations = Vec::new();

    for path in files {
        let relative = path.strip_prefix(manifest_dir).unwrap();
        let relative_text = relative.to_string_lossy();
        if relative_text == "src/tests.rs"
            || relative_text == "src/destination_registry_test_support.rs"
        {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap();
        let concrete_driver_import = text.contains("cdf_dest_");
        let concrete_driver_allowed = matches!(
            relative_text.as_ref(),
            "src/destination_registry.rs" | "src/doctor_drift.rs"
        );
        if concrete_driver_import && !concrete_driver_allowed {
            violations.push(format!(
                "{relative_text} imports a concrete destination outside the composition root"
            ));
        }
        if text.contains("builtin_destination_registry()")
            && relative_text != "src/destination_registry.rs"
            && relative_text != "src/lib.rs"
        {
            violations.push(format!(
                "{relative_text} reconstructs the builtin destination registry below the invocation root"
            ));
        }
    }

    let lib = fs::read_to_string(src.join("lib.rs")).unwrap();
    assert_eq!(
        lib.matches("destination_registry::builtin_destination_registry()")
            .count(),
        1,
        "production invocation must construct the builtin destination registry exactly once"
    );
    assert!(
        violations.is_empty(),
        "destination composition boundary regressed:\n{}",
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
    assert!(resources.stdout.contains("compiled id"));
    assert!(resources.stdout.contains("local.events"));
    assert!(resources.stdout.contains("local"));
    assert!(resources.stdout.contains("events"));
    assert!(resources.stdout.contains("resources/files.toml"));
    assert!(resources.stdout.contains("matched local.*"));

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
    assert!(resource.stdout.contains("stream capabilities"));
    assert!(resource.stdout.contains("bounded"));
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
fn resource_mapping_pattern_mismatch_reports_validate_and_plan_commands() {
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

[resources."yellow"]
source = "resources/tlc.toml"
"#,
    )
    .unwrap();
    fs::write(
        project.root.join("resources/tlc.toml"),
        r#"
[source.tlc]
kind = "files"
root = "data"

[resource.yellow]
glob = "*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#,
    )
    .unwrap();

    let validate = run(["cdf", "--project", project.root_str(), "validate"]);
    assert_ne!(validate.exit_code, 0);
    assert!(validate.stderr.contains("cdf validate cannot load project"));
    assert!(
        validate
            .stderr
            .contains("resource mapping pattern `yellow`")
    );
    assert!(validate.stderr.contains("tlc.yellow"));
    assert!(validate.stderr.contains("[resources.\"tlc.yellow\"]"));

    let plan = run(["cdf", "--project", project.root_str(), "plan", "tlc.yellow"]);
    assert_ne!(plan.exit_code, 0);
    assert!(plan.stderr.contains("cdf plan cannot load project"));
    assert!(!plan.stderr.contains("cdf validate cannot load project"));
    assert!(plan.stderr.contains("resource mapping pattern `yellow`"));
    assert!(plan.stderr.contains("tlc.yellow"));
    assert!(plan.stderr.contains("[resources.\"tlc.yellow\"]"));
}

#[test]
fn parser_accepts_canonical_color_policy_anywhere_without_changing_json_envelope() {
    let result = run(["cdf", "version", "--color", "never", "--json"]);

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
    assert!(readme_text.contains("cdf plan local.events"));
    assert!(readme_text.contains("cdf run local.events"));
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
    assert!(readme_text.contains("cdf plan local.events"));
    assert!(readme_text.contains("cdf run local.events"));
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
fn validate_deep_reports_source_front_end_checks_without_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    remove_resource_format(&project, "parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "validate");
    assert_eq!(json["result"]["mode"], "deep");
    assert_eq!(json["result"]["summary"]["resources"], 1);
    assert_eq!(json["result"]["summary"]["failed"], 0);
    assert_eq!(json["result"]["summary"]["partitions"], 1);
    assert_eq!(json["result"]["summary"]["discovery_probes"], 1);
    assert_eq!(json["result"]["writes"]["package"], false);
    assert_eq!(json["result"]["writes"]["destination"], false);
    assert_eq!(json["result"]["writes"]["checkpoint"], false);
    assert_eq!(json["result"]["writes"]["schema_snapshot"], false);
    assert_eq!(json["result"]["writes"]["lockfile"], false);

    let resource = &json["result"]["resources"][0];
    assert_eq!(resource["resource_id"], "local.events");
    assert_eq!(resource["source_file"], "resources/files.toml");
    assert_eq!(resource["mapping_pattern"], "local.*");
    assert_eq!(resource["mapping_status"], "matched");
    assert_eq!(resource["schema_source"], "discovered");
    assert_eq!(resource["partitions"]["count"], 1);
    assert_eq!(resource["partitions"]["files"][0], "vendors.parquet");
    assert_eq!(resource["discovery"]["status"], "ok");
    assert!(
        resource["discovery"]["schema_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        resource["discovery"]["snapshot_path"]
            .as_str()
            .unwrap()
            .starts_with(".cdf/schemas/local.events@sha256:")
    );
    assert_eq!(resource["validation_program"]["status"], "ok");
    assert_eq!(resource["identifier_normalization"]["status"], "ok");
    assert_eq!(resource["execution_extent"], "bounded");
    assert_eq!(resource["stream_policy"]["status"], "ok");
    assert!(
        resource["stream_policy"]["detail"]
            .as_str()
            .unwrap()
            .contains("sha256:")
    );
    assert_eq!(resource["destination"]["status"], "ok");
}

#[test]
fn validate_deep_rejects_stale_pinned_source_authority_without_runtime_probe() {
    let project = TestProject::new();
    write_minimal_lockfile(&project);
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "stderr: {}", pin.stderr);

    fs::create_dir_all(project.root.join("other-data")).unwrap();
    write_vendor_parquet(&project.root.join("other-data/vendors.parquet"));
    let resource_path = project.root.join("resources/files.toml");
    let resource_text = fs::read_to_string(&resource_path).unwrap();
    fs::write(
        &resource_path,
        resource_text.replace("root = \"data\"", "root = \"other-data\""),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);

    assert_eq!(result.exit_code, 3, "stdout: {}", result.stdout);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let json = stderr_or_stdout_json(&result.stdout);
    let resource = &json["result"]["resources"][0];
    assert_eq!(resource["status"], "failed");
    let diagnostics = resource["diagnostics"].as_array().unwrap();
    let authority = diagnostics
        .iter()
        .find(|diagnostic| diagnostic["check"] == "source_schema_authority")
        .expect("deep validation must report stale pinned source authority");
    assert!(
        authority["message"]
            .as_str()
            .unwrap()
            .contains("does not match compiled source authority")
    );
    assert!(
        authority["remediation"]
            .as_str()
            .unwrap()
            .contains("Repin the schema")
    );
}

#[test]
fn validate_deep_inferred_binary_mismatch_names_all_signals_without_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "events.parquet");
    remove_resource_format(&project, "parquet");
    write_vendor_arrow_ipc(&project, "events.parquet");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);

    assert_ne!(result.exit_code, 0);
    let json = stderr_or_stdout_json(&result.stdout);
    let diagnostics = json["result"]["resources"][0]["diagnostics"]
        .as_array()
        .unwrap();
    let message = diagnostics
        .iter()
        .filter_map(|diagnostic| diagnostic["message"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(message.contains("file format confirmation failed for resource `local.events`"));
    assert!(message.contains("file `events.parquet`"));
    assert!(message.contains("declared format `<omitted>`"));
    assert!(message.contains("inferred format `parquet`"));
    assert!(message.contains("extension signal `parquet`"));
    assert!(message.contains("magic bytes signal `arrow_ipc`"));
    assert!(message.contains("format = \"parquet\""));
    assert_no_schema_discovery_writes(&project);
}

#[test]
fn validate_deep_names_quarantined_physical_and_constraint_types_and_honors_allowance() {
    let project = TestProject::new();
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "vendors.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
schema = { fields = [{ name = "VendorID", type = "int8", nullable = false }] }
"#,
    )
    .unwrap();
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let denied = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);
    assert_eq!(denied.exit_code, 0, "{}", denied.stderr);
    let denied_json = stderr_or_stdout_json(&denied.stdout);
    let messages = denied_json["result"]["resources"][0]["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|diagnostic| diagnostic["message"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(messages.contains("resource `local.events`"), "{messages}");
    assert!(messages.contains("vendors.parquet"), "{messages}");
    assert!(messages.contains("VendorID"), "{messages}");
    assert!(messages.contains("Int32"), "{messages}");
    assert!(messages.contains("Int8"), "{messages}");
    assert!(messages.contains("allow_lossy_mapping"), "{messages}");
    assert_no_schema_discovery_writes(&project);

    let path = project.root.join("resources/files.toml");
    let allowed = fs::read_to_string(&path).unwrap().replace(
        "trust = \"governed\"",
        "trust = \"governed\"\ntypes = { allow_lossy_mapping = true }",
    );
    fs::write(path, allowed).unwrap();
    let allowed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);
    assert_eq!(allowed.exit_code, 0, "{}{}", allowed.stdout, allowed.stderr);
    assert_no_schema_discovery_writes(&project);
}

#[test]
fn validate_deep_reports_json_row_mismatch_as_governed_warning() {
    let project = TestProject::new();
    fs::write(
        project.root.join("data/events.ndjson"),
        b"{\"id\":1,\"updated_at\":1}\n{\"id\":\"bad\",\"updated_at\":2}\n",
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);

    assert_eq!(result.exit_code, 0, "{}{}", result.stdout, result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let diagnostics = json["result"]["resources"][0]["diagnostics"]
        .as_array()
        .unwrap();
    let mismatch = diagnostics
        .iter()
        .find(|diagnostic| diagnostic["check"] == "schema_quarantine")
        .unwrap_or_else(|| panic!("expected typed row-local warning, got {diagnostics:#?}"));
    assert_eq!(mismatch["severity"], "warning");
    assert_eq!(mismatch["code"], "CDF-DEEP-SCHEMA-QUARANTINE");
    assert!(mismatch["message"].as_str().unwrap().contains("id"));
    assert!(mismatch["message"].as_str().unwrap().contains("Utf8"));
    assert!(mismatch["message"].as_str().unwrap().contains("Int64"));
    assert_no_schema_discovery_writes(&project);
}

#[test]
fn validate_deep_rejects_malformed_json_probe_instead_of_downgrading_it() {
    let project = TestProject::new();
    fs::write(project.root.join("data/events.ndjson"), b"{not-json}\n").unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);

    assert_eq!(result.exit_code, 3, "{}{}", result.stdout, result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let diagnostics = json["result"]["resources"][0]["diagnostics"]
        .as_array()
        .unwrap();
    let probe = diagnostics
        .iter()
        .find(|diagnostic| diagnostic["check"] == "physical_schema_probe")
        .unwrap_or_else(|| panic!("expected physical probe failure, got {diagnostics:#?}"));
    assert_eq!(probe["severity"], "error");
    assert_no_schema_discovery_writes(&project);
}

#[test]
fn tier_zero_coerce_types_applies_to_actual_file_execution() {
    let project = TestProject::new();
    let resource_path = project.root.join("resources/files.toml");
    let resource = fs::read_to_string(&resource_path).unwrap().replace(
        "trust = \"governed\"",
        "trust = \"governed\"\ntypes = { coerce_types = true }",
    );
    fs::write(resource_path, resource).unwrap();
    fs::write(
        project.root.join("data/events.ndjson"),
        b"{\"id\":\"1\",\"updated_at\":\"1783296000000000\"}\n{\"id\":\"2\",\"updated_at\":\"1783296060000000\"}\n",
    )
    .unwrap();

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "{}{}", result.stdout, result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 2);
    let connection = duckdb::Connection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let ids = connection
        .prepare("SELECT id FROM events ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn resource_not_compiled_error_names_compiled_ids_origins_and_fix() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.eventz",
    ]);

    assert_eq!(result.exit_code, 3);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["code"], "CDF-RESOURCE-NOT-COMPILED");
    assert_eq!(
        json["error"]["remediation"]["summary"],
        "Use one of the compiled resource ids or repair the project resource mapping."
    );
    let message = json["error"]["message"].as_str().unwrap();
    assert!(message.contains("resource `local.eventz` is not compiled"));
    assert!(message.contains("compiled resource ids: `local.events`"));
    assert!(message.contains("resources/files.toml"));
    assert!(message.contains("mapping `local.*` matched"));
    assert!(message.contains("likely causes"));
    assert!(message.contains("<source>.<resource>"));
    assert!(!message.contains("cdf run requires"));
    assert_eq!(json["error"]["suggestions"][0], "local.events");
}

#[test]
fn add_local_parquet_pins_schema_and_writes_resource_config() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/yellow.parquet"));

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "tlc.yellow",
        project.root.join("data/yellow.parquet").to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "add");
    assert_eq!(report["resource_id"], "tlc.yellow");
    assert_eq!(report["config_path"], "resources/tlc.toml");
    assert_eq!(report["location"], "data");
    assert_eq!(report["selection"], "yellow.parquet");
    assert_eq!(report["write_disposition"], "append");
    assert_eq!(report["schema_source"], "discovered");
    assert_eq!(report["next_command"], "cdf run tlc.yellow");
    assert_eq!(report["writes"]["resource_config"], true);
    assert_eq!(report["writes"]["project_config"], true);
    assert_eq!(report["writes"]["schema_snapshot"], true);
    assert_eq!(report["writes"]["lockfile"], true);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert!(
        report["schema_snapshot_path"]
            .as_str()
            .unwrap()
            .starts_with(".cdf/schemas/tlc.yellow@sha256:")
    );
    assert_eq!(report["fields"][0]["name"], "vendor_id");
    assert_eq!(report["fields"][0]["source_name"], "VendorID");

    let resource_toml = fs::read_to_string(project.root.join("resources/tlc.toml")).unwrap();
    assert!(resource_toml.contains("[source.tlc]"));
    assert!(resource_toml.contains("kind = \"files\""));
    assert!(resource_toml.contains("root = \"data\""));
    assert!(resource_toml.contains("[resource.yellow]"));
    assert!(resource_toml.contains("glob = \"yellow.parquet\""));
    assert!(resource_toml.contains("format = \"parquet\""));
    assert!(resource_toml.contains("write_disposition = \"append\""));
    assert!(!resource_toml.contains("primary_key"));
    assert!(!resource_toml.contains("merge_key"));
    assert!(!resource_toml.contains("schema ="));

    let project_toml = fs::read_to_string(project.root.join("cdf.toml")).unwrap();
    assert!(project_toml.contains("[resources.\"tlc.yellow\"]"));
    assert!(project_toml.contains("source = \"resources/tlc.toml\""));
    assert!(
        project
            .root
            .join(report["schema_snapshot_path"].as_str().unwrap())
            .is_file()
    );

    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let locked = lock.resources.get("tlc.yellow").unwrap();
    assert!(locked.schema_snapshot.is_some());
    assert_eq!(
        locked.schema_snapshot.as_ref().unwrap().path,
        report["schema_snapshot_path"].as_str().unwrap()
    );

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "tlc.yellow",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
}

#[test]
fn add_local_parquet_dry_run_writes_nothing() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/yellow.parquet"));

    let before_project = fs::read_to_string(project.root.join("cdf.toml")).unwrap();
    let before_tree = project_tree_snapshot(&project.root);
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "tlc.yellow",
        project.root.join("data/yellow.parquet").to_str().unwrap(),
        "--dry-run",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["writes"]["resource_config"], false);
    assert_eq!(report["writes"]["project_config"], false);
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["lockfile"], false);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert_eq!(report["next_command"], "cdf run tlc.yellow");
    assert_eq!(
        fs::read_to_string(project.root.join("cdf.toml")).unwrap(),
        before_project
    );
    assert!(!project.root.join("resources/tlc.toml").exists());
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert_project_tree_unchanged(&project.root, &before_tree);
}

#[test]
fn add_local_ndjson_uses_the_registered_file_driver_without_cli_format_wiring() {
    let project = TestProject::new();
    let source = project.root.join("data/events.ndjson");
    fs::write(
        &source,
        "{\"id\":1,\"occurred_at\":1783296000000000}\n{\"id\":2,\"occurred_at\":1783296000000001}\n",
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "ingest.events",
        source.to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let report = stderr_or_stdout_json(&result.stdout);
    assert_eq!(report["result"]["source_driver"], "files");
    assert_eq!(report["result"]["selection"], "events.ndjson");
    assert!(
        report["result"]["cursor_candidates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|candidate| candidate == "occurred_at")
    );
    let resource = fs::read_to_string(project.root.join("resources/ingest.toml")).unwrap();
    assert!(resource.contains("format = \"ndjson\""));
    assert!(resource.contains("write_disposition = \"append\""));
}

#[test]
fn add_rest_requires_explicit_selector_and_cursor_then_pins_sample() {
    let project = TestProject::new();
    let base_url =
        serve_json_once(r#"{"items":[{"id":1,"updated_at":1783296000000000,"name":"first"}]}"#);
    let endpoint = format!("{base_url}/items");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "api.items",
        &endpoint,
        "--option",
        "records=$.items",
        "--option",
        "cursor=updated_at",
        "--option",
        "cursor_param=since",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource_id"], "api.items");
    assert_eq!(json["result"]["selection"], "/items");
    assert_eq!(json["result"]["cursor"], "updated_at");
    assert_eq!(json["result"]["cursor_candidates"][0], "id");
    assert_eq!(json["result"]["writes"]["schema_snapshot"], true);
    let resource = fs::read_to_string(project.root.join("resources/api.toml")).unwrap();
    assert!(resource.contains("kind = \"rest\""));
    assert!(resource.contains(&format!("base_url = {base_url:?}")));
    assert!(resource.contains("path = \"/items\""));
    assert!(resource.contains("records = \"$.items\""));
    assert!(resource.contains("field = \"updated_at\""));
    assert!(resource.contains("param = \"since\""));
    assert!(resource.contains("ordering = \"best_effort\""));
    assert!(!resource.contains("schema ="));

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "api.items",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
}

#[test]
fn add_rest_rejects_partial_semantics_before_network_or_writes() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "api.items",
        "https://api.example.test/items",
        "--option",
        "records=$.items",
    ]);

    assert_eq!(result.exit_code, 2);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("requires options `records`, `cursor`, and `cursor_param` together")
    );
    assert!(!project.root.join("resources/api.toml").exists());
    assert!(!project.root.join("cdf.lock").exists());
}

#[test]
fn p2_s1_add_http_parquet_pins_and_runs_with_zero_typed_fields() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/yellow.parquet"));
    let parquet = fs::read(project.root.join("data/yellow.parquet")).unwrap();
    let (base_url, requests) = serve_parquet_file(parquet, 256);
    let url = format!("{base_url}/yellow.parquet");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "remote.yellow",
        &url,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "remote.yellow");
    assert_eq!(report["selection"], "yellow.parquet");
    assert_eq!(report["write_disposition"], "append");
    assert!(project.root.join("resources/remote.toml").is_file());
    let resource_toml = fs::read_to_string(project.root.join("resources/remote.toml")).unwrap();
    assert!(resource_toml.contains("[source.remote]"));
    assert!(resource_toml.contains("kind = \"files\""));
    assert!(resource_toml.contains("egress_allowlist = [\"127.0.0.1\"]"));
    assert!(resource_toml.contains("glob = \"yellow.parquet\""));
    assert!(!resource_toml.contains("primary_key"));
    assert!(!resource_toml.contains("merge_key"));

    let before_no_pin = project_tree_snapshot(&project.root);
    let no_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "remote.yellow",
        "--no-pin",
    ]);
    assert_eq!(no_pin.exit_code, 0, "stderr: {}", no_pin.stderr);
    let no_pin_report = stderr_or_stdout_json(&no_pin.stdout);
    assert_eq!(
        no_pin_report["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert_eq!(project_tree_snapshot(&project.root), before_no_pin);

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "remote.yellow",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    let plan_report = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(
        plan_report["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        plan_report["result"]["schema_snapshot"]["lockfile_written"],
        false
    );

    let run_result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "remote.yellow",
    ]);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_report = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(run_report["result"]["resource_id"], "remote.yellow");
    assert_eq!(run_report["result"]["row_count"], 2);
    assert!(
        run_package_dir(&project, &run_result)
            .join("manifest.json")
            .is_file()
    );

    let requests = requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("HEAD /yellow.parquet HTTP/1.1")),
        "expected metadata HEAD request, got {requests:?}"
    );
    assert!(
        requests.iter().any(
            |request| request.starts_with("GET /yellow.parquet HTTP/1.1")
                && request.to_ascii_lowercase().contains("range: bytes=")
        ),
        "expected bounded range GET request, got {requests:?}"
    );
}

#[test]
fn p2_s2_http_month_glob_is_incremental_and_no_change_is_a_noop() {
    let project = TestProject::new();
    let files = BTreeMap::from([
        (
            "/yellow_tripdata_2024-01.parquet".to_owned(),
            vendor_parquet_bytes(&[1, 2]),
        ),
        (
            "/yellow_tripdata_2024-02.parquet".to_owned(),
            vendor_parquet_bytes(&[3, 4]),
        ),
    ]);
    let (base_url, files, _) = serve_parquet_files(files, 2_000);
    fs::write(
        project.root.join("resources/tlc.toml"),
        format!(
            r#"
[source.tlc]
kind = "files"
root = "{base_url}"
egress_allowlist = ["127.0.0.1"]

[resource.yellow]
glob = "yellow_tripdata_2024-*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
    let project_path = project.root.join("cdf.toml");
    let mut project_toml = fs::read_to_string(&project_path).unwrap();
    project_toml.push_str("\n[resources.\"tlc.yellow\"]\nsource = \"resources/tlc.toml\"\n");
    fs::write(project_path, project_toml).unwrap();

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "tlc.yellow",
    ]);
    assert_eq!(plan.exit_code, 0, "{}", plan.stderr);
    let before_preview = project_tree_snapshot(&project.root);
    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "tlc.yellow",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let preview_report = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_report["result"]["planned_partition_count"], 2);
    assert_eq!(preview_report["result"]["row_count"], 4);
    assert_eq!(project_tree_snapshot(&project.root), before_preview);

    let first = run_http_monthly_resource(&project, "p2-s2-first", "checkpoint-p2-s2-first");
    assert_eq!(first.exit_code, 0, "{}", first.stderr);
    let first_report = stderr_or_stdout_json(&first.stdout);
    assert_eq!(first_report["result"]["row_count"], 4);
    assert_eq!(
        first_report["result"]["file_manifest"]["changed_file_count"],
        2
    );

    let unchanged =
        run_http_monthly_resource(&project, "p2-s2-unchanged", "checkpoint-p2-s2-unchanged");
    assert_eq!(unchanged.exit_code, 0, "{}", unchanged.stderr);
    let unchanged_report = stderr_or_stdout_json(&unchanged.stdout);
    assert_eq!(unchanged_report["result"]["row_count"], 0);
    assert_eq!(
        unchanged_report["result"]["file_manifest"]["changed_file_count"],
        0
    );
    assert_eq!(unchanged_report["result"]["writes"]["package"], false);

    files.lock().unwrap().insert(
        "/yellow_tripdata_2024-03.parquet".to_owned(),
        vendor_parquet_bytes(&[5, 6]),
    );
    let third = run_http_monthly_resource(&project, "p2-s2-third", "checkpoint-p2-s2-third");
    assert_eq!(third.exit_code, 0, "{}", third.stderr);
    let third_report = stderr_or_stdout_json(&third.stdout);
    assert_eq!(third_report["result"]["row_count"], 2);
    assert_eq!(
        third_report["result"]["file_manifest"]["changed_file_count"],
        1
    );
    let connection = duckdb::Connection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let total: i64 = connection
        .query_row("SELECT COUNT(*) FROM yellow", [], |row| row.get(0))
        .unwrap();
    assert_eq!(total, 6);
}

fn run_http_monthly_resource(
    project: &TestProject,
    _package_id: &str,
    _checkpoint_id: &str,
) -> cdf_cli_core::output::InvocationResult {
    run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "tlc.yellow",
    ])
}

#[test]
fn add_rejects_signed_url_without_leaking_secret_query() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "remote.yellow",
        "https://data.example.test/yellow.parquet?sig=super-secret-token",
    ]);

    assert_ne!(result.exit_code, 0);
    assert_secret_absent(&result, "super-secret-token");
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["code"], "CDF-CLI-USAGE");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("<redacted>")
    );
    assert!(!project.root.join("resources/remote.toml").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/schemas").exists());
}

#[test]
fn contract_show_remains_project_free() {
    let result = run(["cdf", "--json", "contract", "show", "governed"]);

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
    assert!(
        result["scheduler"]["effective_jobs"]["jobs"]
            .as_u64()
            .is_some_and(|jobs| jobs >= 1)
    );
    assert!(
        result["scheduler"]["managed_memory_available_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert_eq!(result["scheduler"]["destination_writer_concurrency"], 1);
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
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        "OK plan local.events -> events",
        "Fetch",
        "execution                 bounded",
        "effective jobs",
        "managed memory available",
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
    let services = test_execution_services();
    let output = crate::scan_command::plan_or_explain(
        &cli,
        cdf_cli_core::args::ScanArgs {
            resource_id: "local.events".to_owned(),
            destination_uri: None,
            projection: Some(vec!["id".to_owned(), "updated_at".to_owned()]),
            filters: vec!["id > 10".to_owned()],
            limit: Some(5),
            order_by: Vec::new(),
            no_pin: false,
        },
        "plan",
        &services,
        &test_destination_registry(),
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
fn plan_human_next_command_preserves_explicit_destination_with_canonical_target() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--to",
        "duckdb://.cdf/plan-explicit.duckdb",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        result
            .stdout
            .contains("-> cdf run local.events --to duckdb://.cdf/plan-explicit.duckdb"),
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
    assert_eq!(report["explain"]["execution_extent"]["kind"], "bounded");
    assert!(report["explain"].get("compiled_stream_policy").is_none());
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

    let (host, services) =
        cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let output = crate::backfill_command::backfill(
        &test_cli(&project),
        cdf_cli_core::args::BackfillArgs {
            resource_id: "warehouse.orders".to_owned(),
            from: "0".to_owned(),
            to: "20".to_owned(),
            target: Some("orders".to_owned()),
            execute: false,
            slice_size: Some(10),
        },
        (host.as_ref(), &services),
        &test_destination_registry(),
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
fn backfill_rejects_removed_resource_alias_before_project_load() {
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
            .contains("--resource")
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
    let table = seed_ordered_cursor_table(
        &postgres,
        "backfill_source_orders",
        "(1, 5), (2, 15), (3, 25)",
    );
    let project = TestProject::new();
    let source_dsn = write_sql_project_with_secret(&project, &postgres, &table);

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
    assert!(!result.stdout.contains("Run progress"));
    assert!(!result.stderr.contains("[plan]"));
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
fn backfill_execute_human_progress_reports_each_slice_and_summary() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = seed_ordered_cursor_table(
        &postgres,
        "backfill_progress_orders",
        "(1, 5), (2, 15), (3, 25)",
    );
    let project = TestProject::new();
    let source_dsn = write_sql_project_with_secret(&project, &postgres, &table);

    let result = run([
        "cdf",
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
        "--slice-size",
        "10",
        "--execute",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        "[plan] running run started",
        "scope=window:0..10",
        "scope=window:10..20",
        "[gate] succeeded run succeeded",
    ] {
        assert!(
            result.stderr.contains(expected),
            "missing {expected:?} in stderr:\n{}",
            result.stderr
        );
    }
    for expected in [
        "OK executed backfill warehouse.orders -> orders",
        "Summary",
        "slices succeeded  2/2",
        "rows              2",
        "segments          2",
        "-> cdf state history <resource>",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in stdout:\n{}",
            result.stdout
        );
    }
}

#[test]
fn backfill_execute_human_failure_reports_failed_slice_and_recovery_guidance() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = seed_ordered_cursor_table(&postgres, "backfill_progress_failure_orders", "(1, 5)");
    let project = TestProject::new();
    let source_dsn = write_sql_project_with_secret(&project, &postgres, &table);

    let args = || {
        vec![
            "cdf".to_owned(),
            "--project".to_owned(),
            project.root_str().to_owned(),
            "backfill".to_owned(),
            "warehouse.orders".to_owned(),
            "--from".to_owned(),
            "0".to_owned(),
            "--to".to_owned(),
            "10".to_owned(),
            "--target".to_owned(),
            "orders".to_owned(),
            "--execute".to_owned(),
        ]
    };
    let first = run_dynamic(args());
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    assert_secret_absent(&first, &source_dsn);

    let second = run_dynamic(args());
    assert_secret_absent(&second, &source_dsn);

    assert_ne!(second.exit_code, 0);
    assert!(second.stdout.is_empty());
    assert!(!second.stderr.contains("\u{1b}["));
    for expected in [
        "backfill slice 1 (0..10) failed",
        "package cdf-backfill-pkg-",
        "checkpoint cdf-backfill-cp-",
        "mutation status:",
        "next recovery command:",
        "not available before a run id is recorded",
    ] {
        assert!(
            second.stderr.contains(expected),
            "missing {expected:?} in:\n{}",
            second.stderr
        );
    }
    assert!(!second.stderr.contains("suggestions:"));
    assert!(!second.stderr.contains("cdf resume "));
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
fn schema_discover_local_parquet_reports_schema_without_project_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "local.events");
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["lockfile"], false);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert!(
        report["schema_snapshot_path"]
            .as_str()
            .unwrap()
            .starts_with(".cdf/schemas/local.events@sha256:")
    );
    assert_eq!(
        report["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(report["snapshot_metadata"]["source_driver"], "files");
    assert_eq!(report["snapshot_metadata"]["cdf:normalizer"], "namecase-v1");
    assert_eq!(report["fields"][0]["name"], "vendor_id");
    assert_eq!(report["fields"][0]["source_name"], "VendorID");
    assert_eq!(
        report["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(report["source_identity"]["path"], "vendors.parquet");
    assert!(
        report["source_identity"]["driver.footer_sha256"]
            .as_str()
            .is_some()
    );
    assert_eq!(report["next_command"], "cdf plan local.events");
}

#[test]
fn local_arrow_ipc_discover_pin_show_diff_preview_and_run_share_pinned_schema() {
    let project = TestProject::new();
    write_arrow_ipc_discover_resource(&project, "events.arrow");
    remove_resource_format(&project, "arrow_ipc");
    write_large_vendor_arrow_ipc(&project, "events.arrow");

    let discover = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_eq!(discover.exit_code, 0, "stderr: {}", discover.stderr);
    let discover_json = stderr_or_stdout_json(&discover.stdout);
    let discovered = &discover_json["result"];
    assert_eq!(
        discovered["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(discovered["snapshot_metadata"]["source_driver"], "files");
    assert!(
        discovered["snapshot_metadata"]["source_discovery_binding"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:"))
    );
    assert_eq!(
        discovered["snapshot_metadata"]["cdf:normalizer"],
        "namecase-v1"
    );
    assert_eq!(discovered["source_identity"]["path"], "events.arrow");
    assert_eq!(discovered["source_identity"]["transport"], "files");
    assert!(
        discovered["source_identity"]["driver.schema_hash"]
            .as_str()
            .is_some()
    );
    let source_size = discovered["source_identity"]["driver.size_bytes"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let probe_bytes = discovered["source_identity"]["probe_bytes_read"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!(
        probe_bytes < source_size / 2,
        "generic CLI discovery read {probe_bytes} of {source_size} source bytes"
    );
    assert_eq!(discovered["fields"][0]["name"], "vendor_id");
    assert_eq!(discovered["fields"][0]["source_name"], "VendorID");
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());

    let no_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--no-pin",
    ]);
    assert_eq!(no_pin.exit_code, 0, "stderr: {}", no_pin.stderr);
    assert_eq!(
        stderr_or_stdout_json(&no_pin.stdout)["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let auto_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(auto_pin.exit_code, 0, "stderr: {}", auto_pin.stderr);
    let auto_pin_json = stderr_or_stdout_json(&auto_pin.stdout);
    assert_eq!(
        auto_pin_json["result"]["schema_snapshot"]["outcome"],
        "added"
    );
    let pinned_hash = auto_pin_json["result"]["resource_schema"]["schema_hash"]
        .as_str()
        .unwrap();
    let baseline_hash = auto_pin_json["result"]["schema_snapshot"]["schema_hash"]
        .as_str()
        .unwrap();
    assert_eq!(pinned_hash, baseline_hash);
    let snapshot_path = auto_pin_json["result"]["resource_schema"]["snapshot_path"]
        .as_str()
        .unwrap();

    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "stderr: {}", pin.stderr);
    let pin_json = stderr_or_stdout_json(&pin.stdout);
    assert_eq!(pin_json["result"]["status"], "unchanged");
    assert_eq!(pin_json["result"]["schema_hash"], baseline_hash);
    let snapshot = read_snapshot_json(&project, snapshot_path);
    assert_eq!(snapshot["schema_hash"], baseline_hash);
    assert_eq!(snapshot["schema"]["metadata"]["owner"], "source-system");
    assert_eq!(
        snapshot["schema"]["fields"][0]["metadata"]["source-tag"],
        "vendor"
    );
    assert_eq!(
        snapshot["schema"]["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );

    let show = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "show",
        "local.events",
    ]);
    assert_eq!(show.exit_code, 0, "stderr: {}", show.stderr);
    assert_eq!(
        stderr_or_stdout_json(&show.stdout)["result"]["schema_hash"],
        baseline_hash
    );

    let diff = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "diff",
        "local.events",
    ]);
    assert_eq!(diff.exit_code, 0, "stderr: {}", diff.stderr);
    assert_eq!(
        stderr_or_stdout_json(&diff.stdout)["result"]["summary"]["changed"],
        false
    );

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(
        plan_json["result"]["resource_schema"]["schema_hash"],
        pinned_hash
    );
    assert_eq!(
        plan_json["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    let preview_json = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_json["result"]["row_count"], 2);
    assert_eq!(
        preview_json["result"]["fields"],
        json!(["vendor_id", "note", "_cdf_variant"])
    );
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(run_json["result"]["schema_hash"], pinned_hash);
    assert_eq!(run_json["result"]["row_count"], 2);
    assert_eq!(run_json["result"]["checkpoint"]["status"], "committed");
    let package_dir = run_package_dir(&project, &run_result);
    let reader = PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    let receipts = collect_package_receipts(&reader);
    assert_eq!(receipts.len(), 1);
    let receipt = &receipts[0];
    assert_eq!(receipt.schema_hash.as_str(), pinned_hash);
    assert_eq!(receipt.disposition, WriteDisposition::Append);
    assert_eq!(receipt.counts.rows_written, 2);
    let destination = DuckDbDestination::new(project.root.join(".cdf/dev.duckdb")).unwrap();
    assert!(destination.verify_receipt(receipt).unwrap().verified);
    let segments = collect_package_segments_for_test(&reader);
    assert_eq!(segments.len(), 1);
    let packaged_schema = segments[0].1[0].schema();
    assert_eq!(packaged_schema.metadata()["owner"], "source-system");
    assert_eq!(
        packaged_schema.field(0).metadata()["cdf:source_name"],
        "VendorID"
    );
    let stream_admission: serde_json::Value = serde_json::from_slice(
        &fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let coercion: cdf_contract::SchemaCoercionPlan =
        serde_json::from_value(stream_admission["observations"][0]["coercion_plan"].clone())
            .unwrap();
    let vendor = coercion
        .fields
        .iter()
        .find(|field| field.source_name == "VendorID")
        .unwrap();
    assert_eq!(
        vendor.decision,
        cdf_contract::FieldCoercionDecision::Preserved
    );
    assert_eq!(vendor.observed_type.as_deref(), Some("Int32"));
    assert_eq!(vendor.constraint_type.as_deref(), Some("Int32"));

    let replay = reader.replay_inputs().unwrap();
    assert_eq!(replay.state_delta.schema_hash.as_str(), pinned_hash);
    assert!(receipt.covers_state_delta(&replay.state_delta));
    let SourcePosition::FileManifest(manifest) = &replay.state_delta.output_position else {
        panic!("Arrow IPC run must commit FileManifest source position");
    };
    assert_eq!(manifest.files.len(), 1);
    let source_path = project.root.join("data/events.arrow");
    let source_bytes = fs::read(&source_path).unwrap();
    let expected_sha = format!(
        "sha256:{}",
        Sha256::digest(&source_bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    assert_eq!(manifest.files[0].path, "events.arrow");
    assert_eq!(
        manifest.files[0].size_bytes,
        u64::try_from(source_bytes.len()).unwrap()
    );
    assert_eq!(
        manifest.files[0].sha256.as_deref(),
        Some(expected_sha.as_str())
    );
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &replay.state_delta.pipeline_id,
            &replay.state_delta.resource_id,
            &replay.state_delta.scope,
        )
        .unwrap()
        .expect("committed Arrow IPC checkpoint head");
    assert_eq!(head.delta.schema_hash.as_str(), pinned_hash);
    assert_eq!(
        head.delta.output_position,
        replay.state_delta.output_position
    );
    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);
}

#[test]
fn arrow_ipc_discovery_supports_compression_multi_file_and_remote_without_writes() {
    let malformed = TestProject::new();
    write_arrow_ipc_discover_resource(&malformed, "events.arrow");
    fs::write(malformed.root.join("data/events.arrow"), b"not-arrow-ipc").unwrap();
    let malformed_result = run([
        "cdf",
        "--json",
        "--project",
        malformed.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_ne!(malformed_result.exit_code, 0);
    assert!(
        malformed_result
            .stderr
            .contains("file format confirmation failed")
    );
    assert_no_schema_discovery_writes(&malformed);

    let truncated = TestProject::new();
    write_arrow_ipc_discover_resource(&truncated, "events.arrow");
    write_vendor_arrow_ipc(&truncated, "events.arrow");
    fs::OpenOptions::new()
        .write(true)
        .open(truncated.root.join("data/events.arrow"))
        .unwrap()
        .set_len(16)
        .unwrap();
    let truncated_result = run([
        "cdf",
        "--json",
        "--project",
        truncated.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_ne!(truncated_result.exit_code, 0);
    assert!(
        truncated_result
            .stderr
            .contains("Arrow file does not contain correct footer"),
        "{}",
        truncated_result.stderr
    );
    assert_no_schema_discovery_writes(&truncated);

    let stream = TestProject::new();
    write_arrow_ipc_discover_resource(&stream, "events.arrow");
    let stream_schema = Arc::new(Schema::new(vec![Field::new(
        "VendorID",
        DataType::Int32,
        false,
    )]));
    let stream_batch = RecordBatch::try_new(
        Arc::clone(&stream_schema),
        vec![Arc::new(Int32Array::from_iter_values([1_i32]))],
    )
    .unwrap();
    let mut stream_bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut stream_bytes, stream_schema.as_ref()).unwrap();
        writer.write(&stream_batch).unwrap();
        writer.finish().unwrap();
    }
    fs::write(stream.root.join("data/events.arrow"), stream_bytes).unwrap();
    let stream_result = run([
        "cdf",
        "--json",
        "--project",
        stream.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_ne!(stream_result.exit_code, 0);
    assert!(
        stream_result
            .stderr
            .contains("alternate format `arrow_ipc_stream`"),
        "{}",
        stream_result.stderr
    );
    assert!(
        stream_result
            .stderr
            .contains("stream framing is unsupported")
    );
    assert_no_schema_discovery_writes(&stream);

    let compression_source = TestProject::new();
    write_vendor_arrow_ipc(&compression_source, "events.arrow");
    let arrow_bytes = fs::read(compression_source.root.join("data/events.arrow")).unwrap();
    let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
    gzip.write_all(&arrow_bytes).unwrap();
    let gzip_bytes = gzip.finish().unwrap();
    for (label, bytes, compression_override, succeeds) in [
        ("gzip-auto", gzip_bytes.clone(), None, true),
        ("gzip-override", gzip_bytes, Some("gzip"), true),
        ("zstd-malformed", vec![0x28, 0xb5, 0x2f, 0xfd], None, false),
    ] {
        let compressed = TestProject::new();
        write_arrow_ipc_discover_resource(&compressed, "events.arrow");
        if let Some(compression) = compression_override {
            let resource_path = compressed.root.join("resources/files.toml");
            let resource = fs::read_to_string(&resource_path).unwrap().replace(
                "format = \"arrow_ipc\"",
                &format!("format = \"arrow_ipc\"\ncompression = \"{compression}\""),
            );
            fs::write(resource_path, resource).unwrap();
        }
        fs::write(compressed.root.join("data/events.arrow"), bytes).unwrap();
        let compressed_result = run([
            "cdf",
            "--json",
            "--project",
            compressed.root_str(),
            "schema",
            "discover",
            "local.events",
        ]);
        if succeeds {
            assert_eq!(
                compressed_result.exit_code, 0,
                "{label}: {}",
                compressed_result.stderr
            );
        } else {
            assert_ne!(compressed_result.exit_code, 0, "{label}");
            assert!(
                compressed_result.stderr.contains("failed:"),
                "{label}: {}",
                compressed_result.stderr
            );
        }
        assert!(
            !compressed_result.stderr.contains("excluded"),
            "{label}: {}",
            compressed_result.stderr
        );
        assert_no_schema_discovery_writes(&compressed);
    }

    let multi = TestProject::new();
    write_arrow_ipc_discover_resource(&multi, "*.arrow");
    write_vendor_arrow_ipc(&multi, "first.arrow");
    fs::copy(
        multi.root.join("data/first.arrow"),
        multi.root.join("data/second.arrow"),
    )
    .unwrap();
    let multi_result = run([
        "cdf",
        "--json",
        "--project",
        multi.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_eq!(multi_result.exit_code, 0, "{}", multi_result.stderr);
    let multi_json = stderr_or_stdout_json(&multi_result.stdout);
    let multi_report = &multi_json["result"];
    assert_eq!(
        multi_report["source_identity"]["file_coverage"],
        "all_files"
    );
    assert_eq!(
        multi_report["source_identity"]["within_file_coverage"],
        "format_metadata"
    );
    assert_eq!(multi_report["source_identity"]["matched_files"], "2");
    assert_eq!(multi_report["source_identity"]["selected_files"], "2");
    assert_no_schema_discovery_writes(&multi);
    let pin = run([
        "cdf",
        "--json",
        "--project",
        multi.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    let schema_entries = fs::read_dir(multi.root.join(".cdf/schemas"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(schema_entries.len(), 2);
    assert!(
        schema_entries
            .iter()
            .any(|path| path.ends_with(".discovery.json"))
    );
    let diff = run([
        "cdf",
        "--json",
        "--project",
        multi.root_str(),
        "schema",
        "diff",
        "local.events",
    ]);
    assert_eq!(diff.exit_code, 0, "{}", diff.stderr);

    let remote_source = TestProject::new();
    write_vendor_arrow_ipc(&remote_source, "events.arrow");
    let remote_bytes = fs::read(remote_source.root.join("data/events.arrow")).unwrap();
    let (base_url, _requests) = serve_parquet_file(remote_bytes, 16);
    let remote = TestProject::new();
    fs::write(
        remote.root.join("resources/files.toml"),
        format!(
            r#"
[source.local]
kind = "files"
root = "{base_url}/"
egress_allowlist = ["127.0.0.1"]

[resource.events]
glob = "events.arrow"
format = "arrow_ipc"
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
    let remote_result = run([
        "cdf",
        "--json",
        "--project",
        remote.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_eq!(remote_result.exit_code, 0, "{}", remote_result.stderr);
    let remote_report = stderr_or_stdout_json(&remote_result.stdout);
    assert_eq!(
        remote_report["result"]["source_identity"]["driver.format"],
        "arrow_ipc"
    );
    assert_eq!(
        remote_report["result"]["source_identity"]["transport"],
        "files"
    );
    assert_no_schema_discovery_writes(&remote);
}

#[test]
fn pinned_arrow_ipc_type_drift_is_observed_and_quarantined_in_the_preview_run_stream() {
    let project = TestProject::new();
    write_arrow_ipc_discover_resource(&project, "events.arrow");
    write_vendor_arrow_ipc(&project, "events.arrow");
    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "stderr: {}", pin.stderr);

    let drift_schema = Arc::new(Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, false),
        Field::new("Note", DataType::Utf8, true),
    ]));
    let drift_batch = RecordBatch::try_new(
        drift_schema,
        vec![
            Arc::new(StringArray::from(vec!["unexpected"])),
            Arc::new(StringArray::from(vec![Some("drifted")])),
        ],
    )
    .unwrap();
    write_arrow_ipc_source(&project, "events.arrow", drift_batch);

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let preview_report = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_report["result"]["planned_partition_count"], 1);
    assert_eq!(
        preview_report["result"]["payload_opened_partition_count"],
        1
    );
    assert_eq!(preview_report["result"]["attested_partition_count"], 0);
    assert_eq!(preview_report["result"]["terminal_quarantine_count"], 1);
    assert_eq!(preview_report["result"]["row_count"], 0);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    let package_dir = run_package_dir(&project, &run_result);
    let reader = PackageReader::open(package_dir).unwrap();
    reader.verify().unwrap();
    let mut has_quarantine = false;
    reader
        .for_each_identity_file(&mut |file| {
            has_quarantine |= file.path == "quarantine/schema-observations.json";
            Ok(())
        })
        .unwrap();
    assert!(has_quarantine);
}

#[test]
fn declared_arrow_ipc_lossless_widening_records_physical_and_coercion_evidence() {
    let project = TestProject::new();
    for entry in fs::read_dir(project.root.join("data")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.arrow"
format = "arrow_ipc"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "VendorID", type = "int64", nullable = false },
  { name = "Note", type = "string", nullable = true },
] }
"#,
    )
    .unwrap();
    write_vendor_arrow_ipc(&project, "events.arrow");

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    assert_eq!(
        stderr_or_stdout_json(&preview.stdout)["result"]["fields"],
        json!(["vendor_id", "note", "_cdf_variant"])
    );

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    let package_dir = run_package_dir(&project, &run_result);
    let reader = PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    let batches = collect_package_segments_for_test(&reader);
    let schema = batches[0].1[0].schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(0).metadata()["cdf:source_name"], "VendorID");
    assert!(!schema.field(0).metadata().contains_key("cdf:physical_type"));
    assert!(
        !package_dir
            .join("schema/effective-schema-evidence.json")
            .exists(),
        "declared execution must classify the physical schema in-stream without a pre-scan artifact"
    );
    let stream_admission: serde_json::Value = serde_json::from_slice(
        &fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let coercion: cdf_contract::SchemaCoercionPlan =
        serde_json::from_value(stream_admission["observations"][0]["coercion_plan"].clone())
            .unwrap();
    let vendor = coercion
        .fields
        .iter()
        .find(|field| field.source_name == "VendorID")
        .unwrap();
    assert_eq!(
        vendor.decision,
        cdf_contract::FieldCoercionDecision::Widened
    );
    assert_eq!(vendor.observed_type.as_deref(), Some("Int32"));
    assert_eq!(vendor.constraint_type.as_deref(), Some("Int64"));
}

#[test]
fn hints_schema_discovers_pins_and_constrains_observed_parquet() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "vendors.parquet"
format = "parquet"
schema_mode = "hints"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "VendorID", type = "int64", nullable = false },
] }
"#,
    )
    .unwrap();

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(plan.exit_code, 0, "{}", plan.stderr);
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(plan_json["result"]["schema_snapshot"]["outcome"], "added");
    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let initial_reference = lock.resources["local.events"]
        .schema_snapshot
        .as_ref()
        .unwrap()
        .clone();
    let initial_lock = fs::read(project.root.join("cdf.lock")).unwrap();
    let initial_snapshot = fs::read(project.root.join(&initial_reference.path)).unwrap();

    let unchanged = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(unchanged.exit_code, 0, "{}", unchanged.stderr);
    assert_eq!(
        stderr_or_stdout_json(&unchanged.stdout)["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        initial_lock
    );
    assert_eq!(
        fs::read(project.root.join(&initial_reference.path)).unwrap(),
        initial_snapshot
    );

    write_vendor_score_parquet(&project.root.join("data/vendors.parquet"));
    let drifted = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(drifted.exit_code, 0, "{}", drifted.stderr);
    assert_eq!(
        stderr_or_stdout_json(&drifted.stdout)["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        initial_lock
    );
    assert_eq!(
        fs::read(project.root.join(&initial_reference.path)).unwrap(),
        initial_snapshot
    );

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    let reader = PackageReader::open(run_package_dir(&project, &run_result)).unwrap();
    let batches = collect_package_segments_for_test(&reader);
    assert_eq!(
        batches[0].1[0].schema().field(0).data_type(),
        &DataType::Int64
    );
    assert_eq!(batches[0].1[0].schema().field(0).name(), "vendor_id");
}

#[test]
fn schema_discover_rest_reports_sample_schema_without_project_writes_or_secret_leak() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "rest-schema-secret\n").unwrap();
    let (base_url, requests) = serve_json_sequence([r#"{ "items": [
        { "VendorID": 1, "updated_at": 10, "active": true, "score": 4.5 },
        { "VendorID": 2, "updated_at": 20, "active": false, "score": null },
        { "VendorID": 3, "updated_at": 30, "active": true }
    ] }"#]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    fs::write(
        project.root.join("resources/api.toml"),
        rest_discover_resource_with_base_url(&base_url, "secret://file/rest-token"),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "api.items",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "rest-schema-secret");
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "api.items");
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert_eq!(
        report["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(report["snapshot_metadata"]["source_driver"], "rest");
    assert_eq!(report["snapshot_metadata"]["cdf:normalizer"], "namecase-v1");
    assert!(
        report["schema_snapshot_path"]
            .as_str()
            .unwrap()
            .starts_with(".cdf/schemas/api.items@sha256:")
    );
    let fields = report["fields"].as_array().unwrap();
    assert!(fields.iter().any(|field| field["name"] == "active"));
    let score = fields
        .iter()
        .find(|field| field["name"] == "score")
        .unwrap();
    assert_eq!(score["nullable"], true);
    let vendor = fields
        .iter()
        .find(|field| field["name"] == "vendor_id")
        .unwrap();
    assert_eq!(vendor["source_name"], "VendorID");
    assert_eq!(
        report["source_identity"]["driver.record_selector"],
        "$.items"
    );
    assert_eq!(report["source_identity"]["driver.sample_pages"], "1");
    assert_eq!(report["source_identity"]["driver.sample_records"], "3");

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("GET /items HTTP/1.1"));
    assert!(requests[0].contains("authorization: Bearer rest-schema-secret"));
}

#[test]
fn schema_discover_postgres_catalog_uses_project_secret_without_writes_or_secret_leak() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("catalog_discover_orders");
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"VendorID\" INTEGER NOT NULL,
                \"customer_uuid\" UUID,
                \"updated_at\" TIMESTAMP WITH TIME ZONE
            )",
            table
        ))
        .unwrap();

    let project = TestProject::new();
    let source_dsn = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:schema-discover-secret@",
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
        sql_discover_resource("secret://file/sql-dsn", &table),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "warehouse.orders",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert_secret_absent(&result, "schema-discover-secret");
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "warehouse.orders");
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["lockfile"], false);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert_eq!(
        report["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(report["snapshot_metadata"]["source_driver"], "postgres");
    assert_eq!(report["source_identity"]["driver.dialect"], "postgres");
    assert_eq!(report["source_identity"]["driver.table"], table);
    assert_eq!(report["snapshot_metadata"]["cdf:normalizer"], "namecase-v1");
    assert!(
        report["schema_snapshot_path"]
            .as_str()
            .unwrap()
            .starts_with(".cdf/schemas/warehouse.orders@sha256:")
    );
    assert_eq!(report["fields"][0]["name"], "vendor_id");
    assert_eq!(report["fields"][0]["nullable"], false);
    assert_eq!(report["fields"][0]["source_name"], "VendorID");
    assert_eq!(
        report["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(
        report["fields"][0]["metadata"]["cdf:physical_type"],
        "integer"
    );
    assert_eq!(report["fields"][1]["name"], "customer_uuid");
    assert_eq!(report["fields"][1]["metadata"]["cdf:physical_type"], "uuid");
    assert_eq!(report["fields"][2]["name"], "updated_at");
    assert_eq!(
        report["fields"][2]["metadata"]["cdf:physical_type"],
        "timestamp with time zone"
    );
    assert_eq!(report["source_identity"]["driver.source_kind"], "sql");
    assert_eq!(report["source_identity"]["driver.dialect"], "postgres");
    assert_eq!(report["source_identity"]["driver.table"], table);
    assert_eq!(report["next_command"], "cdf plan warehouse.orders");

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "warehouse.orders",
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert_secret_absent(&human, &source_dsn);
    assert_secret_absent(&human, "schema-discover-secret");
    assert!(human.stdout.contains("registered-source-discovery"));
    assert!(human.stdout.contains("postgres"));
}

#[test]
fn schema_pin_show_and_diff_local_parquet_snapshot_with_lockfile_reference() {
    let project = TestProject::new();
    write_minimal_lockfile(&project);
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);

    assert_eq!(pin.exit_code, 0, "stderr: {}", pin.stderr);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let pin_json = stderr_or_stdout_json(&pin.stdout);
    let pin_report = &pin_json["result"];
    assert_eq!(pin_report["resource_id"], "local.events");
    assert_eq!(pin_report["status"], "added");
    assert_eq!(pin_report["writes"]["schema_snapshot"], true);
    assert_eq!(pin_report["writes"]["lockfile"], true);
    assert_eq!(pin_report["writes"]["package"], false);
    assert_eq!(pin_report["fields"][0]["name"], "vendor_id");
    let snapshot_path = pin_report["schema_snapshot_path"].as_str().unwrap();
    assert!(project.root.join(snapshot_path).is_file());

    let lock_text = fs::read_to_string(project.root.join("cdf.lock")).unwrap();
    let lock = parse_lock(&lock_text).unwrap();
    let locked = lock.resources.get("local.events").unwrap();
    assert_eq!(locked.schema_snapshot.as_ref().unwrap().path, snapshot_path);
    assert_eq!(
        locked
            .schema_snapshot
            .as_ref()
            .unwrap()
            .schema_hash
            .as_str(),
        pin_report["schema_hash"].as_str().unwrap()
    );

    let show = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "show",
        "local.events",
    ]);

    assert_eq!(show.exit_code, 0, "stderr: {}", show.stderr);
    let show_json = stderr_or_stdout_json(&show.stdout);
    let show_report = &show_json["result"];
    assert_eq!(show_report["schema_hash"], pin_report["schema_hash"]);
    assert_eq!(show_report["fields"][0]["source_name"], "VendorID");
    assert_eq!(show_report["writes"]["schema_snapshot"], false);

    let diff = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "diff",
        "local.events",
    ]);

    assert_eq!(diff.exit_code, 0, "stderr: {}", diff.stderr);
    let diff_json = stderr_or_stdout_json(&diff.stdout);
    let diff_report = &diff_json["result"];
    assert_eq!(diff_report["summary"]["changed"], false);
    assert_eq!(diff_report["writes"]["schema_snapshot"], false);
    assert_eq!(diff_report["writes"]["lockfile"], false);

    let pin_again = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);

    assert_eq!(pin_again.exit_code, 0, "stderr: {}", pin_again.stderr);
    let pin_again_json = stderr_or_stdout_json(&pin_again.stdout);
    assert_eq!(pin_again_json["result"]["status"], "unchanged");
}

#[test]
fn schema_pin_without_lockfile_creates_semantic_lockfile_reference() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["writes"]["schema_snapshot"], true);
    assert_eq!(report["writes"]["lockfile"], true);
    assert_eq!(report["unsupported"], serde_json::json!([]));
    assert!(project.root.join(".cdf/schemas").exists());
    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert!(lock.resources["local.events"].schema_snapshot.is_some());
}

#[test]
fn schema_promote_plans_fresh_residual_correction_without_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    let pin_json = stderr_or_stdout_json(&pin.stdout);
    let pinned_hash = pin_json["result"]["schema_hash"].as_str().unwrap();

    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture(&project, pinned_hash);
    let before = project_tree_snapshot(&project.root);

    let planned = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(planned.exit_code, 0, "{}", planned.stderr);
    assert_project_tree_unchanged(&project.root, &before);
    let json = stderr_or_stdout_json(&planned.stdout);
    let report = &json["result"];
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["evidence_inventory_complete"], true);
    assert_eq!(report["paths"][0]["path"], "/score", "{report}");
    assert_eq!(report["paths"][0]["source_name"], "score", "{report}");
    assert_eq!(report["paths"][0]["selected_type"], "Int64");
    assert_eq!(report["paths"][0]["observed_count"], 2);
    assert!(
        report["paths"][0]["affected_address_value_digest"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(report["evidence"][0]["availability"], "retained_package");
    assert_eq!(report["targets"][0]["destination"], "duckdb");
    assert_eq!(
        report["paths"][0]["associations"][0]["target"],
        report["targets"][0]["target"]
    );
    assert_eq!(
        report["paths"][0]["associations"][0]["package_hash"],
        report["paths"][0]["affected_packages"][0]
    );
    assert_eq!(report["targets"][0]["strategy"], "in_place_update");
    assert_eq!(report["targets"][0]["migrations"][0]["path"], "/score");
    assert_eq!(report["recovery_argv"][0], "cdf");
    assert!(
        report["recovery_command"]
            .as_str()
            .unwrap()
            .contains("--type /score=Int64")
    );
    assert!(
        report["proposed_snapshot"]["path"]
            .as_str()
            .unwrap()
            .contains(report["new_schema_hash"].as_str().unwrap())
    );
    assert_eq!(
        report["proposed_snapshot"]["artifact"]["version"],
        cdf_project::SCHEMA_SNAPSHOT_ARTIFACT_VERSION
    );
    let repeated = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(repeated.exit_code, 0, "{}", repeated.stderr);
    assert_eq!(project_tree_snapshot(&project.root), before);
    let repeated_json = stderr_or_stdout_json(&repeated.stdout);
    assert_eq!(
        repeated_json["result"]["promotion_id"],
        report["promotion_id"]
    );
    assert_eq!(
        repeated_json["result"]["new_schema_hash"],
        report["new_schema_hash"]
    );

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(human.exit_code, 0, "{}", human.stderr);
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert!(human.stdout.contains("retained_package"));
    assert!(human.stdout.contains("in_place_update"));
    assert!(human.stdout.contains("score"));
    assert!(human.stdout.contains("Writes"));
    assert!(human.stdout.contains("Fresh discovery identity"));
    assert!(human.stdout.contains("Target evidence"));
    assert!(human.stdout.contains("receipt verification"));
    assert!(human.stdout.contains("preserved"));
    assert!(human.stdout.contains("Execution preconditions"));

    let invalid = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--type",
        "/score=not-an-arrow-type",
    ]);
    assert_ne!(invalid.exit_code, 0);
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert!(invalid.stderr.contains("invalid Arrow type declaration"));

    let unknown = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--type",
        "/missing=int64",
    ]);
    assert_eq!(unknown.exit_code, 0, "{}", unknown.stderr);
    assert_eq!(project_tree_snapshot(&project.root), before);
    let unknown_json = stderr_or_stdout_json(&unknown.stdout);
    assert!(
        unknown_json["result"]["conflicts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|conflict| conflict["code"] == "unknown_path")
    );

    let mut stale_lock =
        parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    stale_lock
        .resources
        .get_mut("local.events")
        .unwrap()
        .schema_snapshot
        .as_mut()
        .unwrap()
        .schema_hash = SchemaHash::new("sha256:stale-pin").unwrap();
    fs::write(
        project.root.join("cdf.lock"),
        cdf_project::lock_to_toml(&stale_lock).unwrap(),
    )
    .unwrap();
    let stale_before = project_tree_snapshot(&project.root);
    let stale = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_ne!(stale.exit_code, 0);
    assert_eq!(project_tree_snapshot(&project.root), stale_before);
    let stale_json = stderr_or_stdout_json(&stale.stderr);
    assert_eq!(stale_json["error"]["kind"], "data");
    assert!(
        stale_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("stale-pin"),
        "{}",
        stale.stderr
    );
}

#[test]
fn schema_promote_execute_commits_correction_checkpoint_lock_and_idempotent_publication() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    let resource_path = project.root.join("resources/files.toml");
    let resource_text = fs::read_to_string(&resource_path).unwrap();
    fs::write(
        &resource_path,
        resource_text.replace(
            "trust = \"governed\"",
            "trust = \"governed\"\ncontract = \"events-contract\"",
        ),
    )
    .unwrap();
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    let pin_json = stderr_or_stdout_json(&pin.stdout);
    let old_hash = pin_json["result"]["schema_hash"]
        .as_str()
        .unwrap()
        .to_owned();
    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture(&project, &old_hash);

    let executed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_eq!(executed.exit_code, 0, "{}", executed.stderr);
    let json = stderr_or_stdout_json(&executed.stdout);
    let report = &json["result"];
    assert_eq!(report["phase"], "complete");
    assert_eq!(report["lock_published"], true);
    assert_eq!(report["publication_event_recorded"], true);
    assert_eq!(report["targets"][0]["committed"], true);
    assert!(
        report["recovery_command"]
            .as_str()
            .unwrap()
            .ends_with("--execute")
    );
    let new_hash = report["new_schema_hash"].as_str().unwrap();
    assert_ne!(new_hash, old_hash);

    let correction_package = fs::read_dir(project.root.join(".cdf/packages"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.file_name().unwrap() != "pkg-promote-source")
        .unwrap();
    let replay_inputs = PackageReader::open(correction_package)
        .unwrap()
        .replay_inputs()
        .unwrap();
    assert_eq!(
        replay_inputs.state_delta.scope,
        ScopeKey::SchemaContract {
            contract: cdf_kernel::ContractRef::new("events-contract").unwrap(),
        }
    );

    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert_eq!(
        lock.resources["local.events"]
            .schema_snapshot
            .as_ref()
            .unwrap()
            .schema_hash
            .as_str(),
        new_hash
    );
    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows = conn
        .prepare("SELECT vendor_id, score, _cdf_variant FROM events ORDER BY _cdf_row_key")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i32>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![(1, 10, None), (2, 20, None)]);
    drop(conn);

    let replay = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--type",
        "/score=Int64",
        "--execute",
    ]);
    assert_eq!(replay.exit_code, 0, "{}", replay.stderr);
    let replay_json = stderr_or_stdout_json(&replay.stdout);
    assert_eq!(
        replay_json["result"]["promotion_id"],
        report["promotion_id"]
    );
    assert_eq!(replay_json["result"]["resumed"], true);
}

#[test]
fn sampled_pin_captures_unseen_field_then_fresh_discovery_promotes_without_source_replay() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    set_file_resource_sample_files(&project, 2);
    write_vendor_parquet(&project.root.join("data/a.parquet"));
    write_vendor_score_parquet(&project.root.join("data/middle.parquet"));
    write_vendor_parquet(&project.root.join("data/z.parquet"));

    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    assert_eq!(
        stderr_or_stdout_json(&pin.stdout)["result"]["discovery"]["file_coverage"],
        "sampled_files"
    );
    let before_preview = project_tree_snapshot(&project.root);
    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let preview_report = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_report["result"]["residual_row_count"], 2);
    assert_eq!(preview_report["result"]["quarantined_row_count"], 0);
    assert_eq!(project_tree_snapshot(&project.root), before_preview);
    let loaded = run_valid_run_args(&project);
    assert_eq!(loaded.exit_code, 0, "{}", loaded.stderr);
    let loaded_package_dir = run_package_dir(&project, &loaded);
    let package_reader = PackageReader::open(&loaded_package_dir).unwrap();
    let package_variant_rows = collect_package_segments_for_test(&package_reader)
        .into_iter()
        .flat_map(|(_, batches)| batches)
        .map(|batch| {
            let variant = batch
                .column_by_name("_cdf_variant")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            variant.iter().filter(Option::is_some).count()
        })
        .sum::<usize>();
    assert_eq!(package_variant_rows, 2);
    let connection = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let variant_rows = connection
        .query_row(
            "SELECT count(*) FROM events WHERE _cdf_variant IS NOT NULL",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(variant_rows, 2);
    drop(connection);

    let resource_path = project.root.join("resources/files.toml");
    let resource = fs::read_to_string(&resource_path)
        .unwrap()
        .replace("sample_files = 2", "sample_files = 3");
    fs::write(resource_path, resource).unwrap();
    let dry = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
    let dry_report = stderr_or_stdout_json(&dry.stdout);
    assert_eq!(dry_report["result"]["executable"], true);
    assert_eq!(dry_report["result"]["paths"][0]["path"], "/score");
    let before_repeated_plan = project_tree_snapshot(&project.root);
    let repeated = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(repeated.exit_code, 0, "{}", repeated.stderr);
    assert_eq!(
        stderr_or_stdout_json(&repeated.stdout)["result"],
        dry_report["result"]
    );
    assert_eq!(project_tree_snapshot(&project.root), before_repeated_plan);

    let promoted = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_eq!(promoted.exit_code, 0, "{}", promoted.stderr);
    let report = stderr_or_stdout_json(&promoted.stdout);
    assert_eq!(report["result"]["phase"], "complete");
    let connection = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let promoted_rows = connection
        .prepare("SELECT score, _cdf_variant FROM events ORDER BY _cdf_row_key, vendor_id")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<i64>>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        promoted_rows
            .iter()
            .filter(|(score, _)| score.is_some())
            .count(),
        2
    );
    assert!(promoted_rows.iter().all(|(_, residual)| residual.is_none()));
}

#[test]
fn schema_promote_multi_target_uses_canonical_checkpoint_chain_and_exact_publication() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    let old_hash = stderr_or_stdout_json(&pin.stdout)["result"]["schema_hash"]
        .as_str()
        .unwrap()
        .to_owned();
    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture_for_target(
        &project,
        "pkg-promote-z",
        "z_events",
        &old_hash,
    );
    write_schema_promote_package_fixture_for_target(
        &project,
        "pkg-promote-a",
        "a_events",
        &old_hash,
    );

    let dry = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
    let plan: SchemaPromotionPlanReport =
        serde_json::from_value(stderr_or_stdout_json(&dry.stdout)["result"].clone()).unwrap();
    let context = crate::context::ProjectContext::load(Some(&project.root), None).unwrap();
    let destinations = plan
        .targets
        .iter()
        .map(|target| {
            crate::destination_uri::resolve_environment_destination(
                &test_destination_registry(),
                &context,
                &TargetName::new(target.target.clone()).unwrap(),
            )
            .unwrap()
            .destination
        })
        .collect();
    let store = SqlitePromotionSettlementStore::open(context.state_store_path().unwrap()).unwrap();
    let failure = execute_schema_promotion(SchemaPromotionExecutionRequest {
        project_root: &context.root,
        package_root: &context.package_root(),
        resource: context.resource("local.events").unwrap(),
        lock: context.lock.as_ref().unwrap(),
        lock_authority: context.lock_authority.as_ref().unwrap(),
        dry_plan: &plan,
        destinations,
        execution_services: test_execution_services(),
        pipeline_id: PipelineId::new("cdf-schema-promotion").unwrap(),
        lease_owner: LeaseOwnerId::new("multi-target-crash").unwrap(),
        lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
        settlement_store: &store,
        failpoint: Some(SchemaPromotionExecutionFailpoint::AfterTargetCheckpointIndex(1)),
    })
    .unwrap_err();
    assert!(failure.message.contains("schema promotion failpoint"));
    drop(store);
    drop(context);
    fs::remove_dir_all(project.root.join(".cdf/packages/pkg-promote-a")).unwrap();
    fs::remove_dir_all(project.root.join(".cdf/packages/pkg-promote-z")).unwrap();

    let executed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--type",
        "/score=Int64",
        "--execute",
    ]);
    assert_eq!(executed.exit_code, 0, "{}", executed.stderr);
    let report = stderr_or_stdout_json(&executed.stdout)["result"].clone();
    assert_eq!(report["resumed"], true);
    let targets = report["targets"].as_array().unwrap();
    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0]["target"], "a_events");
    assert_eq!(targets[1]["target"], "z_events");

    let store = SqlitePromotionSettlementStore::open(project.root.join(".cdf/state.db")).unwrap();
    let scope = ScopeKey::SchemaContract {
        contract: cdf_kernel::ContractRef::new("local.events").unwrap(),
    };
    let history = store
        .history(
            &PipelineId::new("cdf-schema-promotion").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &scope,
        )
        .unwrap();
    let committed = history
        .iter()
        .filter(|checkpoint| checkpoint.status == CheckpointStatus::Committed)
        .collect::<Vec<_>>();
    assert_eq!(committed.len(), 2);
    assert_eq!(
        committed[1].delta.parent_checkpoint_id.as_ref(),
        Some(&committed[0].delta.checkpoint_id)
    );
    assert_eq!(
        committed[1].delta.input_position.as_ref(),
        Some(&committed[0].delta.output_position)
    );
    let publication = store
        .promotion_publication(
            &cdf_kernel::PromotionId::new(report["promotion_id"].as_str().unwrap()).unwrap(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(publication.targets.len(), 2);
    assert_eq!(publication.targets[0].target.as_str(), "a_events");
    assert_eq!(publication.targets[1].target.as_str(), "z_events");
    assert_eq!(
        publication.targets[1].checkpoint_id,
        committed[1].delta.checkpoint_id
    );
}

#[test]
fn schema_promote_execute_recovers_every_persisted_crash_boundary() {
    for failpoint in [
        SchemaPromotionExecutionFailpoint::AfterStagedArtifacts,
        SchemaPromotionExecutionFailpoint::AfterCorrectionPackages,
        SchemaPromotionExecutionFailpoint::AfterDestinationReceipt,
        SchemaPromotionExecutionFailpoint::AfterTargetCheckpoint,
        SchemaPromotionExecutionFailpoint::AfterLockPublication,
        SchemaPromotionExecutionFailpoint::AfterPublicationEvent,
    ] {
        let project = TestProject::new();
        write_parquet_discover_resource(&project, "*.parquet");
        let source_path = project.root.join("data/events.parquet");
        write_vendor_parquet(&source_path);
        let pin = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "pin",
            "local.events",
        ]);
        assert_eq!(pin.exit_code, 0, "{failpoint:?}: {}", pin.stderr);
        let pin_json = stderr_or_stdout_json(&pin.stdout);
        let old_hash = pin_json["result"]["schema_hash"].as_str().unwrap();
        write_vendor_score_parquet(&source_path);
        write_schema_promote_package_fixture(&project, old_hash);
        let dry = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
        ]);
        assert_eq!(dry.exit_code, 0, "{failpoint:?}: {}", dry.stderr);
        let dry_json = stderr_or_stdout_json(&dry.stdout);
        let plan: SchemaPromotionPlanReport =
            serde_json::from_value(dry_json["result"].clone()).unwrap();

        let context = crate::context::ProjectContext::load(Some(&project.root), None).unwrap();
        let resource = context.resource("local.events").unwrap();
        let target = TargetName::new(plan.targets[0].target.clone()).unwrap();
        let destination = crate::destination_uri::resolve_environment_destination(
            &test_destination_registry(),
            &context,
            &target,
        )
        .unwrap()
        .destination;
        let state_path = context.state_store_path().unwrap();
        let settlement_store = SqlitePromotionSettlementStore::open(&state_path).unwrap();
        let run_ledger = SqliteRunLedger::open(&state_path).unwrap();
        let error = execute_schema_promotion(SchemaPromotionExecutionRequest {
            project_root: &context.root,
            package_root: &context.package_root(),
            resource,
            lock: context.lock.as_ref().unwrap(),
            lock_authority: context.lock_authority.as_ref().unwrap(),
            dry_plan: &plan,
            destinations: vec![destination],
            execution_services: test_execution_services(),
            pipeline_id: PipelineId::new("cdf-schema-promotion").unwrap(),
            lease_owner: LeaseOwnerId::new(format!("crash-{failpoint:?}")).unwrap(),
            lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
            settlement_store: &settlement_store,
            failpoint: Some(failpoint),
        })
        .unwrap_err();
        assert!(
            error.message.contains("schema promotion failpoint"),
            "{failpoint:?}: {error}"
        );
        let expected_phase = match failpoint {
            SchemaPromotionExecutionFailpoint::AfterStagedArtifacts => {
                SchemaPromotionExecutionPhase::Staged
            }
            SchemaPromotionExecutionFailpoint::AfterCorrectionPackages => {
                SchemaPromotionExecutionPhase::Packaged
            }
            SchemaPromotionExecutionFailpoint::AfterDestinationReceipt => {
                SchemaPromotionExecutionPhase::DestinationSettled
            }
            SchemaPromotionExecutionFailpoint::AfterTargetCheckpoint => {
                SchemaPromotionExecutionPhase::Checkpointed
            }
            SchemaPromotionExecutionFailpoint::AfterLockPublication => {
                SchemaPromotionExecutionPhase::LockPublished
            }
            SchemaPromotionExecutionFailpoint::AfterPublicationEvent => {
                SchemaPromotionExecutionPhase::Complete
            }
            SchemaPromotionExecutionFailpoint::AfterTargetCheckpointIndex(_) => unreachable!(),
        };
        let status = load_schema_promotion_recovery_status(
            &project.root,
            &cdf_kernel::PromotionId::new(plan.promotion_id.clone()).unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(status.phase, expected_phase, "{failpoint:?}");
        assert!(status.recovery_command.ends_with("--execute"));
        drop(run_ledger);
        drop(settlement_store);
        drop(context);

        if failpoint != SchemaPromotionExecutionFailpoint::AfterStagedArtifacts {
            fs::remove_dir_all(project.root.join(".cdf/packages/pkg-promote-source")).unwrap();
            let correction_packages = fs::read_dir(project.root.join(".cdf/packages"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.join(MANIFEST_FILE).is_file())
                .collect::<Vec<_>>();
            assert_eq!(correction_packages.len(), 1, "{failpoint:?}");
            PackageReader::open(&correction_packages[0])
                .unwrap()
                .replay_inputs()
                .unwrap();
        }

        let recovered = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
            "--type",
            "/score=Int64",
            "--execute",
        ]);
        assert_eq!(
            recovered.exit_code, 0,
            "{failpoint:?}: {}",
            recovered.stderr
        );
        let recovered_json = stderr_or_stdout_json(&recovered.stdout);
        assert_eq!(recovered_json["result"]["phase"], "complete");
        assert_eq!(recovered_json["result"]["resumed"], true);
        let ledger = SqliteRunLedger::open_read_only(&state_path).unwrap();
        assert!(
            ledger
                .promotion_publication(&cdf_kernel::PromotionId::new(plan.promotion_id).unwrap())
                .unwrap()
                .is_some(),
            "{failpoint:?}"
        );
    }
}

#[test]
fn schema_promote_failure_reports_persisted_recovery_status_without_secret_leak() {
    let project = TestProject::new();
    let secret = format!(
        "postgresql://cdf:promotion-secret@127.0.0.1:{}/cdf",
        free_port()
    );
    fs::write(project.root.join("destination-dsn"), format!("{secret}\n")).unwrap();
    write_project_destination_with_postgres_policy(
        &project,
        "postgres://secret://file/destination-dsn",
        "fail",
    );
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    let old_hash = stderr_or_stdout_json(&pin.stdout)["result"]["schema_hash"]
        .as_str()
        .unwrap()
        .to_owned();
    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture(&project, &old_hash);
    let source_package = project.root.join(".cdf/packages/pkg-promote-source");
    let mut receipts = collect_package_receipts(&PackageReader::open(&source_package).unwrap());
    receipts[0].destination = DestinationId::new("postgres").unwrap();
    fs::write(
        source_package.join(RECEIPTS_FILE),
        cdf_package::canonical_json_bytes(&receipts).unwrap(),
    )
    .unwrap();

    let json_failure = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_ne!(json_failure.exit_code, 0);
    assert_secret_absent(&json_failure, "promotion-secret");
    assert_secret_absent(&json_failure, &secret);
    let error = stderr_or_stdout_json(&json_failure.stderr);
    assert_eq!(
        error["error"]["details"]["phase"], "staged",
        "{}",
        json_failure.stderr
    );
    assert_eq!(
        error["error"]["details"]["remaining_action"],
        "build authenticated correction packages"
    );
    assert!(
        error["error"]["details"]["recovery_command"]
            .as_str()
            .unwrap()
            .ends_with("--execute")
    );

    let human_failure = run([
        "cdf",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_ne!(human_failure.exit_code, 0);
    assert_secret_absent(&human_failure, "promotion-secret");
    assert!(human_failure.stderr.contains("phase: staged"));
    assert!(human_failure.stderr.contains("recovery_command:"));
    assert!(
        project_tree_snapshot(&project.root)
            .into_iter()
            .filter(|(path, _)| path != "destination-dsn")
            .all(|(_, bytes)| !String::from_utf8_lossy(&bytes).contains("promotion-secret"))
    );
}

#[test]
fn schema_promote_rejects_tampered_staged_and_correction_authority_before_mutation() {
    for tamper_correction_package in [false, true] {
        let project = TestProject::new();
        write_parquet_discover_resource(&project, "*.parquet");
        let source_path = project.root.join("data/events.parquet");
        write_vendor_parquet(&source_path);
        let pin = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "pin",
            "local.events",
        ]);
        assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
        let old_hash = stderr_or_stdout_json(&pin.stdout)["result"]["schema_hash"]
            .as_str()
            .unwrap()
            .to_owned();
        write_vendor_score_parquet(&source_path);
        write_schema_promote_package_fixture(&project, &old_hash);
        let dry = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
        ]);
        assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
        let plan: SchemaPromotionPlanReport =
            serde_json::from_value(stderr_or_stdout_json(&dry.stdout)["result"].clone()).unwrap();
        let context = crate::context::ProjectContext::load(Some(&project.root), None).unwrap();
        let resource = context.resource("local.events").unwrap();
        let target = TargetName::new(plan.targets[0].target.clone()).unwrap();
        let destination = crate::destination_uri::resolve_environment_destination(
            &test_destination_registry(),
            &context,
            &target,
        )
        .unwrap()
        .destination;
        let state_path = context.state_store_path().unwrap();
        let settlement_store = SqlitePromotionSettlementStore::open(&state_path).unwrap();
        let run_ledger = SqliteRunLedger::open(&state_path).unwrap();
        let failpoint = if tamper_correction_package {
            SchemaPromotionExecutionFailpoint::AfterCorrectionPackages
        } else {
            SchemaPromotionExecutionFailpoint::AfterStagedArtifacts
        };
        execute_schema_promotion(SchemaPromotionExecutionRequest {
            project_root: &context.root,
            package_root: &context.package_root(),
            resource,
            lock: context.lock.as_ref().unwrap(),
            lock_authority: context.lock_authority.as_ref().unwrap(),
            dry_plan: &plan,
            destinations: vec![destination],
            execution_services: test_execution_services(),
            pipeline_id: PipelineId::new("cdf-schema-promotion").unwrap(),
            lease_owner: LeaseOwnerId::new("tamper-fixture").unwrap(),
            lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
            settlement_store: &settlement_store,
            failpoint: Some(failpoint),
        })
        .unwrap_err();
        drop(run_ledger);
        drop(settlement_store);
        drop(context);

        if tamper_correction_package {
            let correction = fs::read_dir(project.root.join(".cdf/packages"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| path.file_name().unwrap() != "pkg-promote-source")
                .unwrap();
            let artifact = correction.join("plan/promotion-correction.json");
            let mut bytes = fs::read(&artifact).unwrap();
            bytes.push(b' ');
            fs::write(&artifact, bytes).unwrap();
            fs::remove_dir_all(project.root.join(".cdf/packages/pkg-promote-source")).unwrap();
        } else {
            let staged = project.root.join(cdf_project::promotion_plan_relative_path(
                &cdf_kernel::PromotionId::new(plan.promotion_id.clone()).unwrap(),
            ));
            let mut artifact: cdf_project::SchemaPromotionExecutionPlanArtifact =
                serde_json::from_slice(&fs::read(&staged).unwrap()).unwrap();
            artifact.dry_plan.targets[0]
                .affected_packages
                .push("sha256:forged".to_owned());
            let forged = cdf_project::recompute_schema_promotion_id(&artifact.dry_plan).unwrap();
            artifact.promotion_id = forged.clone();
            artifact.dry_plan.promotion_id = forged.to_string();
            fs::write(&staged, serde_json::to_vec_pretty(&artifact).unwrap()).unwrap();
        }

        let recovered = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
            "--type",
            "/score=Int64",
            "--execute",
        ]);
        assert_ne!(recovered.exit_code, 0, "{}", recovered.stdout);
        let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
        assert_eq!(
            lock.resources["local.events"]
                .schema_snapshot
                .as_ref()
                .unwrap()
                .schema_hash
                .as_str(),
            old_hash
        );
        let connection = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
        let score_columns = connection
            .prepare("SELECT count(*) FROM pragma_table_info('events') WHERE name = 'score'")
            .unwrap()
            .query_row([], |row| row.get::<_, i64>(0))
            .unwrap();
        assert_eq!(score_columns, 0);
    }
}

#[test]
fn schema_promote_api_rejects_divergent_caller_lock_before_mutation() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    let old_hash = stderr_or_stdout_json(&pin.stdout)["result"]["schema_hash"]
        .as_str()
        .unwrap()
        .to_owned();
    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture(&project, &old_hash);
    let dry = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
    let plan: SchemaPromotionPlanReport =
        serde_json::from_value(stderr_or_stdout_json(&dry.stdout)["result"].clone()).unwrap();
    let context = crate::context::ProjectContext::load(Some(&project.root), None).unwrap();
    let resource = context.resource("local.events").unwrap();
    let target = TargetName::new(plan.targets[0].target.clone()).unwrap();
    let destination = crate::destination_uri::resolve_environment_destination(
        &test_destination_registry(),
        &context,
        &target,
    )
    .unwrap()
    .destination;
    let state_path = context.state_store_path().unwrap();
    let settlement_store = SqlitePromotionSettlementStore::open(&state_path).unwrap();
    let run_ledger = SqliteRunLedger::open(&state_path).unwrap();
    let mut divergent_lock = context.lock.as_ref().unwrap().clone();
    divergent_lock.normalizer = "divergent-caller-projection".to_owned();
    let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();

    let error = execute_schema_promotion(SchemaPromotionExecutionRequest {
        project_root: &context.root,
        package_root: &context.package_root(),
        resource,
        lock: &divergent_lock,
        lock_authority: context.lock_authority.as_ref().unwrap(),
        dry_plan: &plan,
        destinations: vec![destination],
        execution_services: test_execution_services(),
        pipeline_id: PipelineId::new("cdf-schema-promotion").unwrap(),
        lease_owner: LeaseOwnerId::new("divergent-lock-fixture").unwrap(),
        lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
        settlement_store: &settlement_store,
        failpoint: None,
    })
    .unwrap_err();

    assert!(error.message.contains("caller lock projection"));
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
    assert!(!project.root.join(".cdf/promotions").exists());
    assert_eq!(
        fs::read_dir(project.root.join(".cdf/packages"))
            .unwrap()
            .count(),
        1
    );
    assert!(
        run_ledger
            .promotion_publication(&cdf_kernel::PromotionId::new(plan.promotion_id).unwrap())
            .unwrap()
            .is_none()
    );
}

#[test]
fn schema_promote_rejects_semantically_rebuilt_correction_packages_without_sources() {
    for tamper in [
        CorrectionSemanticRepackage::Subset,
        CorrectionSemanticRepackage::ValueSubstitution,
    ] {
        let project = TestProject::new();
        write_parquet_discover_resource(&project, "*.parquet");
        let source_path = project.root.join("data/events.parquet");
        write_vendor_parquet(&source_path);
        let pin = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "pin",
            "local.events",
        ]);
        assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
        let old_hash = stderr_or_stdout_json(&pin.stdout)["result"]["schema_hash"]
            .as_str()
            .unwrap()
            .to_owned();
        write_vendor_score_parquet(&source_path);
        write_schema_promote_package_fixture(&project, &old_hash);
        let dry = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
        ]);
        assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
        let plan: SchemaPromotionPlanReport =
            serde_json::from_value(stderr_or_stdout_json(&dry.stdout)["result"].clone()).unwrap();
        let context = crate::context::ProjectContext::load(Some(&project.root), None).unwrap();
        let resource = context.resource("local.events").unwrap();
        let target = TargetName::new(plan.targets[0].target.clone()).unwrap();
        let destination = crate::destination_uri::resolve_environment_destination(
            &test_destination_registry(),
            &context,
            &target,
        )
        .unwrap()
        .destination;
        let state_path = context.state_store_path().unwrap();
        let settlement_store = SqlitePromotionSettlementStore::open(&state_path).unwrap();
        let run_ledger = SqliteRunLedger::open(&state_path).unwrap();
        execute_schema_promotion(SchemaPromotionExecutionRequest {
            project_root: &context.root,
            package_root: &context.package_root(),
            resource,
            lock: context.lock.as_ref().unwrap(),
            lock_authority: context.lock_authority.as_ref().unwrap(),
            dry_plan: &plan,
            destinations: vec![destination],
            execution_services: test_execution_services(),
            pipeline_id: PipelineId::new("cdf-schema-promotion").unwrap(),
            lease_owner: LeaseOwnerId::new("semantic-repackage-fixture").unwrap(),
            lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
            settlement_store: &settlement_store,
            failpoint: Some(SchemaPromotionExecutionFailpoint::AfterCorrectionPackages),
        })
        .unwrap_err();
        drop(run_ledger);
        drop(settlement_store);
        drop(context);

        let correction = fs::read_dir(project.root.join(".cdf/packages"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.file_name().unwrap() != "pkg-promote-source")
            .unwrap();
        rebuild_correction_package_semantically(&correction, tamper);
        fs::remove_dir_all(project.root.join(".cdf/packages/pkg-promote-source")).unwrap();
        let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();

        let recovered = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
            "--type",
            "/score=Int64",
            "--execute",
        ]);
        assert_ne!(recovered.exit_code, 0, "{}", recovered.stdout);
        assert_eq!(
            fs::read(project.root.join("cdf.lock")).unwrap(),
            lock_before
        );
        assert!(collect_package_receipts(&PackageReader::open(&correction).unwrap()).is_empty());
        let connection = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
        let score_columns = connection
            .prepare("SELECT count(*) FROM pragma_table_info('events') WHERE name = 'score'")
            .unwrap()
            .query_row([], |row| row.get::<_, i64>(0))
            .unwrap();
        assert_eq!(score_columns, 0);
        let ledger = SqliteRunLedger::open_read_only(&state_path).unwrap();
        assert!(
            ledger
                .promotion_publication(&cdf_kernel::PromotionId::new(plan.promotion_id).unwrap())
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn schema_promote_execute_routes_parquet_through_correction_sidecar() {
    let project = TestProject::new();
    let project_toml = fs::read_to_string(project.root.join("cdf.toml"))
        .unwrap()
        .replace("duckdb://.cdf/dev.duckdb", "parquet://.cdf/parquet");
    fs::write(project.root.join("cdf.toml"), project_toml).unwrap();
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    let pin_json = stderr_or_stdout_json(&pin.stdout);
    let old_hash = pin_json["result"]["schema_hash"].as_str().unwrap();
    let target = TargetName::new("events").unwrap();
    let policy = cdf_project::DestinationPolicy::default();
    let services = test_execution_services();
    let resolution = cdf_project::ProjectResolutionContext::for_project_run(&project.root, &target)
        .with_environment_name("dev")
        .with_destination_policy(&policy)
        .with_execution_services(&services);
    let registry = crate::destination_registry::builtin_destination_registry().unwrap();
    let mut runtime = registry
        .resolve("parquet://.cdf/parquet", &resolution)
        .unwrap();
    runtime.ensure_protocol_ready().unwrap();
    let mut lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let artifact = runtime.protocol().sheet_artifact().unwrap();
    lock.destinations.insert(
        artifact.sheet.destination.to_string(),
        cdf_project::LockedDestination::new(artifact).unwrap(),
    );
    fs::write(
        project.root.join("cdf.lock"),
        cdf_project::lock_to_toml(&lock).unwrap(),
    )
    .unwrap();
    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture_for_target_with_commit(
        &project,
        "pkg-promote-source",
        "events",
        old_hash,
        false,
    );
    let source_package = project.root.join(".cdf/packages/pkg-promote-source");
    let store = SqliteCheckpointStore::open(
        project
            .root
            .join(".cdf/schema-promote-parquet-fixture-state.db"),
    )
    .unwrap();
    replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: source_package,
        destination: ResolvedProjectDestination::new(
            Box::new(
                ParquetDestination::new_filesystem(
                    project.root.join(".cdf/parquet"),
                    services.clone(),
                )
                .unwrap(),
            ),
            target.clone(),
        )
        .with_bound_execution_services(services)
        .unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: None,
    })
    .unwrap();

    let dry = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
    let dry_json = stderr_or_stdout_json(&dry.stdout);
    assert_eq!(dry_json["result"]["executable"], true, "{}", dry.stdout);

    let executed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_eq!(executed.exit_code, 0, "{}", executed.stderr);
    let json = stderr_or_stdout_json(&executed.stdout);
    assert_eq!(json["result"]["phase"], "complete");
    assert_eq!(
        json["result"]["targets"][0]["destination"],
        "parquet_object_store"
    );
    assert_eq!(json["result"]["targets"][0]["committed"], true);
    assert_eq!(json["result"]["lock_published"], true);
    assert_eq!(json["result"]["publication_event_recorded"], true);
    assert!(
        project_tree_snapshot(&project.root)
            .keys()
            .any(|path| path.starts_with(".cdf/parquet/targets/events/corrections/manifests/"))
    );
}

#[test]
fn schema_promote_execute_updates_postgres_through_generic_command_dispatch() {
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
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    let old_hash = stderr_or_stdout_json(&pin.stdout)["result"]["schema_hash"]
        .as_str()
        .unwrap()
        .to_owned();
    write_vendor_score_parquet(&source_path);
    let target = postgres.table("events_promotion");
    write_schema_promote_package_fixture_for_target_with_commit(
        &project,
        "pkg-promote-source",
        &target,
        &old_hash,
        false,
    );
    let package_dir = project.root.join(".cdf/packages/pkg-promote-source");
    let reader = PackageReader::open(&package_dir).unwrap();
    let package_hash = PackageHash::new(reader.manifest().package_hash.clone()).unwrap();
    let delta = reader
        .state_delta_preimage()
        .unwrap()
        .into_state_delta(package_hash.clone());
    let segment = &delta.segments[0];
    let batches = reader
        .verified_canonical_segment_stream(test_execution_services().memory(), 128 * 1024 * 1024)
        .unwrap()
        .find_map(|candidate| {
            let candidate = candidate.unwrap();
            (candidate.entry.segment_id == segment.segment_id).then_some(candidate.batches)
        })
        .unwrap();
    let residuals = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let row_key_start = 1_i64;
    let row_key_end = row_key_start + segment.row_count as i64;
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (vendor_id INTEGER NOT NULL, _cdf_variant TEXT, _cdf_row_key BIGINT NOT NULL, _cdf_loaded_at_ms BIGINT NOT NULL); \
             CREATE TABLE {}._cdf_segments (row_key_start BIGINT PRIMARY KEY, row_key_end BIGINT NOT NULL, target TEXT NOT NULL, package_hash TEXT NOT NULL, segment_id TEXT NOT NULL, CHECK (row_key_start < row_key_end), UNIQUE (target, package_hash, segment_id))",
            target, postgres.schema
        ))
        .unwrap();
    for (row, vendor_id) in [1_i32, 2_i32].into_iter().enumerate() {
        client
            .execute(
                &format!(
                    "INSERT INTO {} (vendor_id, _cdf_variant, _cdf_row_key, _cdf_loaded_at_ms) VALUES ($1, $2, $3, $4)",
                    target
                ),
                &[
                    &vendor_id,
                    &residuals.value(row),
                    &(row_key_start + row as i64),
                    &1_i64,
                ],
            )
            .unwrap();
    }
    client
        .execute(
            &format!(
                "INSERT INTO {}._cdf_segments (row_key_start, row_key_end, target, package_hash, segment_id) VALUES ($1, $2, $3, $4, $5)",
                postgres.schema
            ),
            &[
                &row_key_start,
                &row_key_end,
                &target,
                &package_hash.as_str(),
                &segment.segment_id.as_str(),
            ],
        )
        .unwrap();
    let receipt = Receipt {
            receipt_id: ReceiptId::new("receipt-postgres-promotion-source").unwrap(),
            destination: DestinationId::new("postgres").unwrap(),
            target: TargetName::new(target.clone()).unwrap(),
            package_hash: package_hash.clone(),
            segment_acks: vec![SegmentAck {
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            }],
            disposition: WriteDisposition::Append,
            idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
            transaction: None,
            counts: CommitCounts {
                rows_written: 2,
                rows_inserted: Some(2),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: SchemaHash::new(&old_hash).unwrap(),
            migrations: Vec::new(),
            committed_at_ms: now_ms_for_test(),
            verify: VerifyClause {
                kind: "postgres_sql".to_owned(),
                statement: "SELECT \"receipt_id\", \"xid\", \"rows_written\", \"schema_hash\", \"receipt_json\"::text AS \"receipt_json\" FROM \"_cdf_loads\" WHERE \"destination\" = 'postgres' AND \"target\" = $1 AND \"package_hash\" = $2 AND \"idempotency_token\" = $3 AND \"schema_hash\" = $4".to_owned(),
                parameters: BTreeMap::from([
                    ("target".to_owned(), target.clone()),
                    ("package_hash".to_owned(), package_hash.to_string()),
                    ("idempotency_token".to_owned(), package_hash.to_string()),
                    ("schema_hash".to_owned(), old_hash.clone()),
                    ("destination".to_owned(), "postgres".to_owned()),
                    ("target_schema".to_owned(), postgres.schema.clone()),
                ]),
            },
        };
    client
        .batch_execute(&format!(
            "CREATE TABLE {}._cdf_loads (receipt_id TEXT PRIMARY KEY, destination TEXT NOT NULL, target TEXT NOT NULL, resource_id TEXT, package_hash TEXT NOT NULL, idempotency_token TEXT NOT NULL, disposition TEXT NOT NULL, schema_hash TEXT NOT NULL, rows_written BIGINT NOT NULL, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT, segment_count BIGINT NOT NULL, migrations_json JSONB NOT NULL, receipt_json JSONB NOT NULL, xid TEXT NOT NULL, duplicate BOOLEAN NOT NULL DEFAULT FALSE, committed_at_ms BIGINT NOT NULL, UNIQUE (target, package_hash))",
            postgres.schema
        ))
        .unwrap();
    let receipt_json = serde_json::to_string(&receipt).unwrap();
    client
        .execute(
            &format!("INSERT INTO {}._cdf_loads (receipt_id, destination, target, resource_id, package_hash, idempotency_token, disposition, schema_hash, rows_written, rows_inserted, rows_updated, rows_deleted, segment_count, migrations_json, receipt_json, xid, duplicate, committed_at_ms) VALUES ($1, 'postgres', $2, 'local.events', $3, $4, 'append', $5, 2, 2, 0, 0, 1, '[]'::jsonb, $6::text::jsonb, 'fixture', false, $7)", postgres.schema),
            &[&receipt.receipt_id.as_str(), &target, &package_hash.as_str(), &package_hash.as_str(), &old_hash, &receipt_json, &receipt.committed_at_ms],
        )
        .unwrap();
    reader.append_receipt(receipt).unwrap();
    drop(client);

    let executed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_eq!(executed.exit_code, 0, "{}", executed.stderr);
    assert_secret_absent(&executed, &postgres.url);
    let report = stderr_or_stdout_json(&executed.stdout);
    assert_eq!(report["result"]["targets"][0]["destination"], "postgres");
    assert_eq!(report["result"]["targets"][0]["committed"], true);
    let rows = postgres
        .client()
        .query(
            &format!("SELECT vendor_id, score, _cdf_variant FROM {target} ORDER BY _cdf_row_key"),
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i64>(1), 10);
    assert_eq!(rows[0].get::<_, Option<String>>(2), None);
    assert_eq!(rows[1].get::<_, i64>(1), 20);
}

#[test]
fn schema_diff_rest_compares_pinned_snapshot_to_fresh_probe_without_writes_or_secret_leak() {
    let project = TestProject::new();
    write_minimal_lockfile(&project);
    fs::write(project.root.join("rest-token"), "rest-diff-secret\n").unwrap();
    let (base_url, requests) = serve_json_sequence([
        r#"{ "items": [{ "VendorID": 1, "updated_at": 10 }] }"#,
        r#"{ "items": [{ "VendorID": 1, "updated_at": 10, "score": 4.5 }] }"#,
    ]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    fs::write(
        project.root.join("resources/api.toml"),
        rest_discover_resource_with_base_url(&base_url, "secret://file/rest-token"),
    )
    .unwrap();

    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "api.items",
    ]);

    assert_eq!(pin.exit_code, 0, "stderr: {}", pin.stderr);
    assert_secret_absent(&pin, "rest-diff-secret");
    let pinned_snapshot_count = fs::read_dir(project.root.join(".cdf/schemas"))
        .unwrap()
        .count();
    assert!(pinned_snapshot_count >= 2);

    let diff = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "diff",
        "api.items",
    ]);

    assert_eq!(diff.exit_code, 0, "stderr: {}", diff.stderr);
    assert_secret_absent(&diff, "rest-diff-secret");
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert_eq!(
        fs::read_dir(project.root.join(".cdf/schemas"))
            .unwrap()
            .count(),
        pinned_snapshot_count
    );
    let diff_json = stderr_or_stdout_json(&diff.stdout);
    let report = &diff_json["result"];
    assert_eq!(report["summary"]["changed"], true);
    assert_eq!(report["summary"]["added_fields"], 1);
    assert_eq!(report["added_fields"][0]["name"], "score");
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["lockfile"], false);

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.contains("authorization: Bearer rest-diff-secret"))
    );
}

#[test]
fn schema_pin_postgres_catalog_updates_lock_without_secret_leak() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("schema_pin_orders");
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"VendorID\" INTEGER NOT NULL,
                \"updated_at\" BIGINT NOT NULL
            )",
            table
        ))
        .unwrap();

    let project = TestProject::new();
    write_minimal_lockfile(&project);
    let source_dsn = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:schema-pin-secret@",
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
        sql_discover_resource("secret://file/sql-dsn", &table),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "warehouse.orders",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert_secret_absent(&result, "schema-pin-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["writes"]["schema_snapshot"], true);
    assert_eq!(report["writes"]["lockfile"], true);
    assert_eq!(
        report["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(report["source_identity"]["driver.table"], table);
    assert_eq!(report["fields"][0]["source_name"], "VendorID");
    let lock_text = fs::read_to_string(project.root.join("cdf.lock")).unwrap();
    assert!(!lock_text.contains(&source_dsn));
    assert!(!lock_text.contains("schema-pin-secret"));
    assert!(
        parse_lock(&lock_text)
            .unwrap()
            .resources
            .get("warehouse.orders")
            .unwrap()
            .schema_snapshot
            .is_some()
    );
}

#[test]
fn plan_local_parquet_discover_autopins_snapshot_and_reports_hash() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["schema_snapshot"]["outcome"], "added");
    assert_eq!(report["schema_snapshot"]["snapshot_written"], true);
    assert_eq!(report["schema_snapshot"]["lockfile_written"], true);
    assert_eq!(report["resource_schema"]["schema_source"], "discovered");
    let snapshot_path = report["resource_schema"]["snapshot_path"].as_str().unwrap();
    assert!(snapshot_path.starts_with(".cdf/schemas/local.events@sha256:"));
    let snapshot = read_snapshot_json(&project, snapshot_path);
    assert_eq!(
        report["resource_schema"]["schema_hash"],
        snapshot["schema_hash"]
    );
    assert_eq!(
        report["resource_schema"]["baseline_schema_hash"],
        snapshot["schema_hash"]
    );
    assert_eq!(
        report["resource_schema"]["effective_schema_hash"],
        report["resource_schema"]["schema_hash"]
    );
    assert_eq!(
        report["schema_snapshot"]["schema_hash"],
        snapshot["schema_hash"]
    );
    assert_eq!(
        report["resource_schema"]["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(snapshot["schema"]["fields"][0]["name"], "vendor_id");
    assert_eq!(
        snapshot["schema"]["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );
    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert_eq!(
        lock.resources["local.events"]
            .schema_snapshot
            .as_ref()
            .unwrap()
            .schema_hash
            .as_str(),
        snapshot["schema_hash"].as_str().unwrap()
    );
}

#[test]
fn keyless_append_file_validate_plan_preview_run_has_no_key_nudge() {
    let project = TestProject::new();
    fs::write(
        project.root.join("resources/files.toml"),
        RESOURCE.replace("primary_key = [\"id\"]\n", ""),
    )
    .unwrap();

    let validate = run(["cdf", "--json", "--project", project.root_str(), "validate"]);
    assert_eq!(validate.exit_code, 0, "stderr: {}", validate.stderr);
    assert_no_key_nudge(&validate);

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_no_key_nudge(&plan);
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(plan_json["result"]["destination"]["disposition"], "append");
    assert_eq!(
        plan_json["result"]["delivery_guarantee"],
        "effectively_once_per_package"
    );

    let human_plan = run([
        "cdf",
        "--color",
        "never",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(human_plan.exit_code, 0, "stderr: {}", human_plan.stderr);
    assert!(human_plan.stdout.contains("disposition  append"));
    assert_no_key_nudge(&human_plan);

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    assert_no_key_nudge(&preview);

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    assert_no_key_nudge(&run_result);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(run_json["result"]["receipt"]["disposition"], "append");
    assert_eq!(run_json["result"]["row_count"], 2);
}

#[test]
fn keyless_append_exact_row_dedup_is_explicit_and_evidence_preserving() {
    let project = TestProject::new();
    fs::write(
        project.root.join("resources/files.toml"),
        RESOURCE.replace("primary_key = [\"id\"]\n", "deduplicate = \"exact_row\"\n"),
    )
    .unwrap();
    fs::write(
        project.root.join("data/events.ndjson"),
        concat!(
            "{\"id\":1,\"updated_at\":1783296000000000}\n",
            "{\"id\":1,\"updated_at\":1783296000000000}\n",
            "{\"id\":1,\"updated_at\":1783296060000000}\n"
        ),
    )
    .unwrap();

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_no_key_nudge(&result);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 2);
    let reader = PackageReader::open(run_package_dir(&project, &result)).unwrap();
    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["input_rows"], 3);
    assert_eq!(summary["output_rows"], 2);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(summary["keep"], "first");
}

#[test]
fn keyless_append_rest_validate_plan_preview_run_has_no_key_nudge() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "keyless-rest-token\n").unwrap();
    let body = r#"{ "items": [
        { "id": 1, "updated_at": 10 },
        { "id": 2, "updated_at": 20 }
    ] }"#;
    let (base_url, requests) = serve_json_sequence([body, body]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    let resource_path = project.root.join("resources/api.toml");
    let resource = fs::read_to_string(&resource_path)
        .unwrap()
        .replace("primary_key = [\"id\"]\n", "");
    fs::write(&resource_path, resource).unwrap();

    let validate = run(["cdf", "--json", "--project", project.root_str(), "validate"]);
    assert_eq!(validate.exit_code, 0, "stderr: {}", validate.stderr);
    assert_no_key_nudge(&validate);

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "api.items",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_no_key_nudge(&plan);

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "api.items",
    ]);
    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    assert_no_key_nudge(&preview);

    let run_result = run_valid_run_resource(&project, "api.items");
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    assert_no_key_nudge(&run_result);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(run_json["result"]["receipt"]["disposition"], "append");
    assert_eq!(run_json["result"]["row_count"], 2);
    assert_eq!(requests.lock().unwrap().len(), 2);
}

#[test]
fn merge_without_key_fails_all_entry_commands_before_contact_or_writes() {
    let project = TestProject::new();
    let body = r#"{ "items": [{ "id": 1, "updated_at": 10 }] }"#;
    let (base_url, requests) = serve_json_sequence([body]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/missing-token",
    );
    let resource_path = project.root.join("resources/api.toml");
    let resource = fs::read_to_string(&resource_path)
        .unwrap()
        .replace("primary_key = [\"id\"]\n", "")
        .replace(
            "write_disposition = \"append\"",
            "write_disposition = \"merge\"",
        );
    fs::write(resource_path, resource).unwrap();
    let before = project_tree_snapshot(&project.root);

    for (command_name, command_args) in [
        ("validate", vec!["validate"]),
        ("plan", vec!["plan", "api.items"]),
        ("preview", vec!["preview", "api.items"]),
        ("run", vec!["run", "api.items"]),
    ] {
        let mut args = vec![
            "cdf".to_owned(),
            "--json".to_owned(),
            "--project".to_owned(),
            project.root_str().to_owned(),
        ];
        args.extend(command_args.into_iter().map(ToOwned::to_owned));
        let result = run_dynamic(args);
        assert_eq!(result.exit_code, 3, "{}", result.stderr);
        let error = stderr_or_stdout_json(&result.stderr);
        assert_eq!(error["error"]["code"], "CDF-PROJECT-MERGE-KEY");
        let message = error["error"]["message"].as_str().unwrap();
        assert!(
            message.contains(&format!("cdf {command_name}")),
            "{message}"
        );
        assert!(message.contains("resource `api.items`"), "{message}");
        assert!(message.contains("missing merge_key"), "{message}");
        assert_eq!(message.matches("missing merge_key").count(), 1, "{message}");
        assert!(message.contains("add `merge_key = [...]`"), "{message}");
        assert!(
            message.contains("use `write_disposition = \"append\"`"),
            "{message}"
        );
        assert_eq!(
            error["error"]["remediation"]["summary"],
            "Choose append or declare the merge identity before contacting the source or destination."
        );
        let steps = error["error"]["remediation"]["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 2);
        assert!(steps[0].as_str().unwrap().contains("merge_key = [...]"));
        assert!(
            steps[1]
                .as_str()
                .unwrap()
                .contains("write_disposition = \"append\"")
        );
        assert_eq!(project_tree_snapshot(&project.root), before);
    }

    let human = run([
        "cdf",
        "--color",
        "never",
        "--project",
        project.root_str(),
        "plan",
        "api.items",
    ]);
    assert_eq!(human.exit_code, 3);
    assert!(
        human.stderr.contains("resource `api.items`"),
        "{}",
        human.stderr
    );
    assert!(
        human.stderr.contains("Add `merge_key = [...]`"),
        "{}",
        human.stderr
    );
    assert!(
        human
            .stderr
            .contains("use `write_disposition = \"append\"`"),
        "{}",
        human.stderr
    );
    assert_eq!(requests.lock().unwrap().len(), 0);
    assert_eq!(project_tree_snapshot(&project.root), before);
}

#[test]
fn multi_file_parquet_no_pin_and_autopin_are_all_file_metadata_and_byte_stable() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/a.parquet"));
    write_vendor_score_parquet(&project.root.join("data/b.parquet"));

    let inspection = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--no-pin",
    ]);
    assert_eq!(inspection.exit_code, 0, "{}", inspection.stderr);
    assert_eq!(
        stderr_or_stdout_json(&inspection.stdout)["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());

    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(first.exit_code, 0, "{}", first.stderr);
    let first_report = stderr_or_stdout_json(&first.stdout);
    assert_eq!(
        first_report["result"]["schema_snapshot"]["outcome"],
        "added"
    );
    let snapshot_path = first_report["result"]["schema_snapshot"]["path"]
        .as_str()
        .unwrap();
    let snapshot_before = fs::read(project.root.join(snapshot_path)).unwrap();
    let snapshot_json: serde_json::Value = serde_json::from_slice(&snapshot_before).unwrap();
    let manifest_path = snapshot_json["metadata"]["cdf:discovery_manifest_path"]
        .as_str()
        .unwrap();
    let manifest_before = fs::read(project.root.join(manifest_path)).unwrap();
    let manifest_json: serde_json::Value = serde_json::from_slice(&manifest_before).unwrap();
    assert_eq!(manifest_json["file_coverage"], "all_files");
    assert_eq!(manifest_json["within_file_coverage"], "format_metadata");
    assert_eq!(manifest_json["candidates"].as_array().unwrap().len(), 2);
    assert!(
        manifest_json["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|candidate| candidate["participation"] == "observed")
    );
    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(second.exit_code, 0, "{}", second.stderr);
    assert_eq!(
        stderr_or_stdout_json(&second.stdout)["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        fs::read(project.root.join(snapshot_path)).unwrap(),
        snapshot_before
    );
    assert_eq!(
        fs::read(project.root.join(manifest_path)).unwrap(),
        manifest_before
    );
}

#[test]
fn sampled_discovery_renders_every_cli_path_and_routes_unseen_drift_to_package_quarantine() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    set_file_resource_sample_files(&project, 2);
    write_vendor_parquet(&project.root.join("data/a.parquet"));
    write_string_vendor_parquet(&project.root.join("data/middle.parquet"));
    write_vendor_parquet(&project.root.join("data/z.parquet"));

    let discover = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_eq!(discover.exit_code, 0, "{}", discover.stderr);
    let discovery = &stderr_or_stdout_json(&discover.stdout)["result"]["discovery"];
    assert_eq!(discovery["file_coverage"], "sampled_files");
    assert_eq!(discovery["within_file_coverage"], "format_metadata");
    assert_eq!(discovery["selector"], STRATIFIED_HASH_SELECTOR_V1);
    assert_eq!(discovery["sample_files"], 2);
    assert_eq!(discovery["matched_files"], 3);
    assert_eq!(discovery["selected_files"], 2);
    assert_eq!(discovery["unobserved_files"], 1);
    assert_no_schema_discovery_writes(&project);

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_eq!(human.exit_code, 0, "{}", human.stderr);
    assert!(
        human.stdout.contains("Discovery Coverage"),
        "{}",
        human.stdout
    );
    assert!(human.stdout.contains("sampled_files"), "{}", human.stdout);
    assert!(human.stdout.contains("matched files"), "{}", human.stdout);
    assert_no_schema_discovery_writes(&project);

    let no_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--no-pin",
    ]);
    assert_eq!(no_pin.exit_code, 0, "{}", no_pin.stderr);
    assert_eq!(
        stderr_or_stdout_json(&no_pin.stdout)["result"]["schema_snapshot"]["discovery"]["file_coverage"],
        "sampled_files"
    );
    assert_no_schema_discovery_writes(&project);

    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    assert_eq!(
        stderr_or_stdout_json(&pin.stdout)["result"]["discovery"]["file_coverage"],
        "sampled_files"
    );

    let diff = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "diff",
        "local.events",
    ]);
    assert_eq!(diff.exit_code, 0, "{}", diff.stderr);
    assert_eq!(
        stderr_or_stdout_json(&diff.stdout)["result"]["discovery"]["unobserved_files"],
        1
    );

    let before_preview = project_tree_snapshot(&project.root);
    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let preview_report = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(
        preview_report["result"]["schema_snapshot"]["discovery"]["file_coverage"],
        "sampled_files"
    );
    assert_eq!(preview_report["result"]["planned_partition_count"], 3);
    assert_eq!(
        preview_report["result"]["payload_opened_partition_count"],
        3
    );
    assert_eq!(preview_report["result"]["attested_partition_count"], 0);
    assert_eq!(preview_report["result"]["inspected_partition_count"], 3);
    assert_eq!(preview_report["result"]["inspected_batch_count"], 3);
    assert_eq!(preview_report["result"]["terminal_quarantine_count"], 1);
    assert_eq!(preview_report["result"]["row_count"], 4);
    assert_eq!(preview_report["result"]["limits"]["max_rows"], 500);
    assert_eq!(
        preview_report["result"]["limits"]["max_bytes"],
        64 * 1024 * 1024
    );
    assert_eq!(preview_report["result"]["limits"]["max_batches"], 64);
    assert_eq!(
        preview_report["result"]["selection"]["policy"],
        "preview-balanced-stratified-v1"
    );
    assert_eq!(
        preview_report["result"]["selection"]["selector"],
        "stratified-hash-v1"
    );
    assert_eq!(project_tree_snapshot(&project.root), before_preview);

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    assert_eq!(
        stderr_or_stdout_json(&run_result.stdout)["result"]["schema_snapshot"]["discovery"]["file_coverage"],
        "sampled_files"
    );
    let package = run_package_dir(&project, &run_result);
    let schema_evidence: serde_json::Value = serde_json::from_slice(
        &fs::read(package.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(schema_evidence["observations"].as_array().unwrap().len(), 2);
    assert!(schema_evidence["baseline_schema_hash"].is_string());
    assert_eq!(
        schema_evidence["baseline_schema_hash"],
        schema_evidence["effective_schema_hash"]
    );
    let quarantine: serde_json::Value = serde_json::from_slice(
        &fs::read(package.join("quarantine/schema-observations.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine[0]["observation_id"], "middle.parquet");
    assert_eq!(quarantine[0]["rule_id"], "schema-observation:incompatible");
    let quarantine_admission: serde_json::Value = serde_json::from_slice(
        &fs::read(package.join("quarantine/schema-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        quarantine_admission["observations"][0]["observation_id"],
        "middle.parquet"
    );
    assert!(cdf_package::PackageReader::open(&package).is_ok());
    let processed: serde_json::Value = serde_json::from_slice(
        &fs::read(package.join("state/processed-observations.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(processed["observations"].as_array().unwrap().len(), 3);

    let auto = TestProject::new();
    write_parquet_discover_resource(&auto, "*.parquet");
    set_file_resource_sample_files(&auto, 2);
    for name in ["a.parquet", "middle.parquet", "z.parquet"] {
        write_vendor_parquet(&auto.root.join("data").join(name));
    }
    let auto_pin = run([
        "cdf",
        "--json",
        "--project",
        auto.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(auto_pin.exit_code, 0, "{}", auto_pin.stderr);
    let auto_report = stderr_or_stdout_json(&auto_pin.stdout);
    assert_eq!(auto_report["result"]["schema_snapshot"]["outcome"], "added");
    assert_eq!(
        auto_report["result"]["schema_snapshot"]["discovery"]["file_coverage"],
        "sampled_files"
    );
}

#[test]
fn plan_discover_autopin_is_byte_stable_and_preserves_unrelated_semantic_locks() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let first_report = stderr_or_stdout_json(&first.stdout);
    assert_eq!(
        first_report["result"]["schema_snapshot"]["outcome"],
        "added"
    );
    let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();
    let snapshot_path = first_report["result"]["schema_snapshot"]["path"]
        .as_str()
        .unwrap();
    let snapshot_before = fs::read(project.root.join(snapshot_path)).unwrap();

    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    let second_report = stderr_or_stdout_json(&second.stdout);
    assert_eq!(
        second_report["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        second_report["result"]["schema_snapshot"]["snapshot_written"],
        false
    );
    assert_eq!(
        second_report["result"]["schema_snapshot"]["lockfile_written"],
        false
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
    assert_eq!(
        fs::read(project.root.join(snapshot_path)).unwrap(),
        snapshot_before
    );
    let human = run([
        "cdf",
        "--color",
        "never",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(human.stdout.contains("Schema Snapshot"), "{}", human.stdout);
    assert!(human.stdout.contains("outcome"), "{}", human.stdout);
    assert!(human.stdout.contains("unchanged"), "{}", human.stdout);

    let mut lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let mut unrelated = lock.resources["local.events"].clone();
    unrelated.descriptor.resource_id = ResourceId::new("unrelated.events").unwrap();
    lock.resources
        .insert("unrelated.events".to_owned(), unrelated.clone());
    fs::write(
        project.root.join("cdf.lock"),
        cdf_project::lock_to_toml(&lock).unwrap(),
    )
    .unwrap();
    let third = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(third.exit_code, 0, "stderr: {}", third.stderr);
    let updated = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert_eq!(updated.resources["unrelated.events"], unrelated);

    write_vendor_score_parquet(&project.root.join("data/vendors.parquet"));
    let locked_before_drift = fs::read(project.root.join("cdf.lock")).unwrap();
    let snapshots_before_drift = schema_snapshot_paths(&project);
    let pinned_hash = first_report["result"]["schema_snapshot"]["schema_hash"]
        .as_str()
        .unwrap();
    let pinned_plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(pinned_plan.exit_code, 0, "stderr: {}", pinned_plan.stderr);
    let pinned_plan_report = stderr_or_stdout_json(&pinned_plan.stdout);
    assert_eq!(
        pinned_plan_report["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        pinned_plan_report["result"]["schema_snapshot"]["schema_hash"],
        pinned_hash
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        locked_before_drift
    );
    assert_eq!(schema_snapshot_paths(&project), snapshots_before_drift);

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        locked_before_drift
    );
    assert_eq!(schema_snapshot_paths(&project), snapshots_before_drift);

    let run_result = run_valid_run_resource(&project, "local.events");
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_report = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(
        run_report["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(run_report["result"]["schema_hash"], pinned_hash);
    assert_eq!(
        run_report["result"]["schema_snapshot"]["schema_hash"],
        pinned_hash
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        locked_before_drift
    );
    assert_eq!(schema_snapshot_paths(&project), snapshots_before_drift);

    let inspection = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--no-pin",
    ]);
    assert_eq!(inspection.exit_code, 0, "stderr: {}", inspection.stderr);
    let inspection_report = stderr_or_stdout_json(&inspection.stdout);
    assert_eq!(
        inspection_report["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert_ne!(
        inspection_report["result"]["schema_snapshot"]["schema_hash"],
        pinned_hash
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        locked_before_drift
    );
    assert_eq!(schema_snapshot_paths(&project), snapshots_before_drift);

    let explicit_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(explicit_pin.exit_code, 0, "stderr: {}", explicit_pin.stderr);
    let explicit_pin_report = stderr_or_stdout_json(&explicit_pin.stdout);
    assert_eq!(explicit_pin_report["result"]["status"], "refreshed");
    assert_ne!(explicit_pin_report["result"]["schema_hash"], pinned_hash);
    let refreshed_lock =
        parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert_eq!(refreshed_lock.resources["unrelated.events"], unrelated);
}

#[test]
fn plan_and_explain_no_pin_discover_without_project_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));
    let before = project_tree_snapshot(&project.root);

    for command in ["plan", "explain"] {
        let result = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            command,
            "local.events",
            "--no-pin",
        ]);
        assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
        let report = stderr_or_stdout_json(&result.stdout);
        assert_eq!(
            report["result"]["schema_snapshot"]["outcome"],
            "inspection_only"
        );
        assert_eq!(
            report["result"]["schema_snapshot"]["snapshot_written"],
            false
        );
        assert_eq!(
            report["result"]["schema_snapshot"]["lockfile_written"],
            false
        );
        assert_eq!(project_tree_snapshot(&project.root), before);
    }
}

#[test]
fn ordinary_plan_fails_closed_when_locked_snapshot_artifact_is_missing() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));
    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let report = stderr_or_stdout_json(&first.stdout);
    let snapshot_path = report["result"]["schema_snapshot"]["path"]
        .as_str()
        .unwrap();
    let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();
    fs::remove_file(project.root.join(snapshot_path)).unwrap();

    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);

    assert_ne!(second.exit_code, 0);
    let output = format!("{}{}", second.stdout, second.stderr);
    assert!(output.contains(snapshot_path), "{output}");
    assert!(!project.root.join(snapshot_path).exists());
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );

    let inspection = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--no-pin",
    ]);
    assert_eq!(inspection.exit_code, 0, "stderr: {}", inspection.stderr);
    let inspection_report = stderr_or_stdout_json(&inspection.stdout);
    assert_eq!(
        inspection_report["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert!(!project.root.join(snapshot_path).exists());
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
}

#[test]
fn no_pin_is_documented_for_plan_and_explain_but_rejected_by_run() {
    for command in ["plan", "explain"] {
        let help = run(["cdf", "help", command]);
        assert_eq!(help.exit_code, 0, "stderr: {}", help.stderr);
        assert!(help.stdout.contains("--no-pin"), "{}", help.stdout);
    }

    let run_help = run(["cdf", "help", "run"]);
    assert_eq!(run_help.exit_code, 0, "stderr: {}", run_help.stderr);
    assert!(!run_help.stdout.contains("--no-pin"));
    let rejected = run(["cdf", "run", "local.events", "--no-pin"]);
    assert_eq!(rejected.exit_code, 2);
    assert!(rejected.stderr.contains("unexpected argument '--no-pin'"));
}

#[test]
fn rest_plan_no_pin_is_write_free_and_redacts_resolved_secret() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "rest-no-pin-secret\n").unwrap();
    let (base_url, requests) =
        serve_json_sequence([r#"{ "items": [{ "VendorID": 1, "updated_at": 10 }] }"#]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    fs::write(
        project.root.join("resources/api.toml"),
        rest_discover_resource_with_base_url(&base_url, "secret://file/rest-token"),
    )
    .unwrap();
    let before = project_tree_snapshot(&project.root);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "api.items",
        "--no-pin",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "rest-no-pin-secret");
    let report = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        report["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[test]
fn postgres_discover_mode_plan_preview_run_autopins_through_file_secret_without_leaks() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("discover_run_orders");
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"VendorID\" BIGINT NOT NULL,
                \"updated_at\" BIGINT NOT NULL
            );
            INSERT INTO {} (\"VendorID\", \"updated_at\") VALUES (1, 10), (2, 20), (3, 30)",
            table, table
        ))
        .unwrap();

    let project = TestProject::new();
    let source_dsn = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:source-discover-run-secret@",
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
        sql_discover_resource_with_vendor_cursor("secret://file/sql-dsn", &table),
    )
    .unwrap();

    let before_no_pin = project_tree_snapshot(&project.root);
    let no_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "warehouse.orders",
        "--no-pin",
    ]);
    assert_eq!(no_pin.exit_code, 0, "stderr: {}", no_pin.stderr);
    assert_secret_absent(&no_pin, &source_dsn);
    assert_secret_absent(&no_pin, "source-discover-run-secret");
    let no_pin_report = stderr_or_stdout_json(&no_pin.stdout);
    assert_eq!(
        no_pin_report["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert_eq!(project_tree_snapshot(&project.root), before_no_pin);

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "warehouse.orders",
    ]);

    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_secret_absent(&plan, &source_dsn);
    assert_secret_absent(&plan, "source-discover-run-secret");
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    let plan_report = &plan_json["result"];
    assert_eq!(
        plan_report["resource_schema"]["schema_source"],
        "discovered"
    );
    assert_eq!(
        plan_report["resource_schema"]["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    let snapshot_path = plan_report["resource_schema"]["snapshot_path"]
        .as_str()
        .unwrap();
    let snapshot = read_snapshot_json(&project, snapshot_path);
    let snapshot_text = snapshot.to_string();
    assert!(!snapshot_text.contains(&source_dsn));
    assert!(!snapshot_text.contains("source-discover-run-secret"));
    let lock_text = fs::read_to_string(project.root.join("cdf.lock")).unwrap();
    assert!(!lock_text.contains(&source_dsn));
    assert!(!lock_text.contains("source-discover-run-secret"));
    assert_eq!(snapshot["schema"]["fields"][0]["name"], "vendor_id");
    assert_eq!(
        snapshot["schema"]["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "warehouse.orders",
        "--filter",
        "vendor_id >= 2",
        "--limit",
        "1",
    ]);

    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    assert_secret_absent(&preview, &source_dsn);
    assert_secret_absent(&preview, "source-discover-run-secret");
    assert_no_preview_writes(&project);
    let preview_json = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_json["result"]["resource"], "warehouse.orders");
    assert_eq!(preview_json["result"]["partition"], "sql");
    assert_eq!(preview_json["result"]["row_count"], 1);

    let run_result = run_valid_run_resource(&project, "warehouse.orders");

    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    assert_secret_absent(&run_result, &source_dsn);
    assert_secret_absent(&run_result, "source-discover-run-secret");
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_report = &run_json["result"];
    assert_eq!(run_report["resource_id"], "warehouse.orders");
    assert_eq!(run_report["schema_snapshot"]["outcome"], "unchanged");
    assert_eq!(run_report["schema_snapshot"]["snapshot_written"], false);
    assert_eq!(run_report["schema_snapshot"]["lockfile_written"], false);
    assert_eq!(run_report["target"], "orders");
    assert_eq!(run_report["schema_hash"], snapshot["schema_hash"]);
    assert_eq!(run_report["row_count"], 3);
    assert_eq!(run_report["checkpoint"]["status"], "committed");
    let package_dir = run_package_dir(&project, &run_result);
    let admission_plan: cdf_engine::CompiledSchemaAdmissionPlan =
        serde_json::from_slice(&fs::read(package_dir.join("plan/schema-admission.json")).unwrap())
            .unwrap();
    let admission_evidence: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    admission_evidence.validate(&admission_plan).unwrap();
    assert_eq!(admission_evidence.observations.len(), 1);

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows = conn
        .prepare("SELECT vendor_id, updated_at FROM orders ORDER BY vendor_id")
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30)]);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("warehouse.orders").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed Postgres discover run head");
    assert_eq!(
        head.delta.schema_hash.as_str(),
        snapshot["schema_hash"].as_str().unwrap()
    );
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        run_report["checkpoint_id"].as_str().unwrap()
    );
}

#[test]
fn p2_s4_postgres_add_pins_private_secret_and_runs_discovered_table() {
    use std::os::unix::fs::PermissionsExt;

    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("orders_add");
    postgres
        .client()
        .batch_execute(&format!(
            "CREATE TABLE {} (id BIGSERIAL PRIMARY KEY, updated_at TIMESTAMP NOT NULL, amount BIGINT); INSERT INTO {} (updated_at, amount) VALUES (NOW(), 10), (NOW(), 20)",
            table,
            table,
        ))
        .unwrap();
    let project = TestProject::new();
    let source_url = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:s4-private-password@",
        1,
    );
    let location = format!("{}/{}", source_url.trim_end_matches('/'), table);

    let dry = TestProject::new();
    let dry_run = run([
        "cdf",
        "--json",
        "--project",
        dry.root_str(),
        "add",
        "warehouse.orders",
        &location,
        "--dry-run",
    ]);
    assert_eq!(dry_run.exit_code, 0, "{}", dry_run.stderr);
    assert!(!dry.root.join("resources/warehouse.toml").exists());
    assert!(!dry.root.join(".cdf/secrets").exists());
    assert!(!dry.root.join("cdf.lock").exists());

    let add = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "warehouse.orders",
        &location,
    ]);
    assert_eq!(add.exit_code, 0, "{}", add.stderr);
    assert_secret_absent(&add, "s4-private-password");
    let report = stderr_or_stdout_json(&add.stdout);
    assert_eq!(report["result"]["resource_id"], "warehouse.orders");
    assert_eq!(report["result"]["schema_source"], "discovered");
    assert!(
        report["result"]["cursor_candidates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|candidate| candidate == "updated_at")
    );
    let resource = fs::read_to_string(project.root.join("resources/warehouse.toml")).unwrap();
    assert!(resource.contains("connection = \"secret://file/.cdf/secrets/sources/warehouse.dsn\""));
    assert!(resource.contains(&format!("table = \"{table}\"")));
    assert!(!resource.contains("s4-private-password"));
    let secret = project.root.join(".cdf/secrets/sources/warehouse.dsn");
    assert_eq!(
        fs::metadata(&secret).unwrap().permissions().mode() & 0o777,
        0o600
    );

    for command in ["plan", "preview"] {
        let result = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            command,
            "warehouse.orders",
        ]);
        assert_eq!(result.exit_code, 0, "{command}: {}", result.stderr);
    }
    let resource_path = project.root.join("resources/warehouse.toml");
    let with_cursor = fs::read_to_string(&resource_path)
        .unwrap()
        .replace(
            "write_disposition = \"append\"",
            "cursor = { field = \"updated_at\", ordering = \"exact\", lag = \"0ms\" }\nwrite_disposition = \"append\"",
        );
    fs::write(resource_path, with_cursor).unwrap();
    let run_result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "warehouse.orders",
    ]);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    assert_eq!(
        stderr_or_stdout_json(&run_result.stdout)["result"]["row_count"],
        2
    );
}

#[test]
fn rest_discover_mode_plan_preview_run_autopins_through_file_secret_without_leaks() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "rest-autopin-secret\n").unwrap();
    let body = r#"{ "items": [
        { "VendorID": 1, "updated_at": 10 },
        { "VendorID": 2, "updated_at": 20 }
    ] }"#;
    let (base_url, requests) = serve_json_sequence([body, body, body, body, body]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    fs::write(
        project.root.join("resources/api.toml"),
        rest_discover_resource_with_base_url(&base_url, "secret://file/rest-token"),
    )
    .unwrap();

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "api.items",
    ]);

    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_secret_absent(&plan, "rest-autopin-secret");
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    let plan_report = &plan_json["result"];
    assert_eq!(
        plan_report["resource_schema"]["schema_source"],
        "discovered"
    );
    assert_eq!(
        plan_report["resource_schema"]["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    let snapshot_path = plan_report["resource_schema"]["snapshot_path"]
        .as_str()
        .unwrap();
    let snapshot = read_snapshot_json(&project, snapshot_path);
    let snapshot_text = snapshot.to_string();
    assert!(!snapshot_text.contains("rest-autopin-secret"));
    assert!(
        !fs::read_to_string(project.root.join("cdf.lock"))
            .unwrap()
            .contains("rest-autopin-secret")
    );
    let snapshot_fields = snapshot["schema"]["fields"].as_array().unwrap();
    assert!(
        snapshot_fields
            .iter()
            .any(|field| field["name"] == "updated_at")
    );
    let vendor = snapshot_fields
        .iter()
        .find(|field| field["name"] == "vendor_id")
        .unwrap();
    assert_eq!(vendor["metadata"]["cdf:source_name"], "VendorID");

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "api.items",
    ]);

    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    assert_secret_absent(&preview, "rest-autopin-secret");
    assert_no_preview_writes(&project);
    let preview_json = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_json["result"]["resource"], "api.items");
    assert_eq!(preview_json["result"]["partition"], "rest");
    assert_eq!(preview_json["result"]["row_count"], 2);

    let run_result = run_valid_run_resource(&project, "api.items");

    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    assert_secret_absent(&run_result, "rest-autopin-secret");
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_report = &run_json["result"];
    assert_eq!(run_report["resource_id"], "api.items");
    assert_eq!(run_report["schema_snapshot"]["outcome"], "unchanged");
    assert_eq!(run_report["schema_snapshot"]["snapshot_written"], false);
    assert_eq!(run_report["schema_snapshot"]["lockfile_written"], false);
    assert_eq!(run_report["schema_hash"], snapshot["schema_hash"]);
    assert_eq!(run_report["row_count"], 2);
    assert_eq!(run_report["checkpoint"]["status"], "committed");

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows = conn
        .prepare("SELECT vendor_id, updated_at FROM items ORDER BY vendor_id")
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("api.items").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed REST discover run head");
    assert_eq!(
        head.delta.schema_hash.as_str(),
        snapshot["schema_hash"].as_str().unwrap()
    );
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        run_report["checkpoint_id"].as_str().unwrap()
    );

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| request.contains("authorization: Bearer rest-autopin-secret"))
    );
}

#[test]
fn cold_rest_run_reuses_the_discovery_page_without_a_second_request() {
    let project = TestProject::new();
    fs::write(
        project.root.join("rest-token"),
        "rest-single-request-secret\n",
    )
    .unwrap();
    let base_url = serve_json_once(
        r#"{ "items": [
            { "VendorID": 1, "updated_at": 10 },
            { "VendorID": 2, "updated_at": 20 }
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
        rest_discover_resource_with_base_url(&base_url, "secret://file/rest-token"),
    )
    .unwrap();

    let result = run_valid_run_resource(&project, "api.items");

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "rest-single-request-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 2);
    assert_eq!(json["result"]["schema_snapshot"]["outcome"], "added");
    assert_eq!(json["result"]["checkpoint"]["status"], "committed");
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
            .contains("requires field `table`"),
        "{}",
        result.stderr
    );
}

#[test]
fn preview_file_filter_runs_through_shared_engine_without_writes() {
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

    assert_eq!(result.exit_code, 0, "{}", result.stderr);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 1);
    assert_eq!(json["result"]["planned_partition_count"], 1);
    assert_eq!(json["result"]["payload_opened_partition_count"], 1);
    assert_eq!(json["result"]["inspected_batch_count"], 1);
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
            .contains("matched no files"),
        "{}",
        result.stderr
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
            .contains("matched no files"),
        "{}",
        result.stderr
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
fn preview_multi_match_file_glob_reads_every_sorted_match_without_writes() {
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
    assert!(
        json["result"]["partition"]
            .as_str()
            .unwrap()
            .starts_with("file-")
    );
    assert_eq!(json["result"]["planned_partition_count"], 2);
    assert_eq!(json["result"]["payload_opened_partition_count"], 2);
    assert_eq!(json["result"]["inspected_partition_count"], 2);
    assert_eq!(json["result"]["row_count"], 3);
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
fn run_command_commits_package_rows_mirrors_and_checkpoint() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "run");
    assert_eq!(report["command"], "run");
    assert!(!report["run_id"].as_str().unwrap().is_empty());
    assert_eq!(report["resource_id"], "local.events");
    assert_eq!(report["pipeline_id"], "cdf-run");
    assert_eq!(report["target"], "events");
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["destination"]["destination_id"], "duckdb");
    assert!(
        report["destination"]["database_path"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/dev.duckdb")
    );
    assert!(
        report["package_id"]
            .as_str()
            .unwrap()
            .starts_with("pkg-local-events-")
    );
    assert_eq!(report["package_status"], "checkpointed");
    assert!(
        report["checkpoint_id"]
            .as_str()
            .unwrap()
            .starts_with("checkpoint-local-events-")
    );
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
    assert_eq!(
        report["memory"]["budget"]["resolution"]["process_budget_bytes"],
        4 * 1024 * 1024 * 1024_u64
    );
    assert_eq!(
        report["memory"]["managed"]["budget_bytes"],
        report["memory"]["budget"]["resolution"]["managed_pool_bytes"]
    );
    assert!(report["memory"]["managed"]["peak_bytes"].as_u64().is_some());
    assert!(
        report["memory"]["budget"]["memory_authority"]["enforcement"]
            .as_str()
            .is_some()
    );
    assert_eq!(
        report["ledger_events"]["event_count"],
        report["ledger_events"]["events"].as_array().unwrap().len()
    );
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    let events = report["ledger_events"]["events"].as_array().unwrap();
    assert_eq!(events.first().unwrap()["kind"], "run_started");
    assert_eq!(events.last().unwrap()["kind"], "run_succeeded");
    assert_eq!(report["writes"]["package"], true);
    assert_eq!(report["writes"]["destination"], true);
    assert_eq!(report["writes"]["checkpoint"], true);

    let package_dir = run_package_dir(&project, &result);
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
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
    );
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
fn run_adhoc_local_parquet_reuses_identity_and_ordinary_evidence_spine() {
    const PATH_SECRET: &str = "local-path-secret-sentinel";
    let project = TestProject::new();
    let source_dir = project.root.join("data").join(PATH_SECRET);
    fs::create_dir_all(&source_dir).unwrap();
    let source = source_dir.join("yellow.parquet");
    write_vendor_parquet(&source);
    let source = source.to_str().unwrap();

    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source,
        "--to",
        "duckdb://.cdf/adhoc-local.duckdb",
    ]);

    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    assert_secret_absent(&first, PATH_SECRET);
    let first_json = stderr_or_stdout_json(&first.stdout);
    let report = &first_json["result"];
    let resource_id = report["resource_id"].as_str().unwrap();
    assert!(resource_id.starts_with("adhoc.parquet_"));
    assert_eq!(report["adhoc"]["resource_id"], resource_id);
    assert_eq!(report["adhoc"]["reused"], false);
    let config_path = report["adhoc"]["config_path"].as_str().unwrap();
    let staged_path = report["adhoc"]["source_artifact_path"].as_str().unwrap();
    assert!(config_path.starts_with(".cdf/adhoc/parquet_"));
    assert!(staged_path.starts_with(".cdf/adhoc/data/parquet_"));
    assert!(project.root.join(config_path).is_file());
    assert!(project.root.join(staged_path).is_file());
    assert_eq!(
        report["schema_hash"],
        report["schema_snapshot"]["schema_hash"]
    );
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    assert_eq!(report["writes"]["package"], true);
    assert_eq!(report["writes"]["destination"], true);
    assert_eq!(report["writes"]["checkpoint"], true);
    assert!(
        report["adhoc"]["make_permanent_command"]
            .as_str()
            .unwrap()
            .starts_with(&format!("cdf add {resource_id} .cdf/adhoc/data/"))
    );

    let resource_toml = fs::read_to_string(project.root.join(config_path)).unwrap();
    let staged_root = Path::new(staged_path).parent().unwrap().to_str().unwrap();
    assert!(resource_toml.contains(&format!("root = {staged_root:?}")));
    assert!(!resource_toml.contains(PATH_SECRET));
    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let locked = &lock.resources[resource_id];
    assert_eq!(
        locked.schema_hash.as_deref(),
        report["schema_snapshot"]["schema_hash"].as_str()
    );
    assert_eq!(
        locked.schema_snapshot.as_ref().unwrap().path,
        report["schema_snapshot"]["path"].as_str().unwrap()
    );

    let package = PackageReader::open(run_package_dir(&project, &first)).unwrap();
    package.verify().unwrap();
    let receipt = collect_package_receipts(&package).remove(0);
    assert_eq!(receipt.schema_hash.as_str(), report["schema_hash"]);
    let destination = DuckDbDestination::new(project.root.join(".cdf/adhoc-local.duckdb")).unwrap();
    assert!(destination.verify_receipt(&receipt).unwrap().verified);
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new(resource_id).unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .unwrap();
    assert_eq!(head.delta.schema_hash.as_str(), report["schema_hash"]);
    assert!(receipt.covers_state_delta(&head.delta));

    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source,
        "--to",
        "duckdb://.cdf/adhoc-local.duckdb",
    ]);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    assert_secret_absent(&second, PATH_SECRET);
    let second_json = stderr_or_stdout_json(&second.stdout);
    assert_eq!(second_json["result"]["resource_id"], resource_id);
    assert_eq!(second_json["result"]["adhoc"]["reused"], true);
    assert_eq!(second_json["result"]["schema_hash"], report["schema_hash"]);
    assert_eq!(
        fs::read_dir(project.root.join(".cdf/adhoc"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(
                |entry| entry.path().extension().and_then(|value| value.to_str()) == Some("toml")
            )
            .count(),
        1
    );
    assert_generated_artifacts_exclude(&project.root, PATH_SECRET);

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "run",
        source,
        "--to",
        "duckdb://.cdf/adhoc-local.duckdb",
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert_secret_absent(&human, PATH_SECRET);
    assert!(human.stdout.contains("Ad-hoc Resource"));
    assert!(human.stdout.contains(config_path));
    assert!(human.stdout.contains(&format!("cdf add {resource_id}")));
}

#[test]
fn run_adhoc_destination_failure_preserves_recoverable_evidence_and_retry() {
    let project = TestProject::new();
    let source = project.root.join("data/yellow.parquet");
    write_vendor_parquet(&source);
    let canonical = fs::canonicalize(&source)
        .unwrap()
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    let digest = Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let target = format!("parquet_{}", &digest[..24]);
    let destination_path = project.root.join(".cdf/adhoc-retry.duckdb");
    let connection = DuckConnection::open(&destination_path).unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TABLE {target} (vendor_id INTEGER NOT NULL UNIQUE); INSERT INTO {target} VALUES (1), (2)"
        ))
        .unwrap();
    drop(connection);

    let failed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source.to_str().unwrap(),
        "--to",
        "duckdb://.cdf/adhoc-retry.duckdb",
    ]);
    assert_ne!(failed.exit_code, 0);

    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let resource_id = lock
        .resources
        .keys()
        .find(|id| id.starts_with("adhoc.parquet_"))
        .unwrap()
        .clone();
    let package_dir = single_package_dir(&project);
    let package = PackageReader::open(&package_dir).unwrap();
    package.verify().unwrap();
    assert!(collect_package_receipts(&package).is_empty());
    assert_eq!(
        package.manifest().lifecycle.status,
        PackageStatus::Loading,
        "failed run stderr: {}",
        failed.stderr
    );

    let state_path = project.root.join(".cdf/state.db");
    let state = Connection::open(&state_path).unwrap();
    let run_id: String = state
        .query_row(
            "SELECT run_id FROM cdf_runs ORDER BY created_at_ms DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    drop(state);
    let ledger = SqliteRunLedger::open(&state_path).unwrap();
    let events = ledger.events(&RunId::new(run_id).unwrap()).unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.kind == RunEventKind::PackageFinalized)
    );
    assert_eq!(events.last().unwrap().kind, RunEventKind::RunFailed);
    assert!(
        events
            .iter()
            .all(|event| event.kind != RunEventKind::DestinationReceiptRecorded)
    );
    assert!(
        events
            .iter()
            .all(|event| event.kind != RunEventKind::CheckpointCommitted)
    );
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    assert!(
        store
            .head(
                &PipelineId::new("cdf-run").unwrap(),
                &ResourceId::new(&resource_id).unwrap(),
                &ScopeKey::Resource,
            )
            .unwrap()
            .is_none()
    );

    let connection = DuckConnection::open(&destination_path).unwrap();
    connection
        .execute_batch(&format!("DROP TABLE {target}"))
        .unwrap();
    drop(connection);
    let retry = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source.to_str().unwrap(),
        "--to",
        "duckdb://.cdf/adhoc-retry.duckdb",
    ]);
    assert_eq!(retry.exit_code, 0, "stderr: {}", retry.stderr);
    let retry = stderr_or_stdout_json(&retry.stdout);
    assert_eq!(retry["result"]["resource_id"], resource_id);
    assert_eq!(retry["result"]["adhoc"]["reused"], true);
    assert_eq!(retry["result"]["checkpoint"]["status"], "committed");
    assert_eq!(
        retry["result"]["ledger_events"]["terminal_kind"],
        "run_succeeded"
    );
}

#[test]
fn run_adhoc_http_parquet_uses_bounded_discovery_and_ordinary_run() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/yellow.parquet"));
    let parquet = fs::read(project.root.join("data/yellow.parquet")).unwrap();
    let max_requests = parquet.len() + 64;
    let (base_url, requests) = serve_parquet_file(parquet, max_requests);
    let url = format!("{base_url}/yellow.parquet");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        &url,
        "--to",
        "duckdb://.cdf/adhoc-http.duckdb",
    ]);

    let observed_requests = requests.lock().unwrap().clone();
    assert_eq!(
        result.exit_code, 0,
        "stderr: {}\nrequests: {observed_requests:#?}",
        result.stderr
    );
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert!(
        report["resource_id"]
            .as_str()
            .unwrap()
            .starts_with("adhoc.parquet_")
    );
    assert!(report["adhoc"]["source_artifact_path"].is_null());
    assert_eq!(
        report["schema_hash"],
        report["schema_snapshot"]["schema_hash"]
    );
    assert_eq!(
        report["schema_snapshot"]["discovery"]["file_coverage"],
        "all_files"
    );
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    let config = fs::read_to_string(
        project
            .root
            .join(report["adhoc"]["config_path"].as_str().unwrap()),
    )
    .unwrap();
    assert!(config.contains(&format!("root = \"{base_url}\"")));
    assert!(config.contains("glob = \"yellow.parquet\""));
    assert!(
        observed_requests
            .iter()
            .any(|request| request.starts_with("HEAD /yellow.parquet"))
    );
    assert!(observed_requests.iter().any(|request| {
        request.starts_with("GET /yellow.parquet")
            && request.to_ascii_lowercase().contains("range: bytes=")
    }));
}

#[test]
fn run_adhoc_rejects_missing_destination_and_sensitive_or_unsupported_urls_without_writes() {
    let project = TestProject::new();
    let source = project.root.join("data/yellow.parquet");
    write_vendor_parquet(&source);
    let before = project_tree_snapshot(&project.root);
    let missing_destination = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source.to_str().unwrap(),
    ]);
    assert_ne!(missing_destination.exit_code, 0);
    assert!(
        missing_destination
            .stderr
            .contains("explicit `--to <destination>`")
    );
    assert_eq!(project_tree_snapshot(&project.root), before);

    const URL_SECRET: &str = "signed-url-secret-sentinel";
    let signed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "https://data.example.test/yellow.parquet?sig=signed-url-secret-sentinel",
        "--to",
        "duckdb://.cdf/adhoc-rejected.duckdb",
    ]);
    assert_ne!(signed.exit_code, 0);
    assert_secret_absent(&signed, URL_SECRET);
    assert_eq!(project_tree_snapshot(&project.root), before);

    const USERINFO_SECRET: &str = "userinfo-secret-sentinel";
    let userinfo = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "https://public-user:userinfo-secret-sentinel@data.example.test/yellow.parquet",
        "--to",
        "duckdb://.cdf/adhoc-rejected.duckdb",
    ]);
    assert_ne!(userinfo.exit_code, 0);
    assert!(
        userinfo
            .stderr
            .contains("does not accept URL userinfo credentials")
    );
    assert_secret_absent(&userinfo, USERINFO_SECRET);
    assert_eq!(project_tree_snapshot(&project.root), before);

    const MALFORMED_URL_SECRET: &str = "malformed-url-secret-sentinel";
    let malformed_url = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "https://public-user:malformed-url-secret-sentinel@[bad/yellow.parquet",
        "--to",
        "duckdb://.cdf/adhoc-rejected.duckdb",
    ]);
    assert_ne!(malformed_url.exit_code, 0);
    assert!(malformed_url.stderr.contains("[redacted-url]"));
    assert_secret_absent(&malformed_url, MALFORMED_URL_SECRET);
    assert_eq!(project_tree_snapshot(&project.root), before);

    const UNSUPPORTED_SECRET: &str = "unsupported-location-secret";
    let unsupported = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "s3://unsupported-location-secret@bucket/yellow.parquet",
        "--to",
        "duckdb://.cdf/adhoc-rejected.duckdb",
    ]);
    assert_ne!(unsupported.exit_code, 0);
    assert_secret_absent(&unsupported, UNSUPPORTED_SECRET);
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert!(!project.root.join(".cdf/adhoc").exists());
}

#[test]
fn run_adhoc_rejected_local_paths_redact_details_without_writes() {
    let project = TestProject::new();
    const MISSING_SECRET: &str = "missing-local-secret-sentinel";
    const EXTENSION_SECRET: &str = "wrong-extension-secret-sentinel";
    const DIRECTORY_SECRET: &str = "directory-local-secret-sentinel";
    let missing = project
        .root
        .join("data")
        .join(MISSING_SECRET)
        .join("yellow.parquet");
    let wrong_extension = project
        .root
        .join("data")
        .join(format!("{EXTENSION_SECRET}.unknown"));
    fs::write(&wrong_extension, "not parquet").unwrap();
    let directory = project
        .root
        .join("data")
        .join(format!("{DIRECTORY_SECRET}.parquet"));
    fs::create_dir_all(&directory).unwrap();
    let before = project_tree_snapshot(&project.root);

    for (path, secret) in [
        (missing, MISSING_SECRET),
        (wrong_extension, EXTENSION_SECRET),
        (directory, DIRECTORY_SECRET),
    ] {
        let result = run_dynamic(vec![
            "cdf".to_owned(),
            "--json".to_owned(),
            "--project".to_owned(),
            project.root_str().to_owned(),
            "run".to_owned(),
            path.to_string_lossy().into_owned(),
            "--to".to_owned(),
            "duckdb://.cdf/adhoc-rejected-local.duckdb".to_owned(),
        ]);
        assert_ne!(result.exit_code, 0);
        assert!(result.stderr.contains("[redacted-local-source-path]"));
        assert_secret_absent(&result, secret);
        assert_eq!(project_tree_snapshot(&project.root), before);
    }
}

#[test]
fn run_adhoc_synthetic_resource_id_collision_fails_before_mutation() {
    let project = TestProject::new();
    let source = project.root.join("data/yellow.parquet");
    write_vendor_parquet(&source);
    let canonical = fs::canonicalize(&source)
        .unwrap()
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    let digest = Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let resource_name = format!("parquet_{}", &digest[..24]);
    let resource_id = format!("adhoc.{resource_name}");
    fs::write(
        project.root.join("resources/collision.toml"),
        format!(
            r#"
[source.adhoc]
kind = "files"
root = "data"

[resource.{resource_name}]
glob = "*.ndjson"
format = "ndjson"
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
  {{ name = "updated_at", type = "int64", nullable = false }},
] }}
"#
        ),
    )
    .unwrap();
    let mut project_toml = fs::read_to_string(project.root.join("cdf.toml")).unwrap();
    project_toml.push_str(&format!(
        "\n[resources.\"{resource_id}\"]\nsource = \"resources/collision.toml\"\n"
    ));
    fs::write(project.root.join("cdf.toml"), project_toml).unwrap();
    let before = project_tree_snapshot(&project.root);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source.to_str().unwrap(),
        "--to",
        "duckdb://.cdf/adhoc-collision.duckdb",
    ]);

    assert_ne!(result.exit_code, 0);
    assert!(result.stderr.contains(&resource_id));
    assert!(
        result
            .stderr
            .contains("conflicts with an already compiled project resource")
    );
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert!(!project.root.join(".cdf/adhoc").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/adhoc-collision.duckdb").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
}

#[test]
fn run_human_output_mentions_receipt_verified_commit_gate() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "--progress",
        "always",
        "run",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_no_headless_progress_controls(&result.stdout);
    assert_no_headless_progress_controls(&result.stderr);
    for expected in [
        "[plan] running run started",
        "[gate] succeeded run succeeded",
    ] {
        assert!(
            result.stderr.contains(expected),
            "missing {expected:?} in stderr:\n{}",
            result.stderr
        );
    }
    for expected in [
        "OK run ",
        "Run",
        "Package",
        "Rows",
        "Verdicts",
        "Receipt",
        "Gate",
        "resource     local.events",
        "target       events",
        "checkpoint           checkpoint-local-events-",
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
    let mut cli = test_cli(&project);
    cli.terminal.progress = cdf_cli_core::terminal::PolicyMode::Always;
    let (host, services) =
        cdf_engine::StandaloneExecutionHost::default_services(512 * 1024 * 1024).unwrap();
    let output = crate::run_command::run(
        &cli,
        cdf_cli_core::args::RunArgs {
            resource_id: Some("local.events".to_owned()),
            destination_uri: None,
            jobs: None,
            stats_profile: false,
            explain_memory: true,
            loop_mode: false,
        },
        host.as_ref(),
        &services,
        &test_destination_registry(),
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(result.stderr.contains("Run progress"), "{}", result.stderr);
    for expected in [
        "\u{1b}[32m✓\u{1b}[0m run ",
        "\u{1b}[36mRun\u{1b}[0m",
        "\u{1b}[36mPackage\u{1b}[0m",
        "\u{1b}[36mRows\u{1b}[0m",
        "\u{1b}[36mMemory\u{1b}[0m",
        "\u{1b}[36mVerdicts\u{1b}[0m",
        "\u{1b}[36mReceipt\u{1b}[0m",
        "\u{1b}[36mGate\u{1b}[0m",
        "checkpoint           checkpoint-local-events-",
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
    let run_result = run_valid_run_args(&project);
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
        json!([run_json["result"]["package_id"].clone()])
    );
    assert_eq!(
        report["pointers"]["checkpoint_ids"],
        json!([run_json["result"]["checkpoint_id"].clone()])
    );
    let events = report["events"].as_array().unwrap();
    assert!(!events.is_empty());
    assert_eq!(events[0]["sequence"], 1);
    assert_eq!(events[0]["kind"], "run_started");
    assert_eq!(events.last().unwrap()["kind"], "run_succeeded");
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
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_id = run_json["result"]["run_id"].as_str().unwrap();
    fs::remove_dir_all(run_package_dir(&project, &run_result)).unwrap();

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
    let run_result = run_valid_run_args(&project);
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
        "seq:",
        "kind:",
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

    let run_result = run_valid_run_args(&project);
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
    let package_dir = create_replay_package_fixture(&project);
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

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_id = run_json["result"]["run_id"].as_str().unwrap();

    let result = resume_command(&project, run_id);

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

    let result = resume_command(&project, run_id.as_str());

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
        run_id.as_str(),
    ]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    assert_secret_absent(&result, "resume-render-secret");
    for expected in ["[plan] running run started", "[plan] failed run failed"] {
        assert!(
            result.stderr.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stderr
        );
    }
    for expected in [
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
        cdf_cli_core::args::ResumeArgs {
            run_id: Some(run_id.to_string()),
        },
        &test_execution_services(),
        &test_destination_registry(),
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    for expected in [
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
fn injected_quasar_destination_reaches_lock_plan_run_duplicate_replay_doctor_and_inspect() {
    let project = TestProject::new();
    let destination_uri = crate::destination_registry_test_support::destination_uri();
    let secret = crate::destination_registry_test_support::secret_sentinel();
    write_project_destination(&project, &destination_uri);
    let (registry, state) =
        crate::destination_registry_test_support::registry_with_quasar_destination().unwrap();

    for command in [
        vec!["contract".to_owned(), "freeze".to_owned()],
        vec!["inspect".to_owned(), "destinations".to_owned()],
        vec!["doctor".to_owned()],
        vec!["plan".to_owned(), "local.events".to_owned()],
    ] {
        let command_label = command.join(" ");
        let result = run_injected_dynamic(&project, &registry, command);
        assert_eq!(
            result.exit_code, 0,
            "{command_label} failed; stdout: {}; stderr: {}",
            result.stdout, result.stderr
        );
        assert!(
            !result.stdout.contains(secret),
            "read-only command leaked fixture secret:\n{}",
            result.stdout
        );
        assert_secret_absent(&result, secret);
        assert_eq!(
            state.durable_commits(),
            0,
            "inspection, health, lock, and planning must not mutate the destination"
        );
    }
    assert!(project.root.join("cdf.lock").is_file());
    assert!(state.inspections() >= 3);
    assert!(state.health_checks() >= 2);
    assert!(state.resolutions() >= 1);

    let loaded = run_injected_dynamic(
        &project,
        &registry,
        vec!["run".to_owned(), "local.events".to_owned()],
    );
    assert_eq!(loaded.exit_code, 0, "stderr: {}", loaded.stderr);
    assert!(
        !loaded.stdout.contains(secret),
        "run leaked fixture secret:\n{}",
        loaded.stdout
    );
    assert_secret_absent(&loaded, secret);
    assert_eq!(state.durable_commits(), 1);
    assert_eq!(state.commit_begins(), 1);
    assert!(state.plans() >= 1);
    let package_dir = run_package_dir(&project, &loaded);

    remove_state_store(&project);
    let userinfo_uri = crate::destination_registry_test_support::destination_uri_with_userinfo();
    let replayed = run_injected_dynamic(
        &project,
        &registry,
        vec![
            "replay".to_owned(),
            "package".to_owned(),
            package_dir.display().to_string(),
            "--to".to_owned(),
            userinfo_uri.clone(),
        ],
    );
    assert_eq!(replayed.exit_code, 0, "stderr: {}", replayed.stderr);
    assert!(
        !replayed.stdout.contains(secret),
        "replay leaked fixture secret:\n{}",
        replayed.stdout
    );
    assert_secret_absent(&replayed, secret);
    assert_eq!(
        state.durable_commits(),
        1,
        "duplicate replay must not create another durable destination commit"
    );
    assert_eq!(state.commit_begins(), 2);
    assert!(state.receipt_verifications() >= 2);

    remove_state_store(&project);
    let human_replay = run_injected_human_dynamic(
        &project,
        &registry,
        vec![
            "replay".to_owned(),
            "package".to_owned(),
            package_dir.display().to_string(),
            "--to".to_owned(),
            userinfo_uri.clone(),
        ],
    );
    assert_eq!(human_replay.exit_code, 0, "stderr: {}", human_replay.stderr);
    assert_secret_absent(&human_replay, secret);
    assert_eq!(state.durable_commits(), 1);

    let errored = run_injected_dynamic(
        &project,
        &registry,
        vec![
            "replay".to_owned(),
            "package".to_owned(),
            project.root.join("missing-package").display().to_string(),
            "--to".to_owned(),
            userinfo_uri,
        ],
    );
    assert_ne!(errored.exit_code, 0);
    assert_secret_absent(&errored, secret);
}

#[test]
fn injected_quasar_destination_resume_replays_finalized_package_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let destination_uri = crate::destination_registry_test_support::destination_uri();
    let secret = crate::destination_registry_test_support::secret_sentinel();
    write_project_destination(&project, &destination_uri);
    let (registry, state) =
        crate::destination_registry_test_support::registry_with_quasar_destination().unwrap();
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-quasar-replay",
        &package_dir,
        &[RunEventKind::PackageFinalized, RunEventKind::RunFailed],
    );

    let result = run_injected_dynamic(
        &project,
        &registry,
        vec!["resume".to_owned(), run_id.to_string()],
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        !result.stdout.contains(secret),
        "resume replay leaked fixture secret:\n{}",
        result.stdout
    );
    assert_secret_absent(&result, secret);
    let report = &stderr_or_stdout_json(&result.stdout)["result"];
    assert_eq!(report["state"], "package_finalized_without_receipt");
    assert_eq!(report["action"], "replay_package");
    assert_eq!(report["source_contact"], false);
    assert_eq!(report["mutated"], true);
    assert_eq!(report["receipt"]["destination_id"], "quasar");
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["package"]["status"], "checkpointed");
    assert_eq!(state.durable_commits(), 1);
    assert_eq!(state.commit_begins(), 1);
    assert_eq!(package_receipt_count(&package_dir), 1);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert!(!project.root.join("data/events.ndjson").exists());
}

#[test]
fn injected_quasar_destination_resume_verifies_durable_receipt_without_duplicate_commit() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let destination_uri = crate::destination_registry_test_support::destination_uri();
    let secret = crate::destination_registry_test_support::secret_sentinel();
    write_project_destination(&project, &destination_uri);
    let (registry, state) =
        crate::destination_registry_test_support::registry_with_quasar_destination().unwrap();
    let run_id = seed_quasar_resume_receipt_before_checkpoint(
        &project,
        &package_dir,
        &destination_uri,
        &registry,
        "run-resume-quasar-receipt",
    );
    let commits_before_resume = state.durable_commits();
    let begins_before_resume = state.commit_begins();
    let verifications_before_resume = state.receipt_verifications();

    let result = run_injected_dynamic(
        &project,
        &registry,
        vec!["resume".to_owned(), run_id.to_string()],
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        !result.stdout.contains(secret),
        "resume receipt recovery leaked fixture secret:\n{}",
        result.stdout
    );
    assert_secret_absent(&result, secret);
    let report = &stderr_or_stdout_json(&result.stdout)["result"];
    assert_eq!(
        report["state"],
        "receipt_recorded_without_checkpoint_commit"
    );
    assert_eq!(report["action"], "verify_receipt_then_commit_checkpoint");
    assert_eq!(report["source_contact"], false);
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(state.durable_commits(), commits_before_resume);
    assert_eq!(state.commit_begins(), begins_before_resume);
    assert!(state.receipt_verifications() > verifications_before_resume);
    assert_eq!(package_receipt_count(&package_dir), 1);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert!(!project.root.join("data/events.ndjson").exists());
}

#[test]
fn resume_finalized_package_without_receipt_replays_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-replay",
        &package_dir,
        &[RunEventKind::PackageFinalized, RunEventKind::RunFailed],
    );

    let result = resume_command(&project, run_id.as_str());

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
    let package_dir = create_replay_package_fixture(&project);
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
        run_id.to_string(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join("data/events.ndjson").exists());
    assert_no_headless_progress_controls(&result.stdout);
    for expected in [
        "[package] running package finalized",
        "[package] failed run failed",
        "[verify] running destination receipt recorded",
        "[gate] succeeded run resumed",
    ] {
        assert!(
            result.stderr.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stderr
        );
    }
    for expected in [
        "OK resume run run-resume-progress completed",
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
        format!(
            "{}?options=-csearch_path%3D{}\n",
            postgres.url, postgres.schema
        ),
    )
    .unwrap();
    let target = "events";
    let package_dir = create_replay_package_fixture(&project);
    write_project_destination_with_postgres_policy(
        &project,
        "postgres://secret://file/destination-dsn",
        "fail",
    );
    let mut reader = PackageReader::open(&package_dir).unwrap();
    let checkpoint_id = reader.replay_inputs().unwrap().state_delta.checkpoint_id;
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-postgres-replay",
        &package_dir,
        &[RunEventKind::PackageFinalized, RunEventKind::RunFailed],
    );

    let result = resume_command(&project, run_id.as_str());

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
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("resume Postgres checkpoint head");
    assert_eq!(head.delta.checkpoint_id, checkpoint_id);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt"]["receipt_id"].as_str().unwrap()
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
fn resume_durable_receipt_commits_uncommitted_checkpoint_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id =
        seed_resume_receipt_before_checkpoint(&project, &package_dir, "run-resume-receipt");

    let result = resume_command(&project, run_id.as_str());

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
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let package_dir = run_package_dir(&project, &run_result);
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

    let result = resume_command(&project, run_id.as_str());

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
    let first = run_valid_run_args(&project);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    fs::write(
        project.root.join("data/events.ndjson"),
        concat!(
            "{\"id\":1,\"updated_at\":1783296000000000}\n",
            "{\"id\":2,\"updated_at\":1783296060000000}\n",
            "{\"id\":3,\"updated_at\":1783296120000000}\n"
        ),
    )
    .unwrap();
    let second = run_valid_run_args(&project);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let package_dir = run_package_dir(&project, &first);
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

    let result = resume_command(&project, run_id.as_str());

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
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let package_dir = run_package_dir(&project, &run_result);
    let mut reader = PackageReader::open(&package_dir).unwrap();
    let mut wrong_receipt = collect_package_receipts(&reader)[0].clone();
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

    let result = resume_command(&project, run_id.as_str());

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

    let result = resume_command(&project, run_id.as_str());

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
    let result = run(["cdf", "--json", "--project", project.root_str(), "run"]);

    assert_eq!(result.exit_code, 2, "stderr: {}", result.stderr);
    assert_no_run_writes(&project);
    let json = assert_json_error_code(&result, "CDF-RUN-ARGUMENT");
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("run requires RESOURCE")
    );
}

#[test]
fn run_postgres_destination_missing_policy_fails_closed_before_writes() {
    let project = TestProject::new();
    write_project_destination(&project, "postgres://secret://env/WAREHOUSE");

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 3);
    assert_no_run_writes(&project);
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

    let result = run_valid_run_resource(&project, "api.items");

    assert_eq!(
        result.exit_code, 4,
        "stdout: {}\nstderr: {}",
        result.stdout, result.stderr
    );
    assert_no_run_writes(&project);
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

    let result = run_valid_run_resource(&project, "api.items");

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

    let package_dir = run_package_dir(&project, &result);
    let admission: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let validation: cdf_contract::ValidationProgram = serde_json::from_slice(
        &fs::read(package_dir.join("plan/validation-program.json")).unwrap(),
    )
    .unwrap();
    assert!(validation.schema_coercion.is_none());
    assert_eq!(admission.observations.len(), 1);
    assert!(
        admission.observations[0]
            .coercion_plan
            .fields
            .iter()
            .all(|field| field.decision == cdf_contract::FieldCoercionDecision::Preserved)
    );

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("api.items").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed REST run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
    );
}

#[test]
fn run_rest_runtime_defaults_cannot_authorize_parse_or_lossy_coercion() {
    let parse_project = TestProject::new();
    fs::write(
        parse_project.root.join("rest-token"),
        "parse-token-secret\n",
    )
    .unwrap();
    let parse_url = serve_json_once(
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": "20" }
        ] }"#,
    );
    write_rest_project(
        &parse_project,
        "duckdb://.cdf/dev.duckdb",
        &parse_url,
        "secret://file/rest-token",
    );

    let parse = run_valid_run_resource(&parse_project, "api.items");

    assert_eq!(parse.exit_code, 0, "{}", parse.stderr);
    assert_secret_absent(&parse, "parse-token-secret");
    let parse_report = stderr_or_stdout_json(&parse.stdout);
    assert_eq!(parse_report["result"]["row_count"], 1);
    let parse_package = run_package_dir(&parse_project, &parse);
    let quarantine_summary: serde_json::Value = serde_json::from_slice(
        &fs::read(parse_package.join("stats/quarantine-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine_summary["quarantined_rows"], 1);
    assert_eq!(
        quarantine_summary["artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );
    let parse_admission: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &fs::read(parse_package.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    assert!(parse_admission.observations.iter().all(|observation| {
        observation.coercion_plan.fields.iter().all(|field| {
            !matches!(
                field.decision,
                cdf_contract::FieldCoercionDecision::CoercedByPolicy
                    | cdf_contract::FieldCoercionDecision::LossyAllowed
            )
        })
    }));

    let lossy_project = TestProject::new();
    fs::write(
        lossy_project.root.join("rest-token"),
        "lossy-token-secret\n",
    )
    .unwrap();
    let lossy_url = serve_json_once(r#"{ "items": [{ "id": 1, "updated_at": 20 }] }"#);
    write_rest_project(
        &lossy_project,
        "duckdb://.cdf/dev.duckdb",
        &lossy_url,
        "secret://file/rest-token",
    );
    let resource_path = lossy_project.root.join("resources/api.toml");
    let resource = fs::read_to_string(&resource_path).unwrap().replace(
        r#"{ name = "id", type = "int64", nullable = false }"#,
        r#"{ name = "id", type = "u_int64", nullable = false }"#,
    );
    fs::write(resource_path, resource).unwrap();

    let lossy = run_valid_run_resource(&lossy_project, "api.items");

    assert_ne!(lossy.exit_code, 0, "{}{}", lossy.stdout, lossy.stderr);
    assert_secret_absent(&lossy, "lossy-token-secret");
    let lossy_output = format!("{}{}", lossy.stdout, lossy.stderr);
    assert!(
        lossy_output.contains("enable allow_lossy_mapping"),
        "{lossy_output}"
    );
    assert!(!lossy_output.contains("LossyAllowed"), "{lossy_output}");
    let package_root = lossy_project.root.join(".cdf/packages");
    if package_root.exists() {
        let packages = fs::read_dir(&package_root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert!(packages.len() <= 1);
        if let Some(package) = packages.first() {
            let coercion_path = package.join("schema/coercion-plan.json");
            if coercion_path.exists() {
                let coercion: cdf_contract::SchemaCoercionPlan =
                    serde_json::from_slice(&fs::read(coercion_path).unwrap()).unwrap();
                assert!(coercion.fields.iter().all(|field| {
                    field.decision != cdf_contract::FieldCoercionDecision::LossyAllowed
                }));
            }
        }
    }
    assert!(!lossy_project.root.join(".cdf/dev.duckdb").exists());
}

#[test]
fn duckdb_destination_policy_normalizes_plan_preview_package_and_commit() {
    const LONG_SOURCE: &str =
        "this_is_a_very_long_vendor_identifier_column_name_that_exceeds_sixty_three_bytes_total";
    let project = TestProject::new();
    fs::write(
        project.root.join("data/events.ndjson"),
        format!("{{\"VendorID\":1,\"{LONG_SOURCE}\":10}}\n"),
    )
    .unwrap();
    fs::write(
        project.root.join("resources/files.toml"),
        format!(
            r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
  {{ name = "VendorID", type = "int64", nullable = false }},
  {{ name = "{LONG_SOURCE}", type = "int64", nullable = false }},
] }}
"#,
        ),
    )
    .unwrap();

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(plan.exit_code, 0, "{}", plan.stderr);
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(
        plan_json["result"]["normalization"]["version"],
        "namecase-v1"
    );
    assert_eq!(
        plan_json["result"]["normalization"]["max_length"],
        serde_json::Value::Null
    );
    assert_eq!(
        plan_json["result"]["normalization"]["allowed_pattern"],
        "^[a-z_][a-z0-9_]*$"
    );
    assert_eq!(
        plan_json["result"]["resource_schema"]["fields"][0]["name"],
        "vendor_id"
    );
    assert_eq!(
        plan_json["result"]["resource_schema"]["fields"][1]["name"],
        LONG_SOURCE
    );

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let preview_json = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(
        preview_json["result"]["fields"],
        serde_json::json!(["vendor_id", LONG_SOURCE, "_cdf_variant"])
    );
    assert_eq!(
        preview_json["result"]["normalization"],
        plan_json["result"]["normalization"]
    );

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    let package = run_package_dir(&project, &run_result);
    let validation: serde_json::Value =
        serde_json::from_slice(&fs::read(package.join("plan/validation-program.json")).unwrap())
            .unwrap();
    assert_eq!(
        validation["identifier_policy"],
        plan_json["result"]["normalization"]
    );
    let output: serde_json::Value =
        serde_json::from_slice(&fs::read(package.join("schema/output.json")).unwrap()).unwrap();
    assert_eq!(output["fields"][0]["name"], "vendor_id");
    assert_eq!(
        output["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(output["fields"][1]["name"], LONG_SOURCE);

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let mut statement = conn.prepare("PRAGMA table_info('events')").unwrap();
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        columns,
        vec![
            ("vendor_id".to_owned(), "BIGINT".to_owned(), true),
            (LONG_SOURCE.to_owned(), "BIGINT".to_owned(), true),
            ("_cdf_variant".to_owned(), "VARCHAR".to_owned(), false),
            ("_cdf_row_key".to_owned(), "UBIGINT".to_owned(), true),
        ]
    );
}

#[test]
fn destination_normalization_collision_fails_before_writes() {
    let project = TestProject::new();
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "VendorID", type = "int64" },
  { name = "vendor_id", type = "int64" },
] }
"#,
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);

    assert_ne!(result.exit_code, 0);
    let output = format!("{}{}", result.stdout, result.stderr);
    assert!(output.contains("VendorID"), "{output}");
    assert!(output.contains("vendor_id"), "{output}");
    assert!(output.contains("explicit rename"), "{output}");
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
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
    let resource_path = project.root.join("resources/sql.toml");
    let resource = fs::read_to_string(&resource_path).unwrap().replace(
        "primary_key = [\"id\"]",
        "primary_key = [\"id\"]\ncursor = { field = \"id\", ordering = \"exact\", lag = \"0ms\" }",
    );
    fs::write(resource_path, resource).unwrap();

    let result = run_valid_run_resource(&project, "warehouse.orders");

    assert_eq!(
        result.exit_code, 4,
        "stdout: {}\nstderr: {}",
        result.stdout, result.stderr
    );
    assert_no_run_writes(&project);
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

    let result = run_valid_run_resource(&project, "warehouse.orders");

    assert_eq!(result.exit_code, 3);
    assert_no_run_writes(&project);
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

    let result = run_valid_run_resource(&project, "warehouse.orders");

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
        report["ledger_events"]["events"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["kind"],
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
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("warehouse.orders").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed SQL run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
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

    let result = run_valid_run_args(&project);

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
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed Parquet run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
    );
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

        let result = run_valid_run_args(&project);

        assert_eq!(result.exit_code, 78, "uri {uri}: {}", result.stderr);
        assert_no_run_writes(&project);
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
        "local.events".to_owned(),
    ]);

    assert_eq!(result.exit_code, 3);
    assert_no_run_writes(&project);
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

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 3);
    assert_no_run_writes(&project);
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
        format!(
            "{}?options=-csearch_path%3D{}\n",
            postgres.url, postgres.schema
        ),
    )
    .unwrap();
    write_project_destination_with_postgres_policy(
        &project,
        "postgres://secret://file/destination-dsn",
        "fail",
    );
    let target = "events";

    let result = run_valid_run_args(&project);

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
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed Postgres run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
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
fn run_local_parquet_discover_autopins_and_commits_pinned_schema() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    remove_resource_format(&project, "parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    let snapshot_path = single_schema_snapshot_path(&project);
    let snapshot = read_snapshot_json(&project, &snapshot_path);
    let snapshot_hash = snapshot["schema_hash"].as_str().unwrap();
    assert_eq!(report["schema_hash"], snapshot_hash);
    assert_eq!(report["schema_snapshot"]["schema_hash"], snapshot_hash);
    assert_eq!(snapshot["schema"]["fields"][0]["name"], "vendor_id");
    assert_eq!(
        snapshot["schema"]["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed Parquet discover run head");
    assert_eq!(head.delta.schema_hash.as_str(), report["schema_hash"]);
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
    );

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows = conn
        .prepare("SELECT vendor_id FROM events ORDER BY vendor_id")
        .unwrap()
        .query_map([], |row| row.get::<_, i32>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![1, 2]);
}

#[test]
fn pinned_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/a.parquet"));

    let baseline_plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(baseline_plan.exit_code, 0, "{}", baseline_plan.stderr);
    let baseline_report = stderr_or_stdout_json(&baseline_plan.stdout);
    let baseline_hash = baseline_report["result"]["schema_snapshot"]["schema_hash"]
        .as_str()
        .unwrap()
        .to_owned();
    let snapshot_path = baseline_report["result"]["schema_snapshot"]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();
    let snapshot_before = fs::read(project.root.join(&snapshot_path)).unwrap();
    let snapshot = read_snapshot_json(&project, &snapshot_path);
    let manifest_path = snapshot["metadata"]["cdf:discovery_manifest_path"]
        .as_str()
        .unwrap();
    let discovery_manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(project.root.join(manifest_path)).unwrap()).unwrap();
    assert_eq!(
        discovery_manifest["candidates"].as_array().unwrap().len(),
        1
    );

    write_vendor_score_parquet(&project.root.join("data/b.parquet"));
    write_empty_vendor_parquet(&project.root.join("data/c.parquet"));

    let pinned_plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(pinned_plan.exit_code, 0, "{}", pinned_plan.stderr);
    let pinned_report = stderr_or_stdout_json(&pinned_plan.stdout);
    let schema = &pinned_report["result"]["resource_schema"];
    assert_eq!(schema["schema_hash"], baseline_hash);
    assert!(schema.get("baseline_schema_hash").is_none());
    assert!(schema.get("effective_schema_hash").is_none());
    assert!(schema.get("effective_arrow_schema_hash").is_none());
    assert!(
        schema["fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| { field["name"] == "vendor_id" && field["data_type"] == "Int32" })
    );
    assert!(
        schema["fields"]
            .as_array()
            .unwrap()
            .iter()
            .all(|field| field["name"] != "score")
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
    assert_eq!(
        fs::read(project.root.join(&snapshot_path)).unwrap(),
        snapshot_before
    );
    assert_eq!(single_schema_snapshot_path(&project), snapshot_path);

    let before_preview = project_tree_snapshot(&project.root);
    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let preview_report = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_report["result"]["planned_partition_count"], 3);
    assert_eq!(
        preview_report["result"]["payload_opened_partition_count"],
        3
    );
    assert_eq!(preview_report["result"]["attested_partition_count"], 0);
    assert_eq!(preview_report["result"]["inspected_partition_count"], 3);
    assert_eq!(
        preview_report["result"]["inspected_batch_count"], 3,
        "the empty Parquet partition carries its physical schema in a zero-row batch"
    );
    assert_eq!(preview_report["result"]["row_count"], 4);
    assert_eq!(preview_report["result"]["terminal_quarantine_count"], 0);
    assert!(
        preview_report["result"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "_cdf_variant")
    );
    assert_project_tree_unchanged(&project.root, &before_preview);

    let limited_preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
        "--limit",
        "1",
    ]);
    assert_eq!(limited_preview.exit_code, 0, "{}", limited_preview.stderr);
    let limited_report = stderr_or_stdout_json(&limited_preview.stdout);
    assert_eq!(limited_report["result"]["planned_partition_count"], 3);
    assert_eq!(limited_report["result"]["selected_partition_count"], 3);
    assert_eq!(
        limited_report["result"]["payload_opened_partition_count"],
        1
    );
    assert_eq!(limited_report["result"]["inspected_partition_count"], 1);
    assert_eq!(
        limited_report["result"]["payload_uninspected_partition_count"],
        2
    );
    assert_eq!(limited_report["result"]["row_count"], 1);
    assert_eq!(limited_report["result"]["limits"]["max_rows"], 1);
    assert_eq!(limited_report["result"]["truncated"], true);
    assert_eq!(project_tree_snapshot(&project.root), before_preview);

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "{}", result.stderr);
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
    assert_eq!(
        fs::read(project.root.join(&snapshot_path)).unwrap(),
        snapshot_before
    );
    let package_dir = run_package_dir(&project, &result);
    assert!(
        !package_dir
            .join("schema/effective-schema-evidence.json")
            .exists()
    );
    let stream_admission: serde_json::Value = serde_json::from_slice(
        &fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let observations = stream_admission["observations"].as_array().unwrap();
    assert_eq!(observations.len(), 3);
    assert!(observations.iter().any(|observation| {
        observation["coercion_plan"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field["decision"] == "extra")
    }));

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed multi-file Parquet head");
    let SourcePosition::FileManifest(runtime_manifest) = &head.delta.output_position else {
        panic!("multi-file run must commit exact FileManifest identity");
    };
    // Schema evidence and processed-file checkpoint advancement cover the same
    // three-file manifest, including the zero-row file.
    assert_eq!(runtime_manifest.files.len(), 3);
    assert!(
        runtime_manifest
            .files
            .iter()
            .all(|file| file.sha256.is_some())
    );
    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 4);
    let residual_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE _cdf_variant IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(residual_rows, 2);

    drop(conn);
    fs::remove_file(project.root.join("data/a.parquet")).unwrap();
    fs::remove_file(project.root.join("data/b.parquet")).unwrap();
    fs::remove_file(project.root.join("data/c.parquet")).unwrap();
    fs::remove_file(project.root.join(".cdf/state.db")).unwrap();
    fs::remove_file(project.root.join(".cdf/dev.duckdb")).unwrap();
    let replay = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "replay",
        "package",
        package_dir.to_str().unwrap(),
    ]);
    assert_eq!(replay.exit_code, 0, "{}", replay.stderr);
    fs::write(
        package_dir.join("schema/stream-admission-evidence.json"),
        b"{\"tampered\":true}",
    )
    .unwrap();
    let verify = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "package",
        "verify",
        package_dir.to_str().unwrap(),
    ]);
    assert_ne!(verify.exit_code, 0);
}

#[test]
fn financial_freeze_quarantines_deviating_file_and_commits_mixed_processed_manifest() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    set_file_resource_trust(&project, "financial");
    write_vendor_parquet(&project.root.join("data/a.parquet"));

    let baseline = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(baseline.exit_code, 0, "{}", baseline.stderr);
    let baseline_report = stderr_or_stdout_json(&baseline.stdout);
    let baseline_hash = baseline_report["result"]["schema_snapshot"]["schema_hash"]
        .as_str()
        .unwrap()
        .to_owned();
    let snapshot_path = baseline_report["result"]["schema_snapshot"]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();
    let snapshot_before = fs::read(project.root.join(&snapshot_path)).unwrap();

    write_vendor_parquet(&project.root.join("data/b.parquet"));
    let conforming = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(conforming.exit_code, 0, "{}", conforming.stderr);
    let conforming_report = stderr_or_stdout_json(&conforming.stdout);
    let conforming_schema = &conforming_report["result"]["resource_schema"];
    assert_eq!(conforming_schema["schema_hash"], baseline_hash);
    assert!(conforming_schema.get("baseline_schema_hash").is_none());
    assert!(conforming_schema.get("effective_schema_hash").is_none());
    assert!(
        conforming_schema
            .get("effective_arrow_schema_hash")
            .is_none()
    );
    assert_eq!(conforming_schema["fields"].as_array().unwrap().len(), 1);
    assert_eq!(conforming_schema["fields"][0]["name"], "vendor_id");
    assert_eq!(conforming_schema["fields"][0]["data_type"], "Int32");
    assert_eq!(conforming_schema["fields"][0]["nullable"], false);
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
    assert_eq!(
        fs::read(project.root.join(&snapshot_path)).unwrap(),
        snapshot_before
    );

    write_wide_vendor_score_parquet(&project.root.join("data/c.parquet"));
    let drift = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(drift.exit_code, 0, "{}", drift.stderr);
    let drift_report = stderr_or_stdout_json(&drift.stdout);
    assert_eq!(
        drift_report["result"]["resource_schema"]["schema_hash"],
        baseline_hash
    );
    assert!(
        drift_report["result"]["resource_schema"]
            .get("baseline_schema_hash")
            .is_none()
    );
    let result = run_valid_run_args(&project);
    assert_eq!(result.exit_code, 0, "{}", result.stderr);
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
    assert_eq!(
        fs::read(project.root.join(&snapshot_path)).unwrap(),
        snapshot_before
    );
    let package = run_package_dir(&project, &result);
    let quarantines: serde_json::Value = serde_json::from_slice(
        &fs::read(package.join("quarantine/schema-observations.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantines.as_array().unwrap().len(), 1);
    assert_eq!(quarantines[0]["policy"], "freeze");
    assert_eq!(
        quarantines[0]["rule_id"],
        "schema-observation:freeze-deviation"
    );
    let quarantine_admission: serde_json::Value = serde_json::from_slice(
        &fs::read(package.join("quarantine/schema-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        quarantine_admission["observations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let processed: serde_json::Value = serde_json::from_slice(
        &fs::read(package.join("state/processed-observations.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(processed["observations"].as_array().unwrap().len(), 3);
    assert_eq!(
        processed["observations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["outcome"] == "quarantined")
            .count(),
        1
    );
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .unwrap();
    let SourcePosition::FileManifest(manifest) = head.delta.output_position else {
        panic!("freeze run must commit file manifest")
    };
    assert_eq!(manifest.files.len(), 3);
}

#[test]
fn governed_evolve_quarantines_incompatible_file_with_exact_arrow_field_evidence() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    let path = project.root.join("data/a.parquet");
    write_vendor_parquet(&path);
    let baseline = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(baseline.exit_code, 0, "{}", baseline.stderr);

    write_string_vendor_parquet(&path);
    let result = run_valid_run_args(&project);
    assert_eq!(result.exit_code, 0, "{}", result.stderr);
    let report = stderr_or_stdout_json(&result.stdout);
    let rendered = &report["result"]["terminal_schema_quarantines"][0];
    assert_eq!(rendered["observation_id"], "a.parquet");
    assert_eq!(rendered["rule_id"], "schema-observation:incompatible");
    assert_eq!(rendered["fields"][0]["scope"]["path"][0], "VendorID");
    assert_eq!(
        rendered["fields"][0]["observed_field"]["data_type"]["kind"],
        "utf8"
    );
    assert_eq!(
        rendered["fields"][0]["effective_field"]["data_type"]["kind"],
        "int"
    );
    assert!(rendered["remediation"].as_str().unwrap().contains("schema"));
    let package = run_package_dir(&project, &result);
    let quarantine: serde_json::Value = serde_json::from_slice(
        &fs::read(package.join("quarantine/schema-observations.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine[0]["policy"], "evolve");
    assert_eq!(quarantine[0]["rule_id"], "schema-observation:incompatible");
    let field = &quarantine[0]["fields"][0];
    assert_eq!(field["scope"]["kind"], "field_path");
    assert_eq!(field["observed_field"]["name"], "VendorID");
    assert_eq!(field["observed_field"]["data_type"]["kind"], "utf8");
    assert_eq!(field["effective_field"]["name"], "vendor_id");
    assert_eq!(field["effective_field"]["data_type"]["kind"], "int");
    let package_reader = cdf_package::PackageReader::open(&package).unwrap();
    let mut segment_count = 0_u64;
    package_reader
        .for_each_identity_segment(&mut |_| {
            segment_count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(segment_count, 0);

    write_string_vendor_parquet(&project.root.join("data/b.parquet"));
    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "run",
        "local.events",
    ]);
    assert_eq!(human.exit_code, 0, "{}", human.stderr);
    assert!(human.stdout.contains("b.parquet"));
    assert!(human.stdout.contains("VendorID"));
    assert!(human.stdout.contains("Utf8"));
    assert!(
        human.stdout.contains("publish a compat"),
        "{}",
        human.stdout
    );
}

#[test]
fn financial_freeze_admits_heterogeneous_files_that_formed_the_pinned_baseline() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    set_file_resource_trust(&project, "financial");
    write_vendor_parquet(&project.root.join("data/a.parquet"));
    write_wide_vendor_score_parquet(&project.root.join("data/b.parquet"));

    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    assert!(
        !run_package_dir(&project, &run_result)
            .join("quarantine/schema-observations.json")
            .exists()
    );
}

#[test]
fn all_quarantine_run_commits_zero_segments_and_skips_exact_identity_until_changed() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    set_file_resource_trust(&project, "financial");
    let path = project.root.join("data/a.parquet");
    write_vendor_parquet(&path);
    let baseline = run_valid_run_args(&project);
    assert_eq!(baseline.exit_code, 0, "{}", baseline.stderr);
    let baseline_report = stderr_or_stdout_json(&baseline.stdout);
    let contract_schema_hash = baseline_report["result"]["schema_hash"].clone();

    write_wide_vendor_score_parquet(&path);
    let quarantined = run_valid_run_args(&project);
    assert_eq!(quarantined.exit_code, 0, "{}", quarantined.stderr);
    let report = stderr_or_stdout_json(&quarantined.stdout);
    assert_eq!(report["result"]["schema_hash"], contract_schema_hash);
    assert_eq!(report["result"]["segment_count"], 0);
    assert_eq!(report["result"]["row_count"], 0);
    let package = run_package_dir(&project, &quarantined);
    let package_receipts =
        collect_package_receipts(&cdf_package::PackageReader::open(&package).unwrap());
    assert_eq!(package_receipts.len(), 1);
    assert!(package_receipts[0].segment_acks.is_empty());
    assert!(package.join("state/processed-observations.json").is_file());
    assert!(
        package
            .join("quarantine/schema-observations.json")
            .is_file()
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(package.join("manifest.json")).unwrap()).unwrap();
    assert!(
        manifest["identity"]["segments"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let unchanged = run_valid_run_args(&project);
    assert_eq!(unchanged.exit_code, 0, "{}", unchanged.stderr);
    let unchanged_report = stderr_or_stdout_json(&unchanged.stdout);
    assert_eq!(
        unchanged_report["result"]["file_manifest"]["changed_file_count"],
        0
    );
    assert_eq!(unchanged_report["result"]["writes"]["package"], false);

    write_wide_vendor_score_parquet_values(&path, &[9, 10, 11]);
    let changed = run_valid_run_args(&project);
    assert_eq!(changed.exit_code, 0, "{}", changed.stderr);
    let changed_report = stderr_or_stdout_json(&changed.stdout);
    assert_eq!(
        changed_report["result"]["schema_hash"],
        contract_schema_hash
    );
    assert_eq!(
        changed_report["result"]["file_manifest"]["changed_file_count"],
        1
    );

    fs::remove_file(&path).unwrap();
    fs::remove_file(project.root.join(".cdf/state.db")).unwrap();
    fs::remove_file(project.root.join(".cdf/dev.duckdb")).unwrap();
    let replay = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "replay",
        "package",
        package.to_str().unwrap(),
    ]);
    assert_eq!(replay.exit_code, 0, "{}", replay.stderr);
    assert!(
        collect_package_receipts(&cdf_package::PackageReader::open(&package).unwrap())
            .iter()
            .all(|receipt| receipt.segment_acks.is_empty())
    );
    let replay_head = SqliteCheckpointStore::open(project.root.join(".cdf/state.db"))
        .unwrap()
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        replay_head.delta.schema_hash.as_str(),
        contract_schema_hash.as_str().unwrap()
    );
}

#[test]
fn run_ndjson_discover_schema_resource_autopins_and_commits() {
    let project = TestProject::new();
    write_discovered_schema_resource(&project);

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "{}", result.stderr);
    assert!(project.root.join(".cdf/schemas").exists());
    assert!(
        run_package_dir(&project, &result)
            .join("manifest.json")
            .exists()
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource_id"], "local.events");
    assert!(project.root.join(".cdf/state.db").exists());
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
    assert_no_run_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(json["error"]["message"].as_str().unwrap().contains("loop"));
}

#[test]
fn replay_package_without_to_uses_environment_destination_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let package_id = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .identity
        .package_id
        .clone();
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
    assert_eq!(report["package_id"], package_id);
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
    let package_dir = create_replay_package_fixture(&project);
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
    assert_eq!(report["package_id"], manifest.identity.package_id.as_str());
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
    let checkpoint_id = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta
        .checkpoint_id;
    assert_eq!(report["checkpoint_id"], checkpoint_id.as_str());
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
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("replay checkpoint head");
    assert_eq!(head.delta.checkpoint_id, checkpoint_id);
    assert_eq!(head.delta.package_hash.as_str(), manifest.package_hash);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
}

#[test]
fn replay_package_duckdb_duplicate_reports_no_op() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
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
    let package_dir = create_replay_package_fixture(&project);
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
    let package_dir = create_replay_package_fixture(&project);
    let checkpoint_id = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta
        .checkpoint_id;
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
    assert_no_headless_progress_controls(&second.stderr);
    for expected in [
        "[package] running package finalized",
        "[package] failed run failed",
        "error:",
        checkpoint_id.as_str(),
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
    let package_dir = create_replay_package_fixture(&project);
    let reader = PackageReader::open(&package_dir).unwrap();
    let package_id = reader.manifest().identity.package_id.clone();
    let checkpoint_id = reader.replay_inputs().unwrap().state_delta.checkpoint_id;
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
    assert_no_headless_progress_controls(&second.stdout);
    for expected in [
        "[commit] succeeded replay recorded",
        "duplicate=true",
        "no_op=true",
    ] {
        assert!(
            second.stderr.contains(expected),
            "missing {expected:?} in:\n{}",
            second.stderr
        );
    }
    for expected in [
        &format!("OK replay package {package_id} completed"),
        "Replay",
        "Destination",
        "Duplicate",
        "Receipt",
        "Checkpoint",
        "duplicate  yes",
        "no-op      yes",
        &format!("checkpoint       {checkpoint_id}"),
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
    let package_dir = create_replay_package_fixture(&project);
    let reader = PackageReader::open(&package_dir).unwrap();
    let package_id = reader.manifest().identity.package_id.clone();
    let checkpoint_id = reader.replay_inputs().unwrap().state_delta.checkpoint_id;
    let cli = test_cli(&project);
    let output = crate::replay_command::replay_package(
        &cli,
        cdf_cli_core::args::ReplayPackageArgs {
            package_dir,
            destination_uri: Some("duckdb://.cdf/replay-rich.duckdb".to_owned()),
            target: None,
            merge_dedup: None,
        },
        &test_execution_services(),
        &test_destination_registry(),
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        &format!("\u{1b}[32m✓\u{1b}[0m replay package {package_id} completed"),
        "\u{1b}[36mReplay\u{1b}[0m",
        "\u{1b}[36mDestination\u{1b}[0m",
        "\u{1b}[36mDuplicate\u{1b}[0m",
        "\u{1b}[36mReceipt\u{1b}[0m",
        "\u{1b}[36mCheckpoint\u{1b}[0m",
        "duplicate  no",
        "no-op      no",
        &format!("checkpoint       {checkpoint_id}"),
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
    let package_dir = create_replay_package_fixture(&project);
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
    let package_dir = create_replay_package_fixture(&project);
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
    let package_dir = create_replay_package_fixture(&project);
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
    let package_dir = create_replay_package_fixture(&project);
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
    let package_dir = create_replay_package_fixture(&project);
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
        format!(
            "{}?options=-csearch_path%3D{}\n",
            postgres.url, postgres.schema
        ),
    )
    .unwrap();
    let target = "events";
    let package_dir = create_replay_package_fixture(&project);
    let manifest = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .clone();
    let receipts_before = package_receipt_count(&package_dir);

    let result = replay_package_command_with_postgres_options(
        &project,
        &package_dir,
        "postgres://secret://file/destination-dsn",
        Some(target),
        Some("fail"),
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &postgres.url);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "replay package");
    assert_eq!(report["command"], "replay package");
    assert_eq!(report["package_id"], manifest.identity.package_id.as_str());
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
    let checkpoint_id = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta
        .checkpoint_id;
    assert_eq!(report["checkpoint_id"], checkpoint_id.as_str());
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
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("replay checkpoint head");
    assert_eq!(head.delta.checkpoint_id, checkpoint_id);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
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
fn replay_package_parquet_replays_from_artifacts_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
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
    assert_eq!(report["package_id"], manifest.identity.package_id.as_str());
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
    let checkpoint_id = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta
        .checkpoint_id;
    assert_eq!(report["checkpoint_id"], checkpoint_id.as_str());
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
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("replay checkpoint head");
    assert_eq!(head.delta.checkpoint_id, checkpoint_id);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
}

#[test]
fn replay_package_parquet_malformed_uri_fails_before_mutation() {
    for uri in ["parquet://", "parquet://s3://bucket"] {
        let project = TestProject::new();
        let package_dir = create_replay_package_fixture(&project);
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
    let package_dir = create_replay_package_fixture(&project);
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
fn doctor_registered_source_probe_fails_independently_before_network_or_writes() {
    let project = TestProject::new();
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "https://private.example.test/data"
egress_allowlist = ["allowed.example.test"]

[resource.events]
glob = "events.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
] }
"#,
    )
    .unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(
        result.exit_code, 1,
        "stdout: {}\nstderr: {}",
        result.stdout, result.stderr
    );
    let json = stderr_or_stdout_json(&result.stdout);
    let source = named_check(&json, "source.files.local.events");
    assert_eq!(source["status"], "failed");
    assert_eq!(source["details"]["resource_id"], "local.events");
    assert_eq!(source["details"]["error_kind"], "auth");
    assert_eq!(source["message"], "file source inventory probe failed");
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
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

    assert_eq!(
        result.exit_code, 1,
        "stdout: {}\nstderr: {}",
        result.stdout, result.stderr
    );
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
    assert_eq!(json["result"]["failed"], 2);
    assert_eq!(
        named_check(&json, "source.postgres.warehouse.orders")["status"],
        "failed"
    );
    assert_eq!(
        named_check(&json, "source.rest.api.items")["status"],
        "failed"
    );
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
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "skipped");
    assert_eq!(python["details"]["python_resources"], 0);
    assert_eq!(python["details"]["require_free_threaded"], false);
}

#[test]
fn doctor_fails_python_resource_without_interpreter() {
    let project = TestProject::new();
    fs::write(project.root.join("cdf.toml"), PYTHON_RESOURCE_PROJECT).unwrap();
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
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
    let python = named_check(&json, "source.python.interpreter");
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
    let python = named_check(&json, "source.python.interpreter");
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
    let python = named_check(&json, "source.python.interpreter");
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
    let python = named_check(&json, "source.python.interpreter");
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
    let python = named_check(&json, "source.python.interpreter");
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
    let python = named_check(&json, "source.python.interpreter");
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
    let python = named_check(&json, "source.python.interpreter");
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
    let python = named_check(&json, "source.python.interpreter");
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
    let python = named_check(&json, "source.python.interpreter");
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
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("inconsistent version metadata"),
        "{python}"
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
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("inconsistent GIL metadata"),
        "{python}"
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
    let python = named_check(&json, "source.python.interpreter");
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
fn python_resource_plan_preview_run_and_replay_use_the_product_spine() {
    let project = TestProject::new();
    let marker = project.root.join("python-resource-executed");
    let interpreter = cdf_python::attached_interpreter_report()
        .unwrap()
        .executable;
    write_python_frontdoor_project(&project, &interpreter, &marker);

    let inspected = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "resource",
        "events.raw",
    ]);
    assert_eq!(inspected.exit_code, 0, "stderr: {}", inspected.stderr);
    let inspected = stderr_or_stdout_json(&inspected.stdout);
    assert_eq!(inspected["result"]["source_name"], "events");
    assert_eq!(inspected["result"]["resource_name"], "raw");
    assert_eq!(
        inspected["result"]["descriptor"]["freshness"]["max_age_ms"],
        2_700_000
    );
    assert!(!marker.exists(), "inspect executed the Python row callable");

    let before = project_tree_snapshot(&project.root);
    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "events.raw",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert!(!marker.exists(), "plan executed the Python row callable");
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(plan_json["result"]["resource_id"], "events.raw");
    assert_eq!(
        plan_json["result"]["will_fetch"]["partitions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "events.raw",
    ]);
    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    let preview_json = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_json["result"]["row_count"], 2);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/python.duckdb").exists());
    assert!(marker.is_file());
    fs::remove_file(&marker).unwrap();

    let run_result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "events.raw",
    ]);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let report = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(report["result"]["row_count"], 2);
    assert_eq!(report["result"]["checkpoint"]["status"], "committed");
    assert_eq!(
        report["result"]["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    assert_eq!(report["result"]["writes"]["package"], true);
    assert_eq!(report["result"]["writes"]["destination"], true);
    assert_eq!(report["result"]["writes"]["checkpoint"], true);
    let package = run_package_dir(&project, &run_result);
    assert!(package.join("manifest.json").is_file());
    if let Ok(path) = std::env::var("CDF_PYTHON_PACKAGE_DATA_HASH_OUTPUT") {
        let manifest = cdf_package::read_manifest(&package).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(b"cdf-python-package-data-v1");
        for segment in &manifest.identity.segments {
            hasher.update(segment.segment_id.as_str().as_bytes());
            hasher.update(segment.package_row_ord_start.to_le_bytes());
            hasher.update(segment.row_count.to_le_bytes());
            hasher.update(segment.sha256.as_bytes());
        }
        fs::write(path, format!("sha256:{:x}\n", hasher.finalize())).unwrap();
    }
    assert!(marker.is_file());

    fs::write(
        project.root.join("src/events.py"),
        "raise RuntimeError('Python source must not execute during replay')\n",
    )
    .unwrap();
    let replay_project = TestProject::new();
    let replay = run([
        "cdf",
        "--json",
        "--project",
        replay_project.root_str(),
        "replay",
        "package",
        package.to_str().unwrap(),
        "--to",
        "duckdb://.cdf/replayed-python.duckdb",
        "--target",
        "raw_replay",
    ]);
    assert_eq!(replay.exit_code, 0, "stderr: {}", replay.stderr);
    assert!(
        replay_project
            .root
            .join(".cdf/replayed-python.duckdb")
            .is_file()
    );
}

#[test]
fn python_resource_errors_route_to_doctor_without_path_escape() {
    let project = TestProject::new();
    let interpreter = cdf_python::attached_interpreter_report()
        .unwrap()
        .executable;
    write_python_frontdoor_project(
        &project,
        &interpreter,
        &project.root.join("must-not-execute"),
    );
    let text = fs::read_to_string(project.root.join("cdf.toml"))
        .unwrap()
        .replace(
            "python://src/events.py#raw_events",
            "python://../events.py#raw_events",
        );
    fs::write(project.root.join("cdf.toml"), text).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "events.raw",
    ]);
    assert_eq!(result.exit_code, 3);
    let error = stderr_or_stdout_json(&result.stderr);
    assert_eq!(error["error"]["code"], "CDF-SOURCE-REFERENCE");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("cdf doctor")
    );
    assert!(!project.root.join("must-not-execute").exists());
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
    let builder = package_builder!(&package_dir, "pkg-1").unwrap();
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
    assert_eq!(json["result"]["checked_file_count"], 1);
    assert_eq!(json["result"]["checked_archive_count"], 0);
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
    assert!(human.stdout.contains("path"), "{}", human.stdout);
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
    let collectible_builder = package_builder!(&collectible_dir, "pkg-gc-collectible").unwrap();
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
fn package_gc_reports_last_locally_promotable_residual_bytes() {
    let project = TestProject::new();
    let package_root = project.root.join(".cdf/packages");
    fs::create_dir_all(&package_root).unwrap();
    let (package_dir, residual_bytes) =
        build_gc_residual_package(&package_root, "pkg-gc-residual", "local.events");
    let package_hash = cdf_package::read_manifest(&package_dir)
        .unwrap()
        .package_hash;

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
    let availability = json["result"]["promotion_availability"].as_array().unwrap();
    assert_eq!(availability.len(), 1);
    assert_eq!(availability[0]["resource_id"], "local.events");
    assert_eq!(availability[0]["package_hash"], package_hash);
    assert_eq!(availability[0]["contains_local_residual_bytes"], true);
    assert_eq!(availability[0]["locally_promotable"], true);
    assert_eq!(availability[0]["local_residual_bytes"], residual_bytes);
    assert_eq!(availability[0]["promotable_residual_bytes"], residual_bytes);
    assert_eq!(
        availability[0]["last_locally_promotable_for_resource"],
        true
    );
    assert_eq!(
        availability[0]["collection_removes_last_local_promotable_copy"],
        false
    );
    assert_eq!(availability[0]["planned_action"], "retain");
    assert_eq!(availability[0]["authority"], "retained_package");

    let human = run(["cdf", "--project", project.root_str(), "package", "gc"]);
    assert_eq!(human.exit_code, 0, "{}", human.stderr);
    assert!(human.stdout.contains("local bytes"), "{}", human.stdout);
    assert!(human.stdout.contains("retain"), "{}", human.stdout);
    assert!(human.stdout.contains("Promotion availability"));
    assert!(human.stdout.contains("destination readback inferred"));
    assert!(
        human
            .stdout
            .contains("retain or restore one verified receipted package")
    );
}

#[test]
fn package_gc_explicit_directory_is_dry_run_without_deleting_collectible_artifacts() {
    let temp = TempDir::new("cdf-cli-package-gc-dry-run");
    let package_dir = temp.path().join("pkg-validated");
    let builder = package_builder!(&package_dir, "pkg-validated").unwrap();
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
        json["result"]["segment_index_path"],
        "archive/parquet/segments.ndjson"
    );
    assert_eq!(json["result"]["segment_count"], 1);
    assert_eq!(json["result"]["row_count"], 2);
    assert!(json["result"].get("segments").is_none());
    assert!(
        package_dir
            .join("archive/parquet/data/seg-000001.parquet")
            .is_file()
    );
    assert!(package_dir.join("archive/parquet/fidelity.json").is_file());
    assert!(
        package_dir
            .join("archive/parquet/segments.ndjson")
            .is_file()
    );
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
        "local.events",
        "--pipeline",
        "pipeline-1",
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
        "local.events",
        "--pipeline",
        "pipeline-1",
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
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let first_checkpoint = stderr_or_stdout_json(&first.stdout)["result"]["checkpoint_id"]
        .as_str()
        .unwrap()
        .to_owned();
    fs::write(
        project.root.join("data/events.ndjson"),
        concat!(
            "{\"id\":1,\"updated_at\":1783296000000000}\n",
            "{\"id\":2,\"updated_at\":1783296060000000}\n",
            "{\"id\":3,\"updated_at\":1783296120000000}\n"
        ),
    )
    .unwrap();
    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
    ]);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    let second_checkpoint = stderr_or_stdout_json(&second.stdout)["result"]["checkpoint_id"]
        .as_str()
        .unwrap()
        .to_owned();

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
        second_checkpoint
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
        &second_checkpoint,
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
        "checkpoint",
        &first_checkpoint,
        &second_checkpoint,
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
        &first_checkpoint,
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
        first_checkpoint
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
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let first_checkpoint = stderr_or_stdout_json(&first.stdout)["result"]["checkpoint_id"]
        .as_str()
        .unwrap()
        .to_owned();
    fs::write(
        project.root.join("data/events.ndjson"),
        concat!(
            "{\"id\":1,\"updated_at\":1783296000000000}\n",
            "{\"id\":2,\"updated_at\":1783296060000000}\n",
            "{\"id\":3,\"updated_at\":1783296120000000}\n"
        ),
    )
    .unwrap();
    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
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
        &first_checkpoint,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        &format!("OK rewound to {first_checkpoint}"),
        "Rewind",
        "marker              rewind-marker-",
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
fn state_show_human_rich_render_uses_scope_and_head_panels() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let checkpoint_id = stderr_or_stdout_json(&run_result.stdout)["result"]["checkpoint_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let output = crate::state_command::state(
        &test_cli(&project),
        cdf_cli_core::args::StateCommand::Show(cdf_cli_core::args::StateScopeArgs {
            pipeline_id: Some("cdf-run".to_owned()),
            resource_id: "local.events".to_owned(),
            scope_json: None,
            scope: vec!["kind=resource".to_owned()],
        }),
        &test_execution_services(),
        &test_destination_registry(),
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "\u{1b}[32m✓\u{1b}[0m state head found",
        "\u{1b}[36mScope\u{1b}[0m",
        "\u{1b}[36mHead\u{1b}[0m",
        checkpoint_id.as_str(),
        "\u{1b}[36m→\u{1b}[0m cdf state history local.events --pipeline cdf-run --scope kind=resource",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn state_show_renders_typed_table_snapshot_authority() {
    let project = TestProject::new();
    let position = SourcePosition::TableSnapshot(Box::new(TableSnapshotPosition {
        version: CHECKPOINT_STATE_VERSION,
        protocol: "iceberg".to_owned(),
        catalog: "glue:us-east-1:123456789012".to_owned(),
        namespace: vec!["analytics".to_owned(), "curated".to_owned()],
        table: "orders".to_owned(),
        selector: TableSnapshotSelector::Branch {
            name: "main".to_owned(),
        },
        snapshot_id: 42,
        sequence_number: 7,
        parent_snapshot_id: Some(41),
        metadata_location: "s3://warehouse/analytics/orders/metadata/v42.json".to_owned(),
        metadata_generation: "version-id:v42".to_owned(),
    }));
    let package_hash = "package-table-snapshot";
    let mut delta = status_delta("cdf-run", "checkpoint-table-snapshot", package_hash);
    delta.output_position = position.clone();
    delta.segments[0].output_position = position;
    let checkpoint_id = delta.checkpoint_id.clone();
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    store.propose(delta).unwrap();
    store
        .commit(
            &checkpoint_id,
            status_receipt(package_hash, "receipt-table-snapshot", 1_700_000_000_000),
        )
        .unwrap();

    let output = crate::state_command::state(
        &test_cli(&project),
        cdf_cli_core::args::StateCommand::Show(cdf_cli_core::args::StateScopeArgs {
            pipeline_id: Some("cdf-run".to_owned()),
            resource_id: "local.events".to_owned(),
            scope_json: None,
            scope: vec!["kind=resource".to_owned()],
        }),
        &test_execution_services(),
        &test_destination_registry(),
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "source position      table_snapshot",
        "table protocol       iceberg",
        "catalog              glue:us-east-1:123456789012",
        "table                analytics.curated.orders",
        "selector             branch:main",
        "snapshot             42",
        "sequence             7",
        "parent snapshot      41",
        "metadata generation  version-id:v42",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }

    let json_result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--pipeline",
        "cdf-run",
    ]);
    assert_eq!(json_result.exit_code, 0, "stderr: {}", json_result.stderr);
    let json = stderr_or_stdout_json(&json_result.stdout);
    assert_eq!(
        json["result"]["head"]["delta"]["output_position"]["kind"],
        "table_snapshot"
    );
    assert_eq!(
        json["result"]["head"]["delta"]["output_position"]["snapshot_id"],
        42
    );
}

#[test]
fn state_recover_commits_verified_package_receipt_without_destination_rows() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let reader = PackageReader::open(&package_dir).unwrap();
    let package_hash = reader.manifest().package_hash.clone();
    let package_id = reader.manifest().identity.package_id.clone();
    let checkpoint_id = reader.replay_inputs().unwrap().state_delta.checkpoint_id;
    let receipt_id = collect_package_receipts(&reader)[0].receipt_id.to_string();
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
    assert_eq!(report["package_id"], package_id);
    assert_eq!(report["package_hash"], package_hash);
    assert_eq!(report["selected_receipt_id"], receipt_id);
    assert_eq!(report["receipt_selection"], "single_durable_receipt");
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["destination"]["destination_id"], "duckdb");
    assert_eq!(report["checkpoint_id"], checkpoint_id.as_str());
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
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("state recover checkpoint head");
    assert_eq!(head.delta.checkpoint_id, checkpoint_id);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.to_string(),
        receipt_id
    );
}

#[test]
fn state_recover_human_headless_render_reports_receipt_checkpoint_and_limits() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let checkpoint_id = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta
        .checkpoint_id;

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
        &format!("OK recovered checkpoint {checkpoint_id}"),
        "Recovery",
        "Checkpoint",
        "Writes",
        "destination rows  no",
        "verified receipt only; destination rows were not written",
        "evidence limit:",
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
    let package_dir = create_replay_package_fixture(&project);
    let reader = PackageReader::open(&package_dir).unwrap();
    let mut receipts = collect_package_receipts(&reader);
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
    let package_dir = create_replay_package_fixture(&project);
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
    let package_dir = create_replay_package_fixture(&ambiguous_project);
    let reader = PackageReader::open(&package_dir).unwrap();
    let mut duplicate = collect_package_receipts(&reader)[0].clone();
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
    let package_dir = create_replay_package_fixture(&replay_project);
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
    let json = assert_json_error_code(&result, "CDF-RESOURCE-NOT-COMPILED");
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

    let package_dir = create_replay_package_fixture(&project);
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
    let error = cdf_cli_core::output::CliError::not_supported(
        "preview",
        "query resources",
        "native scan runtime",
    );
    let result = cdf_cli_core::output::InvocationResult::from_error(true, error);

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
    let error =
        cdf_cli_core::output::CliError::from(CdfError::destination("destination refused commit"));
    let result = cdf_cli_core::output::InvocationResult::from_error(true, error);

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

fn project_tree_snapshot(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn visit(root: &Path, directory: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
        let mut entries = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .replace(std::path::MAIN_SEPARATOR, "/"),
                    fs::read(path).unwrap(),
                );
            }
        }
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

fn assert_project_tree_unchanged(root: &Path, before: &BTreeMap<String, Vec<u8>>) {
    let after = project_tree_snapshot(root);
    if &after == before {
        return;
    }
    let changed = before
        .keys()
        .chain(after.keys())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter(|path| before.get(*path) != after.get(*path))
        .map(|path| path.as_str())
        .collect::<Vec<_>>();
    panic!("project tree changed unexpectedly at {changed:?}");
}

fn assert_generated_artifacts_exclude(root: &Path, secret: &str) {
    for (path, bytes) in project_tree_snapshot(root) {
        if path == "cdf.lock" || path.starts_with(".cdf/") {
            assert!(
                !bytes
                    .windows(secret.len())
                    .any(|window| window == secret.as_bytes()),
                "generated artifact {path} contains secret sentinel"
            );
        }
    }
}

fn assert_no_headless_progress_controls(output: &str) {
    assert!(
        !output.contains("\u{1b}["),
        "headless output must not contain ANSI controls:\n{output}"
    );
    assert!(
        !output.contains('\r'),
        "headless output must not contain carriage-return progress controls:\n{output}"
    );
}

fn assert_no_run_writes(project: &TestProject) {
    let package_root = project.root.join(".cdf/packages");
    let package_entries = package_root.exists().then(|| {
        fs::read_dir(&package_root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>()
    });
    assert!(
        package_entries.as_ref().is_none_or(Vec::is_empty),
        "rejected run must not create any package artifact: {package_entries:?}"
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

fn assert_no_schema_discovery_writes(project: &TestProject) {
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
}

fn run_package_id(result: &cdf_cli_core::output::InvocationResult) -> String {
    stderr_or_stdout_json(&result.stdout)["result"]["package_id"]
        .as_str()
        .expect("successful run report must name its minted package")
        .to_owned()
}

fn run_package_dir(
    project: &TestProject,
    result: &cdf_cli_core::output::InvocationResult,
) -> PathBuf {
    project
        .root
        .join(".cdf/packages")
        .join(run_package_id(result))
}

fn collect_package_segments_for_test(
    reader: &PackageReader,
) -> Vec<(SegmentEntry, Vec<RecordBatch>)> {
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    reader
        .verified_segment_stream(memory, 64 * 1024 * 1024)
        .unwrap()
        .map(|segment| {
            let segment = segment.unwrap();
            (segment.entry, segment.batches)
        })
        .collect()
}

fn single_package_dir(project: &TestProject) -> PathBuf {
    let mut packages = fs::read_dir(project.root.join(".cdf/packages"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    packages.sort();
    assert_eq!(packages.len(), 1, "expected exactly one package artifact");
    packages.pop().unwrap()
}

fn run_valid_run_args(project: &TestProject) -> cdf_cli_core::output::InvocationResult {
    run_valid_run_resource(project, "local.events")
}

fn run_valid_run_resource(
    project: &TestProject,
    resource_id: &str,
) -> cdf_cli_core::output::InvocationResult {
    run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "run".to_owned(),
        resource_id.to_owned(),
    ])
}

fn create_replay_package_fixture(project: &TestProject) -> PathBuf {
    let result = run_valid_run_args(project);
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let package_id = stderr_or_stdout_json(&result.stdout)["result"]["package_id"]
        .as_str()
        .unwrap()
        .to_owned();
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    remove_state_store(project);
    project.root.join(".cdf/packages").join(package_id)
}

fn replay_package_command(
    project: &TestProject,
    package_dir: &Path,
    destination_uri: &str,
) -> cdf_cli_core::output::InvocationResult {
    replay_package_command_with_postgres_options(project, package_dir, destination_uri, None, None)
}

fn replay_package_command_with_postgres_options(
    project: &TestProject,
    package_dir: &Path,
    destination_uri: &str,
    target: Option<&str>,
    merge_dedup: Option<&str>,
) -> cdf_cli_core::output::InvocationResult {
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
) -> cdf_cli_core::output::InvocationResult {
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

fn resume_command(project: &TestProject, run_id: &str) -> cdf_cli_core::output::InvocationResult {
    run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "resume".to_owned(),
        run_id.to_owned(),
    ])
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
    let execution = test_execution_services();
    let error = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.to_path_buf(),
        destination: ResolvedProjectDestination::new(Box::new(destination), target)
            .with_bound_execution_services(execution)
            .unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: Some(&hook),
    })
    .unwrap_err();
    assert!(error.to_string().contains("stop before resume checkpoint"));
    let reader = PackageReader::open(package_dir).unwrap();
    assert_eq!(collect_package_receipts(&reader).len(), 1);
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

fn seed_quasar_resume_receipt_before_checkpoint(
    project: &TestProject,
    package_dir: &Path,
    destination_uri: &str,
    registry: &cdf_runtime::DestinationRegistry,
    run_id: &str,
) -> RunId {
    let mut reader = PackageReader::open(package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(package_dir);
    let inputs = reader.replay_inputs().unwrap();
    let target = inputs.destination_commit.target.clone();
    let execution = test_execution_services();
    let resolution =
        cdf_runtime::DestinationResolutionContext::for_project_run(&project.root, &target)
            .with_environment_name("dev")
            .with_execution_services(&execution);
    let destination = registry.resolve(destination_uri, &resolution).unwrap();
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let stop_after_receipt = |_receipt: &Receipt| {
        Err(CdfError::internal(
            "stop quasar fixture before checkpoint commit",
        ))
    };
    let error = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.to_path_buf(),
        destination: ResolvedProjectDestination::new(destination, target),
        checkpoint_store: &store,
        after_receipt_verified: Some(&stop_after_receipt),
    })
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("stop quasar fixture before checkpoint commit")
    );
    let reader = PackageReader::open(package_dir).unwrap();
    assert_eq!(collect_package_receipts(&reader).len(), 1);
    assert_eq!(reader.manifest().lifecycle.status, PackageStatus::Loading);
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

    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let run_id = RunId::new(run_id).unwrap();
    ledger.create_run(Some(run_id.clone())).unwrap();
    for kind in [
        RunEventKind::PackageFinalized,
        RunEventKind::CheckpointProposed,
        RunEventKind::DestinationReceiptRecorded,
        RunEventKind::RunFailed,
    ] {
        ledger
            .append_event(&run_id, resume_package_event(kind, package_dir))
            .unwrap();
    }
    run_id
}

fn resume_package_event(kind: RunEventKind, package_dir: &Path) -> RunEventAppend {
    let reader = PackageReader::open(package_dir).unwrap();
    let inputs = reader.replay_inputs().unwrap();
    let receipts = collect_package_receipts(&reader);
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

fn package_receipt_count(package_dir: &Path) -> u64 {
    PackageReader::open(package_dir)
        .unwrap()
        .receipt_count()
        .unwrap()
}

fn remove_package_receipts(package_dir: &Path) {
    let path = package_dir.join(RECEIPTS_FILE);
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
    receipt_count: u64,
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

fn write_parquet_discover_resource(project: &TestProject, glob: &str) {
    for entry in fs::read_dir(project.root.join("data")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    fs::write(
        project.root.join("resources/files.toml"),
        format!(
            r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "{glob}"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
}

fn set_file_resource_trust(project: &TestProject, trust: &str) {
    let path = project.root.join("resources/files.toml");
    let text = fs::read_to_string(&path).unwrap();
    assert!(text.contains("trust = \"governed\""));
    fs::write(
        path,
        text.replacen("trust = \"governed\"", &format!("trust = \"{trust}\""), 1),
    )
    .unwrap();
}

fn set_file_resource_sample_files(project: &TestProject, sample_files: u64) {
    let path = project.root.join("resources/files.toml");
    let text = fs::read_to_string(&path).unwrap();
    assert!(!text.contains("sample_files ="));
    fs::write(
        path,
        text.replacen(
            "write_disposition = \"append\"",
            &format!("sample_files = {sample_files}\nwrite_disposition = \"append\""),
            1,
        ),
    )
    .unwrap();
}

fn write_arrow_ipc_discover_resource(project: &TestProject, glob: &str) {
    for entry in fs::read_dir(project.root.join("data")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    fs::write(
        project.root.join("resources/files.toml"),
        format!(
            r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "{glob}"
format = "arrow_ipc"
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
}

fn remove_resource_format(project: &TestProject, format: &str) {
    let path = project.root.join("resources/files.toml");
    let text = fs::read_to_string(&path).unwrap();
    let explicit = format!("format = \"{format}\"\n");
    assert!(text.contains(&explicit));
    fs::write(path, text.replacen(&explicit, "", 1)).unwrap();
}

fn write_vendor_arrow_ipc(project: &TestProject, filename: &str) {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("VendorID", DataType::Int32, false).with_metadata(HashMap::from([(
                "source-tag".to_owned(),
                "vendor".to_owned(),
            )])),
            Field::new("Note", DataType::Utf8, true),
        ],
        HashMap::from([("owner".to_owned(), "source-system".to_owned())]),
    ));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values([1_i32, 2_i32])),
            Arc::new(StringArray::from(vec![Some("first"), Some("second")])),
        ],
    )
    .unwrap();
    write_arrow_ipc_source(project, filename, batch);
}

fn write_large_vendor_arrow_ipc(project: &TestProject, filename: &str) {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("VendorID", DataType::Int32, false).with_metadata(HashMap::from([(
                "source-tag".to_owned(),
                "vendor".to_owned(),
            )])),
            Field::new("Note", DataType::Utf8, true),
        ],
        HashMap::from([("owner".to_owned(), "source-system".to_owned())]),
    ));
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut payload = String::with_capacity(1_000_000);
    for _ in 0..1_000_000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(char::from(b'a' + (state % 26) as u8));
    }
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values([1_i32, 2_i32])),
            Arc::new(StringArray::from(vec![
                Some(payload),
                Some("second".to_owned()),
            ])),
        ],
    )
    .unwrap();
    write_arrow_ipc_source(project, filename, batch);
}

fn write_arrow_ipc_source(project: &TestProject, filename: &str, batch: RecordBatch) {
    let path = project.root.join("data").join(filename);
    let file = fs::File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, batch.schema().as_ref()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
}

fn write_vendor_parquet(path: &Path) {
    fs::write(path, vendor_parquet_bytes(&[1, 2])).unwrap();
}

fn vendor_parquet_bytes(values: &[i32]) -> Vec<u8> {
    let fields = vec![Field::new("VendorID", DataType::Int32, false)];
    let values: ArrayRef = Arc::new(Int32Array::from_iter_values(values.iter().copied()));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![values]).unwrap();
    cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap()
}

fn write_string_vendor_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "VendorID",
        DataType::Utf8,
        false,
    )]));
    let values: ArrayRef = Arc::new(StringArray::from(vec!["one", "two"]));
    let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    fs::write(path, bytes).unwrap();
}

fn write_vendor_score_parquet(path: &Path) {
    let fields = vec![
        Field::new("VendorID", DataType::Int32, false),
        Field::new("score", DataType::Int64, false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values([1_i32, 2_i32])),
        Arc::new(Int64Array::from_iter_values([10_i64, 20_i64])),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    fs::write(path, bytes).unwrap();
}

fn write_schema_promote_package_fixture(project: &TestProject, schema_hash: &str) {
    write_schema_promote_package_fixture_for_target(
        project,
        "pkg-promote-source",
        "events",
        schema_hash,
    );
}

fn write_schema_promote_package_fixture_for_target(
    project: &TestProject,
    package_id: &str,
    target_name: &str,
    schema_hash: &str,
) {
    write_schema_promote_package_fixture_for_target_with_commit(
        project,
        package_id,
        target_name,
        schema_hash,
        true,
    );
}

fn write_schema_promote_package_fixture_for_target_with_commit(
    project: &TestProject,
    package_id: &str,
    target_name: &str,
    schema_hash: &str,
    commit_duckdb: bool,
) {
    let package_dir = project.root.join(".cdf/packages").join(package_id);
    fs::create_dir_all(project.root.join(".cdf/packages")).unwrap();
    let scores = Int64Array::from_iter_values([10_i64, 20_i64]);
    let residuals = (0..scores.len())
        .map(|row| {
            String::from_utf8(
                cdf_contract::encode_residual_json_v1([cdf_contract::ResidualFieldRef::new(
                    ["score"],
                    &scores,
                    row,
                )
                .unwrap()])
                .unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut variant = cdf_kernel::with_semantic(
        Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true),
        cdf_contract::VARIANT_SEMANTIC_TAG,
    );
    let mut metadata = variant.metadata().clone();
    metadata.insert(
        cdf_contract::RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
        cdf_contract::RESIDUAL_ENCODING_NAME.to_owned(),
    );
    variant = variant.with_metadata(metadata);
    let schema = Arc::new(Schema::new(vec![
        cdf_kernel::with_source_name(Field::new("vendor_id", DataType::Int32, false), "VendorID"),
        variant,
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values([1_i32, 2_i32])),
            Arc::new(StringArray::from(residuals)),
        ],
    )
    .unwrap();
    let builder = package_builder!(&package_dir, package_id).unwrap();
    write_current_replay_artifacts(
        &builder,
        batch.schema().as_ref(),
        schema_hash,
        batch.num_rows() as u64,
        schema_promote_fixture_position(),
    );
    let batch = cdf_package_contract::append_package_row_ord(vec![batch], 0).unwrap();
    let segment = builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), 0, &batch)
        .unwrap();
    let output_position = schema_promote_fixture_position();
    let state_segment = StateSegment {
        segment_id: segment.segment_id.clone(),
        scope: ScopeKey::Resource,
        output_position: output_position.clone(),
        row_count: segment.row_count,
        byte_count: segment.byte_count,
    };
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        pipeline_id: PipelineId::new("pipeline-run").unwrap(),
        resource_id: ResourceId::new("local.events").unwrap(),
        scope: ScopeKey::Resource,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position,
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        schema_hash: SchemaHash::new(schema_hash).unwrap(),
        segments: vec![state_segment.clone()],
    };
    builder.write_input_checkpoint_artifact(&None).unwrap();
    builder
        .write_state_delta_preimage_artifact(&state_delta)
        .unwrap();
    builder
        .write_commit_plan_preimage_artifact(&DestinationCommitPlanPreimage::package_hash_token(
            TargetName::new(target_name).unwrap(),
            WriteDisposition::Append,
            Vec::new(),
            SchemaHash::new(schema_hash).unwrap(),
        ))
        .unwrap();
    let final_status = if commit_duckdb {
        PackageStatus::Packaged
    } else {
        PackageStatus::Checkpointed
    };
    builder.finish_with_status(final_status).unwrap();
    if commit_duckdb {
        let store = SqliteCheckpointStore::open(
            project
                .root
                .join(".cdf")
                .join(format!("{package_id}-fixture-state.db")),
        )
        .unwrap();
        replay_package_from_artifacts(PackageArtifactReplayRequest {
            package_dir,
            destination: ResolvedProjectDestination::new(
                Box::new(DuckDbDestination::new(project.root.join(".cdf/dev.duckdb")).unwrap()),
                TargetName::new(target_name).unwrap(),
            )
            .with_bound_execution_services(test_execution_services())
            .unwrap(),
            checkpoint_store: &store,
            after_receipt_verified: None,
        })
        .unwrap();
    }
}

fn write_current_replay_artifacts(
    builder: &PackageBuilder,
    schema: &Schema,
    schema_hash: &str,
    row_count: u64,
    output_position: SourcePosition,
) {
    let mut program = cdf_contract::compile_validation_program(
        &cdf_contract::ContractPolicy::evolve(),
        &cdf_contract::ObservedSchema::from_arrow(schema),
    )
    .unwrap();
    program.row_rules.clear();
    program.transforms.clear();
    let schema = Arc::new(schema.clone());
    let resource = ReplayArtifactResource::new(Arc::clone(&schema), schema_hash);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            EnginePlanInput {
                request: ScanRequest {
                    resource_id: ResourceId::new("local.events").unwrap(),
                    projection: None,
                    filters: Vec::new(),
                    limit: None,
                    order_by: Vec::new(),
                    scope: ScopeKey::Resource,
                },
                validation_program: program,
                execution_extent: ExecutionExtent::bounded(),
                package_id: "cli-current-fixture-package".to_owned(),
            },
        )
        .unwrap();
    builder
        .write_json_artifact("plan/validation-program.json", &plan.validation_program)
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
    let partition = &plan.scan.inline_partitions().unwrap()[0];
    builder
        .write_lineage_artifact(
            "lineage.json",
            &cdf_package::canonical_json_bytes(&cdf_engine::LineageSummary {
                input_rows: row_count,
                input_observations: vec![cdf_engine::LineageInputObservation {
                    observation_id: "cli-current-fixture".to_owned(),
                    partition_id: partition.partition_id.clone(),
                    partition_binding: cdf_kernel::partition_schema_observation_binding(partition)
                        .unwrap(),
                    observed_rows: row_count,
                    output_position: Some(output_position.clone()),
                }],
            })
            .unwrap(),
        )
        .unwrap();
    let physical_schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap();
    let coercion_plan = plan
        .compiled_schema_admission
        .instantiate(schema.as_ref(), &physical_schema_hash)
        .unwrap();
    let physical_observation =
        cdf_engine::PhysicalObservationEvidence::arrow_schema(schema.as_ref()).unwrap();
    let physical_observation_hash = physical_observation.identity_hash().unwrap();
    builder
        .write_json_artifact(
            "schema/stream-admission-evidence.json",
            &CompiledStreamAdmissionEvidence::new(
                &plan.compiled_schema_admission,
                BTreeMap::from([(physical_observation_hash.to_string(), physical_observation)]),
                vec![
                    StreamAdmissionObservationEvidence::new(
                        "cli-current-fixture",
                        physical_observation_hash,
                        coercion_plan,
                        cdf_engine::StreamAdmissionCompletion::Complete {
                            source_position: output_position.clone(),
                            partition_binding: cdf_kernel::partition_schema_observation_binding(
                                &plan.scan.inline_partitions().unwrap()[0],
                            )
                            .unwrap(),
                        },
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    builder
        .write_json_artifact(
            cdf_package_contract::PROCESSED_OBSERVATIONS_FILE,
            &cdf_package_contract::ProcessedObservationEvidenceArtifact::new(
                None,
                WriteDisposition::Append,
                vec![
                    cdf_kernel::ProcessedObservationPosition::new(
                        "cli-current-fixture",
                        cdf_kernel::ProcessedObservationOutcome::Admitted,
                        output_position.clone(),
                    )
                    .unwrap(),
                ],
                output_position,
            )
            .unwrap(),
        )
        .unwrap();
    builder.write_runtime_arrow_schema(schema.as_ref()).unwrap();
    builder
        .write_json_artifact(
            "schema/output.arrow.json",
            &BTreeMap::from([("schema_hash", schema_hash)]),
        )
        .unwrap();
}

fn schema_promote_fixture_position() -> SourcePosition {
    SourcePosition::FileManifest(FileManifest {
        version: CHECKPOINT_STATE_VERSION,
        files: vec![FilePosition {
            path: "events.parquet".to_owned(),
            size_bytes: 1,
            source_generation: None,
            etag: None,
            object_version: None,
            sha256: Some(format!("sha256:{}", "0".repeat(64))),
        }],
    })
}

struct ReplayArtifactResource {
    descriptor: ResourceDescriptor,
    schema: Arc<Schema>,
}

impl ReplayArtifactResource {
    fn new(schema: Arc<Schema>, schema_hash: &str) -> Self {
        Self {
            descriptor: ResourceDescriptor {
                resource_id: ResourceId::new("local.events").unwrap(),
                schema_source: SchemaSource::Discovered {
                    snapshot: SchemaSnapshotReference {
                        schema_hash: SchemaHash::new(schema_hash).unwrap(),
                        path: format!(".cdf/schemas/local.events@{schema_hash}.json"),
                        metadata: BTreeMap::new(),
                    },
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

impl ResourceStream for ReplayArtifactResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> cdf_kernel::Result<Vec<PartitionPlan>> {
        let partition_id = cdf_kernel::PartitionId::new("cli-current-fixture")?;
        Ok(vec![PartitionPlan {
            partition_id,
            scope: ScopeKey::File {
                path: "events.parquet".to_owned(),
            },
            planned_position: Some(schema_promote_fixture_position()),
            start_position: None,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::new(),
        }])
    }

    fn open(&self, _partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
            let stream: BatchStream = Box::pin(futures_util::stream::empty());
            Ok(cdf_kernel::PartitionStreamPayload::batches(stream))
        }))
    }
}

#[derive(Clone, Copy)]
enum CorrectionSemanticRepackage {
    Subset,
    ValueSubstitution,
}

fn rebuild_correction_package_semantically(
    package_dir: &Path,
    tamper: CorrectionSemanticRepackage,
) {
    let reader = PackageReader::open(package_dir).unwrap();
    let input_checkpoint = reader.input_checkpoint().unwrap();
    let mut state = reader.state_delta_preimage().unwrap();
    let commit = reader.destination_commit_plan_preimage().unwrap();
    let mut artifact: cdf_project::SchemaPromotionCorrectionPackageArtifact =
        serde_json::from_slice(
            &fs::read(package_dir.join("plan/promotion-correction.json")).unwrap(),
        )
        .unwrap();
    match tamper {
        CorrectionSemanticRepackage::Subset => {
            artifact.operations.pop().unwrap();
        }
        CorrectionSemanticRepackage::ValueSubstitution => {
            let replacement = artifact.operations[1]
                .promoted_value_residual_json_v1
                .clone();
            artifact.operations[0].promoted_value_residual_json_v1 = replacement.clone();
            artifact.operations[0]
                .correction
                .request
                .promoted_value_json = String::from_utf8(replacement).unwrap();
        }
    }
    fs::remove_dir_all(package_dir).unwrap();
    let package_id = package_dir.file_name().unwrap().to_str().unwrap();
    let builder = package_builder!(package_dir, package_id).unwrap();
    builder
        .write_json_artifact("plan/promotion-correction.json", &artifact)
        .unwrap();
    builder
        .write_json_artifact("plan/validation-program.json", &artifact.validation_program)
        .unwrap();
    let operation_json = artifact
        .operations
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "correction_operation_json",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(operation_json))],
    )
    .unwrap();
    let segment_id = state.segments[0].segment_id.clone();
    let batch = cdf_package_contract::append_package_row_ord(vec![batch], 0).unwrap();
    let segment = builder.write_segment(segment_id, 0, &batch).unwrap();
    state.segments[0].row_count = segment.row_count;
    state.segments[0].byte_count = segment.byte_count;
    builder
        .write_input_checkpoint_artifact(&input_checkpoint)
        .unwrap();
    builder.write_state_delta_preimage_artifact(&state).unwrap();
    builder
        .write_commit_plan_preimage_artifact(&commit)
        .unwrap();
    builder.finish_with_status(PackageStatus::Packaged).unwrap();
    PackageReader::open(package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap();
}

fn write_wide_vendor_score_parquet(path: &Path) {
    write_wide_vendor_score_parquet_values(path, &[3, 4]);
}

fn write_wide_vendor_score_parquet_values(path: &Path, vendor_ids: &[i64]) {
    let fields = vec![
        Field::new("VendorID", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(vendor_ids.iter().copied())),
        Arc::new(Int64Array::from_iter_values(
            (0..vendor_ids.len()).map(|index| 10_i64 + index as i64),
        )),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    fs::write(path, bytes).unwrap();
}

fn write_empty_vendor_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "VendorID",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::new_empty(schema);
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    fs::write(path, bytes).unwrap();
}

fn single_schema_snapshot_path(project: &TestProject) -> String {
    let entries = schema_snapshot_paths(project);
    assert_eq!(entries.len(), 1);
    entries[0].clone()
}

fn schema_snapshot_paths(project: &TestProject) -> Vec<String> {
    let mut entries = fs::read_dir(project.root.join(".cdf/schemas"))
        .unwrap()
        .map(|entry| {
            entry
                .unwrap()
                .path()
                .strip_prefix(&project.root)
                .unwrap()
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/")
        })
        .collect::<Vec<_>>();
    entries.retain(|path| !path.ends_with(".discovery.json"));
    entries.sort();
    entries
}

fn read_snapshot_json(project: &TestProject, relative_path: &str) -> Value {
    serde_json::from_str(&fs::read_to_string(project.root.join(relative_path)).unwrap()).unwrap()
}

fn write_resource_glob(project: &TestProject, glob: &str) {
    fs::write(
        project.root.join("resources/files.toml"),
        RESOURCE.replace("glob = \"*.ndjson\"", &format!("glob = \"{glob}\"")),
    )
    .unwrap();
}

fn write_resource_disposition(project: &TestProject, disposition: &str) {
    let mut resource = RESOURCE.replace(
        "write_disposition = \"append\"",
        &format!("write_disposition = \"{disposition}\""),
    );
    if disposition == "merge" {
        resource = resource.replace("primary_key = [\"id\"]", "merge_key = [\"id\"]");
    }
    fs::write(project.root.join("resources/files.toml"), resource).unwrap();
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
    let batch = preview_fixture_batch();
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    fs::write(project.root.join("data/events.parquet"), bytes).unwrap();
}

fn write_arrow_ipc_preview_fixture(project: &TestProject) {
    write_arrow_ipc_source(project, "events.arrow", preview_fixture_batch());
}

fn preview_fixture_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
            Arc::new(Int64Array::from(vec![
                1_783_296_000_000_000_i64,
                1_783_296_060_000_000_i64,
            ])),
        ],
    )
    .unwrap()
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
    let builder = package_builder!(&package_dir, package_id).unwrap();
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
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
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

fn rest_discover_resource_with_base_url(base_url: &str, token: &str) -> String {
    format!(
        r#"
[source.api]
kind = "rest"
base_url = "{base_url}"
auth = {{ kind = "bearer", token = "{token}" }}
egress_allowlist = ["127.0.0.1"]

[resource.items]
path = "/items"
records = "$.items"
primary_key = ["vendor_id"]
cursor = {{ field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }}
write_disposition = "append"
trust = "governed"
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

fn serve_json_sequence<I>(bodies: I) -> (String, Arc<Mutex<Vec<String>>>)
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let bodies = bodies.into_iter().map(Into::into).collect::<Vec<_>>();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_thread = Arc::clone(&requests);
    thread::spawn(move || {
        for body in bodies {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let bytes_read = stream.read(&mut request).unwrap_or(0);
            requests_for_thread
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&request[..bytes_read]).into_owned());
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
        }
    });
    (format!("http://{address}"), requests)
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

fn serve_parquet_file(bytes: Vec<u8>, max_requests: usize) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_for_thread = Arc::clone(&requests);
    thread::spawn(move || {
        for _ in 0..max_requests {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut request = [0_u8; 8192];
            let bytes_read = stream.read(&mut request).unwrap_or(0);
            let request_text = String::from_utf8_lossy(&request[..bytes_read]).into_owned();
            requests_for_thread
                .lock()
                .unwrap()
                .push(request_text.clone());
            let method = request_text
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().next())
                .unwrap_or("GET");
            let range = request_text.lines().find_map(parse_range_header);
            let response = match (method, range) {
                ("HEAD", _) => format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\naccept-ranges: bytes\r\netag: \"yellow-fixture\"\r\nconnection: close\r\n\r\n",
                    bytes.len()
                )
                .into_bytes(),
                (_, Some((start, end))) => {
                    let end = end.min(bytes.len().saturating_sub(1));
                    let body = &bytes[start..=end];
                    let mut response = format!(
                        "HTTP/1.1 206 Partial Content\r\ncontent-length: {}\r\ncontent-range: bytes {start}-{end}/{}\r\naccept-ranges: bytes\r\netag: \"yellow-fixture\"\r\nconnection: close\r\n\r\n",
                        body.len(),
                        bytes.len()
                    )
                    .into_bytes();
                    response.extend_from_slice(body);
                    response
                }
                _ => {
                    let mut response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\naccept-ranges: bytes\r\netag: \"yellow-fixture\"\r\nconnection: close\r\n\r\n",
                        bytes.len()
                    )
                    .into_bytes();
                    response.extend_from_slice(&bytes);
                    response
                }
            };
            stream.write_all(&response).unwrap();
            stream.flush().unwrap();
        }
    });
    (format!("http://{address}"), requests)
}

type ServedParquetFiles = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

fn serve_parquet_files(
    initial: BTreeMap<String, Vec<u8>>,
    max_requests: usize,
) -> (String, ServedParquetFiles, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let files = Arc::new(Mutex::new(initial));
    let thread_files = Arc::clone(&files);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let thread_requests = Arc::clone(&requests);
    thread::spawn(move || {
        for _ in 0..max_requests {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut request = [0_u8; 8192];
            let bytes_read = stream.read(&mut request).unwrap_or(0);
            let request_text = String::from_utf8_lossy(&request[..bytes_read]).into_owned();
            thread_requests.lock().unwrap().push(request_text.clone());
            let mut request_line = request_text
                .lines()
                .next()
                .unwrap_or("GET / HTTP/1.1")
                .split_whitespace();
            let method = request_line.next().unwrap_or("GET");
            let path = request_line.next().unwrap_or("/");
            let bytes = thread_files.lock().unwrap().get(path).cloned();
            let range = request_text.lines().find_map(parse_range_header);
            let response = match (method, bytes, range) {
                (_, None, _) => b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_vec(),
                ("HEAD", Some(bytes), _) => format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\naccept-ranges: bytes\r\netag: \"{}\"\r\nconnection: close\r\n\r\n",
                    bytes.len(), path
                ).into_bytes(),
                (_, Some(bytes), Some((start, end))) => {
                    let end = end.min(bytes.len().saturating_sub(1));
                    let body = &bytes[start..=end];
                    let mut response = format!(
                        "HTTP/1.1 206 Partial Content\r\ncontent-length: {}\r\ncontent-range: bytes {start}-{end}/{}\r\naccept-ranges: bytes\r\netag: \"{}\"\r\nconnection: close\r\n\r\n",
                        body.len(), bytes.len(), path
                    ).into_bytes();
                    response.extend_from_slice(body);
                    response
                }
                (_, Some(bytes), None) => {
                    let mut response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\naccept-ranges: bytes\r\netag: \"{}\"\r\nconnection: close\r\n\r\n",
                        bytes.len(), path
                    ).into_bytes();
                    response.extend_from_slice(&bytes);
                    response
                }
            };
            stream.write_all(&response).unwrap();
            stream.flush().unwrap();
        }
    });
    (format!("http://{address}"), files, requests)
}

fn parse_range_header(line: &str) -> Option<(usize, usize)> {
    let (name, value) = line.split_once(':')?;
    if !name.eq_ignore_ascii_case("range") {
        return None;
    }
    let range = value.trim().strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
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

fn sql_discover_resource(connection: &str, table: &str) -> String {
    format!(
        r#"
[source.warehouse]
kind = "sql"
connection = "{connection}"
dialect = "postgres"

[resource.orders]
table = "{table}"
write_disposition = "append"
trust = "governed"
"#
    )
}

fn sql_discover_resource_with_vendor_cursor(connection: &str, table: &str) -> String {
    format!(
        r#"
[source.warehouse]
kind = "sql"
connection = "{connection}"
dialect = "postgres"

[resource.orders]
table = "{table}"
cursor = {{ field = "vendor_id", ordering = "exact", lag = "0ms" }}
write_disposition = "append"
trust = "governed"
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

fn seed_ordered_cursor_table(postgres: &LivePostgres, table: &str, values: &str) -> String {
    let table = postgres.table(table);
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"id\" BIGINT NOT NULL,
                \"updated_at\" BIGINT NOT NULL
            );
            INSERT INTO {} (\"id\", \"updated_at\") VALUES {}",
            table, table, values
        ))
        .unwrap();
    table
}

fn write_sql_project_with_secret(
    project: &TestProject,
    postgres: &LivePostgres,
    table: &str,
) -> String {
    let password = format!(
        "cdf-test-{}-{}",
        std::process::id(),
        LIVE_POSTGRES_SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let source_dsn = postgres.url.replacen(
        "postgresql://cdf@",
        &format!("postgresql://cdf:{password}@"),
        1,
    );
    fs::write(project.root.join("sql-dsn"), format!("{source_dsn}\n")).unwrap();
    write_secret_project(
        project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/sql-dsn"),
    );
    fs::write(
        project.root.join("resources/sql.toml"),
        sql_resource_with_ordered_cursor("secret://file/sql-dsn", table),
    )
    .unwrap();
    source_dsn
}

fn assert_secret_absent(result: &cdf_cli_core::output::InvocationResult, secret: &str) {
    assert!(!result.stdout.contains(secret), "stdout leaked {secret}");
    assert!(!result.stderr.contains(secret), "stderr leaked {secret}");
}

fn assert_no_key_nudge(result: &cdf_cli_core::output::InvocationResult) {
    let output = format!("{}{}", result.stdout, result.stderr).to_ascii_lowercase();
    for forbidden in [
        "primary_key",
        "merge_key",
        "missing key",
        "add a key",
        "invent a key",
    ] {
        assert!(
            !output.contains(forbidden),
            "keyless append output contained {forbidden:?}:\n{output}"
        );
    }
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
arrow_rs = "58.3.0"
"#,
    )
    .unwrap();
}

fn create_system_sql_fixture(project: &TestProject) -> SystemSqlFixture {
    let package_root = project.root.join(".cdf/packages");
    fs::create_dir_all(&package_root).unwrap();
    let package_dir = package_root.join("pkg-sql-1");
    let builder = package_builder!(&package_dir, "pkg-sql-1").unwrap();
    let batch = cdf_package_contract::append_package_row_ord(vec![sample_sql_batch()], 0).unwrap();
    builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), 0, &batch)
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
    let builder = package_builder!(&package_dir, "pkg-doctor-1").unwrap();
    let batch = sample_sql_batch();
    write_current_replay_artifacts(
        &builder,
        batch.schema().as_ref(),
        "schema-doctor-1",
        batch.num_rows() as u64,
        doctor_output_position(42),
    );
    let batch = cdf_package_contract::append_package_row_ord(vec![batch], 0).unwrap();
    let entry = builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), 0, &batch)
        .unwrap();
    let output_position = doctor_output_position(42);
    let segment = doctor_state_segment(&entry, output_position.clone());
    let state_delta = doctor_delta_preimage(output_position.clone(), segment.clone());
    builder.write_input_checkpoint_artifact(&None).unwrap();
    builder
        .write_state_delta_preimage_artifact(&state_delta)
        .unwrap();
    builder
        .write_commit_plan_preimage_artifact(&DestinationCommitPlanPreimage::package_hash_token(
            TargetName::new("events").unwrap(),
            WriteDisposition::Append,
            Vec::new(),
            SchemaHash::new("schema-doctor-1").unwrap(),
        ))
        .unwrap();
    let manifest = builder.finish_with_status(PackageStatus::Packaged).unwrap();
    let package_hash = PackageHash::new(manifest.package_hash).unwrap();
    let commit_store = SqliteCheckpointStore::open(
        project
            .root
            .join(".cdf/doctor-destination-fixture-state.db"),
    )
    .unwrap();
    let outcome = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: ResolvedProjectDestination::new(
            Box::new(DuckDbDestination::new(project.root.join(".cdf/dev.duckdb")).unwrap()),
            TargetName::new("events").unwrap(),
        )
        .with_bound_execution_services(test_execution_services())
        .unwrap(),
        checkpoint_store: &commit_store,
        after_receipt_verified: None,
    })
    .unwrap();

    let ledger_output_position = match mode {
        DoctorDriftFixtureMode::Clean => output_position,
        DoctorDriftFixtureMode::StatePositionDrift => doctor_output_position(43),
        DoctorDriftFixtureMode::TargetDrift => output_position,
    };
    let delta = doctor_delta(&package_hash, ledger_output_position, &segment);
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

fn doctor_state_segment(entry: &SegmentEntry, output_position: SourcePosition) -> StateSegment {
    StateSegment {
        segment_id: entry.segment_id.clone(),
        scope: ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        },
        output_position,
        row_count: entry.row_count,
        byte_count: entry.byte_count,
    }
}

fn doctor_delta_preimage(
    output_position: SourcePosition,
    segment: StateSegment,
) -> StateDeltaPreimage {
    StateDeltaPreimage {
        checkpoint_id: CheckpointId::new("checkpoint-doctor-1").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("local.events").unwrap(),
        scope: ScopeKey::Resource,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: output_position.clone(),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        schema_hash: SchemaHash::new("schema-doctor-1").unwrap(),
        segments: vec![segment],
    }
}

fn doctor_delta(
    package_hash: &PackageHash,
    output_position: SourcePosition,
    segment: &StateSegment,
) -> StateDelta {
    let mut segment = segment.clone();
    segment.output_position = output_position.clone();
    doctor_delta_preimage(output_position, segment).into_state_delta(package_hash.clone())
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
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
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

fn run<const N: usize>(args: [&str; N]) -> cdf_cli_core::output::InvocationResult {
    invoke(args.into_iter().map(OsString::from))
}

fn run_dynamic(args: Vec<String>) -> cdf_cli_core::output::InvocationResult {
    invoke(args.into_iter().map(OsString::from))
}

fn run_injected_dynamic(
    project: &TestProject,
    registry: &cdf_runtime::DestinationRegistry,
    command: Vec<String>,
) -> cdf_cli_core::output::InvocationResult {
    let mut args = vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
    ];
    args.extend(command);
    crate::invoke_with_destination_registry(args.into_iter().map(OsString::from), registry)
}

fn run_injected_human_dynamic(
    project: &TestProject,
    registry: &cdf_runtime::DestinationRegistry,
    command: Vec<String>,
) -> cdf_cli_core::output::InvocationResult {
    let mut args = vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
    ];
    args.extend(command);
    crate::invoke_with_destination_registry(args.into_iter().map(OsString::from), registry)
}

fn render_rich(
    output: cdf_cli_core::output::CommandOutput,
) -> cdf_cli_core::output::InvocationResult {
    cdf_cli_core::output::InvocationResult::from_output(false, &rich_render_config(), output)
}

fn rich_render_config() -> cdf_cli_core::render::RenderConfig {
    cdf_cli_core::render::RenderConfig::new(
        cdf_cli_core::render::config::DisplayMode::Tty,
        96,
        cdf_cli_core::render::config::RenderEnv {
            no_color: false,
            clicolor_force: false,
            unicode_supported: true,
        },
        cdf_cli_core::terminal::TerminalPolicy::default(),
    )
}

fn test_cli(project: &TestProject) -> cdf_cli_core::args::Cli {
    cdf_cli_core::args::Cli {
        json: false,
        terminal: cdf_cli_core::terminal::TerminalPolicy::default(),
        project: Some(project.root.clone()),
        env: None,
        memory_budget: None,
        spill_budget: None,
        command: cdf_cli_core::args::Command::Version,
    }
}

fn build_archive_cli_package(root: &Path, package_id: &str) -> PathBuf {
    let package_dir = root.join(package_id);
    let builder = package_builder!(&package_dir, package_id).unwrap();
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
    let batch = cdf_package_contract::append_package_row_ord(vec![batch], 0).unwrap();
    builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), 0, &batch)
        .unwrap();
    builder.finish_with_status(PackageStatus::Packaged).unwrap();
    package_dir
}

fn build_gc_residual_package(root: &Path, package_id: &str, resource_id: &str) -> (PathBuf, u64) {
    let package_dir = root.join(package_id);
    let builder = package_builder!(&package_dir, package_id).unwrap();
    let mut variant = with_semantic(
        Field::new(VARIANT_COLUMN_NAME, DataType::Utf8, true),
        VARIANT_SEMANTIC_TAG,
    );
    let mut metadata = variant.metadata().clone();
    metadata.insert(
        RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
        RESIDUAL_ENCODING_NAME.to_owned(),
    );
    variant = variant.with_metadata(metadata);
    let values = Int64Array::from_iter_values([1_i64, 12_345_i64]);
    let residuals = (0..values.len())
        .map(|row| {
            String::from_utf8(
                cdf_contract::encode_residual_json_v1([cdf_contract::ResidualFieldRef::new(
                    ["x"],
                    &values,
                    row,
                )
                .unwrap()])
                .unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let residual_byte_count = residuals.iter().map(String::len).sum::<usize>() as u64;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![variant])),
        vec![Arc::new(StringArray::from(residuals))],
    )
    .unwrap();
    let batch = cdf_package_contract::append_package_row_ord(vec![batch], 0).unwrap();
    let segment = builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), 0, &batch)
        .unwrap();
    let output_position = SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "row".to_owned(),
        value: CursorValue::I64(2),
    });
    let scope = ScopeKey::Resource;
    builder.write_input_checkpoint_artifact(&None).unwrap();
    let state_segment = StateSegment {
        segment_id: segment.segment_id,
        scope: scope.clone(),
        output_position: output_position.clone(),
        row_count: segment.row_count,
        byte_count: segment.byte_count,
    };
    let schema_hash = SchemaHash::new("schema-gc-residual").unwrap();
    builder
        .write_state_delta_preimage_artifact(&StateDeltaPreimage {
            checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
            pipeline_id: PipelineId::new("pipeline-gc").unwrap(),
            resource_id: ResourceId::new(resource_id).unwrap(),
            scope: scope.clone(),
            state_version: CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: None,
            input_position: None,
            output_position: output_position.clone(),
            output_watermark: None,
            partition_watermarks: Vec::new(),
            late_data_carryover: Vec::new(),
            source_continuation: None,
            schema_hash: schema_hash.clone(),
            segments: vec![state_segment.clone()],
        })
        .unwrap();
    builder
        .write_commit_plan_preimage_artifact(&DestinationCommitPlanPreimage::package_hash_token(
            TargetName::new("events").unwrap(),
            WriteDisposition::Append,
            Vec::new(),
            schema_hash.clone(),
        ))
        .unwrap();
    let manifest = builder.finish_with_status(PackageStatus::Packaged).unwrap();
    let package_hash = PackageHash::new(manifest.package_hash).unwrap();
    PackageReader::open(&package_dir)
        .unwrap()
        .append_receipt(Receipt {
            receipt_id: ReceiptId::new(format!("receipt-{package_id}")).unwrap(),
            destination: DestinationId::new("duckdb").unwrap(),
            target: TargetName::new("events").unwrap(),
            package_hash: package_hash.clone(),
            segment_acks: vec![SegmentAck {
                segment_id: state_segment.segment_id,
                row_count: state_segment.row_count,
                byte_count: state_segment.byte_count,
            }],
            disposition: WriteDisposition::Append,
            idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
            transaction: None,
            counts: CommitCounts {
                rows_written: 2,
                rows_inserted: Some(2),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash,
            migrations: Vec::new(),
            committed_at_ms: 1,
            verify: VerifyClause {
                kind: "fixture".to_owned(),
                statement: "fixture".to_owned(),
                parameters: BTreeMap::new(),
            },
        })
        .unwrap();
    (package_dir, residual_byte_count)
}

fn stderr_or_stdout_json(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

fn assert_json_error_code(result: &cdf_cli_core::output::InvocationResult, code: &str) -> Value {
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

fn write_python_frontdoor_project(project: &TestProject, interpreter: &Path, marker: &Path) {
    fs::create_dir_all(project.root.join("src")).unwrap();
    fs::write(
        project.root.join("cdf.toml"),
        format!(
            r#"
[project]
name = "python_frontdoor"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/python.duckdb"

[python]
interpreter = {}

[resources."events.raw"]
source = "python://src/events.py#raw_events"
trust = "governed"
freshness = {{ expect_every = "15m", alert_after = "45m" }}
"#,
            serde_json::to_string(interpreter.to_str().unwrap()).unwrap()
        ),
    )
    .unwrap();
    fs::write(
        project.root.join("src/events.py"),
        format!(
            r#"
def raw_events():
    with open({}, "a", encoding="utf-8") as marker:
        marker.write("called\n")
    yield {{"id": 1, "name": "ada", "updated_at": 10}}
    yield {{"id": 2, "name": "grace", "updated_at": 20}}

raw_events.__cdf_resource__ = True
raw_events.__cdf_primary_key__ = ()
raw_events.__cdf_merge_key__ = ()
raw_events.__cdf_cursor__ = "updated_at"
raw_events.__cdf_bounded__ = True
raw_events.__cdf_schema__ = (("id", "int64", False), ("name", "utf8", False), ("updated_at", "int64", False))
raw_events.__cdf_write_disposition__ = "append"
"#,
            serde_json::to_string(marker.to_str().unwrap()).unwrap()
        ),
    )
    .unwrap();
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
