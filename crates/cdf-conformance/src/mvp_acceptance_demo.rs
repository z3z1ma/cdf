use std::{
    cell::Cell,
    ffi::OsString,
    fs,
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
};

use cdf_dest_duckdb::DuckDbDestination;
use cdf_engine::{EnginePlan, EnginePlanInput, Planner};
use cdf_kernel::ExecutionExtent;
use cdf_kernel::{
    CdfError, CheckpointId, CheckpointStatus, CheckpointStore, CursorValue, DestinationProtocol,
    PipelineId, QueryableResource, Receipt, ResourceId, Result, RunId, ScanRequest, ScopeKey,
    SourcePosition, TargetName,
};
use cdf_package::PackageReader;
use cdf_package_contract::PackageStatus;
use cdf_project::{
    PackageArtifactReplayRequest, ProjectReceiptSource, ProjectRunRequest, ProjectRunSource,
    ResolvedProjectDestination, replay_package_from_artifacts, run_project,
};
use cdf_state_sqlite::SqliteCheckpointStore;
use duckdb::Connection as DuckConnection;
use serde::Serialize;
use serde_json::Value;

use crate::{
    live_run::drift_quarantine::{DuckDbDriftQuarantineDemoEvidence, run_duckdb_demo},
    run_matrix::test_support::{RecordingTransport, StaticSecretProvider, json_response},
};

const PROJECT_NAME: &str = "mvp_acceptance_demo";
const RESOURCE_ID: &str = "github.issues";
const TARGET: &str = "issues";
const SECRET_REF: &str = "secret://file/github-token";
const SECRET_VALUE: &str = "source-demo-token";
const PACKAGE_ID: &str = "mvp-acceptance-demo-github-issues";
const CHECKPOINT_ID: &str = "checkpoint-mvp-acceptance-demo-github-issues";
const PIPELINE_ID: &str = "pipeline-mvp-acceptance-demo";
const RUN_ID: &str = "run-mvp-acceptance-demo-crash";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct MvpAcceptanceDemoEvidence {
    cli_plan_before_source_contact: bool,
    plan_resource_id: String,
    plan_partition_count: usize,
    plan_endpoint_path: String,
    plan_query_shape: Vec<String>,
    contract_freeze_count: u64,
    contract_test_passed_count: u64,
    rest_request_count_before_resume: usize,
    rest_request_count_after_resume: usize,
    rest_request_method: String,
    rest_request_url_redacted: String,
    rest_authorization_header: String,
    crash_boundary: &'static str,
    resume_source_contact: bool,
    accepted_issue_rows: i64,
    accepted_issue_titles: Vec<String>,
    cdf_sql_package_rows: usize,
    cdf_sql_surface_boundary: &'static str,
    checkpoint_history_statuses: Vec<String>,
    committed_state_backed_by_duckdb_load: bool,
    committed_state_backed_by_duckdb_state: bool,
    replay_rows_in_second_duckdb: i64,
    duplicate_replay_noop: bool,
    duplicate_replay_receipt_source: String,
    drift: DuckDbDriftQuarantineDemoEvidence,
    lower_api_boundaries: Vec<&'static str>,
}

#[test]
fn mvp_acceptance_demo_fixture_proves_rest_duckdb_recovery_replay_and_drift() {
    let temp = tempfile::tempdir().unwrap();
    let project = DemoProject::new(temp.path()).unwrap();
    project.write_files().unwrap();

    let plan_human = invoke_human(project.root(), ["plan", RESOURCE_ID]);
    let plan_json = invoke_json(project.root(), ["plan", RESOURCE_ID]);
    assert_eq!(plan_json["command"], "plan");
    assert_eq!(plan_json["result"]["resource_id"], RESOURCE_ID);
    let cli_plan_package_id = plan_json["result"]["package_id"].as_str().unwrap();
    assert!(cli_plan_package_id.starts_with("cli-"));
    assert_eq!(
        plan_json["result"]["state_advancement"]["advances_after"],
        "destination receipt is recorded and CheckpointStore::commit verifies coverage"
    );
    assert!(
        !project.package_root().join(cli_plan_package_id).exists(),
        "plan must run before package bytes are written"
    );

    let freeze_json = invoke_json(project.root(), ["contract", "freeze", RESOURCE_ID]);
    assert_eq!(freeze_json["result"]["counts"]["frozen"], 1);
    let contract_test_json = invoke_json(project.root(), ["contract", "test", RESOURCE_ID]);
    assert_eq!(contract_test_json["result"]["counts"]["passed"], 1);
    let contract_human = invoke_human(project.root(), ["contract", "test", RESOURCE_ID]);

    let (resource, transport) = github_issues_resource().unwrap();
    let destination = crate::destination_catalog::resolve(
        &crate::destination_catalog::local_uri("duckdb", &project.destination_path()),
        project.root(),
        TargetName::new(TARGET).unwrap(),
    )
    .unwrap();
    let plan = engine_plan(resource.queryable(), PACKAGE_ID, &destination).unwrap();
    let plan = resource.bind_plan(plan).unwrap();
    assert_plan_matches_github_issues(&plan);

    let receipt_gate_observed = Cell::new(false);
    let gate = |receipt: &Receipt| {
        assert_eq!(receipt.counts.rows_written, 2);
        assert_no_checkpoint_head(
            &project.state_store_path(),
            &PipelineId::new(PIPELINE_ID)?,
            &ResourceId::new(RESOURCE_ID)?,
            &ScopeKey::Resource,
        )?;
        receipt_gate_observed.set(true);
        panic!("simulate crash after destination receipt before checkpoint commit");
    };
    let previous_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let services = crate::test_execution_services();
    let crashed = panic::catch_unwind(AssertUnwindSafe(|| {
        futures_executor::block_on(run_project(
            ProjectRunRequest {
                resource: ProjectRunSource::new(resource.queryable()),
                plan,
                package_root: project.package_root(),
                state_store_path: project.state_store_path(),
                pipeline_id: PipelineId::new(PIPELINE_ID).unwrap(),
                package_id: PACKAGE_ID.to_owned(),
                checkpoint_id: CheckpointId::new(CHECKPOINT_ID).unwrap(),
                destination,
                run_id: Some(RunId::new(RUN_ID).unwrap()),
                event_sink: None,
                after_receipt_verified: Some(&gate),
            },
            &services,
        ))
    }));
    panic::set_hook(previous_hook);
    match crashed {
        Ok(Ok(report)) => {
            panic!("fixture must stop in the crash window, but run succeeded: {report:?}")
        }
        Ok(Err(error)) => panic!("run failed before crash hook: {error}"),
        Err(_) => {}
    }
    assert!(receipt_gate_observed.get(), "receipt gate must be reached");
    assert_eq!(transport.requests().len(), 1);

    let package_dir = project.package_root().join(PACKAGE_ID);
    let crash_receipts = PackageReader::open(&package_dir)
        .unwrap()
        .receipts()
        .unwrap();
    assert_eq!(crash_receipts.len(), 1);
    assert_eq!(
        SqliteCheckpointStore::open(project.state_store_path())
            .unwrap()
            .head(
                &PipelineId::new(PIPELINE_ID).unwrap(),
                &ResourceId::new(RESOURCE_ID).unwrap(),
                &ScopeKey::Resource,
            )
            .unwrap(),
        None
    );

    let resume_json = invoke_json(project.root(), ["resume", RUN_ID]);
    assert_eq!(resume_json["command"], "resume");
    assert_eq!(resume_json["result"]["recovery"]["result"], "success");
    assert_eq!(resume_json["result"]["source_contact"], false);
    assert_eq!(
        resume_json["result"]["action"],
        "verify_receipt_then_commit_checkpoint"
    );
    assert_eq!(transport.requests().len(), 1);

    let accepted = read_accepted_issues(&project.destination_path()).unwrap();
    assert_eq!(
        accepted,
        vec![
            (101, "Fix flaky package replay".to_owned()),
            (102, "Document drift quarantine".to_owned()),
        ]
    );

    let sql_json = invoke_json(
        project.root(),
        [
            "sql",
            "select package_id, status from packages order by package_id",
        ],
    );
    assert_eq!(sql_json["command"], "sql");
    assert_eq!(sql_json["result"]["rows"].as_array().unwrap().len(), 1);
    let sql_human = invoke_human(
        project.root(),
        [
            "sql",
            "select package_id, status from packages order by package_id",
        ],
    );

    let history_json = invoke_json(
        project.root(),
        ["state", "history", RESOURCE_ID, "--pipeline", PIPELINE_ID],
    );
    let history = history_json["result"]["history"].as_array().unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0]["status"], "committed");
    assert_eq!(history[0]["delta"]["checkpoint_id"], CHECKPOINT_ID);
    let history_human = invoke_human(
        project.root(),
        ["state", "history", RESOURCE_ID, "--pipeline", PIPELINE_ID],
    );

    let head = SqliteCheckpointStore::open(project.state_store_path())
        .unwrap()
        .head(
            &PipelineId::new(PIPELINE_ID).unwrap(),
            &ResourceId::new(RESOURCE_ID).unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("resumed run must commit a checkpoint head");
    assert_eq!(head.status, CheckpointStatus::Committed);
    assert_eq!(
        head.receipt.as_ref().unwrap().package_hash,
        head.delta.package_hash
    );
    assert_checkpoint_position(&head.delta.output_position);

    let destination = DuckDbDestination::new(project.destination_path()).unwrap();
    let verification =
        DestinationProtocol::verify(&destination, head.receipt.as_ref().unwrap()).unwrap();
    assert!(verification.verified);
    let mirror = destination.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(mirror.loads.len(), 1);
    assert_eq!(mirror.state.len(), 1);
    assert_eq!(
        mirror.loads[0].package_hash,
        head.delta.package_hash.as_str()
    );
    assert_eq!(
        mirror.state[0].package_hash,
        head.delta.package_hash.as_str()
    );
    assert_eq!(mirror.state[0].row_count, head.delta.segments[0].row_count);

    let replay_project = DemoProject::new(temp.path().join("replay-project"))
        .unwrap()
        .with_destination(project.replay_destination_path());
    replay_project.write_files().unwrap();
    let replay_to = format!("duckdb://{}", replay_project.destination_path().display());
    let replay_json = invoke_json(
        replay_project.root(),
        [
            "replay",
            "package",
            &package_dir.display().to_string(),
            "--to",
            &replay_to,
        ],
    );
    assert_eq!(replay_json["command"], "replay package");
    assert_eq!(
        replay_json["result"]["receipt"]["counts"]["rows_written"],
        2
    );
    assert_eq!(replay_json["result"]["checkpoint"]["status"], "committed");
    let replay_rows = duckdb_row_count(&replay_project.destination_path(), TARGET).unwrap();
    assert_eq!(replay_rows, 2);

    let human_replay_project = DemoProject::new(temp.path().join("human-replay-project"))
        .unwrap()
        .with_destination(temp.path().join("human-replay.duckdb"));
    human_replay_project.write_files().unwrap();
    let human_replay_to = format!(
        "duckdb://{}",
        human_replay_project.destination_path().display()
    );
    let replay_human = invoke_human(
        human_replay_project.root(),
        [
            "replay",
            "package",
            &package_dir.display().to_string(),
            "--to",
            &human_replay_to,
        ],
    );

    let duplicate_store =
        SqliteCheckpointStore::open(temp.path().join("duplicate-state.sqlite")).unwrap();
    let before_duplicate = DuckDbDestination::new(replay_project.destination_path())
        .unwrap()
        .read_mirror_snapshot_read_only()
        .unwrap();
    let duplicate = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: crate::destination_catalog::resolve(
            &crate::destination_catalog::local_uri("duckdb", &replay_project.destination_path()),
            replay_project.root(),
            TargetName::new(TARGET).unwrap(),
        )
        .unwrap(),
        checkpoint_store: &duplicate_store,
        after_receipt_verified: None,
    })
    .unwrap();
    let after_duplicate = DuckDbDestination::new(replay_project.destination_path())
        .unwrap()
        .read_mirror_snapshot_read_only()
        .unwrap();
    assert_eq!(before_duplicate, after_duplicate);
    assert_eq!(duplicate.package_status, PackageStatus::Checkpointed);
    assert!(matches!(
        duplicate.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: true,
            package_receipt_recorded: false,
        }
    ));

    let drift = run_duckdb_demo(&temp.path().join("drift")).to_owned();
    assert_eq!(drift.accepted_rows, 1);
    assert_eq!(drift.quarantined_rows, 1);
    assert!(drift.receipt_verified);
    assert!(drift.checkpoint_gated_after_receipt_verification);
    assert_eq!(drift.quarantine_mirror_outcome, "not_mirrored");

    let requests = transport.requests();
    let request = &requests[0];
    let evidence = MvpAcceptanceDemoEvidence {
        cli_plan_before_source_contact: true,
        plan_resource_id: plan_json["result"]["resource_id"]
            .as_str()
            .unwrap()
            .to_owned(),
        plan_partition_count: plan_json["result"]["will_fetch"]["partitions"]
            .as_array()
            .unwrap()
            .len(),
        plan_endpoint_path: "/repos/acme/cdf/issues".to_owned(),
        plan_query_shape: vec!["per_page=100".to_owned(), "state=all".to_owned()],
        contract_freeze_count: freeze_json["result"]["counts"]["frozen"].as_u64().unwrap(),
        contract_test_passed_count: contract_test_json["result"]["counts"]["passed"]
            .as_u64()
            .unwrap(),
        rest_request_count_before_resume: 1,
        rest_request_count_after_resume: transport.requests().len(),
        rest_request_method: format!("{:?}", request.method),
        rest_request_url_redacted: request.url.clone(),
        rest_authorization_header: "Bearer <redacted>".to_owned(),
        crash_boundary: "destination receipt verified before checkpoint commit",
        resume_source_contact: resume_json["result"]["source_contact"].as_bool().unwrap(),
        accepted_issue_rows: accepted.len() as i64,
        accepted_issue_titles: accepted.into_iter().map(|(_, title)| title).collect(),
        cdf_sql_package_rows: sql_json["result"]["rows"].as_array().unwrap().len(),
        cdf_sql_surface_boundary: "cdf sql proves local system history; DuckDB target rows are queried through local DuckDB SQL",
        checkpoint_history_statuses: history
            .iter()
            .map(|checkpoint| checkpoint["status"].as_str().unwrap().to_owned())
            .collect(),
        committed_state_backed_by_duckdb_load: mirror.loads[0].package_hash
            == head.delta.package_hash.as_str(),
        committed_state_backed_by_duckdb_state: mirror.state[0].package_hash
            == head.delta.package_hash.as_str(),
        replay_rows_in_second_duckdb: replay_rows,
        duplicate_replay_noop: true,
        duplicate_replay_receipt_source: "destination_commit_duplicate".to_owned(),
        drift,
        lower_api_boundaries: vec![
            "run_project supplies deterministic REST transport and crash hook; CLI run uses live reqwest transport",
            "duplicate replay uses a fresh checkpoint store to isolate destination idempotency from checkpoint-id reuse",
        ],
    };
    assert_eq!(evidence.rest_request_method, "Get");
    assert!(
        request
            .url
            .starts_with("https://api.github.test/repos/acme/cdf/issues?")
    );
    assert!(request.url.contains("per_page=100"));
    assert!(request.url.contains("state=all"));
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some(format!("Bearer {SECRET_VALUE}").as_str())
    );

    let rendered = serde_json::to_string_pretty(&evidence).unwrap();
    assert!(!rendered.contains(SECRET_VALUE));
    assert!(!rendered.contains(temp.path().to_str().unwrap()));
    assert!(rendered.contains("Bearer <redacted>"));

    let transcript = format!(
        "$ cdf plan {RESOURCE_ID}\n{plan_human}\n\
         $ cdf contract test {RESOURCE_ID}\n{contract_human}\n\
         # simulated kill after destination receipt verification and before checkpoint commit\n\
         $ cdf resume {RUN_ID}\n{}\n\
         $ cdf sql 'select package_id, status from packages order by package_id'\n{sql_human}\n\
         $ cdf state history {RESOURCE_ID} --pipeline {PIPELINE_ID}\n{history_human}\n\
         $ cdf replay package <package> --to duckdb://<replay>\n{replay_human}\n\
         # duplicate replay: true; destination footprint unchanged\n\
         # drift verdict: accepted_rows=1 quarantined_rows=1 receipt_verified=true checkpoint_gated=true\n",
        serde_json::to_string_pretty(&resume_json).unwrap()
    )
    .replace(temp.path().to_str().unwrap(), "<project>");
    assert!(!transcript.contains(SECRET_VALUE));
    assert!(!transcript.contains(temp.path().to_str().unwrap()));
    if let Ok(path) = std::env::var("CDF_DEMO_TRANSCRIPT_OUTPUT") {
        std::fs::write(path, transcript).unwrap();
    }
}

#[derive(Clone, Debug)]
struct DemoProject {
    root: PathBuf,
    destination_path: PathBuf,
}

impl DemoProject {
    fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        Ok(Self {
            destination_path: root.join(".cdf/demo.duckdb"),
            root,
        })
    }

    fn with_destination(mut self, destination_path: impl AsRef<Path>) -> Self {
        self.destination_path = destination_path.as_ref().to_path_buf();
        self
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn package_root(&self) -> PathBuf {
        self.root.join(".cdf/packages")
    }

    fn state_store_path(&self) -> PathBuf {
        self.root.join(".cdf/state.sqlite")
    }

    fn destination_path(&self) -> PathBuf {
        self.destination_path.clone()
    }

    fn replay_destination_path(&self) -> PathBuf {
        self.root.join(".cdf/replay.duckdb")
    }

    fn write_files(&self) -> Result<()> {
        fs::create_dir_all(self.root.join("resources"))
            .map_err(|error| CdfError::data(format!("create demo resources dir: {error}")))?;
        fs::create_dir_all(self.root.join(".cdf"))
            .map_err(|error| CdfError::data(format!("create demo .cdf dir: {error}")))?;
        fs::write(self.root.join("github-token"), SECRET_VALUE)
            .map_err(|error| CdfError::data(format!("write demo secret file: {error}")))?;
        fs::write(self.root.join("cdf.toml"), self.project_toml())
            .map_err(|error| CdfError::data(format!("write demo cdf.toml: {error}")))?;
        fs::write(self.root.join("resources/github.toml"), GITHUB_ISSUES_TOML)
            .map_err(|error| CdfError::data(format!("write demo resource TOML: {error}")))
    }

    fn project_toml(&self) -> String {
        format!(
            r#"
[project]
name = "{PROJECT_NAME}"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.sqlite"
packages = ".cdf/packages"
destination = "duckdb://{}"

[resources."{RESOURCE_ID}"]
source = "resources/github.toml"
"#,
            self.destination_path.display()
        )
    }
}

fn github_issues_resource() -> Result<(
    crate::source_fixture::ResolvedSourceFixture,
    RecordingTransport,
)> {
    let transport = RecordingTransport::new([json_response(GITHUB_ISSUES_RESPONSE)]);
    let registry = crate::test_rest_source_registry(transport.clone())?;
    let document = cdf_declarative::parse_toml(GITHUB_ISSUES_TOML)?;
    let mut resources = cdf_declarative::compile_document(&registry, &document)?;
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "MVP acceptance proof expected one GitHub issues resource, found {}",
            resources.len()
        )));
    }
    let compiled = resources.remove(0);
    if compiled.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "MVP acceptance proof compiled unexpected resource {}",
            compiled.descriptor().resource_id
        )));
    }
    let execution = crate::test_execution_services();
    let context = cdf_runtime::SourceResolutionContext::new(
        std::path::Path::new("."),
        std::sync::Arc::new(StaticSecretProvider::new([(SECRET_REF, SECRET_VALUE)])),
        &execution,
        std::sync::Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let resource =
        crate::source_fixture::ResolvedSourceFixture::resolve(&compiled, &registry, &context)?;
    Ok((resource, transport))
}

fn engine_plan(
    resource: &dyn QueryableResource,
    package_id: &str,
    destination: &ResolvedProjectDestination,
) -> Result<EnginePlan> {
    let observed_schema = cdf_contract::ObservedSchema::from_arrow(resource.schema().as_ref());
    let mut policy =
        cdf_contract::ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    if let Some(identifier_policy) = destination.column_identifier_policy()? {
        policy.normalization.identifier = identifier_policy;
    }
    let validation_program = cdf_contract::compile_validation_program(&policy, &observed_schema)?;
    Planner::new().plan_tier_b(
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
}

fn assert_plan_matches_github_issues(plan: &EnginePlan) {
    assert_eq!(plan.package_id, PACKAGE_ID);
    assert_eq!(plan.scan.request.resource_id.as_str(), RESOURCE_ID);
    assert_eq!(plan.scan.request.scope, ScopeKey::Resource);
    assert_eq!(plan.scan.partitions.len(), 1);
    let metadata = &plan.scan.partitions[0].metadata;
    assert_eq!(metadata.get("kind").map(String::as_str), Some("rest"));
    assert_eq!(
        metadata.get("path").map(String::as_str),
        Some("/repos/acme/cdf/issues")
    );
}

fn assert_no_checkpoint_head(
    state_store_path: &Path,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
) -> Result<()> {
    let store = SqliteCheckpointStore::open(state_store_path)?;
    if store.head(pipeline_id, resource_id, scope)?.is_some() {
        return Err(CdfError::contract(
            "checkpoint head advanced before crash hook",
        ));
    }
    Ok(())
}

fn assert_checkpoint_position(position: &SourcePosition) {
    let SourcePosition::Cursor(cursor) = position else {
        panic!("GitHub issues REST run must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, 1);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(1_783_505_700_000_000));
}

fn read_accepted_issues(database_path: &Path) -> Result<Vec<(i64, String)>> {
    let conn = DuckConnection::open(database_path)
        .map_err(|error| CdfError::destination(format!("open demo DuckDB: {error}")))?;
    let mut stmt = conn
        .prepare(&format!(
            "SELECT number, title FROM {TARGET} ORDER BY number"
        ))
        .map_err(|error| CdfError::destination(format!("prepare demo issue query: {error}")))?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|error| CdfError::destination(format!("query demo issue rows: {error}")))?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| CdfError::destination(format!("read demo issue row: {error}")))
}

fn duckdb_row_count(database_path: &Path, target: &str) -> Result<i64> {
    let conn = DuckConnection::open(database_path)
        .map_err(|error| CdfError::destination(format!("open replay DuckDB: {error}")))?;
    conn.query_row(&format!("SELECT count(*) FROM {target}"), [], |row| {
        row.get(0)
    })
    .map_err(|error| CdfError::destination(format!("count replay DuckDB rows: {error}")))
}

fn invoke_json<'a, I>(project_root: &Path, args: I) -> Value
where
    I: IntoIterator<Item = &'a str>,
{
    let mut argv = vec![
        OsString::from("cdf"),
        OsString::from("--json"),
        OsString::from("--project"),
        project_root.as_os_str().to_os_string(),
    ];
    argv.extend(args.into_iter().map(OsString::from));
    let result = cdf_cli::invoke(argv);
    assert_eq!(
        result.exit_code, 0,
        "stdout:\n{}\nstderr:\n{}",
        result.stdout, result.stderr
    );
    serde_json::from_str(&result.stdout).unwrap()
}

fn invoke_human<'a, I>(project_root: &Path, args: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let mut argv = vec![
        OsString::from("cdf"),
        OsString::from("--project"),
        project_root.as_os_str().to_os_string(),
    ];
    argv.extend(args.into_iter().map(OsString::from));
    let result = cdf_cli::invoke(argv);
    assert_eq!(
        result.exit_code, 0,
        "stdout:\n{}\nstderr:\n{}",
        result.stdout, result.stderr
    );
    format!("{}{}", result.stderr, result.stdout)
}

const GITHUB_ISSUES_TOML: &str = r#"
[source.github]
kind = "rest"
base_url = "https://api.github.test"
auth = { kind = "bearer", token = "secret://file/github-token" }
egress_allowlist = ["api.github.test"]

[resource.issues]
path = "/repos/acme/cdf/issues"
params = { state = "all", per_page = 100 }
records = "$"
primary_key = ["id"]
merge_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "number", type = "int64", nullable = false },
  { name = "title", type = "string", nullable = false },
  { name = "state", type = "string", nullable = false },
  { name = "updated_at", type = "int64", nullable = false },
  { name = "html_url", type = "string", nullable = false },
  { name = "user_login", type = "string", nullable = false },
] }
"#;

const GITHUB_ISSUES_RESPONSE: &str = r#"[
  {
    "id": 9001,
    "number": 101,
    "title": "Fix flaky package replay",
    "state": "open",
    "updated_at": 1783504800000000,
    "html_url": "https://github.example/acme/cdf/issues/101",
    "user_login": "ada"
  },
  {
    "id": 9002,
    "number": 102,
    "title": "Document drift quarantine",
    "state": "open",
    "updated_at": 1783505700000000,
    "html_url": "https://github.example/acme/cdf/issues/102",
    "user_login": "grace"
  }
]"#;
