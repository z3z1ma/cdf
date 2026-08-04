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
    CDF_VARIANT_SEMANTIC, RESIDUAL_ENCODING_METADATA_KEY, RESIDUAL_ENCODING_NAME,
    VARIANT_COLUMN_NAME,
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
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::context;
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

struct SystemSqlFixture {
    package_hash: String,
}

#[derive(Clone, Copy)]
enum DoctorDriftFixtureMode {
    Clean,
    StatePositionDrift,
    TargetDrift,
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

fn write_protobuf_resource(project: &TestProject) {
    for entry in fs::read_dir(project.root.join("data")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.rows]
glob = "rows.pb"
format = "protobuf"
write_disposition = "append"
trust = "governed"
format_options = { descriptor_set_base64 = "CkQKCWRldi9zdGRpbhIEdGVzdCIpCgNSb3cSDgoCaWQYASABKANSAmlkEhIKBG5hbWUYAiABKAlSBG5hbWViBnByb3RvMw==", message = "test.Row", framing = "length_delimited" }
"#,
    )
    .unwrap();
    fs::write(
        project.root.join("data/rows.pb"),
        [
            0x09, 0x08, 0x2a, 0x12, 0x05, b'a', b'l', b'i', b'c', b'e', 0x07, 0x08, 0x07, 0x12,
            0x03, b'b', b'o', b'b',
        ],
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
        &cdf_contract::CDF_VARIANT_SEMANTIC.parse().unwrap(),
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
                segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
                package_id: "cli-current-fixture-package".to_owned(),
                committed_frontier: None,
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
            Some("secret://file/missing-postgres-dsn"),
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
        resources.push_str("\n[resources.\"warehouse.*\"]\nsource = \"resources/postgres.toml\"\n");
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
            project.root.join("resources/postgres.toml"),
            postgres_resource(connection),
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

fn postgres_resource(connection: &str) -> String {
    format!(
        r#"
[source.warehouse]
kind = "postgres"
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

fn postgres_discover_resource(connection: &str, table: &str) -> String {
    format!(
        r#"
[source.warehouse]
kind = "postgres"
connection = "{connection}"
dialect = "postgres"

[resource.orders]
table = "{table}"
write_disposition = "append"
trust = "governed"
"#
    )
}

fn postgres_discover_resource_with_vendor_cursor(connection: &str, table: &str) -> String {
    format!(
        r#"
[source.warehouse]
kind = "postgres"
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

fn postgres_resource_with_ordered_cursor(connection: &str, table: &str) -> String {
    format!(
        r#"
[source.warehouse]
kind = "postgres"
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

fn write_postgres_project_with_secret(
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
    fs::write(project.root.join("postgres-dsn"), format!("{source_dsn}\n")).unwrap();
    write_secret_project(
        project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/postgres-dsn"),
    );
    fs::write(
        project.root.join("resources/postgres.toml"),
        postgres_resource_with_ordered_cursor("secret://file/postgres-dsn", table),
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

fn render_verbose_rich(
    output: cdf_cli_core::output::CommandOutput,
) -> cdf_cli_core::output::InvocationResult {
    let config = cdf_cli_core::render::RenderConfig::new(
        cdf_cli_core::render::config::DisplayMode::Tty,
        96,
        cdf_cli_core::render::config::RenderEnv {
            no_color: false,
            clicolor_force: false,
            unicode_supported: true,
        },
        cdf_cli_core::terminal::TerminalPolicy {
            verbosity: cdf_cli_core::terminal::Verbosity::Verbose(1),
            ..cdf_cli_core::terminal::TerminalPolicy::default()
        },
    );
    cdf_cli_core::output::InvocationResult::from_output(false, &config, output)
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
        &CDF_VARIANT_SEMANTIC.parse().unwrap(),
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

fn write_python_bootstrap_project(project: &TestProject, interpreter: &Path, marker: &Path) {
    fs::create_dir_all(project.root.join("src")).unwrap();
    fs::write(
        project.root.join("cdf.toml"),
        format!(
            r#"
[project]
name = "python_bootstrap"
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
raw_events.__cdf_schema__ = ()
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

mod add;
mod contract;
mod doctor;
mod doctor_drift;
mod errors;
mod init_validate;
mod inspect;
mod package;
mod planning;
mod preview;
mod python;
mod recovery;
mod replay;
mod run;
mod run_adapters;
mod schema_discovery;
mod schema_promotion;
mod source_planning;
mod sql;
mod state;
mod status;
mod surface;
