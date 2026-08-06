use cdf_dest_duckdb::DuckDbDestination;
use cdf_kernel::{
    CheckpointStore, CursorValue, PipelineId, ResourceId, Result, ScopeKey, SourcePosition,
    WriteDisposition, source_name,
};
use cdf_package::PackageReader;
use cdf_state_sqlite::SqliteCheckpointStore;
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    ffi::OsString,
    fs,
    io::{ErrorKind, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use super::{
    MatrixDestination, MatrixDisposition, RunMatrixCell, SourceArchetype, core,
    destinations::ConformanceEnvironment, file_fixture, plan_json, source_catalog,
};

#[derive(Debug, PartialEq, Eq)]
struct PreviewFingerprint {
    source: SourceArchetype,
    row_count: u64,
    field_names: Vec<String>,
    partition_count: usize,
}

fn planned_partitions(
    resource: &dyn cdf_kernel::QueryableResource,
    scan: &cdf_kernel::ScanPlan,
) -> Result<Vec<cdf_kernel::PartitionPlan>> {
    if let Some(partitions) = scan.inline_partitions() {
        return Ok(partitions.to_vec());
    }
    let reference = scan.external_task_set().ok_or_else(|| {
        cdf_kernel::CdfError::internal("scan omitted canonical partition authority")
    })?;
    let mut reader = resource.planned_partition_reader(reference)?;
    let mut partitions = Vec::with_capacity(usize::try_from(reference.task_count).unwrap_or(0));
    for ordinal in 0..reference.task_count {
        partitions.push(
            reader
                .next_partition(ordinal)?
                .ok_or_else(|| {
                    cdf_kernel::CdfError::data(format!(
                        "external partition authority ended before ordinal {ordinal}"
                    ))
                })?
                .plan()
                .clone(),
        );
    }
    Ok(partitions)
}

#[test]
fn rest_compile_preview_run_package_checkpoint_conformance() {
    const SECRET: &str = "recorded-rest-secret";
    const BODY: &str = r#"{ "items": [
        { "VendorID": 1, "updated_at": 10 },
        { "VendorID": 2, "updated_at": 20 }
    ] }"#;

    let server = RecordedHttpServer::new([BODY, BODY, BODY, BODY]);
    let temp = tempfile::tempdir().unwrap();
    write_s5_project(temp.path(), server.base_url(), SECRET);

    let first_compile = invoke_success_json(temp.path(), &["compile", "api.items"], Some(SECRET));
    let first = &first_compile["result"];
    assert_eq!(first["counts"]["compiled"], 1);
    assert_eq!(first["resources"][0]["resource_id"], "api.items");
    assert_eq!(first["resources"][0]["discovered_schema"], true);

    let lock = cdf_project::parse_lock(&fs::read_to_string(temp.path().join("cdf.lock")).unwrap())
        .unwrap();
    let reference = lock.resources["api.items"]
        .schema_snapshot
        .as_ref()
        .unwrap();
    let locked_snapshot_hash = reference.schema_hash.to_string();
    let snapshot_path = reference.path.clone();
    let snapshot_bytes = fs::read(temp.path().join(&snapshot_path)).unwrap();
    let lock_bytes = fs::read(temp.path().join("cdf.lock")).unwrap();
    let snapshot: Value = serde_json::from_slice(&snapshot_bytes).unwrap();
    assert_eq!(snapshot["metadata"]["probe"], "registered-source-discovery");
    assert_eq!(snapshot["metadata"]["source_driver"], "rest");
    let vendor = snapshot["schema"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|field| field["name"] == "vendor_id")
        .expect("normalized VendorID snapshot field");
    assert_eq!(vendor["metadata"]["cdf:source_name"], "VendorID");

    let second_compile = invoke_success_json(temp.path(), &["compile", "api.items"], Some(SECRET));
    let second = &second_compile["result"];
    assert_eq!(second["counts"]["compiled"], 1);
    assert_eq!(second["resources"][0]["discovered_schema"], false);
    assert_eq!(
        fs::read(temp.path().join(&snapshot_path)).unwrap(),
        snapshot_bytes
    );
    assert_eq!(fs::read(temp.path().join("cdf.lock")).unwrap(), lock_bytes);

    let before_preview = project_tree_snapshot(temp.path());
    let preview = invoke_success_json(temp.path(), &["preview", "api.items"], Some(SECRET));
    assert_eq!(preview["result"]["resource"], "api.items");
    assert_eq!(preview["result"]["partition"], "rest");
    assert_eq!(preview["result"]["row_count"], 2);
    assert_eq!(project_tree_snapshot(temp.path()), before_preview);

    let run = invoke_success_json(temp.path(), &["run", "api.items"], Some(SECRET));
    let report = &run["result"]["resources"][0]["result"];
    assert_eq!(report["resource_id"], "api.items");
    assert_eq!(report["schema_hash"], locked_snapshot_hash);
    assert_eq!(report["schema_snapshot"]["outcome"], "unchanged");
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["checkpoint"]["status"], "committed");
    let package_id = report["package_id"].as_str().unwrap();
    let checkpoint_id = report["checkpoint_id"].as_str().unwrap();

    let package_dir = temp.path().join(".cdf/packages").join(package_id);
    let reader = PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    let mut receipts = Vec::new();
    reader
        .for_each_receipt(&mut |receipt| {
            receipts.push(receipt);
            Ok(())
        })
        .unwrap();
    assert_eq!(receipts.len(), 1);
    let receipt = &receipts[0];
    assert_eq!(receipt.schema_hash.as_str(), locked_snapshot_hash);
    assert_eq!(receipt.disposition, WriteDisposition::Append);
    assert_eq!(receipt.counts.rows_written, 2);

    let destination = DuckDbDestination::new(temp.path().join(".cdf/s5.duckdb")).unwrap();
    assert!(destination.verify_receipt(receipt).unwrap().verified);

    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
    );
    let mut segments = reader
        .verified_segment_stream(memory, 64 * 1024 * 1024)
        .unwrap();
    let segment = segments.next().unwrap().unwrap();
    let output_schema = segment.batches[0].schema();
    let vendor = output_schema.field_with_name("vendor_id").unwrap();
    assert_eq!(source_name(vendor), Some("VendorID"));

    let store = SqliteCheckpointStore::open(temp.path().join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("api.items").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed REST checkpoint head");
    assert_eq!(head.delta.checkpoint_id.as_str(), checkpoint_id);
    assert_eq!(head.delta.schema_hash.as_str(), locked_snapshot_hash);
    assert!(receipt.covers_state_delta(&head.delta));
    let SourcePosition::Cursor(cursor) = &head.delta.output_position else {
        panic!("REST checkpoint must carry the declared cursor");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));

    assert_generated_artifacts_do_not_contain(temp.path(), SECRET);
    let requests = server.requests().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|request| {
        request.contains("GET /items HTTP/1.1")
            && request.contains(&format!("authorization: Bearer {SECRET}"))
    }));
}

#[test]
fn keyless_append_runs_without_key_remediation() {
    let append = tempfile::tempdir().unwrap();
    write_s7_append_project(append.path());
    let validate = invoke_cli(append.path(), &["validate"]);
    assert_success_without_key_nudge(&validate);
    let plan = invoke_cli(append.path(), &["plan", "local.events"]);
    assert_success_without_key_nudge(&plan);
    let plan_json = success_json(&plan);
    assert_eq!(
        plan_json["result"]["resources"][0]["report"]["destination"]["disposition"],
        "append"
    );
    let preview = invoke_cli(append.path(), &["preview", "local.events"]);
    assert_success_without_key_nudge(&preview);
    assert_eq!(success_json(&preview)["result"]["row_count"], 2);
    let run = invoke_cli(append.path(), &["run", "local.events"]);
    assert_success_without_key_nudge(&run);
    let run_json = success_json(&run);
    let run_report = &run_json["result"]["resources"][0]["result"];
    assert_eq!(run_report["receipt"]["disposition"], "append");
    assert_eq!(run_report["row_count"], 2);
}

#[test]
fn preview_run_parity_covers_supported_archetypes() {
    let environment = ConformanceEnvironment::start().expect(
        "preview/run parity conformance requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let cases = source_catalog::archetypes()
        .into_iter()
        .filter(|source| match source.as_str() {
            "clickhouse" => std::env::var_os("CDF_CLICKHOUSE_ENDPOINT").is_some(),
            "mongodb" => std::env::var_os("CDF_MONGODB_ENDPOINT").is_some(),
            _ => true,
        })
        .map(|source| {
            RunMatrixCell::new(
                source,
                MatrixDestination::new("duckdb").unwrap(),
                MatrixDisposition::Append,
            )
        });

    for cell in cases {
        let preview = preview_fingerprint(cell.clone(), &environment).unwrap_or_else(|error| {
            panic!(
                "{} preview failed before parity comparison: {error}",
                cell.source_archetype.as_str()
            )
        });
        let executed = core::execute_cell(cell.clone(), &environment).unwrap_or_else(|error| {
            panic!(
                "{} run failed before parity comparison: {error}",
                cell.source_archetype.as_str()
            )
        });

        assert_eq!(preview.source, cell.source_archetype);
        assert_eq!(
            preview.row_count,
            executed.row_count,
            "{} preview row count must match package-producing run",
            cell.source_archetype.as_str()
        );
        assert_eq!(preview.row_count, core::ROW_COUNT);
        assert_eq!(
            u64::try_from(preview.partition_count).unwrap(),
            core::SEGMENT_COUNT
        );
        assert!(
            preview.field_names.iter().any(|name| name == "id"),
            "{} preview schema should expose the id column consumed by run",
            cell.source_archetype.as_str()
        );
    }
}

#[test]
fn multifile_preview_traverses_the_same_planned_partitions_as_run() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = file_fixture::multi_resource(temp.path(), MatrixDisposition::Append).unwrap();
    let resource = crate::source_fixture::resolve_local_file(&compiled, temp.path()).unwrap();
    let plan = plan_json::file_engine_plan(
        resource.queryable(),
        "p2-s8-multifile-preview-run",
        MatrixDisposition::Append,
        None,
    )
    .unwrap();
    let plan = resource.bind_plan(plan).unwrap();
    let partitions = planned_partitions(resource.queryable(), &plan.scan).unwrap();
    assert_eq!(partitions.len(), 2);
    assert!(
        partitions[0].planned_file().unwrap().unwrap().path
            < partitions[1].planned_file().unwrap().unwrap().path
    );
    let before_preview = project_tree_snapshot(temp.path());

    let preview = futures_executor::block_on(cdf_engine::preview_resource(
        &plan,
        resource.queryable(),
        cdf_engine::EnginePreviewLimits::default(),
    ))
    .unwrap();

    assert_eq!(project_tree_snapshot(temp.path()), before_preview);
    assert_eq!(preview.planned_partition_count, 2);
    assert_eq!(preview.payload_eligible_partition_count, 2);
    assert_eq!(preview.selected_partition_count, 2);
    assert_eq!(preview.payload_opened_partition_count, 2);
    assert_eq!(preview.attested_partition_count, 0);
    assert_eq!(preview.inspected_partition_count, 2);
    assert_eq!(preview.inspected_batch_count, 2);
    assert_eq!(preview.partially_inspected_partition_count, 0);
    assert_eq!(preview.payload_uninspected_partition_count, 0);
    assert_eq!(preview.row_count, 2);
    assert_eq!(
        preview.selection.policy,
        cdf_engine::PREVIEW_POLICY_BALANCED_STRATIFIED_V1
    );
    assert_eq!(
        preview.selection.selector,
        cdf_kernel::STRATIFIED_HASH_SELECTOR_V1
    );
    assert!(preview.fields.iter().any(|field| field == "id"));

    let package = temp.path().join("package");
    let run = futures_executor::block_on(cdf_engine::execute_to_package(
        &plan,
        resource.queryable(),
        &package,
    ))
    .unwrap();
    assert_eq!(run.profile.output_rows, preview.row_count);
    assert_eq!(run.profile.output_batches, preview.inspected_batch_count);
    cdf_package::PackageReader::open(package)
        .unwrap()
        .verify()
        .unwrap();
}

fn preview_fingerprint(
    cell: RunMatrixCell,
    environment: &ConformanceEnvironment,
) -> Result<PreviewFingerprint> {
    let temp = tempfile::tempdir()
        .map_err(|error| crate::conformance_host_error("create parity preview tempdir", error))?;
    let package_id = format!(
        "p2-preview-parity-{}-{}",
        cell.source_archetype.as_str(),
        cell.disposition.as_str()
    );

    let source = source_catalog::prepare(&cell, temp.path(), environment)?;
    let plan = source.engine_plan(&package_id, cell.disposition, None)?;
    let partitions = planned_partitions(source.queryable(), &plan.scan)?;
    let preview = futures_executor::block_on(cdf_engine::preview_resource(
        &plan,
        source.queryable(),
        cdf_engine::EnginePreviewLimits::default(),
    ))?;
    let partition_count = partitions.len();

    Ok(PreviewFingerprint {
        source: cell.source_archetype,
        row_count: preview.row_count,
        field_names: preview.fields,
        partition_count,
    })
}

fn write_s5_project(root: &Path, base_url: &str, secret: &str) {
    fs::create_dir_all(root.join("cdf/api")).unwrap();
    fs::create_dir_all(root.join(".cdf")).unwrap();
    fs::write(root.join("rest-token"), format!("{secret}\n")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        format!(
            r#"
[project]
name = "rest_discovery_conformance"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/s5.duckdb"

[sources.api]
type = "rest"
base_url = "{base_url}"
auth = {{ kind = "bearer", token = "secret://file/rest-token" }}
egress_allowlist = ["127.0.0.1"]
"#
        ),
    )
    .unwrap();
    fs::write(
        root.join("cdf/api/items.cdf.sql"),
        r#"RESOURCE
DISPOSITION APPEND
CURSOR updated_at
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT *
FROM upstream(
  source => 'api',
  path => '/items',
  records => '$.items',
  cursor_param => 'since',
  cursor_filter_fidelity => 'exact'
);
"#,
    )
    .unwrap();
}

fn write_s7_append_project(root: &Path) {
    fs::create_dir_all(root.join("cdf/local")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::create_dir_all(root.join(".cdf")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "keyless_append_conformance"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/s7.duckdb"

[sources.local]
type = "files"
root = "data"
"#,
    )
    .unwrap();
    fs::write(
        root.join("cdf/local/events.cdf.sql"),
        r#"RESOURCE
DISPOSITION APPEND
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT *
FROM upstream(source => 'local', glob => 'events.ndjson', format => 'ndjson');
"#,
    )
    .unwrap();
    fs::write(
        root.join("data/events.ndjson"),
        "{\"id\":1,\"updated_at\":10}\n{\"id\":2,\"updated_at\":20}\n",
    )
    .unwrap();
}

fn invoke_cli(root: &Path, args: &[&str]) -> cdf_cli_core::output::InvocationResult {
    let mut argv = vec![
        OsString::from("cdf"),
        OsString::from("--json"),
        OsString::from("--project"),
        root.as_os_str().to_os_string(),
    ];
    argv.extend(args.iter().map(|arg| OsString::from(*arg)));
    cdf_cli::invoke(argv)
}

fn invoke_success_json(root: &Path, args: &[&str], secret: Option<&str>) -> Value {
    let result = invoke_cli(root, args);
    assert_eq!(
        result.exit_code, 0,
        "stdout:\n{}\nstderr:\n{}",
        result.stdout, result.stderr
    );
    if let Some(secret) = secret {
        assert!(!result.stdout.contains(secret));
        assert!(!result.stderr.contains(secret));
    }
    success_json(&result)
}

fn success_json(result: &cdf_cli_core::output::InvocationResult) -> Value {
    serde_json::from_str(&result.stdout).unwrap()
}

fn assert_success_without_key_nudge(result: &cdf_cli_core::output::InvocationResult) {
    assert_eq!(
        result.exit_code, 0,
        "stdout:\n{}\nstderr:\n{}",
        result.stdout, result.stderr
    );
    let output = format!("{}{}", result.stdout, result.stderr).to_ascii_lowercase();
    for forbidden in [
        "primary_key",
        "merge_key",
        "primary key",
        "merge key",
        "composite key",
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

fn assert_generated_artifacts_do_not_contain(root: &Path, secret: &str) {
    for (path, bytes) in project_tree_snapshot(root) {
        if path == "cdf.lock" || path.starts_with(".cdf/") {
            assert!(
                !bytes
                    .windows(secret.len())
                    .any(|window| window == secret.as_bytes()),
                "generated artifact {path} leaked the source secret"
            );
        }
    }
}

fn project_tree_snapshot(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn visit(root: &Path, directory: &Path, entries: &mut BTreeMap<String, Vec<u8>>) {
        let mut paths = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        paths.sort();
        for path in paths {
            let relative = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            if path.is_dir() {
                entries.insert(format!("{relative}/"), Vec::new());
                visit(root, &path, entries);
            } else {
                entries.insert(relative, fs::read(path).unwrap());
            }
        }
    }

    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

struct RecordedHttpServer {
    base_url: String,
    requests: Arc<Mutex<Vec<String>>>,
    failure: Arc<Mutex<Option<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

const RECORDED_HTTP_HEADER_CAP: usize = 8192;
const RECORDED_HTTP_HEADER_DEADLINE: Duration = Duration::from_secs(1);
const RECORDED_HTTP_RESPONSE_DEADLINE: Duration = Duration::from_secs(1);

fn read_recorded_http_header(
    stream: &mut impl Read,
    deadline: Duration,
) -> std::io::Result<Vec<u8>> {
    let started = Instant::now();
    let mut request = Vec::new();

    loop {
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(request);
        }
        if request.len() == RECORDED_HTTP_HEADER_CAP {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "recorded HTTP fixture request header exceeded the {RECORDED_HTTP_HEADER_CAP}-byte cap before the header terminator"
                ),
            ));
        }
        if started.elapsed() >= deadline {
            return Err(std::io::Error::new(
                ErrorKind::TimedOut,
                format!(
                    "recorded HTTP fixture request header remained incomplete after {} ms",
                    deadline.as_millis()
                ),
            ));
        }

        let remaining = RECORDED_HTTP_HEADER_CAP - request.len();
        let mut chunk = [0_u8; 1024];
        let read_len = remaining.min(chunk.len());
        match stream.read(&mut chunk[..read_len]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    format!(
                        "recorded HTTP fixture request header ended after {} bytes before the header terminator",
                        request.len()
                    ),
                ));
            }
            Ok(bytes_read) => request.extend_from_slice(&chunk[..bytes_read]),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn run_recorded_http_server(
    listener: TcpListener,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    mut bodies: VecDeque<String>,
) -> std::result::Result<(), String> {
    while !stop.load(Ordering::Relaxed) && !bodies.is_empty() {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream.set_nonblocking(true).map_err(|error| {
                    format!(
                        "recorded HTTP fixture could not make accepted socket nonblocking: {error}"
                    )
                })?;
                let request = read_recorded_http_header(&mut stream, RECORDED_HTTP_HEADER_DEADLINE)
                    .map_err(|error| {
                        format!("recorded HTTP fixture request capture failed: {error}")
                    })?;
                requests
                    .lock()
                    .map_err(|_| "recorded HTTP fixture request log was poisoned".to_owned())?
                    .push(String::from_utf8_lossy(&request).into_owned());
                let body = bodies.pop_front().ok_or_else(|| {
                    "recorded HTTP fixture accepted more requests than configured responses"
                        .to_owned()
                })?;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );

                stream.set_nonblocking(false).map_err(|error| {
                    format!(
                        "recorded HTTP fixture could not restore blocking response I/O: {error}"
                    )
                })?;
                stream
                    .set_write_timeout(Some(RECORDED_HTTP_RESPONSE_DEADLINE))
                    .map_err(|error| {
                        format!("recorded HTTP fixture could not bound response writes: {error}")
                    })?;
                stream.write_all(response.as_bytes()).map_err(|error| {
                    format!("recorded HTTP fixture response write failed: {error}")
                })?;
                stream.flush().map_err(|error| {
                    format!("recorded HTTP fixture response flush failed: {error}")
                })?;
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => {
                return Err(format!("recorded HTTP fixture accept failed: {error}"));
            }
        }
    }
    Ok(())
}

fn store_recorded_http_failure(failure: &Arc<Mutex<Option<String>>>, message: String) {
    let mut failure = failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if failure.is_none() {
        *failure = Some(message);
    }
}

#[test]
fn recorded_http_server_waits_for_split_request_headers() {
    let server = RecordedHttpServer::new([r#"{"items":[]}"#]);
    let address = server.base_url().strip_prefix("http://").unwrap();
    let mut client = TcpStream::connect(address).unwrap();

    client
        .write_all(b"GET /items HTTP/1.1\r\nhost: local\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(20));
    client
        .write_all(b"authorization: Bearer split-secret\r\n\r\n")
        .unwrap();

    let mut response = String::new();
    client.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    let requests = server.requests().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("authorization: Bearer split-secret"));
}

#[test]
fn recorded_http_server_restores_blocking_response_writes() {
    let body = "x".repeat(4 * 1024 * 1024);
    let server = RecordedHttpServer::new([body.clone()]);
    let address = server.base_url().strip_prefix("http://").unwrap();
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
        .write_all(b"GET /items HTTP/1.1\r\nhost: local\r\n\r\n")
        .unwrap();

    thread::sleep(Duration::from_millis(20));
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    let body_start = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| offset + 4)
        .expect("recorded HTTP response header terminator");
    assert_eq!(&response[body_start..], body.as_bytes());
    assert_eq!(server.requests().unwrap().len(), 1);
}

#[test]
fn recorded_http_server_surfaces_capture_failure_without_drop_panic() {
    let server = RecordedHttpServer::new([r#"{"items":[]}"#]);
    let address = server.base_url().strip_prefix("http://").unwrap();
    let mut client = TcpStream::connect(address).unwrap();
    client
        .write_all(b"GET /items HTTP/1.1\r\nhost: local\r\n")
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let failure = loop {
        match server.requests() {
            Err(failure) => break failure,
            Ok(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
            Ok(_) => panic!("recorded HTTP fixture did not surface incomplete header failure"),
        }
    };
    assert!(failure.contains("request capture failed"));
    assert!(failure.contains("before the header terminator"));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(server))).is_ok());
}

#[test]
fn recorded_http_header_capture_retries_would_block_and_bounds_incomplete_requests() {
    struct BecomesReady {
        would_block: bool,
        bytes: std::io::Cursor<Vec<u8>>,
    }
    impl Read for BecomesReady {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.would_block {
                self.would_block = false;
                return Err(std::io::Error::from(ErrorKind::WouldBlock));
            }
            self.bytes.read(buffer)
        }
    }

    let complete = b"GET /items HTTP/1.1\r\nauthorization: Bearer delayed\r\n\r\n";
    let mut becomes_ready = BecomesReady {
        would_block: true,
        bytes: std::io::Cursor::new(complete.to_vec()),
    };
    assert_eq!(
        read_recorded_http_header(&mut becomes_ready, Duration::from_millis(50)).unwrap(),
        complete
    );

    let mut eof = std::io::Cursor::new(b"GET /items HTTP/1.1\r\nhost: local\r\n".as_slice());
    let error = read_recorded_http_header(&mut eof, Duration::from_millis(50)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    assert!(error.to_string().contains("before the header terminator"));

    let mut oversized = std::io::Cursor::new(vec![b'x'; RECORDED_HTTP_HEADER_CAP]);
    let error = read_recorded_http_header(&mut oversized, Duration::from_millis(50)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("8192-byte cap"));

    struct NeverReady;
    impl Read for NeverReady {
        fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from(ErrorKind::WouldBlock))
        }
    }

    let mut never_ready = NeverReady;
    let error = read_recorded_http_header(&mut never_ready, Duration::from_millis(5)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert!(error.to_string().contains("incomplete after 5 ms"));
}

impl RecordedHttpServer {
    fn new<I, S>(bodies: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let thread_requests = Arc::clone(&requests);
        let failure = Arc::new(Mutex::new(None));
        let thread_failure = Arc::clone(&failure);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let bodies = bodies.into_iter().map(Into::into).collect::<VecDeque<_>>();
        let thread = thread::spawn(move || {
            if let Err(message) =
                run_recorded_http_server(listener, thread_requests, thread_stop, bodies)
            {
                store_recorded_http_failure(&thread_failure, message);
            }
        });
        Self {
            base_url: format!("http://{address}"),
            requests,
            failure,
            stop,
            thread: Some(thread),
        }
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn requests(&self) -> std::result::Result<Vec<String>, String> {
        if let Some(failure) = self
            .failure
            .lock()
            .map_err(|_| "recorded HTTP fixture failure state was poisoned".to_owned())?
            .clone()
        {
            return Err(failure);
        }
        self.requests
            .lock()
            .map_err(|_| "recorded HTTP fixture request log was poisoned".to_owned())
            .map(|requests| requests.clone())
    }
}

impl Drop for RecordedHttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            store_recorded_http_failure(
                &self.failure,
                "recorded HTTP fixture worker panicked".to_owned(),
            );
        }
    }
}
