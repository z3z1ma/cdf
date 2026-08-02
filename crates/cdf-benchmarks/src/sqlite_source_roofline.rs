use std::{
    collections::BTreeMap,
    fs,
    hint::black_box,
    io::Read,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use arrow_array::{Int64Array, RecordBatch, builder::Int64Builder};
use arrow_schema::{DataType, Field, Schema};
use cdf_bench_core::{
    BenchmarkObservation, BiasLabel, CachePreparation, Capability, ChildCommand, ChildObservation,
    ChildObservationStatus, ComparabilityKey, HostCapabilityProvider, HostFingerprint,
    HostProbeConfig, IoMode, MacroRunRequest, MeasurementProviderIdentity, ObservationStatus,
    PhaseMetric, ReferenceIdentity, SystemHostProvider, ToolIdentity, WorkerMeasurement,
    bench_error, host_class, run_macro_cell,
};
use cdf_http::{EgressAllowlist, SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{CdfError, OrderBy, QueryableResource, Result, ScanRequest, SortDirection};
use cdf_runtime::{SourceRegistry, SourceResolutionContext};
use futures_util::StreamExt;
use nix::sys::{
    resource::{UsageWho, getrusage},
    time::TimeValLike,
};
use rusqlite::{Connection, OpenFlags, TransactionBehavior, params_from_iter};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const REPORT_SCHEMA_VERSION: u16 = 2;
const TIMED_REGION_VERSION: u16 = 1;
const DIRECT_BATCH_ROWS: usize = 32 * 1024;
const COLUMN_COUNT: u64 = 3;
const COLUMN_WIDTH_BYTES: u64 = 8;
const MANAGED_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(120);
const PASS_RATIO_PPM: u64 = 900_000;
const MAX_MAD_PERCENT: u64 = 10;
const RUSQLITE_CRATE_VERSION: &str = "0.40.1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteSourceRooflineSettings {
    pub journal_mode: String,
    pub durability: String,
    pub projection: Vec<String>,
    pub arrow_batch_rows: usize,
    pub maximum_emitted_batch_bytes: u64,
    pub in_flight_batch_bound: u16,
    pub connection_count: u16,
    pub writer_count: u16,
    pub compression: String,
    pub database_file_bytes: u64,
    pub physical_io_bytes_observed: bool,
    pub physical_io_bytes_reason: String,
    pub workspace_content_sha256: String,
    pub workspace_content_inputs: Vec<String>,
    pub executable_sha256: String,
    pub sqlite_version: String,
    pub rusqlite_version: String,
    pub semantic_bias: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteSourceRooflineReport {
    pub schema_version: u16,
    pub status: String,
    pub reason: Option<String>,
    pub host: HostFingerprint,
    pub comparability: ComparabilityKey,
    pub measurement_provider: MeasurementProviderIdentity,
    pub settings: SqliteSourceRooflineSettings,
    pub cdf: BenchmarkObservation,
    pub direct_rusqlite: BenchmarkObservation,
    pub roofline_ratio_ppm: u64,
}

struct NoSecrets;

impl SecretProvider for NoSecrets {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        Err(CdfError::auth(format!(
            "SQLite source roofline has no secret for {uri}"
        )))
    }
}

struct RooflineHostProvider {
    system: SystemHostProvider,
}

impl RooflineHostProvider {
    fn new(config: HostProbeConfig) -> Self {
        Self {
            system: SystemHostProvider::new(config),
        }
    }
}

impl HostCapabilityProvider for RooflineHostProvider {
    fn fingerprint(&self) -> cdf_bench_core::BenchResult<HostFingerprint> {
        self.system.fingerprint()
    }

    fn prepare_io_mode(
        &self,
        mode: IoMode,
        allow_privileged: bool,
    ) -> Capability<CachePreparation> {
        self.system.prepare_io_mode(mode, allow_privileged)
    }

    fn observe_child(
        &self,
        command: &ChildCommand,
        timeout: Duration,
    ) -> cdf_bench_core::BenchResult<ChildObservationStatus> {
        observe_worker_child(command, timeout)
    }

    fn discover_tool(&self, name: &str) -> Capability<ToolIdentity> {
        self.system.discover_tool(name)
    }

    fn process_observer_identity(&self) -> MeasurementProviderIdentity {
        MeasurementProviderIdentity {
            method: "sqlite-worker-monotonic-with-rusage-self".to_owned(),
            version: "sqlite-roofline-observer-v2".to_owned(),
            observes_cpu_time: true,
            observes_peak_rss: true,
        }
    }

    fn cgroup_memory_report(&self) -> Capability<cdf_memory::CgroupV2MemoryReport> {
        self.system.cgroup_memory_report()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SqliteWorkerMeasurement {
    #[serde(flatten)]
    workload: WorkerMeasurement,
    process_cpu_time_ns: u64,
    process_peak_rss_bytes: u64,
}

fn observe_worker_child(
    command: &ChildCommand,
    timeout: Duration,
) -> cdf_bench_core::BenchResult<ChildObservationStatus> {
    let started = Instant::now();
    let mut process = Command::new(&command.program);
    process
        .args(&command.args)
        .envs(&command.environment)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(current_dir) = &command.current_dir {
        process.current_dir(current_dir);
    }
    let mut child = process.spawn()?;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break Some(status);
        }
        if started.elapsed() >= timeout {
            child.kill()?;
            child.wait()?;
            break None;
        }
        thread::sleep(Duration::from_millis(5));
    };
    let Some(status) = status else {
        return Ok(ChildObservationStatus::TimedOut);
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    child
        .stdout
        .take()
        .ok_or_else(|| bench_error("SQLite roofline child stdout pipe is absent"))?
        .read_to_end(&mut stdout)?;
    child
        .stderr
        .take()
        .ok_or_else(|| bench_error("SQLite roofline child stderr pipe is absent"))?
        .read_to_end(&mut stderr)?;
    if !status.success() {
        return Ok(ChildObservationStatus::Failed {
            exit_code: status.code(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        });
    }
    let measurement: SqliteWorkerMeasurement =
        serde_json::from_slice(&stdout).map_err(|error| {
            bench_error(format!(
                "SQLite roofline worker emitted invalid resource measurement JSON: {error}"
            ))
        })?;
    Ok(ChildObservationStatus::Completed(ChildObservation {
        wall_time_ns: elapsed_ns(started),
        cpu_time_ns: Some(measurement.process_cpu_time_ns),
        peak_rss_bytes: Some(measurement.process_peak_rss_bytes),
        stdout,
    }))
}

pub fn run_sqlite_source_roofline(
    output: &Path,
    samples: u32,
    rows: u64,
) -> cdf_bench_core::BenchResult<SqliteSourceRooflineReport> {
    if !cfg!(not(debug_assertions)) {
        return Err(bench_error(
            "SQLite source roofline must run from a release build",
        ));
    }
    if samples < 3 || rows < 100_000 {
        return Err(bench_error(
            "SQLite source roofline requires at least 3 samples and 100000 rows",
        ));
    }
    let fixture = tempfile::tempdir()?;
    let database = fixture.path().join("sqlite-source-roofline.sqlite");
    create_fixture(&database, rows)?;
    let file_bytes = fs::metadata(&database)?.len();
    let dependency_versions = BTreeMap::from([
        ("rusqlite".to_owned(), RUSQLITE_CRATE_VERSION.to_owned()),
        ("sqlite".to_owned(), rusqlite::version().to_owned()),
    ]);
    let provider = RooflineHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions,
        benchmark_profile: "release-sqlite-source-roofline".to_owned(),
        storage_target: Some(database.clone()),
    });
    let host = provider.fingerprint()?;
    let host_class = host_class(&host)?;
    let executable = std::env::current_exe()?;
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let (workspace_content_sha256, workspace_content_inputs) =
        workspace_content_revision(&workspace_root)?;
    let executable_sha256 = executable_revision(&executable)?;
    let comparability = ComparabilityKey {
        dataset_id: format!("sqlite-strict-i64x3-{rows}"),
        workload_id: "sqlite-table-source-full-projection".to_owned(),
        timed_region_version: TIMED_REGION_VERSION,
        cdf_revision: base_git_revision(&workspace_root)?,
        dependency_tuple: format!(
            "rusqlite={};sqlite={}",
            RUSQLITE_CRATE_VERSION,
            rusqlite::version()
        ),
        host_class,
        os_toolchain: format!(
            "{}-{};{}",
            host.os.family, host.os.version, host.rust_version
        ),
        io_mode: IoMode::Warm,
    };
    let command = |mode: &str| ChildCommand {
        program: executable.clone(),
        args: vec![
            "worker".to_owned(),
            mode.to_owned(),
            database.display().to_string(),
            rows.to_string(),
        ],
        environment: BTreeMap::new(),
        current_dir: std::env::current_dir().ok(),
    };
    let direct = run_macro_cell(
        &provider,
        &MacroRunRequest {
            comparability: comparability.clone(),
            expected_host_class: Some(comparability.host_class.clone()),
            sample_count: samples,
            timeout: SAMPLE_TIMEOUT,
            allow_privileged_cache_control: false,
            command: command("direct"),
            reference: None,
            bias: Vec::new(),
        },
    )?;
    let cdf = run_macro_cell(
        &provider,
        &MacroRunRequest {
            comparability: comparability.clone(),
            expected_host_class: Some(comparability.host_class.clone()),
            sample_count: samples,
            timeout: SAMPLE_TIMEOUT,
            allow_privileged_cache_control: false,
            command: command("cdf"),
            reference: Some(ReferenceIdentity {
                kind: "direct_library".to_owned(),
                name: "rusqlite".to_owned(),
                version: RUSQLITE_CRATE_VERSION.to_owned(),
                semantic_work: "prepared SELECT in one explicit read transaction, identical projection/order/type conversion, and full Arrow consumption".to_owned(),
            }),
            bias: vec![
                BiasLabel {
                    code: "cdf-governance-overhead".to_owned(),
                    description: "CDF additionally performs managed-lane admission, memory leasing, schema validation, and canonical batch evidence".to_owned(),
                },
                BiasLabel {
                    code: "child-cpu-scope".to_owned(),
                    description: "process CPU and peak RSS include untimed child setup; wall time is the explicit query-through-Arrow timed region".to_owned(),
                },
            ],
        },
    )?;
    let ratio = ratio_ppm(&cdf, &direct);
    let (status, reason) = evaluate(&cdf, &direct, ratio);
    let report = SqliteSourceRooflineReport {
        schema_version: REPORT_SCHEMA_VERSION,
        status,
        reason,
        host,
        comparability,
        measurement_provider: provider.process_observer_identity(),
        settings: SqliteSourceRooflineSettings {
            journal_mode: "delete".to_owned(),
            durability: "fixture commit synchronous=full; read-only explicit transaction"
                .to_owned(),
            projection: vec![
                "id:int64".to_owned(),
                "metric:int64".to_owned(),
                "updated_at:int64".to_owned(),
            ],
            arrow_batch_rows: DIRECT_BATCH_ROWS,
            maximum_emitted_batch_bytes: 32 * 1024 * 1024,
            in_flight_batch_bound: 1,
            connection_count: 1,
            writer_count: 0,
            compression: "none".to_owned(),
            database_file_bytes: file_bytes,
            physical_io_bytes_observed: false,
            physical_io_bytes_reason:
                "portable per-process physical read-byte counters are unavailable; samples record zero and make no physical-throughput claim"
                    .to_owned(),
            workspace_content_sha256,
            workspace_content_inputs,
            executable_sha256,
            sqlite_version: rusqlite::version().to_owned(),
            rusqlite_version: RUSQLITE_CRATE_VERSION.to_owned(),
            semantic_bias: vec![
                "direct runner omits CDF governance work and is therefore a favorable roofline"
                    .to_owned(),
                "both cells use warm OS cache in isolated child processes".to_owned(),
                "CPU time and peak RSS come from each isolated worker's final RUSAGE_SELF snapshot; they include untimed worker setup"
                    .to_owned(),
                "physical I/O bytes are unavailable and intentionally recorded as zero rather than inferred from database file length"
                    .to_owned(),
            ],
        },
        cdf,
        direct_rusqlite: direct,
        roofline_ratio_ppm: ratio,
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output, serde_json::to_vec_pretty(&report)?)?;
    if report.status != "pass" {
        return Err(bench_error(format!(
            "SQLite source roofline is {}: {}",
            report.status,
            report.reason.as_deref().unwrap_or("no reason recorded")
        )));
    }
    Ok(report)
}

pub fn run_sqlite_source_roofline_worker(
    mode: &str,
    database: &Path,
    expected_rows: u64,
) -> cdf_bench_core::BenchResult<serde_json::Value> {
    let measurement = match mode {
        "cdf" => run_cdf_worker(database, expected_rows),
        "direct" => run_direct_worker(database, expected_rows),
        _ => Err(bench_error(format!(
            "unknown SQLite source roofline worker mode `{mode}`"
        ))),
    }?;
    Ok(serde_json::to_value(measurement)?)
}

fn create_fixture(path: &Path, rows: u64) -> cdf_bench_core::BenchResult<()> {
    let mut connection = Connection::open(path)?;
    connection.pragma_update(None, "journal_mode", "DELETE")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            metric INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        ) STRICT;",
    )?;
    {
        let mut insert = transaction.prepare("INSERT INTO events VALUES (?1, ?2, ?3)")?;
        for id in 1..=rows {
            let id = i64::try_from(id)?;
            insert.execute((id, id.wrapping_mul(17), id))?;
        }
    }
    transaction.commit()?;
    Ok(())
}

fn run_cdf_worker(
    database: &Path,
    expected_rows: u64,
) -> cdf_bench_core::BenchResult<SqliteWorkerMeasurement> {
    let project_root = database
        .parent()
        .ok_or_else(|| bench_error("SQLite roofline database has no parent"))?;
    let file_name = database
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| bench_error("SQLite roofline database filename is not UTF-8"))?;
    let mut registry = SourceRegistry::new();
    registry.register(cdf_source_sqlite::SqliteSourceDriver::new()?)?;
    let document = cdf_declarative::parse_toml(&format!(
        r#"
[source.local]
kind = "sqlite"
location = "sqlite://{file_name}"

[resource.events]
table = "events"
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
  {{ name = "metric", type = "int64", nullable = false }},
  {{ name = "updated_at", type = "int64", nullable = false }},
] }}
"#
    ))?;
    let compiled = cdf_declarative::compile_document(&registry, &document)?
        .into_iter()
        .next()
        .ok_or_else(|| bench_error("SQLite roofline compiled no resource"))?;
    let (host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(MANAGED_MEMORY_BYTES)?;
    let context = SourceResolutionContext::new(
        project_root,
        Arc::new(NoSecrets),
        &execution,
        Arc::new(EgressAllowlist::allow_any()),
    );
    let resource = registry.resolve(compiled.source_plan(), &context)?;
    let scan = resource.negotiate(&full_scan_request(resource.as_ref()))?;
    let started = Instant::now();
    let (rows, logical_bytes, checksum) = host.block_on_root(read_cdf_batches(
        resource.as_ref(),
        scan.inline_partitions()
            .and_then(|partitions| partitions.first())
            .cloned()
            .ok_or_else(|| CdfError::internal("SQLite roofline negotiated no partition"))?,
    ))?;
    let elapsed = elapsed_ns(started);
    if rows != expected_rows {
        return Err(bench_error(format!(
            "CDF SQLite roofline read {rows} rows, expected {expected_rows}"
        )));
    }
    black_box(checksum);
    worker_measurement(elapsed, rows, logical_bytes)
}

async fn read_cdf_batches(
    resource: &dyn QueryableResource,
    partition: cdf_kernel::PartitionPlan,
) -> Result<(u64, u64, i128)> {
    let mut stream = resource.open(partition).await?;
    let mut rows = 0_u64;
    let mut logical_bytes = 0_u64;
    let mut checksum = 0_i128;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let record_batch = batch
            .record_batch()
            .ok_or_else(|| CdfError::internal("SQLite roofline received non-Arrow batch"))?;
        rows = rows.saturating_add(u64::try_from(record_batch.num_rows()).unwrap_or(u64::MAX));
        logical_bytes = logical_bytes.saturating_add(
            u64::try_from(record_batch.num_rows())
                .unwrap_or(u64::MAX)
                .saturating_mul(COLUMN_COUNT * COLUMN_WIDTH_BYTES),
        );
        checksum = checksum.saturating_add(sum_i64_columns(record_batch));
    }
    stream.completion().await?;
    Ok((rows, logical_bytes, checksum))
}

fn run_direct_worker(
    database: &Path,
    expected_rows: u64,
) -> cdf_bench_core::BenchResult<SqliteWorkerMeasurement> {
    let started = Instant::now();
    let mut connection = Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(Duration::ZERO)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let mut statement = transaction
        .prepare("SELECT id, metric, updated_at FROM events ORDER BY updated_at ASC, id ASC")?;
    let mut rows = statement.query(params_from_iter(
        std::iter::empty::<rusqlite::types::Value>(),
    ))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("metric", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
    ]));
    let mut row_count = 0_u64;
    let mut logical_bytes = 0_u64;
    let mut checksum = 0_i128;
    loop {
        let mut id = Int64Builder::with_capacity(DIRECT_BATCH_ROWS);
        let mut metric = Int64Builder::with_capacity(DIRECT_BATCH_ROWS);
        let mut updated_at = Int64Builder::with_capacity(DIRECT_BATCH_ROWS);
        let mut batch_rows = 0_usize;
        while batch_rows < DIRECT_BATCH_ROWS {
            let Some(row) = rows.next()? else {
                break;
            };
            id.append_value(row.get::<_, i64>(0)?);
            metric.append_value(row.get::<_, i64>(1)?);
            updated_at.append_value(row.get::<_, i64>(2)?);
            batch_rows += 1;
        }
        if batch_rows == 0 {
            break;
        }
        let record_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id.finish()),
                Arc::new(metric.finish()),
                Arc::new(updated_at.finish()),
            ],
        )?;
        row_count = row_count.saturating_add(u64::try_from(batch_rows)?);
        logical_bytes = logical_bytes.saturating_add(
            u64::try_from(batch_rows)?.saturating_mul(COLUMN_COUNT * COLUMN_WIDTH_BYTES),
        );
        checksum = checksum.saturating_add(sum_i64_columns(&record_batch));
    }
    drop(rows);
    drop(statement);
    transaction.commit()?;
    let elapsed = elapsed_ns(started);
    if row_count != expected_rows {
        return Err(bench_error(format!(
            "direct SQLite roofline read {row_count} rows, expected {expected_rows}"
        )));
    }
    black_box(checksum);
    worker_measurement(elapsed, row_count, logical_bytes)
}

fn full_scan_request(resource: &dyn QueryableResource) -> ScanRequest {
    ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: vec![
            OrderBy {
                field: "updated_at".to_owned(),
                direction: SortDirection::Asc,
            },
            OrderBy {
                field: "id".to_owned(),
                direction: SortDirection::Asc,
            },
        ],
        scope: resource.descriptor().state_scope.clone(),
    }
}

fn sum_i64_columns(batch: &RecordBatch) -> i128 {
    batch
        .columns()
        .iter()
        .filter_map(|column| column.as_any().downcast_ref::<Int64Array>())
        .flat_map(|column| column.values().iter().copied())
        .map(i128::from)
        .sum()
}

fn worker_measurement(
    elapsed: u64,
    rows: u64,
    logical_bytes: u64,
) -> cdf_bench_core::BenchResult<SqliteWorkerMeasurement> {
    let usage = getrusage(UsageWho::RUSAGE_SELF)?;
    let cpu_micros = usage
        .user_time()
        .num_microseconds()
        .saturating_add(usage.system_time().num_microseconds());
    let process_cpu_time_ns = u64::try_from(cpu_micros)
        .unwrap_or_default()
        .saturating_mul(1_000);
    let max_rss = u64::try_from(usage.max_rss())?;
    let process_peak_rss_bytes = if cfg!(any(target_os = "macos", target_os = "ios")) {
        max_rss
    } else {
        max_rss
            .checked_mul(1024)
            .ok_or_else(|| bench_error("SQLite worker peak RSS counter overflowed"))?
    };
    Ok(SqliteWorkerMeasurement {
        workload: WorkerMeasurement {
            timed_wall_time_ns: Some(elapsed),
            rows,
            logical_bytes,
            physical_bytes: 0,
            spill_bytes: 0,
            phases: vec![PhaseMetric {
                phase: "query_through_arrow_consumption".to_owned(),
                duration_ns: elapsed,
                bytes: logical_bytes,
            }],
        },
        process_cpu_time_ns,
        process_peak_rss_bytes,
    })
}

fn executable_revision(executable: &Path) -> cdf_bench_core::BenchResult<String> {
    let mut file = fs::File::open(executable)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn base_git_revision(workspace_root: &Path) -> cdf_bench_core::BenchResult<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(["rev-parse", "HEAD"])
        .output()?;
    if !output.status.success() {
        return Err(bench_error(
            "SQLite roofline could not resolve the base Git revision",
        ));
    }
    let revision = String::from_utf8(output.stdout)?;
    let revision = revision.trim();
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(bench_error(
            "SQLite roofline resolved an invalid base Git revision",
        ));
    }
    Ok(revision.to_owned())
}

fn workspace_content_revision(
    workspace_root: &Path,
) -> cdf_bench_core::BenchResult<(String, Vec<String>)> {
    const INPUTS: &[&str] = &[
        "Cargo.toml",
        "Cargo.lock",
        "crates/cdf-benchmarks/Cargo.toml",
        "crates/cdf-benchmarks/src/bin/sqlite-source-roofline.rs",
        "crates/cdf-benchmarks/src/sqlite_source_roofline.rs",
        "crates/cdf-source-sqlite/Cargo.toml",
        "crates/cdf-source-sqlite/src/catalog.rs",
        "crates/cdf-source-sqlite/src/driver.rs",
        "crates/cdf-source-sqlite/src/error.rs",
        "crates/cdf-source-sqlite/src/identifier.rs",
        "crates/cdf-source-sqlite/src/lib.rs",
        "crates/cdf-source-sqlite/src/source.rs",
        "crates/cdf-source-sqlite/src/source/execution.rs",
        "crates/cdf-source-sqlite/src/source/query.rs",
        "crates/cdf-source-sqlite/src/source/schema.rs",
        "crates/cdf-source-sqlite/src/source/temporal.rs",
        "crates/cdf-source-sqlite/src/source/tests.rs",
    ];
    let mut hasher = Sha256::new();
    for relative in INPUTS {
        let contents = fs::read(workspace_root.join(relative))?;
        hasher.update(relative.as_bytes());
        hasher.update([0]);
        hasher.update(u64::try_from(contents.len())?.to_le_bytes());
        hasher.update(contents);
    }
    Ok((
        format!("sha256:{:x}", hasher.finalize()),
        INPUTS.iter().map(|path| (*path).to_owned()).collect(),
    ))
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn ratio_ppm(cdf: &BenchmarkObservation, direct: &BenchmarkObservation) -> u64 {
    let Some(cdf) = &cdf.summary else {
        return 0;
    };
    let Some(direct) = &direct.summary else {
        return 0;
    };
    if direct.median_logical_bytes_per_second == 0 {
        return 0;
    }
    u64::try_from(
        u128::from(cdf.median_logical_bytes_per_second).saturating_mul(1_000_000)
            / u128::from(direct.median_logical_bytes_per_second),
    )
    .unwrap_or(u64::MAX)
}

fn evaluate(
    cdf: &BenchmarkObservation,
    direct: &BenchmarkObservation,
    ratio: u64,
) -> (String, Option<String>) {
    if !matches!(cdf.status, ObservationStatus::Observed)
        || !matches!(direct.status, ObservationStatus::Observed)
    {
        return (
            "inconclusive".to_owned(),
            Some("one or both comparable cells were not observed".to_owned()),
        );
    }
    if cdf
        .samples
        .iter()
        .chain(&direct.samples)
        .any(|sample| sample.cpu_time_ns.is_none() || sample.peak_rss_bytes.is_none())
    {
        return (
            "inconclusive".to_owned(),
            Some("one or more comparable samples is missing CPU or peak RSS counters".to_owned()),
        );
    }
    let high_variance = |observation: &BenchmarkObservation| {
        observation.summary.as_ref().is_none_or(|summary| {
            u128::from(summary.median_absolute_deviation_ns).saturating_mul(100)
                > u128::from(summary.median_wall_time_ns)
                    .saturating_mul(u128::from(MAX_MAD_PERCENT))
        })
    };
    if high_variance(cdf) || high_variance(direct) {
        return (
            "inconclusive".to_owned(),
            Some("median absolute deviation exceeds 10 percent for a comparable cell".to_owned()),
        );
    }
    if ratio < PASS_RATIO_PPM {
        return (
            "fail".to_owned(),
            Some(format!(
                "roofline ratio {:.3} is below the required 0.900",
                ratio as f64 / 1_000_000.0
            )),
        );
    }
    ("pass".to_owned(), None)
}
