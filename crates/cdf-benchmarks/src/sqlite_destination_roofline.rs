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

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_bench_core::{
    BenchmarkObservation, BiasLabel, CachePreparation, Capability, ChildCommand, ChildObservation,
    ChildObservationStatus, ComparabilityKey, HostCapabilityProvider, HostFingerprint,
    HostProbeConfig, IoMode, MacroRunRequest, MeasurementProviderIdentity, ObservationStatus,
    PhaseMetric, ReferenceIdentity, SystemHostProvider, ToolIdentity, WorkerMeasurement,
    bench_error, host_class, run_macro_cell,
};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, CheckpointId, CommitCounts, CommitSegment, CursorPosition,
    CursorValue, DestinationCommitRequest, DestinationId, DestinationProtocol, IdempotencyToken,
    PackageHash, PipelineId, Receipt, ReceiptId, ResourceId, Result, ScanPlan, SchemaHash,
    ScopeKey, SegmentAck, SegmentId, SourcePosition, StateDelta, StateSegment, TargetName,
    TransactionMetadata, VerifyClause, WriteDisposition,
};
use cdf_package_contract::{
    PackageReplayInputs, QuarantineRecord, ReceiptDraft, ReceiptEvidence, SegmentEntry,
    VerifiedPackageAccess,
};
use cdf_runtime::{
    BulkPathPreparationInput, DestinationIngress, DestinationPlanningContext, DestinationRegistry,
    DestinationResolutionContext,
};
use nix::sys::{
    resource::{UsageWho, getrusage},
    time::TimeValLike,
};
use rusqlite::{Connection, OpenFlags, TransactionBehavior, params, params_from_iter};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const REPORT_SCHEMA_VERSION: u16 = 1;
const TIMED_REGION_VERSION: u16 = 2;
const BATCH_ROWS: u64 = 32 * 1024;
const COLUMN_COUNT: u64 = 3;
const COLUMN_WIDTH_BYTES: u64 = 8;
const MANAGED_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(180);
const PASS_RATIO_PPM: u64 = 900_000;
const MAX_MAD_PERCENT: u64 = 10;
const RUSQLITE_CRATE_VERSION: &str = "0.40.1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteDestinationRooflineSettings {
    pub journal_mode: String,
    pub durability: String,
    pub schema: Vec<String>,
    pub arrow_batch_rows: u64,
    pub connection_count: u16,
    pub writer_count: u16,
    pub useful_bytes_per_row: u64,
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
pub struct SqliteDestinationRooflineReport {
    pub schema_version: u16,
    pub status: String,
    pub reason: Option<String>,
    pub host: HostFingerprint,
    pub comparability: ComparabilityKey,
    pub measurement_provider: MeasurementProviderIdentity,
    pub settings: SqliteDestinationRooflineSettings,
    pub cdf: BenchmarkObservation,
    pub direct_rusqlite: BenchmarkObservation,
    pub roofline_ratio_ppm: u64,
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
            method: "sqlite-destination-worker-monotonic-with-rusage-self".to_owned(),
            version: "sqlite-destination-roofline-observer-v1".to_owned(),
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

struct RooflinePackage {
    hash: String,
    entry: SegmentEntry,
    schema: SchemaRef,
}

#[derive(Serialize)]
struct DirectSqliteColumn {
    name: String,
    sqlite_type: String,
    nullable: bool,
    framework_owned: bool,
}

#[derive(Serialize)]
struct DirectStoredRowRange {
    segment_id: SegmentId,
    row_key_start: u64,
    row_key_end: u64,
}

#[derive(Serialize)]
struct DirectStoredSegment {
    target: TargetName,
    package_hash: PackageHash,
    idempotency_token: IdempotencyToken,
    segment_id: SegmentId,
    scope: Option<ScopeKey>,
    output_position: Option<SourcePosition>,
    row_count: u64,
    byte_count: u64,
    committed_at_ms: i64,
    row_range: Option<DirectStoredRowRange>,
}

#[derive(Serialize)]
struct DirectStoredState {
    pipeline_id: PipelineId,
    resource_id: ResourceId,
    scope: ScopeKey,
    state_version: u16,
    checkpoint_id: CheckpointId,
    parent_checkpoint_id: Option<CheckpointId>,
    package_hash: PackageHash,
    schema_hash: SchemaHash,
    output_position: SourcePosition,
    receipt_id: ReceiptId,
    committed_at_ms: i64,
}

#[derive(Serialize)]
struct DirectProvenanceEvidence {
    index_name: String,
    target: String,
    row_key_column: String,
    unique: bool,
    partial: bool,
}

#[derive(Serialize)]
struct DirectQuarantineEvidence {
    count: u64,
    record_sha256: Vec<[u8; 32]>,
}

#[derive(Serialize)]
struct DirectCommitEvidence {
    version: u16,
    target_schema: Option<Vec<DirectSqliteColumn>>,
    provenance: Option<DirectProvenanceEvidence>,
    segments: Vec<DirectStoredSegment>,
    state: Option<DirectStoredState>,
    quarantine: DirectQuarantineEvidence,
}

impl VerifiedPackageAccess for RooflinePackage {
    fn package_hash(&self) -> &str {
        &self.hash
    }

    fn for_each_identity_segment(
        &self,
        visitor: &mut dyn FnMut(SegmentEntry) -> Result<()>,
    ) -> Result<()> {
        visitor(self.entry.clone())
    }

    fn recorded_scan_plan(&self) -> Result<ScanPlan> {
        Err(CdfError::internal(
            "SQLite destination roofline has no recorded scan plan",
        ))
    }

    fn replay_inputs(&self) -> Result<PackageReplayInputs> {
        Err(CdfError::internal(
            "SQLite destination roofline supplies explicit replay inputs",
        ))
    }

    fn runtime_arrow_schema(&self) -> Result<SchemaRef> {
        Ok(Arc::clone(&self.schema))
    }

    fn for_each_quarantine_record(
        &self,
        _visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
    ) -> Result<()> {
        Ok(())
    }
}

pub fn run_sqlite_destination_roofline(
    output: &Path,
    samples: u32,
    rows: u64,
) -> cdf_bench_core::BenchResult<SqliteDestinationRooflineReport> {
    if !cfg!(not(debug_assertions)) {
        return Err(bench_error(
            "SQLite destination roofline must run from a release build",
        ));
    }
    if samples < 3 || rows < 100_000 {
        return Err(bench_error(
            "SQLite destination roofline requires at least 3 samples and 100000 rows",
        ));
    }
    let fixture = tempfile::tempdir()?;
    let database = fixture.path().join("sqlite-destination-roofline.sqlite");
    configure_database(&database)?;
    let dependency_versions = BTreeMap::from([
        ("rusqlite".to_owned(), RUSQLITE_CRATE_VERSION.to_owned()),
        ("sqlite".to_owned(), rusqlite::version().to_owned()),
    ]);
    let provider = RooflineHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions,
        benchmark_profile: "release-sqlite-destination-roofline".to_owned(),
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
        dataset_id: format!("sqlite-destination-strict-i64x3-{rows}"),
        workload_id: "sqlite-destination-append-commit-fresh-verify".to_owned(),
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
                semantic_work: "one explicit transaction, prepared row binds from identical Arrow batches, full receipt/state/segment/evidence mirror writes, durable commit, the production fresh read-only verifier, and target-count verification".to_owned(),
            }),
            bias: vec![
                BiasLabel {
                    code: "cdf-governance-overhead".to_owned(),
                    description: "CDF additionally validates finalized-package identity, schema, segment ordering, mirror readback, quarantine evidence, and execution-lane admission".to_owned(),
                },
            ],
        },
    )?;
    let ratio = ratio_ppm(&cdf, &direct);
    let (status, reason) = evaluate(&cdf, &direct, ratio);
    let report = SqliteDestinationRooflineReport {
        schema_version: REPORT_SCHEMA_VERSION,
        status,
        reason,
        host,
        comparability,
        measurement_provider: provider.process_observer_identity(),
        settings: SqliteDestinationRooflineSettings {
            journal_mode: "delete".to_owned(),
            durability: "synchronous=full; explicit BEGIN IMMEDIATE and durable COMMIT".to_owned(),
            schema: vec![
                "id:int64".to_owned(),
                "metric:int64".to_owned(),
                "updated_at:int64".to_owned(),
            ],
            arrow_batch_rows: BATCH_ROWS,
            connection_count: 1,
            writer_count: 1,
            useful_bytes_per_row: COLUMN_COUNT * COLUMN_WIDTH_BYTES,
            physical_io_bytes_observed: false,
            physical_io_bytes_reason: "portable per-process physical write-byte counters are unavailable; samples record zero and make no physical-throughput claim".to_owned(),
            workspace_content_sha256,
            workspace_content_inputs,
            executable_sha256,
            sqlite_version: rusqlite::version().to_owned(),
            rusqlite_version: RUSQLITE_CRATE_VERSION.to_owned(),
            semantic_bias: vec![
                "both cells build identical Arrow batches, create their schemas, plan the commit or equivalent mirror artifacts, and begin the transaction outside the timed region; both prepare row inserts inside it".to_owned(),
                "timing starts immediately before the first row/segment is offered and ends after the production fresh read-only verifier and identical target-count verification".to_owned(),
                "direct rusqlite omits package validation but writes full typed mirrors and runs the same production durable verifier".to_owned(),
                "CPU time and peak RSS include untimed worker setup; wall time covers only delivery, commit, and verification".to_owned(),
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
            "SQLite destination roofline is {}: {}",
            report.status,
            report.reason.as_deref().unwrap_or("no reason recorded")
        )));
    }
    Ok(report)
}

pub fn run_sqlite_destination_roofline_worker(
    mode: &str,
    database: &Path,
    expected_rows: u64,
) -> cdf_bench_core::BenchResult<serde_json::Value> {
    reset_database(database)?;
    let batches = build_batches(expected_rows)?;
    let measurement = match mode {
        "cdf" => run_cdf_worker(database, expected_rows, batches),
        "direct" => run_direct_worker(database, expected_rows, batches),
        _ => Err(bench_error(format!(
            "unknown SQLite destination roofline worker mode `{mode}`"
        ))),
    }?;
    Ok(serde_json::to_value(measurement)?)
}

fn run_cdf_worker(
    database: &Path,
    expected_rows: u64,
    batches: Vec<RecordBatch>,
) -> cdf_bench_core::BenchResult<SqliteWorkerMeasurement> {
    configure_database(database)?;
    let logical_schema = batches
        .first()
        .ok_or_else(|| bench_error("SQLite destination roofline produced no batches"))?
        .schema();
    let (inputs, state) = replay_inputs(expected_rows)?;
    let canonical = cdf_package_contract::append_package_row_ord(batches, 0)?;
    let package_bytes = expected_rows.saturating_mul(COLUMN_COUNT * COLUMN_WIDTH_BYTES);
    let package = Arc::new(RooflinePackage {
        hash: inputs.destination_commit.package_hash.as_str().to_owned(),
        entry: SegmentEntry {
            segment_id: state.segment_id.clone(),
            path: "data/segment-1.arrow".to_owned(),
            package_row_ord_start: 0,
            row_count: expected_rows,
            byte_count: package_bytes,
            sha256: "0".repeat(64),
        },
        schema: Arc::clone(&logical_schema),
    });
    let mut registry = DestinationRegistry::new();
    registry.register(cdf_dest_sqlite::SqliteRuntimeDriver)?;
    let target = TargetName::new("events")?;
    let project_root = database
        .parent()
        .ok_or_else(|| bench_error("SQLite destination roofline database has no parent"))?;
    let file_name = database
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| bench_error("SQLite destination roofline filename is not UTF-8"))?;
    let (_host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(MANAGED_MEMORY_BYTES)?;
    let context = DestinationResolutionContext::for_project_run(project_root, &target)
        .with_execution_services(&execution);
    let mut runtime = registry.resolve(&format!("sqlite://{file_name}"), &context)?;
    let bulk_path = runtime.prepare_selected_bulk_path(
        &BulkPathPreparationInput::new(logical_schema.as_ref())
            .with_commit(&inputs.destination_commit)
            .with_execution(execution.capabilities()),
    )?;
    let ingress = match runtime.ingress() {
        DestinationIngress::FinalizedPackage(ingress) => ingress,
        DestinationIngress::StagedSegments(_) => {
            return Err(bench_error(
                "SQLite destination roofline resolved staged ingress",
            ));
        }
    };
    let mut prepared = ingress.prepare_package_commit(
        &inputs,
        &DestinationPlanningContext::new(package, &bulk_path),
    )?;
    let mut session = ingress.begin_prepared_commit(&mut prepared)?;
    session.apply_migrations()?;
    let started = Instant::now();
    session.write_segments(Box::new(std::iter::once(Ok(CommitSegment::new(
        state,
        package_bytes,
        canonical,
    )))))?;
    let receipt = session.finalize()?;
    finish_timed_commit(started, database, expected_rows, &receipt, "CDF")
}

fn run_direct_worker(
    database: &Path,
    expected_rows: u64,
    batches: Vec<RecordBatch>,
) -> cdf_bench_core::BenchResult<SqliteWorkerMeasurement> {
    let mut connection = configure_direct_schema(database)?;
    let (inputs, state) = replay_inputs(expected_rows)?;
    let artifacts = direct_commit_artifacts(&inputs, &state, expected_rows)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let started = Instant::now();
    let mut insert = transaction.prepare(
        "INSERT INTO events(id, metric, updated_at, \"_cdf_row_key\") VALUES (?1, ?2, ?3, ?4)",
    )?;
    let mut row_key = 0_i64;
    for batch in batches {
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| bench_error("direct SQLite id column is not int64"))?;
        let metric = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| bench_error("direct SQLite metric column is not int64"))?;
        let updated_at = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| bench_error("direct SQLite updated_at column is not int64"))?;
        for row in 0..batch.num_rows() {
            insert.execute(params_from_iter([
                id.value(row),
                metric.value(row),
                updated_at.value(row),
                row_key,
            ]))?;
            row_key = row_key
                .checked_add(1)
                .ok_or_else(|| bench_error("direct SQLite row key overflowed"))?;
        }
    }
    drop(insert);
    transaction.execute(
        "INSERT INTO _cdf_loads VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            "events",
            artifacts.receipt.package_hash.as_str(),
            artifacts.receipt.idempotency_token.as_str(),
            "resource-sqlite-destination-roofline",
            artifacts.receipt_json
        ],
    )?;
    transaction.execute(
        "INSERT INTO _cdf_state VALUES (?1, ?2, ?3, ?4)",
        params![
            "pipeline-sqlite-destination-roofline",
            "resource-sqlite-destination-roofline",
            artifacts.scope_json,
            artifacts.state_json
        ],
    )?;
    transaction.execute(
        "INSERT INTO _cdf_state_history VALUES (?1, ?2)",
        params![artifacts.receipt.receipt_id.as_str(), artifacts.state_json],
    )?;
    transaction.execute(
        "INSERT INTO _cdf_segments VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            "events",
            artifacts.receipt.package_hash.as_str(),
            artifacts.receipt.idempotency_token.as_str(),
            "segment-1",
            0_i64,
            artifacts.rows_i64,
            artifacts.segment_json
        ],
    )?;
    transaction.execute(
        "INSERT INTO _cdf_commit_evidence VALUES (?1, ?2)",
        params![
            artifacts.receipt.receipt_id.as_str(),
            artifacts.evidence_json
        ],
    )?;
    transaction.execute(
        "UPDATE _cdf_row_key_allocator SET next_row_key = ?1 WHERE singleton = 1",
        [artifacts.rows_i64],
    )?;
    transaction.commit()?;
    finish_timed_commit(
        started,
        database,
        expected_rows,
        &artifacts.receipt,
        "direct",
    )
}

fn finish_timed_commit(
    started: Instant,
    database: &Path,
    expected_rows: u64,
    receipt: &Receipt,
    cell: &str,
) -> cdf_bench_core::BenchResult<SqliteWorkerMeasurement> {
    let verification =
        cdf_dest_sqlite::SqliteDestination::connect(database.to_path_buf())?.verify(receipt)?;
    if !verification.verified {
        return Err(bench_error(format!(
            "{cell} SQLite destination fresh verification failed: {}",
            verification
                .reason
                .as_deref()
                .unwrap_or("no reason recorded")
        )));
    }
    let target_rows = verify_target_count(database, expected_rows)?;
    black_box(target_rows);
    let elapsed = elapsed_ns(started);
    worker_measurement(elapsed, expected_rows)
}

struct DirectCommitArtifacts {
    receipt: Receipt,
    receipt_json: String,
    evidence_json: String,
    state_json: String,
    segment_json: String,
    scope_json: String,
    rows_i64: i64,
}

fn direct_commit_artifacts(
    inputs: &PackageReplayInputs,
    state: &StateSegment,
    rows: u64,
) -> cdf_bench_core::BenchResult<DirectCommitArtifacts> {
    let committed_at_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let mut receipt_hasher = Sha256::new();
    receipt_hasher.update(b"sqlite\0events\0");
    receipt_hasher.update(inputs.destination_commit.package_hash.as_str().as_bytes());
    let receipt_id = ReceiptId::new(format!("sqlite-{:x}", receipt_hasher.finalize()))?;
    let segment = DirectStoredSegment {
        target: inputs.destination_commit.target.clone(),
        package_hash: inputs.destination_commit.package_hash.clone(),
        idempotency_token: inputs.destination_commit.idempotency_token.clone(),
        segment_id: state.segment_id.clone(),
        scope: Some(state.scope.clone()),
        output_position: Some(state.output_position.clone()),
        row_count: state.row_count,
        byte_count: state.byte_count,
        committed_at_ms,
        row_range: Some(DirectStoredRowRange {
            segment_id: state.segment_id.clone(),
            row_key_start: 0,
            row_key_end: rows,
        }),
    };
    let stored_state = DirectStoredState {
        pipeline_id: inputs.state_delta.pipeline_id.clone(),
        resource_id: inputs.state_delta.resource_id.clone(),
        scope: inputs.state_delta.scope.clone(),
        state_version: inputs.state_delta.state_version,
        checkpoint_id: inputs.state_delta.checkpoint_id.clone(),
        parent_checkpoint_id: inputs.state_delta.parent_checkpoint_id.clone(),
        package_hash: inputs.state_delta.package_hash.clone(),
        schema_hash: inputs.state_delta.schema_hash.clone(),
        output_position: inputs.state_delta.output_position.clone(),
        receipt_id: receipt_id.clone(),
        committed_at_ms,
    };
    let provenance_index = sqlite_row_key_index_name("events");
    let evidence = DirectCommitEvidence {
        version: 1,
        target_schema: Some(vec![
            DirectSqliteColumn {
                name: "id".to_owned(),
                sqlite_type: "INTEGER".to_owned(),
                nullable: false,
                framework_owned: false,
            },
            DirectSqliteColumn {
                name: "metric".to_owned(),
                sqlite_type: "INTEGER".to_owned(),
                nullable: false,
                framework_owned: false,
            },
            DirectSqliteColumn {
                name: "updated_at".to_owned(),
                sqlite_type: "INTEGER".to_owned(),
                nullable: false,
                framework_owned: false,
            },
        ]),
        provenance: Some(DirectProvenanceEvidence {
            index_name: provenance_index,
            target: "events".to_owned(),
            row_key_column: "_cdf_row_key".to_owned(),
            unique: true,
            partial: true,
        }),
        segments: vec![segment],
        state: Some(stored_state),
        quarantine: DirectQuarantineEvidence {
            count: 0,
            record_sha256: Vec::new(),
        },
    };
    let evidence_json = serde_json::to_string(&evidence)?;
    let evidence_sha = format!("{:x}", Sha256::digest(evidence_json.as_bytes()));
    let mut quarantine_hasher = Sha256::new();
    quarantine_hasher.update(b"cdf.sqlite.quarantine-multiset.v1\0");
    quarantine_hasher.update(0_u64.to_be_bytes());
    let quarantine_sha = format!("{:x}", quarantine_hasher.finalize());
    let transaction = TransactionMetadata {
        system: "sqlite".to_owned(),
        values: [
            ("connection_scope".to_owned(), "single_file".to_owned()),
            ("journal_mode".to_owned(), "delete".to_owned()),
            ("synchronous".to_owned(), "2".to_owned()),
            ("duplicate".to_owned(), "false".to_owned()),
            ("quarantine_count".to_owned(), "0".to_owned()),
            ("quarantine_multiset_sha256_v1".to_owned(), quarantine_sha),
            ("commit_evidence_sha256_v1".to_owned(), evidence_sha),
            ("loads_table".to_owned(), "_cdf_loads".to_owned()),
            ("state_table".to_owned(), "_cdf_state".to_owned()),
            (
                "state_history_table".to_owned(),
                "_cdf_state_history".to_owned(),
            ),
            ("segments_table".to_owned(), "_cdf_segments".to_owned()),
            (
                "commit_evidence_table".to_owned(),
                "_cdf_commit_evidence".to_owned(),
            ),
        ]
        .into_iter()
        .collect(),
    };
    let plan = cdf_dest_sqlite::SqliteDestination::connect("<roofline-plan>")?
        .plan_commit(&inputs.destination_commit)?;
    let mut parameters = BTreeMap::new();
    parameters.insert("target".to_owned(), "events".to_owned());
    parameters.insert(
        "package_hash".to_owned(),
        inputs.destination_commit.package_hash.as_str().to_owned(),
    );
    parameters.insert(
        "idempotency_token".to_owned(),
        inputs
            .destination_commit
            .idempotency_token
            .as_str()
            .to_owned(),
    );
    parameters.insert(
        "schema_hash".to_owned(),
        inputs.schema_hash.as_str().to_owned(),
    );
    parameters.insert("segment_count".to_owned(), "1".to_owned());
    parameters.insert("segment.0.id".to_owned(), state.segment_id.to_string());
    parameters.insert("segment.0.rows".to_owned(), state.row_count.to_string());
    parameters.insert("segment.0.bytes".to_owned(), state.byte_count.to_string());
    let receipt = ReceiptDraft::ordinary(
        receipt_id,
        DestinationId::new("sqlite")?,
        &inputs.destination_commit,
        &plan,
        vec![SegmentAck {
            segment_id: state.segment_id.clone(),
            row_count: state.row_count,
            byte_count: state.byte_count,
        }],
        inputs.schema_hash.clone(),
        ReceiptEvidence {
            transaction: Some(transaction),
            counts: CommitCounts {
                rows_written: rows,
                rows_inserted: Some(rows),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            committed_at_ms,
            verify: VerifyClause {
                kind: "sqlite_mirror_receipt_v1".to_owned(),
                statement: "SELECT receipt_json FROM _cdf_loads WHERE target = ?1 AND package_hash = ?2 AND idempotency_token = ?3".to_owned(),
                parameters,
            },
        },
    )?
    .finalize()?;
    let receipt_json = serde_json::to_string(&receipt)?;
    Ok(DirectCommitArtifacts {
        receipt_json,
        state_json: serde_json::to_string(
            evidence
                .state
                .as_ref()
                .ok_or_else(|| bench_error("direct SQLite commit evidence omitted state"))?,
        )?,
        segment_json: serde_json::to_string(&evidence.segments[0])?,
        scope_json: serde_json::to_string(&inputs.state_delta.scope)?,
        evidence_json,
        receipt,
        rows_i64: i64::try_from(rows)?,
    })
}

fn build_batches(rows: u64) -> cdf_bench_core::BenchResult<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("metric", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
    ]));
    let mut batches = Vec::new();
    let mut start = 0_u64;
    while start < rows {
        let end = rows.min(start.saturating_add(BATCH_ROWS));
        let ids = (start..end)
            .map(i64::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let metrics = ids
            .iter()
            .map(|value| value.wrapping_mul(17))
            .collect::<Vec<_>>();
        let updated = ids.clone();
        batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(metrics)),
                Arc::new(Int64Array::from(updated)),
            ],
        )?);
        start = end;
    }
    Ok(batches)
}

fn replay_inputs(rows: u64) -> cdf_bench_core::BenchResult<(PackageReplayInputs, StateSegment)> {
    let package_hash = PackageHash::new(format!("sha256:{}", "a".repeat(64)))?;
    let schema_hash = SchemaHash::new("schema-sqlite-destination-roofline")?;
    let output_position = SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(i64::try_from(rows)?),
    });
    let state = StateSegment {
        segment_id: SegmentId::new("segment-1")?,
        scope: ScopeKey::Resource,
        output_position: output_position.clone(),
        row_count: rows,
        byte_count: rows.saturating_mul(COLUMN_COUNT * COLUMN_WIDTH_BYTES),
    };
    let destination_commit = DestinationCommitRequest {
        package_hash: package_hash.clone(),
        target: TargetName::new("events")?,
        disposition: WriteDisposition::Append,
        segments: vec![state.clone()],
        idempotency_token: IdempotencyToken::new(package_hash.as_str())?,
    };
    Ok((
        PackageReplayInputs {
            input_checkpoint: None,
            state_delta: StateDelta {
                checkpoint_id: CheckpointId::new("checkpoint-sqlite-destination-roofline")?,
                pipeline_id: PipelineId::new("pipeline-sqlite-destination-roofline")?,
                resource_id: ResourceId::new("resource-sqlite-destination-roofline")?,
                scope: ScopeKey::Resource,
                state_version: CHECKPOINT_STATE_VERSION,
                parent_checkpoint_id: None,
                input_position: None,
                output_position,
                output_watermark: None,
                partition_watermarks: Vec::new(),
                late_data_carryover: Vec::new(),
                source_continuation: None,
                package_hash,
                schema_hash: schema_hash.clone(),
                segments: vec![state.clone()],
            },
            destination_commit,
            merge_keys: Vec::new(),
            schema_hash,
            destination_policy: Default::default(),
        },
        state,
    ))
}

fn configure_database(path: &Path) -> cdf_bench_core::BenchResult<()> {
    let connection = Connection::open(path)?;
    connection.pragma_update(None, "journal_mode", "DELETE")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

fn configure_direct_schema(path: &Path) -> cdf_bench_core::BenchResult<Connection> {
    let connection = Connection::open(path)?;
    connection.pragma_update(None, "journal_mode", "DELETE")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.execute_batch(
        "CREATE TABLE events (
            id INTEGER NOT NULL,
            metric INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            \"_cdf_row_key\" INTEGER
        ) STRICT;
        CREATE TABLE _cdf_loads (
            target TEXT NOT NULL, package_hash TEXT NOT NULL,
            idempotency_token TEXT NOT NULL, resource_id TEXT, receipt_json TEXT NOT NULL,
            PRIMARY KEY (target, package_hash, idempotency_token)
        ) STRICT;
        CREATE TABLE _cdf_state (
            pipeline_id TEXT NOT NULL, resource_id TEXT NOT NULL,
            scope_json TEXT NOT NULL, state_json TEXT NOT NULL,
            PRIMARY KEY (pipeline_id, resource_id, scope_json)
        ) STRICT;
        CREATE TABLE _cdf_state_history (
            receipt_id TEXT NOT NULL PRIMARY KEY, state_json TEXT NOT NULL
        ) STRICT;
        CREATE TABLE _cdf_segments (
            target TEXT NOT NULL, package_hash TEXT NOT NULL,
            idempotency_token TEXT NOT NULL, segment_id TEXT NOT NULL,
            row_key_start INTEGER, row_key_end INTEGER, segment_json TEXT NOT NULL,
            PRIMARY KEY (target, package_hash, idempotency_token, segment_id),
            CHECK ((row_key_start IS NULL) = (row_key_end IS NULL)),
            CHECK (row_key_start IS NULL OR row_key_start <= row_key_end)
        ) STRICT;
        CREATE UNIQUE INDEX _cdf_segments_row_range_start
            ON _cdf_segments(target, row_key_start) WHERE row_key_start IS NOT NULL;
        CREATE TABLE _cdf_quarantine (
            target TEXT NOT NULL, package_hash TEXT NOT NULL,
            source_row_ordinal INTEGER NOT NULL, rule_id TEXT NOT NULL,
            error_code TEXT NOT NULL, quarantine_json TEXT NOT NULL,
            PRIMARY KEY (target, package_hash, source_row_ordinal, rule_id, error_code)
        ) STRICT;
        CREATE TABLE _cdf_commit_evidence (
            receipt_id TEXT NOT NULL PRIMARY KEY, evidence_json TEXT NOT NULL
        ) STRICT;
        CREATE TABLE _cdf_row_key_allocator (
            singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
            next_row_key INTEGER NOT NULL CHECK (next_row_key >= 0)
        ) STRICT;
        INSERT INTO _cdf_row_key_allocator VALUES (1, 0);",
    )?;
    connection.execute_batch(&format!(
        "CREATE UNIQUE INDEX \"{}\" ON events(\"_cdf_row_key\")
         WHERE \"_cdf_row_key\" IS NOT NULL",
        sqlite_row_key_index_name("events")
    ))?;
    Ok(connection)
}

fn sqlite_row_key_index_name(target: &str) -> String {
    format!("_cdf_row_key_{:x}", Sha256::digest(target.as_bytes()))
}

fn reset_database(path: &Path) -> cdf_bench_core::BenchResult<()> {
    for candidate in [path.to_path_buf(), path.with_extension("sqlite-journal")] {
        match fs::remove_file(candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn verify_target_count(path: &Path, expected: u64) -> cdf_bench_core::BenchResult<u64> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let count: i64 = connection.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?;
    let count = u64::try_from(count)?;
    if count != expected {
        return Err(bench_error(format!(
            "SQLite destination roofline verified {count} rows, expected {expected}"
        )));
    }
    Ok(count)
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
        .ok_or_else(|| bench_error("SQLite destination roofline child stdout pipe is absent"))?
        .read_to_end(&mut stdout)?;
    child
        .stderr
        .take()
        .ok_or_else(|| bench_error("SQLite destination roofline child stderr pipe is absent"))?
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
                "SQLite destination roofline worker emitted invalid measurement JSON: {error}"
            ))
        })?;
    Ok(ChildObservationStatus::Completed(ChildObservation {
        wall_time_ns: elapsed_ns(started),
        cpu_time_ns: Some(measurement.process_cpu_time_ns),
        peak_rss_bytes: Some(measurement.process_peak_rss_bytes),
        stdout,
    }))
}

fn worker_measurement(
    elapsed: u64,
    rows: u64,
) -> cdf_bench_core::BenchResult<SqliteWorkerMeasurement> {
    let logical_bytes = rows.saturating_mul(COLUMN_COUNT * COLUMN_WIDTH_BYTES);
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
            .ok_or_else(|| bench_error("SQLite destination worker peak RSS overflowed"))?
    };
    Ok(SqliteWorkerMeasurement {
        workload: WorkerMeasurement {
            timed_wall_time_ns: Some(elapsed),
            rows,
            logical_bytes,
            physical_bytes: 0,
            spill_bytes: 0,
            phases: vec![PhaseMetric {
                phase: "package_delivery_commit_and_fresh_verification".to_owned(),
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
            "SQLite destination roofline could not resolve the base Git revision",
        ));
    }
    let revision = String::from_utf8(output.stdout)?;
    let revision = revision.trim();
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(bench_error(
            "SQLite destination roofline resolved an invalid base Git revision",
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
        "crates/cdf-benchmarks/src/bin/sqlite-destination-roofline.rs",
        "crates/cdf-benchmarks/src/sqlite_destination_roofline.rs",
        "crates/cdf-contract/src/policy.rs",
        "crates/cdf-contract/src/residual.rs",
        "crates/cdf-dest-sqlite/Cargo.toml",
        "crates/cdf-dest-sqlite/src/error.rs",
        "crates/cdf-dest-sqlite/src/identifier.rs",
        "crates/cdf-dest-sqlite/src/lib.rs",
        "crates/cdf-dest-sqlite/src/mapping.rs",
        "crates/cdf-dest-sqlite/src/mirrors.rs",
        "crates/cdf-dest-sqlite/src/models.rs",
        "crates/cdf-dest-sqlite/src/package.rs",
        "crates/cdf-dest-sqlite/src/plan.rs",
        "crates/cdf-dest-sqlite/src/receipts.rs",
        "crates/cdf-dest-sqlite/src/runtime.rs",
        "crates/cdf-dest-sqlite/src/sheet.rs",
        "crates/cdf-dest-sqlite/src/transaction.rs",
        "crates/cdf-dest-sqlite/src/transaction/session.rs",
        "crates/cdf-dest-sqlite/src/transaction/verifier.rs",
        "crates/cdf-dest-sqlite/src/transaction/writer.rs",
        "crates/cdf-runtime/src/execution_host.rs",
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
