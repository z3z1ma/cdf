use std::{
    collections::BTreeMap,
    fs,
    hint::black_box,
    io::Read,
    num::NonZeroUsize,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use arrow_array::{ArrayRef, FixedSizeBinaryArray, Int64Array, RecordBatch};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_bench_core::{
    BenchmarkObservation, BiasLabel, CachePreparation, Capability, ChildCommand, ChildObservation,
    ChildObservationStatus, ComparabilityKey, HostCapabilityProvider, HostFingerprint,
    HostProbeConfig, IoMode, MacroRunRequest, MeasurementProviderIdentity, ObservationStatus,
    PhaseMetric, ReferenceIdentity, SystemHostProvider, ToolIdentity, WorkerMeasurement,
    bench_error, host_class, run_macro_cell,
};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, CheckpointId, CommitSegment, CursorPosition, CursorValue,
    DestinationCommitRequest, IdempotencyToken, PackageHash, PipelineId, ResourceId, Result,
    ScanPlan, SchemaHash, ScopeKey, SegmentId, SourcePosition, StateDelta, StateSegment,
    TargetName, WriteDisposition,
};
use cdf_package_contract::{
    PackageReplayInputs, QuarantineRecord, SegmentEntry, VerifiedPackageAccess,
};
use cdf_runtime::{
    BulkPathPreparationInput, DestinationIngress, DestinationPlanningContext, DestinationRegistry,
    DestinationResolutionContext,
};
use clickhouse::{Compression, ResponseLimits};
use clickhouse_ext_arrow::ArrowClientExt;
use nix::sys::{
    resource::{UsageWho, getrusage},
    time::TimeValLike,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

const REPORT_SCHEMA_VERSION: u16 = 1;
const TIMED_REGION_VERSION: u16 = 1;
const DATABASE: &str = "cdf_destination_roofline";
const TARGET: &str = "events";
const BATCH_ROWS: u64 = 65_536;
const COLUMN_COUNT: u64 = 3;
const COLUMN_WIDTH_BYTES: u64 = 8;
const MANAGED_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(180);
const PASS_RATIO_PPM: u64 = 900_000;
const MAX_MAD_PERCENT: u64 = 10;
const CLICKHOUSE_CRATE_VERSION: &str = "0.15.1";
const CLICKHOUSE_ARROW_CRATE_VERSION: &str = "0.1.0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseDestinationRooflineSettings {
    pub server_version: String,
    pub clickhouse_crate_version: String,
    pub clickhouse_ext_arrow_version: String,
    pub compression: String,
    pub acknowledgement: String,
    pub insert_deduplication_token: String,
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
    pub semantic_bias: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseDestinationRooflineCell {
    pub disposition: String,
    pub comparability: ComparabilityKey,
    pub cdf: BenchmarkObservation,
    pub direct_arrowstream: BenchmarkObservation,
    pub roofline_ratio_ppm: u64,
    pub status: String,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseDestinationRooflineReport {
    pub schema_version: u16,
    pub status: String,
    pub reason: Option<String>,
    pub host: HostFingerprint,
    pub measurement_provider: MeasurementProviderIdentity,
    pub settings: ClickHouseDestinationRooflineSettings,
    pub cells: Vec<ClickHouseDestinationRooflineCell>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ClickHouseWorkerMeasurement {
    #[serde(flatten)]
    workload: WorkerMeasurement,
    process_cpu_time_ns: u64,
    process_peak_rss_bytes: u64,
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
            method: "clickhouse-destination-worker-monotonic-with-rusage-self".to_owned(),
            version: "clickhouse-destination-roofline-observer-v1".to_owned(),
            observes_cpu_time: true,
            observes_peak_rss: true,
        }
    }

    fn cgroup_memory_report(&self) -> Capability<cdf_memory::CgroupV2MemoryReport> {
        self.system.cgroup_memory_report()
    }
}

struct RooflinePackage {
    hash: String,
    entry: SegmentEntry,
    schema: SchemaRef,
    dedup_summary: Option<cdf_package_contract::PackageDedupSummary>,
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
            "ClickHouse destination roofline has no recorded scan plan",
        ))
    }

    fn replay_inputs(&self) -> Result<PackageReplayInputs> {
        Err(CdfError::internal(
            "ClickHouse destination roofline supplies explicit replay inputs",
        ))
    }

    fn runtime_arrow_schema(&self) -> Result<SchemaRef> {
        Ok(Arc::clone(&self.schema))
    }

    fn verified_dedup_summary(&self) -> Result<Option<cdf_package_contract::PackageDedupSummary>> {
        Ok(self.dedup_summary.clone())
    }

    fn for_each_quarantine_record(
        &self,
        _visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
    ) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Deserialize, clickhouse::Row)]
struct VersionRow {
    version: String,
}

#[derive(Debug, Deserialize, clickhouse::Row)]
struct TargetCountRow {
    rows: u64,
    unique_ordinals: u64,
    minimum_ordinal: Option<u64>,
    maximum_ordinal: Option<u64>,
}

pub fn run_clickhouse_destination_roofline(
    endpoint: &str,
    output: &Path,
    samples: u32,
    rows: u64,
) -> cdf_bench_core::BenchResult<ClickHouseDestinationRooflineReport> {
    if !cfg!(not(debug_assertions)) {
        return Err(bench_error(
            "ClickHouse destination roofline must run from a release build",
        ));
    }
    if samples < 3 || rows < 100_000 {
        return Err(bench_error(
            "ClickHouse destination roofline requires at least 3 samples and 100000 rows",
        ));
    }
    let connection = roofline_connection(endpoint)?;
    let (_, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(MANAGED_MEMORY_BYTES)?;
    let server_version = execution.run_io({
        let client = roofline_client(&connection.http_endpoint);
        async move {
            client
                .query("SELECT version() AS version")
                .fetch_one::<VersionRow>()
                .await
                .map(|row| row.version)
                .map_err(|_| CdfError::environment("query ClickHouse roofline server version"))
        }
    })?;
    initialize_database(&execution, &connection.http_endpoint)?;
    let dependency_versions = BTreeMap::from([
        ("clickhouse".to_owned(), CLICKHOUSE_CRATE_VERSION.to_owned()),
        (
            "clickhouse-ext-arrow".to_owned(),
            CLICKHOUSE_ARROW_CRATE_VERSION.to_owned(),
        ),
        ("clickhouse-server".to_owned(), server_version.clone()),
    ]);
    let provider = RooflineHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions,
        benchmark_profile: "release-clickhouse-destination-roofline".to_owned(),
        storage_target: None,
    });
    let host = provider.fingerprint()?;
    let host_class = host_class(&host)?;
    let executable = std::env::current_exe()?;
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let (workspace_content_sha256, workspace_content_inputs) =
        workspace_content_revision(&workspace_root)?;
    let executable_sha256 = executable_revision(&executable)?;
    let mut cells = Vec::new();
    for disposition in ["append", "native_merge"] {
        let comparability = ComparabilityKey {
            dataset_id: format!("clickhouse-destination-i64x3-{rows}"),
            workload_id: format!("clickhouse-destination-{disposition}-commit-verify"),
            timed_region_version: TIMED_REGION_VERSION,
            cdf_revision: base_git_revision(&workspace_root)?,
            dependency_tuple: format!(
                "clickhouse={CLICKHOUSE_CRATE_VERSION};clickhouse-ext-arrow={CLICKHOUSE_ARROW_CRATE_VERSION};server={server_version}"
            ),
            host_class: host_class.clone(),
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
                disposition.to_owned(),
                endpoint.to_owned(),
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
                    name: "clickhouse plus clickhouse-ext-arrow".to_owned(),
                    version: format!(
                        "{CLICKHOUSE_CRATE_VERSION}+{CLICKHOUSE_ARROW_CRATE_VERSION}"
                    ),
                    semantic_work: "one LZ4 ArrowStream insert with synchronous acknowledgement and a deterministic deduplication token, followed by exact canonical package-ordinal verification".to_owned(),
                }),
                bias: vec![BiasLabel {
                    code: "cdf-governance-overhead".to_owned(),
                    description: "CDF additionally validates package identity and schema, writes exact segment/state/receipt mirrors, and independently verifies the finalized receipt".to_owned(),
                }],
            },
        )?;
        let ratio = ratio_ppm(&cdf, &direct);
        let (status, reason) = evaluate(&cdf, &direct, ratio);
        cells.push(ClickHouseDestinationRooflineCell {
            disposition: disposition.to_owned(),
            comparability,
            cdf,
            direct_arrowstream: direct,
            roofline_ratio_ppm: ratio,
            status,
            reason,
        });
    }
    let status = if cells.iter().all(|cell| cell.status == "pass") {
        "pass"
    } else if cells.iter().any(|cell| cell.status == "fail") {
        "fail"
    } else {
        "inconclusive"
    }
    .to_owned();
    let reason = (status != "pass").then(|| {
        cells
            .iter()
            .filter(|cell| cell.status != "pass")
            .map(|cell| {
                format!(
                    "{}: {}",
                    cell.disposition,
                    cell.reason.as_deref().unwrap_or(cell.status.as_str())
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    });
    let report = ClickHouseDestinationRooflineReport {
        schema_version: REPORT_SCHEMA_VERSION,
        status,
        reason,
        host,
        measurement_provider: provider.process_observer_identity(),
        settings: ClickHouseDestinationRooflineSettings {
            server_version,
            clickhouse_crate_version: CLICKHOUSE_CRATE_VERSION.to_owned(),
            clickhouse_ext_arrow_version: CLICKHOUSE_ARROW_CRATE_VERSION.to_owned(),
            compression: "lz4".to_owned(),
            acknowledgement: "async_insert=0;wait_for_async_insert=1".to_owned(),
            insert_deduplication_token: "one deterministic package/segment token".to_owned(),
            schema: vec![
                "id:Int64".to_owned(),
                "metric:Int64".to_owned(),
                "updated_at:Int64".to_owned(),
                "_cdf_package_hash:FixedString(32)".to_owned(),
                "_cdf_package_row_ord:UInt64".to_owned(),
            ],
            arrow_batch_rows: BATCH_ROWS,
            connection_count: 1,
            writer_count: 1,
            useful_bytes_per_row: COLUMN_COUNT * COLUMN_WIDTH_BYTES,
            physical_io_bytes_observed: false,
            physical_io_bytes_reason: "the official HTTP client does not expose exact compressed request bytes; samples record zero and make no physical-throughput claim".to_owned(),
            workspace_content_sha256,
            workspace_content_inputs,
            executable_sha256,
            semantic_bias: vec![
                "both cells build identical Arrow batches, reset and seed identical targets, and create the official client outside the timed region".to_owned(),
                "timing starts immediately before the ArrowStream insert and ends after exact canonical package-ordinal verification; CDF also performs receipt settlement and fresh receipt verification".to_owned(),
                "the direct cell omits CDF settlement mirrors and package-governance validation, making it a favorable roofline".to_owned(),
                "CPU time and peak RSS include untimed worker setup; wall time covers only delivery, commit, and verification".to_owned(),
            ],
        },
        cells,
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output, serde_json::to_vec_pretty(&report)?)?;
    if report.status != "pass" {
        return Err(bench_error(format!(
            "ClickHouse destination roofline is {}: {}",
            report.status,
            report.reason.as_deref().unwrap_or("no reason recorded")
        )));
    }
    Ok(report)
}

pub fn run_clickhouse_destination_roofline_worker(
    mode: &str,
    disposition: &str,
    endpoint: &str,
    rows: u64,
) -> cdf_bench_core::BenchResult<serde_json::Value> {
    let disposition = match disposition {
        "append" => WriteDisposition::Append,
        "native_merge" => WriteDisposition::Merge,
        value => {
            return Err(bench_error(format!(
                "unknown ClickHouse destination roofline disposition `{value}`"
            )));
        }
    };
    let connection = roofline_connection(endpoint)?;
    let (_, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(MANAGED_MEMORY_BYTES)?;
    setup_database(&execution, &connection.http_endpoint, &disposition, rows)?;
    let batches = build_batches(rows)?;
    let measurement = match mode {
        "cdf" => run_cdf_worker(
            &execution,
            &connection.destination_uri,
            disposition,
            rows,
            batches,
        ),
        "direct" => run_direct_worker(
            &execution,
            &connection.http_endpoint,
            disposition,
            rows,
            batches,
        ),
        value => Err(bench_error(format!(
            "unknown ClickHouse destination roofline worker mode `{value}`"
        ))),
    }?;
    Ok(serde_json::to_value(measurement)?)
}

fn run_cdf_worker(
    execution: &cdf_runtime::ExecutionServices,
    destination_uri: &str,
    disposition: WriteDisposition,
    rows: u64,
    batches: Vec<RecordBatch>,
) -> cdf_bench_core::BenchResult<ClickHouseWorkerMeasurement> {
    let logical_schema = batches
        .first()
        .ok_or_else(|| bench_error("ClickHouse destination roofline produced no batches"))?
        .schema();
    let (inputs, state) = replay_inputs(rows, disposition.clone())?;
    let canonical = cdf_package_contract::append_package_row_ord(batches, 0)?;
    let package_bytes = rows.saturating_mul(COLUMN_COUNT * COLUMN_WIDTH_BYTES);
    let package = Arc::new(RooflinePackage {
        hash: inputs.destination_commit.package_hash.as_str().to_owned(),
        entry: SegmentEntry {
            segment_id: state.segment_id.clone(),
            path: "data/segment-1.arrow".to_owned(),
            package_row_ord_start: 0,
            row_count: rows,
            byte_count: package_bytes,
            sha256: "0".repeat(64),
        },
        schema: Arc::clone(&logical_schema),
        dedup_summary: (disposition == WriteDisposition::Merge).then(|| {
            cdf_package_contract::PackageDedupSummary {
                version: cdf_package_contract::DEDUP_SUMMARY_VERSION,
                rule_id: "roofline-merge-key".to_owned(),
                keys: vec!["id".to_owned()],
                keep: cdf_package_contract::PackageDedupKeep::Last,
                input_rows: rows,
                output_rows: rows,
                duplicate_key_count: 0,
                dropped_row_count: 0,
                provenance_format: "parquet".to_owned(),
                provenance_version: cdf_package_contract::DEDUP_PROVENANCE_VERSION,
                provenance_path: cdf_package_contract::DEDUP_PROVENANCE_DIRECTORY.to_owned(),
                provenance_shard_row_target: 65_536,
                shard_count: 0,
            }
        }),
    });
    let mut registry = DestinationRegistry::new();
    registry.register(cdf_dest_clickhouse::ClickHouseRuntimeDriver)?;
    let target = TargetName::new(TARGET)?;
    let context = DestinationResolutionContext::for_project_run(Path::new("."), &target)
        .with_execution_services(execution);
    let mut runtime = registry.resolve(destination_uri, &context)?;
    let bulk_path = runtime.prepare_selected_bulk_path(
        &BulkPathPreparationInput::new(logical_schema.as_ref())
            .with_commit(&inputs.destination_commit)
            .with_execution(execution.capabilities()),
    )?;
    let mut prepared = match runtime.ingress() {
        DestinationIngress::FinalizedPackage(ingress) => ingress.prepare_package_commit(
            &inputs,
            &DestinationPlanningContext::new(package, &bulk_path),
        )?,
        DestinationIngress::StagedSegments(_) => {
            return Err(bench_error(
                "ClickHouse destination roofline resolved staged ingress",
            ));
        }
    };
    let (receipt, started) = {
        let ingress = match runtime.ingress() {
            DestinationIngress::FinalizedPackage(ingress) => ingress,
            DestinationIngress::StagedSegments(_) => {
                return Err(bench_error(
                    "ClickHouse destination roofline changed ingress mode",
                ));
            }
        };
        let mut session = ingress.begin_prepared_commit(&mut prepared)?;
        session.apply_migrations()?;
        let started = Instant::now();
        session.write_segments(Box::new(std::iter::once(Ok(CommitSegment::new(
            state,
            package_bytes,
            canonical,
        )))))?;
        (session.finalize()?, started)
    };
    let verification = runtime.verify_receipt(&receipt)?;
    if !verification.verified {
        return Err(bench_error(format!(
            "CDF ClickHouse destination fresh verification failed: {}",
            verification
                .reason
                .as_deref()
                .unwrap_or("no reason recorded")
        )));
    }
    let elapsed = elapsed_ns(started);
    black_box(&receipt);
    worker_measurement(elapsed, rows)
}

fn run_direct_worker(
    execution: &cdf_runtime::ExecutionServices,
    endpoint: &str,
    disposition: WriteDisposition,
    rows: u64,
    batches: Vec<RecordBatch>,
) -> cdf_bench_core::BenchResult<ClickHouseWorkerMeasurement> {
    let (inputs, _) = replay_inputs(rows, disposition.clone())?;
    let canonical = cdf_package_contract::append_package_row_ord(batches, 0)?;
    let package_hash = inputs.destination_commit.package_hash.clone();
    let client = roofline_client(endpoint)
        .with_database(DATABASE)
        .with_setting(
            "insert_deduplication_token",
            inputs.destination_commit.idempotency_token.as_str(),
        );
    let started = Instant::now();
    execution.run_io(async move {
        let physical = add_package_hash(canonical, &package_hash)?;
        let mut insert = client.insert_arrow_with(
            "INSERT INTO events (id, metric, updated_at, _cdf_package_hash, _cdf_package_row_ord) FORMAT ArrowStream",
        );
        for batch in physical {
            insert
                .write(&batch)
                .await
                .map_err(|_| CdfError::destination("direct ClickHouse ArrowStream write failed"))?;
        }
        insert
            .flush()
            .await
            .map_err(|_| CdfError::destination("direct ClickHouse ArrowStream flush failed"))?;
        insert
            .end()
            .await
            .map_err(|_| CdfError::destination("direct ClickHouse ArrowStream finish failed"))?;
        let final_clause = if disposition == WriteDisposition::Merge {
            " FINAL"
        } else {
            ""
        };
        let verification = client
            .query(&format!(
                "SELECT count() AS rows, uniqExact(_cdf_package_row_ord) AS unique_ordinals, minOrNull(_cdf_package_row_ord) AS minimum_ordinal, maxOrNull(_cdf_package_row_ord) AS maximum_ordinal FROM events{final_clause} WHERE hex(_cdf_package_hash) = ?"
            ))
            .with_response_limits(response_limits())
            .bind("AA".repeat(32))
            .fetch_one::<TargetCountRow>()
            .await
            .map_err(|_| CdfError::destination("verify direct ClickHouse package ordinals"))?;
        if verification.rows != rows
            || verification.unique_ordinals != rows
            || verification.minimum_ordinal != (rows > 0).then_some(0)
            || verification.maximum_ordinal != rows.checked_sub(1)
        {
            return Err(CdfError::destination(format!(
                "direct ClickHouse verification did not observe the exact canonical ordinal set for {rows} rows"
            )));
        }
        black_box(verification);
        Ok(())
    })?;
    worker_measurement(elapsed_ns(started), rows)
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

fn add_package_hash(
    batches: Vec<RecordBatch>,
    package_hash: &PackageHash,
) -> Result<Vec<RecordBatch>> {
    let text = package_hash
        .as_str()
        .strip_prefix("sha256:")
        .unwrap_or(package_hash.as_str());
    let digest = hex::decode(text)
        .map_err(|_| CdfError::data("ClickHouse roofline package hash is not hexadecimal"))?;
    if digest.len() != 32 {
        return Err(CdfError::data(
            "ClickHouse destination roofline package hash is not SHA-256",
        ));
    }
    batches
        .into_iter()
        .map(|batch| {
            let mut values = Vec::with_capacity(batch.num_rows().saturating_mul(32));
            for _ in 0..batch.num_rows() {
                values.extend_from_slice(&digest);
            }
            let hashes = FixedSizeBinaryArray::try_new(32, Buffer::from(values), None)?;
            let schema = batch.schema();
            let (ordinal_field, logical_fields) = schema
                .fields()
                .split_last()
                .ok_or_else(|| CdfError::data("canonical roofline batch omitted its ordinal"))?;
            let mut fields = logical_fields.to_vec();
            fields
                .push(Field::new("_cdf_package_hash", DataType::FixedSizeBinary(32), false).into());
            fields.push(ordinal_field.clone());
            let mut columns = batch.columns()[..batch.num_columns().saturating_sub(1)].to_vec();
            columns.push(Arc::new(hashes) as ArrayRef);
            columns.push(
                batch
                    .columns()
                    .last()
                    .cloned()
                    .ok_or_else(|| CdfError::data("canonical roofline batch lost its ordinal"))?,
            );
            Ok(RecordBatch::try_new(
                Arc::new(Schema::new(fields)),
                columns,
            )?)
        })
        .collect()
}

fn replay_inputs(
    rows: u64,
    disposition: WriteDisposition,
) -> cdf_bench_core::BenchResult<(PackageReplayInputs, StateSegment)> {
    let package_hash = PackageHash::new(format!("sha256:{}", "a".repeat(64)))?;
    let schema_hash = SchemaHash::new("schema-clickhouse-destination-roofline")?;
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
        target: TargetName::new(TARGET)?,
        disposition: disposition.clone(),
        segments: vec![state.clone()],
        idempotency_token: IdempotencyToken::new(package_hash.as_str())?,
    };
    Ok((
        PackageReplayInputs {
            input_checkpoint: None,
            state_delta: StateDelta {
                checkpoint_id: CheckpointId::new("checkpoint-clickhouse-destination-roofline")?,
                pipeline_id: PipelineId::new("pipeline-clickhouse-destination-roofline")?,
                resource_id: ResourceId::new("resource-clickhouse-destination-roofline")?,
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
            merge_keys: if disposition == WriteDisposition::Merge {
                vec!["id".to_owned()]
            } else {
                Vec::new()
            },
            schema_hash,
            destination_policy: if disposition == WriteDisposition::Merge {
                [("merge_mode".to_owned(), "replacing_merge_tree".to_owned())]
                    .into_iter()
                    .collect()
            } else {
                Default::default()
            },
            run_schema_authority: None,
        },
        state,
    ))
}

struct RooflineConnection {
    destination_uri: String,
    http_endpoint: String,
}

fn roofline_connection(endpoint: &str) -> cdf_bench_core::BenchResult<RooflineConnection> {
    let parsed = Url::parse(endpoint)?;
    if !matches!(parsed.scheme(), "clickhouse" | "clickhouses")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !matches!(parsed.path(), "" | "/")
    {
        return Err(bench_error(
            "CDF_CLICKHOUSE_ENDPOINT must be clickhouse://host:port with no credentials, path, query, or fragment",
        ));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| bench_error("parsed ClickHouse endpoint lost its host"))?;
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    let authority = parsed
        .port()
        .map_or(host.clone(), |port| format!("{host}:{port}"));
    let transport = if parsed.scheme() == "clickhouses" {
        "https"
    } else {
        "http"
    };
    Ok(RooflineConnection {
        destination_uri: format!("{}://{authority}/{DATABASE}", parsed.scheme()),
        http_endpoint: format!("{transport}://{authority}"),
    })
}

fn roofline_client(endpoint: &str) -> clickhouse::Client {
    clickhouse::Client::default()
        .with_url(endpoint)
        .with_compression(Compression::Lz4)
        .with_setting("async_insert", "0")
        .with_setting("wait_for_async_insert", "1")
}

fn setup_database(
    execution: &cdf_runtime::ExecutionServices,
    endpoint: &str,
    disposition: &WriteDisposition,
    rows: u64,
) -> Result<()> {
    let client = roofline_client(endpoint);
    let database = client.clone().with_database(DATABASE);
    let seed_rows = if disposition == &WriteDisposition::Merge {
        rows / 10
    } else {
        0
    };
    let engine = if disposition == &WriteDisposition::Merge {
        "ReplacingMergeTree"
    } else {
        "MergeTree"
    };
    execution.run_io(async move {
        client
            .query(&format!("CREATE DATABASE IF NOT EXISTS {DATABASE} ENGINE = Atomic"))
            .execute()
            .await
            .map_err(|_| CdfError::environment("open ClickHouse destination roofline database"))?;
        database
            .query("DROP TABLE IF EXISTS events SYNC")
            .execute()
            .await
            .map_err(|_| CdfError::environment("reset ClickHouse destination roofline target"))?;
        database
            .query(&format!(
                "CREATE TABLE events (id Int64, metric Int64, updated_at Int64, _cdf_package_hash FixedString(32), _cdf_package_row_ord UInt64) ENGINE = {engine} ORDER BY id SETTINGS non_replicated_deduplication_window = 100000"
            ))
            .execute()
            .await
            .map_err(|_| CdfError::environment("create ClickHouse destination roofline target"))?;
        for table in ["events", "_cdf_loads", "_cdf_segments", "_cdf_state"] {
            database
                .query(&format!("TRUNCATE TABLE IF EXISTS `{table}` SYNC"))
                .execute()
                .await
                .map_err(|_| {
                    CdfError::environment("truncate ClickHouse destination roofline table")
                })?;
        }
        if seed_rows > 0 {
            database
                .query(&format!(
                    "INSERT INTO events SELECT toInt64(number), toInt64(-1), toInt64(-1), unhex('{}'), number FROM numbers({seed_rows})",
                    "bb".repeat(32)
                ))
                .with_setting("insert_deduplication_token", "clickhouse-roofline-seed")
                .execute()
                .await
                .map_err(|_| CdfError::environment("seed ClickHouse destination roofline target"))?;
        }
        Ok(())
    })
}

fn initialize_database(execution: &cdf_runtime::ExecutionServices, endpoint: &str) -> Result<()> {
    let client = roofline_client(endpoint);
    execution.run_io(async move {
        client
            .query(&format!("DROP DATABASE IF EXISTS {DATABASE} SYNC"))
            .execute()
            .await
            .map_err(|_| {
                CdfError::environment("initialize ClickHouse destination roofline database")
            })?;
        client
            .query(&format!("CREATE DATABASE {DATABASE} ENGINE = Atomic"))
            .execute()
            .await
            .map_err(|_| {
                CdfError::environment("initialize ClickHouse destination roofline database")
            })
    })
}

fn response_limits() -> ResponseLimits {
    let response = NonZeroUsize::new(4 * 1024 * 1024).unwrap_or(NonZeroUsize::MIN);
    let chunk = NonZeroUsize::new(1024 * 1024).unwrap_or(NonZeroUsize::MIN);
    ResponseLimits::new(response, response, chunk, response)
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
        .ok_or_else(|| bench_error("ClickHouse destination worker stdout pipe is absent"))?
        .read_to_end(&mut stdout)?;
    child
        .stderr
        .take()
        .ok_or_else(|| bench_error("ClickHouse destination worker stderr pipe is absent"))?
        .read_to_end(&mut stderr)?;
    if !status.success() {
        return Ok(ChildObservationStatus::Failed {
            exit_code: status.code(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        });
    }
    let measurement: ClickHouseWorkerMeasurement =
        serde_json::from_slice(&stdout).map_err(|error| {
            bench_error(format!(
                "ClickHouse destination worker emitted invalid measurement JSON: {error}"
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
) -> cdf_bench_core::BenchResult<ClickHouseWorkerMeasurement> {
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
            .ok_or_else(|| bench_error("ClickHouse destination worker peak RSS overflowed"))?
    };
    Ok(ClickHouseWorkerMeasurement {
        workload: WorkerMeasurement {
            timed_wall_time_ns: Some(elapsed),
            rows,
            logical_bytes,
            physical_bytes: 0,
            spill_bytes: 0,
            phases: vec![PhaseMetric {
                phase: "arrowstream_delivery_commit_and_verification".to_owned(),
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
            "ClickHouse destination roofline could not resolve the base Git revision",
        ));
    }
    let revision = String::from_utf8(output.stdout)?;
    let revision = revision.trim();
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(bench_error(
            "ClickHouse destination roofline resolved an invalid base Git revision",
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
        "crates/cdf-benchmarks/src/bin/clickhouse-destination-roofline.rs",
        "crates/cdf-benchmarks/src/clickhouse_destination_roofline.rs",
        "crates/cdf-dest-clickhouse/Cargo.toml",
        "crates/cdf-dest-clickhouse/src/client.rs",
        "crates/cdf-dest-clickhouse/src/error.rs",
        "crates/cdf-dest-clickhouse/src/identifier.rs",
        "crates/cdf-dest-clickhouse/src/lib.rs",
        "crates/cdf-dest-clickhouse/src/mapping.rs",
        "crates/cdf-dest-clickhouse/src/models.rs",
        "crates/cdf-dest-clickhouse/src/package.rs",
        "crates/cdf-dest-clickhouse/src/plan.rs",
        "crates/cdf-dest-clickhouse/src/receipt.rs",
        "crates/cdf-dest-clickhouse/src/runtime.rs",
        "crates/cdf-dest-clickhouse/src/session.rs",
        "crates/cdf-dest-clickhouse/src/sheet.rs",
        "third-party/clickhouse-0.15.1-cdf/src/lib.rs",
        "third-party/clickhouse-ext-arrow-0.1.0-cdf/src/lib.rs",
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
