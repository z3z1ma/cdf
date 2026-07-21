use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_declarative::{
    CompiledResource, compile_document, compile_document_with_project_root, parse_toml,
};
use cdf_dest_postgres::{MergeDedupPolicy, PostgresTarget};
use cdf_engine::{
    EngineExecutionConfig, EnginePackageDraft, EnginePlanInput, Planner, execute_to_package,
    execute_to_package_with_segment_positions_and_pre_finalize,
};
use cdf_kernel::ExecutionExtent;
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, CheckpointId, CursorPosition, CursorValue, PipelineId,
    PredicateId, QueryableResource, ResourceId, ResourceStream, RunId, ScanPredicate, ScanRequest,
    ScopeKey, ScopeLeaseStore, SegmentId, SourcePosition, StateSegment, TargetName,
    WriteDisposition, canonical_arrow_schema_hash,
};
use cdf_object_access::{FileTransportFacade, ObjectStoreClientPool};
use cdf_package::{PackageBuilder, PackageReader, persist_package_parquet_archive};
use cdf_package_contract::{DestinationCommitPlanPreimage, PackageStatus, StateDeltaPreimage};
use cdf_project::{
    EnvSecretProvider, PackageArtifactReplayRequest, ProjectRunRequest, ProjectRunSource,
    ResolvedProjectDestination, RunTelemetryConfig, replay_package_from_artifacts, run_project,
    run_project_with_scheduler_and_telemetry,
};
use cdf_runtime::{
    ByteTransformRegistry, FormatRegistry, SourceIoControllerReport, SourceRegistry,
    SourceResolutionContext,
};
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver, file_source_blocking_lane};
use cdf_source_iceberg::{
    AwsGlueCatalogClient, IcebergRuntimeDependencies, IcebergSourceDriver,
    UnsupportedGlueCatalogClient,
};
use cdf_state_sqlite::InMemoryCheckpointStore;
use cdf_transport_http::ReqwestHttpProvider;
use datafusion::prelude::{SessionContext, col, lit};
use duckdb::{Connection, appender_params_from_iter, types::Value};
use futures_executor::block_on;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BenchResult, PhaseMetric, WorkerMeasurement, bench_error,
    catalog::{FixtureSpec, fixture_spec, validate_spec},
    fixtures::{
        active_for_id, arrow_filter_project, category_for_id, record_batch_range,
        record_batches_for_spec, rest_fixture_body, startup_ndjson, write_local_fixture_file,
    },
    matrix::{CaseDefinition, CaseKind, CaseOutcome, LocalFormat, ReplayDestination},
    resource::{FixtureTransport, MemoryResource},
};

const POSTGRES_URL_ENV: &str = "CDF_BENCH_POSTGRES_URL";
const BENCHMARK_MANAGED_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
static POSTGRES_PACKAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
struct WorkMetric {
    rows: u64,
    bytes: u64,
}

struct PackageFixture {
    package_dir: PathBuf,
}

struct BenchmarkFileSource {
    resource: Arc<dyn QueryableResource>,
    source_plan: cdf_runtime::CompiledSourcePlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparedFileFormat {
    Csv,
    Json,
    Ndjson,
    Parquet,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedFilePackageWorkload {
    pub fixture_name: String,
    pub source_root: PathBuf,
    pub glob: String,
    pub package_dir: PathBuf,
    pub format: PreparedFileFormat,
    pub jobs: Option<u16>,
    pub execution_host_jobs: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedSourcePackageRun {
    #[serde(flatten)]
    pub measurement: WorkerMeasurement,
    pub configured_jobs: Option<u16>,
    pub effective_jobs: u16,
    pub limiting_factors: Vec<String>,
    pub partition_count: u64,
    pub planned_source_bytes: Option<u64>,
    pub package_hash: String,
    pub segments: Vec<cdf_package_contract::SegmentEntry>,
    /// Identity-bearing package artifacts other than canonical data segments.
    ///
    /// The lab exposes these digests so jobs/retry/repeat comparisons can distinguish a
    /// data-plane determinism failure from drift in recorded plan or evidence artifacts without
    /// retaining multi-gigabyte benchmark packages.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identity_artifacts: Vec<cdf_package_contract::FileEntry>,
    pub runtime_scheduler: cdf_runtime::RuntimeSchedulerReport,
    pub source_frontier: cdf_runtime::SourceFrontierReport,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_io_stages: Vec<PreparedSourceIoStage>,
}

fn engine_source_physical_bytes(metrics: &[cdf_kernel::RunPhaseMetric]) -> BenchResult<u64> {
    metrics
        .iter()
        .filter(|metric| metric.phase == cdf_kernel::RunPhase::SourceRead)
        .try_fold(0_u64, |total, metric| {
            total
                .checked_add(metric.input_bytes)
                .ok_or_else(|| bench_error("observed source physical bytes exceed u64"))
        })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PreparedIcebergCatalog {
    Filesystem {
        warehouse: PathBuf,
    },
    Glue {
        region: String,
        #[serde(default)]
        catalog_id: Option<String>,
        #[serde(default)]
        catalog_credentials: Option<String>,
        #[serde(default)]
        object_credentials: Option<String>,
        egress_allowlist: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedIcebergPackageWorkload {
    pub project_root: PathBuf,
    pub catalog: PreparedIcebergCatalog,
    pub namespace: Vec<String>,
    pub table: String,
    #[serde(default)]
    pub projection: Option<Vec<String>>,
    pub package_dir: PathBuf,
    /// Retain the completed package at `package_dir` for artifact-level diagnostics.
    /// Ordinary timed workers leave this false and use an automatically cleaned child directory.
    #[serde(default)]
    pub retain_package: bool,
    pub maximum_metadata_bytes: u64,
    #[serde(default)]
    pub maximum_concurrency: Option<u16>,
    #[serde(default)]
    pub parquet_batch_rows: Option<usize>,
    #[serde(default)]
    pub maximum_batch_bytes: Option<u64>,
    #[serde(default)]
    pub parquet_whole_object_prefetch_bytes: Option<u64>,
    pub jobs: Option<u16>,
    pub execution_host_jobs: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedSourceIoStage {
    pub stage: String,
    pub wall_duration_ns: u64,
    pub request_duration_ns: u64,
    pub physical_bytes: u64,
    pub requests: u64,
    pub queue_wait_ns: u64,
    /// Maximum controller concurrency observed through the end of this stage.
    pub peak_active_through_stage: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparedDestinationKind {
    DuckDb,
    Parquet,
    Postgres {
        database_url: String,
        schema: Option<String>,
        table: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedFileDestinationWorkload {
    pub fixture_name: String,
    pub source_root: PathBuf,
    pub glob: String,
    pub format: PreparedFileFormat,
    pub output_root: PathBuf,
    pub destination: PreparedDestinationKind,
    pub jobs: Option<u16>,
    pub execution_host_jobs: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedFileDestinationRun {
    pub configured_jobs: Option<u16>,
    pub effective_jobs: u16,
    pub limiting_factors: Vec<String>,
    pub partition_count: u64,
    pub package_hash: String,
    pub receipt_package_hash: String,
    pub receipt_segment_ids: Vec<String>,
    pub state_segment_ids: Vec<String>,
    /// Destination-neutral receipt semantics with wall-clock and transport-generation fields
    /// removed. This is the identity-bearing projection expected to survive scheduler changes.
    pub logical_receipt: serde_json::Value,
    /// Canonical digest of the logical Parquet manifest, when the destination publishes one.
    pub logical_manifest_sha256: Option<String>,
    /// The full logical manifest projection keeps object keys, content hashes, segment offsets,
    /// schema, disposition, and counts available to determinism assertions.
    pub logical_manifest: Option<serde_json::Value>,
    pub row_count: u64,
    pub runtime_scheduler: cdf_runtime::RuntimeSchedulerReport,
    pub source_frontier: cdf_runtime::SourceFrontierReport,
}

fn logical_destination_evidence(
    receipt: &cdf_kernel::Receipt,
    destination: &PreparedDestinationKind,
    output_root: &Path,
) -> BenchResult<(serde_json::Value, Option<String>, Option<serde_json::Value>)> {
    let mut logical_receipt = serde_json::to_value(receipt)?;
    let receipt_object = logical_receipt
        .as_object_mut()
        .ok_or_else(|| bench_error("destination receipt did not serialize as an object"))?;
    receipt_object.remove("committed_at_ms");
    if let Some(values) = receipt_object
        .get_mut("transaction")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|transaction| transaction.get_mut("values"))
        .and_then(serde_json::Value::as_object_mut)
    {
        values.remove("manifest_etag");
        values.remove("manifest_sha256");
        values.remove("replace_pointer_etag");
        values.remove("replace_pointer_sha256");
        values.remove("database_path");
        values.remove("writer_lock");
        values.remove("xid");
    }
    if let Some(parameters) = receipt_object
        .get_mut("verify")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|verify| verify.get_mut("parameters"))
        .and_then(serde_json::Value::as_object_mut)
    {
        parameters.remove("manifest_sha256");
    }

    let PreparedDestinationKind::Parquet = destination else {
        return Ok((logical_receipt, None, None));
    };
    let manifest_key = receipt
        .verify
        .parameters
        .get("manifest_key")
        .ok_or_else(|| bench_error("Parquet receipt omitted manifest_key"))?;
    let destination_root = output_root.join("parquet");
    let manifest_path = destination_root.join(manifest_key);
    let manifest_bytes = match fs::read(&manifest_path) {
        Ok(bytes) => bytes,
        Err(direct_error) => {
            // Local object stores are free to encode key components in their filesystem view.
            // The benchmark inspects the published artifact, not that private mapping.
            let mut candidates = Vec::new();
            collect_named_files(&destination_root, "manifest.json", &mut candidates)?;
            candidates.sort();
            let mut matching = Vec::new();
            for candidate in candidates {
                let bytes = fs::read(&candidate)?;
                let value: serde_json::Value = serde_json::from_slice(&bytes)?;
                if value
                    .get("package_hash")
                    .and_then(serde_json::Value::as_str)
                    == Some(receipt.package_hash.as_str())
                {
                    matching.push((candidate, bytes));
                }
            }
            if matching.is_empty() {
                return Err(bench_error(format!(
                    "read Parquet manifest {}: {direct_error}; found no matching published manifest under {}",
                    manifest_path.display(),
                    destination_root.display()
                )));
            }
            let canonical = &matching[0].1;
            if matching.iter().any(|(_, bytes)| bytes != canonical) {
                return Err(bench_error(format!(
                    "Parquet package and provenance manifests disagree under {}",
                    destination_root.display()
                )));
            }
            matching.remove(0).1
        }
    };
    let mut logical_manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)?;
    let manifest_object = logical_manifest
        .as_object_mut()
        .ok_or_else(|| bench_error("Parquet manifest did not serialize as an object"))?;
    manifest_object.remove("committed_at_ms");
    let objects = manifest_object
        .get_mut("objects")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| bench_error("Parquet manifest omitted its object list"))?;
    for object in objects {
        object
            .as_object_mut()
            .ok_or_else(|| bench_error("Parquet manifest object was not an object"))?
            .remove("etag");
    }
    let logical_manifest_sha256 = format!(
        "sha256:{:x}",
        Sha256::digest(cdf_package::canonical_json_bytes(&logical_manifest)?)
    );
    Ok((
        logical_receipt,
        Some(logical_manifest_sha256),
        Some(logical_manifest),
    ))
}

fn collect_named_files(root: &Path, name: &str, output: &mut Vec<PathBuf>) -> BenchResult<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_named_files(&path, name, output)?;
        } else if entry.file_name() == name {
            output.push(path);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupControlWorkload {
    pub case_label: String,
    pub output_root: PathBuf,
}

fn benchmark_file_resource(
    source_root: &Path,
    glob: &str,
    format_id: &str,
    spec: &FixtureSpec,
    execution: &cdf_runtime::ExecutionServices,
) -> BenchResult<BenchmarkFileSource> {
    let fields = benchmark_schema_fields(spec);
    let document = parse_toml(&format!(
        r#"
[source.bench]
kind = "files"
root = "."

[resource.orders]
glob = {}
format = {}
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
{}
] }}
"#,
        serde_json::to_string(glob)?,
        serde_json::to_string(format_id)?,
        fields.join(",\n")
    ))?;
    let registry = benchmark_source_registry()?;
    let compiled = compile_document_with_project_root(&registry, &document, source_root)?
        .into_iter()
        .next()
        .ok_or_else(|| bench_error("benchmark file declaration compiled no resource"))?;
    resolve_benchmark_file_resource(compiled, &registry, source_root, execution)
}

fn resolve_benchmark_file_resource(
    compiled: CompiledResource,
    registry: &SourceRegistry,
    project_root: &Path,
    execution: &cdf_runtime::ExecutionServices,
) -> BenchResult<BenchmarkFileSource> {
    let secrets = Arc::new(EnvSecretProvider::from_map(
        std::iter::empty::<(&str, &str)>(),
    ));
    let source_plan = compiled.source_plan().clone();
    let resolution = SourceResolutionContext::new(
        project_root,
        secrets,
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let resource = registry.resolve(&source_plan, &resolution)?;
    Ok(BenchmarkFileSource {
        resource,
        source_plan,
    })
}

fn benchmark_schema_fields(spec: &FixtureSpec) -> Vec<String> {
    let mut fields = vec![
        r#"  { name = "id", type = "int64", nullable = false }"#.to_owned(),
        r#"  { name = "active", type = "boolean", nullable = false }"#.to_owned(),
        r#"  { name = "category", type = "string", nullable = false }"#.to_owned(),
        r#"  { name = "amount", type = "float64", nullable = false }"#.to_owned(),
    ];
    fields.extend((0..spec.wide_columns).map(|column| {
        format!(r#"  {{ name = "metric_{column:03}", type = "int64", nullable = false }}"#)
    }));
    fields
}

fn benchmark_format_registry() -> BenchResult<Arc<FormatRegistry>> {
    let mut formats = FormatRegistry::default();
    formats.register(Arc::new(cdf_format_delimited::CsvFormatDriver::new()?))?;
    formats.register(Arc::new(cdf_format_delimited::DelimitedFormatDriver::tsv()?))?;
    formats.register(Arc::new(cdf_format_delimited::DelimitedFormatDriver::psv()?))?;
    formats.register(Arc::new(
        cdf_format_delimited::DelimitedFormatDriver::custom()?,
    ))?;
    formats.register(Arc::new(
        cdf_format_delimited::FixedWidthFormatDriver::new()?
    ))?;
    formats.register(Arc::new(cdf_format_json::NdjsonFormatDriver::new()?))?;
    formats.register(Arc::new(cdf_format_json::JsonDocumentFormatDriver::new()?))?;
    formats.register(Arc::new(cdf_format_parquet::ParquetFormatDriver::new()?))?;
    Ok(Arc::new(formats))
}

fn benchmark_source_registry() -> BenchResult<SourceRegistry> {
    let mut registry = SourceRegistry::new();
    let formats = benchmark_format_registry()?;
    let runtime_formats = Arc::clone(&formats);
    registry.register(FileSourceDriver::new(
        formats,
        move |secrets, execution, egress| {
            Ok(FileRuntimeDependencies::new(
                FileTransportFacade::new()
                    .with_shared_secret_provider(secrets)
                    .with_execution_services(execution.clone())
                    .with_local_listing_lane(file_source_blocking_lane())?,
                execution,
                Arc::clone(&runtime_formats),
                Arc::new(ByteTransformRegistry::default()),
                egress,
            ))
        },
    )?)?;
    Ok(registry)
}

fn benchmark_execution_services(host_jobs: u16) -> BenchResult<cdf_runtime::ExecutionServices> {
    if host_jobs == 0 {
        return Err(bench_error(
            "prepared workload execution_host_jobs must be nonzero",
        ));
    }
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> =
        Arc::new(cdf_memory::DeterministicMemoryCoordinator::new(
            BENCHMARK_MANAGED_MEMORY_BYTES,
            BTreeMap::new(),
        )?);
    let host = Arc::new(cdf_engine::StandaloneExecutionHost::new(
        cdf_runtime::ExecutionHostCapabilities {
            logical_cpu_slots: host_jobs,
            io_workers: host_jobs.min(4),
            blocking_lanes: Vec::new(),
        },
        memory,
    )?);
    cdf_runtime::ExecutionServices::new(host).map_err(Into::into)
}

fn benchmark_replay_execution_services(
    host_jobs: u16,
) -> BenchResult<cdf_runtime::ExecutionServices> {
    let services = benchmark_execution_services(host_jobs)?;
    let scopes: Arc<dyn ScopeLeaseStore> =
        Arc::new(cdf_state_sqlite::InMemoryScopeLeaseStore::new());
    services
        .with_staging_lease_authority(Arc::new(cdf_runtime::ScopeStagingLeaseAuthority::new(
            scopes,
        )))
        .map_err(Into::into)
}

fn available_host_jobs() -> u16 {
    std::thread::available_parallelism()
        .map(|jobs| u16::try_from(jobs.get()).unwrap_or(u16::MAX))
        .unwrap_or(1)
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn source_io_stage(
    stage: &str,
    wall_duration_ns: u64,
    before: &SourceIoControllerReport,
    after: &SourceIoControllerReport,
) -> PreparedSourceIoStage {
    PreparedSourceIoStage {
        stage: stage.to_owned(),
        wall_duration_ns,
        request_duration_ns: after
            .request_duration_ns
            .saturating_sub(before.request_duration_ns),
        physical_bytes: after.physical_bytes.saturating_sub(before.physical_bytes),
        requests: after
            .acquired_requests
            .saturating_sub(before.acquired_requests),
        queue_wait_ns: after.queue_wait_ns.saturating_sub(before.queue_wait_ns),
        peak_active_through_stage: after.peak_active,
    }
}

pub fn run_startup_control_workload(
    request: &StartupControlWorkload,
) -> BenchResult<WorkerMeasurement> {
    let case = crate::benchmark_cases()
        .iter()
        .find(|case| case.label == request.case_label)
        .ok_or_else(|| {
            bench_error(format!(
                "unknown startup-control benchmark case `{}`",
                request.case_label
            ))
        })?;
    let output = run_case(case, &request.output_root)?;
    Ok(WorkerMeasurement {
        timed_wall_time_ns: None,
        rows: output.rows,
        logical_bytes: output.bytes,
        physical_bytes: output.bytes,
        spill_bytes: 0,
        phases: Vec::new(),
    })
}

pub fn run_prepared_file_to_package(
    request: &PreparedFilePackageWorkload,
) -> BenchResult<PreparedSourcePackageRun> {
    if request.jobs == Some(0) {
        return Err(bench_error("prepared file workload jobs must be nonzero"));
    }
    let spec = fixture_spec(&request.fixture_name)?;
    validate_spec(&spec)?;
    let format_id = match request.format {
        PreparedFileFormat::Csv => "csv",
        PreparedFileFormat::Json => "json",
        PreparedFileFormat::Ndjson => "ndjson",
        PreparedFileFormat::Parquet => "parquet",
    };
    let execution = benchmark_execution_services(request.execution_host_jobs)?;
    let host_jobs = execution.capabilities().logical_cpu_slots;
    let execution = execution
        .with_run_job_ceiling(request.jobs.unwrap_or(host_jobs))?
        .with_scheduler_measurement(true)?;
    let source = benchmark_file_resource(
        &request.source_root,
        &request.glob,
        format_id,
        &spec,
        &execution,
    )?;
    let plan = identity_queryable_engine_plan(source.resource.as_ref(), "pkg-p3-prepared")?
        .bind_compiled_source(&source.source_plan)?
        .bind_operator_graph(
            &source.source_plan,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )?;
    let source_execution = plan.compiled_source_execution.as_ref().ok_or_else(|| {
        bench_error("prepared file plan omitted its compiled source execution authority")
    })?;
    let partition_count = plan.scan.partition_count()?;
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        partition_count,
        source_execution.execution_capabilities(),
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &execution,
        request.jobs,
    )?;
    execution.tighten_run_job_ceiling(scheduler.effective_jobs.jobs)?;
    let planned_source_bytes = plan.scan.planned_source_bytes.map(|bytes| bytes.get());
    let pre_finalize = |_builder: &PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        source.resource.as_ref(),
        &request.package_dir,
        &pre_finalize,
        EngineExecutionConfig::default()
            .with_phase_metrics(true)
            .with_execution_services(execution.clone())
            .with_scheduler_resolution(scheduler.clone())
            .new_invocation(),
    ))?;
    let physical_bytes = engine_source_physical_bytes(&output.phase_metrics)?;
    let (segments, identity_artifacts) = package_identity_parts(&output.output)?;
    Ok(PreparedSourcePackageRun {
        measurement: WorkerMeasurement {
            timed_wall_time_ns: None,
            rows: output.output.profile.output_rows,
            logical_bytes: output.output.profile.output_bytes,
            physical_bytes,
            spill_bytes: 0,
            phases: output
                .phase_metrics
                .into_iter()
                .map(|metric| PhaseMetric {
                    phase: metric.phase.as_str().to_owned(),
                    duration_ns: metric.duration_ns,
                    bytes: metric.output_bytes.max(metric.input_bytes),
                })
                .collect(),
        },
        configured_jobs: request.jobs,
        effective_jobs: scheduler.effective_jobs.jobs,
        limiting_factors: scheduler.effective_jobs.limiting_factors,
        partition_count,
        planned_source_bytes,
        package_hash: output.output.manifest.package_hash,
        segments,
        identity_artifacts,
        runtime_scheduler: execution.scheduler_report()?,
        source_frontier: output.source_frontier,
        source_io_stages: Vec::new(),
    })
}

pub fn run_prepared_iceberg_to_package(
    request: &PreparedIcebergPackageWorkload,
) -> BenchResult<PreparedSourcePackageRun> {
    if request.namespace.is_empty() || request.namespace.iter().any(String::is_empty) {
        return Err(bench_error(
            "prepared Iceberg workload namespace must contain nonempty components",
        ));
    }
    if request.table.is_empty() {
        return Err(bench_error(
            "prepared Iceberg workload table must be nonempty",
        ));
    }
    if request.maximum_metadata_bytes == 0 {
        return Err(bench_error(
            "prepared Iceberg workload metadata budget must be nonzero",
        ));
    }
    if request.jobs == Some(0) {
        return Err(bench_error(
            "prepared Iceberg workload jobs must be nonzero",
        ));
    }
    if request.maximum_concurrency == Some(0) {
        return Err(bench_error(
            "prepared Iceberg workload maximum_concurrency must be nonzero",
        ));
    }
    if request.parquet_batch_rows == Some(0) || request.maximum_batch_bytes == Some(0) {
        return Err(bench_error(
            "prepared Iceberg workload batch rows and bytes must be nonzero",
        ));
    }
    fs::create_dir_all(&request.project_root)?;
    let execution = benchmark_execution_services(request.execution_host_jobs)?;
    let host_jobs = execution.capabilities().logical_cpu_slots;
    let execution = execution
        .with_run_job_ceiling(request.jobs.unwrap_or(host_jobs))?
        .with_scheduler_measurement(true)?;
    let registry = prepared_iceberg_registry(&request.catalog)?;
    let document = parse_toml(&prepared_iceberg_declaration(request)?)?;
    let compiled = compile_document_with_project_root(&registry, &document, &request.project_root)?
        .into_iter()
        .next()
        .ok_or_else(|| bench_error("prepared Iceberg declaration compiled no resource"))?;
    let mut source_plan = compiled.source_plan().clone();
    let secrets = Arc::new(EnvSecretProvider::process());
    let resolution = SourceResolutionContext::new(
        &request.project_root,
        secrets,
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let initial_io = execution.scheduler_report()?.source_io_controller;
    let discovery_started = Instant::now();
    let discovery = registry.discovery_session(&source_plan, &resolution)?;
    let mut candidates = discovery.candidates()?;
    if candidates.len() != 1 {
        return Err(bench_error(format!(
            "prepared Iceberg discovery produced {} candidates instead of one",
            candidates.len()
        )));
    }
    let observation = discovery.observe(
        &candidates.remove(0),
        &cdf_runtime::SourceDiscoveryRequest::new(request.maximum_metadata_bytes, 1)?,
    )?;
    let discovery_duration_ns = elapsed_ns(discovery_started);
    let discovery_bytes = observation.bytes_read;
    let discovery_io = execution.scheduler_report()?.source_io_controller;
    source_plan.schema = observation.schema.as_ref().clone();

    let resolve_started = Instant::now();
    let resource = registry.resolve(&source_plan, &resolution)?;
    let resolve_duration_ns = elapsed_ns(resolve_started);

    let planning_started = Instant::now();
    let plan = Planner::new()
        .plan_tier_b(
            resource.as_ref(),
            EnginePlanInput {
                request: ScanRequest {
                    resource_id: resource.descriptor().resource_id.clone(),
                    projection: request.projection.clone(),
                    filters: Vec::new(),
                    limit: None,
                    order_by: Vec::new(),
                    scope: resource.descriptor().state_scope.clone(),
                },
                validation_program: compile_validation_program(
                    &ContractPolicy::for_trust(resource.descriptor().trust_level.clone()),
                    &ObservedSchema::from_arrow(resource.schema().as_ref()),
                )?,
                execution_extent: ExecutionExtent::bounded(),
                package_id: "pkg-p3-iceberg-prepared".to_owned(),
            },
        )?
        .bind_compiled_source(&source_plan)?
        .bind_operator_graph(
            &source_plan,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )?;
    let planning_duration_ns = elapsed_ns(planning_started);
    let planning_io = execution.scheduler_report()?.source_io_controller;
    let source_execution = plan.compiled_source_execution.as_ref().ok_or_else(|| {
        bench_error("prepared Iceberg plan omitted compiled source execution authority")
    })?;
    let partition_count = plan.scan.partition_count()?;
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        partition_count,
        source_execution.execution_capabilities(),
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &execution,
        request.jobs,
    )?;
    execution.tighten_run_job_ceiling(scheduler.effective_jobs.jobs)?;
    let planned_source_bytes = plan.scan.planned_source_bytes.map(|bytes| bytes.get());
    let execution_started = Instant::now();
    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        resource.as_ref(),
        &request.package_dir,
        &|_builder: &PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(()),
        EngineExecutionConfig::default()
            .with_phase_metrics(true)
            .with_execution_services(execution.clone())
            .with_scheduler_resolution(scheduler.clone())
            .new_invocation(),
    ))?;
    let execution_duration_ns = elapsed_ns(execution_started);
    let runtime_scheduler = execution.scheduler_report()?;
    let physical_bytes = runtime_scheduler
        .source_io_controller
        .physical_bytes
        .saturating_sub(initial_io.physical_bytes);
    let mut phases = vec![
        PhaseMetric {
            phase: "iceberg.discovery".to_owned(),
            duration_ns: discovery_duration_ns,
            bytes: discovery_bytes,
        },
        PhaseMetric {
            phase: "iceberg.resolve".to_owned(),
            duration_ns: resolve_duration_ns,
            bytes: 0,
        },
        PhaseMetric {
            phase: "iceberg.plan".to_owned(),
            duration_ns: planning_duration_ns,
            bytes: planning_io
                .physical_bytes
                .saturating_sub(discovery_io.physical_bytes),
        },
    ];
    phases.extend(output.phase_metrics.into_iter().map(|metric| PhaseMetric {
        phase: metric.phase.as_str().to_owned(),
        duration_ns: metric.duration_ns,
        bytes: metric.output_bytes.max(metric.input_bytes),
    }));
    let source_io_stages = vec![
        source_io_stage(
            "iceberg.discovery",
            discovery_duration_ns,
            &initial_io,
            &discovery_io,
        ),
        source_io_stage(
            "iceberg.plan",
            planning_duration_ns,
            &discovery_io,
            &planning_io,
        ),
        source_io_stage(
            "iceberg.execute",
            execution_duration_ns,
            &planning_io,
            &runtime_scheduler.source_io_controller,
        ),
    ];
    let (segments, identity_artifacts) = package_identity_parts(&output.output)?;
    Ok(PreparedSourcePackageRun {
        measurement: WorkerMeasurement {
            timed_wall_time_ns: None,
            rows: output.output.profile.output_rows,
            logical_bytes: output.output.profile.output_bytes,
            physical_bytes,
            spill_bytes: 0,
            phases,
        },
        configured_jobs: request.jobs,
        effective_jobs: scheduler.effective_jobs.jobs,
        limiting_factors: scheduler.effective_jobs.limiting_factors,
        partition_count,
        planned_source_bytes,
        package_hash: output.output.manifest.package_hash,
        segments,
        identity_artifacts,
        runtime_scheduler,
        source_frontier: output.source_frontier,
        source_io_stages,
    })
}

fn package_identity_parts(
    output: &cdf_engine::EngineRunOutput,
) -> BenchResult<(
    Vec<cdf_package_contract::SegmentEntry>,
    Vec<cdf_package_contract::FileEntry>,
)> {
    let mut segments = Vec::new();
    let mut segment_paths = BTreeSet::new();
    output.for_each_identity_segment(&mut |segment| {
        segment_paths.insert(segment.path.clone());
        segments.push(segment);
        Ok(())
    })?;
    let mut identity_artifacts = Vec::new();
    output.for_each_identity_file(&mut |entry| {
        if !segment_paths.contains(&entry.path) {
            identity_artifacts.push(entry);
        }
        Ok(())
    })?;
    Ok((segments, identity_artifacts))
}

fn prepared_iceberg_registry(catalog: &PreparedIcebergCatalog) -> BenchResult<SourceRegistry> {
    let mut registry = SourceRegistry::new();
    match catalog {
        PreparedIcebergCatalog::Filesystem { .. } => {
            registry.register(IcebergSourceDriver::new(
                move |secrets, execution, _egress, local_listing_lane| {
                    Ok(IcebergRuntimeDependencies::new(
                        Arc::new(
                            FileTransportFacade::new()
                                .with_shared_secret_provider(secrets)
                                .with_execution_services(execution)
                                .with_local_listing_lane(local_listing_lane)?,
                        ),
                        Arc::new(FixtureTransport::new(Vec::new())),
                        Arc::new(UnsupportedGlueCatalogClient),
                    ))
                },
            )?)?;
        }
        PreparedIcebergCatalog::Glue { .. } => {
            let http = ReqwestHttpProvider::new()?;
            let object_store_clients = ObjectStoreClientPool::default();
            registry.register(IcebergSourceDriver::new(
                move |secrets, execution, egress, local_listing_lane| {
                    let rest_http: Arc<dyn cdf_http::HttpTransport> = Arc::new(http.clone());
                    Ok(IcebergRuntimeDependencies::new(
                        Arc::new(
                            FileTransportFacade::new()
                                .with_http_transport(http.clone())
                                .with_shared_secret_provider(Arc::clone(&secrets))
                                .with_shared_object_store_clients(object_store_clients.clone())
                                .with_execution_services(execution.clone())
                                .with_local_listing_lane(local_listing_lane)?,
                        ),
                        Arc::clone(&rest_http),
                        Arc::new(AwsGlueCatalogClient::new(
                            rest_http, secrets, execution, egress,
                        )),
                    ))
                },
            )?)?;
        }
    }
    Ok(registry)
}

fn prepared_iceberg_declaration(request: &PreparedIcebergPackageWorkload) -> BenchResult<String> {
    let (catalog, object_credentials, egress_allowlist) = match &request.catalog {
        PreparedIcebergCatalog::Filesystem { warehouse } => (
            format!(
                "{{ kind = \"filesystem\", warehouse = {} }}",
                serde_json::to_string(warehouse)?
            ),
            None,
            Vec::new(),
        ),
        PreparedIcebergCatalog::Glue {
            region,
            catalog_id,
            catalog_credentials,
            object_credentials,
            egress_allowlist,
        } => {
            let mut fields = vec![
                "kind = \"glue\"".to_owned(),
                format!("region = {}", serde_json::to_string(region)?),
            ];
            if let Some(catalog_id) = catalog_id {
                fields.push(format!(
                    "catalog_id = {}",
                    serde_json::to_string(catalog_id)?
                ));
            }
            if let Some(credentials) = catalog_credentials {
                fields.push(format!(
                    "credentials = {}",
                    serde_json::to_string(credentials)?
                ));
            }
            (
                format!("{{ {} }}", fields.join(", ")),
                object_credentials.as_ref(),
                egress_allowlist.clone(),
            )
        }
    };
    let object_credentials = object_credentials
        .map(|credentials| {
            serde_json::to_string(credentials)
                .map(|credentials| format!("object_credentials = {credentials}\n"))
        })
        .transpose()?
        .unwrap_or_default();
    let maximum_concurrency = request
        .maximum_concurrency
        .map_or_else(String::new, |value| {
            format!("maximum_concurrency = {value}\n")
        });
    let parquet_batch_rows = request
        .parquet_batch_rows
        .map_or_else(String::new, |value| {
            format!("parquet_batch_rows = {value}\n")
        });
    let maximum_batch_bytes = request
        .maximum_batch_bytes
        .map_or_else(String::new, |value| {
            format!("maximum_batch_bytes = {value}\n")
        });
    let parquet_whole_object_prefetch_bytes = request
        .parquet_whole_object_prefetch_bytes
        .map_or_else(String::new, |value| {
            format!("parquet_whole_object_prefetch_bytes = {value}\n")
        });
    Ok(format!(
        r#"
[source.lake]
kind = "iceberg"
catalog = {catalog}
{object_credentials}maximum_metadata_bytes = {maximum_metadata_bytes}
{maximum_concurrency}{parquet_batch_rows}{maximum_batch_bytes}{parquet_whole_object_prefetch_bytes}egress_allowlist = {egress_allowlist}

[resource.table]
namespace = {namespace}
table = {table}
write_disposition = "append"
trust = "governed"
"#,
        maximum_metadata_bytes = request.maximum_metadata_bytes,
        egress_allowlist = serde_json::to_string(&egress_allowlist)?,
        namespace = serde_json::to_string(&request.namespace)?,
        table = serde_json::to_string(&request.table)?,
    ))
}

pub fn run_prepared_file_to_destination(
    request: &PreparedFileDestinationWorkload,
) -> BenchResult<PreparedFileDestinationRun> {
    if request.jobs == Some(0) {
        return Err(bench_error(
            "prepared destination workload jobs must be nonzero",
        ));
    }
    let spec = fixture_spec(&request.fixture_name)?;
    validate_spec(&spec)?;
    let format_id = match request.format {
        PreparedFileFormat::Csv => "csv",
        PreparedFileFormat::Json => "json",
        PreparedFileFormat::Ndjson => "ndjson",
        PreparedFileFormat::Parquet => "parquet",
    };
    let execution = benchmark_execution_services(request.execution_host_jobs)?;
    let host_jobs = execution.capabilities().logical_cpu_slots;
    let execution = execution
        .with_run_job_ceiling(request.jobs.unwrap_or(host_jobs))?
        .with_scheduler_measurement(true)?;
    let source = benchmark_file_resource(
        &request.source_root,
        &request.glob,
        format_id,
        &spec,
        &execution,
    )?;
    fs::create_dir_all(&request.output_root)?;
    let target = TargetName::new("orders")?;
    let destination = match &request.destination {
        PreparedDestinationKind::DuckDb => ResolvedProjectDestination::new(
            Box::new(cdf_dest_duckdb::DuckDbDestination::new(
                request.output_root.join("destination.duckdb"),
            )?),
            target,
        )
        .with_bound_execution_services(execution.clone())?,
        PreparedDestinationKind::Parquet => ResolvedProjectDestination::new(
            Box::new(
                cdf_dest_parquet::FilesystemParquetRuntime::with_execution_services(
                    request.output_root.join("parquet"),
                    execution.clone(),
                ),
            ),
            target,
        )
        .with_bound_execution_services(execution.clone())?,
        PreparedDestinationKind::Postgres {
            database_url,
            schema,
            table,
        } => {
            let postgres_target = PostgresTarget::new(schema.as_deref(), table)?;
            let target = TargetName::new(postgres_target.display_name())?;
            let destination = cdf_dest_postgres::PostgresDestination::connect(database_url)?;
            ResolvedProjectDestination::new(
                Box::new(cdf_dest_postgres::PostgresRuntime::for_replay(
                    &destination,
                    postgres_target,
                    MergeDedupPolicy::Last,
                    None,
                )),
                target,
            )
            .with_bound_execution_services(execution.clone())?
        }
    };
    let destination_capabilities = destination.runtime_capabilities();
    let mut policy = ContractPolicy::for_trust(source.resource.descriptor().trust_level.clone());
    if let Some(identifier) = destination.column_identifier_policy()? {
        policy.normalization.identifier = identifier;
    }
    let plan = identity_queryable_engine_plan_with_policy(
        source.resource.as_ref(),
        "pkg-p3-destination-jobs",
        &policy,
    )?
    .bind_compiled_source(&source.source_plan)?
    .bind_operator_graph(&source.source_plan, &destination_capabilities)?;
    let source_execution = plan.compiled_source_execution.as_ref().ok_or_else(|| {
        bench_error("prepared destination plan omitted its compiled source execution authority")
    })?;
    let partition_count = plan.scan.partition_count()?;
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        partition_count,
        source_execution.execution_capabilities(),
        &destination_capabilities,
        &execution,
        request.jobs,
    )?;
    execution.tighten_run_job_ceiling(scheduler.effective_jobs.jobs)?;
    let report = block_on(run_project_with_scheduler_and_telemetry(
        ProjectRunRequest {
            resource: ProjectRunSource::new(source.resource.as_ref()),
            plan,
            package_root: request.output_root.join("packages"),
            state_store_path: request.output_root.join("state.db"),
            pipeline_id: PipelineId::new("pipeline-p3-destination-jobs")?,
            package_id: "pkg-p3-destination-jobs".to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-p3-destination-jobs")?,
            destination,
            run_id: Some(RunId::new("run-p3-destination-jobs")?),
            event_sink: None,
            after_receipt_verified: None,
        },
        &execution,
        Some(scheduler.clone()),
        RunTelemetryConfig::phase_metrics(),
    ))?
    .into_committed()?;
    let (logical_receipt, logical_manifest_sha256, logical_manifest) =
        logical_destination_evidence(&report.receipt, &request.destination, &request.output_root)?;
    Ok(PreparedFileDestinationRun {
        configured_jobs: request.jobs,
        effective_jobs: scheduler.effective_jobs.jobs,
        limiting_factors: scheduler.effective_jobs.limiting_factors,
        partition_count,
        package_hash: report.package_hash.to_string(),
        receipt_package_hash: report.receipt.package_hash.to_string(),
        receipt_segment_ids: report
            .receipt
            .segment_acks
            .iter()
            .map(|ack| ack.segment_id.to_string())
            .collect(),
        state_segment_ids: report
            .checkpoint
            .delta
            .segments
            .iter()
            .map(|segment| segment.segment_id.to_string())
            .collect(),
        logical_receipt,
        logical_manifest_sha256,
        logical_manifest,
        row_count: report.row_count,
        runtime_scheduler: report.runtime_scheduler,
        source_frontier: report.source_frontier,
    })
}

pub fn run_case(case: &CaseDefinition, root: &Path) -> BenchResult<CaseOutcome> {
    let spec = fixture_spec(case.kind.fixture())?;
    validate_spec(&spec)?;

    let metric = match case.kind {
        CaseKind::NativeArrow { .. } => run_native_arrow_filter_project(&spec)?,
        CaseKind::NativeDataFusion { .. } => run_native_datafusion_filter_project(&spec)?,
        CaseKind::NativeDuckDb { .. } => run_native_duckdb_insert(&spec)?,
        CaseKind::CdfEnginePackage { .. } => run_cdf_engine_package(&spec, root)?,
        CaseKind::FileToPackage { format, .. } => run_file_to_package(&spec, root, format)?,
        CaseKind::RestDecode { .. } => run_rest_decode(&spec)?,
        CaseKind::ArchiveIpcToParquet { .. } => run_archive_ipc_to_parquet(&spec, root)?,
        CaseKind::PackageReplay { destination, .. } => {
            run_package_replay(&spec, root, destination)?
        }
        CaseKind::StartupFileToDuckDb { .. } => run_startup_file_to_duckdb(&spec, root)?,
    };

    Ok(CaseOutcome {
        label: case.label,
        metric_class: case.metric_class,
        rows: metric.rows,
        bytes: metric.bytes,
    })
}

fn run_native_arrow_filter_project(spec: &FixtureSpec) -> BenchResult<WorkMetric> {
    let batch = record_batch_range(spec, 0, spec.rows)?;
    let filtered = arrow_filter_project(&batch)?;
    Ok(WorkMetric {
        rows: filtered.num_rows() as u64,
        bytes: filtered.get_array_memory_size() as u64,
    })
}

fn run_native_datafusion_filter_project(spec: &FixtureSpec) -> BenchResult<WorkMetric> {
    let batch = record_batch_range(spec, 0, spec.rows)?;
    let rows = block_on(async move {
        let ctx = SessionContext::new();
        let frame = ctx
            .read_batch(batch)?
            .filter(col("active").eq(lit(true)).and(col("id").gt_eq(lit(0_i64))))?
            .select_columns(&["id", "category"])?;
        frame.collect().await
    })?;
    let row_count = rows.iter().map(|batch| batch.num_rows() as u64).sum();
    let byte_count = rows
        .iter()
        .map(|batch| batch.get_array_memory_size() as u64)
        .sum();
    Ok(WorkMetric {
        rows: row_count,
        bytes: byte_count,
    })
}

fn run_native_duckdb_insert(spec: &FixtureSpec) -> BenchResult<WorkMetric> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        "CREATE TABLE orders (id BIGINT NOT NULL, active BOOLEAN NOT NULL, category VARCHAR NOT NULL)",
    )?;
    let mut appender = conn.appender_with_columns("orders", &["id", "active", "category"])?;
    for row in 0..spec.rows {
        let id = row as i64;
        appender.append_row(appender_params_from_iter(vec![
            Value::BigInt(id),
            Value::Boolean(active_for_id(id)),
            Value::Text(category_for_id(id)),
        ]))?;
    }
    appender.flush()?;
    Ok(WorkMetric {
        rows: spec.rows as u64,
        bytes: 0,
    })
}

fn run_cdf_engine_package(spec: &FixtureSpec, root: &Path) -> BenchResult<WorkMetric> {
    let resource = MemoryResource::from_record_batches(
        "bench.orders",
        "memory",
        record_batches_for_spec(spec)?,
    )?;
    let source_plan = resource.compiled_source_plan();
    let plan = engine_plan(&resource, "pkg-engine-benchmark")?
        .bind_compiled_source(source_plan)?
        .bind_operator_graph(
            source_plan,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )?;
    let output = block_on(execute_to_package(
        &plan,
        &resource,
        root.join("pkg-engine-benchmark"),
    ))?;
    Ok(WorkMetric {
        rows: output.profile.output_rows,
        bytes: output.profile.output_bytes,
    })
}

fn run_file_to_package(
    spec: &FixtureSpec,
    root: &Path,
    format: LocalFormat,
) -> BenchResult<WorkMetric> {
    let data_root = root.join("data");
    let path = write_local_fixture_file(&data_root, spec, format)?;
    let execution = benchmark_execution_services(available_host_jobs())?;
    let source_root = path
        .parent()
        .ok_or_else(|| bench_error("benchmark source path must have a parent directory"))?;
    let glob = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| bench_error("benchmark source file name must be valid UTF-8"))?;
    let source = benchmark_file_resource(source_root, glob, format.format_id(), spec, &execution)?;
    let pre_finalize = |_builder: &PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let plan = queryable_engine_plan(source.resource.as_ref(), "pkg-file-benchmark")?
        .bind_compiled_source(&source.source_plan)?
        .bind_operator_graph(
            &source.source_plan,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )?;
    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        source.resource.as_ref(),
        root.join(format!("pkg-file-{}", format.label())),
        &pre_finalize,
        EngineExecutionConfig::default()
            .with_execution_services(execution)
            .new_invocation(),
    ))?;
    Ok(WorkMetric {
        rows: output.output.profile.output_rows,
        bytes: output.output.profile.output_bytes,
    })
}

fn run_rest_decode(spec: &FixtureSpec) -> BenchResult<WorkMetric> {
    let transport = FixtureTransport::new(rest_fixture_body(spec));
    let mut registry = SourceRegistry::new();
    let runtime_transport = transport.clone();
    registry.register(cdf_source_rest::RestSourceDriver::new(move || {
        Ok(Box::new(runtime_transport.clone()))
    })?)?;
    let document = parse_toml(
        r#"
[source.api]
kind = "rest"
base_url = "https://fixtures.example.test"
egress_allowlist = ["fixtures.example.test"]

[resource.items]
path = "/items"
records = "$.items"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "active", type = "boolean", nullable = false },
  { name = "category", type = "string", nullable = false }
] }
"#,
    )?;
    let compiled = compile_document(&registry, &document)?.remove(0);
    let execution =
        cdf_engine::StandaloneExecutionHost::default_services(BENCHMARK_MANAGED_MEMORY_BYTES)?.1;
    let resolution = SourceResolutionContext::new(
        Path::new("."),
        Arc::new(EnvSecretProvider::from_map(
            std::iter::empty::<(&str, &str)>(),
        )),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let resource = registry.resolve(compiled.source_plan(), &resolution)?;
    let request = ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: resource.descriptor().state_scope.clone(),
    };
    let partitions = resource.plan_partitions(&request)?;
    let batches = block_on(async {
        let mut stream = resource.open(partitions[0].clone()).await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        stream.completion().await?;
        Ok::<_, CdfError>(batches)
    })?;
    let rows = batches.iter().map(|batch| batch.header.row_count).sum();
    let bytes = batches.iter().map(|batch| batch.header.byte_count).sum();
    Ok(WorkMetric { rows, bytes })
}

fn run_archive_ipc_to_parquet(spec: &FixtureSpec, root: &Path) -> BenchResult<WorkMetric> {
    let fixture = build_archive_input_fixture(spec, root, "pkg-archive-benchmark")?;
    let report = persist_package_parquet_archive(&fixture.package_dir, false)?;
    Ok(WorkMetric {
        rows: report.row_count,
        bytes: report.archive_byte_count,
    })
}

fn run_package_replay(
    spec: &FixtureSpec,
    root: &Path,
    destination: ReplayDestination,
) -> BenchResult<WorkMetric> {
    let package_id = match destination {
        ReplayDestination::DuckDb | ReplayDestination::Parquet => "pkg-replay-benchmark".to_owned(),
        ReplayDestination::Postgres => postgres_package_id()?,
    };
    let fixture = build_replay_package_fixture(spec, root, &package_id)?;
    let target = TargetName::new("orders")?;
    let execution = benchmark_replay_execution_services(available_host_jobs())?;
    let destination = match destination {
        ReplayDestination::DuckDb => ResolvedProjectDestination::new(
            Box::new(cdf_dest_duckdb::DuckDbDestination::new(
                root.join("replay.duckdb"),
            )?),
            target,
        )
        .with_bound_execution_services(execution.clone())?,
        ReplayDestination::Parquet => ResolvedProjectDestination::new(
            Box::new(
                cdf_dest_parquet::FilesystemParquetRuntime::with_execution_services(
                    root.join("parquet"),
                    execution.clone(),
                ),
            ),
            target,
        )
        .with_bound_execution_services(execution.clone())?,
        ReplayDestination::Postgres => {
            let database_url = std::env::var(POSTGRES_URL_ENV).map_err(|_| {
                bench_error(format!(
                    "{POSTGRES_URL_ENV} must be set to run the opt-in postgres benchmark suite"
                ))
            })?;
            let postgres_target = PostgresTarget::new(None, "orders")?;
            let destination = cdf_dest_postgres::PostgresDestination::connect(database_url)?;
            ResolvedProjectDestination::new(
                Box::new(cdf_dest_postgres::PostgresRuntime::for_replay(
                    &destination,
                    postgres_target,
                    MergeDedupPolicy::Last,
                    None,
                )),
                target,
            )
            .with_bound_execution_services(execution.clone())?
        }
    };
    let checkpoint_store = InMemoryCheckpointStore::new();
    let report = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: fixture.package_dir,
        destination,
        checkpoint_store: &checkpoint_store,
        after_receipt_verified: None,
    })?;
    Ok(WorkMetric {
        rows: report
            .checkpoint
            .delta
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .sum(),
        bytes: report
            .checkpoint
            .delta
            .segments
            .iter()
            .map(|segment| segment.byte_count)
            .sum(),
    })
}

fn postgres_package_id() -> BenchResult<String> {
    let observed_at_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| bench_error(format!("system clock before unix epoch: {error}")))?
        .as_nanos();
    let counter = POSTGRES_PACKAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(format!("pkg-postgres-replay-{observed_at_ns}-{counter}"))
}

fn run_startup_file_to_duckdb(spec: &FixtureSpec, root: &Path) -> BenchResult<WorkMetric> {
    let project_root = root.join("startup");
    let data_root = project_root.join("data");
    fs::create_dir_all(&data_root)?;
    fs::write(data_root.join("events.ndjson"), startup_ndjson(spec))?;
    let document = parse_toml(
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "int64", nullable = false }
] }
"#,
    )?;
    let registry = benchmark_source_registry()?;
    let resource =
        compile_document_with_project_root(&registry, &document, &project_root)?.remove(0);
    let services = benchmark_execution_services(available_host_jobs())?;
    let source = resolve_benchmark_file_resource(resource, &registry, &project_root, &services)?;
    let package_id = "pkg-startup-benchmark";
    let destination = ResolvedProjectDestination::new(
        Box::new(cdf_dest_duckdb::DuckDbDestination::new(
            project_root.join(".cdf/dev.duckdb"),
        )?),
        TargetName::new("events")?,
    )
    .with_bound_execution_services(services.clone())?;
    let mut policy = ContractPolicy::for_trust(source.resource.descriptor().trust_level.clone());
    if let Some(identifier) = destination.column_identifier_policy()? {
        policy.normalization.identifier = identifier;
    }
    let plan = queryable_engine_plan_with_policy(source.resource.as_ref(), package_id, &policy)?
        .bind_compiled_source(&source.source_plan)?
        .bind_operator_graph(&source.source_plan, &destination.runtime_capabilities())?;
    let report = block_on(run_project(
        ProjectRunRequest {
            resource: ProjectRunSource::new(source.resource.as_ref()),
            plan,
            package_root: project_root.join(".cdf/packages"),
            state_store_path: project_root.join(".cdf/state.db"),
            pipeline_id: PipelineId::new("pipeline-startup-benchmark")?,
            package_id: package_id.to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-startup-benchmark")?,
            destination,
            run_id: Some(RunId::new("run-startup-benchmark")?),
            event_sink: None,
            after_receipt_verified: None,
        },
        &services,
    ))?
    .into_committed()?;
    Ok(WorkMetric {
        rows: report.row_count,
        bytes: report
            .checkpoint
            .delta
            .segments
            .iter()
            .map(|segment| segment.byte_count)
            .sum(),
    })
}

fn engine_plan<R: ResourceStream + ?Sized>(
    resource: &R,
    package_id: &str,
) -> BenchResult<cdf_engine::EnginePlan> {
    engine_plan_with_policy(
        resource,
        package_id,
        &ContractPolicy::for_trust(resource.descriptor().trust_level.clone()),
    )
}

fn identity_engine_plan<R: ResourceStream + ?Sized>(
    resource: &R,
    package_id: &str,
) -> BenchResult<cdf_engine::EnginePlan> {
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let validation_program = compile_validation_program(
        &ContractPolicy::for_trust(resource.descriptor().trust_level.clone()),
        &observed_schema,
    )?;
    Planner::new()
        .plan_tier_a(
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
        .map_err(Into::into)
}

fn engine_plan_with_policy<R: ResourceStream + ?Sized>(
    resource: &R,
    package_id: &str,
    policy: &ContractPolicy,
) -> BenchResult<cdf_engine::EnginePlan> {
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let validation_program = compile_validation_program(policy, &observed_schema)?;
    let projection = if resource.schema().field_with_name("category").is_ok() {
        Some(vec!["id".to_owned(), "category".to_owned()])
    } else {
        None
    };
    let filters = if resource.schema().field_with_name("active").is_ok() {
        vec![ScanPredicate::new(
            PredicateId::new("active-filter")?,
            "active = true",
        )?]
    } else {
        Vec::new()
    };
    Planner::new()
        .plan_tier_a(
            resource,
            EnginePlanInput {
                request: ScanRequest {
                    resource_id: resource.descriptor().resource_id.clone(),
                    projection,
                    filters,
                    limit: None,
                    order_by: Vec::new(),
                    scope: resource.descriptor().state_scope.clone(),
                },
                validation_program,
                execution_extent: ExecutionExtent::bounded(),
                package_id: package_id.to_owned(),
            },
        )
        .map_err(Into::into)
}

fn queryable_engine_plan<R: QueryableResource + ?Sized>(
    resource: &R,
    package_id: &str,
) -> BenchResult<cdf_engine::EnginePlan> {
    queryable_engine_plan_with_policy(
        resource,
        package_id,
        &ContractPolicy::for_trust(resource.descriptor().trust_level.clone()),
    )
}

fn queryable_engine_plan_with_policy<R: QueryableResource + ?Sized>(
    resource: &R,
    package_id: &str,
    policy: &ContractPolicy,
) -> BenchResult<cdf_engine::EnginePlan> {
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let validation_program = compile_validation_program(policy, &observed_schema)?;
    let projection = if resource.schema().field_with_name("category").is_ok() {
        Some(vec!["id".to_owned(), "category".to_owned()])
    } else {
        None
    };
    let filters = if resource.schema().field_with_name("active").is_ok() {
        vec![ScanPredicate::new(
            PredicateId::new("active-filter")?,
            "active = true",
        )?]
    } else {
        Vec::new()
    };
    Planner::new()
        .plan_tier_b(
            resource,
            EnginePlanInput {
                request: ScanRequest {
                    resource_id: resource.descriptor().resource_id.clone(),
                    projection,
                    filters,
                    limit: None,
                    order_by: Vec::new(),
                    scope: resource.descriptor().state_scope.clone(),
                },
                validation_program,
                execution_extent: ExecutionExtent::bounded(),
                package_id: package_id.to_owned(),
            },
        )
        .map_err(Into::into)
}

fn identity_queryable_engine_plan<R: QueryableResource + ?Sized>(
    resource: &R,
    package_id: &str,
) -> BenchResult<cdf_engine::EnginePlan> {
    identity_queryable_engine_plan_with_policy(
        resource,
        package_id,
        &ContractPolicy::for_trust(resource.descriptor().trust_level.clone()),
    )
}

fn identity_queryable_engine_plan_with_policy<R: QueryableResource + ?Sized>(
    resource: &R,
    package_id: &str,
    policy: &ContractPolicy,
) -> BenchResult<cdf_engine::EnginePlan> {
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let validation_program = compile_validation_program(policy, &observed_schema)?;
    Planner::new()
        .plan_tier_b(
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
        .map_err(Into::into)
}

fn build_archive_input_fixture(
    spec: &FixtureSpec,
    root: &Path,
    package_id: &str,
) -> BenchResult<PackageFixture> {
    let package_dir = root.join(package_id);
    let batches = record_batches_for_spec(spec)?;
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .ok_or_else(|| bench_error("package fixture requires at least one batch"))?;
    let schema_hash = canonical_arrow_schema_hash(schema.as_ref())?;
    let builder = PackageBuilder::create(
        &package_dir,
        package_id,
        cdf_package::PackageBuilderResources::standalone(64 * 1024 * 1024, 1024 * 1024 * 1024)?,
    )?;
    builder.update_status(PackageStatus::Extracting)?;
    builder.write_json_artifact(
        "plan/benchmark-fixture.json",
        &serde_json::json!({
            "fixture": spec.name,
            "rows": spec.rows,
            "wide_columns": spec.wide_columns
        }),
    )?;
    builder.write_json_artifact(
        "schema/observed.json",
        &serde_json::json!({ "schema_hash": schema_hash.as_str() }),
    )?;
    builder.write_runtime_arrow_schema(schema.as_ref())?;
    let batches = cdf_package_contract::append_package_row_ord(batches, 0)?;
    let segment = builder.write_segment(SegmentId::new("seg-000001")?, 0, &batches)?;
    let scope = ScopeKey::Resource;
    let output_position = SourcePosition::Cursor(CursorPosition {
        version: CHECKPOINT_STATE_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64((spec.rows - 1) as i64),
    });
    let state_segments = vec![StateSegment {
        segment_id: segment.segment_id.clone(),
        scope: scope.clone(),
        output_position: output_position.clone(),
        row_count: segment.row_count,
        byte_count: segment.byte_count,
    }];
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}"))?,
        pipeline_id: PipelineId::new("pipeline-benchmark")?,
        resource_id: ResourceId::new("bench.orders")?,
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position,
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        schema_hash: schema_hash.clone(),
        segments: state_segments,
    };
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders")?,
        WriteDisposition::Append,
        Vec::new(),
        schema_hash,
    );
    builder.write_input_checkpoint_artifact(&None)?;
    builder.write_state_delta_preimage_artifact(&state_delta)?;
    builder.write_commit_plan_preimage_artifact(&commit_plan)?;
    builder.finish()?;
    PackageReader::open(&package_dir)?.verify()?;
    Ok(PackageFixture { package_dir })
}

fn build_replay_package_fixture(
    spec: &FixtureSpec,
    root: &Path,
    package_id: &str,
) -> BenchResult<PackageFixture> {
    let package_dir = root.join(package_id);
    let resource = MemoryResource::from_record_batches(
        "bench.orders",
        "memory",
        record_batches_for_spec(spec)?,
    )?;
    let schema_hash = canonical_arrow_schema_hash(resource.schema().as_ref())?;
    let source_plan = resource.compiled_source_plan();
    let plan = identity_engine_plan(&resource, package_id)?
        .bind_compiled_source(source_plan)?
        .bind_operator_graph(
            source_plan,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )?;
    let output_position = SourcePosition::Cursor(CursorPosition {
        version: CHECKPOINT_STATE_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64((spec.rows - 1) as i64),
    });
    let pre_finalize = |builder: &PackageBuilder, draft: EnginePackageDraft<'_>| {
        if draft.profile.output_rows != spec.rows as u64 {
            return Err(CdfError::internal(format!(
                "replay fixture expected {} output rows but engine produced {}",
                spec.rows, draft.profile.output_rows
            )));
        }
        let mut positions = draft.segment_positions.iter();
        let mut state_segments = Vec::with_capacity(draft.segment_positions.len());
        builder.visit_segment_entries(&mut |segment| {
            let position = positions.next().ok_or_else(|| {
                CdfError::internal(format!(
                    "replay fixture omitted engine position evidence for segment {}",
                    segment.segment_id
                ))
            })?;
            if position.segment_id != segment.segment_id {
                return Err(CdfError::internal(format!(
                    "replay fixture engine segment {} does not match package segment {}",
                    position.segment_id, segment.segment_id
                )));
            }
            state_segments.push(StateSegment {
                segment_id: segment.segment_id,
                scope: ScopeKey::Resource,
                output_position: output_position.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            });
            Ok(())
        })?;
        if positions.next().is_some() || state_segments.len() != draft.segment_positions.len() {
            return Err(CdfError::internal(format!(
                "replay fixture has {} engine segment positions but {} package segments",
                draft.segment_positions.len(),
                state_segments.len()
            )));
        }
        let state_delta = StateDeltaPreimage {
            checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}"))?,
            pipeline_id: PipelineId::new("pipeline-benchmark")?,
            resource_id: resource.descriptor().resource_id.clone(),
            scope: ScopeKey::Resource,
            state_version: CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: None,
            input_position: None,
            output_position: output_position.clone(),
            output_watermark: None,
            partition_watermarks: Vec::new(),
            late_data_carryover: Vec::new(),
            source_continuation: None,
            schema_hash: schema_hash.clone(),
            segments: state_segments,
        };
        let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
            TargetName::new("orders")?,
            WriteDisposition::Append,
            Vec::new(),
            schema_hash.clone(),
        );
        builder.write_input_checkpoint_artifact(&None)?;
        builder.write_state_delta_preimage_artifact(&state_delta)?;
        builder.write_commit_plan_preimage_artifact(&commit_plan)?;
        Ok(())
    };
    let execution = benchmark_execution_services(available_host_jobs())?;
    block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        &package_dir,
        &pre_finalize,
        EngineExecutionConfig::default()
            .with_execution_services(execution)
            .new_invocation(),
    ))?;
    PackageReader::open(&package_dir)?.verify()?;
    Ok(PackageFixture { package_dir })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdf_engine_package_case_uses_current_compiled_source_authority() {
        let root = tempfile::tempdir().unwrap();
        let case = crate::matrix::cases_for(crate::matrix::BenchmarkSuite::Smoke)
            .into_iter()
            .find(|case| case.label == "trend.cdf_engine.package_filter_project.medium")
            .unwrap();

        let outcome = run_case(case, root.path()).unwrap();

        assert_eq!(outcome.label, case.label);
        assert!(outcome.rows > 0);
        assert!(outcome.bytes > 0);
    }

    #[test]
    fn duckdb_replay_case_uses_current_package_authority() {
        let root = tempfile::tempdir().unwrap();
        let case = crate::matrix::cases_for(crate::matrix::BenchmarkSuite::Smoke)
            .into_iter()
            .find(|case| {
                case.label == "trend.cdf_package_replay.duckdb_package_receipt_checkpoint.medium"
            })
            .unwrap();

        let outcome = run_case(case, root.path()).unwrap();

        assert_eq!(outcome.label, case.label);
        assert_eq!(outcome.rows, fixture_spec("medium").unwrap().rows as u64);
        assert!(outcome.bytes > 0);
    }

    fn iceberg_workload(catalog: PreparedIcebergCatalog) -> PreparedIcebergPackageWorkload {
        PreparedIcebergPackageWorkload {
            project_root: PathBuf::from("/tmp/cdf-bench-project"),
            catalog,
            namespace: vec!["analytics".to_owned()],
            table: "events".to_owned(),
            projection: Some(vec!["event_id".to_owned()]),
            package_dir: PathBuf::from("/tmp/cdf-bench-package"),
            retain_package: false,
            maximum_metadata_bytes: 128 * 1024 * 1024,
            maximum_concurrency: Some(24),
            parquet_batch_rows: Some(32 * 1024),
            maximum_batch_bytes: Some(64 * 1024 * 1024),
            parquet_whole_object_prefetch_bytes: Some(8 * 1024 * 1024),
            jobs: Some(16),
            execution_host_jobs: 16,
        }
    }

    #[test]
    fn prepared_iceberg_catalogs_compile_through_the_real_driver() {
        let filesystem = iceberg_workload(PreparedIcebergCatalog::Filesystem {
            warehouse: PathBuf::from("/tmp/warehouse"),
        });
        let glue = iceberg_workload(PreparedIcebergCatalog::Glue {
            region: "us-west-2".to_owned(),
            catalog_id: Some("123456789012".to_owned()),
            catalog_credentials: None,
            object_credentials: Some("secret://env/CDF_BENCH_S3".to_owned()),
            egress_allowlist: vec![
                "glue.us-west-2.amazonaws.com".to_owned(),
                "bench-bucket.s3.us-west-2.amazonaws.com".to_owned(),
            ],
        });

        for workload in [filesystem, glue] {
            let registry = prepared_iceberg_registry(&workload.catalog).unwrap();
            let document = parse_toml(&prepared_iceberg_declaration(&workload).unwrap()).unwrap();
            let compiled =
                compile_document_with_project_root(&registry, &document, &workload.project_root)
                    .unwrap();
            assert_eq!(compiled.len(), 1);
            assert_eq!(compiled[0].descriptor().resource_id.as_str(), "lake.table");
        }
    }

    #[test]
    fn source_io_stage_reports_counter_deltas_without_inventing_a_wall_sum() {
        let before = SourceIoControllerReport {
            acquired_requests: 2,
            queue_wait_ns: 3,
            physical_bytes: 5,
            request_duration_ns: 7,
            peak_active: 1,
            ..SourceIoControllerReport::default()
        };
        let after = SourceIoControllerReport {
            acquired_requests: 13,
            queue_wait_ns: 20,
            physical_bytes: 105,
            request_duration_ns: 207,
            peak_active: 8,
            ..SourceIoControllerReport::default()
        };

        assert_eq!(
            source_io_stage("iceberg.plan", 50, &before, &after),
            PreparedSourceIoStage {
                stage: "iceberg.plan".to_owned(),
                wall_duration_ns: 50,
                request_duration_ns: 200,
                physical_bytes: 100,
                requests: 11,
                queue_wait_ns: 17,
                peak_active_through_stage: 8,
            }
        );
    }
}
