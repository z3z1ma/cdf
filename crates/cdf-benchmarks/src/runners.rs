use std::{
    fs,
    io::Cursor,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_declarative::{
    RestRuntimeDependencies, compile_document, compile_document_with_project_root, parse_toml,
};
use cdf_dest_postgres::{MergeDedupPolicy, PostgresTarget};
use cdf_engine::{
    EngineExecutionOptions, EnginePackageDraft, EnginePlanInput, PlanBoundedness, Planner,
    execute_to_package, execute_to_package_with_segment_positions_and_pre_finalize,
};
use cdf_formats::{
    CsvOptions, FileFormat, FileResource, FileSource, JsonOptions, ReadOptions,
    read_arrow_ipc_stream, schema_hash,
};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, CheckpointId, CursorPosition, CursorValue, PartitionId,
    PipelineId, PredicateId, ResourceId, ResourceStream, RunId, ScanPredicate, ScanRequest,
    ScopeKey, SegmentId, SourcePosition, StateSegment, TargetName, WriteDisposition,
};
use cdf_package::{PackageBuilder, PackageReader, archive_package_to_parquet};
use cdf_package_contract::{DestinationCommitPlanPreimage, PackageStatus, StateDeltaPreimage};
use cdf_project::{
    PackageArtifactReplayRequest, ProjectRunRequest, ProjectRunSource, ResolvedProjectDestination,
    replay_package_from_artifacts, run_project,
};
use cdf_state_sqlite::InMemoryCheckpointStore;
use datafusion::prelude::{SessionContext, col, lit};
use duckdb::{Connection, appender_params_from_iter, types::Value};
use futures_executor::block_on;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

use crate::{
    BenchResult, PhaseMetric, WorkerMeasurement, bench_error,
    catalog::{FixtureSpec, fixture_spec, validate_spec},
    fixtures::{
        active_for_id, arrow_filter_project, arrow_ipc_stream_bytes, category_for_id,
        record_batch_range, record_batches_for_spec, rest_fixture_body, startup_ndjson,
        write_local_fixture_file,
    },
    matrix::{CaseDefinition, CaseKind, CaseOutcome, LocalFormat, ReplayDestination},
    resource::{FixtureTransport, MemoryResource},
};

const POSTGRES_URL_ENV: &str = "CDF_BENCH_POSTGRES_URL";
static POSTGRES_PACKAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
struct WorkMetric {
    rows: u64,
    bytes: u64,
}

struct PackageFixture {
    package_dir: PathBuf,
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
    pub source_path: PathBuf,
    pub package_dir: PathBuf,
    pub format: PreparedFileFormat,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCaseWorkload {
    pub case_label: String,
    pub output_root: PathBuf,
}

pub fn run_legacy_case_workload(request: &LegacyCaseWorkload) -> BenchResult<WorkerMeasurement> {
    let case = crate::benchmark_cases()
        .iter()
        .find(|case| case.label == request.case_label)
        .ok_or_else(|| {
            bench_error(format!(
                "unknown legacy benchmark case `{}`",
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
) -> BenchResult<WorkerMeasurement> {
    let spec = fixture_spec(&request.fixture_name)?;
    validate_spec(&spec)?;
    let format = match request.format {
        PreparedFileFormat::Csv => FileFormat::Csv(CsvOptions::default()),
        PreparedFileFormat::Json => FileFormat::Json(JsonOptions::default()),
        PreparedFileFormat::Ndjson => FileFormat::Ndjson(JsonOptions::default()),
        PreparedFileFormat::Parquet => FileFormat::Parquet,
    };
    let options = ReadOptions::new(
        ResourceId::new("bench.prepared")?,
        PartitionId::new("prepared-file")?,
    )
    .with_batch_size(spec.batch_size)?;
    let resource = FileResource::new(FileSource::new(&request.source_path, format, options))?;
    let pre_finalize = |_builder: &PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &identity_engine_plan(&resource, "pkg-p3-prepared")?,
        &resource,
        &request.package_dir,
        &pre_finalize,
        EngineExecutionOptions::default().with_phase_metrics(true),
    ))?;
    Ok(WorkerMeasurement {
        timed_wall_time_ns: None,
        rows: output.output.profile.output_rows,
        logical_bytes: output.output.profile.output_bytes,
        physical_bytes: fs::metadata(&request.source_path)?.len(),
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
        CaseKind::ArrowIpcStreamToPackage { .. } => run_arrow_ipc_stream_to_package(&spec, root)?,
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
    let output = block_on(execute_to_package(
        &engine_plan(&resource, "pkg-engine-benchmark")?,
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
    let options = ReadOptions::new(
        ResourceId::new("bench.orders")?,
        PartitionId::new(format!("file-{}", format.label()))?,
    )
    .with_batch_size(spec.batch_size)?;
    let resource = FileResource::new(FileSource::new(path, format.file_format(), options))?;
    let output = block_on(execute_to_package(
        &engine_plan(&resource, "pkg-file-benchmark")?,
        &resource,
        root.join(format!("pkg-file-{}", format.label())),
    ))?;
    Ok(WorkMetric {
        rows: output.profile.output_rows,
        bytes: output.profile.output_bytes,
    })
}

fn run_arrow_ipc_stream_to_package(spec: &FixtureSpec, root: &Path) -> BenchResult<WorkMetric> {
    let read = read_arrow_ipc_stream(
        Cursor::new(arrow_ipc_stream_bytes(spec)?),
        &ReadOptions::new(
            ResourceId::new("bench.orders")?,
            PartitionId::new("arrow-ipc-stream")?,
        )
        .with_batch_size(spec.batch_size)?,
    )?;
    let resource = MemoryResource::from_batches(
        read.descriptor,
        PartitionId::new("arrow-ipc-stream")?,
        ScopeKey::Stream {
            name: "arrow_ipc_stdout".to_owned(),
        },
        read.batches,
    )?;
    let output = block_on(execute_to_package(
        &engine_plan(&resource, "pkg-arrow-ipc-benchmark")?,
        &resource,
        root.join("pkg-arrow-ipc-benchmark"),
    ))?;
    Ok(WorkMetric {
        rows: output.profile.output_rows,
        bytes: output.profile.output_bytes,
    })
}

fn run_rest_decode(spec: &FixtureSpec) -> BenchResult<WorkMetric> {
    let document = parse_toml(
        r#"
[source.api]
kind = "rest"
base_url = "https://fixtures.example.test"
egress_allowlist = ["fixtures.example.test"]

[resource.items]
id = "api.items"
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
    let compiled = compile_document(&document)?.remove(0);
    let resource = compiled.to_rest_resource(RestRuntimeDependencies::new(
        FixtureTransport::new(rest_fixture_body(spec)),
    ))?;
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
        Ok::<_, CdfError>(batches)
    })?;
    let rows = batches.iter().map(|batch| batch.header.row_count).sum();
    let bytes = batches.iter().map(|batch| batch.header.byte_count).sum();
    Ok(WorkMetric { rows, bytes })
}

fn run_archive_ipc_to_parquet(spec: &FixtureSpec, root: &Path) -> BenchResult<WorkMetric> {
    let fixture = build_package_fixture(spec, root, "pkg-archive-benchmark")?;
    let report = archive_package_to_parquet(&fixture.package_dir)?;
    Ok(WorkMetric {
        rows: report
            .segments
            .iter()
            .map(|segment| segment.parquet_row_count)
            .sum(),
        bytes: report
            .segments
            .iter()
            .map(|segment| segment.parquet_byte_count)
            .sum(),
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
    let fixture = build_package_fixture(spec, root, &package_id)?;
    let target = TargetName::new("orders")?;
    let destination = match destination {
        ReplayDestination::DuckDb => ResolvedProjectDestination::new(
            Box::new(cdf_dest_duckdb::DuckDbDestination::new(
                root.join("replay.duckdb"),
            )?),
            target,
        ),
        ReplayDestination::Parquet => ResolvedProjectDestination::new(
            Box::new(
                cdf_dest_parquet::FilesystemParquetRuntime::with_execution_services(
                    root.join("parquet"),
                    cdf_engine::StandaloneExecutionHost::default_services(512 * 1024 * 1024)?.1,
                ),
            ),
            target,
        ),
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
id = "local.events"
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
    let resource = compile_document_with_project_root(&document, &project_root)?.remove(0);
    let package_id = "pkg-startup-benchmark";
    let destination = ResolvedProjectDestination::new(
        Box::new(cdf_dest_duckdb::DuckDbDestination::new(
            project_root.join(".cdf/dev.duckdb"),
        )?),
        TargetName::new("events")?,
    );
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    if let Some(identifier) = destination.column_identifier_policy()? {
        policy.normalization.identifier = identifier;
    }
    let plan = engine_plan_with_policy(&resource, package_id, &policy)?;
    let report = block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::local_file(&resource),
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
    }))?;
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
                boundedness: PlanBoundedness::Bounded,
                package_id: package_id.to_owned(),
            },
        )
        .map_err(Into::into)
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
                boundedness: PlanBoundedness::Bounded,
                package_id: package_id.to_owned(),
            },
        )
        .map_err(Into::into)
}

fn build_package_fixture(
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
    let schema_hash = schema_hash(schema.as_ref())?;
    let builder = PackageBuilder::create(&package_dir, package_id)?;
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
    let segment = builder.write_segment(SegmentId::new("seg-000001")?, &batches)?;
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
        schema_hash: schema_hash.clone(),
        segments: state_segments.clone(),
    };
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders")?,
        WriteDisposition::Append,
        Vec::new(),
        schema_hash,
        state_segments,
    );
    builder.write_input_checkpoint_artifact(&None)?;
    builder.write_state_delta_preimage_artifact(&state_delta)?;
    builder.write_commit_plan_preimage_artifact(&commit_plan)?;
    builder.finish()?;
    PackageReader::open(&package_dir)?.verify()?;
    Ok(PackageFixture { package_dir })
}
