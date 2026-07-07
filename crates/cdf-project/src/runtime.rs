use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use arrow_schema::{DataType, Schema, TimeUnit};
use cdf_declarative::{CompiledResource, CompiledResourcePlan, RestResource, SqlResource};
use cdf_dest_duckdb::{DuckDbCommitRequest, DuckDbDestination};
use cdf_dest_parquet::{ParquetCommitRequest, ParquetDestination};
use cdf_dest_postgres::{
    MergeDedupPolicy, PostgresColumn, PostgresCommitRequest, PostgresDestination,
    PostgresExistingTable, PostgresIdentifier, PostgresLoadPlan, PostgresLoadPlanInput,
    PostgresTarget, postgres_columns_for_schema,
};
#[cfg(test)]
use cdf_engine::EngineRunOutputWithSegmentPositions;
use cdf_engine::{
    EnginePackageDraft, EnginePlan, execute_to_package_with_segment_positions_and_pre_finalize,
};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, Checkpoint, CheckpointId, CheckpointStatus,
    CheckpointStore, CursorOrderingClaim, CursorPosition, CursorValue, DestinationCommitRequest,
    DestinationId, DestinationProtocol, IdempotencyToken, PackageHash, PipelineId, PlanId, Receipt,
    ResourceDescriptor, ResourceId, ResourceStream, Result, RunId, SchemaHash, SchemaSource,
    ScopeKey, SegmentId, SourcePosition, StateDelta, StateSegment, TargetName, WriteDisposition,
};
use cdf_package::{
    DestinationCommitPlanPreimage, PackageReader, PackageReplayInputs, PackageStatus, ReplayView,
    SegmentEntry, StateDeltaPreimage,
};
use cdf_state_sqlite::{
    RunEventAppend, RunEventDetails, RunEventKind, RunEventValue, RunLedgerSnapshot,
    SqliteCheckpointStore, SqliteRunLedger,
};

pub type ReceiptVerifiedHook<'a> = &'a dyn Fn(&Receipt) -> Result<()>;
pub type LocalDuckDbLifecycleFailpointHook<'a> =
    &'a dyn Fn(LocalDuckDbLifecycleFailpoint, Option<&Receipt>) -> Result<()>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalDuckDbLifecycleFailpoint {
    AfterPackagedBeforeDestinationWrite,
    AfterCheckpointProposalBeforeDestinationWrite,
    AfterReceiptVerifiedBeforeCheckpointCommit,
    AfterCheckpointCommitBeforePackageStatusCheckpointed,
}

pub struct PreparedDuckDbReplayRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a DuckDbDestination,
    pub checkpoint_store: &'a Store,
    pub delta: StateDelta,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub merge_keys: Vec<String>,
    pub schema_hash: SchemaHash,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PreparedDuckDbRecoveryRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a DuckDbDestination,
    pub checkpoint_store: &'a Store,
    pub delta: StateDelta,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub schema_hash: SchemaHash,
    pub receipt: Receipt,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactDuckDbReplayRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a DuckDbDestination,
    pub checkpoint_store: &'a Store,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactDuckDbRecoveryRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a DuckDbDestination,
    pub checkpoint_store: &'a Store,
    pub receipt: Receipt,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactParquetRecoveryRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a ParquetDestination,
    pub checkpoint_store: &'a Store,
    pub receipt: Receipt,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactParquetReplayRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a ParquetDestination,
    pub checkpoint_store: &'a Store,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactPostgresRecoveryRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a PostgresDestination,
    pub checkpoint_store: &'a Store,
    pub receipt: Receipt,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactPostgresReplayRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a PostgresDestination,
    pub checkpoint_store: &'a Store,
    pub target: PostgresTarget,
    pub dedup: MergeDedupPolicy,
    pub existing_table: Option<PostgresExistingTable>,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedDuckDbReplayReport {
    pub checkpoint: Checkpoint,
    pub receipt: Receipt,
    pub receipt_source: PreparedReceiptSource,
    pub package_status: PackageStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreparedReceiptSource {
    DuckDbCommit {
        duplicate: bool,
        package_receipt_recorded: bool,
    },
    SuppliedDurableReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectReceiptSource {
    DestinationCommit {
        duplicate: bool,
        package_receipt_recorded: bool,
    },
    DestinationCommitReceiptOnly {
        package_receipt_recorded: bool,
    },
    SuppliedDurableReceipt,
}

impl ProjectReceiptSource {
    fn into_duckdb_receipt_source(self) -> PreparedReceiptSource {
        match self {
            Self::DestinationCommit {
                duplicate,
                package_receipt_recorded,
            } => PreparedReceiptSource::DuckDbCommit {
                duplicate,
                package_receipt_recorded,
            },
            Self::DestinationCommitReceiptOnly { .. } => {
                unreachable!("Postgres receipt-only metadata cannot become a DuckDB report")
            }
            Self::SuppliedDurableReceipt => PreparedReceiptSource::SuppliedDurableReceipt,
        }
    }
}

impl From<PreparedReceiptSource> for ProjectReceiptSource {
    fn from(source: PreparedReceiptSource) -> Self {
        match source {
            PreparedReceiptSource::DuckDbCommit {
                duplicate,
                package_receipt_recorded,
            } => Self::DestinationCommit {
                duplicate,
                package_receipt_recorded,
            },
            PreparedReceiptSource::SuppliedDurableReceipt => Self::SuppliedDurableReceipt,
        }
    }
}

pub struct LocalFileDuckDbRunRequest<'a> {
    pub resource: &'a CompiledResource,
    pub plan: EnginePlan,
    pub package_root: PathBuf,
    pub destination_path: PathBuf,
    pub state_store_path: PathBuf,
    pub pipeline_id: PipelineId,
    pub target: TargetName,
    pub package_id: String,
    pub checkpoint_id: CheckpointId,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

#[derive(Clone, PartialEq, Eq)]
pub enum ProjectRunDestination {
    DuckDb {
        database_path: PathBuf,
        target: TargetName,
    },
    ParquetFilesystem {
        root: PathBuf,
        target: TargetName,
    },
    Postgres {
        database_url: String,
        target: PostgresTarget,
        dedup: MergeDedupPolicy,
        existing_table: Option<PostgresExistingTable>,
    },
}

impl std::fmt::Debug for ProjectRunDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuckDb {
                database_path,
                target,
            } => f
                .debug_struct("DuckDb")
                .field("database_path", database_path)
                .field("target", target)
                .finish(),
            Self::ParquetFilesystem { root, target } => f
                .debug_struct("ParquetFilesystem")
                .field("root", root)
                .field("target", target)
                .finish(),
            Self::Postgres {
                target,
                dedup,
                existing_table,
                ..
            } => f
                .debug_struct("Postgres")
                .field("database_url", &"<redacted>")
                .field("target", target)
                .field("dedup", dedup)
                .field("existing_table", existing_table)
                .finish(),
        }
    }
}

pub struct ProjectRunRequest<'a> {
    pub resource: ProjectRunResource<'a>,
    pub plan: EnginePlan,
    pub package_root: PathBuf,
    pub state_store_path: PathBuf,
    pub pipeline_id: PipelineId,
    pub package_id: String,
    pub checkpoint_id: CheckpointId,
    pub destination: ProjectRunDestination,
    pub run_id: Option<RunId>,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

#[derive(Clone, Copy, Debug)]
pub enum ProjectRunResource<'a> {
    LocalFile(&'a CompiledResource),
    Rest(&'a RestResource),
    Sql(&'a SqlResource),
}

impl<'a> ProjectRunResource<'a> {
    pub fn local_file(resource: &'a CompiledResource) -> Self {
        Self::LocalFile(resource)
    }

    pub fn rest(resource: &'a RestResource) -> Self {
        Self::Rest(resource)
    }

    pub fn sql(resource: &'a SqlResource) -> Self {
        Self::Sql(resource)
    }

    fn stream(self) -> &'a dyn ResourceStream {
        match self {
            Self::LocalFile(resource) => resource,
            Self::Rest(resource) => resource,
            Self::Sql(resource) => resource,
        }
    }

    fn descriptor(self) -> &'a ResourceDescriptor {
        self.stream().descriptor()
    }

    fn validate_supported(self) -> Result<()> {
        match self {
            Self::LocalFile(resource) => validate_local_file_run_resource(resource),
            Self::Rest(resource) => resource.validate_runtime_dependencies(),
            Self::Sql(resource) => resource.validate_runtime_dependencies(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFileDuckDbRunReport {
    pub package_dir: PathBuf,
    pub package_id: String,
    pub package_hash: PackageHash,
    pub package_status: PackageStatus,
    pub checkpoint: Checkpoint,
    pub receipt: Receipt,
    pub receipt_source: PreparedReceiptSource,
    pub row_count: u64,
    pub segment_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectRunReport {
    pub run_id: RunId,
    pub ledger_snapshot: RunLedgerSnapshot,
    pub package_dir: PathBuf,
    pub package_id: String,
    pub package_hash: PackageHash,
    pub package_status: PackageStatus,
    pub checkpoint: Checkpoint,
    pub receipt: Receipt,
    pub receipt_source: ProjectReceiptSource,
    pub row_count: u64,
    pub segment_count: usize,
}

impl ProjectRunReport {
    fn into_local_file_duckdb_report(self) -> LocalFileDuckDbRunReport {
        LocalFileDuckDbRunReport {
            package_dir: self.package_dir,
            package_id: self.package_id,
            package_hash: self.package_hash,
            package_status: self.package_status,
            checkpoint: self.checkpoint,
            receipt: self.receipt,
            receipt_source: self.receipt_source.into_duckdb_receipt_source(),
            row_count: self.row_count,
            segment_count: self.segment_count,
        }
    }
}

pub async fn run_local_file_to_duckdb_checkpoint(
    request: LocalFileDuckDbRunRequest<'_>,
) -> Result<LocalFileDuckDbRunReport> {
    run_local_file_to_duckdb_checkpoint_with_failpoint(request, None).await
}

pub async fn run_local_file_to_duckdb_checkpoint_with_failpoint(
    request: LocalFileDuckDbRunRequest<'_>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
) -> Result<LocalFileDuckDbRunReport> {
    let request = ProjectRunRequest {
        resource: ProjectRunResource::LocalFile(request.resource),
        plan: request.plan,
        package_root: request.package_root,
        state_store_path: request.state_store_path,
        pipeline_id: request.pipeline_id,
        package_id: request.package_id,
        checkpoint_id: request.checkpoint_id,
        destination: ProjectRunDestination::DuckDb {
            database_path: request.destination_path,
            target: request.target,
        },
        run_id: None,
        after_receipt_verified: request.after_receipt_verified,
    };
    Ok(run_project_with_failpoint(request, lifecycle_failpoint)
        .await?
        .into_local_file_duckdb_report())
}

pub async fn run_project(request: ProjectRunRequest<'_>) -> Result<ProjectRunReport> {
    run_project_with_failpoint(request, None).await
}

async fn run_project_with_failpoint(
    request: ProjectRunRequest<'_>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
) -> Result<ProjectRunReport> {
    validate_project_run_request(&request)?;
    validate_explicit_package_id(&request.package_id)?;

    let schema_hash = declared_schema_hash(request.resource.stream())?;
    let package_dir = request.package_root.join(&request.package_id);
    refuse_existing_package_dir(&package_dir)?;
    ensure_parent_directory(&request.state_store_path)?;

    match &request.destination {
        ProjectRunDestination::DuckDb {
            database_path,
            target,
        } => {
            ensure_parent_directory(database_path)?;
            let run_ledger = SqliteRunLedger::open(&request.state_store_path)?;
            let run = run_ledger.create_run(request.run_id.clone())?;
            let checkpoint_store = SqliteCheckpointStore::open(&request.state_store_path)?;
            let destination = DuckDbDestination::new(database_path)?;
            let recorder = ProjectRunRecorder::new(
                &run_ledger,
                run.run_id,
                recorder_context(
                    &request,
                    &package_dir,
                    destination.sheet().destination.clone(),
                ),
            );
            let execution = ProjectDuckDbRunExecution {
                target,
                checkpoint_store: &checkpoint_store,
                destination: &destination,
                recorder: &recorder,
                lifecycle_failpoint,
            };
            match run_project_duckdb_inner(&request, schema_hash, package_dir, execution).await {
                Ok(report) => Ok(report),
                Err(error) => {
                    let _ = recorder.append_run_failed();
                    Err(error)
                }
            }
        }
        ProjectRunDestination::ParquetFilesystem { root, target } => {
            let run_ledger = SqliteRunLedger::open(&request.state_store_path)?;
            let run = run_ledger.create_run(request.run_id.clone())?;
            let checkpoint_store = SqliteCheckpointStore::open(&request.state_store_path)?;
            let destination = ParquetDestination::new_filesystem(root)?;
            let recorder = ProjectRunRecorder::new(
                &run_ledger,
                run.run_id,
                recorder_context(
                    &request,
                    &package_dir,
                    destination.sheet().destination.clone(),
                ),
            );
            let execution = ProjectParquetRunExecution {
                target,
                checkpoint_store: &checkpoint_store,
                destination: &destination,
                recorder: &recorder,
            };
            match run_project_parquet_inner(&request, schema_hash, package_dir, execution).await {
                Ok(report) => Ok(report),
                Err(error) => {
                    let _ = recorder.append_run_failed();
                    Err(error)
                }
            }
        }
        ProjectRunDestination::Postgres { database_url, .. } => {
            let run_ledger = SqliteRunLedger::open(&request.state_store_path)?;
            let run = run_ledger.create_run(request.run_id.clone())?;
            let checkpoint_store = SqliteCheckpointStore::open(&request.state_store_path)?;
            let destination = PostgresDestination::connect(database_url.clone())?;
            let recorder = ProjectRunRecorder::new(
                &run_ledger,
                run.run_id,
                recorder_context(
                    &request,
                    &package_dir,
                    destination.sheet().destination.clone(),
                ),
            );
            let execution = ProjectPostgresRunExecution {
                checkpoint_store: &checkpoint_store,
                destination: &destination,
                recorder: &recorder,
            };
            match run_project_postgres_inner(&request, schema_hash, package_dir, execution).await {
                Ok(report) => Ok(report),
                Err(error) => {
                    let _ = recorder.append_run_failed();
                    Err(error)
                }
            }
        }
    }
}

fn recorder_context(
    request: &ProjectRunRequest<'_>,
    package_dir: &Path,
    destination_id: DestinationId,
) -> ProjectRunRecorderContext {
    ProjectRunRecorderContext {
        resource_id: request.resource.descriptor().resource_id.clone(),
        scope: request.resource.descriptor().state_scope.clone(),
        package_id: request.package_id.clone(),
        package_path: package_dir.display().to_string(),
        destination_id,
        plan_id: request.plan.scan.plan_id.clone(),
        pipeline_id: request.pipeline_id.clone(),
    }
}

async fn run_project_duckdb_inner(
    request: &ProjectRunRequest<'_>,
    schema_hash: SchemaHash,
    package_dir: PathBuf,
    execution: ProjectDuckDbRunExecution<'_>,
) -> Result<ProjectRunReport> {
    execution.recorder.append_run_started()?;
    execution.recorder.append_plan_recorded()?;
    execution.recorder.append_package_started()?;

    let resource = request.resource.stream();
    let descriptor = resource.descriptor();
    let schema = resource.schema();
    let scope = descriptor.state_scope.clone();
    let head =
        execution
            .checkpoint_store
            .head(&request.pipeline_id, &descriptor.resource_id, &scope)?;

    let write_state_commit_artifacts =
        |builder: &cdf_package::PackageBuilder, draft: EnginePackageDraft<'_>| {
            write_run_state_commit_artifacts(
                builder,
                draft,
                &StateCommitArtifactContext {
                    descriptor,
                    schema: schema.as_ref(),
                    pipeline_id: &request.pipeline_id,
                    checkpoint_id: &request.checkpoint_id,
                    target: execution.target,
                },
                &schema_hash,
                &scope,
                &head,
            )
        };
    let output = execute_to_package_with_segment_positions_and_pre_finalize(
        &request.plan,
        resource,
        &package_dir,
        &write_state_commit_artifacts,
    )
    .await?;

    let replay_inputs = PackageReader::open(&package_dir)?.replay_inputs()?;
    let package_hash = replay_inputs.state_delta.package_hash.clone();
    let row_count = output.output.profile.output_rows;
    let segment_count = output.output.segments.len();
    execution
        .recorder
        .append_package_finalized(&package_hash, row_count, segment_count)?;

    let stage_hook =
        |stage: DestinationReplayStage<'_>| execution.recorder.append_replay_stage(stage);
    let replay_report = replay_duckdb_package_from_artifacts_with_hooks(
        PackageArtifactDuckDbReplayRequest {
            package_dir: package_dir.clone(),
            destination: execution.destination,
            checkpoint_store: execution.checkpoint_store,
            after_receipt_verified: request.after_receipt_verified,
        },
        Some(&stage_hook),
        execution.lifecycle_failpoint,
    )?;
    execution.recorder.append_run_succeeded()?;
    let ledger_snapshot = execution.recorder.snapshot()?;

    Ok(ProjectRunReport {
        run_id: execution.recorder.run_id.clone(),
        ledger_snapshot,
        package_dir,
        package_id: request.package_id.clone(),
        package_hash,
        package_status: replay_report.package_status,
        checkpoint: replay_report.checkpoint,
        receipt: replay_report.receipt,
        receipt_source: replay_report.receipt_source.into(),
        row_count,
        segment_count,
    })
}

struct ProjectDuckDbRunExecution<'a> {
    target: &'a TargetName,
    checkpoint_store: &'a SqliteCheckpointStore,
    destination: &'a DuckDbDestination,
    recorder: &'a ProjectRunRecorder<'a>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'a>>,
}

async fn run_project_parquet_inner(
    request: &ProjectRunRequest<'_>,
    schema_hash: SchemaHash,
    package_dir: PathBuf,
    execution: ProjectParquetRunExecution<'_>,
) -> Result<ProjectRunReport> {
    execution.recorder.append_run_started()?;
    execution.recorder.append_plan_recorded()?;
    execution.recorder.append_package_started()?;

    let resource = request.resource.stream();
    let descriptor = resource.descriptor();
    let schema = resource.schema();
    let scope = descriptor.state_scope.clone();
    let head =
        execution
            .checkpoint_store
            .head(&request.pipeline_id, &descriptor.resource_id, &scope)?;

    let write_state_commit_artifacts =
        |builder: &cdf_package::PackageBuilder, draft: EnginePackageDraft<'_>| {
            write_run_state_commit_artifacts(
                builder,
                draft,
                &StateCommitArtifactContext {
                    descriptor,
                    schema: schema.as_ref(),
                    pipeline_id: &request.pipeline_id,
                    checkpoint_id: &request.checkpoint_id,
                    target: execution.target,
                },
                &schema_hash,
                &scope,
                &head,
            )
        };
    let output = execute_to_package_with_segment_positions_and_pre_finalize(
        &request.plan,
        resource,
        &package_dir,
        &write_state_commit_artifacts,
    )
    .await?;

    let replay_inputs = PackageReader::open(&package_dir)?.replay_inputs()?;
    let package_hash = replay_inputs.state_delta.package_hash.clone();
    let row_count = output.output.profile.output_rows;
    let segment_count = output.output.segments.len();
    execution
        .recorder
        .append_package_finalized(&package_hash, row_count, segment_count)?;

    let stage_hook =
        |stage: DestinationReplayStage<'_>| execution.recorder.append_replay_stage(stage);
    let replay_report = replay_parquet_package_with_inputs(
        PackageReader::open(&package_dir)?,
        package_dir.clone(),
        execution.destination,
        execution.checkpoint_store,
        ParquetPackageReplayInputs::from_package_artifacts(replay_inputs),
        ParquetReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: Some(&stage_hook),
        },
    )?;
    execution.recorder.append_run_succeeded()?;
    let ledger_snapshot = execution.recorder.snapshot()?;

    Ok(ProjectRunReport {
        run_id: execution.recorder.run_id.clone(),
        ledger_snapshot,
        package_dir,
        package_id: request.package_id.clone(),
        package_hash,
        package_status: replay_report.package_status,
        checkpoint: replay_report.checkpoint,
        receipt: replay_report.receipt,
        receipt_source: replay_report.receipt_source,
        row_count,
        segment_count,
    })
}

struct ProjectParquetRunExecution<'a> {
    target: &'a TargetName,
    checkpoint_store: &'a SqliteCheckpointStore,
    destination: &'a ParquetDestination,
    recorder: &'a ProjectRunRecorder<'a>,
}

async fn run_project_postgres_inner(
    request: &ProjectRunRequest<'_>,
    schema_hash: SchemaHash,
    package_dir: PathBuf,
    execution: ProjectPostgresRunExecution<'_>,
) -> Result<ProjectRunReport> {
    execution.recorder.append_run_started()?;
    execution.recorder.append_plan_recorded()?;
    execution.recorder.append_package_started()?;

    let resource = request.resource.stream();
    let descriptor = resource.descriptor();
    let schema = resource.schema();
    let scope = descriptor.state_scope.clone();
    let head =
        execution
            .checkpoint_store
            .head(&request.pipeline_id, &descriptor.resource_id, &scope)?;
    let target = postgres_target(request)?;

    let write_state_commit_artifacts =
        |builder: &cdf_package::PackageBuilder, draft: EnginePackageDraft<'_>| {
            write_run_state_commit_artifacts(
                builder,
                draft,
                &StateCommitArtifactContext {
                    descriptor,
                    schema: schema.as_ref(),
                    pipeline_id: &request.pipeline_id,
                    checkpoint_id: &request.checkpoint_id,
                    target: &target,
                },
                &schema_hash,
                &scope,
                &head,
            )
        };
    let output = execute_to_package_with_segment_positions_and_pre_finalize(
        &request.plan,
        resource,
        &package_dir,
        &write_state_commit_artifacts,
    )
    .await?;

    let replay_inputs = PackageReader::open(&package_dir)?.replay_inputs()?;
    let package_hash = replay_inputs.state_delta.package_hash.clone();
    let row_count = output.output.profile.output_rows;
    let segment_count = output.output.segments.len();
    execution
        .recorder
        .append_package_finalized(&package_hash, row_count, segment_count)?;

    let stage_hook =
        |stage: DestinationReplayStage<'_>| execution.recorder.append_replay_stage(stage);
    let replay_report = replay_postgres_package_with_inputs(
        PackageReader::open(&package_dir)?,
        package_dir.clone(),
        execution.destination,
        execution.checkpoint_store,
        PostgresPackageReplayInputs::from_package_artifacts(
            request,
            &PackageReader::open(&package_dir)?,
            replay_inputs,
        )?,
        PostgresReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: Some(&stage_hook),
        },
    )?;
    execution.recorder.append_run_succeeded()?;
    let ledger_snapshot = execution.recorder.snapshot()?;

    Ok(ProjectRunReport {
        run_id: execution.recorder.run_id.clone(),
        ledger_snapshot,
        package_dir,
        package_id: request.package_id.clone(),
        package_hash,
        package_status: replay_report.package_status,
        checkpoint: replay_report.checkpoint,
        receipt: replay_report.receipt,
        receipt_source: replay_report.receipt_source,
        row_count,
        segment_count,
    })
}

struct ProjectPostgresRunExecution<'a> {
    checkpoint_store: &'a SqliteCheckpointStore,
    destination: &'a PostgresDestination,
    recorder: &'a ProjectRunRecorder<'a>,
}

fn write_run_state_commit_artifacts(
    builder: &cdf_package::PackageBuilder,
    draft: EnginePackageDraft<'_>,
    context: &StateCommitArtifactContext<'_>,
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: &Option<Checkpoint>,
) -> Result<()> {
    let state_delta = state_delta_preimage_from_run_draft(
        context,
        draft.segments,
        draft.segment_positions,
        schema_hash,
        scope,
        head.as_ref(),
    )?;
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        context.target.clone(),
        context.descriptor.write_disposition.clone(),
        context.descriptor.merge_key.clone(),
        schema_hash.clone(),
        state_delta.segments.clone(),
    );
    builder.write_input_checkpoint_artifact(head)?;
    builder.write_state_delta_preimage_artifact(&state_delta)?;
    builder.write_commit_plan_preimage_artifact(&commit_plan)?;
    Ok(())
}

fn validate_project_run_request(request: &ProjectRunRequest<'_>) -> Result<()> {
    request.resource.validate_supported()?;
    validate_checkpointable_source_position(request.resource)?;
    validate_run_plan(
        request.resource.stream(),
        &request.plan,
        &request.package_id,
    )?;
    match &request.destination {
        ProjectRunDestination::DuckDb { database_path, .. } => {
            let destination = DuckDbDestination::new(database_path)?;
            if !destination
                .sheet()
                .supported_dispositions
                .contains(&request.resource.descriptor().write_disposition)
            {
                return Err(CdfError::contract(format!(
                    "DuckDB destination does not support {:?}",
                    request.resource.descriptor().write_disposition
                )));
            }
        }
        ProjectRunDestination::ParquetFilesystem { .. } => {
            if !matches!(
                request.resource.descriptor().write_disposition,
                WriteDisposition::Append | WriteDisposition::Replace
            ) {
                return Err(CdfError::contract(format!(
                    "Parquet destination does not support {:?}; append and replace are supported in this slice",
                    request.resource.descriptor().write_disposition
                )));
            }
        }
        ProjectRunDestination::Postgres { database_url, .. } => {
            PostgresDestination::connect(database_url.clone())?;
            validate_postgres_preflight(request)?;
        }
    }
    Ok(())
}

fn validate_postgres_preflight(request: &ProjectRunRequest<'_>) -> Result<()> {
    let resource = request.resource.stream();
    let schema_hash = declared_schema_hash(resource)?;
    let delta = postgres_preflight_delta(resource, &schema_hash)?;
    let commit = commit_request(
        &delta,
        postgres_target(request)?,
        resource.descriptor().write_disposition.clone(),
    )?;
    let replay = PackageReplayInputs {
        input_checkpoint: None,
        state_delta: delta,
        destination_commit: commit,
        schema_hash,
        merge_keys: Vec::new(),
    };
    let input =
        postgres_load_plan_input(request, &replay, postgres_columns_from_schema(resource)?)?;
    PostgresDestination::new().plan_load(input)?;
    Ok(())
}

fn postgres_preflight_delta(
    resource: &dyn ResourceStream,
    schema_hash: &SchemaHash,
) -> Result<StateDelta> {
    let descriptor = resource.descriptor();
    let segment = StateSegment {
        segment_id: SegmentId::new("seg-postgres-preflight")?,
        scope: descriptor.state_scope.clone(),
        output_position: SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "preflight".to_owned(),
            value: CursorValue::I64(0),
        }),
        row_count: 1,
        byte_count: 1,
    };
    Ok(StateDelta {
        checkpoint_id: CheckpointId::new("checkpoint-postgres-preflight")?,
        pipeline_id: PipelineId::new("pipeline-postgres-preflight")?,
        resource_id: descriptor.resource_id.clone(),
        scope: descriptor.state_scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: segment.output_position.clone(),
        package_hash: PackageHash::new("sha256:postgres-preflight")?,
        schema_hash: schema_hash.clone(),
        segments: vec![segment],
    })
}

fn postgres_target(request: &ProjectRunRequest<'_>) -> Result<TargetName> {
    match &request.destination {
        ProjectRunDestination::Postgres { target, .. } => TargetName::new(target.display_name()),
        _ => Err(CdfError::internal(
            "postgres target requested for non-Postgres project destination",
        )),
    }
}

fn postgres_load_plan_input(
    request: &ProjectRunRequest<'_>,
    inputs: &PackageReplayInputs,
    columns: Vec<PostgresColumn>,
) -> Result<PostgresLoadPlanInput> {
    let ProjectRunDestination::Postgres {
        target,
        dedup,
        existing_table,
        ..
    } = &request.destination
    else {
        return Err(CdfError::internal(
            "postgres load plan requested for non-Postgres project destination",
        ));
    };
    let descriptor = request.resource.descriptor();
    Ok(PostgresLoadPlanInput {
        package_hash: inputs.state_delta.package_hash.clone(),
        idempotency_token: inputs.destination_commit.idempotency_token.clone(),
        target: target.clone(),
        disposition: descriptor.write_disposition.clone(),
        schema_hash: inputs.schema_hash.clone(),
        segments: inputs.state_delta.segments.clone(),
        columns,
        merge_keys: postgres_merge_keys(descriptor)?,
        dedup: dedup.clone(),
        existing_table: existing_table.clone(),
        resource_id: Some(descriptor.resource_id.clone()),
        state_delta: Some(inputs.state_delta.clone()),
    })
}

fn postgres_load_plan_input_from_artifacts(
    inputs: &PackageReplayInputs,
    target: PostgresTarget,
    dedup: MergeDedupPolicy,
    existing_table: Option<PostgresExistingTable>,
    columns: Vec<PostgresColumn>,
) -> Result<PostgresLoadPlanInput> {
    validate_postgres_replay_target(&target, &inputs.destination_commit.target)?;
    Ok(PostgresLoadPlanInput {
        package_hash: inputs.state_delta.package_hash.clone(),
        idempotency_token: inputs.destination_commit.idempotency_token.clone(),
        target,
        disposition: inputs.destination_commit.disposition.clone(),
        schema_hash: inputs.schema_hash.clone(),
        segments: inputs.state_delta.segments.clone(),
        columns,
        merge_keys: postgres_merge_keys_from_artifacts(&inputs.merge_keys)?,
        dedup,
        existing_table,
        resource_id: Some(inputs.state_delta.resource_id.clone()),
        state_delta: Some(inputs.state_delta.clone()),
    })
}

fn validate_postgres_replay_target(
    target: &PostgresTarget,
    package_target: &TargetName,
) -> Result<()> {
    let explicit = target.display_name();
    if explicit != package_target.as_str() {
        return Err(CdfError::contract(format!(
            "explicit Postgres replay target {explicit} does not match package destination commit target {package_target}"
        )));
    }
    Ok(())
}

fn postgres_merge_keys(descriptor: &ResourceDescriptor) -> Result<Vec<PostgresIdentifier>> {
    if descriptor.write_disposition != WriteDisposition::Merge {
        return Ok(Vec::new());
    }
    descriptor
        .merge_key
        .iter()
        .map(PostgresIdentifier::user)
        .collect()
}

fn postgres_merge_keys_from_artifacts(keys: &[String]) -> Result<Vec<PostgresIdentifier>> {
    keys.iter().map(PostgresIdentifier::user).collect()
}

fn postgres_columns_from_schema(resource: &dyn ResourceStream) -> Result<Vec<PostgresColumn>> {
    postgres_columns_for_schema(resource.schema().as_ref())
}

fn postgres_columns_from_package(reader: &PackageReader) -> Result<Vec<PostgresColumn>> {
    let segments = reader.read_all_segments()?;
    let schema = segments
        .iter()
        .flat_map(|(_, batches)| batches.iter())
        .next()
        .map(|batch| batch.schema())
        .ok_or_else(|| {
            CdfError::data("Postgres destination requires at least one package batch")
        })?;
    postgres_columns_for_schema(schema.as_ref())
}

fn validate_checkpointable_source_position(resource: ProjectRunResource<'_>) -> Result<()> {
    match resource {
        ProjectRunResource::LocalFile(_) => Ok(()),
        ProjectRunResource::Rest(_) | ProjectRunResource::Sql(_) => {
            let descriptor = resource.descriptor();
            let cursor = descriptor.cursor.as_ref().ok_or_else(|| {
                CdfError::contract(format!(
                    "cdf run requires non-file resource `{}` to declare an ordered cursor; page-token-only checkpoint semantics are not ratified",
                    descriptor.resource_id
                ))
            })?;
            if cursor.ordering == CursorOrderingClaim::Unordered {
                return Err(CdfError::contract(format!(
                    "cdf run requires non-file resource `{}` to declare an ordered cursor for checkpoint advancement",
                    descriptor.resource_id
                )));
            }
            Ok(())
        }
    }
}

fn validate_local_file_run_resource(resource: &CompiledResource) -> Result<()> {
    match resource.plan() {
        CompiledResourcePlan::Files(_) => Ok(()),
        CompiledResourcePlan::Rest(_) => Err(CdfError::contract(
            "cdf run local-file resource input supports only declarative local file resources; use RestResource for REST execution",
        )),
        CompiledResourcePlan::Sql(_) => Err(CdfError::contract(
            "cdf run local-file resource input supports only declarative local file resources; use SqlResource for SQL execution",
        )),
    }
}

fn validate_run_plan(
    resource: &dyn ResourceStream,
    plan: &EnginePlan,
    package_id: &str,
) -> Result<()> {
    let descriptor = resource.descriptor();
    if plan.scan.request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "run plan resource {} does not match selected resource {}",
            plan.scan.request.resource_id, descriptor.resource_id
        )));
    }
    if plan.package_id != package_id {
        return Err(CdfError::contract(format!(
            "run plan package id {} does not match explicit package id {}",
            plan.package_id, package_id
        )));
    }
    if plan.scan.request.scope != descriptor.state_scope {
        return Err(CdfError::contract(
            "run plan scope must come from the current resource descriptor state scope",
        ));
    }
    Ok(())
}

fn declared_schema_hash(resource: &dyn ResourceStream) -> Result<SchemaHash> {
    match &resource.descriptor().schema_source {
        SchemaSource::Declared { schema_hash, .. } => Ok(schema_hash.clone()),
        SchemaSource::Discovered { schema_hash: None } => Err(CdfError::contract(
            "cdf run requires a declared schema with a concrete schema hash; discovered schema resources are unsupported in this slice",
        )),
        SchemaSource::Discovered {
            schema_hash: Some(_),
        } => Err(CdfError::contract(
            "cdf run requires SchemaSource::Declared; discovered schema hashes are unsupported in this slice",
        )),
        SchemaSource::Contract { .. } => Err(CdfError::contract(
            "cdf run requires SchemaSource::Declared; contract-sourced schemas are unsupported in this slice",
        )),
    }
}

fn refuse_existing_package_dir(package_dir: &Path) -> Result<()> {
    if package_dir.exists() {
        return Err(CdfError::data(format!(
            "package directory already exists at {}; explicit run package ids must not overwrite existing packages",
            package_dir.display()
        )));
    }
    Ok(())
}

fn ensure_parent_directory(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| CdfError::internal(format!("create {}: {error}", parent.display())))?;
    }
    Ok(())
}

fn validate_explicit_package_id(package_id: &str) -> Result<()> {
    if package_id.trim().is_empty() {
        return Err(CdfError::contract("run package id cannot be empty"));
    }
    let mut components = Path::new(package_id).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(CdfError::contract(
            "run package id must be one path component under the environment package root",
        )),
    }
}

#[cfg(test)]
pub(crate) fn state_delta_from_run(
    request: &LocalFileDuckDbRunRequest<'_>,
    output: &EngineRunOutputWithSegmentPositions,
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: Option<&Checkpoint>,
) -> Result<StateDelta> {
    let schema = request.resource.schema();
    let context = StateCommitArtifactContext {
        descriptor: request.resource.descriptor(),
        schema: schema.as_ref(),
        pipeline_id: &request.pipeline_id,
        checkpoint_id: &request.checkpoint_id,
        target: &request.target,
    };
    let preimage = state_delta_preimage_from_run_draft(
        &context,
        &output.output.segments,
        &output.segment_positions,
        schema_hash,
        scope,
        head,
    )?;
    Ok(preimage.into_state_delta(PackageHash::new(
        output.output.manifest.package_hash.clone(),
    )?))
}

struct StateCommitArtifactContext<'a> {
    descriptor: &'a ResourceDescriptor,
    schema: &'a Schema,
    pipeline_id: &'a PipelineId,
    checkpoint_id: &'a CheckpointId,
    target: &'a TargetName,
}

fn state_delta_preimage_from_run_draft(
    context: &StateCommitArtifactContext<'_>,
    segments: &[SegmentEntry],
    segment_positions: &[cdf_engine::EngineSegmentPosition],
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: Option<&Checkpoint>,
) -> Result<StateDeltaPreimage> {
    let positions = segment_positions_by_id(segments, segment_positions)?;
    let mut segment_evidence = Vec::with_capacity(segments.len());

    for segment in segments {
        let segment_position = positions
            .get(&segment.segment_id)
            .ok_or_else(|| {
                CdfError::internal(format!(
                    "engine output omitted source position evidence for segment {}",
                    segment.segment_id
                ))
            })?
            .clone()
            .ok_or_else(|| {
                CdfError::data(format!(
                    "package segment {} has no source position evidence; cdf run cannot checkpoint without source position evidence",
                    segment.segment_id
                ))
            })?;
        let segment_position = normalize_source_position_for_scope(segment_position, scope);
        segment_evidence.push((segment, segment_position));
    }

    let output_positions = segment_evidence
        .iter()
        .map(|(_, position)| position.clone())
        .collect::<Vec<_>>();
    let output_position = aggregate_output_position(context, &output_positions)?;
    let state_segments = segment_evidence
        .into_iter()
        .map(|(segment, segment_position)| StateSegment {
            segment_id: segment.segment_id.clone(),
            scope: scope.clone(),
            output_position: segment_position,
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        })
        .collect();
    Ok(StateDeltaPreimage {
        checkpoint_id: context.checkpoint_id.clone(),
        pipeline_id: context.pipeline_id.clone(),
        resource_id: context.descriptor.resource_id.clone(),
        scope: scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: head.map(|checkpoint| checkpoint.delta.checkpoint_id.clone()),
        input_position: head.map(|checkpoint| checkpoint.delta.output_position.clone()),
        output_position,
        schema_hash: schema_hash.clone(),
        segments: state_segments,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CursorArithmetic {
    I64,
    U64,
    TimestampMicros,
    Date32,
}

fn aggregate_output_position(
    context: &StateCommitArtifactContext<'_>,
    positions: &[SourcePosition],
) -> Result<SourcePosition> {
    if positions.is_empty() {
        return Err(CdfError::data(
            "package execution produced no output segments to checkpoint",
        ));
    }
    if context.descriptor.cursor.is_some() {
        aggregate_cursor_output_position(context, positions)
    } else {
        identical_output_position(positions)
    }
}

fn identical_output_position(positions: &[SourcePosition]) -> Result<SourcePosition> {
    let first = positions
        .first()
        .expect("aggregate_output_position checks non-empty positions");
    if positions.iter().any(|position| position != first) {
        return Err(CdfError::data(
            "single resource run produced divergent segment source positions",
        ));
    }
    Ok(first.clone())
}

fn aggregate_cursor_output_position(
    context: &StateCommitArtifactContext<'_>,
    positions: &[SourcePosition],
) -> Result<SourcePosition> {
    let cursor = context
        .descriptor
        .cursor
        .as_ref()
        .expect("aggregate_output_position routes only cursor descriptors");
    if cursor.ordering == CursorOrderingClaim::Unordered {
        return Err(CdfError::contract(format!(
            "resource `{}` cursor field `{}` is unordered and cannot advance checkpoints",
            context.descriptor.resource_id, cursor.field
        )));
    }

    let arithmetic = cursor_arithmetic(context)?;
    let cursor_positions = cursor_positions_for_aggregation(context, positions)?;
    let mut max_position = None::<&CursorPosition>;
    for position in cursor_positions {
        if position.field != cursor.field {
            return Err(CdfError::data(format!(
                "source position cursor field `{}` does not match resource cursor field `{}`",
                position.field, cursor.field
            )));
        }
        ensure_cursor_value_supported(context, arithmetic, &position.value)?;
        if max_position
            .is_none_or(|current| cursor_value_greater(arithmetic, &position.value, &current.value))
        {
            max_position = Some(position);
        }
    }

    let max_position =
        max_position.expect("cursor_positions_for_aggregation returns one cursor per position");
    let closed_value = close_cursor_value(
        context,
        arithmetic,
        &max_position.value,
        cursor.lag_tolerance_ms,
    )?;
    Ok(SourcePosition::Cursor(CursorPosition {
        version: max_position.version,
        field: cursor.field.clone(),
        value: closed_value,
    }))
}

fn cursor_arithmetic(context: &StateCommitArtifactContext<'_>) -> Result<CursorArithmetic> {
    let cursor = context
        .descriptor
        .cursor
        .as_ref()
        .expect("cursor_arithmetic is called only for cursor descriptors");
    let field = context.schema.field_with_name(&cursor.field).map_err(|_| {
        CdfError::contract(format!(
            "resource `{}` cursor field `{}` is missing from the declared schema",
            context.descriptor.resource_id, cursor.field
        ))
    })?;
    match field.data_type() {
        DataType::Int64 => Ok(CursorArithmetic::I64),
        DataType::UInt64 => Ok(CursorArithmetic::U64),
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
            _,
        ) => Ok(CursorArithmetic::TimestampMicros),
        DataType::Date32 => Ok(CursorArithmetic::Date32),
        other => Err(CdfError::contract(format!(
            "resource `{}` cursor field `{}` has unsupported cursor value kind {other}; only int64, uint64, timestamp, and date32 cursors have ratified window-close semantics",
            context.descriptor.resource_id, cursor.field
        ))),
    }
}

fn cursor_positions_for_aggregation<'a>(
    context: &StateCommitArtifactContext<'_>,
    positions: &'a [SourcePosition],
) -> Result<Vec<&'a CursorPosition>> {
    let mut cursor_positions = Vec::with_capacity(positions.len());
    let mut saw_cursor = false;
    let mut saw_page_token = false;
    let mut saw_non_cursor_variant = false;

    for position in positions {
        match position {
            SourcePosition::Cursor(cursor) => {
                saw_cursor = true;
                cursor_positions.push(cursor);
            }
            SourcePosition::PageToken(_) => saw_page_token = true,
            SourcePosition::Composite(composite) => {
                saw_non_cursor_variant = true;
                let summary = composite_position_summary(composite);
                saw_cursor |= summary.saw_cursor;
                saw_page_token |= summary.saw_page_token;
            }
            SourcePosition::Log(_)
            | SourcePosition::FileManifest(_)
            | SourcePosition::ForeignState(_) => saw_non_cursor_variant = true,
        }
    }

    if saw_page_token && saw_cursor {
        return Err(CdfError::data(format!(
            "resource `{}` produced mixed cursor/page-token source positions; mixed pagination transport and checkpoint cursor semantics are not ratified",
            context.descriptor.resource_id
        )));
    }
    if saw_page_token {
        return Err(CdfError::data(format!(
            "resource `{}` produced page-token-only source positions; page tokens are pagination transport and cannot advance checkpoints",
            context.descriptor.resource_id
        )));
    }
    if saw_non_cursor_variant || cursor_positions.len() != positions.len() {
        return Err(CdfError::data(format!(
            "resource `{}` produced divergent source-position variants; non-file checkpoint advancement requires cursor positions only",
            context.descriptor.resource_id
        )));
    }

    Ok(cursor_positions)
}

#[derive(Clone, Copy, Debug, Default)]
struct PositionSummary {
    saw_cursor: bool,
    saw_page_token: bool,
}

fn composite_position_summary(composite: &cdf_kernel::CompositePosition) -> PositionSummary {
    let mut summary = PositionSummary::default();
    for position in composite.positions.values() {
        match position {
            SourcePosition::Cursor(_) => summary.saw_cursor = true,
            SourcePosition::PageToken(_) => summary.saw_page_token = true,
            SourcePosition::Composite(nested) => {
                let nested = composite_position_summary(nested);
                summary.saw_cursor |= nested.saw_cursor;
                summary.saw_page_token |= nested.saw_page_token;
            }
            SourcePosition::Log(_)
            | SourcePosition::FileManifest(_)
            | SourcePosition::ForeignState(_) => {}
        }
    }
    summary
}

fn ensure_cursor_value_supported(
    context: &StateCommitArtifactContext<'_>,
    arithmetic: CursorArithmetic,
    value: &CursorValue,
) -> Result<()> {
    let supported = matches!(
        (arithmetic, value),
        (
            CursorArithmetic::I64 | CursorArithmetic::Date32,
            CursorValue::I64(_)
        ) | (CursorArithmetic::U64, CursorValue::U64(_))
            | (
                CursorArithmetic::TimestampMicros,
                CursorValue::TimestampMicros { .. }
            )
    );
    if supported {
        return Ok(());
    }
    let cursor = context
        .descriptor
        .cursor
        .as_ref()
        .expect("ensure_cursor_value_supported is called only for cursor descriptors");
    Err(CdfError::data(format!(
        "resource `{}` cursor field `{}` produced unsupported cursor value kind for its declared schema",
        context.descriptor.resource_id, cursor.field
    )))
}

fn cursor_value_greater(
    arithmetic: CursorArithmetic,
    left: &CursorValue,
    right: &CursorValue,
) -> bool {
    match (arithmetic, left, right) {
        (
            CursorArithmetic::I64 | CursorArithmetic::Date32,
            CursorValue::I64(left),
            CursorValue::I64(right),
        ) => left > right,
        (CursorArithmetic::U64, CursorValue::U64(left), CursorValue::U64(right)) => left > right,
        (
            CursorArithmetic::TimestampMicros,
            CursorValue::TimestampMicros { micros: left, .. },
            CursorValue::TimestampMicros { micros: right, .. },
        ) => left > right,
        _ => false,
    }
}

fn close_cursor_value(
    context: &StateCommitArtifactContext<'_>,
    arithmetic: CursorArithmetic,
    value: &CursorValue,
    lag_tolerance_ms: u64,
) -> Result<CursorValue> {
    match (arithmetic, value) {
        (CursorArithmetic::I64, CursorValue::I64(value)) => {
            let lag = i64::try_from(lag_tolerance_ms)
                .map_err(|_| incompatible_cursor_lag(context, lag_tolerance_ms))?;
            value
                .checked_sub(lag)
                .map(CursorValue::I64)
                .ok_or_else(|| incompatible_cursor_lag(context, lag_tolerance_ms))
        }
        (CursorArithmetic::U64, CursorValue::U64(value)) => value
            .checked_sub(lag_tolerance_ms)
            .map(CursorValue::U64)
            .ok_or_else(|| incompatible_cursor_lag(context, lag_tolerance_ms)),
        (CursorArithmetic::TimestampMicros, CursorValue::TimestampMicros { micros, timezone }) => {
            let lag_micros = lag_tolerance_ms
                .checked_mul(1_000)
                .and_then(|value| i64::try_from(value).ok())
                .ok_or_else(|| incompatible_cursor_lag(context, lag_tolerance_ms))?;
            micros
                .checked_sub(lag_micros)
                .map(|micros| CursorValue::TimestampMicros {
                    micros,
                    timezone: timezone.clone(),
                })
                .ok_or_else(|| incompatible_cursor_lag(context, lag_tolerance_ms))
        }
        (CursorArithmetic::Date32, CursorValue::I64(value)) => {
            const MILLIS_PER_DAY: u64 = 86_400_000;
            if !lag_tolerance_ms.is_multiple_of(MILLIS_PER_DAY) {
                return Err(incompatible_cursor_lag(context, lag_tolerance_ms));
            }
            let lag_days = i64::try_from(lag_tolerance_ms / MILLIS_PER_DAY)
                .map_err(|_| incompatible_cursor_lag(context, lag_tolerance_ms))?;
            value
                .checked_sub(lag_days)
                .map(CursorValue::I64)
                .ok_or_else(|| incompatible_cursor_lag(context, lag_tolerance_ms))
        }
        _ => Err(incompatible_cursor_lag(context, lag_tolerance_ms)),
    }
}

fn incompatible_cursor_lag(
    context: &StateCommitArtifactContext<'_>,
    lag_tolerance_ms: u64,
) -> CdfError {
    let cursor = context
        .descriptor
        .cursor
        .as_ref()
        .expect("incompatible_cursor_lag is called only for cursor descriptors");
    CdfError::data(format!(
        "resource `{}` cursor field `{}` has incompatible cursor lag {}ms for the observed cursor value",
        context.descriptor.resource_id, cursor.field, lag_tolerance_ms
    ))
}

fn normalize_source_position_for_scope(
    position: SourcePosition,
    scope: &ScopeKey,
) -> SourcePosition {
    match (scope, position) {
        (ScopeKey::File { path }, SourcePosition::FileManifest(mut manifest)) => {
            for file in &mut manifest.files {
                file.path = path.clone();
            }
            SourcePosition::FileManifest(manifest)
        }
        (_, position) => position,
    }
}

fn segment_positions_by_id(
    segments: &[SegmentEntry],
    segment_positions: &[cdf_engine::EngineSegmentPosition],
) -> Result<BTreeMap<SegmentId, Option<SourcePosition>>> {
    if segment_positions.len() != segments.len() {
        return Err(CdfError::internal(format!(
            "engine output has {} segment(s) but {} segment source position record(s)",
            segments.len(),
            segment_positions.len()
        )));
    }

    let positions = segment_positions
        .iter()
        .map(|position| {
            (
                position.segment_id.clone(),
                position.output_position.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if positions.len() != segment_positions.len() {
        return Err(CdfError::internal(
            "engine output contains duplicate segment source position records",
        ));
    }
    Ok(positions)
}

struct ProjectRunRecorderContext {
    resource_id: ResourceId,
    scope: ScopeKey,
    package_id: String,
    package_path: String,
    destination_id: DestinationId,
    plan_id: PlanId,
    pipeline_id: PipelineId,
}

struct ProjectRunRecorder<'a> {
    ledger: &'a SqliteRunLedger,
    run_id: RunId,
    context: ProjectRunRecorderContext,
}

impl<'a> ProjectRunRecorder<'a> {
    fn new(ledger: &'a SqliteRunLedger, run_id: RunId, context: ProjectRunRecorderContext) -> Self {
        Self {
            ledger,
            run_id,
            context,
        }
    }

    fn append_run_started(&self) -> Result<()> {
        let mut event = self.base_event(RunEventKind::RunStarted);
        event.details = RunEventDetails::new([(
            "pipeline_id",
            RunEventValue::String(self.context.pipeline_id.as_str().to_owned()),
        )]);
        self.append(event)
    }

    fn append_plan_recorded(&self) -> Result<()> {
        let mut event = self.base_event(RunEventKind::PlanRecorded);
        event.details = RunEventDetails::new([("planned_packages", RunEventValue::U64(1))]);
        self.append(event)
    }

    fn append_package_started(&self) -> Result<()> {
        self.append(self.base_event(RunEventKind::PackageStarted))
    }

    fn append_package_finalized(
        &self,
        package_hash: &PackageHash,
        row_count: u64,
        segment_count: usize,
    ) -> Result<()> {
        let mut event = self.base_event(RunEventKind::PackageFinalized);
        event.package_hash = Some(package_hash.clone());
        event.details = RunEventDetails::new([
            ("row_count", RunEventValue::U64(row_count)),
            (
                "segment_count",
                RunEventValue::U64(
                    u64::try_from(segment_count)
                        .map_err(|error| CdfError::internal(error.to_string()))?,
                ),
            ),
        ]);
        self.append(event)
    }

    fn append_replay_stage(&self, stage: DestinationReplayStage<'_>) -> Result<()> {
        match stage {
            DestinationReplayStage::CheckpointProposed { delta } => {
                let mut event = self.base_event(RunEventKind::CheckpointProposed);
                event.checkpoint_id = Some(delta.checkpoint_id.clone());
                event.package_hash = Some(delta.package_hash.clone());
                self.append(event)
            }
            DestinationReplayStage::DestinationCommitStarted { plan_id } => {
                let mut event = self.base_event(RunEventKind::DestinationCommitStarted);
                event.plan_id = Some(plan_id.clone());
                self.append(event)
            }
            DestinationReplayStage::DestinationReceiptRecorded { receipt } => {
                let mut event = self.base_event(RunEventKind::DestinationReceiptRecorded);
                event.package_hash = Some(receipt.package_hash.clone());
                event.receipt_id = Some(receipt.receipt_id.clone());
                event.destination_id = Some(receipt.destination.clone());
                self.append(event)
            }
            DestinationReplayStage::CheckpointCommitted { checkpoint } => {
                let mut event = self.base_event(RunEventKind::CheckpointCommitted);
                event.checkpoint_id = Some(checkpoint.delta.checkpoint_id.clone());
                event.package_hash = Some(checkpoint.delta.package_hash.clone());
                event.receipt_id = checkpoint
                    .receipt
                    .as_ref()
                    .map(|receipt| receipt.receipt_id.clone());
                self.append(event)
            }
            DestinationReplayStage::PackageStatusUpdated { status } => {
                let mut event = self.base_event(RunEventKind::PackageStatusUpdated);
                event.details = RunEventDetails::new([(
                    "status",
                    RunEventValue::String(status.as_str().to_owned()),
                )]);
                self.append(event)
            }
        }
    }

    fn append_run_succeeded(&self) -> Result<()> {
        self.append(self.base_event(RunEventKind::RunSucceeded))
    }

    fn append_run_failed(&self) -> Result<()> {
        self.append(self.base_event(RunEventKind::RunFailed))
    }

    fn snapshot(&self) -> Result<RunLedgerSnapshot> {
        self.ledger.snapshot(&self.run_id)?.ok_or_else(|| {
            CdfError::internal(format!(
                "run {} disappeared from the run ledger",
                self.run_id
            ))
        })
    }

    fn base_event(&self, kind: RunEventKind) -> RunEventAppend {
        let mut event = RunEventAppend::new(kind);
        event.resource_id = Some(self.context.resource_id.clone());
        event.scope = Some(self.context.scope.clone());
        event.partition_id = partition_id_for_scope(&self.context.scope);
        event.package_id = Some(self.context.package_id.clone());
        event.package_path = Some(self.context.package_path.clone());
        event.destination_id = Some(self.context.destination_id.clone());
        event.plan_id = Some(self.context.plan_id.clone());
        event
    }

    fn append(&self, event: RunEventAppend) -> Result<()> {
        self.ledger.append_event(&self.run_id, event)?;
        Ok(())
    }
}

fn partition_id_for_scope(scope: &ScopeKey) -> Option<cdf_kernel::PartitionId> {
    match scope {
        ScopeKey::Partition { partition_id } => Some(partition_id.clone()),
        _ => None,
    }
}

type DestinationReplayStageHook<'a> = &'a dyn Fn(DestinationReplayStage<'_>) -> Result<()>;

enum DestinationReplayStage<'a> {
    CheckpointProposed { delta: &'a StateDelta },
    DestinationCommitStarted { plan_id: &'a PlanId },
    DestinationReceiptRecorded { receipt: &'a Receipt },
    CheckpointCommitted { checkpoint: &'a Checkpoint },
    PackageStatusUpdated { status: &'a PackageStatus },
}

struct DuckDbReplayHooks<'a> {
    after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    stage: Option<DestinationReplayStageHook<'a>>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'a>>,
}

struct DuckDbPackageReplayInputs {
    delta: StateDelta,
    target: TargetName,
    disposition: WriteDisposition,
    merge_keys: Vec<String>,
    schema_hash: SchemaHash,
    commit: DestinationCommitRequest,
}

impl DuckDbPackageReplayInputs {
    fn from_package_artifacts(inputs: PackageReplayInputs) -> Self {
        Self {
            target: inputs.destination_commit.target.clone(),
            disposition: inputs.destination_commit.disposition.clone(),
            merge_keys: inputs.merge_keys,
            schema_hash: inputs.schema_hash,
            commit: inputs.destination_commit,
            delta: inputs.state_delta,
        }
    }

    fn from_explicit(
        delta: StateDelta,
        target: TargetName,
        disposition: WriteDisposition,
        merge_keys: Vec<String>,
        schema_hash: SchemaHash,
    ) -> Result<Self> {
        let commit = commit_request(&delta, target.clone(), disposition.clone())?;
        Ok(Self {
            delta,
            target,
            disposition,
            merge_keys,
            schema_hash,
            commit,
        })
    }
}

struct ParquetPackageReplayInputs {
    delta: StateDelta,
    target: TargetName,
    disposition: WriteDisposition,
    schema_hash: SchemaHash,
    commit: DestinationCommitRequest,
}

impl ParquetPackageReplayInputs {
    fn from_package_artifacts(inputs: PackageReplayInputs) -> Self {
        Self {
            target: inputs.destination_commit.target.clone(),
            disposition: inputs.destination_commit.disposition.clone(),
            schema_hash: inputs.schema_hash,
            commit: inputs.destination_commit,
            delta: inputs.state_delta,
        }
    }
}

struct PostgresPackageReplayInputs {
    delta: StateDelta,
    target: TargetName,
    disposition: WriteDisposition,
    load_plan: Option<PostgresLoadPlan>,
    commit: DestinationCommitRequest,
}

impl PostgresPackageReplayInputs {
    fn from_package_artifacts(
        request: &ProjectRunRequest<'_>,
        reader: &PackageReader,
        inputs: PackageReplayInputs,
    ) -> Result<Self> {
        let load_input =
            postgres_load_plan_input(request, &inputs, postgres_columns_from_package(reader)?)?;
        let load_plan = PostgresDestination::new().plan_load(load_input)?;
        Ok(Self {
            target: inputs.destination_commit.target.clone(),
            disposition: inputs.destination_commit.disposition.clone(),
            commit: inputs.destination_commit,
            delta: inputs.state_delta,
            load_plan: Some(load_plan),
        })
    }

    fn from_explicit_artifact_replay(
        reader: &PackageReader,
        inputs: PackageReplayInputs,
        target: PostgresTarget,
        dedup: MergeDedupPolicy,
        existing_table: Option<PostgresExistingTable>,
    ) -> Result<Self> {
        let load_input = postgres_load_plan_input_from_artifacts(
            &inputs,
            target,
            dedup,
            existing_table,
            postgres_columns_from_package(reader)?,
        )?;
        let load_plan = PostgresDestination::new().plan_load(load_input)?;
        Ok(Self {
            target: inputs.destination_commit.target.clone(),
            disposition: inputs.destination_commit.disposition.clone(),
            commit: inputs.destination_commit,
            delta: inputs.state_delta,
            load_plan: Some(load_plan),
        })
    }
}

pub fn replay_duckdb_package_from_artifacts<Store>(
    request: PackageArtifactDuckDbReplayRequest<'_, Store>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    replay_duckdb_package_from_artifacts_with_failpoint(request, None)
}

pub fn replay_duckdb_package_from_artifacts_with_failpoint<Store>(
    request: PackageArtifactDuckDbReplayRequest<'_, Store>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    replay_duckdb_package_from_artifacts_with_hooks(request, None, lifecycle_failpoint)
}

fn replay_duckdb_package_from_artifacts_with_hooks<Store>(
    request: PackageArtifactDuckDbReplayRequest<'_, Store>,
    stage_hook: Option<DestinationReplayStageHook<'_>>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    let inputs = DuckDbPackageReplayInputs::from_package_artifacts(reader.replay_inputs()?);
    replay_duckdb_package_with_inputs(
        reader,
        request.package_dir,
        request.destination,
        request.checkpoint_store,
        inputs,
        DuckDbReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: stage_hook,
            lifecycle_failpoint,
        },
    )
}

pub fn recover_duckdb_package_from_artifacts<Store>(
    request: PackageArtifactDuckDbRecoveryRequest<'_, Store>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    recover_duckdb_package_from_artifacts_with_failpoint(request, None)
}

pub fn recover_duckdb_package_from_artifacts_with_failpoint<Store>(
    request: PackageArtifactDuckDbRecoveryRequest<'_, Store>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    let inputs = DuckDbPackageReplayInputs::from_package_artifacts(reader.replay_inputs()?);
    recover_duckdb_package_with_inputs(
        reader,
        request.destination,
        request.checkpoint_store,
        inputs,
        request.receipt,
        request.after_receipt_verified,
        lifecycle_failpoint,
    )
}

pub fn replay_prepared_duckdb_package<Store>(
    request: PreparedDuckDbReplayRequest<'_, Store>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    replay_prepared_duckdb_package_with_failpoint(request, None)
}

pub fn replay_prepared_duckdb_package_with_failpoint<Store>(
    request: PreparedDuckDbReplayRequest<'_, Store>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    validate_prepared_package(&reader, &request.delta, &request.schema_hash)?;
    let inputs = DuckDbPackageReplayInputs::from_explicit(
        request.delta,
        request.target,
        request.disposition,
        request.merge_keys,
        request.schema_hash,
    )?;
    replay_duckdb_package_with_inputs(
        reader,
        request.package_dir,
        request.destination,
        request.checkpoint_store,
        inputs,
        DuckDbReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
            lifecycle_failpoint,
        },
    )
}

fn replay_duckdb_package_with_inputs<Store>(
    mut reader: PackageReader,
    package_dir: PathBuf,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
    inputs: DuckDbPackageReplayInputs,
    hooks: DuckDbReplayHooks<'_>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    trigger_lifecycle_failpoint(
        hooks.lifecycle_failpoint,
        LocalDuckDbLifecycleFailpoint::AfterPackagedBeforeDestinationWrite,
        None,
    )?;
    let checkpoint_id = inputs.delta.checkpoint_id.clone();
    checkpoint_store.propose(inputs.delta.clone())?;
    if let Err(error) = notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::CheckpointProposed {
            delta: &inputs.delta,
        },
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    if let Err(error) = reader.update_status(PackageStatus::Loading) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    trigger_lifecycle_failpoint(
        hooks.lifecycle_failpoint,
        LocalDuckDbLifecycleFailpoint::AfterCheckpointProposalBeforeDestinationWrite,
        None,
    )?;

    let request = DuckDbCommitRequest {
        package_dir,
        commit: inputs.commit.clone(),
        schema_hash: inputs.schema_hash.clone(),
        merge_keys: inputs.merge_keys.clone(),
    };
    let receipts_before = reader.receipts()?.len();
    let duplicate = duckdb_has_duplicate_receipt(destination, &request.commit)?;
    let plan = match destination.plan_package_commit(&request) {
        Ok(plan) => plan,
        Err(error) => {
            let _ = checkpoint_store.abandon(&checkpoint_id);
            return Err(error);
        }
    };
    if let Err(error) = notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::DestinationCommitStarted {
            plan_id: &plan.kernel.plan_id,
        },
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    let receipt = match commit_duckdb_package_through_session(destination, request, plan.kernel) {
        Ok(receipt) => receipt,
        Err(error) => {
            let _ = checkpoint_store.abandon(&checkpoint_id);
            return Err(error);
        }
    };

    let package_receipt_recorded = reader.receipts()?.len() > receipts_before;
    verify_receipt_before_checkpoint(
        destination,
        &inputs.delta,
        &inputs.target,
        &inputs.disposition,
        &receipt,
    )?;
    notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::DestinationReceiptRecorded { receipt: &receipt },
    )?;
    trigger_lifecycle_failpoint(
        hooks.lifecycle_failpoint,
        LocalDuckDbLifecycleFailpoint::AfterReceiptVerifiedBeforeCheckpointCommit,
        Some(&receipt),
    )?;
    if let Some(hook) = hooks.after_receipt_verified {
        hook(&receipt)?;
    }

    let checkpoint = checkpoint_store.commit(&inputs.delta.checkpoint_id, receipt.clone())?;
    notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::CheckpointCommitted {
            checkpoint: &checkpoint,
        },
    )?;
    trigger_lifecycle_failpoint(
        hooks.lifecycle_failpoint,
        LocalDuckDbLifecycleFailpoint::AfterCheckpointCommitBeforePackageStatusCheckpointed,
        Some(&receipt),
    )?;
    let package_status = reader
        .update_status(PackageStatus::Checkpointed)?
        .lifecycle
        .status
        .clone();
    notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::PackageStatusUpdated {
            status: &package_status,
        },
    )?;

    Ok(PreparedDuckDbReplayReport {
        checkpoint,
        receipt,
        receipt_source: PreparedReceiptSource::DuckDbCommit {
            duplicate,
            package_receipt_recorded,
        },
        package_status,
    })
}

fn notify_destination_replay_stage(
    hook: Option<DestinationReplayStageHook<'_>>,
    stage: DestinationReplayStage<'_>,
) -> Result<()> {
    if let Some(hook) = hook {
        hook(stage)?;
    }
    Ok(())
}

fn commit_duckdb_package_through_session(
    destination: &DuckDbDestination,
    request: DuckDbCommitRequest,
    plan: cdf_kernel::CommitPlan,
) -> Result<Receipt> {
    let mut session = destination.begin(request.commit.clone(), plan)?;
    if let Err(error) = session.apply_migrations() {
        let _ = session.abort();
        return Err(error);
    }
    if let Err(error) =
        write_package_segments_to_session(session.as_mut(), &request.package_dir, &request.commit)
    {
        let _ = session.abort();
        return Err(error);
    }
    session.finalize()
}

fn write_package_segments_to_session(
    session: &mut dyn cdf_kernel::CommitSession,
    package_dir: &Path,
    commit: &DestinationCommitRequest,
) -> Result<()> {
    let reader = PackageReader::open(package_dir)?;
    reader.verify()?;
    for segment in reader.read_commit_segments(&commit.segments)? {
        session.write_segment(segment)?;
    }
    Ok(())
}

fn duckdb_has_duplicate_receipt(
    destination: &DuckDbDestination,
    request: &DestinationCommitRequest,
) -> Result<bool> {
    if !destination.database_path().exists() {
        return Ok(false);
    }
    let snapshot = destination.read_mirror_snapshot_read_only()?;
    for load in snapshot.loads {
        if load.target == request.target.as_str()
            && load.idempotency_token == request.idempotency_token.as_str()
            && load.package_hash == request.package_hash.as_str()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedParquetReplayReport {
    pub checkpoint: Checkpoint,
    pub receipt: Receipt,
    pub receipt_source: ProjectReceiptSource,
    pub package_status: PackageStatus,
}

pub fn recover_parquet_package_from_artifacts<Store>(
    request: PackageArtifactParquetRecoveryRequest<'_, Store>,
) -> Result<PreparedParquetReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    let inputs = ParquetPackageReplayInputs::from_package_artifacts(reader.replay_inputs()?);
    recover_parquet_package_with_inputs(
        reader,
        request.destination,
        request.checkpoint_store,
        inputs,
        request.receipt,
        request.after_receipt_verified,
    )
}

pub fn replay_parquet_package_from_artifacts<Store>(
    request: PackageArtifactParquetReplayRequest<'_, Store>,
) -> Result<PreparedParquetReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    let inputs = ParquetPackageReplayInputs::from_package_artifacts(reader.replay_inputs()?);
    replay_parquet_package_with_inputs(
        reader,
        request.package_dir,
        request.destination,
        request.checkpoint_store,
        inputs,
        ParquetReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
        },
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedPostgresReplayReport {
    pub checkpoint: Checkpoint,
    pub receipt: Receipt,
    pub receipt_source: ProjectReceiptSource,
    pub package_status: PackageStatus,
}

pub fn recover_postgres_package_from_artifacts<Store>(
    request: PackageArtifactPostgresRecoveryRequest<'_, Store>,
) -> Result<PreparedPostgresReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    let replay_inputs = reader.replay_inputs()?;
    let inputs = PostgresPackageReplayInputs {
        target: replay_inputs.destination_commit.target.clone(),
        disposition: replay_inputs.destination_commit.disposition.clone(),
        commit: replay_inputs.destination_commit,
        delta: replay_inputs.state_delta,
        load_plan: None,
    };
    recover_postgres_package_with_inputs(
        reader,
        request.destination,
        request.checkpoint_store,
        inputs,
        request.receipt,
        request.after_receipt_verified,
    )
}

pub fn replay_postgres_package_from_artifacts<Store>(
    request: PackageArtifactPostgresReplayRequest<'_, Store>,
) -> Result<PreparedPostgresReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    let inputs = PostgresPackageReplayInputs::from_explicit_artifact_replay(
        &reader,
        reader.replay_inputs()?,
        request.target,
        request.dedup,
        request.existing_table,
    )?;
    replay_postgres_package_with_inputs(
        reader,
        request.package_dir,
        request.destination,
        request.checkpoint_store,
        inputs,
        PostgresReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
        },
    )
}

fn replay_parquet_package_with_inputs<Store>(
    mut reader: PackageReader,
    package_dir: PathBuf,
    destination: &ParquetDestination,
    checkpoint_store: &Store,
    inputs: ParquetPackageReplayInputs,
    hooks: ParquetReplayHooks<'_>,
) -> Result<PreparedParquetReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let checkpoint_id = inputs.delta.checkpoint_id.clone();
    checkpoint_store.propose(inputs.delta.clone())?;
    if let Err(error) = notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::CheckpointProposed {
            delta: &inputs.delta,
        },
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    if let Err(error) = reader.update_status(PackageStatus::Loading) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }

    let request = ParquetCommitRequest {
        package_dir,
        commit: inputs.commit.clone(),
        schema_hash: inputs.schema_hash.clone(),
    };
    let receipts_before = reader.receipts()?.len();
    let plan = match destination.plan_package_commit(&request) {
        Ok(plan) => plan,
        Err(error) => {
            let _ = checkpoint_store.abandon(&checkpoint_id);
            return Err(error);
        }
    };
    let duplicate = plan.duplicate;
    if let Err(error) = notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::DestinationCommitStarted {
            plan_id: &plan.kernel.plan_id,
        },
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    let receipt = match commit_parquet_package_through_session(destination, request, plan.kernel) {
        Ok(receipt) => receipt,
        Err(error) => {
            let _ = checkpoint_store.abandon(&checkpoint_id);
            return Err(error);
        }
    };

    let package_receipt_recorded = reader.receipts()?.len() > receipts_before;
    verify_parquet_receipt_before_checkpoint(
        destination,
        &inputs.delta,
        &inputs.target,
        &inputs.disposition,
        &receipt,
    )?;
    notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::DestinationReceiptRecorded { receipt: &receipt },
    )?;
    if let Some(hook) = hooks.after_receipt_verified {
        hook(&receipt)?;
    }

    let checkpoint = checkpoint_store.commit(&inputs.delta.checkpoint_id, receipt.clone())?;
    notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::CheckpointCommitted {
            checkpoint: &checkpoint,
        },
    )?;
    let package_status = reader
        .update_status(PackageStatus::Checkpointed)?
        .lifecycle
        .status
        .clone();
    notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::PackageStatusUpdated {
            status: &package_status,
        },
    )?;

    Ok(PreparedParquetReplayReport {
        checkpoint,
        receipt,
        receipt_source: ProjectReceiptSource::DestinationCommit {
            duplicate,
            package_receipt_recorded,
        },
        package_status,
    })
}

fn replay_postgres_package_with_inputs<Store>(
    mut reader: PackageReader,
    package_dir: PathBuf,
    destination: &PostgresDestination,
    checkpoint_store: &Store,
    inputs: PostgresPackageReplayInputs,
    hooks: PostgresReplayHooks<'_>,
) -> Result<PreparedPostgresReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let checkpoint_id = inputs.delta.checkpoint_id.clone();
    checkpoint_store.propose(inputs.delta.clone())?;
    if let Err(error) = notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::CheckpointProposed {
            delta: &inputs.delta,
        },
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    if let Err(error) = reader.update_status(PackageStatus::Loading) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }

    let load_plan = inputs
        .load_plan
        .clone()
        .ok_or_else(|| CdfError::internal("Postgres replay requires a load plan"))?;
    let request = PostgresCommitRequest {
        package_dir,
        plan: load_plan.clone(),
    };
    let receipts_before = reader.receipts()?.len();
    if let Err(error) = notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::DestinationCommitStarted {
            plan_id: &load_plan.kernel.plan_id,
        },
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    let receipt = match commit_postgres_package_through_session(
        destination,
        request,
        inputs.commit.clone(),
    ) {
        Ok(receipt) => receipt,
        Err(error) => {
            let _ = checkpoint_store.abandon(&checkpoint_id);
            return Err(error);
        }
    };

    let package_receipt_recorded = reader.receipts()?.len() > receipts_before;
    verify_postgres_receipt_before_checkpoint(
        destination,
        &inputs.delta,
        &inputs.target,
        &inputs.disposition,
        &receipt,
    )?;
    notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::DestinationReceiptRecorded { receipt: &receipt },
    )?;
    if let Some(hook) = hooks.after_receipt_verified {
        hook(&receipt)?;
    }

    let checkpoint = checkpoint_store.commit(&inputs.delta.checkpoint_id, receipt.clone())?;
    notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::CheckpointCommitted {
            checkpoint: &checkpoint,
        },
    )?;
    let package_status = reader
        .update_status(PackageStatus::Checkpointed)?
        .lifecycle
        .status
        .clone();
    notify_destination_replay_stage(
        hooks.stage,
        DestinationReplayStage::PackageStatusUpdated {
            status: &package_status,
        },
    )?;

    Ok(PreparedPostgresReplayReport {
        checkpoint,
        receipt,
        receipt_source: ProjectReceiptSource::DestinationCommitReceiptOnly {
            package_receipt_recorded,
        },
        package_status,
    })
}

fn recover_parquet_package_with_inputs<Store>(
    mut reader: PackageReader,
    destination: &ParquetDestination,
    checkpoint_store: &Store,
    inputs: ParquetPackageReplayInputs,
    receipt: Receipt,
    after_receipt_verified: Option<ReceiptVerifiedHook<'_>>,
) -> Result<PreparedParquetReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    verify_parquet_receipt_before_checkpoint(
        destination,
        &inputs.delta,
        &inputs.target,
        &inputs.disposition,
        &receipt,
    )?;
    if let Some(hook) = after_receipt_verified {
        hook(&receipt)?;
    }

    let checkpoint =
        commit_or_reuse_committed_checkpoint(checkpoint_store, &inputs.delta, receipt.clone())?;
    let package_status = reader
        .update_status(PackageStatus::Checkpointed)?
        .lifecycle
        .status
        .clone();

    Ok(PreparedParquetReplayReport {
        checkpoint,
        receipt,
        receipt_source: ProjectReceiptSource::SuppliedDurableReceipt,
        package_status,
    })
}

fn recover_postgres_package_with_inputs<Store>(
    mut reader: PackageReader,
    destination: &PostgresDestination,
    checkpoint_store: &Store,
    inputs: PostgresPackageReplayInputs,
    receipt: Receipt,
    after_receipt_verified: Option<ReceiptVerifiedHook<'_>>,
) -> Result<PreparedPostgresReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    verify_postgres_receipt_before_checkpoint(
        destination,
        &inputs.delta,
        &inputs.target,
        &inputs.disposition,
        &receipt,
    )?;
    if let Some(hook) = after_receipt_verified {
        hook(&receipt)?;
    }

    let checkpoint =
        commit_or_reuse_committed_checkpoint(checkpoint_store, &inputs.delta, receipt.clone())?;
    let package_status = reader
        .update_status(PackageStatus::Checkpointed)?
        .lifecycle
        .status
        .clone();

    Ok(PreparedPostgresReplayReport {
        checkpoint,
        receipt,
        receipt_source: ProjectReceiptSource::SuppliedDurableReceipt,
        package_status,
    })
}

struct ParquetReplayHooks<'a> {
    after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    stage: Option<DestinationReplayStageHook<'a>>,
}

struct PostgresReplayHooks<'a> {
    after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    stage: Option<DestinationReplayStageHook<'a>>,
}

fn commit_parquet_package_through_session(
    destination: &ParquetDestination,
    request: ParquetCommitRequest,
    plan: cdf_kernel::CommitPlan,
) -> Result<Receipt> {
    let mut session = destination.begin(request.commit.clone(), plan)?;
    if let Err(error) = session.apply_migrations() {
        let _ = session.abort();
        return Err(error);
    }
    if let Err(error) =
        write_package_segments_to_session(session.as_mut(), &request.package_dir, &request.commit)
    {
        let _ = session.abort();
        return Err(error);
    }
    session.finalize()
}

fn commit_postgres_package_through_session(
    destination: &PostgresDestination,
    request: PostgresCommitRequest,
    commit: DestinationCommitRequest,
) -> Result<Receipt> {
    let plan = request.plan.kernel.clone();
    let package_dir = request.package_dir.clone();
    let session_destination = destination.clone().with_commit_request(request);
    let mut session = session_destination.begin(commit.clone(), plan)?;
    if let Err(error) = session.apply_migrations() {
        let _ = session.abort();
        return Err(error);
    }
    if let Err(error) = write_package_segments_to_session(session.as_mut(), &package_dir, &commit) {
        let _ = session.abort();
        return Err(error);
    }
    session.finalize()
}

pub fn recover_prepared_duckdb_package<Store>(
    request: PreparedDuckDbRecoveryRequest<'_, Store>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    recover_prepared_duckdb_package_with_failpoint(request, None)
}

pub fn recover_prepared_duckdb_package_with_failpoint<Store>(
    request: PreparedDuckDbRecoveryRequest<'_, Store>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    validate_prepared_package(&reader, &request.delta, &request.schema_hash)?;
    let inputs = DuckDbPackageReplayInputs::from_explicit(
        request.delta,
        request.target,
        request.disposition,
        Vec::new(),
        request.schema_hash,
    )?;
    recover_duckdb_package_with_inputs(
        reader,
        request.destination,
        request.checkpoint_store,
        inputs,
        request.receipt,
        request.after_receipt_verified,
        lifecycle_failpoint,
    )
}

fn recover_duckdb_package_with_inputs<Store>(
    mut reader: PackageReader,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
    inputs: DuckDbPackageReplayInputs,
    receipt: Receipt,
    after_receipt_verified: Option<ReceiptVerifiedHook<'_>>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    verify_receipt_before_checkpoint(
        destination,
        &inputs.delta,
        &inputs.target,
        &inputs.disposition,
        &receipt,
    )?;
    trigger_lifecycle_failpoint(
        lifecycle_failpoint,
        LocalDuckDbLifecycleFailpoint::AfterReceiptVerifiedBeforeCheckpointCommit,
        Some(&receipt),
    )?;
    if let Some(hook) = after_receipt_verified {
        hook(&receipt)?;
    }

    let checkpoint =
        commit_or_reuse_committed_checkpoint(checkpoint_store, &inputs.delta, receipt.clone())?;
    trigger_lifecycle_failpoint(
        lifecycle_failpoint,
        LocalDuckDbLifecycleFailpoint::AfterCheckpointCommitBeforePackageStatusCheckpointed,
        Some(&receipt),
    )?;
    let package_status = reader
        .update_status(PackageStatus::Checkpointed)?
        .lifecycle
        .status
        .clone();

    Ok(PreparedDuckDbReplayReport {
        checkpoint,
        receipt,
        receipt_source: PreparedReceiptSource::SuppliedDurableReceipt,
        package_status,
    })
}

fn trigger_lifecycle_failpoint(
    hook: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
    failpoint: LocalDuckDbLifecycleFailpoint,
    receipt: Option<&Receipt>,
) -> Result<()> {
    if let Some(hook) = hook {
        hook(failpoint, receipt)?;
    }
    Ok(())
}

fn commit_or_reuse_committed_checkpoint<Store>(
    checkpoint_store: &Store,
    delta: &StateDelta,
    receipt: Receipt,
) -> Result<Checkpoint>
where
    Store: CheckpointStore + ?Sized,
{
    match checkpoint_store.commit(&delta.checkpoint_id, receipt.clone()) {
        Ok(checkpoint) => Ok(checkpoint),
        Err(error) => {
            let Some(head) =
                checkpoint_store.head(&delta.pipeline_id, &delta.resource_id, &delta.scope)?
            else {
                return Err(error);
            };
            if head.status == CheckpointStatus::Committed
                && head.is_head
                && head.delta == *delta
                && head.receipt.as_ref() == Some(&receipt)
            {
                Ok(head)
            } else {
                Err(error)
            }
        }
    }
}

fn validate_prepared_package(
    reader: &PackageReader,
    delta: &StateDelta,
    schema_hash: &SchemaHash,
) -> Result<ReplayView> {
    reader.verify()?;
    let replay = reader.replay_view()?;
    if replay.package_hash != delta.package_hash {
        return Err(CdfError::data(format!(
            "package hash {} does not match StateDelta package hash {}",
            replay.package_hash, delta.package_hash
        )));
    }
    if schema_hash != &delta.schema_hash {
        return Err(CdfError::contract(format!(
            "explicit schema hash {} does not match StateDelta schema hash {}",
            schema_hash, delta.schema_hash
        )));
    }
    validate_package_segments_match_delta(&replay.segments, &delta.segments)?;
    Ok(replay)
}

fn validate_package_segments_match_delta(
    package_segments: &[SegmentEntry],
    state_segments: &[StateSegment],
) -> Result<()> {
    if state_segments.is_empty() {
        return Err(CdfError::contract(
            "StateDelta must include at least one state segment for package replay",
        ));
    }
    if package_segments.len() != state_segments.len() {
        return Err(CdfError::data(format!(
            "package has {} segment(s) but StateDelta has {} segment(s)",
            package_segments.len(),
            state_segments.len()
        )));
    }

    let package_by_id = package_segments
        .iter()
        .map(|segment| (&segment.segment_id, segment))
        .collect::<BTreeMap<_, _>>();
    if package_by_id.len() != package_segments.len() {
        return Err(CdfError::data(
            "package manifest contains duplicate segment ids",
        ));
    }

    let mut seen_state_segments = BTreeSet::<&SegmentId>::new();
    for segment in state_segments {
        if !seen_state_segments.insert(&segment.segment_id) {
            return Err(CdfError::contract(format!(
                "StateDelta contains duplicate segment {}",
                segment.segment_id
            )));
        }
        let Some(package_segment) = package_by_id.get(&segment.segment_id) else {
            return Err(CdfError::data(format!(
                "StateDelta segment {} is not present in the package manifest",
                segment.segment_id
            )));
        };
        if package_segment.row_count != segment.row_count
            || package_segment.byte_count != segment.byte_count
        {
            return Err(CdfError::data(format!(
                "StateDelta segment {} has {} rows/{} bytes but package manifest has {} rows/{} bytes",
                segment.segment_id,
                segment.row_count,
                segment.byte_count,
                package_segment.row_count,
                package_segment.byte_count
            )));
        }
    }

    Ok(())
}

fn commit_request(
    delta: &StateDelta,
    target: TargetName,
    disposition: WriteDisposition,
) -> Result<DestinationCommitRequest> {
    Ok(DestinationCommitRequest {
        package_hash: delta.package_hash.clone(),
        target,
        disposition,
        segments: delta.segments.clone(),
        idempotency_token: IdempotencyToken::new(delta.package_hash.as_str())?,
    })
}

fn verify_receipt_before_checkpoint(
    destination: &DuckDbDestination,
    delta: &StateDelta,
    target: &TargetName,
    disposition: &WriteDisposition,
    receipt: &Receipt,
) -> Result<()> {
    validate_receipt_identity(delta, target, disposition, receipt)?;
    let verification = DestinationProtocol::verify(destination, receipt)?;
    if !verification.verified {
        return Err(CdfError::destination(format!(
            "DuckDB receipt {} did not verify: {}",
            verification.receipt_id,
            verification
                .reason
                .unwrap_or_else(|| "verification returned false".to_owned())
        )));
    }
    Ok(())
}

fn verify_parquet_receipt_before_checkpoint(
    destination: &ParquetDestination,
    delta: &StateDelta,
    target: &TargetName,
    disposition: &WriteDisposition,
    receipt: &Receipt,
) -> Result<()> {
    validate_receipt_identity(delta, target, disposition, receipt)?;
    let verification = DestinationProtocol::verify(destination, receipt)?;
    if !verification.verified {
        return Err(CdfError::destination(format!(
            "Parquet receipt {} did not verify: {}",
            verification.receipt_id,
            verification
                .reason
                .unwrap_or_else(|| "verification returned false".to_owned())
        )));
    }
    Ok(())
}

fn verify_postgres_receipt_before_checkpoint(
    destination: &PostgresDestination,
    delta: &StateDelta,
    target: &TargetName,
    disposition: &WriteDisposition,
    receipt: &Receipt,
) -> Result<()> {
    validate_receipt_identity(delta, target, disposition, receipt)?;
    let verification = DestinationProtocol::verify(destination, receipt)?;
    if !verification.verified {
        return Err(CdfError::destination(format!(
            "Postgres receipt {} did not verify: {}",
            verification.receipt_id,
            verification
                .reason
                .unwrap_or_else(|| "verification returned false".to_owned())
        )));
    }
    Ok(())
}

fn validate_receipt_identity(
    delta: &StateDelta,
    target: &TargetName,
    disposition: &WriteDisposition,
    receipt: &Receipt,
) -> Result<()> {
    if receipt.package_hash != delta.package_hash {
        return Err(CdfError::contract(format!(
            "receipt {} package hash {} does not match StateDelta package hash {}",
            receipt.receipt_id, receipt.package_hash, delta.package_hash
        )));
    }
    if receipt.schema_hash != delta.schema_hash {
        return Err(CdfError::contract(format!(
            "receipt {} schema hash {} does not match StateDelta schema hash {}",
            receipt.receipt_id, receipt.schema_hash, delta.schema_hash
        )));
    }
    if &receipt.target != target {
        return Err(CdfError::contract(format!(
            "receipt {} target {} does not match explicit target {}",
            receipt.receipt_id, receipt.target, target
        )));
    }
    if &receipt.disposition != disposition {
        return Err(CdfError::contract(format!(
            "receipt {} disposition {:?} does not match explicit disposition {:?}",
            receipt.receipt_id, receipt.disposition, disposition
        )));
    }
    if receipt.idempotency_token.as_str() != delta.package_hash.as_str() {
        return Err(CdfError::contract(format!(
            "receipt {} idempotency token {} does not match package hash {}",
            receipt.receipt_id, receipt.idempotency_token, delta.package_hash
        )));
    }
    validate_segment_acks(delta, receipt)
}

fn validate_segment_acks(delta: &StateDelta, receipt: &Receipt) -> Result<()> {
    if receipt.segment_acks.len() != delta.segments.len() {
        return Err(CdfError::contract(format!(
            "receipt {} acknowledges {} segment(s) but StateDelta has {} segment(s)",
            receipt.receipt_id,
            receipt.segment_acks.len(),
            delta.segments.len()
        )));
    }

    let acks = receipt
        .segment_acks
        .iter()
        .map(|ack| (&ack.segment_id, ack))
        .collect::<BTreeMap<_, _>>();
    if acks.len() != receipt.segment_acks.len() {
        return Err(CdfError::contract(format!(
            "receipt {} contains duplicate segment acknowledgements",
            receipt.receipt_id
        )));
    }

    for segment in &delta.segments {
        let Some(ack) = acks.get(&segment.segment_id) else {
            return Err(CdfError::contract(format!(
                "receipt {} does not acknowledge segment {}",
                receipt.receipt_id, segment.segment_id
            )));
        };
        if ack.row_count != segment.row_count || ack.byte_count != segment.byte_count {
            return Err(CdfError::contract(format!(
                "receipt {} acknowledges segment {} as {} rows/{} bytes but StateDelta has {} rows/{} bytes",
                receipt.receipt_id,
                segment.segment_id,
                ack.row_count,
                ack.byte_count,
                segment.row_count,
                segment.byte_count
            )));
        }
    }

    Ok(())
}
