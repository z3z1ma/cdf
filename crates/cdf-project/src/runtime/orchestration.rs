use super::{
    artifacts::{StateCommitArtifactContext, write_run_state_commit_artifacts},
    destinations::postgres_target,
    hooks::{LocalDuckDbLifecycleFailpointHook, RuntimeStage},
    ledger::{ProjectRunRecorder, ProjectRunRecorderContext},
    prelude::*,
    replay::{
        ParquetPackageReplayInputs, ParquetReplayHooks, PostgresPackageReplayInputs,
        PostgresReplayHooks, replay_duckdb_package_from_artifacts_with_hooks,
        replay_parquet_package_with_inputs, replay_postgres_package_with_inputs,
    },
    resources::ProjectRunResource,
    types::*,
    validation::{
        declared_schema_hash, ensure_parent_directory, refuse_existing_package_dir,
        validate_explicit_package_id, validate_project_run_request,
    },
};

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

    let stage_hook = |stage: RuntimeStage<'_>| execution.recorder.append_replay_stage(stage);
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

    let stage_hook = |stage: RuntimeStage<'_>| execution.recorder.append_replay_stage(stage);
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

    let stage_hook = |stage: RuntimeStage<'_>| execution.recorder.append_replay_stage(stage);
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
