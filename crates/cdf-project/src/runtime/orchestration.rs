use super::{
    artifacts::{
        QuarantineMirrorArtifactContext, StateCommitArtifactContext,
        write_quarantine_mirror_outcome_artifact, write_run_state_commit_artifacts,
    },
    destinations::ResolvedProjectDestination,
    hooks::{ReceiptVerifiedHook, RuntimeStage},
    ledger::{ProjectRunRecorder, ProjectRunRecorderContext, ValidationDepthTransitionRecord},
    prelude::*,
    replay::{PackageReplayHooks, PackageReplayStage, replay_package_with_runtime},
    resources::ProjectRunSource,
    types::*,
    validation::{
        declared_schema_hash, ensure_parent_directory, refuse_existing_package_dir,
        validate_explicit_package_id, validate_project_run_request,
    },
};
use cdf_contract::{ValidationDepth, ValidationProgram, ValidationTransitionTrigger};

#[cfg(test)]
pub(crate) async fn run_local_file_to_duckdb_checkpoint(
    request: LocalFileDuckDbRunRequest<'_>,
) -> Result<LocalFileDuckDbRunReport> {
    let destination = ResolvedProjectDestination::duckdb(request.destination_path, request.target)?;
    let request = ProjectRunRequest {
        resource: ProjectRunSource::local_file(request.resource),
        plan: request.plan,
        package_root: request.package_root,
        state_store_path: request.state_store_path,
        pipeline_id: request.pipeline_id,
        package_id: request.package_id,
        checkpoint_id: request.checkpoint_id,
        destination,
        run_id: None,
        after_receipt_verified: request.after_receipt_verified,
    };
    Ok(run_project(request).await?.into_local_file_duckdb_report())
}

pub async fn run_project(request: ProjectRunRequest<'_>) -> Result<ProjectRunReport> {
    let mut request = request;
    validate_project_run_request(&mut request)?;
    validate_explicit_package_id(&request.package_id)?;

    let schema_hash = declared_schema_hash(request.resource.stream())?;
    let package_dir = request.package_root.join(&request.package_id);
    refuse_existing_package_dir(&package_dir)?;
    ensure_parent_directory(&request.state_store_path)?;

    let ProjectRunRequest {
        resource,
        plan,
        state_store_path,
        pipeline_id,
        package_id,
        checkpoint_id,
        mut destination,
        run_id,
        after_receipt_verified,
        ..
    } = request;
    let run_ledger = SqliteRunLedger::open(&state_store_path)?;
    let run = run_ledger.create_run(run_id)?;
    let checkpoint_store = SqliteCheckpointStore::open(&state_store_path)?;
    let recorder = ProjectRunRecorder::new(
        &run_ledger,
        run.run_id,
        recorder_context(
            resource,
            &plan,
            &pipeline_id,
            &package_id,
            &package_dir,
            destination.describe().destination_id,
        ),
    );
    let execution = ProjectRunExecution {
        resource,
        plan: &plan,
        package_id: &package_id,
        package_dir,
        pipeline_id: &pipeline_id,
        checkpoint_id: &checkpoint_id,
        target: destination.target().clone(),
        checkpoint_store: &checkpoint_store,
        destination: &mut destination,
        recorder: &recorder,
        after_receipt_verified,
        schema_hash,
    };
    match run_project_inner(execution).await {
        Ok(report) => Ok(report),
        Err(error) => {
            let _ = recorder.append_run_failed();
            Err(error)
        }
    }
}

fn recorder_context(
    resource: ProjectRunSource<'_>,
    plan: &EnginePlan,
    pipeline_id: &PipelineId,
    package_id: &str,
    package_dir: &Path,
    destination_id: DestinationId,
) -> ProjectRunRecorderContext {
    ProjectRunRecorderContext {
        resource_id: resource.descriptor().resource_id.clone(),
        scope: resource.descriptor().state_scope.clone(),
        package_id: package_id.to_owned(),
        package_path: package_dir.display().to_string(),
        destination_id,
        plan_id: plan.scan.plan_id.clone(),
        pipeline_id: pipeline_id.clone(),
    }
}

struct ProjectRunExecution<'a> {
    resource: ProjectRunSource<'a>,
    plan: &'a EnginePlan,
    package_id: &'a str,
    package_dir: PathBuf,
    pipeline_id: &'a PipelineId,
    checkpoint_id: &'a CheckpointId,
    target: TargetName,
    checkpoint_store: &'a SqliteCheckpointStore,
    destination: &'a mut ResolvedProjectDestination,
    recorder: &'a ProjectRunRecorder<'a>,
    after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    schema_hash: SchemaHash,
}

async fn run_project_inner(execution: ProjectRunExecution<'_>) -> Result<ProjectRunReport> {
    execution.recorder.append_run_started()?;
    execution.recorder.append_plan_recorded()?;
    execution.recorder.append_package_started()?;

    let resource = execution.resource.stream();
    let descriptor = resource.descriptor();
    let schema = resource.schema();
    let scope = descriptor.state_scope.clone();
    let quarantine_mirror = QuarantineMirrorArtifactContext {
        destination_id: execution.destination.describe().destination_id,
        quarantine_table_support: execution
            .destination
            .runtime_mut()
            .quarantine_table_support(),
    };
    let head =
        execution
            .checkpoint_store
            .head(execution.pipeline_id, &descriptor.resource_id, &scope)?;
    let history = execution.checkpoint_store.history(
        execution.pipeline_id,
        &descriptor.resource_id,
        &scope,
    )?;

    let write_package_pre_finalize_artifacts =
        |builder: &cdf_package::PackageBuilder, draft: EnginePackageDraft<'_>| {
            write_run_state_commit_artifacts(
                builder,
                draft,
                &StateCommitArtifactContext {
                    descriptor,
                    schema: schema.as_ref(),
                    pipeline_id: execution.pipeline_id,
                    checkpoint_id: execution.checkpoint_id,
                    target: &execution.target,
                },
                &execution.schema_hash,
                &scope,
                &head,
            )?;
            write_quarantine_mirror_outcome_artifact(builder, &quarantine_mirror)
        };
    let output = execute_to_package_with_segment_positions_and_pre_finalize(
        execution.plan,
        resource,
        &execution.package_dir,
        &write_package_pre_finalize_artifacts,
    )
    .await?;

    let reader = PackageReader::open(&execution.package_dir)?;
    let replay_inputs = reader.replay_inputs()?;
    let package_hash = replay_inputs.state_delta.package_hash.clone();
    let row_count = output.output.profile.output_rows;
    let segment_count = output.output.segments.len();
    execution
        .recorder
        .append_package_finalized(&package_hash, row_count, segment_count)?;
    let has_quarantine_artifacts = output
        .output
        .manifest
        .identity
        .files
        .iter()
        .any(|file| file.path.starts_with("quarantine/") && file.path.ends_with(".parquet"));
    for transition in validation_depth_transitions_recorded(
        &execution.plan.validation_program,
        head.as_ref(),
        &history,
        &execution.schema_hash,
        has_quarantine_artifacts,
    ) {
        execution
            .recorder
            .append_validation_depth_transition_recorded(
                &package_hash,
                execution.checkpoint_id,
                transition,
            )?;
    }

    let stage_hook =
        |stage: PackageReplayStage<'_>| notify_run_replay_stage(execution.recorder, stage);
    let replay_report = replay_package_with_runtime(
        reader,
        execution.package_dir.clone(),
        execution.destination.runtime_mut(),
        execution.checkpoint_store,
        replay_inputs,
        PackageReplayHooks {
            after_receipt_verified: execution.after_receipt_verified,
            stage: Some(&stage_hook),
        },
    )?;
    execution.recorder.append_run_succeeded()?;
    let ledger_snapshot = execution.recorder.snapshot()?;

    Ok(ProjectRunReport {
        run_id: execution.recorder.run_id.clone(),
        ledger_snapshot,
        package_dir: execution.package_dir,
        package_id: execution.package_id.to_owned(),
        package_hash,
        package_status: replay_report.package_status,
        checkpoint: replay_report.checkpoint,
        receipt: replay_report.receipt,
        receipt_source: replay_report.receipt_source,
        row_count,
        segment_count,
    })
}

fn notify_run_replay_stage(
    recorder: &ProjectRunRecorder<'_>,
    stage: PackageReplayStage<'_>,
) -> Result<()> {
    match stage {
        PackageReplayStage::CheckpointProposed { delta } => {
            recorder.append_replay_stage(RuntimeStage::CheckpointProposed { delta })
        }
        PackageReplayStage::DestinationCommitStarted { plan_id } => {
            recorder.append_replay_stage(RuntimeStage::DestinationCommitStarted { plan_id })
        }
        PackageReplayStage::DestinationReceiptRecorded { receipt } => {
            recorder.append_replay_stage(RuntimeStage::DestinationReceiptRecorded { receipt })
        }
        PackageReplayStage::CheckpointCommitted { checkpoint } => {
            recorder.append_replay_stage(RuntimeStage::CheckpointCommitted { checkpoint })
        }
        PackageReplayStage::PackageStatusUpdated { status } => {
            recorder.append_replay_stage(RuntimeStage::PackageStatusUpdated { status })
        }
        PackageReplayStage::PackageReplayVerified | PackageReplayStage::DestinationWriteReady => {
            Ok(())
        }
    }
}

fn validation_depth_transitions_recorded<'a>(
    program: &ValidationProgram,
    head: Option<&'a Checkpoint>,
    history: &'a [Checkpoint],
    schema_hash: &'a SchemaHash,
    has_quarantine_artifacts: bool,
) -> Vec<ValidationDepthTransitionRecord<'a>> {
    let mut transitions = Vec::new();
    let promotion = &program.promotion;
    if head.is_none() {
        transitions.push(ValidationDepthTransitionRecord {
            from_depth: ValidationDepth::Discovery,
            to_depth: ValidationDepth::Full,
            trigger: ValidationTransitionTrigger::NewResource,
            schema_hash: Some(schema_hash),
            previous_schema_hash: None,
        });
    }

    if !promotion.allow_sampled_fast_path {
        return transitions;
    }

    let sampled_fast_path = ValidationDepth::SampledFastPath {
        clean_runs_required: promotion.clean_runs_required,
    };
    let prior_promoted = head
        .map(|checkpoint| {
            consecutive_committed_schema_hash_count(history, &checkpoint.delta.schema_hash)
                >= promotion.clean_runs_required
        })
        .unwrap_or(false);
    let drift = head
        .map(|checkpoint| checkpoint.delta.schema_hash != *schema_hash)
        .unwrap_or(false);

    if prior_promoted {
        if drift && promotion.demote_on_drift {
            transitions.push(ValidationDepthTransitionRecord {
                from_depth: sampled_fast_path,
                to_depth: ValidationDepth::Full,
                trigger: ValidationTransitionTrigger::Drift,
                schema_hash: Some(schema_hash),
                previous_schema_hash: head.map(|checkpoint| &checkpoint.delta.schema_hash),
            });
            return transitions;
        }
        if has_quarantine_artifacts && promotion.demote_on_quarantine {
            transitions.push(ValidationDepthTransitionRecord {
                from_depth: sampled_fast_path,
                to_depth: ValidationDepth::Full,
                trigger: ValidationTransitionTrigger::QuarantineEvent,
                schema_hash: Some(schema_hash),
                previous_schema_hash: None,
            });
            return transitions;
        }
    }

    if drift || has_quarantine_artifacts || promotion.clean_runs_required == 0 {
        return transitions;
    }
    let prior_stable_count = consecutive_committed_schema_hash_count(history, schema_hash);
    let clean_run_count = prior_stable_count.saturating_add(1);
    if prior_stable_count < promotion.clean_runs_required
        && clean_run_count >= promotion.clean_runs_required
    {
        transitions.push(ValidationDepthTransitionRecord {
            from_depth: ValidationDepth::Full,
            to_depth: sampled_fast_path,
            trigger: ValidationTransitionTrigger::CleanStableRuns {
                count: clean_run_count,
            },
            schema_hash: Some(schema_hash),
            previous_schema_hash: None,
        });
    }

    transitions
}

fn consecutive_committed_schema_hash_count(
    history: &[Checkpoint],
    schema_hash: &SchemaHash,
) -> u32 {
    let mut count = 0_u32;
    for checkpoint in history
        .iter()
        .rev()
        .filter(|checkpoint| checkpoint.status == CheckpointStatus::Committed)
    {
        if checkpoint.delta.schema_hash != *schema_hash {
            break;
        }
        count = count.saturating_add(1);
    }
    count
}
