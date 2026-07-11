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
        ensure_parent_directory, refuse_existing_package_dir, validate_explicit_package_id,
        validate_project_run_request,
    },
};
use cdf_contract::{AnomalyFact, ValidationDepth, ValidationProgram, ValidationTransitionTrigger};

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
        event_sink: None,
        after_receipt_verified: request.after_receipt_verified,
    };
    Ok(run_project(request).await?.into_local_file_duckdb_report())
}

pub async fn run_project(request: ProjectRunRequest<'_>) -> Result<ProjectRunReport> {
    run_project_with_telemetry(request, RunTelemetryConfig::disabled()).await
}

pub async fn run_project_with_telemetry(
    request: ProjectRunRequest<'_>,
    telemetry: RunTelemetryConfig,
) -> Result<ProjectRunReport> {
    let mut request = request;
    validate_project_run_request(&mut request)?;
    validate_explicit_package_id(&request.package_id)?;

    let schema_hash = request
        .destination
        .output_schema(&request.plan)?
        .schema_hash;
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
        event_sink,
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
        event_sink,
        telemetry,
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
            let _ = recorder.append_run_failed(&error);
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
    let manifest_plan =
        plan_file_manifest_incrementality(execution.plan, descriptor, head.as_ref())?;
    execution
        .recorder
        .append_plan_recorded(if manifest_plan.no_changed_files() {
            0
        } else {
            1
        })?;
    if manifest_plan.no_changed_files() {
        return no_changed_files_report(execution, head, manifest_plan.summary);
    }

    execution.recorder.append_package_started()?;

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
        &manifest_plan.plan,
        resource,
        &execution.package_dir,
        &write_package_pre_finalize_artifacts,
        EngineExecutionOptions::default()
            .with_phase_metrics(execution.recorder.phase_telemetry_enabled()),
    )
    .await?;

    for metric in output.phase_metrics.iter().cloned() {
        execution.recorder.append_phase_metric(metric)?;
    }
    execution.recorder.complete_phase(
        RunPhase::PackageExecution,
        output.output.profile.output_bytes,
        output.output.profile.output_bytes,
        output.output.profile.output_batches,
    )?;

    let reader = PackageReader::open(&execution.package_dir)?;
    let replay_inputs = reader.replay_inputs()?;
    let package_hash = replay_inputs.state_delta.package_hash.clone();
    let profile = &output.output.profile;
    let row_count = profile.output_rows;
    let segment_count = output.output.segments.len();
    for (index, segment) in output.output.segments.iter().enumerate() {
        execution.recorder.append_package_segment_recorded(
            segment,
            index.saturating_add(1),
            segment_count,
        )?;
    }
    let quarantine_record_count = u64::try_from(reader.read_quarantine_records()?.len())
        .map_err(|error| CdfError::internal(error.to_string()))?;
    execution.recorder.append_package_finalized(
        &package_hash,
        row_count,
        profile.output_bytes,
        profile.output_batches,
        segment_count,
        quarantine_record_count,
    )?;
    let has_quarantine_artifacts = output
        .output
        .manifest
        .identity
        .files
        .iter()
        .any(|file| file.path.starts_with("quarantine/"));
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
        file_manifest: manifest_plan.summary,
        terminal_schema_quarantines: execution
            .plan
            .effective_schema_evidence()
            .map(|evidence| evidence.terminal_quarantines.clone())
            .unwrap_or_default(),
    })
}

struct FileManifestPlanning {
    plan: EnginePlan,
    summary: Option<FileManifestRunSummary>,
}

impl FileManifestPlanning {
    fn no_changed_files(&self) -> bool {
        self.summary
            .as_ref()
            .is_some_and(|summary| summary.total_file_count > 0 && summary.changed_file_count == 0)
    }
}

fn plan_file_manifest_incrementality(
    plan: &EnginePlan,
    descriptor: &ResourceDescriptor,
    head: Option<&Checkpoint>,
) -> Result<FileManifestPlanning> {
    let Some(current_files) = file_positions_from_partitions(&plan.scan.partitions)? else {
        return Ok(FileManifestPlanning {
            plan: plan.clone(),
            summary: None,
        });
    };
    if descriptor.write_disposition != WriteDisposition::Append {
        return Ok(FileManifestPlanning {
            plan: plan.clone(),
            summary: Some(FileManifestRunSummary {
                total_file_count: current_files.len(),
                changed_file_count: current_files.len(),
                unchanged_file_count: 0,
            }),
        });
    }

    let previous_files = match head.map(|checkpoint| &checkpoint.delta.output_position) {
        Some(SourcePosition::FileManifest(manifest)) => manifest_files_by_path(&manifest.files)?,
        _ => BTreeMap::new(),
    };
    let changed_paths = current_files
        .iter()
        .filter(|file| {
            previous_files
                .get(file.path.as_str())
                .is_none_or(|previous| !same_file_identity(previous, file))
        })
        .map(|file| file.path.clone())
        .collect::<BTreeSet<_>>();
    let changed_file_count = changed_paths.len();
    let mut filtered = plan.clone();
    filtered.scan.partitions.retain(|partition| {
        partition
            .metadata
            .get("path")
            .is_some_and(|path| changed_paths.contains(path))
    });
    filtered.explain.partitions.retain(|partition| {
        filtered
            .scan
            .partitions
            .iter()
            .any(|planned| planned.partition_id.as_str() == partition.partition_id)
    });

    Ok(FileManifestPlanning {
        plan: filtered,
        summary: Some(FileManifestRunSummary {
            total_file_count: current_files.len(),
            changed_file_count,
            unchanged_file_count: current_files.len().saturating_sub(changed_file_count),
        }),
    })
}

fn file_positions_from_partitions(
    partitions: &[PartitionPlan],
) -> Result<Option<Vec<FilePosition>>> {
    if partitions
        .iter()
        .all(|partition| partition.metadata.get("kind").map(String::as_str) != Some("files"))
    {
        return Ok(None);
    }
    let mut files = Vec::with_capacity(partitions.len());
    for partition in partitions {
        if partition.metadata.get("kind").map(String::as_str) != Some("files") {
            return Ok(None);
        }
        let path = partition.metadata.get("path").cloned().ok_or_else(|| {
            CdfError::contract("file partition manifest comparison requires path metadata")
        })?;
        let size_bytes = partition
            .metadata
            .get("bytes")
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "file partition `{path}` manifest comparison requires bytes metadata"
                ))
            })?
            .parse::<u64>()
            .map_err(|error| {
                CdfError::contract(format!(
                    "file partition `{path}` has invalid bytes metadata: {error}"
                ))
            })?;
        let sha256 = partition.metadata.get("sha256").cloned();
        let etag = partition.metadata.get("etag").cloned();
        if sha256.is_none() && etag.is_none() {
            return Err(CdfError::contract(format!(
                "file partition `{path}` manifest comparison requires checksum or ETag metadata"
            )));
        }
        files.push(FilePosition {
            path,
            size_bytes,
            etag,
            sha256,
        });
    }
    Ok(Some(files))
}

fn manifest_files_by_path(files: &[FilePosition]) -> Result<BTreeMap<&str, &FilePosition>> {
    let mut by_path = BTreeMap::new();
    for file in files {
        if by_path.insert(file.path.as_str(), file).is_some() {
            return Err(CdfError::data(format!(
                "committed file manifest contains duplicate path `{}`",
                file.path
            )));
        }
    }
    Ok(by_path)
}

fn same_file_identity(previous: &FilePosition, current: &FilePosition) -> bool {
    previous.size_bytes == current.size_bytes
        && match (
            &previous.sha256,
            &current.sha256,
            &previous.etag,
            &current.etag,
        ) {
            (Some(previous), Some(current), _, _) => previous == current,
            (_, _, Some(previous), Some(current)) => previous == current,
            _ => false,
        }
}

fn no_changed_files_report(
    execution: ProjectRunExecution<'_>,
    head: Option<Checkpoint>,
    summary: Option<FileManifestRunSummary>,
) -> Result<ProjectRunReport> {
    let checkpoint = head.ok_or_else(|| {
        CdfError::internal("file manifest no-op requires a committed checkpoint head")
    })?;
    let receipt = checkpoint.receipt.clone().ok_or_else(|| {
        CdfError::data(format!(
            "checkpoint {} cannot satisfy a file manifest no-op because it has no receipt",
            checkpoint.delta.checkpoint_id
        ))
    })?;
    execution.recorder.append_run_succeeded()?;
    let ledger_snapshot = execution.recorder.snapshot()?;
    Ok(ProjectRunReport {
        run_id: execution.recorder.run_id.clone(),
        ledger_snapshot,
        package_dir: execution.package_dir,
        package_id: execution.package_id.to_owned(),
        package_hash: checkpoint.delta.package_hash.clone(),
        package_status: PackageStatus::Checkpointed,
        checkpoint,
        receipt,
        receipt_source: ProjectReceiptSource::FileManifestNoChangedFiles,
        row_count: 0,
        segment_count: 0,
        file_manifest: summary,
        terminal_schema_quarantines: execution
            .plan
            .effective_schema_evidence()
            .map(|evidence| evidence.terminal_quarantines.clone())
            .unwrap_or_default(),
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
        PackageReplayStage::DestinationCommitStarted {
            plan_id,
            segment_count,
        } => recorder.append_replay_stage(RuntimeStage::DestinationCommitStarted {
            plan_id,
            segment_count,
        }),
        PackageReplayStage::DestinationSegmentAcknowledged { ack } => {
            recorder.append_replay_stage(RuntimeStage::DestinationSegmentAcknowledged { ack })
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
    program: &'a ValidationProgram,
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
            anomaly: None,
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
    let anomaly = explicit_anomaly_for_run(program);

    if prior_promoted {
        if drift && promotion.demote_on_drift {
            transitions.push(ValidationDepthTransitionRecord {
                from_depth: sampled_fast_path,
                to_depth: ValidationDepth::Full,
                trigger: ValidationTransitionTrigger::Drift,
                schema_hash: Some(schema_hash),
                previous_schema_hash: head.map(|checkpoint| &checkpoint.delta.schema_hash),
                anomaly: None,
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
                anomaly: None,
            });
            return transitions;
        }
        if let Some(anomaly) = anomaly.filter(|_| promotion.demote_on_anomaly) {
            transitions.push(ValidationDepthTransitionRecord {
                from_depth: sampled_fast_path,
                to_depth: ValidationDepth::Full,
                trigger: ValidationTransitionTrigger::AnomalySpike,
                schema_hash: Some(schema_hash),
                previous_schema_hash: None,
                anomaly: Some(anomaly),
            });
            return transitions;
        }
    }

    if drift || has_quarantine_artifacts || anomaly.is_some() || promotion.clean_runs_required == 0
    {
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
            anomaly: None,
        });
    }

    transitions
}

fn explicit_anomaly_for_run(program: &ValidationProgram) -> Option<&AnomalyFact> {
    program.explicit_anomalies.first()
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
