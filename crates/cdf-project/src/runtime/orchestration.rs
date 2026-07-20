use super::{
    artifacts::{
        QuarantineMirrorArtifactContext, StateCommitArtifactContext,
        write_quarantine_mirror_outcome_artifact, write_run_state_commit_artifacts,
    },
    destinations::ResolvedProjectDestination,
    hooks::{ReceiptVerifiedHook, RuntimeStage},
    ledger::{ProjectRunRecorder, ProjectRunRecorderContext, ValidationDepthTransitionRecord},
    prelude::*,
    replay::{
        ActiveStagedIngress, PackageReplayHooks, PackageReplayStage, StagedIngressPlan,
        replay_package_with_runtime_and_staged, settle_existing_package_with_runtime,
    },
    resources::ProjectRunSource,
    types::*,
    validation::{
        ensure_parent_directory, refuse_existing_package_dir, validate_explicit_package_id,
        validate_project_run_request,
    },
};
use cdf_contract::{AnomalyFact, ValidationDepth, ValidationProgram, ValidationTransitionTrigger};
use cdf_kernel::ScopeLeaseStore;
use std::{borrow::Cow, sync::Arc, time::Instant};

pub async fn run_project(
    request: ProjectRunRequest<'_>,
    services: &ExecutionServices,
) -> Result<ProjectRunOutcome> {
    run_project_with_context(
        request,
        RunTelemetryConfig::disabled(),
        services.clone(),
        None,
    )
    .await
}

pub async fn run_project_with_scheduler_and_telemetry(
    request: ProjectRunRequest<'_>,
    services: &ExecutionServices,
    scheduler: Option<cdf_runtime::RuntimeSchedulerResolution>,
    telemetry: RunTelemetryConfig,
) -> Result<ProjectRunOutcome> {
    run_project_with_context(request, telemetry, services.clone(), scheduler).await
}

pub async fn run_project_with_telemetry(
    request: ProjectRunRequest<'_>,
    services: &ExecutionServices,
    telemetry: RunTelemetryConfig,
) -> Result<ProjectRunOutcome> {
    run_project_with_context(request, telemetry, services.clone(), None).await
}

async fn run_project_with_context(
    request: ProjectRunRequest<'_>,
    telemetry: RunTelemetryConfig,
    services: ExecutionServices,
    scheduler: Option<cdf_runtime::RuntimeSchedulerResolution>,
) -> Result<ProjectRunOutcome> {
    let mut request = request;
    validate_project_run_request(&mut request)?;
    validate_explicit_package_id(&request.package_id)?;
    let services = services.with_scheduler_measurement(telemetry.phase_metrics)?;

    let schema_hash = request
        .destination
        .output_schema(&request.plan)?
        .schema_hash;
    ensure_parent_directory(&request.state_store_path)?;

    let ProjectRunRequest {
        resource,
        plan,
        package_root,
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
    let staging_scopes: Arc<dyn ScopeLeaseStore> = Arc::new(
        cdf_state_sqlite::SqliteScopeLeaseStore::open(&state_store_path)?,
    );
    let services = services.with_staging_lease_authority(Arc::new(
        cdf_runtime::ScopeStagingLeaseAuthority::new(staging_scopes),
    ))?;
    let services = services.with_content_reachability_store(Arc::new(
        cdf_state_sqlite::SqliteContentReachabilityStore::open(&state_store_path)?,
    ));
    destination.bind_execution_services(services.clone())?;
    let run_ledger = SqliteRunLedger::open(&state_store_path)?;
    let checkpoint_store = SqliteCheckpointStore::open(&state_store_path)?;
    if matches!(
        plan.execution_extent,
        cdf_kernel::ExecutionExtent::Drain { .. }
    ) {
        return Box::pin(run_project_drain(DrainProjectExecution {
            resource,
            plan,
            package_root,
            pipeline_id,
            base_package_id: package_id,
            base_checkpoint_id: checkpoint_id,
            destination: &mut destination,
            run_id,
            event_sink,
            after_receipt_verified,
            schema_hash,
            services,
            scheduler,
            telemetry,
            run_ledger: &run_ledger,
            checkpoint_store: &checkpoint_store,
        }))
        .await;
    }

    let mut plan = plan;
    if let Some(frontier) = checkpoint_store
        .head(
            &pipeline_id,
            &resource.descriptor().resource_id,
            &resource.descriptor().state_scope,
        )?
        .as_ref()
        .map(|checkpoint| checkpoint.delta.source_resume_position())
        .filter(|position| !matches!(position, SourcePosition::FileManifest(_)))
    {
        plan.rebind_initial_committed_frontier(resource.stream(), frontier)?;
    }

    let package_dir = package_root.join(&package_id);
    refuse_existing_package_dir(&package_dir)?;
    let run = run_ledger.create_run(run_id)?;
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
        package_root,
        package_dir,
        pipeline_id: &pipeline_id,
        checkpoint_id: &checkpoint_id,
        target: destination.target().clone(),
        checkpoint_store: &checkpoint_store,
        destination: &mut destination,
        recorder: &recorder,
        after_receipt_verified,
        schema_hash,
        services,
        scheduler,
        telemetry,
        manifest_planning: ManifestPlanning::ResolveAgainstCheckpoint,
        drain_command_started: None,
    };
    match run_project_inner(execution, None).await {
        Ok(ProjectRunUnitOutcome::Committed(unit)) => {
            Ok(ProjectRunOutcome::Committed(Box::new(unit.report)))
        }
        Ok(ProjectRunUnitOutcome::NoOp(report)) => Ok(ProjectRunOutcome::NoOp(report)),
        Err(error) => {
            let _ = recorder.append_run_failed(&error);
            Err(error)
        }
    }
}

struct DrainProjectExecution<'a> {
    resource: ProjectRunSource<'a>,
    plan: EnginePlan,
    package_root: PathBuf,
    pipeline_id: PipelineId,
    base_package_id: String,
    base_checkpoint_id: CheckpointId,
    destination: &'a mut ResolvedProjectDestination,
    run_id: Option<RunId>,
    event_sink: Option<&'a dyn RunEventSink>,
    after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    schema_hash: SchemaHash,
    services: ExecutionServices,
    scheduler: Option<cdf_runtime::RuntimeSchedulerResolution>,
    telemetry: RunTelemetryConfig,
    run_ledger: &'a SqliteRunLedger,
    checkpoint_store: &'a SqliteCheckpointStore,
}

struct DrainRecoveryContext<'a> {
    package_root: &'a Path,
    base_package_id: &'a str,
    base_checkpoint_id: &'a CheckpointId,
    plan: &'a EnginePlan,
    descriptor: &'a ResourceDescriptor,
    schema_hash: &'a SchemaHash,
    services: &'a ExecutionServices,
}

async fn run_project_drain(execution: DrainProjectExecution<'_>) -> Result<ProjectRunOutcome> {
    let DrainProjectExecution {
        resource,
        plan,
        package_root,
        pipeline_id,
        base_package_id,
        base_checkpoint_id,
        destination,
        mut run_id,
        event_sink,
        after_receipt_verified,
        schema_hash,
        services,
        scheduler,
        telemetry,
        run_ledger,
        checkpoint_store,
    } = execution;
    let recovered_epoch_count = settle_existing_drain_prefix(
        DrainRecoveryContext {
            package_root: &package_root,
            base_package_id: &base_package_id,
            base_checkpoint_id: &base_checkpoint_id,
            plan: &plan,
            descriptor: resource.descriptor(),
            schema_hash: &schema_hash,
            services: &services,
        },
        destination,
        checkpoint_store,
    )?;
    let mut controller = cdf_runtime::DrainEpochController::new(&plan.execution_extent)?;
    let initial_head = checkpoint_store.head(
        &pipeline_id,
        &resource.descriptor().resource_id,
        &resource.descriptor().state_scope,
    )?;
    if let (Some(retention), Some(head)) =
        (resource.stream().replay_retention(), initial_head.as_ref())
    {
        retention.reconcile_committed_frontier(head.delta.source_resume_position())?;
    }
    controller.bind_initial_committed_state(
        initial_head
            .as_ref()
            .map(|checkpoint| checkpoint.delta.output_position.clone()),
        initial_head
            .as_ref()
            .and_then(|checkpoint| checkpoint.delta.source_continuation.clone()),
        initial_head
            .as_ref()
            .and_then(|checkpoint| checkpoint.delta.output_watermark.clone()),
        initial_head
            .as_ref()
            .map(|checkpoint| checkpoint.delta.partition_watermarks.clone())
            .unwrap_or_default(),
        recovered_epoch_count,
    )?;
    let initial_manifest =
        plan_file_manifest_incrementality(&plan, resource, initial_head.as_ref())?;
    let mut next_manifest_summary = initial_manifest.summary;
    let mut remaining_plan = initial_manifest.plan.into_owned();
    if let Some(frontier) = initial_head
        .as_ref()
        .map(|checkpoint| checkpoint.delta.source_resume_position())
        .filter(|position| !matches!(position, SourcePosition::FileManifest(_)))
    {
        remaining_plan.rebind_initial_committed_frontier(resource.stream(), frontier)?;
    }
    let mut first_run_id = None;
    let mut epoch_count = 0_u64;
    let mut total_row_count = 0_u64;
    let mut total_segment_count = 0_u64;
    let mut last_committed = None::<(ProjectRunReport, ProjectDrainEpochReport)>;
    let drain_command_started = Instant::now();

    loop {
        controller.advance_monotonic_clock(
            u64::try_from(drain_command_started.elapsed().as_millis()).unwrap_or(u64::MAX),
        )?;
        controller.validate_ready_for_epoch()?;
        let epoch_ordinal = controller.epoch_ordinal();
        let package_id = drain_epoch_string_id(&base_package_id, epoch_ordinal);
        let checkpoint_id = CheckpointId::new(drain_epoch_string_id(
            base_checkpoint_id.as_str(),
            epoch_ordinal,
        ))?;
        if remaining_plan.package_id != package_id {
            remaining_plan = remaining_plan.rebind_package_id(package_id.clone())?;
        }
        let package_dir = package_root.join(&package_id);
        refuse_existing_package_dir(&package_dir)?;

        let supplied_run_id = if first_run_id.is_none() {
            run_id.take()
        } else {
            Some(RunId::new(format!(
                "{}-epoch-{epoch_ordinal:020}",
                first_run_id
                    .as_ref()
                    .ok_or_else(|| CdfError::internal("drain run omitted its first run id"))?
            ))?)
        };
        let run = run_ledger.create_run(supplied_run_id)?;
        first_run_id.get_or_insert_with(|| run.run_id.clone());
        let recorder = ProjectRunRecorder::new(
            run_ledger,
            run.run_id,
            recorder_context(
                resource,
                &remaining_plan,
                &pipeline_id,
                &package_id,
                &package_dir,
                destination.describe().destination_id,
            ),
            event_sink,
            telemetry,
        );
        let unit = match Box::pin(run_project_inner(
            ProjectRunExecution {
                resource,
                plan: &remaining_plan,
                package_id: &package_id,
                package_root: package_root.clone(),
                package_dir,
                pipeline_id: &pipeline_id,
                checkpoint_id: &checkpoint_id,
                target: destination.target().clone(),
                checkpoint_store,
                destination: &mut *destination,
                recorder: &recorder,
                after_receipt_verified,
                schema_hash: schema_hash.clone(),
                services: services.clone(),
                scheduler: scheduler.clone(),
                telemetry,
                manifest_planning: ManifestPlanning::Preselected(next_manifest_summary.take()),
                drain_command_started: Some(drain_command_started),
            },
            Some(&mut controller),
        ))
        .await
        {
            Ok(ProjectRunUnitOutcome::Committed(unit)) => unit,
            Ok(ProjectRunUnitOutcome::NoOp(report)) => {
                if let Some((mut committed, last_epoch)) = last_committed.take() {
                    committed.drain = Some(ProjectDrainRunReport {
                        epoch_count,
                        total_row_count,
                        total_segment_count,
                        first_run_id: first_run_id.clone().ok_or_else(|| {
                            CdfError::internal("drain run omitted its first run id")
                        })?,
                        last_epoch: Box::new(last_epoch),
                    });
                    return Ok(ProjectRunOutcome::Committed(Box::new(committed)));
                }
                return Ok(ProjectRunOutcome::NoOp(report));
            }
            Err(error) => {
                let _ = recorder.append_run_failed(&error);
                return Err(error);
            }
        };

        let drain_epoch = unit.drain_epoch.ok_or_else(|| {
            CdfError::internal(
                "committed drain execution completed without canonical epoch closure evidence",
            )
        })?;
        remaining_plan.advance_committed_drain_frontier(
            drain_epoch.consumed_partition_count,
            drain_epoch.resume_partition.as_deref(),
        )?;
        epoch_count = epoch_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("drain epoch count overflow"))?;
        total_row_count = total_row_count
            .checked_add(unit.report.row_count)
            .ok_or_else(|| CdfError::data("drain row count overflow"))?;
        total_segment_count = total_segment_count
            .checked_add(unit.report.segment_count)
            .ok_or_else(|| CdfError::data("drain segment count overflow"))?;
        let last_epoch = ProjectDrainEpochReport {
            epoch_ordinal,
            run_id: unit.report.run_id.clone(),
            package_dir: unit.report.package_dir.clone(),
            package_id: unit.report.package_id.clone(),
            package_hash: unit.report.package_hash.clone(),
            checkpoint: unit.report.checkpoint.clone(),
            receipt: unit.report.receipt.clone(),
            row_count: unit.report.row_count,
            segment_count: unit.report.segment_count,
            observed_at_unix_milliseconds: drain_epoch.closure.observed_at_unix_milliseconds,
            closure: drain_epoch.closure.evidence,
        };
        if controller.is_finished() {
            let mut report = unit.report;
            report.drain = Some(ProjectDrainRunReport {
                epoch_count,
                total_row_count,
                total_segment_count,
                first_run_id: first_run_id
                    .ok_or_else(|| CdfError::internal("drain run omitted its first run id"))?,
                last_epoch: Box::new(last_epoch),
            });
            return Ok(ProjectRunOutcome::Committed(Box::new(report)));
        }
        if remaining_plan.scan.partitions.is_empty() {
            return Err(CdfError::internal(
                "drain source exhausted without a terminal epoch closure",
            ));
        }
        last_committed = Some((unit.report, last_epoch));
        next_manifest_summary = preselected_manifest_summary(&remaining_plan)?;
    }
}

fn settle_existing_drain_prefix<Store>(
    context: DrainRecoveryContext<'_>,
    destination: &mut ResolvedProjectDestination,
    checkpoint_store: &Store,
) -> Result<u64>
where
    Store: CheckpointStore + ?Sized,
{
    let mut ordinal = 0_u64;
    loop {
        let package_id = drain_epoch_string_id(context.base_package_id, ordinal);
        let package_dir = context.package_root.join(&package_id);
        if !package_dir.exists() {
            return Ok(ordinal);
        }
        let expected_checkpoint_id = CheckpointId::new(drain_epoch_string_id(
            context.base_checkpoint_id.as_str(),
            ordinal,
        ))?;
        let package = PackageReader::open(&package_dir)?;
        if matches!(
            package.manifest().lifecycle.status,
            PackageStatus::Planned | PackageStatus::Extracting | PackageStatus::Validated
        ) {
            let target = destination.target().clone();
            let active = ActiveStagedIngress::begin(
                destination.runtime_mut(),
                StagedIngressPlan {
                    checkpoint_id: expected_checkpoint_id.clone(),
                    execution_plan_id: context.plan.scan.plan_id.clone(),
                    target,
                    disposition: context.plan.write_disposition.clone(),
                    schema_hash: context.schema_hash.clone(),
                    output_schema: context.plan.output_arrow_schema()?.as_ref().clone(),
                    merge_keys: context.descriptor.merge_key.clone(),
                },
                context.services,
            )?;
            if let Some(active) = active {
                active.abort()?;
            }
            package.discard_incomplete_construction(&package_id)?;
            return Ok(ordinal);
        }
        let replay = settle_existing_package_with_runtime(
            &package_dir,
            destination.runtime_mut(),
            checkpoint_store,
            context.services.memory(),
            Some(context.services),
        )?;
        if replay.checkpoint.delta.checkpoint_id != expected_checkpoint_id {
            return Err(CdfError::data(format!(
                "drain package {} belongs to checkpoint {}, expected {} for epoch {}",
                package_dir.display(),
                replay.checkpoint.delta.checkpoint_id,
                expected_checkpoint_id,
                ordinal
            )));
        }
        let package = PackageReader::open(&package_dir)?.into_verified()?;
        let closure: cdf_kernel::EpochClosureEvidence = package
            .reader()
            .verified_json_artifact(package.verification(), "plan/epoch-closure.json")?;
        closure.validate()?;
        if closure.frontier.epoch_ordinal != ordinal
            || closure.frontier.frontier != replay.checkpoint.delta.output_position
        {
            return Err(CdfError::data(format!(
                "drain package {} closure does not match its ordinal and committed checkpoint frontier",
                package_dir.display()
            )));
        }
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("recovered drain epoch ordinal overflow"))?;
    }
}

fn drain_epoch_string_id(base: &str, epoch_ordinal: u64) -> String {
    if epoch_ordinal == 0 {
        base.to_owned()
    } else {
        format!("{base}-epoch-{epoch_ordinal:020}")
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
    package_root: PathBuf,
    package_dir: PathBuf,
    pipeline_id: &'a PipelineId,
    checkpoint_id: &'a CheckpointId,
    target: TargetName,
    checkpoint_store: &'a SqliteCheckpointStore,
    destination: &'a mut ResolvedProjectDestination,
    recorder: &'a ProjectRunRecorder<'a>,
    after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    schema_hash: SchemaHash,
    services: ExecutionServices,
    scheduler: Option<cdf_runtime::RuntimeSchedulerResolution>,
    telemetry: RunTelemetryConfig,
    manifest_planning: ManifestPlanning,
    drain_command_started: Option<Instant>,
}

struct ProjectRunUnit {
    report: ProjectRunReport,
    drain_epoch: Option<cdf_engine::EngineDrainEpoch>,
}

enum ProjectRunUnitOutcome {
    Committed(Box<ProjectRunUnit>),
    NoOp(Box<ProjectRunNoOpReport>),
}

enum ManifestPlanning {
    ResolveAgainstCheckpoint,
    Preselected(Option<FileManifestRunSummary>),
}

pub(crate) fn load_late_data_carryover(
    package_root: &Path,
    head: Option<&Checkpoint>,
) -> Result<Vec<cdf_engine::LateDataCarryoverInput>> {
    let Some(head) = head else {
        return Ok(Vec::new());
    };
    let references = &head.delta.late_data_carryover;
    cdf_kernel::validate_late_data_carryover_refs(references)?;
    let Some(package_id) = references
        .first()
        .map(|reference| reference.package_id.as_str())
    else {
        return Ok(Vec::new());
    };
    let reader = PackageReader::open(package_root.join(package_id))?;
    if reader.manifest().identity.package_id != package_id {
        return Err(CdfError::data(
            "late-data carryover package directory does not match its package identity",
        ));
    }
    let verified = Arc::new(reader.verify_for_consumption()?);
    if verified.package_hash() != head.delta.package_hash.as_str() {
        return Err(CdfError::data(
            "late-data carryover package does not match the committed checkpoint head",
        ));
    }
    references
        .iter()
        .map(|reference| {
            let object =
                reader.verified_identity_object(Arc::clone(&verified), &reference.relative_path)?;
            cdf_engine::LateDataCarryoverInput::new(reference.clone(), object)
        })
        .collect()
}

async fn run_project_inner(
    execution: ProjectRunExecution<'_>,
    mut drain_controller: Option<&mut cdf_runtime::DrainEpochController>,
) -> Result<ProjectRunUnitOutcome> {
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
    let prior_schema_streak = match head.as_ref() {
        Some(checkpoint)
            if execution
                .plan
                .validation_program
                .promotion
                .allow_sampled_fast_path =>
        {
            execution.checkpoint_store.committed_schema_streak(
                execution.pipeline_id,
                &descriptor.resource_id,
                &scope,
                &checkpoint.delta.schema_hash,
                execution
                    .plan
                    .validation_program
                    .promotion
                    .clean_runs_required,
            )?
        }
        Some(_) | None => 0,
    };
    let manifest_plan = match &execution.manifest_planning {
        ManifestPlanning::ResolveAgainstCheckpoint => {
            plan_file_manifest_incrementality(execution.plan, execution.resource, head.as_ref())?
        }
        ManifestPlanning::Preselected(summary) => FileManifestPlanning {
            plan: Cow::Borrowed(execution.plan),
            summary: summary.clone(),
        },
    };
    let no_planned_partitions = manifest_plan.plan.scan.partition_count()? == 0;
    execution.recorder.append_plan_recorded(
        if manifest_plan.no_changed_files() || no_planned_partitions {
            0
        } else {
            1
        },
    )?;
    let pending_late_data_carryover = head
        .as_ref()
        .is_some_and(|checkpoint| !checkpoint.delta.late_data_carryover.is_empty());
    if manifest_plan.no_changed_files() && !pending_late_data_carryover {
        return no_op_report(
            execution,
            head,
            manifest_plan.summary,
            ProjectRunNoOpReason::FileManifestUnchanged,
        )
        .map(Box::new)
        .map(ProjectRunUnitOutcome::NoOp);
    }
    if no_planned_partitions && head.is_some() && !pending_late_data_carryover {
        return no_op_report(
            execution,
            head,
            manifest_plan.summary,
            ProjectRunNoOpReason::SourcePositionUnchanged,
        )
        .map(Box::new)
        .map(ProjectRunUnitOutcome::NoOp);
    }
    if pending_late_data_carryover && drain_controller.is_none() {
        return Err(CdfError::contract(
            "pending late-data carryover requires a drain execution plan",
        ));
    }
    let mut late_data_carryover = Some(load_late_data_carryover(
        &execution.package_root,
        head.as_ref(),
    )?);

    execution.recorder.append_package_started()?;

    let destination_capabilities = execution.destination.runtime_capabilities();
    if let Some(graph) = &manifest_plan.plan.operator_graph {
        graph.validate_destination_join(&destination_capabilities)?;
    }

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
    let mut active_staged = ActiveStagedIngress::begin(
        execution.destination.runtime_mut(),
        StagedIngressPlan {
            checkpoint_id: execution.checkpoint_id.clone(),
            execution_plan_id: manifest_plan.plan.scan.plan_id.clone(),
            target: execution.target.clone(),
            disposition: manifest_plan.plan.write_disposition.clone(),
            schema_hash: execution.schema_hash.clone(),
            output_schema: manifest_plan.plan.output_arrow_schema()?.as_ref().clone(),
            merge_keys: descriptor.merge_key.clone(),
        },
        &execution.services,
    )?;
    let options = EngineExecutionOptions::default()
        .with_phase_metrics(execution.recorder.phase_telemetry_enabled())
        .with_statistics_profile(execution.telemetry.statistics_profile)
        .with_execution_services(execution.services.clone());
    let options = match execution.scheduler.as_ref() {
        Some(scheduler) => {
            let partition_count = usize::try_from(manifest_plan.plan.scan.partition_count()?)
                .map_err(|_| {
                    CdfError::data("scan partition count exceeds this process address space")
                })?;
            options.with_scheduler_resolution(scheduler.narrow_to_partition_count(partition_count))
        }
        None => options,
    };
    let retry_evidence = options.source_retry_evidence();
    let output_result = match (active_staged.as_mut(), drain_controller.as_deref_mut()) {
        (Some(staged), Some(controller)) => {
            let staged = std::cell::RefCell::new(staged);
            let mut durable_segment =
                |entry: &SegmentEntry, payload: cdf_engine::DurableSegmentPayload| {
                    staged.borrow_mut().stage_segment(entry, payload)
                };
            let mut stream_finalize = || staged.borrow_mut().finish_background();
            cdf_engine::execute_drain_epoch_with_hooks(
                &manifest_plan.plan,
                resource,
                &execution.package_dir,
                &write_package_pre_finalize_artifacts,
                cdf_engine::DrainEpochExecution::new(controller)
                    .with_streaming_hooks(&mut durable_segment, &mut stream_finalize)
                    .with_late_data_carryover(late_data_carryover.take().ok_or_else(|| {
                        CdfError::internal("late-data carryover input was consumed twice")
                    })?),
                options,
            )
            .await
        }
        (None, Some(controller)) => {
            cdf_engine::execute_drain_epoch_with_hooks(
                &manifest_plan.plan,
                resource,
                &execution.package_dir,
                &write_package_pre_finalize_artifacts,
                cdf_engine::DrainEpochExecution::new(controller).with_late_data_carryover(
                    late_data_carryover.take().ok_or_else(|| {
                        CdfError::internal("late-data carryover input was consumed twice")
                    })?,
                ),
                options,
            )
            .await
        }
        (Some(staged), None) => {
            let staged = std::cell::RefCell::new(staged);
            let mut durable_segment =
                |entry: &SegmentEntry, payload: cdf_engine::DurableSegmentPayload| {
                    staged.borrow_mut().stage_segment(entry, payload)
                };
            let mut stream_finalize = || staged.borrow_mut().finish_background();
            execute_to_package_with_streaming_hooks(
                &manifest_plan.plan,
                resource,
                &execution.package_dir,
                &write_package_pre_finalize_artifacts,
                &mut durable_segment,
                &mut stream_finalize,
                options,
            )
            .await
            .map(|output| cdf_engine::EngineDrainEpochOutcome::Package(Box::new(output)))
        }
        (None, None) => execute_to_package_with_segment_positions_and_pre_finalize(
            &manifest_plan.plan,
            resource,
            &execution.package_dir,
            &write_package_pre_finalize_artifacts,
            options,
        )
        .await
        .map(|output| cdf_engine::EngineDrainEpochOutcome::Package(Box::new(output))),
    };
    let retry_history = retry_evidence.snapshot().and_then(|retry_history| {
        execution.recorder.append_source_retries(
            &retry_history,
            manifest_plan.plan.partition_schedule.as_ref(),
        )?;
        Ok(retry_history)
    });
    let output = match output_result {
        Ok(cdf_engine::EngineDrainEpochOutcome::Package(output)) => *output,
        Ok(cdf_engine::EngineDrainEpochOutcome::FinishedNoOp { source_frontier }) => {
            if let Some(staged) = active_staged.take() {
                staged.abort()?;
            }
            retry_history?;
            execution
                .recorder
                .complete_phase(RunPhase::PackageExecution, 0, 0, 0)?;
            let mut report = no_op_report(
                execution,
                head,
                manifest_plan.summary,
                ProjectRunNoOpReason::SourceExhausted,
            )?;
            report.source_frontier = source_frontier;
            return Ok(ProjectRunUnitOutcome::NoOp(Box::new(report)));
        }
        Err(mut error) => {
            if let Some(staged) = active_staged.take()
                && let Err(cleanup) = staged.abort()
            {
                error.message = format!(
                    "{}; staged ingress cleanup also failed: {}",
                    error.message, cleanup.message
                );
            }
            if let Err(evidence_error) = retry_history {
                error.message = format!(
                    "{}; source retry evidence persistence also failed: {}",
                    error.message, evidence_error.message
                );
            }
            return Err(error);
        }
    };
    let retry_history = retry_history?;
    if output.execution_evidence().source_retries() != retry_history {
        return Err(CdfError::internal(
            "engine output retry evidence diverged from the durable runtime journal",
        ));
    }

    for metric in output.phase_metrics.iter().cloned() {
        execution.recorder.append_phase_metric(metric)?;
    }
    if let Some(metric) = active_staged
        .as_ref()
        .map(ActiveStagedIngress::ingress_metric)
        .transpose()?
        .flatten()
    {
        execution.recorder.append_phase_metric(metric)?;
    }
    execution.recorder.complete_phase(
        RunPhase::PackageExecution,
        output.output.profile.output_bytes,
        output.output.profile.output_bytes,
        output.output.profile.output_batches,
    )?;

    let package = PackageReader::open(&execution.package_dir)?
        .with_verification(output.output.verification.clone())?;
    let replay_inputs = package.replay_inputs()?;
    let package_hash = replay_inputs.state_delta.package_hash.clone();
    let profile = &output.output.profile;
    let row_count = profile.output_rows;
    let segment_count = output.output.identity_segment_count();
    let mut segment_index = 0_u64;
    output.output.for_each_identity_segment(&mut |segment| {
        segment_index = segment_index
            .checked_add(1)
            .ok_or_else(|| CdfError::data("package progress segment index overflowed u64"))?;
        execution
            .recorder
            .append_package_segment_recorded(&segment, segment_index, segment_count)
    })?;
    let quarantine_record_count = package.reader().quarantine_record_count()?;
    execution.recorder.append_package_finalized(
        &package_hash,
        row_count,
        profile.output_bytes,
        profile.output_batches,
        segment_count,
        quarantine_record_count,
    )?;
    let mut has_quarantine_artifacts = false;
    output.output.for_each_identity_file(&mut |file| {
        has_quarantine_artifacts |= file.path.starts_with("quarantine/");
        Ok(())
    })?;
    for transition in validation_depth_transitions_recorded(
        &execution.plan.validation_program,
        head.as_ref(),
        prior_schema_streak,
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
    if drain_controller.is_some()
        && let Some(retention) = resource.replay_retention()
    {
        retention
            .validate_checkpoint_frontier(replay_inputs.state_delta.source_resume_position())?;
    }
    let replay_memory = execution.services.memory();
    let replay_report = replay_package_with_runtime_and_staged(
        package,
        execution.destination.runtime_mut(),
        execution.checkpoint_store,
        replay_memory,
        PackageReplayHooks {
            after_receipt_verified: execution.after_receipt_verified,
            stage: Some(&stage_hook),
        },
        active_staged,
        Some(&execution.services),
    )?;
    let drain_epoch = output.drain_epoch.clone();
    match (drain_controller, drain_epoch.as_ref()) {
        (Some(controller), Some(epoch)) => {
            if replay_report.checkpoint.delta.output_position != epoch.closure.frontier.frontier {
                return Err(CdfError::data(
                    "drain checkpoint output position does not match the package's canonical epoch frontier",
                ));
            }
            if let Some(retention) = resource.replay_retention() {
                retention.commit_checkpoint_frontier(
                    replay_report.checkpoint.delta.source_resume_position(),
                )?;
            }
            let command_started = execution.drain_command_started.ok_or_else(|| {
                CdfError::internal("drain settlement omitted its command clock authority")
            })?;
            controller.advance_monotonic_clock(
                u64::try_from(command_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            )?;
            controller.acknowledge_settlement(&replay_report.checkpoint.delta.output_position)?;
        }
        (Some(_), None) => {
            return Err(CdfError::internal(
                "drain package reached settlement without epoch closure evidence",
            ));
        }
        (None, Some(_)) => {
            return Err(CdfError::internal(
                "bounded project execution produced drain epoch closure evidence",
            ));
        }
        (None, None) => {}
    }
    execution.recorder.append_run_succeeded()?;
    let ledger_snapshot = execution.recorder.snapshot()?;

    Ok(ProjectRunUnitOutcome::Committed(Box::new(ProjectRunUnit {
        report: ProjectRunReport {
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
            terminal_schema_quarantines: output.output.terminal_schema_quarantines.clone(),
            runtime_scheduler: execution.services.scheduler_report()?,
            source_frontier: output.source_frontier.clone(),
            drain: None,
        },
        drain_epoch,
    })))
}

struct FileManifestPlanning<'a> {
    plan: Cow<'a, EnginePlan>,
    summary: Option<FileManifestRunSummary>,
}

impl FileManifestPlanning<'_> {
    fn no_changed_files(&self) -> bool {
        self.summary
            .as_ref()
            .is_some_and(|summary| summary.total_file_count > 0 && summary.changed_file_count == 0)
    }
}

fn plan_file_manifest_incrementality<'a>(
    plan: &'a EnginePlan,
    resource: ProjectRunSource<'_>,
    head: Option<&Checkpoint>,
) -> Result<FileManifestPlanning<'a>> {
    let descriptor = resource.descriptor();
    if resource.capabilities().incremental != IncrementalShape::File {
        return Ok(FileManifestPlanning {
            plan: Cow::Borrowed(plan),
            summary: None,
        });
    }
    if plan.scan.planned_task_set.is_some() {
        let total_file_count = plan.scan.partition_count()?;
        if descriptor.write_disposition != WriteDisposition::Append {
            return Ok(FileManifestPlanning {
                plan: Cow::Borrowed(plan),
                summary: Some(FileManifestRunSummary {
                    total_file_count,
                    changed_file_count: total_file_count,
                    unchanged_file_count: 0,
                }),
            });
        }
        let Some(committed) = head.map(|checkpoint| &checkpoint.delta.output_position) else {
            return Ok(FileManifestPlanning {
                plan: Cow::Borrowed(plan),
                summary: Some(FileManifestRunSummary {
                    total_file_count,
                    changed_file_count: total_file_count,
                    unchanged_file_count: 0,
                }),
            });
        };
        let mut selected = plan.clone();
        selected.rebind_initial_committed_frontier(resource.stream(), committed)?;
        let changed_file_count = selected.scan.partition_count()?;
        return Ok(FileManifestPlanning {
            plan: Cow::Owned(selected),
            summary: Some(FileManifestRunSummary {
                total_file_count,
                changed_file_count,
                unchanged_file_count: total_file_count.saturating_sub(changed_file_count),
            }),
        });
    }
    let Some(current_files) = file_positions_from_partitions(&plan.scan.partitions)? else {
        return Ok(FileManifestPlanning {
            plan: Cow::Borrowed(plan),
            summary: None,
        });
    };
    if descriptor.write_disposition != WriteDisposition::Append {
        let current_file_count = u64::try_from(current_files.len())
            .map_err(|_| CdfError::data("file manifest count exceeds u64"))?;
        return Ok(FileManifestPlanning {
            plan: Cow::Borrowed(plan),
            summary: Some(FileManifestRunSummary {
                total_file_count: current_file_count,
                changed_file_count: current_file_count,
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
                .is_none_or(|previous| !cdf_kernel::same_file_position_identity(previous, file))
        })
        .map(|file| file.path.clone())
        .collect::<BTreeSet<_>>();
    let total_file_count = u64::try_from(current_files.len())
        .map_err(|_| CdfError::data("file manifest count exceeds u64"))?;
    let changed_file_count = u64::try_from(changed_paths.len())
        .map_err(|_| CdfError::data("changed file manifest count exceeds u64"))?;
    let selected = plan
        .scan
        .partitions
        .iter()
        .zip(&current_files)
        .filter(|(_, file)| changed_paths.contains(&file.path))
        .map(|(partition, _)| partition.partition_id.clone())
        .collect::<BTreeSet<_>>();
    let filtered = plan.clone().select_partitions(&selected)?;

    Ok(FileManifestPlanning {
        plan: Cow::Owned(filtered),
        summary: Some(FileManifestRunSummary {
            total_file_count,
            changed_file_count,
            unchanged_file_count: total_file_count.saturating_sub(changed_file_count),
        }),
    })
}

fn preselected_manifest_summary(plan: &EnginePlan) -> Result<Option<FileManifestRunSummary>> {
    let Some(files) = file_positions_from_partitions(&plan.scan.partitions)? else {
        return Ok(None);
    };
    let file_count = u64::try_from(files.len())
        .map_err(|_| CdfError::data("file manifest count exceeds u64"))?;
    Ok(Some(FileManifestRunSummary {
        total_file_count: file_count,
        changed_file_count: file_count,
        unchanged_file_count: 0,
    }))
}

fn file_positions_from_partitions(
    partitions: &[PartitionPlan],
) -> Result<Option<Vec<FilePosition>>> {
    let mut files = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let Some(file) = partition.planned_file()? else {
            if files.is_empty() {
                continue;
            }
            return Err(CdfError::contract(
                "one scan plan cannot mix file-manifest and non-file planned positions",
            ));
        };
        files.push(file.clone());
    }
    if files.is_empty() {
        return Ok(None);
    }
    if files.len() != partitions.len() {
        return Err(CdfError::contract(
            "one scan plan cannot mix file-manifest and non-file planned positions",
        ));
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

fn no_op_report(
    execution: ProjectRunExecution<'_>,
    head: Option<Checkpoint>,
    summary: Option<FileManifestRunSummary>,
    reason: ProjectRunNoOpReason,
) -> Result<ProjectRunNoOpReport> {
    execution.recorder.append_run_succeeded()?;
    let ledger_snapshot = execution.recorder.snapshot()?;
    Ok(ProjectRunNoOpReport {
        run_id: execution.recorder.run_id.clone(),
        ledger_snapshot,
        reason,
        current_checkpoint: head,
        file_manifest: summary,
        terminal_schema_quarantines: execution
            .plan
            .effective_schema_evidence()
            .map(|evidence| evidence.terminal_quarantines.clone())
            .unwrap_or_default(),
        runtime_scheduler: execution.services.scheduler_report()?,
        source_frontier: cdf_runtime::SourceFrontierReport::default(),
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
            bulk_path,
        } => recorder.append_replay_stage(RuntimeStage::DestinationCommitStarted {
            plan_id,
            segment_count,
            bulk_path,
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
    prior_schema_streak: u32,
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
    let prior_promoted = head.is_some() && prior_schema_streak >= promotion.clean_runs_required;
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
    let prior_stable_count =
        if head.is_some_and(|checkpoint| checkpoint.delta.schema_hash == *schema_hash) {
            prior_schema_streak
        } else {
            0
        };
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
