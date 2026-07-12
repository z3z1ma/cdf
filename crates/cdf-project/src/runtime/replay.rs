use super::{
    destinations::{
        DestinationPlanningContext, ProjectDestinationRuntime, ResolvedProjectDestination,
    },
    hooks::{ReceiptVerifiedHook, RuntimeStage, RuntimeStageHook},
    prelude::*,
    receipts::validate_destination_receipt_before_checkpoint,
    types::*,
};
use std::sync::Arc;

use cdf_memory::{DEFAULT_PROCESS_BUDGET_BYTES, DeterministicMemoryCoordinator, MemoryCoordinator};

type DestinationReplayStageHook<'a> = RuntimeStageHook<'a>;
pub(crate) type PackageReplayStageHook<'a> = &'a dyn Fn(PackageReplayStage<'_>) -> Result<()>;

#[derive(Clone, Copy, Debug)]
pub(crate) enum PackageReplayStage<'a> {
    PackageReplayVerified,
    CheckpointProposed {
        delta: &'a StateDelta,
    },
    DestinationWriteReady,
    DestinationCommitStarted {
        plan_id: &'a PlanId,
        segment_count: usize,
    },
    DestinationSegmentAcknowledged {
        ack: &'a SegmentAck,
    },
    DestinationReceiptRecorded {
        receipt: &'a Receipt,
    },
    CheckpointCommitted {
        checkpoint: &'a Checkpoint,
    },
    PackageStatusUpdated {
        status: &'a PackageStatus,
    },
}

#[derive(Default)]
pub(crate) struct PackageReplayHooks<'a> {
    pub(crate) after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    pub(crate) stage: Option<PackageReplayStageHook<'a>>,
}

pub fn replay_package_from_artifacts<Store>(
    request: PackageArtifactReplayRequest<'_, Store>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    replay_package_from_artifacts_with_stage_hook(request, None)
}

pub fn replay_package_from_artifacts_with_stage_hook<Store>(
    request: PackageArtifactReplayRequest<'_, Store>,
    stage_hook: Option<RuntimeStageHook<'_>>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    let inputs = reader.replay_inputs()?;
    let runtime_stage_hook =
        |stage: PackageReplayStage<'_>| notify_runtime_replay_stage(stage_hook, stage);
    replay_package_with_resolved_destination(
        reader,
        request.package_dir,
        request.destination,
        request.checkpoint_store,
        inputs,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: Some(&runtime_stage_hook),
        },
    )
}

pub fn recover_package_from_artifacts<Store>(
    request: PackageArtifactRecoveryRequest<'_, Store>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    let inputs = reader.replay_inputs()?;
    recover_package_with_resolved_destination(
        reader,
        request.destination,
        request.checkpoint_store,
        inputs,
        request.receipt,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
        },
    )
}

pub fn replay_prepared_package<Store>(
    request: PreparedPackageReplayRequest<'_, Store>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    replay_prepared_package_with_stage_hook(request, None)
}

pub fn replay_prepared_package_with_stage_hook<Store>(
    request: PreparedPackageReplayRequest<'_, Store>,
    stage_hook: Option<RuntimeStageHook<'_>>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    let runtime_stage_hook =
        |stage: PackageReplayStage<'_>| notify_runtime_replay_stage(stage_hook, stage);
    replay_package_with_resolved_destination(
        reader,
        request.package_dir,
        request.destination,
        request.checkpoint_store,
        request.inputs,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: Some(&runtime_stage_hook),
        },
    )
}

pub fn recover_prepared_package<Store>(
    request: PreparedPackageRecoveryRequest<'_, Store>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    recover_package_with_resolved_destination(
        reader,
        request.destination,
        request.checkpoint_store,
        request.inputs,
        request.receipt,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
        },
    )
}

fn replay_package_with_resolved_destination<Store>(
    reader: PackageReader,
    package_dir: PathBuf,
    mut destination: ResolvedProjectDestination,
    checkpoint_store: &Store,
    inputs: PackageReplayInputs,
    hooks: PackageReplayHooks<'_>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    validate_resolved_destination_target(&destination, &inputs)?;
    let memory = default_replay_memory()?;
    replay_package_with_runtime(
        reader,
        package_dir,
        destination.runtime_mut(),
        checkpoint_store,
        inputs,
        memory,
        hooks,
    )
}

fn recover_package_with_resolved_destination<Store>(
    reader: PackageReader,
    mut destination: ResolvedProjectDestination,
    checkpoint_store: &Store,
    inputs: PackageReplayInputs,
    receipt: Receipt,
    hooks: PackageReplayHooks<'_>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    validate_resolved_destination_target(&destination, &inputs)?;
    recover_package_with_runtime(
        reader,
        destination.runtime_mut(),
        checkpoint_store,
        inputs,
        receipt,
        hooks,
    )
}

fn validate_resolved_destination_target(
    destination: &ResolvedProjectDestination,
    inputs: &PackageReplayInputs,
) -> Result<()> {
    if destination.target() != &inputs.destination_commit.target {
        return Err(CdfError::contract(format!(
            "resolved destination target {} does not match package destination commit target {}",
            destination.target(),
            inputs.destination_commit.target
        )));
    }
    Ok(())
}

pub(crate) fn replay_package_with_runtime<Store>(
    mut reader: PackageReader,
    package_dir: PathBuf,
    runtime: &mut dyn ProjectDestinationRuntime,
    checkpoint_store: &Store,
    inputs: PackageReplayInputs,
    memory: Arc<dyn MemoryCoordinator>,
    hooks: PackageReplayHooks<'_>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    validate_package_replay_inputs(&reader, &inputs)?;
    notify_destination_replay_stage(&hooks, PackageReplayStage::PackageReplayVerified)?;

    let checkpoint_id = inputs.state_delta.checkpoint_id.clone();
    checkpoint_store.propose(inputs.state_delta.clone())?;
    if let Err(error) = notify_destination_replay_stage(
        &hooks,
        PackageReplayStage::CheckpointProposed {
            delta: &inputs.state_delta,
        },
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    if let Err(error) = reader.update_status(PackageStatus::Loading) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    notify_destination_replay_stage(&hooks, PackageReplayStage::DestinationWriteReady)?;

    let mut prepared = match runtime.prepare_package_commit(
        &package_dir,
        &reader,
        &inputs,
        &DestinationPlanningContext::new()
            .with_after_receipt_verified(hooks.after_receipt_verified),
    ) {
        Ok(prepared) => prepared,
        Err(error) => {
            let _ = checkpoint_store.abandon(&checkpoint_id);
            return Err(error);
        }
    };
    let receipt_policy = prepared.reporting_policy.clone();
    let receipts_before = reader.receipts()?.len();
    if let Err(error) = runtime.bind_prepared_commit(&mut prepared) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    if let Err(error) = notify_destination_replay_stage(
        &hooks,
        PackageReplayStage::DestinationCommitStarted {
            plan_id: &prepared.plan.plan_id,
            segment_count: prepared.commit.segments.len(),
        },
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }

    let receipt = match commit_prepared_package_through_session(
        runtime, &reader, &prepared, memory, &hooks,
    ) {
        Ok(receipt) => receipt,
        Err(error) => {
            let _ = checkpoint_store.abandon(&checkpoint_id);
            return Err(error);
        }
    };

    let package_receipt_recorded = reader.receipts()?.len() > receipts_before;
    verify_receipt_and_notify(runtime, &inputs, &receipt, &hooks)?;

    let checkpoint = checkpoint_store.commit(&inputs.state_delta.checkpoint_id, receipt.clone())?;
    let package_status = mark_package_checkpointed_after_commit(&mut reader, &checkpoint, &hooks)?;

    Ok(PackageReplayReport {
        checkpoint,
        receipt,
        receipt_source: super::destinations::project_receipt_source(
            receipt_policy,
            package_receipt_recorded,
        ),
        package_status,
    })
}

pub(crate) fn recover_package_with_runtime<Store>(
    mut reader: PackageReader,
    runtime: &mut dyn ProjectDestinationRuntime,
    checkpoint_store: &Store,
    inputs: PackageReplayInputs,
    receipt: Receipt,
    hooks: PackageReplayHooks<'_>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    validate_package_replay_inputs(&reader, &inputs)?;
    verify_receipt_and_notify(runtime, &inputs, &receipt, &hooks)?;

    let checkpoint = commit_or_reuse_committed_checkpoint(
        checkpoint_store,
        &inputs.state_delta,
        receipt.clone(),
    )?;
    let package_status = mark_package_checkpointed_after_commit(&mut reader, &checkpoint, &hooks)?;

    Ok(PackageReplayReport {
        checkpoint,
        receipt,
        receipt_source: ProjectReceiptSource::SuppliedDurableReceipt,
        package_status,
    })
}

fn commit_prepared_package_through_session(
    runtime: &dyn ProjectDestinationRuntime,
    reader: &PackageReader,
    prepared: &super::destinations::PreparedDestinationCommit,
    memory: Arc<dyn MemoryCoordinator>,
    hooks: &PackageReplayHooks<'_>,
) -> Result<Receipt> {
    let capabilities = runtime.runtime_capabilities();
    let mut session = runtime
        .protocol()
        .begin(prepared.commit.clone(), prepared.plan.clone())?;
    if let Err(error) = session.apply_migrations() {
        let _ = session.abort();
        return Err(error);
    }
    if let Err(error) = write_package_segments_to_session(
        session.as_mut(),
        reader,
        &prepared.commit,
        &capabilities,
        memory,
        hooks,
    ) {
        let _ = session.abort();
        return Err(error);
    }
    session.finalize()
}

fn verify_receipt_and_notify(
    runtime: &mut dyn ProjectDestinationRuntime,
    inputs: &PackageReplayInputs,
    receipt: &Receipt,
    hooks: &PackageReplayHooks<'_>,
) -> Result<()> {
    verify_destination_receipt_before_checkpoint_with_runtime(
        runtime,
        &inputs.state_delta,
        &inputs.destination_commit.target,
        &inputs.destination_commit.disposition,
        receipt,
    )?;
    notify_destination_replay_stage(
        hooks,
        PackageReplayStage::DestinationReceiptRecorded { receipt },
    )?;
    if let Some(hook) = hooks.after_receipt_verified {
        hook(receipt)?;
    }
    Ok(())
}

fn verify_destination_receipt_before_checkpoint_with_runtime(
    runtime: &mut dyn ProjectDestinationRuntime,
    delta: &StateDelta,
    target: &TargetName,
    disposition: &WriteDisposition,
    receipt: &Receipt,
) -> Result<()> {
    validate_destination_receipt_before_checkpoint(delta, target, disposition, receipt)?;
    let verification = runtime.verify_receipt(receipt)?;
    if !verification.verified {
        return Err(CdfError::destination(format!(
            "destination receipt {} did not verify: {}",
            verification.receipt_id,
            verification
                .reason
                .unwrap_or_else(|| "verification returned false".to_owned())
        )));
    }
    Ok(())
}

fn mark_package_checkpointed_after_commit(
    reader: &mut PackageReader,
    checkpoint: &Checkpoint,
    hooks: &PackageReplayHooks<'_>,
) -> Result<PackageStatus> {
    notify_destination_replay_stage(
        hooks,
        PackageReplayStage::CheckpointCommitted { checkpoint },
    )?;
    let package_status = reader
        .update_status(PackageStatus::Checkpointed)?
        .lifecycle
        .status
        .clone();
    notify_destination_replay_stage(
        hooks,
        PackageReplayStage::PackageStatusUpdated {
            status: &package_status,
        },
    )?;
    Ok(package_status)
}

fn write_package_segments_to_session(
    session: &mut dyn cdf_kernel::CommitSession,
    reader: &PackageReader,
    commit: &DestinationCommitRequest,
    capabilities: &cdf_runtime::DestinationRuntimeCapabilities,
    memory: Arc<dyn MemoryCoordinator>,
    hooks: &PackageReplayHooks<'_>,
) -> Result<()> {
    let mut acknowledge = |segment| {
        let ack = session.write_segment(segment)?;
        notify_destination_replay_stage(
            hooks,
            PackageReplayStage::DestinationSegmentAcknowledged { ack: &ack },
        )
    };
    match capabilities.commit_payload_mode {
        cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming => {
            let budget = memory.snapshot().budget_bytes;
            let maximum_segment_bytes = capabilities
                .max_in_flight_bytes
                .unwrap_or(64 * 1024 * 1024)
                .min(budget);
            let stream = reader.verified_commit_segment_stream(
                &commit.segments,
                memory,
                maximum_segment_bytes,
            )?;
            for segment in stream {
                acknowledge(segment?.into_commit_segment()?)?;
            }
        }
        cdf_runtime::DestinationCommitPayloadMode::MaterializedPackage => {
            reader.verify()?;
            for segment in reader.read_commit_segments(&commit.segments)? {
                acknowledge(segment)?;
            }
        }
    }
    Ok(())
}

fn default_replay_memory() -> Result<Arc<dyn MemoryCoordinator>> {
    Ok(Arc::new(DeterministicMemoryCoordinator::new(
        DEFAULT_PROCESS_BUDGET_BYTES,
        Default::default(),
    )?))
}

fn validate_package_replay_inputs(
    reader: &PackageReader,
    inputs: &PackageReplayInputs,
) -> Result<ReplayView> {
    reader.verify()?;
    let replay = reader.replay_view()?;
    if replay.package_hash != inputs.state_delta.package_hash {
        return Err(CdfError::data(format!(
            "package hash {} does not match StateDelta package hash {}",
            replay.package_hash, inputs.state_delta.package_hash
        )));
    }
    if inputs.schema_hash != inputs.state_delta.schema_hash {
        return Err(CdfError::contract(format!(
            "destination schema hash {} does not match StateDelta schema hash {}",
            inputs.schema_hash, inputs.state_delta.schema_hash
        )));
    }
    if inputs.destination_commit.package_hash != inputs.state_delta.package_hash {
        return Err(CdfError::contract(format!(
            "destination commit package hash {} does not match StateDelta package hash {}",
            inputs.destination_commit.package_hash, inputs.state_delta.package_hash
        )));
    }
    if inputs.destination_commit.segments != inputs.state_delta.segments {
        return Err(CdfError::contract(
            "destination commit segments do not match StateDelta segments",
        ));
    }
    if inputs.destination_commit.idempotency_token.as_str()
        != inputs.state_delta.package_hash.as_str()
    {
        return Err(CdfError::contract(format!(
            "destination commit idempotency token {} does not match package hash {}",
            inputs.destination_commit.idempotency_token, inputs.state_delta.package_hash
        )));
    }
    validate_package_segments_match_delta(reader, &replay.segments, &inputs.state_delta)?;
    Ok(replay)
}

fn notify_runtime_replay_stage(
    hook: Option<DestinationReplayStageHook<'_>>,
    stage: PackageReplayStage<'_>,
) -> Result<()> {
    let Some(hook) = hook else {
        return Ok(());
    };
    match stage {
        PackageReplayStage::PackageReplayVerified => hook(RuntimeStage::PackageReplayVerified),
        PackageReplayStage::CheckpointProposed { delta } => {
            hook(RuntimeStage::CheckpointProposed { delta })
        }
        PackageReplayStage::DestinationWriteReady => hook(RuntimeStage::DestinationWriteReady),
        PackageReplayStage::DestinationCommitStarted {
            plan_id,
            segment_count,
        } => hook(RuntimeStage::DestinationCommitStarted {
            plan_id,
            segment_count,
        }),
        PackageReplayStage::DestinationSegmentAcknowledged { ack } => {
            hook(RuntimeStage::DestinationSegmentAcknowledged { ack })
        }
        PackageReplayStage::DestinationReceiptRecorded { receipt } => {
            hook(RuntimeStage::DestinationReceiptRecorded { receipt })
        }
        PackageReplayStage::CheckpointCommitted { checkpoint } => {
            hook(RuntimeStage::CheckpointCommitted { checkpoint })
        }
        PackageReplayStage::PackageStatusUpdated { status } => {
            hook(RuntimeStage::PackageStatusUpdated { status })
        }
    }
}

fn notify_destination_replay_stage(
    hooks: &PackageReplayHooks<'_>,
    stage: PackageReplayStage<'_>,
) -> Result<()> {
    if let Some(hook) = hooks.stage {
        hook(stage)?;
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

fn validate_package_segments_match_delta(
    reader: &PackageReader,
    package_segments: &[SegmentEntry],
    state_delta: &StateDelta,
) -> Result<()> {
    let state_segments = &state_delta.segments;
    if state_segments.is_empty() {
        if !package_segments.is_empty() {
            return Err(CdfError::contract(
                "zero-segment StateDelta cannot cover package data segments",
            ));
        }
        let processed = reader.processed_observation_evidence()?.ok_or_else(|| {
            CdfError::contract(
                "zero-segment StateDelta requires typed processed-observation package evidence",
            )
        })?;
        processed.validate()?;
        if processed.input_position != state_delta.input_position
            || processed.output_position != state_delta.output_position
        {
            return Err(CdfError::contract(
                "processed-observation package evidence does not match StateDelta positions",
            ));
        }
        return Ok(());
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
