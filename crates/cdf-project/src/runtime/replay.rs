use super::{
    destinations::{
        DestinationPlanningContext, DuckDbProjectDestinationRuntime,
        ParquetProjectDestinationRuntime, PostgresProjectDestinationRuntime,
        ProjectDestinationRuntime, commit_request, validate_postgres_replay_target,
    },
    hooks::{
        LocalDuckDbLifecycleFailpoint, LocalDuckDbLifecycleFailpointHook, ReceiptVerifiedHook,
        RuntimeStage, RuntimeStageHook,
    },
    prelude::*,
    receipts::validate_destination_receipt_before_checkpoint,
    types::*,
};

type DestinationReplayStageHook<'a> = RuntimeStageHook<'a>;
pub(crate) type PackageReplayStageHook<'a> = &'a dyn Fn(PackageReplayStage<'_>) -> Result<()>;

#[derive(Clone, Copy, Debug)]
pub(crate) enum PackageReplayStage<'a> {
    PackageReplayVerified,
    CheckpointProposed { delta: &'a StateDelta },
    DestinationWriteReady,
    DestinationCommitStarted { plan_id: &'a PlanId },
    DestinationReceiptRecorded { receipt: &'a Receipt },
    CheckpointCommitted { checkpoint: &'a Checkpoint },
    PackageStatusUpdated { status: &'a PackageStatus },
}

#[derive(Default)]
pub(crate) struct PackageReplayHooks<'a> {
    pub(crate) after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    pub(crate) stage: Option<PackageReplayStageHook<'a>>,
    pub(crate) lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GenericPackageReplayReport {
    pub(crate) checkpoint: Checkpoint,
    pub(crate) receipt: Receipt,
    pub(crate) receipt_source: ProjectReceiptSource,
    pub(crate) package_status: PackageStatus,
}

impl GenericPackageReplayReport {
    fn into_duckdb(self) -> PreparedDuckDbReplayReport {
        PreparedDuckDbReplayReport {
            checkpoint: self.checkpoint,
            receipt: self.receipt,
            receipt_source: self.receipt_source.into_duckdb_receipt_source(),
            package_status: self.package_status,
        }
    }

    fn into_parquet(self) -> PreparedParquetReplayReport {
        PreparedParquetReplayReport {
            checkpoint: self.checkpoint,
            receipt: self.receipt,
            receipt_source: self.receipt_source,
            package_status: self.package_status,
        }
    }

    fn into_postgres(self) -> PreparedPostgresReplayReport {
        PreparedPostgresReplayReport {
            checkpoint: self.checkpoint,
            receipt: self.receipt,
            receipt_source: self.receipt_source,
            package_status: self.package_status,
        }
    }
}

struct DuckDbPackageReplayInputs {
    inputs: PackageReplayInputs,
}

impl DuckDbPackageReplayInputs {
    fn from_package_artifacts(inputs: PackageReplayInputs) -> Self {
        Self { inputs }
    }

    fn from_explicit(
        delta: StateDelta,
        target: TargetName,
        disposition: WriteDisposition,
        merge_keys: Vec<String>,
        schema_hash: SchemaHash,
    ) -> Result<Self> {
        Ok(Self {
            inputs: package_replay_inputs_from_explicit(
                delta,
                target,
                disposition,
                merge_keys,
                schema_hash,
            )?,
        })
    }
}

pub(super) struct ParquetPackageReplayInputs {
    inputs: PackageReplayInputs,
}

impl ParquetPackageReplayInputs {
    pub(super) fn from_package_artifacts(inputs: PackageReplayInputs) -> Self {
        Self { inputs }
    }
}

pub(super) struct PostgresPackageReplayInputs {
    inputs: PackageReplayInputs,
    target: PostgresTarget,
    dedup: MergeDedupPolicy,
    existing_table: Option<PostgresExistingTable>,
}

impl PostgresPackageReplayInputs {
    fn from_explicit_artifact_replay(
        _reader: &PackageReader,
        inputs: PackageReplayInputs,
        target: PostgresTarget,
        dedup: MergeDedupPolicy,
        existing_table: Option<PostgresExistingTable>,
    ) -> Result<Self> {
        validate_postgres_replay_target(&target, &inputs.destination_commit.target)?;
        Ok(Self {
            inputs,
            target,
            dedup,
            existing_table,
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

pub(super) fn replay_duckdb_package_from_artifacts_with_hooks<Store>(
    request: PackageArtifactDuckDbReplayRequest<'_, Store>,
    stage_hook: Option<DestinationReplayStageHook<'_>>,
    lifecycle_failpoint: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(&request.package_dir)?;
    let inputs = DuckDbPackageReplayInputs::from_package_artifacts(reader.replay_inputs()?);
    let runtime_stage_hook =
        |stage: PackageReplayStage<'_>| notify_runtime_replay_stage(stage_hook, stage);
    replay_duckdb_package_with_inputs(
        reader,
        request.package_dir,
        request.destination,
        request.checkpoint_store,
        inputs,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: Some(&runtime_stage_hook),
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
    let mut runtime = DuckDbProjectDestinationRuntime::new(request.destination);
    recover_package_with_runtime(
        reader,
        &mut runtime,
        request.checkpoint_store,
        inputs.inputs,
        request.receipt,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
            lifecycle_failpoint,
        },
    )
    .map(GenericPackageReplayReport::into_duckdb)
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
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
            lifecycle_failpoint,
        },
    )
}

fn replay_duckdb_package_with_inputs<Store>(
    reader: PackageReader,
    package_dir: PathBuf,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
    inputs: DuckDbPackageReplayInputs,
    hooks: PackageReplayHooks<'_>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let mut runtime = DuckDbProjectDestinationRuntime::new(destination);
    replay_package_with_runtime(
        reader,
        package_dir,
        &mut runtime,
        checkpoint_store,
        inputs.inputs,
        hooks,
    )
    .map(GenericPackageReplayReport::into_duckdb)
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
    let mut runtime = ParquetProjectDestinationRuntime::new(request.destination);
    recover_package_with_runtime(
        reader,
        &mut runtime,
        request.checkpoint_store,
        inputs.inputs,
        request.receipt,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
            lifecycle_failpoint: None,
        },
    )
    .map(GenericPackageReplayReport::into_parquet)
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
    let mut runtime = PostgresProjectDestinationRuntime::for_recovery(request.destination);
    recover_package_with_runtime(
        reader,
        &mut runtime,
        request.checkpoint_store,
        replay_inputs,
        request.receipt,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
            lifecycle_failpoint: None,
        },
    )
    .map(GenericPackageReplayReport::into_postgres)
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

pub(super) fn replay_parquet_package_with_inputs<Store>(
    reader: PackageReader,
    package_dir: PathBuf,
    destination: &ParquetDestination,
    checkpoint_store: &Store,
    inputs: ParquetPackageReplayInputs,
    hooks: ParquetReplayHooks<'_>,
) -> Result<PreparedParquetReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let mut runtime = ParquetProjectDestinationRuntime::new(destination);
    let runtime_stage_hook =
        |stage: PackageReplayStage<'_>| notify_runtime_replay_stage(hooks.stage, stage);
    replay_package_with_runtime(
        reader,
        package_dir,
        &mut runtime,
        checkpoint_store,
        inputs.inputs,
        PackageReplayHooks {
            after_receipt_verified: hooks.after_receipt_verified,
            stage: Some(&runtime_stage_hook),
            lifecycle_failpoint: None,
        },
    )
    .map(GenericPackageReplayReport::into_parquet)
}

pub(super) fn replay_postgres_package_with_inputs<Store>(
    reader: PackageReader,
    package_dir: PathBuf,
    destination: &PostgresDestination,
    checkpoint_store: &Store,
    inputs: PostgresPackageReplayInputs,
    hooks: PostgresReplayHooks<'_>,
) -> Result<PreparedPostgresReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let mut runtime = PostgresProjectDestinationRuntime::for_replay(
        destination,
        inputs.target,
        inputs.dedup,
        inputs.existing_table,
    );
    let runtime_stage_hook =
        |stage: PackageReplayStage<'_>| notify_runtime_replay_stage(hooks.stage, stage);
    replay_package_with_runtime(
        reader,
        package_dir,
        &mut runtime,
        checkpoint_store,
        inputs.inputs,
        PackageReplayHooks {
            after_receipt_verified: hooks.after_receipt_verified,
            stage: Some(&runtime_stage_hook),
            lifecycle_failpoint: None,
        },
    )
    .map(GenericPackageReplayReport::into_postgres)
}

pub(super) struct ParquetReplayHooks<'a> {
    pub(super) after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    pub(super) stage: Option<DestinationReplayStageHook<'a>>,
}

pub(super) struct PostgresReplayHooks<'a> {
    pub(super) after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    pub(super) stage: Option<DestinationReplayStageHook<'a>>,
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
    let mut runtime = DuckDbProjectDestinationRuntime::new(request.destination);
    recover_package_with_runtime(
        reader,
        &mut runtime,
        request.checkpoint_store,
        inputs.inputs,
        request.receipt,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
            lifecycle_failpoint,
        },
    )
    .map(GenericPackageReplayReport::into_duckdb)
}

pub(crate) fn replay_package_with_runtime<Store>(
    mut reader: PackageReader,
    package_dir: PathBuf,
    runtime: &mut dyn ProjectDestinationRuntime,
    checkpoint_store: &Store,
    inputs: PackageReplayInputs,
    hooks: PackageReplayHooks<'_>,
) -> Result<GenericPackageReplayReport>
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
        &DestinationPlanningContext {
            after_receipt_verified: hooks.after_receipt_verified,
        },
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
        },
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }

    let receipt = match commit_prepared_package_through_session(runtime, &reader, &prepared) {
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

    Ok(GenericPackageReplayReport {
        checkpoint,
        receipt,
        receipt_source: receipt_policy.into_project_receipt_source(package_receipt_recorded),
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
) -> Result<GenericPackageReplayReport>
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

    Ok(GenericPackageReplayReport {
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
) -> Result<Receipt> {
    let mut session = runtime
        .protocol()
        .begin(prepared.commit.clone(), prepared.plan.clone())?;
    if let Err(error) = session.apply_migrations() {
        let _ = session.abort();
        return Err(error);
    }
    if let Err(error) =
        write_package_segments_to_session(session.as_mut(), reader, &prepared.commit)
    {
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
) -> Result<()> {
    reader.verify()?;
    for segment in reader.read_commit_segments(&commit.segments)? {
        session.write_segment(segment)?;
    }
    Ok(())
}

fn package_replay_inputs_from_explicit(
    delta: StateDelta,
    target: TargetName,
    disposition: WriteDisposition,
    merge_keys: Vec<String>,
    schema_hash: SchemaHash,
) -> Result<PackageReplayInputs> {
    let destination_commit = commit_request(&delta, target, disposition)?;
    Ok(PackageReplayInputs {
        input_checkpoint: None,
        state_delta: delta,
        destination_commit,
        merge_keys,
        schema_hash,
    })
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
    validate_package_segments_match_delta(&replay.segments, &inputs.state_delta.segments)?;
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
        PackageReplayStage::CheckpointProposed { delta } => {
            hook(RuntimeStage::CheckpointProposed { delta })
        }
        PackageReplayStage::DestinationCommitStarted { plan_id } => {
            hook(RuntimeStage::DestinationCommitStarted { plan_id })
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
        PackageReplayStage::PackageReplayVerified | PackageReplayStage::DestinationWriteReady => {
            Ok(())
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
    trigger_lifecycle_failpoint_for_stage(hooks.lifecycle_failpoint, stage)
}

fn trigger_lifecycle_failpoint_for_stage(
    hook: Option<LocalDuckDbLifecycleFailpointHook<'_>>,
    stage: PackageReplayStage<'_>,
) -> Result<()> {
    let Some(hook) = hook else {
        return Ok(());
    };
    match stage {
        PackageReplayStage::PackageReplayVerified => hook(
            LocalDuckDbLifecycleFailpoint::AfterPackagedBeforeDestinationWrite,
            None,
        ),
        PackageReplayStage::DestinationWriteReady => hook(
            LocalDuckDbLifecycleFailpoint::AfterCheckpointProposalBeforeDestinationWrite,
            None,
        ),
        PackageReplayStage::DestinationReceiptRecorded { receipt } => hook(
            LocalDuckDbLifecycleFailpoint::AfterReceiptVerifiedBeforeCheckpointCommit,
            Some(receipt),
        ),
        PackageReplayStage::CheckpointCommitted { checkpoint } => hook(
            LocalDuckDbLifecycleFailpoint::AfterCheckpointCommitBeforePackageStatusCheckpointed,
            checkpoint.receipt.as_ref(),
        ),
        PackageReplayStage::CheckpointProposed { .. }
        | PackageReplayStage::DestinationCommitStarted { .. }
        | PackageReplayStage::PackageStatusUpdated { .. } => Ok(()),
    }
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
