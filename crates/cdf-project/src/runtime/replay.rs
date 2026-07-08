use super::{
    destinations::{
        commit_request, postgres_columns_from_package, postgres_load_plan_input,
        postgres_load_plan_input_from_artifacts,
    },
    hooks::{
        LocalDuckDbLifecycleFailpoint, LocalDuckDbLifecycleFailpointHook, ReceiptVerifiedHook,
    },
    prelude::*,
    receipts::{
        verify_parquet_receipt_before_checkpoint, verify_postgres_receipt_before_checkpoint,
        verify_receipt_before_checkpoint,
    },
    types::*,
};

type DestinationReplayStageHook<'a> = super::hooks::RuntimeStageHook<'a>;
type DestinationReplayStage<'a> = super::hooks::RuntimeStage<'a>;

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

pub(super) struct ParquetPackageReplayInputs {
    delta: StateDelta,
    target: TargetName,
    disposition: WriteDisposition,
    schema_hash: SchemaHash,
    commit: DestinationCommitRequest,
}

impl ParquetPackageReplayInputs {
    pub(super) fn from_package_artifacts(inputs: PackageReplayInputs) -> Self {
        Self {
            target: inputs.destination_commit.target.clone(),
            disposition: inputs.destination_commit.disposition.clone(),
            schema_hash: inputs.schema_hash,
            commit: inputs.destination_commit,
            delta: inputs.state_delta,
        }
    }
}

pub(super) struct PostgresPackageReplayInputs {
    delta: StateDelta,
    target: TargetName,
    disposition: WriteDisposition,
    load_plan: Option<PostgresLoadPlan>,
    commit: DestinationCommitRequest,
}

impl PostgresPackageReplayInputs {
    pub(super) fn from_package_artifacts(
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

pub(super) fn replay_parquet_package_with_inputs<Store>(
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

pub(super) fn replay_postgres_package_with_inputs<Store>(
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

pub(super) struct ParquetReplayHooks<'a> {
    pub(super) after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    pub(super) stage: Option<DestinationReplayStageHook<'a>>,
}

pub(super) struct PostgresReplayHooks<'a> {
    pub(super) after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    pub(super) stage: Option<DestinationReplayStageHook<'a>>,
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
