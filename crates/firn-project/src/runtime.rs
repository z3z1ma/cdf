use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use firn_declarative::{CompiledResource, CompiledResourcePlan};
use firn_dest_duckdb::{DuckDbCommitRequest, DuckDbDestination};
#[cfg(test)]
use firn_engine::EngineRunOutputWithSegmentPositions;
use firn_engine::{
    EnginePackageDraft, EnginePlan, execute_to_package_with_segment_positions_and_pre_finalize,
};
use firn_kernel::{
    CHECKPOINT_STATE_VERSION, Checkpoint, CheckpointId, CheckpointStore, DestinationCommitRequest,
    FirnError, IdempotencyToken, PackageHash, PipelineId, Receipt, Result, SchemaHash,
    SchemaSource, ScopeKey, SegmentId, SourcePosition, StateDelta, StateSegment, TargetName,
    WriteDisposition,
};
use firn_package::{
    DestinationCommitPlanPreimage, PackageReader, PackageReplayInputs, PackageStatus, ReplayView,
    SegmentEntry, StateDeltaPreimage,
};
use firn_state_sqlite::SqliteCheckpointStore;

pub type ReceiptVerifiedHook<'a> = &'a dyn Fn(&Receipt) -> Result<()>;

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

pub async fn run_local_file_to_duckdb_checkpoint(
    request: LocalFileDuckDbRunRequest<'_>,
) -> Result<LocalFileDuckDbRunReport> {
    validate_local_file_run_resource(request.resource)?;
    validate_run_plan(&request)?;
    validate_explicit_package_id(&request.package_id)?;

    let schema_hash = declared_schema_hash(request.resource)?;
    let package_dir = request.package_root.join(&request.package_id);
    refuse_existing_package_dir(&package_dir)?;
    ensure_parent_directory(&request.state_store_path)?;
    ensure_parent_directory(&request.destination_path)?;
    let checkpoint_store = SqliteCheckpointStore::open(&request.state_store_path)?;
    let destination = DuckDbDestination::new(&request.destination_path)?;
    let scope = request.resource.descriptor().state_scope.clone();
    let head = checkpoint_store.head(
        &request.pipeline_id,
        &request.resource.descriptor().resource_id,
        &scope,
    )?;

    let write_state_commit_artifacts =
        |builder: &firn_package::PackageBuilder, draft: EnginePackageDraft<'_>| {
            write_run_state_commit_artifacts(builder, draft, &request, &schema_hash, &scope, &head)
        };
    let output = execute_to_package_with_segment_positions_and_pre_finalize(
        &request.plan,
        request.resource,
        &package_dir,
        &write_state_commit_artifacts,
    )
    .await?;

    let replay_inputs = PackageReader::open(&package_dir)?.replay_inputs()?;
    let package_hash = replay_inputs.state_delta.package_hash.clone();
    let row_count = output.output.profile.output_rows;
    let segment_count = output.output.segments.len();

    let report = replay_duckdb_package_from_artifacts(PackageArtifactDuckDbReplayRequest {
        package_dir: package_dir.clone(),
        destination: &destination,
        checkpoint_store: &checkpoint_store,
        after_receipt_verified: request.after_receipt_verified,
    })?;

    Ok(LocalFileDuckDbRunReport {
        package_dir,
        package_id: request.package_id,
        package_hash,
        package_status: report.package_status,
        checkpoint: report.checkpoint,
        receipt: report.receipt,
        receipt_source: report.receipt_source,
        row_count,
        segment_count,
    })
}

fn write_run_state_commit_artifacts(
    builder: &firn_package::PackageBuilder,
    draft: EnginePackageDraft<'_>,
    request: &LocalFileDuckDbRunRequest<'_>,
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: &Option<Checkpoint>,
) -> Result<()> {
    let state_delta = state_delta_preimage_from_run_draft(
        request,
        draft.segments,
        draft.segment_positions,
        schema_hash,
        scope,
        head.as_ref(),
    )?;
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        request.target.clone(),
        request.resource.descriptor().write_disposition.clone(),
        request.resource.descriptor().merge_key.clone(),
        schema_hash.clone(),
        state_delta.segments.clone(),
    );
    builder.write_input_checkpoint_artifact(head)?;
    builder.write_state_delta_preimage_artifact(&state_delta)?;
    builder.write_commit_plan_preimage_artifact(&commit_plan)?;
    Ok(())
}

fn validate_local_file_run_resource(resource: &CompiledResource) -> Result<()> {
    match resource.plan() {
        CompiledResourcePlan::Files(_) => Ok(()),
        CompiledResourcePlan::Rest(_) => Err(FirnError::contract(
            "firn run supports only declarative local file resources in this slice; REST execution is excluded",
        )),
        CompiledResourcePlan::Sql(_) => Err(FirnError::contract(
            "firn run supports only declarative local file resources in this slice; SQL execution is excluded",
        )),
    }
}

fn validate_run_plan(request: &LocalFileDuckDbRunRequest<'_>) -> Result<()> {
    let descriptor = request.resource.descriptor();
    if request.plan.scan.request.resource_id != descriptor.resource_id {
        return Err(FirnError::contract(format!(
            "run plan resource {} does not match selected resource {}",
            request.plan.scan.request.resource_id, descriptor.resource_id
        )));
    }
    if request.plan.package_id != request.package_id {
        return Err(FirnError::contract(format!(
            "run plan package id {} does not match explicit package id {}",
            request.plan.package_id, request.package_id
        )));
    }
    if request.plan.scan.request.scope != descriptor.state_scope {
        return Err(FirnError::contract(
            "run plan scope must come from the current resource descriptor state scope",
        ));
    }
    Ok(())
}

fn declared_schema_hash(resource: &CompiledResource) -> Result<SchemaHash> {
    match &resource.descriptor().schema_source {
        SchemaSource::Declared { schema_hash, .. } => Ok(schema_hash.clone()),
        SchemaSource::Discovered { schema_hash: None } => Err(FirnError::contract(
            "firn run requires a declared schema with a concrete schema hash; discovered schema resources are unsupported in this slice",
        )),
        SchemaSource::Discovered {
            schema_hash: Some(_),
        } => Err(FirnError::contract(
            "firn run requires SchemaSource::Declared; discovered schema hashes are unsupported in this slice",
        )),
        SchemaSource::Contract { .. } => Err(FirnError::contract(
            "firn run requires SchemaSource::Declared; contract-sourced schemas are unsupported in this slice",
        )),
    }
}

fn refuse_existing_package_dir(package_dir: &Path) -> Result<()> {
    if package_dir.exists() {
        return Err(FirnError::data(format!(
            "package directory already exists at {}; explicit run package ids must not overwrite existing packages",
            package_dir.display()
        )));
    }
    Ok(())
}

fn ensure_parent_directory(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            FirnError::internal(format!("create {}: {error}", parent.display()))
        })?;
    }
    Ok(())
}

fn validate_explicit_package_id(package_id: &str) -> Result<()> {
    if package_id.trim().is_empty() {
        return Err(FirnError::contract("run package id cannot be empty"));
    }
    let mut components = Path::new(package_id).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(FirnError::contract(
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
    let preimage = state_delta_preimage_from_run_draft(
        request,
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

fn state_delta_preimage_from_run_draft(
    request: &LocalFileDuckDbRunRequest<'_>,
    segments: &[SegmentEntry],
    segment_positions: &[firn_engine::EngineSegmentPosition],
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: Option<&Checkpoint>,
) -> Result<StateDeltaPreimage> {
    let positions = segment_positions_by_id(segments, segment_positions)?;
    let mut state_segments = Vec::with_capacity(segments.len());
    let mut output_position = None;

    for segment in segments {
        let segment_position = positions
            .get(&segment.segment_id)
            .ok_or_else(|| {
                FirnError::internal(format!(
                    "engine output omitted source position evidence for segment {}",
                    segment.segment_id
                ))
            })?
            .clone()
            .ok_or_else(|| {
                FirnError::data(format!(
                    "package segment {} has no source position evidence; local file run cannot checkpoint without a FileManifest position",
                    segment.segment_id
                ))
            })?;
        if !matches!(segment_position, SourcePosition::FileManifest(_)) {
            return Err(FirnError::data(format!(
                "package segment {} recorded a non-file source position; local file run requires FileManifest checkpoint evidence",
                segment.segment_id
            )));
        }
        let segment_position = normalize_file_manifest_position_for_scope(segment_position, scope);
        if let Some(existing) = &output_position {
            if existing != &segment_position {
                return Err(FirnError::data(
                    "single local file run produced divergent segment source positions",
                ));
            }
        } else {
            output_position = Some(segment_position.clone());
        }
        state_segments.push(StateSegment {
            segment_id: segment.segment_id.clone(),
            scope: scope.clone(),
            output_position: segment_position,
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        });
    }

    let output_position = output_position.ok_or_else(|| {
        FirnError::data("package execution produced no output segments to checkpoint")
    })?;
    Ok(StateDeltaPreimage {
        checkpoint_id: request.checkpoint_id.clone(),
        pipeline_id: request.pipeline_id.clone(),
        resource_id: request.resource.descriptor().resource_id.clone(),
        scope: scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: head.map(|checkpoint| checkpoint.delta.checkpoint_id.clone()),
        input_position: head.map(|checkpoint| checkpoint.delta.output_position.clone()),
        output_position,
        schema_hash: schema_hash.clone(),
        segments: state_segments,
    })
}

fn normalize_file_manifest_position_for_scope(
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
    segment_positions: &[firn_engine::EngineSegmentPosition],
) -> Result<BTreeMap<SegmentId, Option<SourcePosition>>> {
    if segment_positions.len() != segments.len() {
        return Err(FirnError::internal(format!(
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
        return Err(FirnError::internal(
            "engine output contains duplicate segment source position records",
        ));
    }
    Ok(positions)
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

pub fn replay_duckdb_package_from_artifacts<Store>(
    request: PackageArtifactDuckDbReplayRequest<'_, Store>,
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
        request.after_receipt_verified,
    )
}

pub fn recover_duckdb_package_from_artifacts<Store>(
    request: PackageArtifactDuckDbRecoveryRequest<'_, Store>,
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
    )
}

pub fn replay_prepared_duckdb_package<Store>(
    request: PreparedDuckDbReplayRequest<'_, Store>,
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
        request.after_receipt_verified,
    )
}

fn replay_duckdb_package_with_inputs<Store>(
    mut reader: PackageReader,
    package_dir: PathBuf,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
    inputs: DuckDbPackageReplayInputs,
    after_receipt_verified: Option<ReceiptVerifiedHook<'_>>,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let checkpoint_id = inputs.delta.checkpoint_id.clone();
    checkpoint_store.propose(inputs.delta.clone())?;
    if let Err(error) = reader.update_status(PackageStatus::Loading) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }

    let outcome = match destination.commit_package(DuckDbCommitRequest {
        package_dir,
        commit: inputs.commit.clone(),
        schema_hash: inputs.schema_hash.clone(),
        merge_keys: inputs.merge_keys.clone(),
    }) {
        Ok(outcome) => outcome,
        Err(error) => {
            let _ = checkpoint_store.abandon(&checkpoint_id);
            return Err(error);
        }
    };

    let receipt = outcome.receipt;
    verify_receipt_before_checkpoint(
        destination,
        &inputs.delta,
        &inputs.target,
        &inputs.disposition,
        &receipt,
    )?;
    if let Some(hook) = after_receipt_verified {
        hook(&receipt)?;
    }

    let checkpoint = checkpoint_store.commit(&inputs.delta.checkpoint_id, receipt.clone())?;
    let package_status = reader
        .update_status(PackageStatus::Checkpointed)?
        .lifecycle
        .status
        .clone();

    Ok(PreparedDuckDbReplayReport {
        checkpoint,
        receipt,
        receipt_source: PreparedReceiptSource::DuckDbCommit {
            duplicate: outcome.duplicate,
            package_receipt_recorded: outcome.package_receipt_recorded,
        },
        package_status,
    })
}

pub fn recover_prepared_duckdb_package<Store>(
    request: PreparedDuckDbRecoveryRequest<'_, Store>,
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
    )
}

fn recover_duckdb_package_with_inputs<Store>(
    mut reader: PackageReader,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
    inputs: DuckDbPackageReplayInputs,
    receipt: Receipt,
    after_receipt_verified: Option<ReceiptVerifiedHook<'_>>,
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
    if let Some(hook) = after_receipt_verified {
        hook(&receipt)?;
    }

    let checkpoint = checkpoint_store.commit(&inputs.delta.checkpoint_id, receipt.clone())?;
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

fn validate_prepared_package(
    reader: &PackageReader,
    delta: &StateDelta,
    schema_hash: &SchemaHash,
) -> Result<ReplayView> {
    reader.verify()?;
    let replay = reader.replay_view()?;
    if replay.package_hash != delta.package_hash {
        return Err(FirnError::data(format!(
            "package hash {} does not match StateDelta package hash {}",
            replay.package_hash, delta.package_hash
        )));
    }
    if schema_hash != &delta.schema_hash {
        return Err(FirnError::contract(format!(
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
        return Err(FirnError::contract(
            "StateDelta must include at least one state segment for package replay",
        ));
    }
    if package_segments.len() != state_segments.len() {
        return Err(FirnError::data(format!(
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
        return Err(FirnError::data(
            "package manifest contains duplicate segment ids",
        ));
    }

    let mut seen_state_segments = BTreeSet::<&SegmentId>::new();
    for segment in state_segments {
        if !seen_state_segments.insert(&segment.segment_id) {
            return Err(FirnError::contract(format!(
                "StateDelta contains duplicate segment {}",
                segment.segment_id
            )));
        }
        let Some(package_segment) = package_by_id.get(&segment.segment_id) else {
            return Err(FirnError::data(format!(
                "StateDelta segment {} is not present in the package manifest",
                segment.segment_id
            )));
        };
        if package_segment.row_count != segment.row_count
            || package_segment.byte_count != segment.byte_count
        {
            return Err(FirnError::data(format!(
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
    let verification = destination.verify_receipt(receipt)?;
    if !verification.verified {
        return Err(FirnError::destination(format!(
            "DuckDB receipt {} did not verify: {}",
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
        return Err(FirnError::contract(format!(
            "receipt {} package hash {} does not match StateDelta package hash {}",
            receipt.receipt_id, receipt.package_hash, delta.package_hash
        )));
    }
    if receipt.schema_hash != delta.schema_hash {
        return Err(FirnError::contract(format!(
            "receipt {} schema hash {} does not match StateDelta schema hash {}",
            receipt.receipt_id, receipt.schema_hash, delta.schema_hash
        )));
    }
    if &receipt.target != target {
        return Err(FirnError::contract(format!(
            "receipt {} target {} does not match explicit target {}",
            receipt.receipt_id, receipt.target, target
        )));
    }
    if &receipt.disposition != disposition {
        return Err(FirnError::contract(format!(
            "receipt {} disposition {:?} does not match explicit disposition {:?}",
            receipt.receipt_id, receipt.disposition, disposition
        )));
    }
    if receipt.idempotency_token.as_str() != delta.package_hash.as_str() {
        return Err(FirnError::contract(format!(
            "receipt {} idempotency token {} does not match package hash {}",
            receipt.receipt_id, receipt.idempotency_token, delta.package_hash
        )));
    }
    validate_segment_acks(delta, receipt)
}

fn validate_segment_acks(delta: &StateDelta, receipt: &Receipt) -> Result<()> {
    if receipt.segment_acks.len() != delta.segments.len() {
        return Err(FirnError::contract(format!(
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
        return Err(FirnError::contract(format!(
            "receipt {} contains duplicate segment acknowledgements",
            receipt.receipt_id
        )));
    }

    for segment in &delta.segments {
        let Some(ack) = acks.get(&segment.segment_id) else {
            return Err(FirnError::contract(format!(
                "receipt {} does not acknowledge segment {}",
                receipt.receipt_id, segment.segment_id
            )));
        };
        if ack.row_count != segment.row_count || ack.byte_count != segment.byte_count {
            return Err(FirnError::contract(format!(
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
