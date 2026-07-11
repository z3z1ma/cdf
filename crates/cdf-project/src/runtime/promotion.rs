use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{
    ContractPolicy, ObservedSchema, ResidualFieldRef, compile_resource_validation_program,
    decode_residual_json_v1, encode_residual_json_v1, is_framework_variant_field,
};
use cdf_declarative::CompiledResource;
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CanonicalArrowField, Checkpoint, CheckpointId, CheckpointStatus,
    CheckpointStore, CompositePosition, ContractRef, CorrectionStrategy,
    DestinationCorrectionCommitRequest, DestinationCorrectionOperation, DestinationCorrectionPlan,
    DestinationId, IdempotencyToken, LeaseOwnerId, PROMOTION_PUBLICATION_EVENT_VERSION,
    PackageHash, PipelineId, PromotionId, PromotionPublicationEvent, PromotionPublicationTarget,
    PromotionSettlementStore, Receipt, ResourceId, SchemaHash, ScopeKey, ScopeLease,
    SourcePosition, StateDelta, StateSegment, TargetName,
};
use cdf_package::{
    DestinationCommitPlanPreimage, MANIFEST_FILE, PackageBuilder, PackageReader, PackageStatus,
    StateDeltaPreimage,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::destinations::ResolvedProjectDestination;
use crate::{
    CdfLock, LOCK_FILE_NAME, LocalPackagePromotionEvidenceInventory, LockFileAuthority,
    PromotionEvidenceInventory, SchemaPromotionEvidenceAvailability, SchemaPromotionPlanReport,
    SchemaPromotionTargetReport, SchemaSnapshotStore, compare_and_swap_lock_file, lock_to_toml,
    parse_lock, read_lock_file_authority, validate_schema_promotion_plan_identity,
};

pub const SCHEMA_PROMOTION_EXECUTION_ARTIFACT_VERSION: u16 = 1;
pub const SCHEMA_PROMOTION_CORRECTION_PACKAGE_VERSION: u16 = 1;
pub const SCHEMA_PROMOTION_CORRECTION_TARGET_AUTHORITY_VERSION: u16 = 1;
pub const SCHEMA_PROMOTION_RECOVERY_STATUS_VERSION: u16 = 1;
pub const DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS: u64 = 300_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaPromotionExecutionFailpoint {
    AfterStagedArtifacts,
    AfterCorrectionPackages,
    AfterDestinationReceipt,
    AfterTargetCheckpoint,
    AfterTargetCheckpointIndex(usize),
    AfterLockPublication,
    AfterPublicationEvent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaPromotionExecutionPhase {
    Staged,
    Packaged,
    DestinationSettled,
    Checkpointed,
    LockPublished,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionExecutionTargetReport {
    pub destination: String,
    pub target: String,
    pub correction_package_hash: String,
    pub receipt_id: Option<String>,
    pub checkpoint_id: Option<String>,
    pub committed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionExecutionReport {
    pub resource_id: String,
    pub promotion_id: String,
    pub phase: SchemaPromotionExecutionPhase,
    pub resumed: bool,
    pub old_schema_hash: String,
    pub new_schema_hash: String,
    pub staged_plan_path: String,
    pub snapshot_path: String,
    pub targets: Vec<SchemaPromotionExecutionTargetReport>,
    pub lock_published: bool,
    pub publication_event_recorded: bool,
    pub remaining_action: String,
    pub recovery_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionRecoveryStatus {
    pub version: u16,
    pub resource_id: String,
    pub promotion_id: String,
    pub phase: SchemaPromotionExecutionPhase,
    pub targets: Vec<SchemaPromotionExecutionTargetReport>,
    pub remaining_action: String,
    pub recovery_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionExecutionPlanArtifact {
    pub version: u16,
    pub promotion_id: PromotionId,
    pub resource_id: ResourceId,
    pub old_lock_authority: LockFileAuthority,
    pub dry_plan: SchemaPromotionPlanReport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionCorrectionPackageArtifact {
    pub version: u16,
    pub promotion_id: PromotionId,
    pub resource_id: ResourceId,
    pub destination_id: DestinationId,
    pub target: TargetName,
    pub old_schema_hash: SchemaHash,
    pub new_schema_hash: SchemaHash,
    pub strategy: CorrectionStrategy,
    pub disposition: cdf_kernel::WriteDisposition,
    pub source_packages: Vec<PackageHash>,
    pub validation_program: cdf_contract::ValidationProgram,
    pub operations: Vec<DestinationCorrectionOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaPromotionCorrectionPathAuthority {
    path: String,
    observed_count: u64,
    affected_address_value_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaPromotionCorrectionTargetAuthority {
    version: u16,
    promotion_id: PromotionId,
    resource_id: ResourceId,
    destination_id: DestinationId,
    target: TargetName,
    correction_package_hash: PackageHash,
    operation_count: u64,
    operation_digest: String,
    checkpoint_id: CheckpointId,
    input_checkpoint: Option<Checkpoint>,
    paths: Vec<SchemaPromotionCorrectionPathAuthority>,
}

pub struct SchemaPromotionExecutionRequest<'a, Store>
where
    Store: PromotionSettlementStore,
{
    pub project_root: &'a Path,
    pub package_root: &'a Path,
    pub resource: &'a CompiledResource,
    pub lock: &'a CdfLock,
    pub lock_authority: &'a LockFileAuthority,
    pub dry_plan: &'a SchemaPromotionPlanReport,
    pub destinations: Vec<ResolvedProjectDestination>,
    pub pipeline_id: PipelineId,
    pub lease_owner: LeaseOwnerId,
    pub lease_duration_ms: u64,
    pub settlement_store: &'a Store,
    pub failpoint: Option<SchemaPromotionExecutionFailpoint>,
}

pub fn execute_schema_promotion<Store>(
    mut request: SchemaPromotionExecutionRequest<'_, Store>,
) -> cdf_kernel::Result<SchemaPromotionExecutionReport>
where
    Store: PromotionSettlementStore,
{
    validate_execution_request(&request)?;
    let scope = promotion_scope(request.resource);
    let lease = request.settlement_store.acquire(
        scope,
        request.lease_owner.clone(),
        request.lease_duration_ms,
    )?;
    let result = execute_under_lease(&mut request, &lease);
    let release = request.settlement_store.release(&lease);
    match (result, release) {
        (Ok(report), Ok(())) => Ok(report),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), _) => Err(error),
    }
}

fn execute_under_lease<Store>(
    request: &mut SchemaPromotionExecutionRequest<'_, Store>,
    lease: &ScopeLease,
) -> cdf_kernel::Result<SchemaPromotionExecutionReport>
where
    Store: PromotionSettlementStore,
{
    request.settlement_store.assert_current(lease)?;
    let staged_path = request
        .project_root
        .join(promotion_plan_relative_path(&PromotionId::new(
            request.dry_plan.promotion_id.clone(),
        )?));
    let resumed = verify_input_authority(request)? || staged_path.exists();
    let staged = stage_execution_artifacts(request)?;
    write_recovery_status(
        request.project_root,
        &staged,
        0,
        SchemaPromotionExecutionPhase::Staged,
        Vec::new(),
        "build authenticated correction packages",
    )?;
    fail_if(
        request.failpoint,
        SchemaPromotionExecutionFailpoint::AfterStagedArtifacts,
    )?;

    let packages = build_or_load_correction_packages(request, &staged)?;
    let packaged_targets = packages
        .iter()
        .map(pending_target_report)
        .collect::<Vec<_>>();
    write_recovery_status(
        request.project_root,
        &staged,
        100,
        SchemaPromotionExecutionPhase::Packaged,
        packaged_targets,
        "settle remaining destination targets",
    )?;
    fail_if(
        request.failpoint,
        SchemaPromotionExecutionFailpoint::AfterCorrectionPackages,
    )?;

    if let Some(publication) = request
        .settlement_store
        .promotion_publication(&staged.promotion_id)?
    {
        let installed_lock = read_lock_file_authority(request.project_root.join(LOCK_FILE_NAME))?;
        let mut verified_targets = Vec::new();
        for package in &packages {
            let destination = take_destination(
                &mut request.destinations,
                &package.artifact.destination_id,
                &package.artifact.target,
            )?;
            let receipt = verify_stored_correction_receipt(destination, package)?;
            verified_targets.push(committed_target_report(
                request.settlement_store,
                package,
                &receipt,
            )?);
        }
        let expected_targets = publication_targets(&verified_targets)?;
        verify_publication_authority(&publication, &staged, &installed_lock, &expected_targets)?;
        write_recovery_status(
            request.project_root,
            &staged,
            900,
            SchemaPromotionExecutionPhase::Complete,
            verified_targets,
            "none",
        )?;
        return Ok(report_from_publication(&staged, &publication, true));
    }

    let lock_already_published = lock_has_staged_snapshot(request.project_root, &staged)?;
    let mut targets = Vec::new();
    if lock_already_published {
        for package in &packages {
            let destination = take_destination(
                &mut request.destinations,
                &package.artifact.destination_id,
                &package.artifact.target,
            )?;
            let receipt = verify_stored_correction_receipt(destination, package)?;
            targets.push(committed_target_report(
                request.settlement_store,
                package,
                &receipt,
            )?);
        }
    } else {
        for (target_index, package) in packages.into_iter().enumerate() {
            request.settlement_store.assert_current(lease)?;
            let destination = take_destination(
                &mut request.destinations,
                &package.artifact.destination_id,
                &package.artifact.target,
            )?;
            let receipt = settle_correction_package(destination, &package)?;
            let mut destination_settled = targets.clone();
            destination_settled.push(SchemaPromotionExecutionTargetReport {
                destination: package.artifact.destination_id.to_string(),
                target: package.artifact.target.to_string(),
                correction_package_hash: package.package_hash.to_string(),
                receipt_id: Some(receipt.receipt_id.to_string()),
                checkpoint_id: None,
                committed: false,
            });
            write_recovery_status(
                request.project_root,
                &staged,
                200 + (target_index as u64 * 2),
                SchemaPromotionExecutionPhase::DestinationSettled,
                destination_settled,
                "commit the settled target checkpoint",
            )?;
            fail_if(
                request.failpoint,
                SchemaPromotionExecutionFailpoint::AfterDestinationReceipt,
            )?;
            let checkpoint = settle_promotion_checkpoint(
                request.settlement_store,
                lease,
                &package,
                receipt.clone(),
            )?;
            targets.push(SchemaPromotionExecutionTargetReport {
                destination: package.artifact.destination_id.to_string(),
                target: package.artifact.target.to_string(),
                correction_package_hash: package.package_hash.to_string(),
                receipt_id: Some(receipt.receipt_id.to_string()),
                checkpoint_id: Some(checkpoint.delta.checkpoint_id.to_string()),
                committed: checkpoint.status == CheckpointStatus::Committed,
            });
            let remaining_action = if targets.len() == staged.dry_plan.targets.len() {
                "publish the pinned schema lock"
            } else {
                "settle the next destination target"
            };
            write_recovery_status(
                request.project_root,
                &staged,
                201 + (target_index as u64 * 2),
                SchemaPromotionExecutionPhase::Checkpointed,
                targets.clone(),
                remaining_action,
            )?;
            fail_if(
                request.failpoint,
                SchemaPromotionExecutionFailpoint::AfterTargetCheckpoint,
            )?;
            fail_if(
                request.failpoint,
                SchemaPromotionExecutionFailpoint::AfterTargetCheckpointIndex(target_index),
            )?;
        }
    }

    request.settlement_store.assert_current(lease)?;
    let installed_lock = publish_lock(request, lease, &staged)?;
    write_recovery_status(
        request.project_root,
        &staged,
        800,
        SchemaPromotionExecutionPhase::LockPublished,
        targets.clone(),
        "record the exact promotion publication event",
    )?;
    fail_if(
        request.failpoint,
        SchemaPromotionExecutionFailpoint::AfterLockPublication,
    )?;
    let publication = publish_event(request, lease, &installed_lock, &targets)?;
    write_recovery_status(
        request.project_root,
        &staged,
        900,
        SchemaPromotionExecutionPhase::Complete,
        targets.clone(),
        "none",
    )?;
    fail_if(
        request.failpoint,
        SchemaPromotionExecutionFailpoint::AfterPublicationEvent,
    )?;

    Ok(SchemaPromotionExecutionReport {
        resource_id: staged.resource_id.to_string(),
        promotion_id: staged.promotion_id.to_string(),
        phase: SchemaPromotionExecutionPhase::Complete,
        resumed,
        old_schema_hash: staged.dry_plan.old_schema_hash.clone(),
        new_schema_hash: staged
            .dry_plan
            .new_schema_hash
            .clone()
            .expect("validated executable plan has a new schema hash"),
        staged_plan_path: promotion_plan_relative_path(&staged.promotion_id),
        snapshot_path: staged
            .dry_plan
            .new_schema_snapshot_path
            .clone()
            .expect("validated executable plan has a snapshot path"),
        targets,
        lock_published: true,
        publication_event_recorded: publication.promotion_id == staged.promotion_id,
        remaining_action: "none".to_owned(),
        recovery_command: execution_recovery_command(&staged.dry_plan),
    })
}

fn validate_execution_request<Store>(
    request: &SchemaPromotionExecutionRequest<'_, Store>,
) -> cdf_kernel::Result<()>
where
    Store: PromotionSettlementStore,
{
    if !request.dry_plan.executable || !request.dry_plan.conflicts.is_empty() {
        return Err(cdf_kernel::CdfError::contract(
            "schema promotion execution requires an executable conflict-free dry plan",
        ));
    }
    if request.dry_plan.resource_id != request.resource.descriptor().resource_id.as_str()
        || request.dry_plan.lock_precondition_sha256 != request.lock_authority.sha256
    {
        return Err(cdf_kernel::CdfError::contract(
            "schema promotion dry plan does not match resource and lock authority",
        ));
    }
    let authoritative_lock = parse_lock_authority(&request.lock_authority.bytes)?;
    if &authoritative_lock != request.lock {
        return Err(cdf_kernel::CdfError::contract(
            "schema promotion caller lock projection does not equal the exact old lock bytes",
        ));
    }
    validate_schema_promotion_plan_identity(request.dry_plan, request.lock_authority)?;
    let snapshot =
        request.dry_plan.proposed_snapshot.as_ref().ok_or_else(|| {
            cdf_kernel::CdfError::contract("executable promotion has no snapshot")
        })?;
    snapshot.artifact.validate_hash_input()?;
    if snapshot.artifact.promotion_authority.is_none() {
        return Err(cdf_kernel::CdfError::contract(
            "schema promotion execution requires typed version-3 snapshot authority",
        ));
    }
    if request.lease_duration_ms == 0 {
        return Err(cdf_kernel::CdfError::contract(
            "schema promotion lease duration must be positive",
        ));
    }
    Ok(())
}

fn parse_lock_authority(bytes: &[u8]) -> cdf_kernel::Result<CdfLock> {
    parse_lock(
        std::str::from_utf8(bytes)
            .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?,
    )
}

fn verify_input_authority<Store>(
    request: &SchemaPromotionExecutionRequest<'_, Store>,
) -> cdf_kernel::Result<bool>
where
    Store: PromotionSettlementStore,
{
    let lock_path = request.project_root.join(LOCK_FILE_NAME);
    let current = read_lock_file_authority(&lock_path)?;
    if current != *request.lock_authority {
        let current_lock = parse_lock(
            std::str::from_utf8(&current.bytes)
                .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?,
        )?;
        let proposed = request
            .dry_plan
            .proposed_snapshot
            .as_ref()
            .ok_or_else(|| cdf_kernel::CdfError::contract("promotion snapshot is missing"))?;
        let installed = current_lock
            .resources
            .get(&request.dry_plan.resource_id)
            .and_then(|resource| resource.schema_snapshot.as_ref());
        if installed == Some(&proposed.artifact.reference()) {
            return Ok(true);
        }
        return Err(cdf_kernel::CdfError::contract(format!(
            "concurrent schema authority conflict: promotion planned lock {}, current lock {}",
            request.lock_authority.sha256, current.sha256
        )));
    }
    let locked = request
        .lock
        .resources
        .get(&request.dry_plan.resource_id)
        .and_then(|resource| resource.schema_snapshot.as_ref())
        .ok_or_else(|| cdf_kernel::CdfError::contract("current lock has no pinned snapshot"))?;
    if locked.schema_hash.to_string() != request.dry_plan.old_schema_hash {
        return Err(cdf_kernel::CdfError::contract(
            "schema promotion old snapshot no longer matches cdf.lock",
        ));
    }
    Ok(false)
}

fn stage_execution_artifacts<Store>(
    request: &SchemaPromotionExecutionRequest<'_, Store>,
) -> cdf_kernel::Result<SchemaPromotionExecutionPlanArtifact>
where
    Store: PromotionSettlementStore,
{
    let promotion_id = PromotionId::new(request.dry_plan.promotion_id.clone())?;
    let artifact = SchemaPromotionExecutionPlanArtifact {
        version: SCHEMA_PROMOTION_EXECUTION_ARTIFACT_VERSION,
        promotion_id: promotion_id.clone(),
        resource_id: request.resource.descriptor().resource_id.clone(),
        old_lock_authority: request.lock_authority.clone(),
        dry_plan: request.dry_plan.clone(),
    };
    artifact.validate()?;
    let snapshot = &request
        .dry_plan
        .proposed_snapshot
        .as_ref()
        .expect("validated executable plan has a snapshot")
        .artifact;
    write_create_or_verify(
        &request.project_root.join(&snapshot.path),
        &canonical_json_bytes(snapshot)?,
    )?;
    let path = request
        .project_root
        .join(promotion_plan_relative_path(&promotion_id));
    write_create_or_verify(&path, &canonical_json_bytes(&artifact)?)?;
    let hydrated: SchemaPromotionExecutionPlanArtifact = read_json_file(&path)?;
    hydrated.validate()?;
    if hydrated != artifact {
        return Err(cdf_kernel::CdfError::data(
            "staged promotion plan conflicts with current exact authority",
        ));
    }
    Ok(hydrated)
}

impl SchemaPromotionExecutionPlanArtifact {
    pub fn validate(&self) -> cdf_kernel::Result<()> {
        if self.version != SCHEMA_PROMOTION_EXECUTION_ARTIFACT_VERSION
            || self.promotion_id.as_str() != self.dry_plan.promotion_id
            || self.resource_id.as_str() != self.dry_plan.resource_id
            || self.old_lock_authority.sha256 != self.dry_plan.lock_precondition_sha256
        {
            return Err(cdf_kernel::CdfError::data(
                "staged schema promotion plan does not match its typed dry-plan authority",
            ));
        }
        let snapshot = self
            .dry_plan
            .proposed_snapshot
            .as_ref()
            .ok_or_else(|| cdf_kernel::CdfError::data("staged promotion has no snapshot"))?;
        snapshot.artifact.validate_hash_input()?;
        let recomputed =
            validate_schema_promotion_plan_identity(&self.dry_plan, &self.old_lock_authority)?;
        if recomputed != self.promotion_id {
            return Err(cdf_kernel::CdfError::data(
                "staged schema promotion id does not match canonical RP5 authority",
            ));
        }
        Ok(())
    }
}

struct PreparedCorrectionPackage {
    package_dir: PathBuf,
    package_hash: PackageHash,
    artifact: SchemaPromotionCorrectionPackageArtifact,
    state_delta: StateDelta,
}

fn build_or_load_correction_packages<Store>(
    request: &mut SchemaPromotionExecutionRequest<'_, Store>,
    staged: &SchemaPromotionExecutionPlanArtifact,
) -> cdf_kernel::Result<Vec<PreparedCorrectionPackage>>
where
    Store: PromotionSettlementStore,
{
    let validation_program =
        promotion_validation_program(request.project_root, request.resource, staged)?;
    let scope = promotion_scope(request.resource);
    let mut chain_parent =
        request
            .settlement_store
            .head(&request.pipeline_id, &staged.resource_id, &scope)?;
    let correction_directories = staged
        .dry_plan
        .targets
        .iter()
        .map(|target| {
            request
                .package_root
                .join(correction_package_id(&staged.promotion_id, target))
        })
        .collect::<BTreeSet<_>>();
    let mut package_index = None;
    let mut packages = Vec::new();
    for target in &staged.dry_plan.targets {
        let package_id = correction_package_id(&staged.promotion_id, target);
        let package_dir = request.package_root.join(&package_id);
        let checkpoint_id = correction_checkpoint_id(&staged.promotion_id, target)?;
        let authority_path = request
            .project_root
            .join(correction_target_authority_relative_path(
                &staged.promotion_id,
                target,
            ));
        let prepared = if package_dir.join(MANIFEST_FILE).exists() {
            let authority: SchemaPromotionCorrectionTargetAuthority =
                read_json_file(&authority_path)?;
            load_correction_package(CorrectionPackageLoadAuthority {
                package_dir: &package_dir,
                staged,
                target,
                authority: &authority,
                validation_program: &validation_program,
                pipeline_id: &request.pipeline_id,
                scope: &scope,
                disposition: &request.resource.descriptor().write_disposition,
            })?
        } else {
            if package_dir.exists() {
                fs::remove_dir_all(&package_dir)
                    .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
            }
            if package_index.is_none() {
                package_index = Some(source_package_index(
                    request.package_root,
                    &correction_directories,
                )?);
            }
            let package_index = package_index
                .as_ref()
                .expect("source inventory was initialized");
            verify_target_source_receipts(
                &mut request.destinations,
                staged,
                target,
                package_index,
            )?;
            let artifact = correction_package_artifact(
                request,
                staged,
                target,
                &validation_program,
                package_index,
            )?;
            let prepared = build_correction_package(
                &package_dir,
                &package_id,
                artifact,
                &request.pipeline_id,
                checkpoint_id.clone(),
                chain_parent.clone(),
                scope.clone(),
                package_index,
            )?;
            let authority = correction_target_authority(
                staged,
                target,
                &prepared,
                checkpoint_id,
                chain_parent.clone(),
            )?;
            write_create_or_verify(&authority_path, &canonical_json_bytes(&authority)?)?;
            let hydrated: SchemaPromotionCorrectionTargetAuthority =
                read_json_file(&authority_path)?;
            if hydrated != authority {
                return Err(cdf_kernel::CdfError::data(
                    "persisted promotion correction target authority conflicts with built package",
                ));
            }
            validate_prepared_correction_package_authority(&prepared, staged, target, &hydrated)?;
            prepared
        };
        let checkpoint =
            ensure_promotion_checkpoint(request.settlement_store, &prepared.state_delta)?;
        chain_parent = Some(checkpoint_input_authority(&checkpoint));
        packages.push(prepared);
    }
    Ok(packages)
}

fn checkpoint_input_authority(checkpoint: &Checkpoint) -> Checkpoint {
    let mut authority = checkpoint.clone();
    authority.status = CheckpointStatus::Committed;
    authority.receipt = None;
    authority.is_head = true;
    authority.committed_at_ms = Some(authority.created_at_ms);
    authority
}

fn ensure_promotion_checkpoint<Store: CheckpointStore>(
    store: &Store,
    expected: &StateDelta,
) -> cdf_kernel::Result<Checkpoint> {
    let existing = store
        .history(
            &expected.pipeline_id,
            &expected.resource_id,
            &expected.scope,
        )?
        .into_iter()
        .find(|checkpoint| checkpoint.delta.checkpoint_id == expected.checkpoint_id);
    match existing {
        Some(checkpoint) if checkpoint.delta == *expected => Ok(checkpoint),
        Some(_) => Err(cdf_kernel::CdfError::contract(
            "promotion checkpoint conflicts with deterministic package authority",
        )),
        None => store.propose(expected.clone()),
    }
}

fn promotion_validation_program(
    project_root: &Path,
    resource: &CompiledResource,
    staged: &SchemaPromotionExecutionPlanArtifact,
) -> cdf_kernel::Result<cdf_contract::ValidationProgram> {
    let authority = staged
        .dry_plan
        .proposed_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.artifact.promotion_authority.as_ref())
        .ok_or_else(|| cdf_kernel::CdfError::data("promotion snapshot authority is missing"))?;
    let old_snapshot = SchemaSnapshotStore::new(project_root).read(&authority.old_snapshot)?;
    let old_schema = old_snapshot.schema.to_arrow()?;
    let policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let observed = ObservedSchema::from_arrow(&old_schema);
    let program = compile_resource_validation_program(&policy, &observed, resource.descriptor())?;
    let hash = crate::internal::semantic_hash(&program)?;
    if authority.validation_program_hash.as_deref() != Some(hash.as_str()) {
        return Err(cdf_kernel::CdfError::contract(format!(
            "promotion validation program hash {hash} does not match typed snapshot authority {:?}",
            authority.validation_program_hash
        )));
    }
    Ok(program)
}

fn verify_target_source_receipts(
    destinations: &mut [ResolvedProjectDestination],
    staged: &SchemaPromotionExecutionPlanArtifact,
    target: &SchemaPromotionTargetReport,
    package_index: &BTreeMap<String, PathBuf>,
) -> cdf_kernel::Result<()> {
    let destination_id = DestinationId::new(target.destination.clone())?;
    let target_name = TargetName::new(target.target.clone())?;
    let destination = take_destination(destinations, &destination_id, &target_name)?;
    destination.runtime_mut().ensure_protocol_ready()?;

    let mut packages = BTreeMap::<String, Vec<String>>::new();
    for path in staged
        .dry_plan
        .paths
        .iter()
        .filter(|path| target.affected_paths.contains(&path.path))
    {
        for association in path.associations.iter().filter(|association| {
            association.destination == target.destination && association.target == target.target
        }) {
            let prior = packages.insert(
                association.package_hash.clone(),
                association.recorded_receipt_ids.clone(),
            );
            if prior
                .as_ref()
                .is_some_and(|prior| prior != &association.recorded_receipt_ids)
            {
                return Err(cdf_kernel::CdfError::data(format!(
                    "source package {} has conflicting receipt authority across promoted paths",
                    association.package_hash
                )));
            }
        }
    }
    if packages.is_empty() {
        return Err(cdf_kernel::CdfError::data(format!(
            "promotion target {}/{} has no source package receipt authority",
            target.destination, target.target
        )));
    }
    for (package_hash, expected_receipt_ids) in packages {
        let package_dir = package_index.get(&package_hash).ok_or_else(|| {
            cdf_kernel::CdfError::data(format!(
                "retained promotion source package {package_hash} is missing"
            ))
        })?;
        verify_source_package_receipts(
            package_dir,
            &expected_receipt_ids,
            &destination_id,
            &target_name,
            destination,
        )?;
    }
    Ok(())
}

fn verify_source_package_receipts(
    package_dir: &Path,
    expected_receipt_ids: &[String],
    destination_id: &DestinationId,
    target: &TargetName,
    destination: &mut ResolvedProjectDestination,
) -> cdf_kernel::Result<()> {
    let reader = PackageReader::open(package_dir)?;
    reader.verify()?;
    let replay = reader.replay_inputs()?;
    let mut receipts = reader
        .receipts()?
        .into_iter()
        .filter(|receipt| &receipt.destination == destination_id && &receipt.target == target)
        .collect::<Vec<_>>();
    receipts.sort_by(|left, right| left.receipt_id.cmp(&right.receipt_id));
    let actual_ids = receipts
        .iter()
        .map(|receipt| receipt.receipt_id.to_string())
        .collect::<Vec<_>>();
    let mut expected_ids = expected_receipt_ids.to_vec();
    expected_ids.sort();
    if actual_ids != expected_ids {
        return Err(cdf_kernel::CdfError::data(format!(
            "source package {} receipt ids changed before correction packaging",
            replay.state_delta.package_hash
        )));
    }
    let expected_acks = replay
        .state_delta
        .segments
        .iter()
        .map(|segment| cdf_kernel::SegmentAck {
            segment_id: segment.segment_id.clone(),
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        })
        .collect::<Vec<_>>();
    for receipt in receipts {
        if receipt.package_hash != replay.state_delta.package_hash
            || receipt.target != replay.destination_commit.target
            || receipt.disposition != replay.destination_commit.disposition
            || receipt.idempotency_token != replay.destination_commit.idempotency_token
            || receipt.schema_hash != replay.schema_hash
            || receipt.segment_acks != expected_acks
            || !receipt.covers_state_delta(&replay.state_delta)
        {
            return Err(cdf_kernel::CdfError::data(format!(
                "source receipt {} does not exactly cover package/state/segment authority",
                receipt.receipt_id
            )));
        }
        let verification = destination.runtime_mut().verify_receipt(&receipt)?;
        if !verification.verified {
            return Err(cdf_kernel::CdfError::destination(format!(
                "source receipt {} did not verify against the live destination",
                receipt.receipt_id
            )));
        }
    }
    Ok(())
}

fn correction_package_artifact<Store>(
    request: &SchemaPromotionExecutionRequest<'_, Store>,
    staged: &SchemaPromotionExecutionPlanArtifact,
    target: &SchemaPromotionTargetReport,
    validation_program: &cdf_contract::ValidationProgram,
    package_index: &BTreeMap<String, PathBuf>,
) -> cdf_kernel::Result<SchemaPromotionCorrectionPackageArtifact>
where
    Store: PromotionSettlementStore,
{
    let strategy = target.strategy.ok_or_else(|| {
        cdf_kernel::CdfError::contract("promotion target has no selected correction strategy")
    })?;
    let locked_destination = request
        .lock
        .destinations
        .get(&target.destination)
        .ok_or_else(|| {
            cdf_kernel::CdfError::contract(format!(
                "promotion destination {:?} is absent from cdf.lock",
                target.destination
            ))
        })?;
    let capability = locked_destination
        .protocol_capabilities
        .corrections
        .strategy(strategy)
        .ok_or_else(|| {
            cdf_kernel::CdfError::contract("selected correction strategy is no longer locked")
        })?;
    let snapshot = &staged
        .dry_plan
        .proposed_snapshot
        .as_ref()
        .expect("validated staged plan has snapshot")
        .artifact;
    let promotion_id = staged.promotion_id.clone();
    let old_schema_hash = SchemaHash::new(staged.dry_plan.old_schema_hash.clone())?;
    let new_schema_hash = snapshot.schema_hash.clone();
    let mut operations = Vec::new();
    let mut source_packages = BTreeSet::new();
    for path in &staged.dry_plan.paths {
        if !target.affected_paths.contains(&path.path) {
            continue;
        }
        let selected = path.selected_arrow_type.as_ref().ok_or_else(|| {
            cdf_kernel::CdfError::contract("promotion path has no selected Arrow type")
        })?;
        let proposed_schema = snapshot.schema.to_arrow()?;
        let proposed_field = proposed_schema
            .field_with_name(&path.output_field)
            .map_err(|_| cdf_kernel::CdfError::data("promoted output field is missing"))?
            .clone();
        let output_field = CanonicalArrowField::from_arrow(&proposed_field)?;
        let associated = path
            .associations
            .iter()
            .filter(|association| {
                association.destination == target.destination && association.target == target.target
            })
            .collect::<Vec<_>>();
        for association in associated {
            if association.availability != SchemaPromotionEvidenceAvailability::RetainedPackage {
                return Err(cdf_kernel::CdfError::contract(
                    "promotion execution requires retained package residual authority",
                ));
            }
            let package_dir = package_index
                .get(&association.package_hash)
                .ok_or_else(|| {
                    cdf_kernel::CdfError::data(format!(
                        "retained promotion package {} is missing",
                        association.package_hash
                    ))
                })?;
            let package_hash = PackageHash::new(association.package_hash.clone())?;
            source_packages.insert(package_hash.clone());
            operations.extend(extract_operations(
                package_dir,
                &association.recorded_receipt_ids,
                &target.destination,
                &target.target,
                &promotion_id,
                &old_schema_hash,
                &new_schema_hash,
                strategy,
                capability.transaction_guarantee.clone(),
                capability.idempotency_guarantee.clone(),
                &path.path,
                &path.source_name,
                selected,
                &output_field,
            )?);
        }
    }
    operations.sort_by(|left, right| {
        let left = &left.correction.request;
        let right = &right.correction.request;
        (&left.original_row, &left.promoted_path).cmp(&(&right.original_row, &right.promoted_path))
    });
    if operations.is_empty() {
        return Err(cdf_kernel::CdfError::contract(format!(
            "promotion target {}/{} has no executable retained correction operations",
            target.destination, target.target
        )));
    }
    let artifact = SchemaPromotionCorrectionPackageArtifact {
        version: SCHEMA_PROMOTION_CORRECTION_PACKAGE_VERSION,
        promotion_id,
        resource_id: staged.resource_id.clone(),
        destination_id: DestinationId::new(target.destination.clone())?,
        target: TargetName::new(target.target.clone())?,
        old_schema_hash,
        new_schema_hash,
        strategy,
        disposition: request.resource.descriptor().write_disposition.clone(),
        source_packages: source_packages.into_iter().collect(),
        validation_program: validation_program.clone(),
        operations,
    };
    artifact.validate()?;
    Ok(artifact)
}

impl SchemaPromotionCorrectionPackageArtifact {
    pub fn validate(&self) -> cdf_kernel::Result<()> {
        if self.version != SCHEMA_PROMOTION_CORRECTION_PACKAGE_VERSION
            || self.operations.is_empty()
            || self.source_packages.is_empty()
        {
            return Err(cdf_kernel::CdfError::data(
                "promotion correction package has incomplete typed authority",
            ));
        }
        for operation in &self.operations {
            operation.validate_structure()?;
            let correction = &operation.correction.request;
            if correction.promotion_id != self.promotion_id
                || correction.old_schema_hash != self.old_schema_hash
                || correction.new_schema_hash != self.new_schema_hash
                || correction.selected_strategy != self.strategy
                || !self
                    .source_packages
                    .contains(&correction.original_row.original_package_hash)
            {
                return Err(cdf_kernel::CdfError::data(
                    "promotion correction operation does not match package authority",
                ));
            }
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn extract_operations(
    package_dir: &Path,
    expected_receipt_ids: &[String],
    destination: &str,
    target: &str,
    promotion_id: &PromotionId,
    old_schema_hash: &SchemaHash,
    new_schema_hash: &SchemaHash,
    strategy: CorrectionStrategy,
    transaction_guarantee: cdf_kernel::TransactionSupport,
    idempotency_guarantee: cdf_kernel::IdempotencySupport,
    path: &str,
    source_name: &str,
    selected_type: &cdf_kernel::CanonicalArrowType,
    output_field: &CanonicalArrowField,
) -> cdf_kernel::Result<Vec<DestinationCorrectionOperation>> {
    let reader = PackageReader::open(package_dir)?;
    reader.verify()?;
    let package_hash = PackageHash::new(reader.manifest().package_hash.clone())?;
    let receipts = reader
        .receipts()?
        .into_iter()
        .filter(|receipt| {
            receipt.destination.as_str() == destination && receipt.target.as_str() == target
        })
        .map(|receipt| receipt.receipt_id.to_string())
        .collect::<BTreeSet<_>>();
    if receipts
        != expected_receipt_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
    {
        return Err(cdf_kernel::CdfError::data(format!(
            "source package {package_hash} receipt authority changed before promotion"
        )));
    }
    let selected = selected_type.to_arrow()?;
    let mut operations = Vec::new();
    for segment in &reader.manifest().identity.segments {
        let mut ordinal = 0_u64;
        for batch in reader.read_segment(&segment.segment_id)? {
            let variant_index = batch
                .schema()
                .fields()
                .iter()
                .position(|field| is_framework_variant_field(field))
                .ok_or_else(|| {
                    cdf_kernel::CdfError::data(format!(
                        "source package {package_hash} segment {} has no framework residual column",
                        segment.segment_id
                    ))
                })?;
            let variant = batch
                .column(variant_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| cdf_kernel::CdfError::data("framework residual is not utf8"))?;
            for row in 0..batch.num_rows() {
                if variant.is_null(row) {
                    continue;
                }
                let decoded = decode_residual_json_v1(variant.value(row).as_bytes())
                    .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
                let Some(field) = decoded.into_iter().find(|field| field.path == path) else {
                    continue;
                };
                let casted = arrow_cast::cast(field.array.as_ref(), &selected)
                    .map_err(cdf_kernel::CdfError::from)?;
                let envelope = encode_residual_json_v1([ResidualFieldRef::new(
                    [source_name],
                    casted.as_ref(),
                    0,
                )
                .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?])
                .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
                let request = cdf_kernel::DestinationCorrectionRequest {
                    promotion_id: promotion_id.clone(),
                    original_row: cdf_kernel::RowProvenanceAddress::new(
                        package_hash.clone(),
                        segment.segment_id.clone(),
                        ordinal + row as u64,
                    ),
                    old_schema_hash: old_schema_hash.clone(),
                    new_schema_hash: new_schema_hash.clone(),
                    promoted_path: path.to_owned(),
                    promoted_value_json: String::from_utf8(envelope.clone())
                        .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?,
                    residual_operation: cdf_kernel::ResidualCorrectionOperation::RemovePromotedPath,
                    selected_strategy: strategy,
                };
                operations.push(DestinationCorrectionOperation {
                    correction: DestinationCorrectionPlan {
                        request,
                        transaction_guarantee: transaction_guarantee.clone(),
                        idempotency_guarantee: idempotency_guarantee.clone(),
                    },
                    output_field: output_field.clone(),
                    promoted_value_residual_json_v1: envelope,
                });
            }
            ordinal += batch.num_rows() as u64;
        }
    }
    Ok(operations)
}

#[allow(clippy::too_many_arguments)]
fn build_correction_package(
    package_dir: &Path,
    package_id: &str,
    artifact: SchemaPromotionCorrectionPackageArtifact,
    pipeline_id: &PipelineId,
    checkpoint_id: CheckpointId,
    input_checkpoint: Option<Checkpoint>,
    scope: ScopeKey,
    package_index: &BTreeMap<String, PathBuf>,
) -> cdf_kernel::Result<PreparedCorrectionPackage> {
    let mut builder = PackageBuilder::create(package_dir, package_id)?;
    builder.write_json_artifact("plan/promotion-correction.json", &artifact)?;
    builder.write_json_artifact("plan/validation-program.json", &artifact.validation_program)?;
    let operation_json = artifact
        .operations
        .iter()
        .map(serde_json::to_string)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| cdf_kernel::CdfError::internal(error.to_string()))?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "correction_operation_json",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(operation_json))],
    )
    .map_err(cdf_kernel::CdfError::from)?;
    let segment =
        builder.write_segment(cdf_kernel::SegmentId::new("correction-000001")?, &[batch])?;
    let output_position = source_package_position(&artifact.source_packages, package_index)?;
    let state_segment = StateSegment {
        segment_id: segment.segment_id.clone(),
        scope: scope.clone(),
        output_position: output_position.clone(),
        row_count: segment.row_count,
        byte_count: segment.byte_count,
    };
    let preimage = StateDeltaPreimage {
        checkpoint_id,
        pipeline_id: pipeline_id.clone(),
        resource_id: artifact.resource_id.clone(),
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: input_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.delta.checkpoint_id.clone()),
        input_position: input_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.delta.output_position.clone()),
        output_position,
        schema_hash: artifact.new_schema_hash.clone(),
        segments: vec![state_segment],
    };
    builder.write_input_checkpoint_artifact(&input_checkpoint)?;
    builder.write_state_delta_preimage_artifact(&preimage)?;
    builder.write_commit_plan_preimage_artifact(
        &DestinationCommitPlanPreimage::package_hash_token(
            artifact.target.clone(),
            artifact.disposition.clone(),
            Vec::new(),
            artifact.new_schema_hash.clone(),
            preimage.segments.clone(),
        ),
    )?;
    let manifest = builder.finish_with_status(PackageStatus::Packaged)?;
    let package_hash = PackageHash::new(manifest.package_hash)?;
    let reader = PackageReader::open(package_dir)?;
    let replay = reader.replay_inputs()?;
    let state_delta = replay.state_delta;
    Ok(PreparedCorrectionPackage {
        package_dir: package_dir.to_path_buf(),
        package_hash,
        artifact,
        state_delta,
    })
}

struct CorrectionPackageLoadAuthority<'a> {
    package_dir: &'a Path,
    staged: &'a SchemaPromotionExecutionPlanArtifact,
    target: &'a SchemaPromotionTargetReport,
    authority: &'a SchemaPromotionCorrectionTargetAuthority,
    validation_program: &'a cdf_contract::ValidationProgram,
    pipeline_id: &'a PipelineId,
    scope: &'a ScopeKey,
    disposition: &'a cdf_kernel::WriteDisposition,
}

fn load_correction_package(
    expected: CorrectionPackageLoadAuthority<'_>,
) -> cdf_kernel::Result<PreparedCorrectionPackage> {
    let CorrectionPackageLoadAuthority {
        package_dir,
        staged,
        target,
        authority,
        validation_program,
        pipeline_id,
        scope,
        disposition,
    } = expected;
    let reader = PackageReader::open(package_dir)?;
    if reader.manifest().lifecycle.status == PackageStatus::Archived {
        return Err(cdf_kernel::CdfError::data(
            "promotion correction package was archived before publication completed",
        ));
    }
    reader.verify()?;
    let artifact: SchemaPromotionCorrectionPackageArtifact =
        read_json_file(&package_dir.join("plan/promotion-correction.json"))?;
    artifact.validate()?;
    validate_correction_artifact_for_staged(
        &artifact,
        staged,
        target,
        validation_program,
        disposition,
    )?;
    let package_hash = PackageHash::new(reader.manifest().package_hash.clone())?;
    let input_checkpoint = reader.input_checkpoint()?;
    let replay = reader.replay_inputs()?;
    if replay.state_delta.pipeline_id != *pipeline_id
        || replay.state_delta.resource_id != staged.resource_id
        || replay.state_delta.scope != *scope
        || replay.state_delta.schema_hash != artifact.new_schema_hash
        || replay
            .state_delta
            .segments
            .iter()
            .any(|segment| segment.scope != *scope)
        || replay.destination_commit.target != artifact.target
        || replay.destination_commit.disposition != artifact.disposition
        || replay.destination_commit.package_hash != package_hash
        || replay.destination_commit.segments != replay.state_delta.segments
        || input_checkpoint != authority.input_checkpoint
        || replay.state_delta.checkpoint_id != authority.checkpoint_id
        || replay.state_delta.parent_checkpoint_id
            != authority
                .input_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.delta.checkpoint_id.clone())
        || replay.state_delta.input_position
            != authority
                .input_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.delta.output_position.clone())
    {
        return Err(cdf_kernel::CdfError::data(
            "correction package replay preimages conflict with staged promotion authority",
        ));
    }
    let operations = read_correction_package_operations(&reader)?;
    if operations != artifact.operations {
        return Err(cdf_kernel::CdfError::data(
            "correction package operation segment conflicts with typed correction artifact",
        ));
    }
    let state_delta = replay.state_delta;
    let prepared = PreparedCorrectionPackage {
        package_dir: package_dir.to_path_buf(),
        package_hash,
        artifact,
        state_delta,
    };
    validate_prepared_correction_package_authority(&prepared, staged, target, authority)?;
    Ok(prepared)
}

fn validate_correction_artifact_for_staged(
    artifact: &SchemaPromotionCorrectionPackageArtifact,
    staged: &SchemaPromotionExecutionPlanArtifact,
    target: &SchemaPromotionTargetReport,
    validation_program: &cdf_contract::ValidationProgram,
    disposition: &cdf_kernel::WriteDisposition,
) -> cdf_kernel::Result<()> {
    let snapshot = staged
        .dry_plan
        .proposed_snapshot
        .as_ref()
        .ok_or_else(|| cdf_kernel::CdfError::data("staged promotion snapshot is missing"))?;
    let expected_packages = target
        .affected_packages
        .iter()
        .map(|package| PackageHash::new(package.clone()))
        .collect::<cdf_kernel::Result<Vec<_>>>()?;
    if artifact.promotion_id != staged.promotion_id
        || artifact.resource_id != staged.resource_id
        || artifact.destination_id.as_str() != target.destination
        || artifact.target.as_str() != target.target
        || artifact.old_schema_hash.as_str() != staged.dry_plan.old_schema_hash
        || artifact.new_schema_hash != snapshot.artifact.schema_hash
        || Some(artifact.strategy) != target.strategy
        || &artifact.disposition != disposition
        || artifact.source_packages != expected_packages
        || &artifact.validation_program != validation_program
    {
        return Err(cdf_kernel::CdfError::data(
            "existing promotion correction package conflicts with staged target authority",
        ));
    }
    let proposed_schema = snapshot.artifact.schema.to_arrow()?;
    let mut path_authority = BTreeMap::new();
    for path in staged
        .dry_plan
        .paths
        .iter()
        .filter(|path| target.affected_paths.contains(&path.path))
    {
        let field = proposed_schema
            .field_with_name(&path.output_field)
            .map_err(|_| cdf_kernel::CdfError::data("promoted output field is missing"))?;
        let packages = path
            .associations
            .iter()
            .filter(|association| {
                association.destination == target.destination && association.target == target.target
            })
            .map(|association| association.package_hash.as_str())
            .collect::<BTreeSet<_>>();
        path_authority.insert(
            path.path.as_str(),
            (CanonicalArrowField::from_arrow(field)?, packages),
        );
    }
    let mut addresses = BTreeSet::new();
    for operation in &artifact.operations {
        let request = &operation.correction.request;
        let Some((field, packages)) = path_authority.get(request.promoted_path.as_str()) else {
            return Err(cdf_kernel::CdfError::data(
                "correction package operation names a path outside staged target authority",
            ));
        };
        if &operation.output_field != field
            || !packages.contains(request.original_row.original_package_hash.as_str())
            || !addresses.insert((request.original_row.clone(), request.promoted_path.clone()))
        {
            return Err(cdf_kernel::CdfError::data(
                "correction package operation conflicts with staged path/package authority",
            ));
        }
    }
    Ok(())
}

fn read_correction_package_operations(
    reader: &PackageReader,
) -> cdf_kernel::Result<Vec<DestinationCorrectionOperation>> {
    let mut operations = Vec::new();
    for segment in &reader.manifest().identity.segments {
        for batch in reader.read_segment(&segment.segment_id)? {
            if batch.num_columns() != 1 {
                return Err(cdf_kernel::CdfError::data(
                    "correction package segment must contain one typed operation column",
                ));
            }
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    cdf_kernel::CdfError::data(
                        "correction package operation column must be non-null utf8",
                    )
                })?;
            for row in 0..values.len() {
                if values.is_null(row) {
                    return Err(cdf_kernel::CdfError::data(
                        "correction package operation column contains null",
                    ));
                }
                operations.push(
                    serde_json::from_str(values.value(row))
                        .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?,
                );
            }
        }
    }
    Ok(operations)
}

fn correction_operation_digest(
    operations: &[DestinationCorrectionOperation],
) -> cdf_kernel::Result<String> {
    Ok(format!(
        "sha256:{}",
        hex::encode(Sha256::digest(canonical_json_bytes(&operations)?))
    ))
}

fn correction_target_path_authority(
    staged: &SchemaPromotionExecutionPlanArtifact,
    target: &SchemaPromotionTargetReport,
) -> Vec<SchemaPromotionCorrectionPathAuthority> {
    staged
        .dry_plan
        .paths
        .iter()
        .filter(|path| target.affected_paths.contains(&path.path))
        .map(|path| SchemaPromotionCorrectionPathAuthority {
            path: path.path.clone(),
            observed_count: path.observed_count,
            affected_address_value_digest: path.affected_address_value_digest.clone(),
        })
        .collect()
}

fn correction_target_authority(
    staged: &SchemaPromotionExecutionPlanArtifact,
    target: &SchemaPromotionTargetReport,
    package: &PreparedCorrectionPackage,
    checkpoint_id: CheckpointId,
    input_checkpoint: Option<Checkpoint>,
) -> cdf_kernel::Result<SchemaPromotionCorrectionTargetAuthority> {
    Ok(SchemaPromotionCorrectionTargetAuthority {
        version: SCHEMA_PROMOTION_CORRECTION_TARGET_AUTHORITY_VERSION,
        promotion_id: staged.promotion_id.clone(),
        resource_id: staged.resource_id.clone(),
        destination_id: DestinationId::new(target.destination.clone())?,
        target: TargetName::new(target.target.clone())?,
        correction_package_hash: package.package_hash.clone(),
        operation_count: package.artifact.operations.len() as u64,
        operation_digest: correction_operation_digest(&package.artifact.operations)?,
        checkpoint_id,
        input_checkpoint,
        paths: correction_target_path_authority(staged, target),
    })
}

fn validate_prepared_correction_package_authority(
    package: &PreparedCorrectionPackage,
    staged: &SchemaPromotionExecutionPlanArtifact,
    target: &SchemaPromotionTargetReport,
    authority: &SchemaPromotionCorrectionTargetAuthority,
) -> cdf_kernel::Result<()> {
    if authority.version != SCHEMA_PROMOTION_CORRECTION_TARGET_AUTHORITY_VERSION
        || authority.promotion_id != staged.promotion_id
        || authority.resource_id != staged.resource_id
        || authority.destination_id.as_str() != target.destination
        || authority.target.as_str() != target.target
        || authority.correction_package_hash != package.package_hash
        || authority.operation_count != package.artifact.operations.len() as u64
        || authority.operation_digest != correction_operation_digest(&package.artifact.operations)?
        || authority.checkpoint_id != package.state_delta.checkpoint_id
        || authority.paths != correction_target_path_authority(staged, target)
    {
        return Err(cdf_kernel::CdfError::data(
            "promotion correction package conflicts with persisted target authority",
        ));
    }
    Ok(())
}

fn settle_correction_package(
    destination: &mut ResolvedProjectDestination,
    package: &PreparedCorrectionPackage,
) -> cdf_kernel::Result<Receipt> {
    destination.runtime_mut().ensure_protocol_ready()?;
    let reader = PackageReader::open(&package.package_dir)?;
    let request = DestinationCorrectionCommitRequest::new(
        package.package_hash.clone(),
        IdempotencyToken::new(package.package_hash.to_string())?,
        package.artifact.target.clone(),
        package.artifact.disposition.clone(),
        package.state_delta.segments.clone(),
        package.artifact.operations.clone(),
    )?;
    if !reader.receipts()?.is_empty() {
        return verify_stored_correction_receipt(destination, package);
    }
    let runtime = destination.runtime_mut();
    let plan = runtime.prepare_correction_commit(&package.package_dir, &request)?;
    let protocol = runtime.protocol();
    let mut session = protocol.begin_correction(request.clone(), plan.clone())?;
    session.apply_migrations()?;
    session.apply_corrections()?;
    let receipt = session.finalize()?;
    plan.validate_receipt(&request, &receipt)?;
    let verification = protocol.verify_correction(&receipt)?;
    if !verification.verified {
        return Err(cdf_kernel::CdfError::destination(
            "promotion correction receipt verification failed",
        ));
    }
    reader.append_receipt(receipt.clone())?;
    Ok(receipt)
}

fn verify_stored_correction_receipt(
    destination: &mut ResolvedProjectDestination,
    package: &PreparedCorrectionPackage,
) -> cdf_kernel::Result<Receipt> {
    destination.runtime_mut().ensure_protocol_ready()?;
    let reader = PackageReader::open(&package.package_dir)?;
    let receipts = reader.receipts()?;
    if receipts.len() != 1 {
        return Err(cdf_kernel::CdfError::contract(
            "promotion correction package must contain exactly one canonical receipt",
        ));
    }
    let receipt = receipts
        .into_iter()
        .next()
        .expect("one receipt was checked");
    let request = DestinationCorrectionCommitRequest::new(
        package.package_hash.clone(),
        IdempotencyToken::new(package.package_hash.to_string())?,
        package.artifact.target.clone(),
        package.artifact.disposition.clone(),
        package.state_delta.segments.clone(),
        package.artifact.operations.clone(),
    )?;
    let runtime = destination.runtime_mut();
    let plan = runtime.prepare_correction_commit(&package.package_dir, &request)?;
    let protocol = runtime.protocol();
    plan.validate_receipt(&request, &receipt)?;
    let verification = protocol.verify_correction(&receipt)?;
    if !verification.verified {
        return Err(cdf_kernel::CdfError::destination(
            "stored promotion correction receipt did not verify",
        ));
    }
    Ok(receipt)
}

fn committed_target_report<StateStore: CheckpointStore>(
    checkpoint_store: &StateStore,
    package: &PreparedCorrectionPackage,
    verified_receipt: &Receipt,
) -> cdf_kernel::Result<SchemaPromotionExecutionTargetReport> {
    let checkpoint = checkpoint_store
        .history(
            &package.state_delta.pipeline_id,
            &package.state_delta.resource_id,
            &package.state_delta.scope,
        )?
        .into_iter()
        .find(|checkpoint| checkpoint.delta.checkpoint_id == package.state_delta.checkpoint_id)
        .ok_or_else(|| {
            cdf_kernel::CdfError::contract(
                "published promotion lock is missing a required target checkpoint",
            )
        })?;
    if checkpoint.status != CheckpointStatus::Committed {
        return Err(cdf_kernel::CdfError::contract(
            "published promotion lock has an uncommitted target checkpoint",
        ));
    }
    let receipt = checkpoint.receipt.as_ref().ok_or_else(|| {
        cdf_kernel::CdfError::contract("committed promotion checkpoint has no receipt")
    })?;
    if receipt != verified_receipt
        || receipt.package_hash != package.package_hash
        || receipt.destination != package.artifact.destination_id
        || receipt.target != package.artifact.target
    {
        return Err(cdf_kernel::CdfError::contract(
            "promotion checkpoint receipt conflicts with correction package authority",
        ));
    }
    Ok(SchemaPromotionExecutionTargetReport {
        destination: package.artifact.destination_id.to_string(),
        target: package.artifact.target.to_string(),
        correction_package_hash: package.package_hash.to_string(),
        receipt_id: Some(receipt.receipt_id.to_string()),
        checkpoint_id: Some(checkpoint.delta.checkpoint_id.to_string()),
        committed: true,
    })
}

fn settle_promotion_checkpoint<Store: PromotionSettlementStore>(
    settlement_store: &Store,
    lease: &ScopeLease,
    package: &PreparedCorrectionPackage,
    receipt: Receipt,
) -> cdf_kernel::Result<cdf_kernel::Checkpoint> {
    let existing = settlement_store
        .history(
            &package.state_delta.pipeline_id,
            &package.state_delta.resource_id,
            &package.state_delta.scope,
        )?
        .into_iter()
        .find(|checkpoint| checkpoint.delta.checkpoint_id == package.state_delta.checkpoint_id);
    let proposed = match existing {
        Some(checkpoint) if checkpoint.status == CheckpointStatus::Committed => {
            if checkpoint.receipt.as_ref() != Some(&receipt) {
                return Err(cdf_kernel::CdfError::contract(
                    "committed promotion checkpoint has conflicting receipt authority",
                ));
            }
            return Ok(checkpoint);
        }
        Some(checkpoint) if checkpoint.status == CheckpointStatus::Proposed => checkpoint,
        Some(_) => {
            return Err(cdf_kernel::CdfError::contract(
                "promotion checkpoint is terminal but not committed",
            ));
        }
        None => settlement_store.propose(package.state_delta.clone())?,
    };
    settlement_store.commit_promotion_checkpoint(lease, &proposed.delta.checkpoint_id, receipt)
}

fn publish_lock<Store>(
    request: &SchemaPromotionExecutionRequest<'_, Store>,
    lease: &ScopeLease,
    staged: &SchemaPromotionExecutionPlanArtifact,
) -> cdf_kernel::Result<LockFileAuthority>
where
    Store: PromotionSettlementStore,
{
    let lock_path = request.project_root.join(LOCK_FILE_NAME);
    let snapshot = staged
        .dry_plan
        .proposed_snapshot
        .as_ref()
        .expect("validated staged plan has snapshot");
    let current = read_lock_file_authority(&lock_path)?;
    if current.sha256 != staged.old_lock_authority.sha256 {
        let current_lock = parse_lock(
            std::str::from_utf8(&current.bytes)
                .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?,
        )?;
        let installed = current_lock
            .resources
            .get(staged.resource_id.as_str())
            .and_then(|resource| resource.schema_snapshot.as_ref());
        if installed == Some(&snapshot.artifact.reference()) {
            return Ok(current);
        }
        return Err(cdf_kernel::CdfError::contract(format!(
            "concurrent schema authority conflict: expected lock {}, found {}",
            staged.old_lock_authority.sha256, current.sha256
        )));
    }
    let mut replacement = parse_lock_authority(&staged.old_lock_authority.bytes)?;
    let locked = replacement
        .resources
        .get_mut(staged.resource_id.as_str())
        .ok_or_else(|| cdf_kernel::CdfError::contract("promotion resource left cdf.lock"))?;
    locked.schema_snapshot = Some(snapshot.artifact.reference());
    locked.schema_hash = Some(snapshot.schema_hash.clone());
    let replacement = lock_to_toml(&replacement)?;
    Ok(compare_and_swap_lock_file(
        lock_path,
        &staged.old_lock_authority,
        replacement.as_bytes(),
        request.settlement_store,
        lease,
    )?
    .installed)
}

fn publish_event<Store>(
    request: &SchemaPromotionExecutionRequest<'_, Store>,
    lease: &ScopeLease,
    installed_lock: &LockFileAuthority,
    targets: &[SchemaPromotionExecutionTargetReport],
) -> cdf_kernel::Result<PromotionPublicationEvent>
where
    Store: PromotionSettlementStore,
{
    let target_events = publication_targets(targets)?;
    request.settlement_store.publish_promotion(
        lease,
        PromotionPublicationEvent {
            version: PROMOTION_PUBLICATION_EVENT_VERSION,
            promotion_id: PromotionId::new(request.dry_plan.promotion_id.clone())?,
            resource_id: request.resource.descriptor().resource_id.clone(),
            old_schema_hash: SchemaHash::new(request.dry_plan.old_schema_hash.clone())?,
            new_schema_hash: SchemaHash::new(
                request
                    .dry_plan
                    .new_schema_hash
                    .clone()
                    .ok_or_else(|| cdf_kernel::CdfError::internal("new schema hash missing"))?,
            )?,
            installed_lock_sha256: installed_lock.sha256.clone(),
            targets: target_events,
            published_at_ms: now_ms()?,
        },
    )
}

fn publication_targets(
    targets: &[SchemaPromotionExecutionTargetReport],
) -> cdf_kernel::Result<Vec<PromotionPublicationTarget>> {
    let mut target_events =
        targets
            .iter()
            .map(|target| {
                Ok(PromotionPublicationTarget {
                    destination_id: DestinationId::new(target.destination.clone())?,
                    target: TargetName::new(target.target.clone())?,
                    correction_package_hash: PackageHash::new(
                        target.correction_package_hash.clone(),
                    )?,
                    receipt_id: cdf_kernel::ReceiptId::new(target.receipt_id.clone().ok_or_else(
                        || cdf_kernel::CdfError::internal("target receipt missing"),
                    )?)?,
                    checkpoint_id: CheckpointId::new(target.checkpoint_id.clone().ok_or_else(
                        || cdf_kernel::CdfError::internal("target checkpoint missing"),
                    )?)?,
                })
            })
            .collect::<cdf_kernel::Result<Vec<_>>>()?;
    target_events.sort_by(|left, right| {
        (&left.destination_id, &left.target).cmp(&(&right.destination_id, &right.target))
    });
    Ok(target_events)
}

fn lock_has_staged_snapshot(
    project_root: &Path,
    staged: &SchemaPromotionExecutionPlanArtifact,
) -> cdf_kernel::Result<bool> {
    let authority = read_lock_file_authority(project_root.join(LOCK_FILE_NAME))?;
    let lock = parse_lock(
        std::str::from_utf8(&authority.bytes)
            .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?,
    )?;
    let expected = staged
        .dry_plan
        .proposed_snapshot
        .as_ref()
        .ok_or_else(|| cdf_kernel::CdfError::data("staged promotion has no snapshot"))?
        .artifact
        .reference();
    Ok(lock
        .resources
        .get(staged.resource_id.as_str())
        .and_then(|resource| resource.schema_snapshot.as_ref())
        == Some(&expected))
}

fn verify_publication_authority(
    publication: &PromotionPublicationEvent,
    staged: &SchemaPromotionExecutionPlanArtifact,
    installed_lock: &LockFileAuthority,
    expected_targets: &[PromotionPublicationTarget],
) -> cdf_kernel::Result<()> {
    publication.validate()?;
    let expected_new = staged
        .dry_plan
        .new_schema_hash
        .as_deref()
        .ok_or_else(|| cdf_kernel::CdfError::data("staged promotion has no new schema hash"))?;
    if publication.promotion_id != staged.promotion_id
        || publication.resource_id != staged.resource_id
        || publication.old_schema_hash.as_str() != staged.dry_plan.old_schema_hash
        || publication.new_schema_hash.as_str() != expected_new
        || publication.installed_lock_sha256 != installed_lock.sha256
        || publication.targets != expected_targets
        || !lock_has_staged_snapshot_from_authority(installed_lock, staged)?
    {
        return Err(cdf_kernel::CdfError::contract(
            "promotion publication event conflicts with staged plan or installed lock authority",
        ));
    }
    Ok(())
}

fn lock_has_staged_snapshot_from_authority(
    authority: &LockFileAuthority,
    staged: &SchemaPromotionExecutionPlanArtifact,
) -> cdf_kernel::Result<bool> {
    let lock = parse_lock(
        std::str::from_utf8(&authority.bytes)
            .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?,
    )?;
    let expected = staged
        .dry_plan
        .proposed_snapshot
        .as_ref()
        .ok_or_else(|| cdf_kernel::CdfError::data("staged promotion has no snapshot"))?
        .artifact
        .reference();
    Ok(lock
        .resources
        .get(staged.resource_id.as_str())
        .and_then(|resource| resource.schema_snapshot.as_ref())
        == Some(&expected))
}

fn report_from_publication(
    staged: &SchemaPromotionExecutionPlanArtifact,
    publication: &PromotionPublicationEvent,
    resumed: bool,
) -> SchemaPromotionExecutionReport {
    SchemaPromotionExecutionReport {
        resource_id: staged.resource_id.to_string(),
        promotion_id: staged.promotion_id.to_string(),
        phase: SchemaPromotionExecutionPhase::Complete,
        resumed,
        old_schema_hash: staged.dry_plan.old_schema_hash.clone(),
        new_schema_hash: publication.new_schema_hash.to_string(),
        staged_plan_path: promotion_plan_relative_path(&staged.promotion_id),
        snapshot_path: staged
            .dry_plan
            .new_schema_snapshot_path
            .clone()
            .expect("validated staged plan has snapshot path"),
        targets: publication
            .targets
            .iter()
            .map(|target| SchemaPromotionExecutionTargetReport {
                destination: target.destination_id.to_string(),
                target: target.target.to_string(),
                correction_package_hash: target.correction_package_hash.to_string(),
                receipt_id: Some(target.receipt_id.to_string()),
                checkpoint_id: Some(target.checkpoint_id.to_string()),
                committed: true,
            })
            .collect(),
        lock_published: true,
        publication_event_recorded: true,
        remaining_action: "none".to_owned(),
        recovery_command: execution_recovery_command(&staged.dry_plan),
    }
}

fn pending_target_report(
    package: &PreparedCorrectionPackage,
) -> SchemaPromotionExecutionTargetReport {
    SchemaPromotionExecutionTargetReport {
        destination: package.artifact.destination_id.to_string(),
        target: package.artifact.target.to_string(),
        correction_package_hash: package.package_hash.to_string(),
        receipt_id: None,
        checkpoint_id: None,
        committed: false,
    }
}

fn write_recovery_status(
    project_root: &Path,
    staged: &SchemaPromotionExecutionPlanArtifact,
    sequence: u64,
    phase: SchemaPromotionExecutionPhase,
    targets: Vec<SchemaPromotionExecutionTargetReport>,
    remaining_action: &str,
) -> cdf_kernel::Result<()> {
    let status = SchemaPromotionRecoveryStatus {
        version: SCHEMA_PROMOTION_RECOVERY_STATUS_VERSION,
        resource_id: staged.resource_id.to_string(),
        promotion_id: staged.promotion_id.to_string(),
        phase,
        targets,
        remaining_action: remaining_action.to_owned(),
        recovery_command: execution_recovery_command(&staged.dry_plan),
    };
    write_create_or_verify(
        &project_root.join(promotion_recovery_status_relative_path(
            &staged.promotion_id,
            sequence,
        )),
        &canonical_json_bytes(&status)?,
    )
}

pub fn load_schema_promotion_recovery_status(
    project_root: &Path,
    promotion_id: &PromotionId,
) -> cdf_kernel::Result<Option<SchemaPromotionRecoveryStatus>> {
    let directory = project_root.join(promotion_recovery_status_directory(promotion_id));
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(cdf_kernel::CdfError::data(error.to_string())),
    };
    let mut paths = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
    paths.retain(|path| path.extension().and_then(|value| value.to_str()) == Some("json"));
    paths.sort();
    let Some(path) = paths.last() else {
        return Ok(None);
    };
    let status: SchemaPromotionRecoveryStatus = read_json_file(path)?;
    if status.version != SCHEMA_PROMOTION_RECOVERY_STATUS_VERSION
        || status.promotion_id != promotion_id.as_str()
    {
        return Err(cdf_kernel::CdfError::data(
            "schema promotion recovery status conflicts with its directory authority",
        ));
    }
    Ok(Some(status))
}

fn execution_recovery_command(plan: &SchemaPromotionPlanReport) -> String {
    format!("{} --execute", plan.recovery_command)
}

fn source_package_index(
    package_root: &Path,
    excluded_correction_directories: &BTreeSet<PathBuf>,
) -> cdf_kernel::Result<BTreeMap<String, PathBuf>> {
    let mut index = BTreeMap::new();
    let mut directories = Vec::new();
    for entry in
        fs::read_dir(package_root).map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?
    {
        let entry = entry.map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
        if !entry
            .file_type()
            .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?
            .is_dir()
        {
            continue;
        }
        if !excluded_correction_directories.contains(&entry.path()) {
            directories.push(entry.path());
        }
    }
    directories.sort();
    for directory in directories {
        let reader = PackageReader::open(&directory).map_err(|error| {
            cdf_kernel::CdfError::data(format!(
                "malformed source package inventory entry {}: {error}",
                directory.display()
            ))
        })?;
        reader.verify().map_err(|error| {
            cdf_kernel::CdfError::data(format!(
                "invalid source package inventory entry {}: {error}",
                directory.display()
            ))
        })?;
        let package_hash = reader.manifest().package_hash.clone();
        if let Some(previous) = index.insert(package_hash.clone(), directory.clone()) {
            return Err(cdf_kernel::CdfError::data(format!(
                "duplicate source package hash {package_hash} at {} and {}",
                previous.display(),
                directory.display()
            )));
        }
    }
    Ok(index)
}

fn source_package_position(
    packages: &[PackageHash],
    index: &BTreeMap<String, PathBuf>,
) -> cdf_kernel::Result<SourcePosition> {
    let mut positions = BTreeMap::new();
    for package in packages {
        let path = index.get(package.as_str()).ok_or_else(|| {
            cdf_kernel::CdfError::data(format!("source package {package} disappeared"))
        })?;
        let delta = PackageReader::open(path)?.state_delta_preimage()?;
        positions.insert(package.to_string(), delta.output_position);
    }
    Ok(SourcePosition::Composite(CompositePosition {
        version: CHECKPOINT_STATE_VERSION,
        positions,
    }))
}

fn promotion_scope(resource: &CompiledResource) -> ScopeKey {
    ScopeKey::SchemaContract {
        contract: resource.descriptor().contract.clone().unwrap_or_else(|| {
            ContractRef::new(resource.descriptor().resource_id.to_string())
                .expect("resource id is a valid contract ref")
        }),
    }
}

fn take_destination<'a>(
    destinations: &'a mut [ResolvedProjectDestination],
    destination_id: &DestinationId,
    target: &TargetName,
) -> cdf_kernel::Result<&'a mut ResolvedProjectDestination> {
    destinations
        .iter_mut()
        .find(|destination| {
            destination.describe().destination_id == *destination_id
                && destination.target() == target
        })
        .ok_or_else(|| {
            cdf_kernel::CdfError::contract(format!(
                "no resolved destination runtime matches {destination_id}/{target}"
            ))
        })
}

fn correction_package_id(
    promotion_id: &PromotionId,
    target: &SchemaPromotionTargetReport,
) -> String {
    let promotion = promotion_id
        .as_str()
        .strip_prefix("sha256:")
        .unwrap_or(promotion_id.as_str());
    let target_hash = hex::encode(Sha256::digest(
        format!("{}:{}", target.destination, target.target).as_bytes(),
    ));
    format!(
        "promotion-{}-{}",
        &promotion[..promotion.len().min(16)],
        &target_hash[..12]
    )
}

fn correction_checkpoint_id(
    promotion_id: &PromotionId,
    target: &SchemaPromotionTargetReport,
) -> cdf_kernel::Result<CheckpointId> {
    CheckpointId::new(format!(
        "promotion:{}:{}",
        promotion_id,
        &hex::encode(Sha256::digest(
            format!("{}:{}", target.destination, target.target).as_bytes()
        ))[..16]
    ))
}

fn correction_target_authority_relative_path(
    promotion_id: &PromotionId,
    target: &SchemaPromotionTargetReport,
) -> String {
    let id = promotion_id
        .as_str()
        .strip_prefix("sha256:")
        .unwrap_or(promotion_id.as_str());
    let target_hash = hex::encode(Sha256::digest(
        format!("{}:{}", target.destination, target.target).as_bytes(),
    ));
    format!(".cdf/promotions/{id}/targets/{target_hash}.json")
}

pub fn promotion_plan_relative_path(promotion_id: &PromotionId) -> String {
    let id = promotion_id
        .as_str()
        .strip_prefix("sha256:")
        .unwrap_or(promotion_id.as_str());
    format!(".cdf/promotions/{id}/plan.json")
}

fn promotion_recovery_status_directory(promotion_id: &PromotionId) -> String {
    let id = promotion_id
        .as_str()
        .strip_prefix("sha256:")
        .unwrap_or(promotion_id.as_str());
    format!(".cdf/promotions/{id}/status")
}

fn promotion_recovery_status_relative_path(promotion_id: &PromotionId, sequence: u64) -> String {
    format!(
        "{}/{sequence:06}.json",
        promotion_recovery_status_directory(promotion_id)
    )
}

pub fn load_resumable_schema_promotion(
    project_root: &Path,
    resource_id: &ResourceId,
    current_lock: &LockFileAuthority,
) -> cdf_kernel::Result<Option<SchemaPromotionExecutionPlanArtifact>> {
    let root = project_root.join(".cdf/promotions");
    if !root.exists() {
        return Ok(None);
    }
    let current = parse_lock(
        std::str::from_utf8(&current_lock.bytes)
            .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?,
    )?;
    let mut matches = Vec::new();
    for entry in
        fs::read_dir(&root).map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?
    {
        let entry = entry.map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
        let path = entry.path().join("plan.json");
        if !path.is_file() {
            continue;
        }
        let artifact: SchemaPromotionExecutionPlanArtifact = read_json_file(&path)?;
        artifact.validate()?;
        if path != project_root.join(promotion_plan_relative_path(&artifact.promotion_id)) {
            return Err(cdf_kernel::CdfError::data(format!(
                "staged promotion plan {} is not stored under its canonical promotion identity",
                path.display()
            )));
        }
        if artifact.resource_id != *resource_id {
            continue;
        }
        let proposed = artifact
            .dry_plan
            .proposed_snapshot
            .as_ref()
            .ok_or_else(|| cdf_kernel::CdfError::data("staged promotion has no snapshot"))?;
        let installed = current
            .resources
            .get(resource_id.as_str())
            .and_then(|resource| resource.schema_snapshot.as_ref());
        if current_lock.sha256 == artifact.old_lock_authority.sha256
            || installed == Some(&proposed.artifact.reference())
        {
            matches.push(artifact);
        }
    }
    if matches.len() > 1 {
        return Err(cdf_kernel::CdfError::contract(format!(
            "multiple staged schema promotions match resource {resource_id}; specify or remove stale staged authority before retrying"
        )));
    }
    let artifact = matches.pop();
    if let Some(artifact) = artifact.as_ref() {
        let snapshot = artifact
            .dry_plan
            .proposed_snapshot
            .as_ref()
            .expect("validated staged promotion has snapshot");
        let stored = SchemaSnapshotStore::new(project_root).read(&snapshot.artifact.reference())?;
        if stored != snapshot.artifact {
            return Err(cdf_kernel::CdfError::data(
                "staged promotion snapshot conflicts with its content-addressed authority",
            ));
        }
    }
    Ok(artifact)
}

fn canonical_json_bytes(value: &impl Serialize) -> cdf_kernel::Result<Vec<u8>> {
    let mut value = serde_json::to_value(value)
        .map_err(|error| cdf_kernel::CdfError::internal(error.to_string()))?;
    value.sort_all_objects();
    serde_json::to_vec_pretty(&value)
        .map_err(|error| cdf_kernel::CdfError::internal(error.to_string()))
}

fn write_create_or_verify(path: &Path, bytes: &[u8]) -> cdf_kernel::Result<()> {
    match fs::read(path) {
        Ok(existing) if existing == bytes => return Ok(()),
        Ok(_) => return Err(content_addressed_conflict(path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(content_addressed_conflict(path)),
    }
    let parent = path
        .parent()
        .ok_or_else(|| cdf_kernel::CdfError::internal("promotion artifact path has no parent"))?;
    fs::create_dir_all(parent).map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
    let temporary = parent.join(format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("promotion"),
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| cdf_kernel::CdfError::internal(error.to_string()))?
            .as_nanos()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
    file.write_all(bytes)
        .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
    file.sync_all()
        .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
    match fs::hard_link(&temporary, path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temporary);
            return match fs::read(path) {
                Ok(existing) if existing == bytes => Ok(()),
                Ok(_) | Err(_) => Err(content_addressed_conflict(path)),
            };
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            return Err(cdf_kernel::CdfError::data(error.to_string()));
        }
    }
    fs::remove_file(&temporary).map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?;
    if let Ok(directory) = OpenOptions::new().read(true).open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

fn content_addressed_conflict(path: &Path) -> cdf_kernel::CdfError {
    cdf_kernel::CdfError::data(format!(
        "content-addressed promotion artifact {} conflicts with an existing unreadable or different entry",
        path.display()
    ))
}

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> cdf_kernel::Result<T> {
    serde_json::from_slice(
        &fs::read(path).map_err(|error| cdf_kernel::CdfError::data(error.to_string()))?,
    )
    .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))
}

fn now_ms() -> cdf_kernel::Result<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| cdf_kernel::CdfError::internal(error.to_string()))?
        .as_millis();
    i64::try_from(millis).map_err(|error| cdf_kernel::CdfError::internal(error.to_string()))
}

fn fail_if(
    actual: Option<SchemaPromotionExecutionFailpoint>,
    expected: SchemaPromotionExecutionFailpoint,
) -> cdf_kernel::Result<()> {
    if actual == Some(expected) {
        Err(cdf_kernel::CdfError::internal(format!(
            "schema promotion failpoint {expected:?}"
        )))
    } else {
        Ok(())
    }
}

pub fn inspect_local_promotion_availability(
    package_root: &Path,
    resource_id: &str,
) -> cdf_kernel::Result<Vec<SchemaPromotionEvidenceAvailability>> {
    Ok(LocalPackagePromotionEvidenceInventory::new(package_root)
        .inventory(resource_id)?
        .evidence
        .into_iter()
        .map(|evidence| evidence.availability)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_package_inventory_rejects_malformed_and_duplicate_hashes() {
        let temp = tempfile::tempdir().unwrap();
        let malformed = temp.path().join("malformed");
        fs::create_dir(&malformed).unwrap();
        fs::write(malformed.join("not-a-manifest"), b"invalid").unwrap();
        let error = source_package_index(temp.path(), &BTreeSet::new()).unwrap_err();
        assert!(error.message.contains("malformed source package inventory"));

        fs::remove_dir_all(&malformed).unwrap();
        let original = temp.path().join("original");
        PackageBuilder::create(&original, "source-package")
            .unwrap()
            .finish_with_status(PackageStatus::Packaged)
            .unwrap();
        let duplicate = temp.path().join("duplicate");
        copy_directory(&original, &duplicate);
        let error = source_package_index(temp.path(), &BTreeSet::new()).unwrap_err();
        assert!(error.message.contains("duplicate source package hash"));
    }

    #[test]
    fn content_addressed_promotion_artifacts_are_create_or_verify() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("artifact.json");
        write_create_or_verify(&path, b"first").unwrap();
        write_create_or_verify(&path, b"first").unwrap();
        let error = write_create_or_verify(&path, b"second").unwrap_err();
        assert!(error.message.contains("conflicts with an existing"));
        assert_eq!(fs::read(path).unwrap(), b"first");
    }

    #[test]
    fn content_addressed_promotion_artifact_directory_conflict_is_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("artifact.json");
        fs::create_dir(&path).unwrap();
        let error = write_create_or_verify(&path, b"first").unwrap_err();
        assert!(
            error
                .message
                .contains("existing unreadable or different entry")
        );
        assert!(path.is_dir());
    }

    fn copy_directory(source: &Path, destination: &Path) {
        fs::create_dir(destination).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = destination.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_directory(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }
}
