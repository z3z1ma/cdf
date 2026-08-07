use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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
    CHECKPOINT_STATE_VERSION, CanonicalArrowField, CanonicalArrowSchema, Checkpoint, CheckpointId,
    CheckpointStatus, CheckpointStore, CompositePosition, CorrectionStrategy,
    DestinationCorrectionCommitRequest, DestinationCorrectionOperation, DestinationCorrectionPlan,
    DestinationId, IdempotencyToken, LeaseOwnerId, PackageHash, PipelineId, PromotionId, Receipt,
    ResourceId, SchemaAuthorityStore, SchemaHash, SchemaHead, SchemaPromotionFence,
    SchemaPromotionLifecyclePhase, SchemaPromotionPlanState, SchemaPromotionState,
    SchemaPromotionTarget, SchemaVersion, SchemaVersionProvenance, ScopeKey, ScopeLease,
    ScopeLeaseStore, SourcePosition, StateDelta, StateSegment, TargetName,
};
use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
use cdf_package::{PackageBuilder, PackageReader};
use cdf_package_contract::{
    DestinationCommitPlanPreimage, MANIFEST_FILE, PackageStatus, StateDeltaPreimage,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::destinations::ResolvedProjectDestination;
use crate::{
    LocalPackagePromotionEvidenceInventory, PromotionEvidenceInventory,
    SchemaPromotionEvidenceAvailability, SchemaPromotionPlanReport,
    SchemaPromotionPlanningAuthority, SchemaPromotionTargetReport,
    validate_schema_promotion_plan_identity,
};

static PROMOTION_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const PROMOTION_SEGMENT_SCAN_WINDOW_BYTES: u64 = 64 * 1024 * 1024;

fn promotion_segment_scan_memory() -> cdf_kernel::Result<Arc<dyn MemoryCoordinator>> {
    Ok(Arc::new(DeterministicMemoryCoordinator::new(
        PROMOTION_SEGMENT_SCAN_WINDOW_BYTES,
        Default::default(),
    )?))
}

pub const SCHEMA_PROMOTION_EXECUTION_ARTIFACT_VERSION: u16 = 1;
pub const SCHEMA_PROMOTION_CORRECTION_PACKAGE_VERSION: u16 = 1;
pub const SCHEMA_PROMOTION_CORRECTION_TARGET_AUTHORITY_VERSION: u16 = 1;
pub const DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS: u64 = 300_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaPromotionExecutionFailpoint {
    AfterPromotionFenced,
    AfterCutoffEstablished,
    AfterCorrectionPackages,
    AfterDestinationReceipt,
    AfterTargetSettlement,
    AfterTargetSettlementIndex(usize),
    AfterHeadPublished,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaPromotionExecutionPhase {
    Fenced,
    CutoffEstablished,
    Packaged,
    DestinationSettled,
    TargetSettled,
    Published,
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
    pub current_generation: u64,
    pub published_generation: u64,
    pub plan_sha256: String,
    pub cutoff_checkpoint_count: u64,
    pub targets: Vec<SchemaPromotionExecutionTargetReport>,
    pub state_published: bool,
    pub remaining_action: String,
    pub recovery_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionExecutionPlanArtifact {
    pub version: u16,
    pub promotion_id: PromotionId,
    pub resource_id: ResourceId,
    pub authority: SchemaPromotionPlanningAuthority,
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
    Store: CheckpointStore + ScopeLeaseStore + SchemaAuthorityStore,
{
    pub project_root: &'a Path,
    pub package_root: &'a Path,
    pub resource: &'a CompiledResource,
    pub authority: &'a SchemaPromotionPlanningAuthority,
    pub dry_plan: &'a SchemaPromotionPlanReport,
    pub destinations: Vec<ResolvedProjectDestination>,
    pub execution_services: cdf_runtime::ExecutionServices,
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
    Store: CheckpointStore + ScopeLeaseStore + SchemaAuthorityStore,
{
    validate_execution_request(&request)?;
    bind_promotion_destinations(&mut request)?;
    let scope = request.authority.head.key.promotion_scope()?;
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

fn bind_promotion_destinations<Store>(
    request: &mut SchemaPromotionExecutionRequest<'_, Store>,
) -> cdf_kernel::Result<()>
where
    Store: CheckpointStore + ScopeLeaseStore + SchemaAuthorityStore,
{
    for target in &request.dry_plan.targets {
        let destination_id = DestinationId::new(target.destination.clone())?;
        let target_name = TargetName::new(target.target.clone())?;
        take_destination(&mut request.destinations, &destination_id, &target_name)?
            .bind_execution_services(request.execution_services.clone())?;
    }
    Ok(())
}

fn execute_under_lease<Store>(
    request: &mut SchemaPromotionExecutionRequest<'_, Store>,
    lease: &ScopeLease,
) -> cdf_kernel::Result<SchemaPromotionExecutionReport>
where
    Store: CheckpointStore + ScopeLeaseStore + SchemaAuthorityStore,
{
    request.settlement_store.assert_current(lease)?;
    let promotion_id = PromotionId::new(request.dry_plan.promotion_id.clone())?;
    let fence = SchemaPromotionFence::new(
        request.authority.head.key.authority_domain_id.clone(),
        promotion_id.clone(),
        lease.clone(),
    )?;
    let existing = request
        .settlement_store
        .promotion_state(&request.authority.head.key, &promotion_id)?;
    let resumed = existing.is_some();
    if let Some(state) = existing.as_ref()
        && state.phase == SchemaPromotionLifecyclePhase::Published
    {
        return execution_report_from_state(state, true);
    }
    let (staged, mut state) = stage_execution_state(request, &fence, existing)?;
    let promoting = if resumed {
        request
            .settlement_store
            .resume_promotion(&request.authority.head, &fence)?
    } else {
        SchemaAuthorityStore::head(request.settlement_store, &request.authority.head.key)?
            .ok_or_else(|| cdf_kernel::CdfError::internal("promotion fenced no schema head"))?
    };
    fail_if(
        request.failpoint,
        SchemaPromotionExecutionFailpoint::AfterPromotionFenced,
    )?;
    if state.phase == SchemaPromotionLifecyclePhase::Fenced {
        state = request
            .settlement_store
            .establish_promotion_cutoff(&promoting, &fence)?;
    }
    fail_if(
        request.failpoint,
        SchemaPromotionExecutionFailpoint::AfterCutoffEstablished,
    )?;

    let packages = build_or_load_correction_packages(request, &staged)?;
    fail_if(
        request.failpoint,
        SchemaPromotionExecutionFailpoint::AfterCorrectionPackages,
    )?;

    let mut targets = Vec::new();
    for (target_index, package) in packages.into_iter().enumerate() {
        let target_key = SchemaPromotionTarget {
            destination_id: package.artifact.destination_id.clone(),
            target: package.artifact.target.clone(),
        };
        if let Some(settlement) = state
            .target_settlements
            .iter()
            .find(|settlement| settlement.target == target_key)
        {
            let destination = take_destination(
                &mut request.destinations,
                &package.artifact.destination_id,
                &package.artifact.target,
            )?;
            let receipt = verify_stored_correction_receipt(destination, &package)?;
            targets.push(SchemaPromotionExecutionTargetReport {
                destination: package.artifact.destination_id.to_string(),
                target: package.artifact.target.to_string(),
                correction_package_hash: package.package_hash.to_string(),
                receipt_id: Some(receipt.receipt_id.to_string()),
                checkpoint_id: Some(settlement.checkpoint_id.to_string()),
                committed: true,
            });
            continue;
        }
        request.settlement_store.assert_current(lease)?;
        let destination = take_destination(
            &mut request.destinations,
            &package.artifact.destination_id,
            &package.artifact.target,
        )?;
        let receipt = settle_correction_package(destination, &package)?;
        fail_if(
            request.failpoint,
            SchemaPromotionExecutionFailpoint::AfterDestinationReceipt,
        )?;
        state = settle_promotion_checkpoint(
            request.settlement_store,
            &promoting,
            &fence,
            &target_key,
            &package,
            receipt.clone(),
        )?;
        targets.push(SchemaPromotionExecutionTargetReport {
            destination: package.artifact.destination_id.to_string(),
            target: package.artifact.target.to_string(),
            correction_package_hash: package.package_hash.to_string(),
            receipt_id: Some(receipt.receipt_id.to_string()),
            checkpoint_id: Some(package.state_delta.checkpoint_id.to_string()),
            committed: true,
        });
        fail_if(
            request.failpoint,
            SchemaPromotionExecutionFailpoint::AfterTargetSettlement,
        )?;
        fail_if(
            request.failpoint,
            SchemaPromotionExecutionFailpoint::AfterTargetSettlementIndex(target_index),
        )?;
    }

    let published =
        SchemaAuthorityStore::publish_promotion(request.settlement_store, &promoting, &fence)?;
    state = request
        .settlement_store
        .promotion_state(&request.authority.head.key, &promotion_id)?
        .ok_or_else(|| cdf_kernel::CdfError::internal("published promotion state is missing"))?;
    if published.generation
        != state.published_generation.ok_or_else(|| {
            cdf_kernel::CdfError::internal("published promotion state has no generation")
        })?
    {
        return Err(cdf_kernel::CdfError::internal(
            "published promotion head and lifecycle generations disagree",
        ));
    }
    fail_if(
        request.failpoint,
        SchemaPromotionExecutionFailpoint::AfterHeadPublished,
    )?;
    execution_report_from_state_with_targets(&state, resumed, targets)
}

fn execution_report_from_state(
    state: &SchemaPromotionState,
    resumed: bool,
) -> cdf_kernel::Result<SchemaPromotionExecutionReport> {
    let targets = state
        .target_settlements
        .iter()
        .map(|settlement| SchemaPromotionExecutionTargetReport {
            destination: settlement.target.destination_id.to_string(),
            target: settlement.target.target.to_string(),
            correction_package_hash: settlement.correction_package_hash.to_string(),
            receipt_id: Some(settlement.receipt_id.to_string()),
            checkpoint_id: Some(settlement.checkpoint_id.to_string()),
            committed: true,
        })
        .collect();
    execution_report_from_state_with_targets(state, resumed, targets)
}

fn execution_report_from_state_with_targets(
    state: &SchemaPromotionState,
    resumed: bool,
    targets: Vec<SchemaPromotionExecutionTargetReport>,
) -> cdf_kernel::Result<SchemaPromotionExecutionReport> {
    if state.phase != SchemaPromotionLifecyclePhase::Published {
        return Err(cdf_kernel::CdfError::internal(
            "completed promotion report requires published state authority",
        ));
    }
    let published_generation = state.published_generation.ok_or_else(|| {
        cdf_kernel::CdfError::internal("published schema promotion has no generation")
    })?;
    let cutoff_checkpoint_count = u64::try_from(
        state
            .cutoff
            .as_ref()
            .ok_or_else(|| {
                cdf_kernel::CdfError::internal("published schema promotion has no cutoff")
            })?
            .checkpoints
            .len(),
    )
    .map_err(|_| cdf_kernel::CdfError::internal("promotion cutoff count overflow"))?;
    let plan: SchemaPromotionPlanReport = serde_json::from_str(&state.plan.canonical_plan_json)
        .map_err(|error| {
            cdf_kernel::CdfError::internal(format!(
                "decode persisted promotion plan for report: {error}"
            ))
        })?;
    Ok(SchemaPromotionExecutionReport {
        resource_id: state.key.resource_id.to_string(),
        promotion_id: state.plan.promotion_id.to_string(),
        phase: SchemaPromotionExecutionPhase::Published,
        resumed,
        old_schema_hash: state.from_schema_hash.to_string(),
        new_schema_hash: state.to_schema_hash.to_string(),
        current_generation: state.from_generation,
        published_generation,
        plan_sha256: state.plan.plan_sha256.clone(),
        cutoff_checkpoint_count,
        targets,
        state_published: true,
        remaining_action: "none".to_owned(),
        recovery_command: execution_recovery_command(&plan),
    })
}

fn validate_execution_request<Store>(
    request: &SchemaPromotionExecutionRequest<'_, Store>,
) -> cdf_kernel::Result<()>
where
    Store: CheckpointStore + ScopeLeaseStore + SchemaAuthorityStore,
{
    if !request.dry_plan.executable || !request.dry_plan.conflicts.is_empty() {
        return Err(cdf_kernel::CdfError::contract(
            "schema promotion execution requires an executable conflict-free dry plan",
        ));
    }
    if request.dry_plan.resource_id != request.resource.descriptor().resource_id.as_str() {
        return Err(cdf_kernel::CdfError::contract(
            "schema promotion dry plan does not match the selected resource",
        ));
    }
    request.authority.validate(request.resource)?;
    validate_schema_promotion_plan_identity(request.dry_plan, request.authority)?;
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

fn stage_execution_state<Store>(
    request: &SchemaPromotionExecutionRequest<'_, Store>,
    fence: &SchemaPromotionFence,
    existing: Option<SchemaPromotionState>,
) -> cdf_kernel::Result<(SchemaPromotionExecutionPlanArtifact, SchemaPromotionState)>
where
    Store: CheckpointStore + ScopeLeaseStore + SchemaAuthorityStore,
{
    let promotion_id = PromotionId::new(request.dry_plan.promotion_id.clone())?;
    let artifact = SchemaPromotionExecutionPlanArtifact {
        version: SCHEMA_PROMOTION_EXECUTION_ARTIFACT_VERSION,
        promotion_id: promotion_id.clone(),
        resource_id: request.resource.descriptor().resource_id.clone(),
        authority: request.authority.clone(),
        dry_plan: request.dry_plan.clone(),
    };
    artifact.validate()?;
    if let Some(state) = existing {
        let persisted_plan: SchemaPromotionPlanReport =
            serde_json::from_str(&state.plan.canonical_plan_json).map_err(|error| {
                cdf_kernel::CdfError::internal(format!(
                    "decode persisted schema promotion plan: {error}"
                ))
            })?;
        if persisted_plan != *request.dry_plan
            || state.key != request.authority.head.key
            || state.from_generation != request.authority.head.generation
            || state.from_schema_hash != request.authority.head.schema_hash
        {
            return Err(cdf_kernel::CdfError::contract(
                "persisted promotion state conflicts with the exact requested dry plan",
            ));
        }
        return Ok((artifact, state));
    }

    let snapshot = request
        .dry_plan
        .proposed_snapshot
        .as_ref()
        .ok_or_else(|| cdf_kernel::CdfError::contract("promotion snapshot is missing"))?;
    let proposed_schema = snapshot.artifact.schema.to_arrow()?;
    let proposed = SchemaVersion::new(
        CanonicalArrowSchema::from_arrow(&proposed_schema)?,
        Some(request.authority.head.schema_hash.clone()),
        None,
        now_ms()?,
        SchemaVersionProvenance::Promotion {
            promotion_id: promotion_id.clone(),
        },
    )?;
    if request.dry_plan.new_schema_hash.as_deref() != Some(proposed.schema_hash.as_str()) {
        return Err(cdf_kernel::CdfError::contract(
            "promotion proposed schema does not match the dry plan's logical schema hash",
        ));
    }
    let canonical_plan_json =
        serde_json::to_string(&serde_json::to_value(request.dry_plan).map_err(|error| {
            cdf_kernel::CdfError::internal(format!("serialize promotion plan: {error}"))
        })?)
        .map_err(|error| {
            cdf_kernel::CdfError::internal(format!("encode canonical promotion plan: {error}"))
        })?;
    let mut required_targets = request
        .dry_plan
        .targets
        .iter()
        .map(|target| {
            Ok(SchemaPromotionTarget {
                destination_id: DestinationId::new(target.destination.clone())?,
                target: TargetName::new(target.target.clone())?,
            })
        })
        .collect::<cdf_kernel::Result<Vec<_>>>()?;
    required_targets.sort();
    required_targets.dedup();
    let mut residual_summary_sha256s = request
        .dry_plan
        .evidence
        .iter()
        .map(crate::internal::semantic_hash)
        .collect::<cdf_kernel::Result<Vec<_>>>()?;
    residual_summary_sha256s.sort();
    residual_summary_sha256s.dedup();
    let plan = SchemaPromotionPlanState::new(
        promotion_id,
        canonical_plan_json,
        required_targets,
        residual_summary_sha256s,
        now_ms()?,
    )?;
    let state =
        request
            .settlement_store
            .begin_promotion(&request.authority.head, proposed, plan, fence)?;
    Ok((artifact, state))
}

impl SchemaPromotionExecutionPlanArtifact {
    pub fn validate(&self) -> cdf_kernel::Result<()> {
        if self.version != SCHEMA_PROMOTION_EXECUTION_ARTIFACT_VERSION
            || self.promotion_id.as_str() != self.dry_plan.promotion_id
            || self.resource_id.as_str() != self.dry_plan.resource_id
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
        let recomputed = validate_schema_promotion_plan_identity(&self.dry_plan, &self.authority)?;
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
    Store: CheckpointStore + ScopeLeaseStore + SchemaAuthorityStore,
{
    let validation_program = promotion_validation_program(request.resource, staged)?;
    let scope = staged.authority.head.key.promotion_scope()?;
    let mut chain_parent = CheckpointStore::head(
        request.settlement_store,
        &request.pipeline_id,
        &staged.resource_id,
        &scope,
    )?;
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
        let manifest_path = package_dir.join(MANIFEST_FILE);
        let package_exists = promotion_directory_exists(&package_dir)?;
        let prepared = if package_exists && promotion_regular_file_exists(&manifest_path)? {
            let authority: SchemaPromotionCorrectionTargetAuthority =
                read_promotion_json_file(request.project_root, &authority_path)?;
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
            if package_exists {
                fs::remove_dir_all(&package_dir).map_err(|error| {
                    promotion_host_error(
                        "remove incomplete correction package",
                        &package_dir,
                        error,
                    )
                })?;
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
                &request.execution_services,
            )?;
            let authority = correction_target_authority(
                staged,
                target,
                &prepared,
                checkpoint_id,
                chain_parent.clone(),
            )?;
            write_create_or_verify(
                request.project_root,
                &authority_path,
                &canonical_json_bytes(&authority)?,
            )?;
            let hydrated: SchemaPromotionCorrectionTargetAuthority =
                read_promotion_json_file(request.project_root, &authority_path)?;
            if hydrated != authority {
                return Err(cdf_kernel::CdfError::data(
                    "persisted promotion correction target authority conflicts with built package",
                ));
            }
            validate_prepared_correction_package_authority(&prepared, staged, target, &hydrated)?;
            prepared
        };
        chain_parent = Some(expected_checkpoint_input_authority(
            request.settlement_store,
            &prepared.state_delta,
        )?);
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

fn expected_checkpoint_input_authority<Store: CheckpointStore>(
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
        Some(checkpoint) if checkpoint.delta == *expected => {
            Ok(checkpoint_input_authority(&checkpoint))
        }
        Some(_) => Err(cdf_kernel::CdfError::contract(
            "promotion checkpoint conflicts with deterministic package authority",
        )),
        None => Ok(Checkpoint {
            delta: expected.clone(),
            status: CheckpointStatus::Committed,
            receipt: None,
            is_head: true,
            created_at_ms: 0,
            committed_at_ms: Some(0),
            rewind_target_checkpoint_id: None,
        }),
    }
}

fn promotion_validation_program(
    resource: &CompiledResource,
    staged: &SchemaPromotionExecutionPlanArtifact,
) -> cdf_kernel::Result<cdf_contract::ValidationProgram> {
    let authority = staged
        .dry_plan
        .proposed_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.artifact.promotion_authority.as_ref())
        .ok_or_else(|| cdf_kernel::CdfError::data("promotion snapshot authority is missing"))?;
    let old_schema = staged.authority.version.canonical_schema.to_arrow()?;
    let policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let policy_hash = crate::internal::semantic_hash(&policy)?;
    if authority.contract_policy_hash != policy_hash {
        return Err(cdf_kernel::CdfError::contract(format!(
            "promotion policy hash {policy_hash} does not match typed plan authority {}",
            authority.contract_policy_hash
        )));
    }
    let observed = ObservedSchema::from_arrow(&old_schema);
    let program = compile_resource_validation_program(&policy, &observed, resource.descriptor())?;
    let hash = crate::internal::semantic_hash(&program)?;
    if authority
        .validation_program_hash
        .as_deref()
        .is_some_and(|expected| expected != hash)
    {
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
    let mut actual_ids = Vec::new();
    reader.for_each_receipt(&mut |receipt| {
        if &receipt.destination != destination_id || &receipt.target != target {
            return Ok(());
        }
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
        actual_ids.push(receipt.receipt_id.to_string());
        Ok(())
    })?;
    actual_ids.sort();
    let mut expected_ids = expected_receipt_ids.to_vec();
    expected_ids.sort();
    if actual_ids != expected_ids {
        return Err(cdf_kernel::CdfError::data(format!(
            "source package {} receipt ids changed before correction packaging",
            replay.state_delta.package_hash
        )));
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
    Store: CheckpointStore + ScopeLeaseStore + SchemaAuthorityStore,
{
    let strategy = target.strategy.ok_or_else(|| {
        cdf_kernel::CdfError::contract("promotion target has no selected correction strategy")
    })?;
    let destination_authority = request
        .authority
        .destinations
        .get(&target.destination)
        .ok_or_else(|| {
            cdf_kernel::CdfError::contract(format!(
                "promotion destination {:?} is absent from the planned state authority",
                target.destination
            ))
        })?;
    let capability = destination_authority
        .protocol_capabilities
        .corrections
        .strategy(strategy)
        .ok_or_else(|| {
            cdf_kernel::CdfError::contract(
                "selected correction strategy is absent from the planned destination authority",
            )
        })?;
    let snapshot = &staged
        .dry_plan
        .proposed_snapshot
        .as_ref()
        .expect("validated staged plan has snapshot")
        .artifact;
    let promotion_id = staged.promotion_id.clone();
    let old_schema_hash = SchemaHash::new(staged.dry_plan.old_schema_hash.clone())?;
    let new_schema_hash =
        SchemaHash::new(
            staged.dry_plan.new_schema_hash.clone().ok_or_else(|| {
                cdf_kernel::CdfError::contract("promotion schema hash is missing")
            })?,
        )?;
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
    let verified = reader.verify_for_consumption()?;
    let package_hash = PackageHash::new(reader.manifest().package_hash.clone())?;
    let mut receipts = BTreeSet::new();
    reader.for_each_receipt(&mut |receipt| {
        if receipt.destination.as_str() == destination && receipt.target.as_str() == target {
            receipts.insert(receipt.receipt_id.to_string());
        }
        Ok(())
    })?;
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
    let stream = reader.verified_canonical_segment_stream_with(
        &verified,
        promotion_segment_scan_memory()?,
        PROMOTION_SEGMENT_SCAN_WINDOW_BYTES,
    )?;
    for segment in stream {
        let segment = segment?;
        let mut ordinal = 0_u64;
        for batch in &segment.batches {
            let variant_index = batch
                .schema()
                .fields()
                .iter()
                .position(|field| is_framework_variant_field(field))
                .ok_or_else(|| {
                    cdf_kernel::CdfError::data(format!(
                        "source package {package_hash} segment {} has no framework residual column",
                        segment.entry.segment_id
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
                        segment.entry.segment_id.clone(),
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
    execution_services: &cdf_runtime::ExecutionServices,
) -> cdf_kernel::Result<PreparedCorrectionPackage> {
    let builder = PackageBuilder::create(
        package_dir,
        package_id,
        cdf_package::PackageBuilderResources::shared(
            execution_services.memory(),
            execution_services.spill(),
        )?,
    )?;
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
    let batch = cdf_package_contract::append_package_row_ord(vec![batch], 0)?;
    let segment =
        builder.write_segment(cdf_kernel::SegmentId::new("correction-000001")?, 0, &batch)?;
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
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
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
    let expected_new_schema_hash = staged
        .dry_plan
        .new_schema_hash
        .as_ref()
        .ok_or_else(|| cdf_kernel::CdfError::data("staged promotion schema hash is missing"))?;
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
        || artifact.new_schema_hash.as_str() != expected_new_schema_hash
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
    let stream = reader.verified_canonical_segment_stream(
        promotion_segment_scan_memory()?,
        PROMOTION_SEGMENT_SCAN_WINDOW_BYTES,
    )?;
    for segment in stream {
        let segment = segment?;
        for batch in &segment.batches {
            let batch = cdf_package_contract::strip_package_row_ord(batch.clone())?;
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
    if reader.receipt_count()? != 0 {
        return verify_stored_correction_receipt(destination, package);
    }
    let verified_package = Arc::new(reader.clone().into_verified()?);
    let runtime = destination.runtime_mut();
    let plan = runtime.prepare_correction_commit(verified_package, &request)?;
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
    let mut receipt = None;
    let count = reader.for_each_receipt(&mut |candidate| {
        if receipt.is_none() {
            receipt = Some(candidate);
        }
        Ok(())
    })?;
    let Some(receipt) = receipt else {
        return Err(cdf_kernel::CdfError::contract(
            "promotion correction package must contain exactly one canonical receipt",
        ));
    };
    if count != 1 {
        return Err(cdf_kernel::CdfError::contract(
            "promotion correction package must contain exactly one canonical receipt",
        ));
    }
    let request = DestinationCorrectionCommitRequest::new(
        package.package_hash.clone(),
        IdempotencyToken::new(package.package_hash.to_string())?,
        package.artifact.target.clone(),
        package.artifact.disposition.clone(),
        package.state_delta.segments.clone(),
        package.artifact.operations.clone(),
    )?;
    let verified_package = Arc::new(reader.clone().into_verified()?);
    let runtime = destination.runtime_mut();
    let plan = runtime.prepare_correction_commit(verified_package, &request)?;
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

fn settle_promotion_checkpoint<Store>(
    settlement_store: &Store,
    promoting: &SchemaHead,
    fence: &SchemaPromotionFence,
    target: &SchemaPromotionTarget,
    package: &PreparedCorrectionPackage,
    receipt: Receipt,
) -> cdf_kernel::Result<SchemaPromotionState>
where
    Store: CheckpointStore + ScopeLeaseStore + SchemaAuthorityStore,
{
    let existing = CheckpointStore::history(
        settlement_store,
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
            checkpoint
        }
        Some(checkpoint) if checkpoint.status == CheckpointStatus::Proposed => checkpoint,
        Some(_) => {
            return Err(cdf_kernel::CdfError::contract(
                "promotion checkpoint is terminal but not committed",
            ));
        }
        None => settlement_store.propose(package.state_delta.clone())?,
    };
    settlement_store.commit_promotion_target(
        promoting,
        fence,
        target,
        &proposed.delta.checkpoint_id,
        receipt,
    )
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
    for entry in fs::read_dir(package_root).map_err(|error| {
        package_inventory_io_error("read source package directory", package_root, error)
    })? {
        let entry = entry.map_err(|error| {
            package_inventory_io_error("read source package entry", package_root, error)
        })?;
        let entry_path = entry.path();
        if !entry
            .file_type()
            .map_err(|error| {
                package_inventory_io_error("inspect source package entry", &entry_path, error)
            })?
            .is_dir()
        {
            continue;
        }
        if !excluded_correction_directories.contains(&entry_path) {
            directories.push(entry_path);
        }
    }
    directories.sort();
    for directory in directories {
        let reader = PackageReader::open(&directory).map_err(|error| {
            package_reader_error_context(
                "malformed source package inventory entry",
                &directory,
                error,
            )
        })?;
        reader.verify().map_err(|error| {
            package_reader_error_context(
                "invalid source package inventory entry",
                &directory,
                error,
            )
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
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        positions,
    }))
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

fn canonical_json_bytes(value: &impl Serialize) -> cdf_kernel::Result<Vec<u8>> {
    let mut value = serde_json::to_value(value)
        .map_err(|error| cdf_kernel::CdfError::internal(error.to_string()))?;
    value.sort_all_objects();
    serde_json::to_vec_pretty(&value)
        .map_err(|error| cdf_kernel::CdfError::internal(error.to_string()))
}

fn write_create_or_verify(
    project_root: &Path,
    path: &Path,
    bytes: &[u8],
) -> cdf_kernel::Result<()> {
    let parent = ensure_promotion_artifact_parent(project_root, path)?;
    match read_promotion_artifact_leaf(path)? {
        Some(existing) if existing == bytes => {
            return sync_promotion_publication_directories(project_root, path);
        }
        Some(_) => return Err(content_addressed_conflict(path)),
        None => {}
    }
    let (temporary, mut file) = create_promotion_temporary(parent, path)?;
    let write_result = (|| {
        file.write_all(bytes).map_err(|error| {
            promotion_host_error("write promotion temporary", &temporary, error)
        })?;
        file.sync_all()
            .map_err(|error| promotion_host_error("sync promotion temporary", &temporary, error))
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    match fs::hard_link(&temporary, path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            fs::remove_file(&temporary).map_err(|error| {
                promotion_host_error("remove promotion temporary", &temporary, error)
            })?;
            return match read_promotion_artifact_leaf(path)? {
                Some(existing) if existing == bytes => {
                    sync_promotion_publication_directories(project_root, path)
                }
                Some(_) | None => Err(content_addressed_conflict(path)),
            };
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            return Err(promotion_host_error(
                "publish promotion artifact",
                path,
                error,
            ));
        }
    }
    fs::remove_file(&temporary)
        .map_err(|error| promotion_host_error("remove promotion temporary", &temporary, error))?;
    sync_promotion_publication_directories(project_root, path)
}

fn create_promotion_temporary(
    parent: &Path,
    path: &Path,
) -> cdf_kernel::Result<(PathBuf, fs::File)> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            cdf_kernel::CdfError::environment(format!(
                "read the host clock for a promotion temporary path: {error}; correct the system clock and retry"
            ))
        })?
        .as_nanos();
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("promotion");
    for _ in 0..100 {
        let sequence = PROMOTION_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(
            ".{name}.{}.{}.{}.tmp",
            std::process::id(),
            nanos,
            sequence
        ));
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(promotion_host_error(
                    "create promotion temporary",
                    &temporary,
                    error,
                ));
            }
        }
    }
    Err(cdf_kernel::CdfError::environment(format!(
        "create a unique promotion temporary beside {}: exhausted 100 attempts; remove stale promotion temporaries and retry",
        path.display()
    )))
}

fn ensure_promotion_artifact_parent<'a>(
    project_root: &Path,
    path: &'a Path,
) -> cdf_kernel::Result<&'a Path> {
    let relative = path.strip_prefix(project_root).map_err(|_| {
        cdf_kernel::CdfError::internal(format!(
            "promotion artifact {} escapes project root {}",
            path.display(),
            project_root.display()
        ))
    })?;
    let relative_parent = relative.parent().ok_or_else(|| {
        cdf_kernel::CdfError::internal("promotion artifact path has no project-relative parent")
    })?;
    let mut current = project_root.to_path_buf();
    for component in relative_parent.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(cdf_kernel::CdfError::internal(format!(
                "promotion artifact {} has a non-canonical project-relative path",
                path.display()
            )));
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(content_addressed_conflict(&current)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let metadata = fs::symlink_metadata(&current).map_err(|error| {
                            promotion_artifact_enumeration_error(
                                "inspect concurrently created promotion directory",
                                &current,
                                error,
                            )
                        })?;
                        if !metadata.is_dir() {
                            return Err(content_addressed_conflict(&current));
                        }
                    }
                    Err(error) => {
                        return Err(promotion_artifact_enumeration_error(
                            "create promotion artifact directory",
                            &current,
                            error,
                        ));
                    }
                }
            }
            Err(error) => {
                return Err(promotion_artifact_enumeration_error(
                    "inspect promotion artifact directory",
                    &current,
                    error,
                ));
            }
        }
    }
    Ok(path
        .parent()
        .expect("project-relative parent was validated above"))
}

fn sync_promotion_publication_directories(
    project_root: &Path,
    path: &Path,
) -> cdf_kernel::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| cdf_kernel::CdfError::internal("promotion artifact path has no parent"))?;
    crate::project_files::sync_directory_ancestry_through_root(
        parent,
        project_root,
        |directory, error| {
            promotion_host_error(
                "sync promotion artifact directory ancestry",
                directory,
                error,
            )
        },
    )
}

fn read_promotion_artifact_leaf(path: &Path) -> cdf_kernel::Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => fs::read(path)
            .map(Some)
            .map_err(|error| promotion_artifact_read_error(path, error)),
        Ok(_) => Err(content_addressed_conflict(path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(promotion_artifact_read_error(path, error)),
    }
}

fn read_promotion_json_file<T: for<'de> Deserialize<'de>>(
    project_root: &Path,
    path: &Path,
) -> cdf_kernel::Result<T> {
    validate_existing_promotion_artifact_parent(project_root, path)?;
    let bytes =
        read_promotion_artifact_leaf(path)?.ok_or_else(|| content_addressed_conflict(path))?;
    serde_json::from_slice(&bytes).map_err(|error| cdf_kernel::CdfError::data(error.to_string()))
}

fn validate_existing_promotion_artifact_parent(
    project_root: &Path,
    path: &Path,
) -> cdf_kernel::Result<()> {
    let relative = path.strip_prefix(project_root).map_err(|_| {
        cdf_kernel::CdfError::internal(format!(
            "promotion artifact {} escapes project root {}",
            path.display(),
            project_root.display()
        ))
    })?;
    let relative_parent = relative.parent().ok_or_else(|| {
        cdf_kernel::CdfError::internal("promotion artifact path has no project-relative parent")
    })?;
    let mut current = project_root.to_path_buf();
    for component in relative_parent.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(cdf_kernel::CdfError::internal(format!(
                "promotion artifact {} has a non-canonical project-relative path",
                path.display()
            )));
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(content_addressed_conflict(&current)),
            Err(error) => {
                return Err(promotion_artifact_enumeration_error(
                    "inspect promotion artifact directory",
                    &current,
                    error,
                ));
            }
        }
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
        &fs::read(path).map_err(|error| promotion_artifact_read_error(path, error))?,
    )
    .map_err(|error| cdf_kernel::CdfError::data(error.to_string()))
}

fn now_ms() -> cdf_kernel::Result<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            cdf_kernel::CdfError::environment(format!(
                "read the host clock for promotion recovery: {error}; correct the system clock and retry"
            ))
        })?
        .as_millis();
    i64::try_from(millis).map_err(|error| {
        cdf_kernel::CdfError::environment(format!(
            "represent the host clock for promotion recovery: {error}; correct the system clock and retry"
        ))
    })
}

fn promotion_artifact_read_error(path: &Path, error: std::io::Error) -> cdf_kernel::CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        content_addressed_conflict(path)
    } else if error.kind() == std::io::ErrorKind::NotFound {
        cdf_kernel::CdfError::data(format!(
            "read promotion artifact {}: {error}",
            path.display()
        ))
    } else {
        promotion_host_error("read promotion artifact", path, error)
    }
}

fn promotion_artifact_enumeration_error(
    action: &str,
    path: &Path,
    error: std::io::Error,
) -> cdf_kernel::CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::AlreadyExists
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        content_addressed_conflict(path)
    } else {
        promotion_host_error(action, path, error)
    }
}

fn promotion_host_error(action: &str, path: &Path, error: std::io::Error) -> cdf_kernel::CdfError {
    cdf_kernel::CdfError::environment(format!(
        "{action} {}: {error}; check project-path permissions, free space, device availability, and process file limits before retrying",
        path.display()
    ))
}

fn package_reader_error_context(
    action: &str,
    path: &Path,
    mut error: cdf_kernel::CdfError,
) -> cdf_kernel::CdfError {
    error.message = format!("{action} {}: {}", path.display(), error.message);
    error
}

fn package_inventory_io_error(
    action: &str,
    path: &Path,
    error: std::io::Error,
) -> cdf_kernel::CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        cdf_kernel::CdfError::data(format!("{action} {}: {error}", path.display()))
    } else {
        promotion_host_error(action, path, error)
    }
}

fn promotion_regular_file_exists(path: &Path) -> cdf_kernel::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(content_addressed_conflict(path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            validate_missing_promotion_ancestors(path)?;
            Ok(false)
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotADirectory
                || cdf_kernel::is_filesystem_loop(&error) =>
        {
            Err(content_addressed_conflict(path))
        }
        Err(error) => Err(promotion_host_error(
            "inspect promotion artifact",
            path,
            error,
        )),
    }
}

fn promotion_directory_exists(path: &Path) -> cdf_kernel::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(true),
        Ok(_) => Err(content_addressed_conflict(path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            validate_missing_promotion_ancestors(path)?;
            Ok(false)
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotADirectory
                || cdf_kernel::is_filesystem_loop(&error) =>
        {
            Err(content_addressed_conflict(path))
        }
        Err(error) => Err(promotion_host_error(
            "inspect promotion directory",
            path,
            error,
        )),
    }
}

fn validate_missing_promotion_ancestors(path: &Path) -> cdf_kernel::Result<()> {
    let mut cursor = path.parent();
    while let Some(parent) = cursor {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        match fs::metadata(parent) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(content_addressed_conflict(parent));
            }
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::symlink_metadata(parent) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(content_addressed_conflict(parent));
                    }
                    Ok(_) => return Err(content_addressed_conflict(parent)),
                    Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {
                        cursor = parent.parent();
                    }
                    Err(link_error) => {
                        return Err(promotion_artifact_enumeration_error(
                            "inspect promotion artifact ancestor",
                            parent,
                            link_error,
                        ));
                    }
                }
            }
            Err(error) => {
                return Err(promotion_artifact_enumeration_error(
                    "inspect promotion artifact ancestor",
                    parent,
                    error,
                ));
            }
        }
    }
    Ok(())
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
    use std::sync::{Arc, Barrier};

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
        PackageBuilder::create(
            &original,
            "source-package",
            cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
                .unwrap(),
        )
        .unwrap()
        .finish_with_status(PackageStatus::Packaged)
        .unwrap();
        let duplicate = temp.path().join("duplicate");
        copy_directory(&original, &duplicate);
        let error = source_package_index(temp.path(), &BTreeSet::new()).unwrap_err();
        assert!(error.message.contains("duplicate source package hash"));
    }

    #[test]
    fn source_package_inventory_wrong_shape_is_data_owned() {
        let temp = tempfile::tempdir().unwrap();
        let package_root = temp.path().join("packages");
        fs::write(&package_root, b"not a directory").unwrap();

        let error = source_package_index(&package_root, &BTreeSet::new()).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("source package directory"));
    }

    #[test]
    fn content_addressed_promotion_artifacts_are_create_or_verify() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("artifact.json");
        write_create_or_verify(temp.path(), &path, b"first").unwrap();
        write_create_or_verify(temp.path(), &path, b"first").unwrap();
        let error = write_create_or_verify(temp.path(), &path, b"second").unwrap_err();
        assert!(error.message.contains("conflicts with an existing"));
        assert_eq!(fs::read(path).unwrap(), b"first");
    }

    #[test]
    fn concurrent_identical_promotion_publishers_converge_without_temporaries() {
        let temp = tempfile::tempdir().unwrap();
        let root = Arc::new(temp.path().to_path_buf());
        let path = Arc::new(root.join("artifact.json"));
        let barrier = Arc::new(Barrier::new(8));
        let handles = (0..8)
            .map(|_| {
                let root = Arc::clone(&root);
                let path = Arc::clone(&path);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    write_create_or_verify(&root, &path, b"identical")
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        assert_eq!(fs::read(path.as_ref()).unwrap(), b"identical");
        assert_no_promotion_temporaries(root.as_ref());
    }

    #[test]
    fn concurrent_conflicting_promotion_publishers_preserve_one_complete_winner() {
        let temp = tempfile::tempdir().unwrap();
        let root = Arc::new(temp.path().to_path_buf());
        let path = Arc::new(root.join("artifact.json"));
        let barrier = Arc::new(Barrier::new(2));
        let handles = [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .map(|bytes| {
                let root = Arc::clone(&root);
                let path = Arc::clone(&path);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    write_create_or_verify(&root, &path, bytes)
                })
            })
            .collect::<Vec<_>>();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let error = results.into_iter().find_map(Result::err).unwrap();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(matches!(
            fs::read(path.as_ref()).unwrap().as_slice(),
            b"first" | b"second"
        ));
        assert_no_promotion_temporaries(root.as_ref());
    }
    #[test]
    fn content_addressed_promotion_artifact_directory_conflict_is_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("artifact.json");
        fs::create_dir(&path).unwrap();
        let error = write_create_or_verify(temp.path(), &path, b"first").unwrap_err();
        assert!(
            error
                .message
                .contains("existing unreadable or different entry")
        );
        assert!(path.is_dir());
    }

    #[test]
    fn promotion_artifact_with_regular_file_parent_is_data_owned() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("promotion");
        fs::write(&parent, b"not a directory").unwrap();
        let path = parent.join("artifact.json");

        let error = write_create_or_verify(temp.path(), &path, b"first").unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("conflicts"));
    }

    #[cfg(unix)]
    #[test]
    fn promotion_artifact_rejects_leaf_and_ancestor_symlinks() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_leaf = outside.path().join("artifact.json");
        fs::write(&outside_leaf, b"first").unwrap();
        let leaf = root.path().join("leaf.json");
        symlink(&outside_leaf, &leaf).unwrap();

        let leaf_error = write_create_or_verify(root.path(), &leaf, b"first").unwrap_err();
        assert_eq!(leaf_error.kind, cdf_kernel::ErrorKind::Data);

        let managed = root.path().join(".cdf");
        symlink(outside.path(), &managed).unwrap();
        let escaped = managed.join("promotions/promotion/artifact.json");
        let ancestor_error =
            write_create_or_verify(root.path(), &escaped, b"outside-write").unwrap_err();
        assert_eq!(ancestor_error.kind, cdf_kernel::ErrorKind::Data);
        assert!(!outside.path().join("promotions").exists());
    }

    #[cfg(unix)]
    #[test]
    fn promotion_json_reader_rejects_symlinked_authority() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        fs::write(outside.path(), b"{}").unwrap();
        let authority_dir = root.path().join(".cdf/promotions/promotion/targets");
        fs::create_dir_all(&authority_dir).unwrap();
        let authority = authority_dir.join("target.json");
        symlink(outside.path(), &authority).unwrap();

        let error =
            read_promotion_json_file::<serde_json::Value>(root.path(), &authority).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    }

    #[test]
    fn source_package_reader_context_preserves_typed_ownership() {
        let error = package_reader_error_context(
            "open source package",
            Path::new("package"),
            cdf_kernel::CdfError::rate_limited("upstream owner", Some(125)),
        );

        assert_eq!(error.kind, cdf_kernel::ErrorKind::RateLimited);
        assert_eq!(error.retry_after_ms, Some(125));
        assert!(error.message.contains("package"));
        assert!(error.message.contains("upstream owner"));
    }

    #[test]
    fn promotion_optional_metadata_rejects_wrong_shapes() {
        let temp = tempfile::tempdir().unwrap();
        let regular_file = temp.path().join("promotions");
        fs::write(&regular_file, b"not a directory").unwrap();

        let error = promotion_directory_exists(&regular_file).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
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

    fn assert_no_promotion_temporaries(directory: &Path) {
        let names = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert!(
            names
                .iter()
                .all(|name| !name.to_string_lossy().ends_with(".tmp")),
            "promotion temporary remained: {names:?}"
        );
    }
}
