use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use cdf_kernel::{
    CdfError, Checkpoint, CheckpointStatus, PackageHash, ResourceId, Result, TrustLevel,
};
use cdf_package_contract::{MANIFEST_FILE, PackageStatus};
use serde::Serialize;

use crate::{
    LocalPromotionCollectionAction, LocalPromotionCollectionAssessment, RetentionPolicy,
    RetentionRule, assess_local_promotion_collection, inspect_local_package_promotion_availability,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageCollectionRequest<'a> {
    pub package_root: &'a Path,
    pub committed_checkpoints: &'a [Checkpoint],
    pub retention_by_resource: &'a BTreeMap<ResourceId, Option<RetentionRule>>,
    pub protected_resources: &'a BTreeSet<ResourceId>,
    pub evaluated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PackageCollectionPlan {
    pub package_root: String,
    pub evaluated_at_ms: i64,
    pub artifacts: Vec<PackageCollectionArtifact>,
    pub promotion_availability: Vec<LocalPromotionCollectionAssessment>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PackageCollectionArtifact {
    pub package_path: Option<String>,
    pub package_hash: Option<String>,
    pub classification: PackageCollectionClassification,
    pub retention_reason: PackageCollectionReason,
    pub planned_action: PackageCollectionAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reclaimed_file_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reclaimed_byte_count: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageCollectionClassification {
    Retained,
    Collectible,
    Collected,
    Missing,
    Corrupt,
    Protected,
    Tombstoned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageCollectionAction {
    Retain,
    WouldCollect,
    Collected,
    RestoreRequired,
    AlreadyTombstoned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageCollectionReason {
    RetentionExpired,
    RetentionRuns,
    RetentionDuration,
    RetentionDisabled,
    RetentionUnresolved,
    ActivePromotion,
    RetentionTombstone,
    PackageReceiptWithoutCheckpoint,
    ReplayOrRecoveryArtifact,
    IncompletePackage,
    ManifestMissing,
    ManifestUnreadable,
    VerificationFailed,
    CommittedCheckpointVerificationFailed,
    ReceiptUnreadable,
    CommittedReceiptMissing,
    CheckpointLifecycleIncomplete,
    CommittedCheckpointMissingArtifact,
    InconsistentCheckpointReceipt,
    RevalidationChanged,
}

impl PackageCollectionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetentionExpired => "retention_expired",
            Self::RetentionRuns => "retention_runs",
            Self::RetentionDuration => "retention_duration",
            Self::RetentionDisabled => "retention_disabled",
            Self::RetentionUnresolved => "retention_unresolved",
            Self::ActivePromotion => "active_promotion",
            Self::RetentionTombstone => "retention_tombstone",
            Self::PackageReceiptWithoutCheckpoint => "package_receipt_without_checkpoint",
            Self::ReplayOrRecoveryArtifact => "replay_or_recovery_artifact",
            Self::IncompletePackage => "incomplete_package",
            Self::ManifestMissing => "manifest_missing",
            Self::ManifestUnreadable => "manifest_unreadable",
            Self::VerificationFailed => "verification_failed",
            Self::CommittedCheckpointVerificationFailed => {
                "committed_checkpoint_verification_failed"
            }
            Self::ReceiptUnreadable => "receipt_unreadable",
            Self::CommittedReceiptMissing => "committed_receipt_missing",
            Self::CheckpointLifecycleIncomplete => "checkpoint_lifecycle_incomplete",
            Self::CommittedCheckpointMissingArtifact => "committed_checkpoint_missing_artifact",
            Self::InconsistentCheckpointReceipt => "inconsistent_checkpoint_receipt",
            Self::RevalidationChanged => "revalidation_changed",
        }
    }
}

#[derive(Clone, Copy)]
enum RetentionVerdict {
    Retain(PackageCollectionReason),
    Collect,
    Invalid,
}

pub fn retention_rule_for_trust(
    policy: Option<&RetentionPolicy>,
    trust: &TrustLevel,
) -> Option<RetentionRule> {
    let policy = policy?;
    let selected = match trust {
        TrustLevel::Experimental => policy.experimental.as_ref(),
        TrustLevel::Governed => policy.governed.as_ref(),
        TrustLevel::Financial => policy.financial.as_ref(),
        TrustLevel::Serving => policy.serving.as_ref(),
    };
    selected.or(policy.default.as_ref()).cloned()
}

pub fn plan_package_collection(
    request: &PackageCollectionRequest<'_>,
) -> Result<PackageCollectionPlan> {
    validate_request(request)?;
    let verdicts = retention_verdicts(request)?;
    let committed_receipts = committed_receipt_ids(request);
    let mut artifacts = Vec::new();
    let mut readable_hashes = BTreeSet::new();

    if package_root_is_directory(request.package_root)? {
        for entry in sorted_child_entries(request.package_root)? {
            let path = entry.path();
            if !package_entry_is_directory(&entry)? {
                continue;
            }
            let artifact = classify_package(&path, &verdicts, &committed_receipts)?;
            if let Some(hash) = artifact.package_hash.as_deref() {
                readable_hashes.insert(hash.to_owned());
            }
            artifacts.push(artifact);
        }
    }

    for (hash, verdict) in &verdicts {
        if !readable_hashes.contains(hash.as_str()) {
            artifacts.push(PackageCollectionArtifact {
                package_path: None,
                package_hash: Some(hash.as_str().to_owned()),
                classification: PackageCollectionClassification::Missing,
                retention_reason: match verdict {
                    RetentionVerdict::Invalid => {
                        PackageCollectionReason::InconsistentCheckpointReceipt
                    }
                    RetentionVerdict::Retain(_) | RetentionVerdict::Collect => {
                        PackageCollectionReason::CommittedCheckpointMissingArtifact
                    }
                },
                planned_action: PackageCollectionAction::RestoreRequired,
                reclaimed_file_count: None,
                reclaimed_byte_count: None,
            });
        }
    }

    artifacts.sort_by(|left, right| {
        (
            left.package_path.as_deref().unwrap_or(""),
            left.package_hash.as_deref().unwrap_or(""),
            left.retention_reason.as_str(),
        )
            .cmp(&(
                right.package_path.as_deref().unwrap_or(""),
                right.package_hash.as_deref().unwrap_or(""),
                right.retention_reason.as_str(),
            ))
    });
    let promotion_availability = promotion_availability(request.package_root, &artifacts)?;
    Ok(PackageCollectionPlan {
        package_root: request.package_root.display().to_string(),
        evaluated_at_ms: request.evaluated_at_ms,
        artifacts,
        promotion_availability,
    })
}

pub fn execute_package_collection(
    request: &PackageCollectionRequest<'_>,
    expected: &PackageCollectionPlan,
) -> Result<PackageCollectionPlan> {
    if expected.package_root != request.package_root.display().to_string() {
        return Err(CdfError::contract(
            "package collection execution root differs from the planned root",
        ));
    }
    let current = plan_package_collection(request)?;
    let expected_candidates = expected
        .artifacts
        .iter()
        .filter(|artifact| artifact.planned_action == PackageCollectionAction::WouldCollect)
        .filter_map(|artifact| {
            Some((
                artifact.package_path.as_ref()?.clone(),
                artifact.package_hash.as_ref()?.clone(),
            ))
        })
        .collect::<BTreeSet<_>>();
    let mut artifacts = current.artifacts;

    for artifact in &mut artifacts {
        let Some(path) = artifact.package_path.as_ref() else {
            continue;
        };
        let Some(hash) = artifact.package_hash.as_ref() else {
            continue;
        };
        if !expected_candidates.contains(&(path.clone(), hash.clone())) {
            continue;
        }
        if artifact.planned_action != PackageCollectionAction::WouldCollect {
            artifact.retention_reason = PackageCollectionReason::RevalidationChanged;
            continue;
        }
        let manifest = cdf_package::read_manifest_header(path)?;
        let reclaimed_byte_count = manifest.identity.file_bytes;
        let report = cdf_package::tombstone_package(path)?;
        if report.package_hash != *hash {
            return Err(CdfError::data(
                "package collection tombstoned a package with changed hash authority",
            ));
        }
        artifact.classification = PackageCollectionClassification::Collected;
        artifact.planned_action = PackageCollectionAction::Collected;
        artifact.reclaimed_file_count = Some(report.removed_file_count);
        artifact.reclaimed_byte_count = Some(reclaimed_byte_count);
    }

    let promotion_availability = promotion_availability(request.package_root, &artifacts)?;
    Ok(PackageCollectionPlan {
        package_root: current.package_root,
        evaluated_at_ms: request.evaluated_at_ms,
        artifacts,
        promotion_availability,
    })
}

fn validate_request(request: &PackageCollectionRequest<'_>) -> Result<()> {
    if request.evaluated_at_ms < 0 {
        return Err(CdfError::contract(
            "package collection evaluation time must be nonnegative",
        ));
    }
    for checkpoint in request.committed_checkpoints {
        checkpoint.delta.validate()?;
    }
    Ok(())
}

fn retention_verdicts(
    request: &PackageCollectionRequest<'_>,
) -> Result<BTreeMap<PackageHash, RetentionVerdict>> {
    let mut groups = BTreeMap::<String, Vec<&Checkpoint>>::new();
    for checkpoint in request
        .committed_checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.status == CheckpointStatus::Committed)
    {
        let scope = serde_json::to_string(&checkpoint.delta.scope)
            .map_err(|error| CdfError::internal(format!("encode checkpoint scope: {error}")))?;
        groups
            .entry(format!(
                "{}\u{0}{}\u{0}{scope}",
                checkpoint.delta.pipeline_id, checkpoint.delta.resource_id
            ))
            .or_default()
            .push(checkpoint);
    }

    let mut verdicts = BTreeMap::<PackageHash, RetentionVerdict>::new();
    for checkpoints in groups.values() {
        let resource_id = &checkpoints[0].delta.resource_id;
        let rule = request.retention_by_resource.get(resource_id);
        for (ordinal, checkpoint) in checkpoints.iter().enumerate() {
            let receipt_valid = checkpoint
                .receipt
                .as_ref()
                .is_some_and(|receipt| receipt.covers_state_delta(&checkpoint.delta))
                && checkpoint.committed_at_ms.is_some();
            let verdict = if !receipt_valid {
                RetentionVerdict::Invalid
            } else if request.protected_resources.contains(resource_id) {
                RetentionVerdict::Retain(PackageCollectionReason::ActivePromotion)
            } else {
                match rule {
                    None => RetentionVerdict::Retain(PackageCollectionReason::RetentionUnresolved),
                    Some(None) => {
                        RetentionVerdict::Retain(PackageCollectionReason::RetentionDisabled)
                    }
                    Some(Some(RetentionRule::Runs(runs))) => {
                        let retained = usize::try_from(*runs).unwrap_or(usize::MAX);
                        if ordinal >= checkpoints.len().saturating_sub(retained) {
                            RetentionVerdict::Retain(PackageCollectionReason::RetentionRuns)
                        } else {
                            RetentionVerdict::Collect
                        }
                    }
                    Some(Some(RetentionRule::Duration(duration))) => {
                        let committed_at = checkpoint
                            .committed_at_ms
                            .expect("validated committed checkpoint timestamp");
                        let age = request.evaluated_at_ms.saturating_sub(committed_at);
                        let duration = i64::try_from(duration.millis()).unwrap_or(i64::MAX);
                        if age < duration {
                            RetentionVerdict::Retain(PackageCollectionReason::RetentionDuration)
                        } else {
                            RetentionVerdict::Collect
                        }
                    }
                }
            };
            merge_verdict(
                &mut verdicts,
                checkpoint.delta.package_hash.clone(),
                verdict,
            );
        }
    }
    Ok(verdicts)
}

fn merge_verdict(
    verdicts: &mut BTreeMap<PackageHash, RetentionVerdict>,
    hash: PackageHash,
    verdict: RetentionVerdict,
) {
    verdicts
        .entry(hash)
        .and_modify(|current| {
            *current = match (*current, verdict) {
                (RetentionVerdict::Invalid, _) | (_, RetentionVerdict::Invalid) => {
                    RetentionVerdict::Invalid
                }
                (RetentionVerdict::Retain(reason), _) => RetentionVerdict::Retain(reason),
                (_, RetentionVerdict::Retain(reason)) => RetentionVerdict::Retain(reason),
                (RetentionVerdict::Collect, RetentionVerdict::Collect) => RetentionVerdict::Collect,
            };
        })
        .or_insert(verdict);
}

fn committed_receipt_ids(
    request: &PackageCollectionRequest<'_>,
) -> BTreeMap<PackageHash, BTreeSet<String>> {
    let mut receipts = BTreeMap::<PackageHash, BTreeSet<String>>::new();
    for checkpoint in request
        .committed_checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.status == CheckpointStatus::Committed)
    {
        if let Some(receipt) = checkpoint.receipt.as_ref() {
            receipts
                .entry(checkpoint.delta.package_hash.clone())
                .or_default()
                .insert(receipt.receipt_id.to_string());
        }
    }
    receipts
}

fn classify_package(
    package_dir: &Path,
    verdicts: &BTreeMap<PackageHash, RetentionVerdict>,
    committed_receipts: &BTreeMap<PackageHash, BTreeSet<String>>,
) -> Result<PackageCollectionArtifact> {
    let package_path = Some(package_dir.display().to_string());
    let manifest_path = package_dir.join(MANIFEST_FILE);
    match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Ok(artifact(
                package_path,
                None,
                PackageCollectionClassification::Corrupt,
                PackageCollectionReason::ManifestUnreadable,
                PackageCollectionAction::Retain,
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(artifact(
                package_path,
                None,
                PackageCollectionClassification::Corrupt,
                PackageCollectionReason::ManifestMissing,
                PackageCollectionAction::Retain,
            ));
        }
        Err(error) => {
            return Err(package_io_error(
                "inspect package manifest",
                &manifest_path,
                error,
            ));
        }
    }

    let manifest = match cdf_package::read_manifest_header(package_dir) {
        Ok(manifest) => manifest,
        Err(error) if error.kind == cdf_kernel::ErrorKind::Data => {
            return Ok(artifact(
                package_path,
                None,
                PackageCollectionClassification::Corrupt,
                PackageCollectionReason::ManifestUnreadable,
                PackageCollectionAction::Retain,
            ));
        }
        Err(error) => return Err(error),
    };
    let package_hash = PackageHash::new(manifest.package_hash.clone()).ok();
    let hash_text = Some(manifest.package_hash.clone());
    if manifest.lifecycle.status == PackageStatus::Archived {
        return match archived_package_has_residual_identity_files(package_dir) {
            Ok(has_residual_files) => Ok(artifact(
                package_path,
                hash_text,
                PackageCollectionClassification::Tombstoned,
                PackageCollectionReason::RetentionTombstone,
                if has_residual_files {
                    PackageCollectionAction::WouldCollect
                } else {
                    PackageCollectionAction::AlreadyTombstoned
                },
            )),
            Err(error) if error.kind == cdf_kernel::ErrorKind::Data => Ok(artifact(
                package_path,
                hash_text,
                PackageCollectionClassification::Corrupt,
                PackageCollectionReason::VerificationFailed,
                PackageCollectionAction::Retain,
            )),
            Err(error) => Err(error),
        };
    }
    let verdict = package_hash.as_ref().and_then(|hash| verdicts.get(hash));
    if let Err(error) = cdf_package::verify_package(package_dir) {
        if error.kind != cdf_kernel::ErrorKind::Data {
            return Err(error);
        }
        return Ok(artifact(
            package_path,
            hash_text,
            PackageCollectionClassification::Corrupt,
            if verdict.is_some() {
                PackageCollectionReason::CommittedCheckpointVerificationFailed
            } else {
                PackageCollectionReason::VerificationFailed
            },
            PackageCollectionAction::Retain,
        ));
    }
    if verdict.is_some() && manifest.lifecycle.status != PackageStatus::Checkpointed {
        return Ok(artifact(
            package_path,
            hash_text,
            PackageCollectionClassification::Retained,
            PackageCollectionReason::CheckpointLifecycleIncomplete,
            PackageCollectionAction::Retain,
        ));
    }
    if let (Some(hash), Some(expected_receipts)) = (
        package_hash.as_ref(),
        package_hash
            .as_ref()
            .and_then(|hash| committed_receipts.get(hash)),
    ) {
        let reader = cdf_package::PackageReader::open(package_dir)?;
        let mut matched = false;
        let receipt_result = reader.for_each_receipt(&mut |receipt| {
            matched |= expected_receipts.contains(receipt.receipt_id.as_str());
            Ok(())
        });
        if let Err(error) = receipt_result {
            if error.kind != cdf_kernel::ErrorKind::Data {
                return Err(error);
            }
            return Ok(artifact(
                package_path,
                Some(hash.to_string()),
                PackageCollectionClassification::Corrupt,
                PackageCollectionReason::ReceiptUnreadable,
                PackageCollectionAction::Retain,
            ));
        }
        if !matched {
            return Ok(artifact(
                package_path,
                Some(hash.to_string()),
                PackageCollectionClassification::Corrupt,
                PackageCollectionReason::CommittedReceiptMissing,
                PackageCollectionAction::Retain,
            ));
        }
    }
    match verdict {
        Some(RetentionVerdict::Collect) => Ok(artifact(
            package_path,
            hash_text,
            PackageCollectionClassification::Collectible,
            PackageCollectionReason::RetentionExpired,
            PackageCollectionAction::WouldCollect,
        )),
        Some(RetentionVerdict::Retain(reason)) => Ok(artifact(
            package_path,
            hash_text,
            PackageCollectionClassification::Protected,
            *reason,
            PackageCollectionAction::Retain,
        )),
        Some(RetentionVerdict::Invalid) => Ok(artifact(
            package_path,
            hash_text,
            PackageCollectionClassification::Corrupt,
            PackageCollectionReason::InconsistentCheckpointReceipt,
            PackageCollectionAction::Retain,
        )),
        None => classify_uncommitted(
            package_dir,
            package_path,
            hash_text,
            manifest.lifecycle.status,
        ),
    }
}

fn archived_package_has_residual_identity_files(package_dir: &Path) -> Result<bool> {
    let mut residual = false;
    cdf_package::visit_manifest_entries(
        package_dir,
        &mut |entry| {
            let path = package_dir.join(&entry.path);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    residual = true;
                    Ok(())
                }
                Ok(_) => Err(CdfError::data(format!(
                    "archived package identity path {} is not a regular file",
                    path.display()
                ))),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(package_io_error(
                    "inspect archived package identity file",
                    &path,
                    error,
                )),
            }
        },
        &mut |_| Ok(()),
    )?;
    Ok(residual)
}

fn classify_uncommitted(
    package_dir: &Path,
    package_path: Option<String>,
    package_hash: Option<String>,
    status: PackageStatus,
) -> Result<PackageCollectionArtifact> {
    match cdf_package::PackageReader::open(package_dir).and_then(|reader| reader.receipt_count()) {
        Ok(count) if count != 0 => Ok(artifact(
            package_path,
            package_hash,
            PackageCollectionClassification::Retained,
            PackageCollectionReason::PackageReceiptWithoutCheckpoint,
            PackageCollectionAction::Retain,
        )),
        Ok(_) => {
            let reason = if matches!(
                status,
                PackageStatus::Planned | PackageStatus::Extracting | PackageStatus::Validated
            ) {
                PackageCollectionReason::IncompletePackage
            } else {
                PackageCollectionReason::ReplayOrRecoveryArtifact
            };
            Ok(artifact(
                package_path,
                package_hash,
                PackageCollectionClassification::Retained,
                reason,
                PackageCollectionAction::Retain,
            ))
        }
        Err(error) if error.kind == cdf_kernel::ErrorKind::Data => Ok(artifact(
            package_path,
            package_hash,
            PackageCollectionClassification::Corrupt,
            PackageCollectionReason::ReceiptUnreadable,
            PackageCollectionAction::Retain,
        )),
        Err(error) => Err(error),
    }
}

fn artifact(
    package_path: Option<String>,
    package_hash: Option<String>,
    classification: PackageCollectionClassification,
    retention_reason: PackageCollectionReason,
    planned_action: PackageCollectionAction,
) -> PackageCollectionArtifact {
    PackageCollectionArtifact {
        package_path,
        package_hash,
        classification,
        retention_reason,
        planned_action,
        reclaimed_file_count: None,
        reclaimed_byte_count: None,
    }
}

fn promotion_availability(
    package_root: &Path,
    artifacts: &[PackageCollectionArtifact],
) -> Result<Vec<LocalPromotionCollectionAssessment>> {
    let local = inspect_local_package_promotion_availability(package_root)?;
    let actions = artifacts
        .iter()
        .filter_map(|artifact| {
            artifact.package_path.as_ref().map(|path| {
                let action = match artifact.planned_action {
                    PackageCollectionAction::WouldCollect | PackageCollectionAction::Collected => {
                        LocalPromotionCollectionAction::WouldCollect
                    }
                    PackageCollectionAction::RestoreRequired => {
                        LocalPromotionCollectionAction::RestoreRequired
                    }
                    PackageCollectionAction::Retain
                    | PackageCollectionAction::AlreadyTombstoned => {
                        LocalPromotionCollectionAction::Retain
                    }
                };
                (path.clone(), action)
            })
        })
        .collect::<BTreeMap<_, _>>();
    Ok(assess_local_promotion_collection(local, &actions))
}

fn package_root_is_directory(root: &Path) -> Result<bool> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(CdfError::data(format!(
            "package root {} is not a regular directory",
            root.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(package_io_error("inspect package root", root, error)),
    }
}

fn sorted_child_entries(root: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| package_io_error("enumerate package root", root, error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| package_io_error("enumerate package root entry", root, error))?;
    entries.sort_by_key(fs::DirEntry::file_name);
    Ok(entries)
}

fn package_entry_is_directory(entry: &fs::DirEntry) -> Result<bool> {
    let path = entry.path();
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| package_io_error("inspect package entry", &path, error))?;
    if metadata.file_type().is_symlink() {
        return Err(CdfError::data(format!(
            "package entry {} is a symlink; remove it before collection",
            path.display()
        )));
    }
    Ok(metadata.is_dir())
}

fn package_io_error(action: &str, path: &Path, error: std::io::Error) -> CdfError {
    CdfError::environment(format!("{action} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_kernel::{
        CheckpointId, CommitCounts, CursorPosition, CursorValue, DestinationId, IdempotencyToken,
        PipelineId, Receipt, ReceiptId, SOURCE_POSITION_VERSION, SchemaHash, ScopeKey,
        SourcePosition, StateDelta, TargetName, VerifyClause, WriteDisposition,
    };
    use tempfile::TempDir;

    fn package_builder(path: &Path, package_id: String) -> cdf_package::PackageBuilder {
        cdf_package::PackageBuilder::create(
            path,
            package_id,
            cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
                .unwrap(),
        )
        .unwrap()
    }

    fn checkpoint(package_hash: &str, resource: &str, committed_at_ms: i64) -> Checkpoint {
        let delta = StateDelta {
            checkpoint_id: CheckpointId::new(format!("checkpoint-{committed_at_ms}")).unwrap(),
            pipeline_id: PipelineId::new("pipeline").unwrap(),
            resource_id: ResourceId::new(resource).unwrap(),
            scope: ScopeKey::Resource,
            state_version: cdf_kernel::CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: None,
            input_position: None,
            output_position: SourcePosition::Cursor(CursorPosition {
                version: SOURCE_POSITION_VERSION,
                field: "cursor".to_owned(),
                value: CursorValue::I64(committed_at_ms),
            }),
            output_watermark: None,
            partition_watermarks: Vec::new(),
            late_data_carryover: Vec::new(),
            source_continuation: None,
            package_hash: PackageHash::new(package_hash).unwrap(),
            schema_hash: SchemaHash::new("schema").unwrap(),
            segments: Vec::new(),
        };
        let receipt = Receipt {
            receipt_id: ReceiptId::new(format!("receipt-{committed_at_ms}")).unwrap(),
            destination: DestinationId::new("duckdb").unwrap(),
            target: TargetName::new("target").unwrap(),
            package_hash: delta.package_hash.clone(),
            segment_acks: Vec::new(),
            disposition: WriteDisposition::Append,
            idempotency_token: IdempotencyToken::new(format!("token-{committed_at_ms}")).unwrap(),
            transaction: None,
            counts: CommitCounts::default(),
            schema_hash: delta.schema_hash.clone(),
            migrations: Vec::new(),
            committed_at_ms,
            verify: VerifyClause {
                kind: "test".to_owned(),
                statement: "true".to_owned(),
                parameters: BTreeMap::new(),
            },
        };
        Checkpoint {
            delta,
            status: CheckpointStatus::Committed,
            receipt: Some(receipt),
            is_head: true,
            created_at_ms: committed_at_ms,
            committed_at_ms: Some(committed_at_ms),
            rewind_target_checkpoint_id: None,
        }
    }

    #[test]
    fn run_retention_collects_only_epochs_outside_newest_count() {
        let temp = TempDir::new().unwrap();
        let mut checkpoints = Vec::new();
        for ordinal in 1..=3 {
            let dir = temp.path().join(format!("package-{ordinal}"));
            let manifest = package_builder(&dir, format!("package-{ordinal}"))
                .finish_with_status(PackageStatus::Checkpointed)
                .unwrap();
            let checkpoint = checkpoint(&manifest.package_hash, "local.events", ordinal);
            cdf_package::append_receipt(
                &dir,
                checkpoint.receipt.clone().expect("committed receipt"),
            )
            .unwrap();
            checkpoints.push(checkpoint);
        }
        let policies = BTreeMap::from([(
            ResourceId::new("local.events").unwrap(),
            Some(RetentionRule::Runs(2)),
        )]);
        let request = PackageCollectionRequest {
            package_root: temp.path(),
            committed_checkpoints: &checkpoints,
            retention_by_resource: &policies,
            protected_resources: &BTreeSet::new(),
            evaluated_at_ms: 10,
        };
        let plan = plan_package_collection(&request).unwrap();
        assert_eq!(
            plan.artifacts
                .iter()
                .filter(|artifact| artifact.planned_action == PackageCollectionAction::WouldCollect)
                .count(),
            1
        );
    }

    #[test]
    fn duration_retention_uses_checkpoint_settlement_time() {
        let temp = TempDir::new().unwrap();
        let package_dir = temp.path().join("package-duration");
        let manifest = package_builder(&package_dir, "package-duration".to_owned())
            .finish_with_status(PackageStatus::Checkpointed)
            .unwrap();
        let checkpoint = checkpoint(&manifest.package_hash, "local.events", 100);
        cdf_package::append_receipt(
            &package_dir,
            checkpoint.receipt.clone().expect("committed receipt"),
        )
        .unwrap();
        let policies = BTreeMap::from([(
            ResourceId::new("local.events").unwrap(),
            Some(RetentionRule::Duration(crate::DurationSpec::from_millis(
                50,
            ))),
        )]);
        let request = PackageCollectionRequest {
            package_root: temp.path(),
            committed_checkpoints: std::slice::from_ref(&checkpoint),
            retention_by_resource: &policies,
            protected_resources: &BTreeSet::new(),
            evaluated_at_ms: 151,
        };
        let plan = plan_package_collection(&request).unwrap();
        assert_eq!(
            plan.artifacts[0].planned_action,
            PackageCollectionAction::WouldCollect
        );
    }

    #[test]
    fn absent_retention_policy_never_collects_settled_package() {
        let temp = TempDir::new().unwrap();
        let package_dir = temp.path().join("package-disabled");
        let manifest = package_builder(&package_dir, "package-disabled".to_owned())
            .finish_with_status(PackageStatus::Checkpointed)
            .unwrap();
        let checkpoint = checkpoint(&manifest.package_hash, "local.events", 100);
        cdf_package::append_receipt(
            &package_dir,
            checkpoint.receipt.clone().expect("committed receipt"),
        )
        .unwrap();
        let policies = BTreeMap::from([(ResourceId::new("local.events").unwrap(), None)]);
        let request = PackageCollectionRequest {
            package_root: temp.path(),
            committed_checkpoints: std::slice::from_ref(&checkpoint),
            retention_by_resource: &policies,
            protected_resources: &BTreeSet::new(),
            evaluated_at_ms: i64::MAX,
        };
        let plan = plan_package_collection(&request).unwrap();
        assert_eq!(
            plan.artifacts[0].retention_reason,
            PackageCollectionReason::RetentionDisabled
        );
        assert_eq!(
            plan.artifacts[0].planned_action,
            PackageCollectionAction::Retain
        );
    }
}
