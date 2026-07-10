use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use arrow_array::{Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{
    ContractPolicy, FieldCoercionDecision, IdentifierPolicy, decode_residual_json_v1,
    is_framework_variant_field, normalize_identifier, plan_schema_reconciliation,
    resolve_destination_type_mapping,
};
use cdf_declarative::{CompiledResource, parse_arrow_field_type};
use cdf_kernel::{
    CanonicalArrowType, CapabilitySupport, CdfError, CorrectionStrategy, PackageHash,
    RowProvenanceAddress, TypeMappingFidelity,
};
use cdf_package::PackageReader;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CdfLock, DiscoveryCoverageMode, DiscoveryManifestArtifact, LockFileAuthority,
    SCHEMA_SNAPSHOT_PROMOTION_AUTHORITY_VERSION, SchemaSnapshotArtifact,
    SchemaSnapshotPromotionAuthority, SchemaSnapshotPromotionCoercionAuthority,
    SchemaSnapshotPromotionEvidenceAvailability, SchemaSnapshotPromotionPathAuthority,
    SchemaSnapshotPromotionTargetAssociationAuthority, SchemaSnapshotSchema,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionPlanReport {
    pub resource_id: String,
    pub promotion_id: String,
    pub old_schema_hash: String,
    pub new_schema_hash: Option<String>,
    pub new_schema_snapshot_path: Option<String>,
    pub proposed_snapshot: Option<SchemaPromotionSnapshotPlan>,
    pub lock_precondition_sha256: String,
    pub evidence_extraction_program: String,
    pub evidence_inventory_complete: bool,
    pub fresh_discovery_schema_hash: Option<String>,
    pub fresh_discovery_manifest_hash: Option<String>,
    pub fresh_discovery_coverage: Option<DiscoveryCoverageMode>,
    pub fresh_discovery_content_identity: BTreeMap<String, String>,
    pub executable: bool,
    pub paths: Vec<SchemaPromotionPathReport>,
    pub evidence: Vec<SchemaPromotionEvidenceReport>,
    pub targets: Vec<SchemaPromotionTargetReport>,
    pub execution_preconditions: Vec<String>,
    pub conflicts: Vec<SchemaPromotionConflict>,
    pub writes: SchemaPromotionWrites,
    pub recovery_argv: Vec<String>,
    pub recovery_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionSnapshotPlan {
    pub schema_hash: String,
    pub path: String,
    pub artifact: SchemaSnapshotArtifact,
}

#[derive(Clone, Debug)]
pub enum SchemaPromotionFreshDiscovery {
    Available {
        snapshot: Box<SchemaSnapshotArtifact>,
        discovery_manifest: Option<Box<DiscoveryManifestArtifact>>,
        content_identity: BTreeMap<String, String>,
    },
    Unavailable {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionPathReport {
    pub path: String,
    pub source_name: String,
    pub projection_supported: bool,
    pub observed_types: Vec<String>,
    pub observed_arrow_types: Vec<CanonicalArrowType>,
    pub observed_count: u64,
    pub selected_type: Option<String>,
    pub selected_arrow_type: Option<CanonicalArrowType>,
    pub coercion_verdicts: Vec<SchemaPromotionCoercionVerdict>,
    pub output_field: String,
    pub affected_address_value_digest: String,
    pub affected_packages: Vec<String>,
    pub affected_row_examples: Vec<RowProvenanceAddress>,
    pub associations: Vec<SchemaPromotionPackageTargetAssociation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionCoercionVerdict {
    pub observed_type: CanonicalArrowType,
    pub selected_type: CanonicalArrowType,
    pub decision: FieldCoercionDecision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionEvidenceReport {
    pub artifact_location: String,
    pub package_hash: Option<String>,
    pub availability: SchemaPromotionEvidenceAvailability,
    pub resource_attribution: SchemaPromotionResourceAttribution,
    pub recorded_receipts: Vec<SchemaPromotionReceiptReport>,
    pub residual_rows: u64,
    pub residual_paths: Vec<String>,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaPromotionResourceAttribution {
    Attributed,
    Unattributed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaPromotionEvidenceAvailability {
    RetainedPackage,
    DestinationReadback,
    TombstoneOnly,
    Missing,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionReceiptReport {
    pub receipt_id: String,
    pub destination: String,
    pub target: String,
    pub verification: SchemaPromotionReceiptVerification,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaPromotionReceiptVerification {
    StructuralCoverageVerifiedDestinationVerificationPending,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionTargetReport {
    pub destination: String,
    pub target: String,
    pub destination_sheet_hash: String,
    pub residual_readback: CapabilitySupport,
    pub strategy_selection_rule: CorrectionStrategySelectionRule,
    pub strategy: Option<CorrectionStrategy>,
    pub recorded_receipt_ids: Vec<String>,
    pub affected_packages: Vec<String>,
    pub affected_paths: Vec<String>,
    pub evidence: Vec<SchemaPromotionTargetEvidenceReport>,
    pub receipt_verification: SchemaPromotionReceiptVerification,
    pub migrations: Vec<SchemaPromotionMigrationReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionTargetEvidenceReport {
    pub package_hash: String,
    pub availability: SchemaPromotionEvidenceAvailability,
    pub recorded_receipt_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionMigrationReport {
    pub path: String,
    pub output_field: String,
    pub destination_field: Option<String>,
    pub arrow_type: String,
    pub destination_type: Option<String>,
    pub fidelity: Option<TypeMappingFidelity>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorrectionStrategySelectionRule {
    OnlySafeStrategyV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionConflict {
    pub code: String,
    pub message: String,
    pub remediation: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionWrites {
    pub schema_snapshot: bool,
    pub lockfile: bool,
    pub package: bool,
    pub destination: bool,
    pub checkpoint: bool,
    pub lease: bool,
    pub ledger: bool,
}

impl SchemaPromotionWrites {
    fn none() -> Self {
        Self {
            schema_snapshot: false,
            lockfile: false,
            package: false,
            destination: false,
            checkpoint: false,
            lease: false,
            ledger: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaPromotionResidualPathFacts {
    pub path: String,
    pub observed_arrow_types: Vec<CanonicalArrowType>,
    pub observed_count: u64,
    pub address_value_digest: String,
    pub packages: Vec<String>,
    pub example_addresses: Vec<RowProvenanceAddress>,
    pub associations: Vec<SchemaPromotionPackageTargetAssociation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPromotionPackageTargetAssociation {
    pub package_hash: String,
    pub destination: String,
    pub target: String,
    pub recorded_receipt_ids: Vec<String>,
    pub availability: SchemaPromotionEvidenceAvailability,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TargetKey {
    destination: String,
    target: String,
}

#[derive(Default)]
struct ResidualPathAccumulator {
    observed_types: BTreeMap<String, CanonicalArrowType>,
    count: u64,
    package_digests: BTreeMap<String, Sha256>,
    packages: BTreeSet<String>,
    examples: BTreeSet<RowProvenanceAddress>,
    associations: BTreeMap<(String, String, String), BTreeSet<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaPromotionEvidenceInventoryFacts {
    pub paths: Vec<SchemaPromotionResidualPathFacts>,
    pub evidence: Vec<SchemaPromotionEvidenceReport>,
    pub coverage_complete: bool,
}

pub trait PromotionEvidenceInventory {
    fn inventory(
        &self,
        resource_id: &str,
    ) -> cdf_kernel::Result<SchemaPromotionEvidenceInventoryFacts>;
}

#[derive(Clone, Debug)]
pub struct LocalPackagePromotionEvidenceInventory {
    package_root: PathBuf,
}

impl LocalPackagePromotionEvidenceInventory {
    pub fn new(package_root: impl Into<PathBuf>) -> Self {
        Self {
            package_root: package_root.into(),
        }
    }
}

impl PromotionEvidenceInventory for LocalPackagePromotionEvidenceInventory {
    fn inventory(
        &self,
        resource_id: &str,
    ) -> cdf_kernel::Result<SchemaPromotionEvidenceInventoryFacts> {
        inventory_local_packages(&self.package_root, resource_id)
    }
}

/// Builds a promotion proposal from immutable package and lock evidence. This function is
/// deliberately read-only; execution lives behind the separate promotion transaction protocol.
pub fn plan_schema_promotion(
    evidence_inventory: &dyn PromotionEvidenceInventory,
    resource: &CompiledResource,
    pinned: &SchemaSnapshotArtifact,
    lock: &CdfLock,
    authority: &LockFileAuthority,
    fresh_discovery: &SchemaPromotionFreshDiscovery,
    type_overrides: &[String],
) -> cdf_kernel::Result<SchemaPromotionPlanReport> {
    let resource_id = resource.descriptor().resource_id.as_str();
    let locked_resource = lock.resources.get(resource_id).ok_or_else(|| {
        CdfError::contract(format!(
            "schema promote resource {resource_id:?} is absent from cdf.lock; run `cdf project lock --update`"
        ))
    })?;
    let locked_snapshot = locked_resource.schema_snapshot.as_ref().ok_or_else(|| {
        CdfError::contract(format!(
            "schema promote resource {resource_id:?} has no pinned snapshot in cdf.lock"
        ))
    })?;
    if locked_snapshot.schema_hash != pinned.schema_hash || pinned.resource_id != resource_id {
        return Err(CdfError::contract(format!(
            "stale schema promotion authority for {resource_id:?}: cdf.lock pins {} but the inspected snapshot is {}; run `cdf schema show {resource_id}` and retry",
            locked_snapshot.schema_hash, pinned.schema_hash
        )));
    }

    let overrides = parse_type_overrides(type_overrides)?;
    let mut conflicts = Vec::new();
    let inventory = canonicalize_inventory(evidence_inventory.inventory(resource_id)?)?;
    let evidence_inventory_complete = inventory.coverage_complete;
    let residual_paths = inventory.paths;
    let evidence = inventory.evidence;
    for item in &evidence {
        if let Some(detail) = &item.detail {
            conflicts.push(conflict(
                "evidence_unavailable",
                format!("promotion evidence at {} is unavailable: {detail}", item.artifact_location),
                "restore verified canonical residual bytes or provide a verified destination readback inventory adapter",
            ));
        }
    }
    if !evidence_inventory_complete {
        conflicts.push(conflict(
            "evidence_inventory_incomplete",
            "one or more package artifacts could not be attributed to the selected resource",
            "restore readable state-preimage authority or remove the ambiguous artifact after preserving evidence",
        ));
    }
    let target_keys = promotion_target_keys(&residual_paths, &evidence);
    let fresh_authority = fresh_discovery_types(resource_id, fresh_discovery)?;
    let fresh_discovery_schema_hash = fresh_authority.schema_hash.clone();
    let policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let locked_contract = locked_resource.contract.as_ref().ok_or_else(|| {
        CdfError::contract(format!(
            "schema promote resource {resource_id:?} has no locked contract authority"
        ))
    })?;
    let locked_policy_hash = locked_contract.policy_hash.as_ref().ok_or_else(|| {
        CdfError::contract(format!(
            "schema promote resource {resource_id:?} has no locked policy hash"
        ))
    })?;
    let derived_policy_hash = semantic_hash(&policy)?;
    if locked_policy_hash != &derived_policy_hash {
        return Err(CdfError::contract(format!(
            "schema promote cannot replace locked policy authority {locked_policy_hash} with derived trust-policy {derived_policy_hash}; refresh the exact contract compiler input or provide a typed policy planner adapter"
        )));
    }
    let mut paths = build_path_reports(
        &residual_paths,
        &overrides,
        &fresh_authority.types,
        fresh_authority.unavailable_reason.as_deref(),
        &policy.types,
        &mut conflicts,
    )?;
    paths.sort_by(|left, right| left.path.cmp(&right.path));

    for unknown in overrides
        .keys()
        .filter(|path| !paths.iter().any(|item| &item.path == *path))
    {
        conflicts.push(conflict(
            "unknown_path",
            format!(
                "--type names {unknown:?}, but no verified residual evidence contains that path"
            ),
            "choose a path listed by this command or restore the package that contains it",
        ));
    }

    let proposed_schema = proposed_schema(pinned, &paths, &mut conflicts)?;
    let proposed_snapshot = proposed_schema
        .as_ref()
        .map(|schema| {
            promotion_snapshot_plan(
                pinned,
                schema,
                &fresh_authority,
                SnapshotCompilerLineage {
                    normalizer_version: &lock.normalizer,
                    contract_policy_hash: locked_policy_hash,
                    validation_program_hash: locked_contract.validation_program_hash.as_deref(),
                },
                &paths,
            )
        })
        .transpose()?;
    let new_schema_hash = proposed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.schema_hash.clone());
    let new_schema_snapshot_path = proposed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.path.clone());
    let targets = plan_targets(
        lock,
        &target_keys,
        &paths,
        &policy,
        &evidence,
        &mut conflicts,
    )?;

    conflicts
        .sort_by(|left, right| (&left.code, &left.message).cmp(&(&right.code, &right.message)));
    conflicts.dedup();
    let executable = !paths.is_empty()
        && paths
            .iter()
            .all(|path| path.selected_arrow_type.is_some() && path.projection_supported)
        && !targets.is_empty()
        && targets.iter().all(|target| target.strategy.is_some())
        && evidence_inventory_complete
        && conflicts.is_empty();
    let recovery_argv = recovery_argv(resource_id, &paths);
    let recovery_command = recovery_argv
        .iter()
        .map(|argument| shell_quote(argument))
        .collect::<Vec<_>>()
        .join(" ");
    let execution_preconditions =
        vec!["reverify_recorded_destination_receipts_and_current_target_state".to_owned()];
    let identity = PromotionIdentity {
        version: 1,
        resource_id,
        old_schema_hash: pinned.schema_hash.to_string(),
        new_schema_hash: new_schema_hash.clone(),
        lock_precondition_sha256: authority.sha256.clone(),
        evidence_extraction_program: "residual-json-v1/all-verified-package-rows/v1".to_owned(),
        evidence_inventory_complete,
        fresh_discovery_schema_hash: fresh_discovery_schema_hash.clone(),
        fresh_discovery_manifest_hash: fresh_authority.manifest_hash.clone(),
        fresh_discovery_coverage: fresh_authority.coverage.clone(),
        fresh_discovery_content_identity: fresh_authority.content_identity.clone(),
        paths: paths
            .iter()
            .map(|path| PromotionIdentityPath {
                path: path.path.clone(),
                projection_supported: path.projection_supported,
                selected_arrow_type: path.selected_arrow_type.clone(),
                output_field: path.output_field.clone(),
                observed_count: path.observed_count,
                affected_address_value_digest: path.affected_address_value_digest.clone(),
                affected_packages: path.affected_packages.clone(),
                associations: path.associations.clone(),
            })
            .collect(),
        targets: targets
            .iter()
            .map(|target| PromotionIdentityTarget {
                destination: target.destination.clone(),
                target: target.target.clone(),
                destination_sheet_hash: target.destination_sheet_hash.clone(),
                strategy_selection_rule: target.strategy_selection_rule,
                strategy: target.strategy,
                recorded_receipt_ids: target.recorded_receipt_ids.clone(),
                affected_packages: target.affected_packages.clone(),
                affected_paths: target.affected_paths.clone(),
                evidence: target.evidence.clone(),
            })
            .collect(),
        execution_preconditions: execution_preconditions.clone(),
    };
    let promotion_id = semantic_hash(&identity)?;
    Ok(SchemaPromotionPlanReport {
        resource_id: resource_id.to_owned(),
        promotion_id,
        old_schema_hash: pinned.schema_hash.to_string(),
        new_schema_hash,
        new_schema_snapshot_path,
        proposed_snapshot,
        lock_precondition_sha256: authority.sha256.clone(),
        evidence_extraction_program: "residual-json-v1/all-verified-package-rows/v1".to_owned(),
        evidence_inventory_complete,
        fresh_discovery_schema_hash,
        fresh_discovery_manifest_hash: fresh_authority.manifest_hash,
        fresh_discovery_coverage: fresh_authority.coverage,
        fresh_discovery_content_identity: fresh_authority.content_identity,
        executable,
        paths,
        evidence,
        targets,
        execution_preconditions,
        conflicts,
        writes: SchemaPromotionWrites::none(),
        recovery_argv,
        recovery_command,
    })
}

fn promotion_target_keys(
    residual_paths: &[SchemaPromotionResidualPathFacts],
    evidence: &[SchemaPromotionEvidenceReport],
) -> BTreeSet<TargetKey> {
    residual_paths
        .iter()
        .flat_map(|path| &path.associations)
        .map(|association| TargetKey {
            destination: association.destination.clone(),
            target: association.target.clone(),
        })
        .chain(
            evidence
                .iter()
                .filter(|item| {
                    item.resource_attribution == SchemaPromotionResourceAttribution::Attributed
                })
                .flat_map(|item| &item.recorded_receipts)
                .map(|receipt| TargetKey {
                    destination: receipt.destination.clone(),
                    target: receipt.target.clone(),
                }),
        )
        .collect()
}

fn canonicalize_inventory(
    mut inventory: SchemaPromotionEvidenceInventoryFacts,
) -> cdf_kernel::Result<SchemaPromotionEvidenceInventoryFacts> {
    for path in &mut inventory.paths {
        path.observed_arrow_types.sort_by_key(|arrow_type| {
            serde_json::to_string(arrow_type).expect("canonical Arrow types serialize")
        });
        path.observed_arrow_types.dedup();
        path.packages.sort();
        path.packages.dedup();
        path.example_addresses.sort();
        path.example_addresses.dedup();
        path.example_addresses.truncate(5);
        let mut associations =
            BTreeMap::<(String, String, String), SchemaPromotionPackageTargetAssociation>::new();
        for mut association in std::mem::take(&mut path.associations) {
            if association.recorded_receipt_ids.is_empty()
                || association
                    .recorded_receipt_ids
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != association.recorded_receipt_ids.len()
            {
                return Err(CdfError::contract(format!(
                    "promotion path {:?} package/target association must carry nonempty unique receipt ids",
                    path.path
                )));
            }
            association.recorded_receipt_ids.sort();
            if !path.packages.contains(&association.package_hash) {
                return Err(CdfError::contract(format!(
                    "promotion path {:?} associates target {}/{} with package {}, but that package is absent from the path evidence",
                    path.path,
                    association.destination,
                    association.target,
                    association.package_hash
                )));
            }
            let key = (
                association.package_hash.clone(),
                association.destination.clone(),
                association.target.clone(),
            );
            if associations.insert(key, association).is_some() {
                return Err(CdfError::contract(format!(
                    "promotion path {:?} contains a duplicate package/target association",
                    path.path
                )));
            }
        }
        path.associations = associations.into_values().collect();
    }
    inventory
        .paths
        .sort_by(|left, right| left.path.cmp(&right.path));
    if let Some(duplicate) = inventory
        .paths
        .windows(2)
        .find(|pair| pair[0].path == pair[1].path)
    {
        return Err(CdfError::contract(format!(
            "promotion evidence inventory contains duplicate path facts for {:?}",
            duplicate[0].path
        )));
    }
    for item in &mut inventory.evidence {
        if item
            .recorded_receipts
            .iter()
            .map(|receipt| (&receipt.destination, &receipt.target, &receipt.receipt_id))
            .collect::<BTreeSet<_>>()
            .len()
            != item.recorded_receipts.len()
        {
            return Err(CdfError::contract(format!(
                "promotion evidence package {:?} contains duplicate receipt authority",
                item.package_hash
            )));
        }
        item.recorded_receipts.sort_by(|left, right| {
            (&left.destination, &left.target, &left.receipt_id).cmp(&(
                &right.destination,
                &right.target,
                &right.receipt_id,
            ))
        });
        item.residual_paths.sort();
        item.residual_paths.dedup();
    }
    inventory.evidence.sort_by(|left, right| {
        (&left.package_hash, &left.artifact_location)
            .cmp(&(&right.package_hash, &right.artifact_location))
    });
    let mut package_hashes = BTreeSet::new();
    let mut receipt_authority = BTreeMap::<
        (String, String, String),
        (SchemaPromotionEvidenceAvailability, Vec<String>),
    >::new();
    for item in &inventory.evidence {
        if item.resource_attribution == SchemaPromotionResourceAttribution::Attributed
            && item.package_hash.is_none()
        {
            return Err(CdfError::contract(
                "attributed promotion evidence must carry its verified package hash",
            ));
        }
        if let Some(package_hash) = &item.package_hash
            && !package_hashes.insert(package_hash)
        {
            return Err(CdfError::contract(format!(
                "promotion evidence inventory contains duplicate package hash {package_hash}"
            )));
        }
        if item.resource_attribution == SchemaPromotionResourceAttribution::Attributed {
            let package_hash = item
                .package_hash
                .as_ref()
                .expect("attributed evidence package hash checked above");
            for receipt in &item.recorded_receipts {
                let key = (
                    package_hash.clone(),
                    receipt.destination.clone(),
                    receipt.target.clone(),
                );
                let entry = receipt_authority
                    .entry(key)
                    .or_insert_with(|| (item.availability.clone(), Vec::new()));
                if entry.0 != item.availability {
                    return Err(CdfError::contract(
                        "promotion receipt authority has conflicting package availability",
                    ));
                }
                entry.1.push(receipt.receipt_id.clone());
            }
        }
    }
    for (_, receipt_ids) in receipt_authority.values_mut() {
        receipt_ids.sort();
        receipt_ids.dedup();
    }
    for path in &inventory.paths {
        for association in &path.associations {
            let key = (
                association.package_hash.clone(),
                association.destination.clone(),
                association.target.clone(),
            );
            let Some((availability, receipt_ids)) = receipt_authority.get(&key) else {
                return Err(CdfError::contract(format!(
                    "promotion path {:?} association {}/{}/{} has no matching canonical receipt evidence",
                    path.path, key.0, key.1, key.2
                )));
            };
            if availability != &association.availability
                || receipt_ids != &association.recorded_receipt_ids
            {
                return Err(CdfError::contract(format!(
                    "promotion path {:?} association {}/{}/{} does not exactly match canonical receipt ids and availability",
                    path.path, key.0, key.1, key.2
                )));
            }
        }
    }
    Ok(inventory)
}

fn parse_type_overrides(raw: &[String]) -> cdf_kernel::Result<BTreeMap<String, DataType>> {
    let mut parsed = BTreeMap::new();
    for value in raw {
        let (path, data_type) = value.rsplit_once('=').ok_or_else(|| {
            CdfError::contract(format!(
                "invalid --type {value:?}; expected JSON_POINTER=ARROW_TYPE"
            ))
        })?;
        validate_json_pointer(path)?;
        let data_type = parse_arrow_field_type(data_type)?;
        if parsed.insert(path.to_owned(), data_type).is_some() {
            return Err(CdfError::contract(format!(
                "duplicate --type override for residual path {path:?}"
            )));
        }
    }
    Ok(parsed)
}

fn inventory_local_packages(
    package_root: &Path,
    resource_id: &str,
) -> cdf_kernel::Result<SchemaPromotionEvidenceInventoryFacts> {
    if !package_root.exists() {
        return Ok(SchemaPromotionEvidenceInventoryFacts {
            paths: Vec::new(),
            evidence: vec![SchemaPromotionEvidenceReport {
                artifact_location: package_root.display().to_string(),
                package_hash: None,
                availability: SchemaPromotionEvidenceAvailability::Missing,
                resource_attribution: SchemaPromotionResourceAttribution::Unattributed,
                recorded_receipts: Vec::new(),
                residual_rows: 0,
                residual_paths: Vec::new(),
                detail: Some("package root does not exist".to_owned()),
            }],
            coverage_complete: false,
        });
    }
    let mut directories = Vec::new();
    for entry in fs::read_dir(package_root)
        .map_err(|error| CdfError::data(format!("read {}: {error}", package_root.display())))?
    {
        let entry = entry.map_err(|error| {
            CdfError::data(format!("enumerate {}: {error}", package_root.display()))
        })?;
        let file_type = entry.file_type().map_err(|error| {
            CdfError::data(format!("inspect {}: {error}", entry.path().display()))
        })?;
        if file_type.is_dir() {
            directories.push(entry.path());
        }
    }
    directories.sort_by_key(|path| {
        PackageReader::open(path)
            .map(|reader| reader.manifest().package_hash.clone())
            .unwrap_or_else(|_| format!("~{}", path.display()))
    });

    let mut path_accumulators = BTreeMap::<String, ResidualPathAccumulator>::new();
    let mut evidence = Vec::new();
    let mut coverage_complete = true;
    for package_dir in directories {
        let mut report = SchemaPromotionEvidenceReport {
            artifact_location: package_dir.display().to_string(),
            package_hash: None,
            availability: SchemaPromotionEvidenceAvailability::Missing,
            resource_attribution: SchemaPromotionResourceAttribution::Unattributed,
            recorded_receipts: Vec::new(),
            residual_rows: 0,
            residual_paths: Vec::new(),
            detail: None,
        };
        let reader = match PackageReader::open(&package_dir) {
            Ok(reader) => reader,
            Err(error) => {
                coverage_complete = false;
                report.detail = Some(error.to_string());
                evidence.push(report);
                continue;
            }
        };
        report.package_hash = Some(reader.manifest().package_hash.clone());
        let archived = reader.manifest().lifecycle.status == cdf_package::PackageStatus::Archived;
        if !archived && let Err(error) = reader.verify() {
            coverage_complete = false;
            report.detail = Some(error.to_string());
            evidence.push(report);
            continue;
        }
        let delta = match reader.state_delta_preimage() {
            Ok(delta) => delta,
            Err(error) => {
                coverage_complete = false;
                if archived {
                    report.availability = SchemaPromotionEvidenceAvailability::TombstoneOnly;
                }
                report.detail = Some(error.to_string());
                evidence.push(report);
                continue;
            }
        };
        if delta.resource_id.as_str() != resource_id {
            continue;
        }
        report.resource_attribution = SchemaPromotionResourceAttribution::Attributed;
        let package_hash = PackageHash::new(reader.manifest().package_hash.clone())?;
        let receipts = match reader.receipts() {
            Ok(receipts) => receipts,
            Err(error) => {
                coverage_complete = false;
                report.detail = Some(error.to_string());
                evidence.push(report);
                continue;
            }
        };
        for receipt in receipts {
            if receipt.package_hash != package_hash
                || !receipt
                    .covers_state_delta(&delta.clone().into_state_delta(package_hash.clone()))
            {
                report.detail = Some(format!(
                    "receipt {} does not cover package {} exactly",
                    receipt.receipt_id, package_hash
                ));
                coverage_complete = false;
                continue;
            }
            report.recorded_receipts.push(SchemaPromotionReceiptReport {
                receipt_id: receipt.receipt_id.to_string(),
                destination: receipt.destination.to_string(),
                target: receipt.target.to_string(),
                verification: SchemaPromotionReceiptVerification::StructuralCoverageVerifiedDestinationVerificationPending,
            });
        }
        report
            .recorded_receipts
            .sort_by(|left, right| left.receipt_id.cmp(&right.receipt_id));
        if archived {
            report.availability = SchemaPromotionEvidenceAvailability::TombstoneOnly;
            report.detail = Some(
                "package and receipt authority remain, but canonical residual value bytes were tombstoned"
                    .to_owned(),
            );
            evidence.push(report);
            continue;
        }
        report.availability = SchemaPromotionEvidenceAvailability::RetainedPackage;
        let mut report_paths = BTreeSet::new();
        for segment in &reader.manifest().identity.segments {
            let mut segment_ordinal = 0_u64;
            for batch in reader.read_segment(&segment.segment_id)? {
                let variant_index = batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|field| is_framework_variant_field(field));
                let Some(variant_index) = variant_index else {
                    segment_ordinal += batch.num_rows() as u64;
                    continue;
                };
                let variant = batch
                    .column(variant_index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        CdfError::data("verified framework variant field is not a StringArray")
                    })?;
                for row in 0..batch.num_rows() {
                    if variant.is_null(row) {
                        continue;
                    }
                    let residual_bytes = variant.value(row).as_bytes();
                    let decoded = decode_residual_json_v1(residual_bytes).map_err(|error| {
                        CdfError::data(format!(
                            "decode residual in package {} segment {} row {}: {error}",
                            package_hash,
                            segment.segment_id,
                            segment_ordinal + row as u64
                        ))
                    })?;
                    for field in decoded {
                        report_paths.insert(field.path.clone());
                        let arrow_type = CanonicalArrowType::from_arrow(field.array.data_type())?;
                        let address = RowProvenanceAddress::new(
                            package_hash.clone(),
                            segment.segment_id.clone(),
                            segment_ordinal + row as u64,
                        );
                        let accumulator = path_accumulators.entry(field.path.clone()).or_default();
                        let canonical_type = serde_json::to_string(&arrow_type)
                            .map_err(|error| CdfError::data(error.to_string()))?;
                        accumulator
                            .observed_types
                            .insert(canonical_type, arrow_type);
                        accumulator.count = accumulator.count.checked_add(1).ok_or_else(|| {
                            CdfError::data("schema promotion residual count overflow")
                        })?;
                        accumulator.packages.insert(package_hash.to_string());
                        for receipt in &report.recorded_receipts {
                            accumulator
                                .associations
                                .entry((
                                    package_hash.to_string(),
                                    receipt.destination.clone(),
                                    receipt.target.clone(),
                                ))
                                .or_default()
                                .insert(receipt.receipt_id.clone());
                        }
                        accumulator.examples.insert(address.clone());
                        if accumulator.examples.len() > 5 {
                            accumulator.examples.pop_last();
                        }
                        let digest_item = serde_json::to_vec(&(
                            &field.path,
                            &address,
                            hex::encode(Sha256::digest(residual_bytes)),
                        ))
                        .map_err(|error| CdfError::data(error.to_string()))?;
                        accumulator
                            .package_digests
                            .entry(package_hash.to_string())
                            .or_default()
                            .update((digest_item.len() as u64).to_be_bytes());
                        accumulator
                            .package_digests
                            .get_mut(package_hash.as_str())
                            .expect("package digest was inserted")
                            .update(digest_item);
                    }
                    report.residual_rows += 1;
                }
                segment_ordinal += batch.num_rows() as u64;
            }
        }
        report.residual_paths = report_paths.into_iter().collect();
        evidence.push(report);
    }
    evidence.sort_by(|left, right| left.artifact_location.cmp(&right.artifact_location));
    let paths = path_accumulators
        .into_iter()
        .map(|(path, accumulator)| {
            let mut digest = Sha256::new();
            for (package_hash, package_digest) in accumulator.package_digests {
                let package_digest = package_digest.finalize();
                digest.update((package_hash.len() as u64).to_be_bytes());
                digest.update(package_hash.as_bytes());
                digest.update(package_digest);
            }
            SchemaPromotionResidualPathFacts {
                path,
                observed_arrow_types: accumulator.observed_types.into_values().collect(),
                observed_count: accumulator.count,
                address_value_digest: format!("sha256:{}", hex::encode(digest.finalize())),
                packages: accumulator.packages.into_iter().collect(),
                example_addresses: accumulator.examples.into_iter().collect(),
                associations: accumulator
                    .associations
                    .into_iter()
                    .map(
                        |((package_hash, destination, target), recorded_receipt_ids)| {
                            SchemaPromotionPackageTargetAssociation {
                                package_hash,
                                destination,
                                target,
                                recorded_receipt_ids: recorded_receipt_ids.into_iter().collect(),
                                availability: SchemaPromotionEvidenceAvailability::RetainedPackage,
                            }
                        },
                    )
                    .collect(),
            }
        })
        .collect();
    Ok(SchemaPromotionEvidenceInventoryFacts {
        paths,
        evidence,
        coverage_complete,
    })
}

fn build_path_reports(
    facts: &[SchemaPromotionResidualPathFacts],
    overrides: &BTreeMap<String, DataType>,
    fresh_types: &BTreeMap<String, DataType>,
    fresh_unavailable_reason: Option<&str>,
    type_policy: &cdf_contract::TypePolicy,
    conflicts: &mut Vec<SchemaPromotionConflict>,
) -> cdf_kernel::Result<Vec<SchemaPromotionPathReport>> {
    let mut reports = Vec::new();
    for fact in facts {
        let path = &fact.path;
        let source_segments = decode_json_pointer(path)?;
        let source_name = source_segments
            .last()
            .expect("validated non-root JSON pointer has one segment")
            .clone();
        let projection_supported = source_segments.len() == 1;
        if !projection_supported {
            conflicts.push(conflict(
                "nested_projection_requires_mapping",
                format!(
                    "residual path {path:?} is nested; promotion cannot invent a flat destination field for it"
                ),
                "declare a governed nested-field projection before executing this promotion",
            ));
        }
        let mut displays = BTreeSet::new();
        let mut observed_types = Vec::new();
        for arrow_type in &fact.observed_arrow_types {
            let data_type = arrow_type.to_arrow()?;
            displays.insert(data_type.to_string());
            observed_types.push(data_type);
        }
        let explicit_type = overrides.get(path);
        let selected = explicit_type
            .cloned()
            .or_else(|| fresh_types.get(path).cloned());
        if selected.is_none() {
            let reason = fresh_unavailable_reason
                .map(|reason| format!("; fresh discovery was unavailable: {reason}"))
                .unwrap_or_else(|| {
                    "; fresh discovery did not contain one field at that original-source path"
                        .to_owned()
                });
            conflicts.push(conflict(
                "fresh_discovery_or_type_required",
                format!(
                    "residual path {path:?} has verified runtime type evidence [{}], but runtime residuals are not fresh-discovery schema authority{reason}",
                    displays.iter().cloned().collect::<Vec<_>>().join(", "),
                ),
                format!("run fresh discovery or pass `--type {path}=ARROW_TYPE`"),
            ));
        }
        let mut coercion_verdicts = Vec::new();
        if let Some(selected) = &selected {
            for observed in &observed_types {
                let report = plan_schema_reconciliation(
                    &Schema::new(vec![Field::new("value", observed.clone(), true)]),
                    &Schema::new(vec![Field::new("value", selected.clone(), true)]),
                    type_policy,
                )?;
                let decision = report.plan.fields[0].decision;
                coercion_verdicts.push(SchemaPromotionCoercionVerdict {
                    observed_type: CanonicalArrowType::from_arrow(observed)?,
                    selected_type: CanonicalArrowType::from_arrow(selected)?,
                    decision,
                });
                match decision {
                    FieldCoercionDecision::Preserved | FieldCoercionDecision::Widened => {}
                    FieldCoercionDecision::CoercedByPolicy
                    | FieldCoercionDecision::LossyAllowed
                        if explicit_type.is_some() => {}
                    FieldCoercionDecision::CoercedByPolicy
                    | FieldCoercionDecision::LossyAllowed => conflicts.push(conflict(
                        "explicit_type_required_for_coercion",
                        format!(
                            "fresh discovery proposes {selected} for residual path {path:?}, but converting from {observed} is not a lossless widening"
                        ),
                        format!("confirm the governed conversion with `--type {path}={selected}`"),
                    )),
                    FieldCoercionDecision::LossyRejected => conflicts.push(conflict(
                        "lossy_allowance_required",
                        format!(
                            "residual path {path:?} conversion from {observed} to {selected} requires the locked lossy allowance"
                        ),
                        "enable allow_lossy_mapping in the governing contract or choose a lossless type",
                    )),
                    FieldCoercionDecision::Unsupported => conflicts.push(conflict(
                        "unsupported_promotion_type",
                        format!(
                            "residual path {path:?} cannot be reconciled from {observed} to {selected}"
                        ),
                        "choose a supported explicit transform and target type",
                    )),
                    other => conflicts.push(conflict(
                        "invalid_promotion_reconciliation",
                        format!(
                            "residual path {path:?} produced unexpected reconciliation decision {other:?}"
                        ),
                        "repair the promotion schema projection before execution",
                    )),
                }
            }
        }
        let output_field = projected_output_field(path)?;
        let selected_arrow_type = selected
            .as_ref()
            .map(CanonicalArrowType::from_arrow)
            .transpose()?;
        reports.push(SchemaPromotionPathReport {
            path: path.clone(),
            source_name,
            projection_supported,
            observed_types: displays.into_iter().collect(),
            observed_arrow_types: fact.observed_arrow_types.clone(),
            observed_count: fact.observed_count,
            selected_type: selected.as_ref().map(ToString::to_string),
            selected_arrow_type,
            coercion_verdicts,
            output_field,
            affected_address_value_digest: fact.address_value_digest.clone(),
            affected_packages: fact.packages.clone(),
            affected_row_examples: fact.example_addresses.clone(),
            associations: fact.associations.clone(),
        });
    }
    Ok(reports)
}

#[derive(Debug)]
struct FreshDiscoveryAuthority {
    types: BTreeMap<String, DataType>,
    schema_hash: Option<String>,
    manifest_hash: Option<String>,
    coverage: Option<DiscoveryCoverageMode>,
    content_identity: BTreeMap<String, String>,
    unavailable_reason: Option<String>,
}

fn fresh_discovery_types(
    resource_id: &str,
    facts: &SchemaPromotionFreshDiscovery,
) -> cdf_kernel::Result<FreshDiscoveryAuthority> {
    match facts {
        SchemaPromotionFreshDiscovery::Available {
            snapshot,
            discovery_manifest,
            content_identity,
        } => {
            if snapshot.resource_id != resource_id {
                return Err(CdfError::contract(format!(
                    "fresh discovery snapshot belongs to resource {:?}, not {resource_id:?}",
                    snapshot.resource_id
                )));
            }
            if let Some(manifest) = discovery_manifest {
                manifest.validate()?;
                if manifest.resource_id != resource_id {
                    return Err(CdfError::contract(format!(
                        "fresh discovery manifest belongs to resource {:?}, not {resource_id:?}",
                        manifest.resource_id
                    )));
                }
                let snapshot_manifest = snapshot.discovery_manifest_reference()?;
                if snapshot_manifest.as_ref() != Some(&manifest.reference()) {
                    return Err(CdfError::contract(format!(
                        "fresh discovery manifest {} is not the manifest referenced by snapshot schema {}",
                        manifest.manifest_hash, snapshot.schema_hash
                    )));
                }
            } else if content_identity.is_empty() {
                return Ok(FreshDiscoveryAuthority {
                    types: BTreeMap::new(),
                    schema_hash: None,
                    manifest_hash: None,
                    coverage: None,
                    content_identity: BTreeMap::new(),
                    unavailable_reason: Some(
                        "fresh discovery supplied neither a verified discovery manifest nor content identity"
                            .to_owned(),
                    ),
                });
            }
            let schema = snapshot.schema.to_arrow()?;
            let mut types = BTreeMap::new();
            for field in schema.fields() {
                collect_fresh_field_types(field.as_ref(), &[], &mut types)?;
            }
            Ok(FreshDiscoveryAuthority {
                types,
                schema_hash: Some(snapshot.schema_hash.to_string()),
                manifest_hash: discovery_manifest
                    .as_ref()
                    .map(|manifest| manifest.manifest_hash.to_string()),
                coverage: discovery_manifest
                    .as_ref()
                    .map(|manifest| manifest.coverage.clone()),
                content_identity: content_identity.clone(),
                unavailable_reason: None,
            })
        }
        SchemaPromotionFreshDiscovery::Unavailable { reason } => Ok(FreshDiscoveryAuthority {
            types: BTreeMap::new(),
            schema_hash: None,
            manifest_hash: None,
            coverage: None,
            content_identity: BTreeMap::new(),
            unavailable_reason: Some(reason.clone()),
        }),
    }
}

fn collect_fresh_field_types(
    field: &Field,
    parent: &[String],
    output: &mut BTreeMap<String, DataType>,
) -> cdf_kernel::Result<()> {
    let source_name = field
        .metadata()
        .get("cdf:source_name")
        .cloned()
        .unwrap_or_else(|| field.name().clone());
    let mut segments = parent.to_vec();
    segments.push(source_name);
    let path = cdf_contract::residual_json_pointer(segments.iter().map(String::as_str));
    if output
        .insert(path.clone(), field.data_type().clone())
        .is_some()
    {
        return Err(CdfError::contract(format!(
            "fresh discovery contains duplicate original-source path {path:?}"
        )));
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            for child in fields {
                collect_fresh_field_types(child.as_ref(), &segments, output)?;
            }
        }
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::ListView(child)
        | DataType::LargeListView(child)
        | DataType::FixedSizeList(child, _) => {
            collect_fresh_field_types(child.as_ref(), &segments, output)?;
        }
        DataType::Map(entries, _) => {
            collect_fresh_field_types(entries.as_ref(), &segments, output)?;
        }
        _ => {}
    }
    Ok(())
}

fn proposed_schema(
    pinned: &SchemaSnapshotArtifact,
    paths: &[SchemaPromotionPathReport],
    conflicts: &mut Vec<SchemaPromotionConflict>,
) -> cdf_kernel::Result<Option<Schema>> {
    if paths
        .iter()
        .any(|path| path.selected_arrow_type.is_none() || !path.projection_supported)
    {
        return Ok(None);
    }
    let pinned_schema = pinned.schema.to_arrow()?;
    let mut fields = pinned_schema.fields().iter().cloned().collect::<Vec<_>>();
    let variant_index = fields
        .iter()
        .position(|field| is_framework_variant_field(field))
        .unwrap_or(fields.len());
    let mut names = fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<BTreeSet<_>>();
    let mut promoted = Vec::new();
    for path in paths {
        if !names.insert(path.output_field.clone()) {
            conflicts.push(conflict(
                "output_field_collision",
                format!(
                    "promoted path {:?} maps to existing field {:?}",
                    path.path, path.output_field
                ),
                "choose a non-colliding explicit schema field mapping before execution",
            ));
            continue;
        }
        let data_type = path
            .selected_arrow_type
            .as_ref()
            .expect("checked above")
            .to_arrow()?;
        promoted.push(
            Field::new(&path.output_field, data_type, true).with_metadata(
                std::collections::HashMap::from([
                    ("cdf:source_name".to_owned(), path.source_name.clone()),
                    ("cdf:promoted_path".to_owned(), path.path.clone()),
                ]),
            ),
        );
    }
    fields.splice(
        variant_index..variant_index,
        promoted.into_iter().map(Into::into),
    );
    let schema = Schema::new_with_metadata(fields, pinned_schema.metadata().clone());
    Ok(Some(schema))
}

struct SnapshotCompilerLineage<'a> {
    normalizer_version: &'a str,
    contract_policy_hash: &'a str,
    validation_program_hash: Option<&'a str>,
}

fn promotion_snapshot_plan(
    pinned: &SchemaSnapshotArtifact,
    proposed_schema: &Schema,
    fresh: &FreshDiscoveryAuthority,
    lineage: SnapshotCompilerLineage<'_>,
    paths: &[SchemaPromotionPathReport],
) -> cdf_kernel::Result<SchemaPromotionSnapshotPlan> {
    let selected_paths = paths
        .iter()
        .filter(|path| path.projection_supported)
        .filter_map(|path| {
            path.selected_arrow_type
                .as_ref()
                .map(|selected_arrow_type| SchemaSnapshotPromotionPathAuthority {
                    path: path.path.clone(),
                    source_name: path.source_name.clone(),
                    output_field: path.output_field.clone(),
                    selected_arrow_type: selected_arrow_type.clone(),
                    coercion_verdicts: path
                        .coercion_verdicts
                        .iter()
                        .map(|verdict| SchemaSnapshotPromotionCoercionAuthority {
                            observed_type: verdict.observed_type.clone(),
                            selected_type: verdict.selected_type.clone(),
                            decision: verdict.decision,
                        })
                        .collect(),
                    observed_count: path.observed_count,
                    address_value_digest: path.affected_address_value_digest.clone(),
                    packages: path.affected_packages.clone(),
                    associations: path
                        .associations
                        .iter()
                        .map(|association| {
                            SchemaSnapshotPromotionTargetAssociationAuthority {
                                package_hash: association.package_hash.clone(),
                                destination: association.destination.clone(),
                                target: association.target.clone(),
                                recorded_receipt_ids: association.recorded_receipt_ids.clone(),
                                availability: match association.availability {
                                    SchemaPromotionEvidenceAvailability::RetainedPackage => {
                                        SchemaSnapshotPromotionEvidenceAvailability::RetainedPackage
                                    }
                                    SchemaPromotionEvidenceAvailability::DestinationReadback => {
                                        SchemaSnapshotPromotionEvidenceAvailability::DestinationReadback
                                    }
                                    SchemaPromotionEvidenceAvailability::TombstoneOnly => {
                                        SchemaSnapshotPromotionEvidenceAvailability::TombstoneOnly
                                    }
                                    SchemaPromotionEvidenceAvailability::Missing => {
                                        SchemaSnapshotPromotionEvidenceAvailability::Missing
                                    }
                                },
                            }
                        })
                        .collect(),
                })
        })
        .collect::<Vec<_>>();
    let promotion_authority = SchemaSnapshotPromotionAuthority {
        version: SCHEMA_SNAPSHOT_PROMOTION_AUTHORITY_VERSION,
        resource_id: pinned.resource_id.clone(),
        old_snapshot: pinned.reference(),
        proposed_schema: SchemaSnapshotSchema::from_arrow(proposed_schema),
        fresh_discovery_schema_hash: fresh.schema_hash.clone(),
        fresh_discovery_manifest_hash: fresh.manifest_hash.clone(),
        fresh_discovery_coverage: fresh.coverage.clone(),
        fresh_discovery_content_identity: fresh.content_identity.clone(),
        normalizer_version: lineage.normalizer_version.to_owned(),
        contract_policy_hash: lineage.contract_policy_hash.to_owned(),
        validation_program_hash: lineage.validation_program_hash.map(str::to_owned),
        selected_paths,
    };
    let artifact = SchemaSnapshotArtifact::new_with_promotion(
        &cdf_kernel::ResourceId::new(pinned.resource_id.clone())?,
        proposed_schema,
        promotion_authority,
    )?;
    Ok(SchemaPromotionSnapshotPlan {
        schema_hash: artifact.schema_hash.to_string(),
        path: artifact.path.clone(),
        artifact,
    })
}

fn plan_targets(
    lock: &CdfLock,
    target_keys: &BTreeSet<TargetKey>,
    paths: &[SchemaPromotionPathReport],
    policy: &ContractPolicy,
    evidence: &[SchemaPromotionEvidenceReport],
    conflicts: &mut Vec<SchemaPromotionConflict>,
) -> cdf_kernel::Result<Vec<SchemaPromotionTargetReport>> {
    let mut reports = Vec::new();
    for key in target_keys {
        let target_paths = target_path_reports(paths, key);
        let target_associations = target_associations(&target_paths, key);
        let target_evidence = target_evidence(evidence, key);
        let retained_values_available = associations_have_values(&target_associations);
        if !retained_values_available {
            let unavailable = target_evidence
                .iter()
                .map(|item| format!("{}:{:?}", item.package_hash, item.availability))
                .collect::<Vec<_>>();
            conflicts.push(conflict(
                "target_residual_values_unavailable",
                format!(
                    "destination {:?} target {:?} lacks path-associated canonical residual values; receipt evidence is [{}]",
                    key.destination,
                    key.target,
                    unavailable.join(", ")
                ),
                "restore the exact retained packages or provide verified destination readback for this target",
            ));
        }
        let Some(locked) = lock.destinations.get(&key.destination) else {
            conflicts.push(conflict(
                "destination_sheet_missing",
                format!(
                    "verified receipt names destination {:?}, absent from cdf.lock",
                    key.destination
                ),
                "refresh the lock from the exact destination capability sheet",
            ));
            continue;
        };
        locked.protocol_capabilities.validate(&locked.sheet)?;
        let identifier_policy = IdentifierPolicy::from_destination_rules(
            &locked.sheet.identifier_rules,
        )
        .map_err(|error| {
            CdfError::contract(format!(
                "destination {:?} cannot project promoted fields: {error}",
                key.destination
            ))
        });
        let mut migrations = Vec::new();
        for path in target_paths
            .iter()
            .copied()
            .filter(|path| path.projection_supported)
        {
            let Some(selected) = path.selected_arrow_type.as_ref() else {
                continue;
            };
            let arrow_type = selected.to_arrow()?.to_string();
            let mapping = resolve_destination_type_mapping(
                &locked.sheet.type_mappings,
                &selected.to_arrow()?,
            )?;
            let destination_field = match &identifier_policy {
                Ok(policy) => match normalize_identifier(&path.source_name, policy) {
                    Ok(field) => Some(field),
                    Err(error) => {
                        conflicts.push(conflict(
                            "identifier_projection_failed",
                            format!(
                                "destination {:?} cannot project path {:?}: {error}",
                                key.destination, path.path
                            ),
                            "choose a non-colliding path mapping supported by the locked destination identifier rules",
                        ));
                        None
                    }
                },
                Err(error) => {
                    conflicts.push(conflict(
                        "identifier_projection_unsupported",
                        error.to_string(),
                        "add a shared identifier-rule adapter before promotion",
                    ));
                    None
                }
            };
            if let Some(mapping) = mapping {
                match mapping.fidelity {
                    TypeMappingFidelity::Lossless => {}
                    TypeMappingFidelity::LossyRequiresContractAllowance
                        if policy.types.allow_lossy_mapping => {}
                    TypeMappingFidelity::LossyRequiresContractAllowance => conflicts.push(conflict(
                        "lossy_destination_mapping",
                        format!(
                            "destination {:?} maps {arrow_type} to {} lossily without allow_lossy_mapping",
                            key.destination, mapping.destination_type
                        ),
                        "enable the existing allowance only if the loss is intended, or choose a lossless type",
                    )),
                    TypeMappingFidelity::Unsupported => conflicts.push(conflict(
                        "unsupported_destination_mapping",
                        format!("destination {:?} does not support promoted Arrow type {arrow_type}", key.destination),
                        "choose a supported target type or destination",
                    )),
                }
            } else {
                conflicts.push(conflict(
                    "destination_mapping_missing",
                    format!(
                        "destination {:?} has no mapping for promoted Arrow type {arrow_type}",
                        key.destination
                    ),
                    "extend and lock the destination type-mapping sheet",
                ));
            }
            migrations.push(SchemaPromotionMigrationReport {
                path: path.path.clone(),
                output_field: path.output_field.clone(),
                destination_field,
                arrow_type,
                destination_type: mapping.map(|mapping| mapping.destination_type.clone()),
                fidelity: mapping.map(|mapping| mapping.fidelity.clone()),
            });
        }
        let strategy_selection = select_correction_strategy(
            &locked.protocol_capabilities.corrections,
            retained_values_available,
        );
        let strategy = match strategy_selection {
            CorrectionStrategySelection::Selected(strategy) => Some(strategy),
            CorrectionStrategySelection::None => {
                conflicts.push(conflict(
                    "safe_correction_strategy_missing",
                    format!(
                        "destination {:?} target {:?} declares no safe correction strategy for the available evidence",
                        key.destination, key.target
                    ),
                    "restore retained residual bytes or add and verify a capability-driven correction strategy",
                ));
                None
            }
            CorrectionStrategySelection::Ambiguous(strategies) => {
                conflicts.push(conflict(
                    "safe_correction_strategy_ambiguous",
                    format!(
                        "destination {:?} target {:?} has multiple safe correction strategies: {strategies:?}",
                        key.destination, key.target
                    ),
                    "ratify and record an explicit strategy choice before execution",
                ));
                None
            }
        };
        let recorded_receipt_ids = target_associations
            .iter()
            .flat_map(|association| association.recorded_receipt_ids.iter().cloned())
            .chain(
                target_evidence
                    .iter()
                    .flat_map(|item| item.recorded_receipt_ids.iter().cloned()),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let affected_packages = target_associations
            .iter()
            .map(|association| association.package_hash.clone())
            .chain(target_evidence.iter().map(|item| item.package_hash.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let affected_paths = target_paths
            .iter()
            .map(|path| path.path.clone())
            .collect::<Vec<_>>();
        reports.push(SchemaPromotionTargetReport {
            destination: key.destination.clone(),
            target: key.target.clone(),
            destination_sheet_hash: locked.sheet_hash.clone(),
            residual_readback: locked
                .protocol_capabilities
                .corrections
                .residual_readback
                .clone(),
            strategy_selection_rule: CorrectionStrategySelectionRule::OnlySafeStrategyV1,
            strategy,
            recorded_receipt_ids,
            affected_packages,
            affected_paths,
            evidence: target_evidence,
            receipt_verification: SchemaPromotionReceiptVerification::StructuralCoverageVerifiedDestinationVerificationPending,
            migrations,
        });
    }
    Ok(reports)
}

fn target_path_reports<'a>(
    paths: &'a [SchemaPromotionPathReport],
    key: &TargetKey,
) -> Vec<&'a SchemaPromotionPathReport> {
    paths
        .iter()
        .filter(|path| {
            path.associations.iter().any(|association| {
                association.destination == key.destination && association.target == key.target
            })
        })
        .collect()
}

fn target_evidence(
    evidence: &[SchemaPromotionEvidenceReport],
    key: &TargetKey,
) -> Vec<SchemaPromotionTargetEvidenceReport> {
    evidence
        .iter()
        .filter(|item| item.resource_attribution == SchemaPromotionResourceAttribution::Attributed)
        .filter_map(|item| {
            let recorded_receipt_ids = item
                .recorded_receipts
                .iter()
                .filter(|receipt| {
                    receipt.destination == key.destination && receipt.target == key.target
                })
                .map(|receipt| receipt.receipt_id.clone())
                .collect::<Vec<_>>();
            (!recorded_receipt_ids.is_empty()).then(|| SchemaPromotionTargetEvidenceReport {
                package_hash: item
                    .package_hash
                    .clone()
                    .expect("attributed package evidence has a package hash"),
                availability: item.availability.clone(),
                recorded_receipt_ids,
            })
        })
        .collect()
}

fn target_associations<'a>(
    paths: &[&'a SchemaPromotionPathReport],
    key: &TargetKey,
) -> Vec<&'a SchemaPromotionPackageTargetAssociation> {
    paths
        .iter()
        .flat_map(|path| &path.associations)
        .filter(|association| {
            association.destination == key.destination && association.target == key.target
        })
        .collect()
}

fn associations_have_values(associations: &[&SchemaPromotionPackageTargetAssociation]) -> bool {
    !associations.is_empty()
        && associations.iter().all(|association| {
            matches!(
                association.availability,
                SchemaPromotionEvidenceAvailability::RetainedPackage
                    | SchemaPromotionEvidenceAvailability::DestinationReadback
            )
        })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CorrectionStrategySelection {
    Selected(CorrectionStrategy),
    None,
    Ambiguous(Vec<CorrectionStrategy>),
}

pub fn select_correction_strategy(
    capabilities: &cdf_kernel::DestinationCorrectionCapabilities,
    residual_values_available: bool,
) -> CorrectionStrategySelection {
    if !residual_values_available {
        return CorrectionStrategySelection::None;
    }
    let strategies = capabilities
        .strategies
        .iter()
        .map(|capability| capability.strategy)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    match strategies.as_slice() {
        [] => CorrectionStrategySelection::None,
        [strategy] => CorrectionStrategySelection::Selected(*strategy),
        _ => CorrectionStrategySelection::Ambiguous(strategies),
    }
}

fn projected_output_field(path: &str) -> cdf_kernel::Result<String> {
    let segments = decode_json_pointer(path)?;
    normalize_identifier(
        segments
            .last()
            .expect("validated non-root JSON pointer has one segment"),
        &IdentifierPolicy::default(),
    )
}

fn validate_json_pointer(path: &str) -> cdf_kernel::Result<()> {
    decode_json_pointer(path).map(drop)
}

fn decode_json_pointer(path: &str) -> cdf_kernel::Result<Vec<String>> {
    if path.is_empty() || !path.starts_with('/') {
        return Err(CdfError::contract(format!(
            "residual path {path:?} must be a non-root RFC 6901 JSON pointer"
        )));
    }
    path[1..]
        .split('/')
        .map(|segment| {
            let mut decoded = String::new();
            let mut chars = segment.chars();
            while let Some(character) = chars.next() {
                if character != '~' {
                    decoded.push(character);
                    continue;
                }
                match chars.next() {
                    Some('0') => decoded.push('~'),
                    Some('1') => decoded.push('/'),
                    _ => {
                        return Err(CdfError::contract(format!(
                            "residual path {path:?} contains an invalid RFC 6901 escape"
                        )));
                    }
                }
            }
            if decoded.is_empty() {
                return Err(CdfError::contract(format!(
                    "residual path {path:?} contains an empty path segment"
                )));
            }
            Ok(decoded)
        })
        .collect()
}

fn recovery_argv(resource_id: &str, paths: &[SchemaPromotionPathReport]) -> Vec<String> {
    let mut command = vec![
        "cdf".to_owned(),
        "schema".to_owned(),
        "promote".to_owned(),
        resource_id.to_owned(),
    ];
    for path in paths {
        if let Some(data_type) = &path.selected_type {
            command.push("--type".to_owned());
            command.push(format!("{}={}", path.path, data_type));
        }
    }
    command
}

fn shell_quote(argument: &str) -> String {
    if !argument.is_empty()
        && argument.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '_' | '-' | '.' | '/' | ':' | '=' | '~')
        })
    {
        return argument.to_owned();
    }
    format!("'{}'", argument.replace('\'', "'\\''"))
}

fn conflict(
    code: impl Into<String>,
    message: impl Into<String>,
    remediation: impl Into<String>,
) -> SchemaPromotionConflict {
    SchemaPromotionConflict {
        code: code.into(),
        message: message.into(),
        remediation: remediation.into(),
    }
}

fn semantic_hash(value: &impl Serialize) -> cdf_kernel::Result<String> {
    let value = serde_json::to_value(value)
        .map_err(|error| CdfError::internal(format!("serialize promotion identity: {error}")))?;
    let bytes = serde_json::to_vec(&value)
        .map_err(|error| CdfError::internal(format!("encode promotion identity: {error}")))?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

#[derive(Serialize)]
struct PromotionIdentity<'a> {
    version: u16,
    resource_id: &'a str,
    old_schema_hash: String,
    new_schema_hash: Option<String>,
    lock_precondition_sha256: String,
    evidence_extraction_program: String,
    evidence_inventory_complete: bool,
    fresh_discovery_schema_hash: Option<String>,
    fresh_discovery_manifest_hash: Option<String>,
    fresh_discovery_coverage: Option<DiscoveryCoverageMode>,
    fresh_discovery_content_identity: BTreeMap<String, String>,
    paths: Vec<PromotionIdentityPath>,
    targets: Vec<PromotionIdentityTarget>,
    execution_preconditions: Vec<String>,
}

#[derive(Serialize)]
struct PromotionIdentityPath {
    path: String,
    projection_supported: bool,
    selected_arrow_type: Option<CanonicalArrowType>,
    output_field: String,
    observed_count: u64,
    affected_address_value_digest: String,
    affected_packages: Vec<String>,
    associations: Vec<SchemaPromotionPackageTargetAssociation>,
}

#[derive(Serialize)]
struct PromotionIdentityTarget {
    destination: String,
    target: String,
    destination_sheet_hash: String,
    strategy_selection_rule: CorrectionStrategySelectionRule,
    strategy: Option<CorrectionStrategy>,
    recorded_receipt_ids: Vec<String>,
    affected_packages: Vec<String>,
    affected_paths: Vec<String>,
    evidence: Vec<SchemaPromotionTargetEvidenceReport>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema, TimeUnit};
    use cdf_kernel::{
        CHECKPOINT_STATE_VERSION, CapabilitySupport, CheckpointId, CommitCounts, ConcurrencyLimit,
        CorrectionStrategyCapability, DestinationCorrectionCapabilities, DestinationId,
        DestinationProtocolCapabilities, DestinationSheet, DestinationSheetArtifact, FileManifest,
        IdempotencySupport, IdempotencyToken, IdentifierRules, PipelineId, Receipt, ReceiptId,
        ResourceId, SchemaHash, ScopeKey, SegmentId, SourcePosition, TargetName,
        TransactionSupport, TypeMapping, TypeMappingFidelity, VerifyClause, WriteDisposition,
    };

    #[test]
    fn nested_json_pointer_uses_terminal_source_name_but_requires_governed_projection() {
        assert_eq!(
            projected_output_field("/Customer/Address~1Line").unwrap(),
            "address_line"
        );
        assert!(projected_output_field("/bad~2escape").is_err());

        let facts = vec![SchemaPromotionResidualPathFacts {
            path: "/Customer/Address~1Line".to_owned(),
            observed_arrow_types: vec![CanonicalArrowType::Utf8 { offset_width: 32 }],
            observed_count: 1,
            address_value_digest: "sha256:evidence".to_owned(),
            packages: vec!["sha256:package".to_owned()],
            example_addresses: Vec::new(),
            associations: Vec::new(),
        }];
        let mut conflicts = Vec::new();
        let reports = build_path_reports(
            &facts,
            &BTreeMap::from([("/Customer/Address~1Line".to_owned(), DataType::Utf8)]),
            &BTreeMap::new(),
            None,
            &ContractPolicy::default().types,
            &mut conflicts,
        )
        .unwrap();
        assert_eq!(reports[0].source_name, "Address/Line");
        assert_eq!(reports[0].output_field, "address_line");
        assert!(!reports[0].projection_supported);
        assert!(
            conflicts
                .iter()
                .any(|conflict| conflict.code == "nested_projection_requires_mapping")
        );
        let pinned = SchemaSnapshotArtifact::new(
            &ResourceId::new("source.resource").unwrap(),
            &Schema::empty(),
            BTreeMap::new(),
        )
        .unwrap();
        assert!(
            proposed_schema(&pinned, &reports, &mut conflicts)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn promoted_source_name_is_verbatim_and_reconciles_on_the_next_run() {
        let resource_id = ResourceId::new("source.resource").unwrap();
        let pinned =
            SchemaSnapshotArtifact::new(&resource_id, &Schema::empty(), BTreeMap::new()).unwrap();
        let path = SchemaPromotionPathReport {
            path: "/score".to_owned(),
            source_name: "score".to_owned(),
            projection_supported: true,
            observed_types: vec!["Int64".to_owned()],
            observed_arrow_types: vec![CanonicalArrowType::Int {
                signed: true,
                bits: 64,
            }],
            observed_count: 1,
            selected_type: Some("Int64".to_owned()),
            selected_arrow_type: Some(CanonicalArrowType::Int {
                signed: true,
                bits: 64,
            }),
            coercion_verdicts: Vec::new(),
            output_field: "score".to_owned(),
            affected_address_value_digest: "sha256:evidence".to_owned(),
            affected_packages: vec!["sha256:package".to_owned()],
            affected_row_examples: Vec::new(),
            associations: Vec::new(),
        };
        let mut conflicts = Vec::new();
        let promoted = proposed_schema(&pinned, &[path], &mut conflicts)
            .unwrap()
            .unwrap();
        assert!(conflicts.is_empty());
        let promoted_field = promoted.field(0);
        assert_eq!(
            promoted_field
                .metadata()
                .get("cdf:source_name")
                .map(String::as_str),
            Some("score")
        );
        assert_eq!(
            promoted_field
                .metadata()
                .get("cdf:promoted_path")
                .map(String::as_str),
            Some("/score")
        );

        let reconciliation = plan_schema_reconciliation(
            &Schema::new(vec![Field::new("score", DataType::Int64, true)]),
            &promoted,
            &ContractPolicy::default().types,
        )
        .unwrap();
        assert!(reconciliation.errors.is_empty());
        assert_eq!(
            reconciliation.plan.fields[0].decision,
            FieldCoercionDecision::Preserved
        );
    }

    #[test]
    fn strategy_selection_depends_only_on_declared_capabilities() {
        assert_eq!(
            select_correction_strategy(&DestinationCorrectionCapabilities::default(), true),
            CorrectionStrategySelection::None
        );
        let capabilities = DestinationCorrectionCapabilities::default().with_strategy(
            CorrectionStrategyCapability::new(
                CorrectionStrategy::CorrectionSidecar,
                TransactionSupport::AtomicTarget,
                IdempotencySupport::PackageToken,
            ),
        );
        assert_eq!(
            select_correction_strategy(&capabilities, true),
            CorrectionStrategySelection::Selected(CorrectionStrategy::CorrectionSidecar)
        );
        assert_eq!(
            select_correction_strategy(&capabilities, false),
            CorrectionStrategySelection::None
        );
        let ambiguous = capabilities
            .clone()
            .with_strategy(CorrectionStrategyCapability::new(
                CorrectionStrategy::VersionedRematerialization,
                TransactionSupport::AtomicTarget,
                IdempotencySupport::PackageToken,
            ));
        assert!(matches!(
            select_correction_strategy(&ambiguous, true),
            CorrectionStrategySelection::Ambiguous(strategies) if strategies.len() == 2
        ));
    }

    #[test]
    fn target_selection_preserves_path_package_and_availability_associations() {
        let path = |name: &str,
                    package: &str,
                    destination: &str,
                    target: &str,
                    availability: SchemaPromotionEvidenceAvailability| {
            SchemaPromotionPathReport {
                path: format!("/{name}"),
                source_name: name.to_owned(),
                projection_supported: true,
                observed_types: vec!["Int64".to_owned()],
                observed_arrow_types: vec![CanonicalArrowType::Int {
                    signed: true,
                    bits: 64,
                }],
                observed_count: 1,
                selected_type: Some("Int64".to_owned()),
                selected_arrow_type: Some(CanonicalArrowType::Int {
                    signed: true,
                    bits: 64,
                }),
                coercion_verdicts: Vec::new(),
                output_field: name.to_owned(),
                affected_address_value_digest: format!("sha256:{name}"),
                affected_packages: vec![package.to_owned()],
                affected_row_examples: Vec::new(),
                associations: vec![SchemaPromotionPackageTargetAssociation {
                    package_hash: package.to_owned(),
                    destination: destination.to_owned(),
                    target: target.to_owned(),
                    recorded_receipt_ids: vec![format!("receipt-{name}")],
                    availability,
                }],
            }
        };
        let paths = vec![
            path(
                "a",
                "sha256:package-a",
                "warehouse",
                "target-a",
                SchemaPromotionEvidenceAvailability::RetainedPackage,
            ),
            path(
                "b",
                "sha256:package-b",
                "warehouse",
                "target-b",
                SchemaPromotionEvidenceAvailability::TombstoneOnly,
            ),
        ];
        let target_a = TargetKey {
            destination: "warehouse".to_owned(),
            target: "target-a".to_owned(),
        };
        let target_b = TargetKey {
            destination: "warehouse".to_owned(),
            target: "target-b".to_owned(),
        };
        let a_paths = target_path_reports(&paths, &target_a);
        let b_paths = target_path_reports(&paths, &target_b);
        assert_eq!(
            a_paths
                .iter()
                .map(|path| path.path.as_str())
                .collect::<Vec<_>>(),
            ["/a"]
        );
        assert_eq!(
            b_paths
                .iter()
                .map(|path| path.path.as_str())
                .collect::<Vec<_>>(),
            ["/b"]
        );
        let a_associations = target_associations(&a_paths, &target_a);
        let b_associations = target_associations(&b_paths, &target_b);
        assert_eq!(a_associations[0].package_hash, "sha256:package-a");
        assert_eq!(b_associations[0].package_hash, "sha256:package-b");
        assert!(associations_have_values(&a_associations));
        assert!(!associations_have_values(&b_associations));
    }

    #[test]
    fn recovery_command_carries_exact_selected_types() {
        let argv = recovery_argv(
            "source.resource",
            &[SchemaPromotionPathReport {
                path: "/price".to_owned(),
                source_name: "price".to_owned(),
                projection_supported: true,
                observed_types: vec!["Int32".to_owned()],
                observed_arrow_types: vec![CanonicalArrowType::Int {
                    signed: true,
                    bits: 32,
                }],
                observed_count: 1,
                selected_type: Some("Int64".to_owned()),
                selected_arrow_type: Some(CanonicalArrowType::Int {
                    signed: true,
                    bits: 64,
                }),
                coercion_verdicts: Vec::new(),
                output_field: "price".to_owned(),
                affected_address_value_digest: "sha256:evidence".to_owned(),
                affected_packages: Vec::new(),
                affected_row_examples: Vec::new(),
                associations: Vec::new(),
            }],
        );
        assert_eq!(argv[5], "/price=Int64");
        assert_eq!(
            argv.iter()
                .map(|value| shell_quote(value))
                .collect::<Vec<_>>()
                .join(" "),
            "cdf schema promote source.resource --type /price=Int64"
        );
    }

    #[test]
    fn type_override_and_shell_recovery_round_trip_adversarial_paths() {
        let path = "/a=b/O'Brien/$value/`literal`/~0~1";
        let parsed = parse_type_overrides(&[format!("{path}=timestamp(us, UTC)")]).unwrap();
        assert_eq!(
            parsed.get(path),
            Some(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
        );
        let encoded = shell_quote(&format!("{path}=timestamp(us, UTC)"));
        assert_eq!(
            encoded,
            "'/a=b/O'\\''Brien/$value/`literal`/~0~1=timestamp(us, UTC)'"
        );
    }

    #[test]
    fn path_helper_classifies_lossy_allowance_while_full_authority_remains_external() {
        let facts = vec![SchemaPromotionResidualPathFacts {
            path: "/value".to_owned(),
            observed_arrow_types: vec![CanonicalArrowType::Int {
                signed: true,
                bits: 64,
            }],
            observed_count: 1,
            address_value_digest: "sha256:evidence".to_owned(),
            packages: vec!["sha256:package".to_owned()],
            example_addresses: Vec::new(),
            associations: Vec::new(),
        }];
        let overrides = BTreeMap::from([("/value".to_owned(), DataType::Int32)]);
        let mut denied_conflicts = Vec::new();
        let denied = build_path_reports(
            &facts,
            &overrides,
            &BTreeMap::new(),
            None,
            &ContractPolicy::default().types,
            &mut denied_conflicts,
        )
        .unwrap();
        assert_eq!(
            denied[0].coercion_verdicts[0].decision,
            FieldCoercionDecision::LossyRejected
        );
        assert!(
            denied_conflicts
                .iter()
                .any(|conflict| conflict.code == "lossy_allowance_required")
        );

        let mut allowed_policy = ContractPolicy::default().types;
        allowed_policy.allow_lossy_mapping = true;
        let mut allowed_conflicts = Vec::new();
        let allowed = build_path_reports(
            &facts,
            &overrides,
            &BTreeMap::new(),
            None,
            &allowed_policy,
            &mut allowed_conflicts,
        )
        .unwrap();
        assert_eq!(
            allowed[0].coercion_verdicts[0].decision,
            FieldCoercionDecision::LossyAllowed
        );
        assert!(allowed_conflicts.is_empty());
    }

    #[test]
    fn authoritative_fresh_discovery_auto_selects_a_compatible_type() {
        let snapshot = SchemaSnapshotArtifact::new(
            &ResourceId::new("source.resource").unwrap(),
            &Schema::new(vec![Field::new("price", DataType::Int64, true)]),
            BTreeMap::new(),
        )
        .unwrap();
        let authority = fresh_discovery_types(
            "source.resource",
            &SchemaPromotionFreshDiscovery::Available {
                snapshot: Box::new(snapshot),
                discovery_manifest: None,
                content_identity: BTreeMap::from([(
                    "content_sha256".to_owned(),
                    "sha256:fresh".to_owned(),
                )]),
            },
        )
        .unwrap();
        let facts = vec![SchemaPromotionResidualPathFacts {
            path: "/price".to_owned(),
            observed_arrow_types: vec![CanonicalArrowType::Int {
                signed: true,
                bits: 32,
            }],
            observed_count: 1,
            address_value_digest: "sha256:evidence".to_owned(),
            packages: vec!["sha256:package".to_owned()],
            example_addresses: vec![RowProvenanceAddress::new(
                PackageHash::new("sha256:package").unwrap(),
                SegmentId::new("segment-1").unwrap(),
                0,
            )],
            associations: Vec::new(),
        }];
        let mut conflicts = Vec::new();
        let paths = build_path_reports(
            &facts,
            &BTreeMap::new(),
            &authority.types,
            None,
            &ContractPolicy::default().types,
            &mut conflicts,
        )
        .unwrap();
        assert!(conflicts.is_empty());
        assert_eq!(paths[0].selected_type.as_deref(), Some("Int64"));
    }

    #[test]
    fn fresh_discovery_for_another_resource_is_rejected() {
        let snapshot = SchemaSnapshotArtifact::new(
            &ResourceId::new("other.resource").unwrap(),
            &Schema::new(vec![Field::new("price", DataType::Int64, true)]),
            BTreeMap::new(),
        )
        .unwrap();
        let error = fresh_discovery_types(
            "source.resource",
            &SchemaPromotionFreshDiscovery::Available {
                snapshot: Box::new(snapshot),
                discovery_manifest: None,
                content_identity: BTreeMap::from([("etag".to_owned(), "fresh".to_owned())]),
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("not \"source.resource\""));
    }

    #[test]
    fn local_inventory_adapter_is_strictly_read_only_and_reports_malformed_entries() {
        let temp = tempfile::tempdir().unwrap();
        let malformed = temp.path().join("malformed-package");
        fs::create_dir(&malformed).unwrap();
        let marker = malformed.join("keep.txt");
        fs::write(&marker, b"unchanged").unwrap();
        let before = fs::read(&marker).unwrap();

        let facts = LocalPackagePromotionEvidenceInventory::new(temp.path())
            .inventory("source.resource")
            .unwrap();

        assert_eq!(fs::read(&marker).unwrap(), before);
        assert_eq!(facts.evidence.len(), 1);
        assert!(!facts.coverage_complete);
        assert_eq!(
            facts.evidence[0].availability,
            SchemaPromotionEvidenceAvailability::Missing
        );
        assert!(facts.evidence[0].detail.is_some());
    }

    #[test]
    fn local_inventory_reports_missing_and_tombstone_only_without_inference() {
        let temp = tempfile::tempdir().unwrap();
        let missing = LocalPackagePromotionEvidenceInventory::new(temp.path().join("missing"))
            .inventory("source.resource")
            .unwrap();
        assert!(!missing.coverage_complete);
        assert_eq!(
            missing.evidence[0].availability,
            SchemaPromotionEvidenceAvailability::Missing
        );

        let package_root = temp.path().join("packages");
        let package_dir = package_root.join("archived");
        fs::create_dir_all(&package_root).unwrap();
        let builder = cdf_package::PackageBuilder::create(&package_dir, "archived").unwrap();
        let output_position = SourcePosition::FileManifest(FileManifest {
            version: CHECKPOINT_STATE_VERSION,
            files: Vec::new(),
        });
        let state_delta = cdf_package::StateDeltaPreimage {
            checkpoint_id: CheckpointId::new("checkpoint-archived").unwrap(),
            pipeline_id: PipelineId::new("pipeline-archived").unwrap(),
            resource_id: ResourceId::new("source.resource").unwrap(),
            scope: ScopeKey::Resource,
            state_version: CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: None,
            input_position: None,
            output_position,
            schema_hash: SchemaHash::new("sha256:old-schema").unwrap(),
            segments: Vec::new(),
        };
        builder.write_input_checkpoint_artifact(&None).unwrap();
        builder
            .write_state_delta_preimage_artifact(&state_delta)
            .unwrap();
        let manifest = builder
            .finish_with_status(cdf_package::PackageStatus::Archived)
            .unwrap();
        let package_hash = PackageHash::new(manifest.package_hash).unwrap();
        cdf_package::PackageReader::open(&package_dir)
            .unwrap()
            .append_receipt(Receipt {
                receipt_id: ReceiptId::new("receipt-archived").unwrap(),
                destination: DestinationId::new("warehouse").unwrap(),
                target: TargetName::new("archived_target").unwrap(),
                package_hash: package_hash.clone(),
                segment_acks: Vec::new(),
                disposition: WriteDisposition::Append,
                idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
                transaction: None,
                counts: CommitCounts {
                    rows_written: 0,
                    rows_inserted: Some(0),
                    rows_updated: Some(0),
                    rows_deleted: Some(0),
                },
                schema_hash: SchemaHash::new("sha256:old-schema").unwrap(),
                migrations: Vec::new(),
                committed_at_ms: 1,
                verify: VerifyClause {
                    kind: "fixture".to_owned(),
                    statement: "fixture".to_owned(),
                    parameters: BTreeMap::new(),
                },
            })
            .unwrap();
        let tombstone = LocalPackagePromotionEvidenceInventory::new(&package_root)
            .inventory("source.resource")
            .unwrap();
        assert!(tombstone.coverage_complete);
        assert!(tombstone.paths.is_empty());
        assert_eq!(
            tombstone.evidence[0].availability,
            SchemaPromotionEvidenceAvailability::TombstoneOnly
        );
        assert_eq!(
            tombstone.evidence[0].resource_attribution,
            SchemaPromotionResourceAttribution::Attributed
        );
        assert_eq!(tombstone.evidence[0].recorded_receipts.len(), 1);

        let target_keys = promotion_target_keys(&tombstone.paths, &tombstone.evidence);
        let corrections = DestinationCorrectionCapabilities::default().with_strategy(
            CorrectionStrategyCapability::new(
                CorrectionStrategy::CorrectionSidecar,
                TransactionSupport::AtomicTarget,
                IdempotencySupport::PackageToken,
            ),
        );
        let sheet = DestinationSheet {
            destination: DestinationId::new("warehouse").unwrap(),
            supported_dispositions: vec![WriteDisposition::Append],
            transactions: TransactionSupport::AtomicTarget,
            idempotency: IdempotencySupport::PackageToken,
            type_mappings: vec![TypeMapping {
                arrow_type: "int64".to_owned(),
                destination_type: "bigint".to_owned(),
                fidelity: TypeMappingFidelity::Lossless,
            }],
            identifier_rules: IdentifierRules {
                normalizer: "namecase-v1".to_owned(),
                max_length: Some(63),
                allowed_pattern: Some("[a-z_][a-z0-9_]*".to_owned()),
            },
            migration_support: CapabilitySupport::Supported,
            quarantine_tables: CapabilitySupport::Supported,
            concurrency: ConcurrencyLimit {
                max_writers: Some(1),
            },
        };
        let locked_destination = crate::LockedDestination::new(
            DestinationSheetArtifact::new(
                sheet,
                DestinationProtocolCapabilities::default().with_corrections(corrections),
            )
            .unwrap(),
        )
        .unwrap();
        let lock = CdfLock {
            version: 1,
            project: crate::ProjectLock {
                name: "fixture".to_owned(),
                default_environment: "test".to_owned(),
            },
            dependency_tuple: crate::DependencyTuple {
                cdf: "fixture".to_owned(),
                arrow_rs: "fixture".to_owned(),
                datafusion: None,
                object_store: None,
                duckdb_rs: None,
                rust: None,
            },
            normalizer: "namecase-v1".to_owned(),
            resources: BTreeMap::new(),
            destinations: BTreeMap::from([("warehouse".to_owned(), locked_destination)]),
        };
        let mut conflicts = Vec::new();
        let targets = plan_targets(
            &lock,
            &target_keys,
            &[],
            &ContractPolicy::default(),
            &tombstone.evidence,
            &mut conflicts,
        )
        .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].target, "archived_target");
        assert!(targets[0].affected_paths.is_empty());
        assert!(targets[0].migrations.is_empty());
        assert_eq!(targets[0].affected_packages, [package_hash.to_string()]);
        assert_eq!(
            targets[0].evidence[0].availability,
            SchemaPromotionEvidenceAvailability::TombstoneOnly
        );
        assert!(targets[0].strategy.is_none());
        assert!(conflicts.iter().any(|conflict| {
            conflict.code == "target_residual_values_unavailable"
                && conflict.message.contains("TombstoneOnly")
        }));
        assert!(
            conflicts
                .iter()
                .any(|conflict| conflict.code == "safe_correction_strategy_missing")
        );
    }

    #[test]
    fn generic_inventory_is_canonicalized_before_hashing() {
        let address = RowProvenanceAddress::new(
            PackageHash::new("sha256:package").unwrap(),
            SegmentId::new("segment-1").unwrap(),
            2,
        );
        let inventory = SchemaPromotionEvidenceInventoryFacts {
            paths: vec![SchemaPromotionResidualPathFacts {
                path: "/z".to_owned(),
                observed_arrow_types: vec![
                    CanonicalArrowType::Int {
                        signed: true,
                        bits: 64,
                    },
                    CanonicalArrowType::Int {
                        signed: true,
                        bits: 64,
                    },
                ],
                observed_count: 1,
                address_value_digest: "sha256:z".to_owned(),
                packages: vec!["b".to_owned(), "a".to_owned(), "a".to_owned()],
                example_addresses: vec![address.clone(), address],
                associations: Vec::new(),
            }],
            evidence: Vec::new(),
            coverage_complete: true,
        };
        let canonical = canonicalize_inventory(inventory).unwrap();
        assert_eq!(canonical.paths[0].observed_arrow_types.len(), 1);
        assert_eq!(canonical.paths[0].packages, ["a", "b"]);
        assert_eq!(canonical.paths[0].example_addresses.len(), 1);
    }

    #[test]
    fn generic_inventory_rejects_noncanonical_or_unverified_receipt_associations() {
        let inventory =
            |association_receipts: Vec<&str>,
             evidence_receipts: Vec<&str>,
             association_target: &str,
             association_availability: SchemaPromotionEvidenceAvailability| {
                SchemaPromotionEvidenceInventoryFacts {
                paths: vec![SchemaPromotionResidualPathFacts {
                    path: "/value".to_owned(),
                    observed_arrow_types: vec![CanonicalArrowType::Int {
                        signed: true,
                        bits: 64,
                    }],
                    observed_count: 1,
                    address_value_digest: "sha256:value".to_owned(),
                    packages: vec!["sha256:package".to_owned()],
                    example_addresses: Vec::new(),
                    associations: vec![SchemaPromotionPackageTargetAssociation {
                        package_hash: "sha256:package".to_owned(),
                        destination: "warehouse".to_owned(),
                        target: association_target.to_owned(),
                        recorded_receipt_ids: association_receipts
                            .into_iter()
                            .map(str::to_owned)
                            .collect(),
                        availability: association_availability,
                    }],
                }],
                evidence: vec![SchemaPromotionEvidenceReport {
                    artifact_location: "fixture".to_owned(),
                    package_hash: Some("sha256:package".to_owned()),
                    availability: SchemaPromotionEvidenceAvailability::RetainedPackage,
                    resource_attribution: SchemaPromotionResourceAttribution::Attributed,
                    recorded_receipts: evidence_receipts
                        .into_iter()
                        .map(|receipt_id| SchemaPromotionReceiptReport {
                            receipt_id: receipt_id.to_owned(),
                            destination: "warehouse".to_owned(),
                            target: "events".to_owned(),
                            verification: SchemaPromotionReceiptVerification::StructuralCoverageVerifiedDestinationVerificationPending,
                        })
                        .collect(),
                    residual_rows: 1,
                    residual_paths: vec!["/value".to_owned()],
                    detail: None,
                }],
                coverage_complete: true,
            }
            };

        let valid = canonicalize_inventory(inventory(
            vec!["receipt-2", "receipt-1"],
            vec!["receipt-1", "receipt-2"],
            "events",
            SchemaPromotionEvidenceAvailability::RetainedPackage,
        ))
        .unwrap();
        assert_eq!(
            valid.paths[0].associations[0].recorded_receipt_ids,
            ["receipt-1", "receipt-2"]
        );
        for invalid in [
            inventory(
                Vec::new(),
                vec!["receipt-1"],
                "events",
                SchemaPromotionEvidenceAvailability::RetainedPackage,
            ),
            inventory(
                vec!["receipt-1"],
                vec!["receipt-1", "receipt-2"],
                "events",
                SchemaPromotionEvidenceAvailability::RetainedPackage,
            ),
            inventory(
                vec!["receipt-1", "receipt-extra"],
                vec!["receipt-1"],
                "events",
                SchemaPromotionEvidenceAvailability::RetainedPackage,
            ),
            inventory(
                vec!["receipt-1"],
                vec!["receipt-1"],
                "other-target",
                SchemaPromotionEvidenceAvailability::RetainedPackage,
            ),
            inventory(
                vec!["receipt-1"],
                vec!["receipt-1"],
                "events",
                SchemaPromotionEvidenceAvailability::TombstoneOnly,
            ),
            inventory(
                vec!["receipt-1", "receipt-1"],
                vec!["receipt-1"],
                "events",
                SchemaPromotionEvidenceAvailability::RetainedPackage,
            ),
        ] {
            assert!(canonicalize_inventory(invalid).is_err());
        }
    }

    #[test]
    fn promotion_snapshot_hash_is_exact_and_binds_evidence_lineage() {
        let resource_id = ResourceId::new("source.resource").unwrap();
        let pinned = SchemaSnapshotArtifact::new(
            &resource_id,
            &Schema::new(vec![Field::new("id", DataType::Int64, false)]),
            BTreeMap::new(),
        )
        .unwrap();
        let proposed = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("price", DataType::Int64, true).with_metadata(
                std::collections::HashMap::from([
                    ("cdf:source_name".to_owned(), "price".to_owned()),
                    ("cdf:promoted_path".to_owned(), "/price".to_owned()),
                ]),
            ),
        ]);
        let fresh = FreshDiscoveryAuthority {
            types: BTreeMap::new(),
            schema_hash: Some("sha256:fresh".to_owned()),
            manifest_hash: Some("sha256:manifest".to_owned()),
            coverage: Some(DiscoveryCoverageMode::Exhaustive),
            content_identity: BTreeMap::from([("etag".to_owned(), "one".to_owned())]),
            unavailable_reason: None,
        };
        let mut paths = vec![SchemaPromotionPathReport {
            path: "/price".to_owned(),
            source_name: "price".to_owned(),
            projection_supported: true,
            observed_types: vec!["Int64".to_owned()],
            observed_arrow_types: vec![CanonicalArrowType::Int {
                signed: true,
                bits: 64,
            }],
            observed_count: 10,
            selected_type: Some("Int64".to_owned()),
            selected_arrow_type: Some(CanonicalArrowType::Int {
                signed: true,
                bits: 64,
            }),
            coercion_verdicts: vec![SchemaPromotionCoercionVerdict {
                observed_type: CanonicalArrowType::Int {
                    signed: true,
                    bits: 64,
                },
                selected_type: CanonicalArrowType::Int {
                    signed: true,
                    bits: 64,
                },
                decision: FieldCoercionDecision::Preserved,
            }],
            output_field: "price".to_owned(),
            affected_address_value_digest: "sha256:evidence-one".to_owned(),
            affected_packages: vec!["sha256:package".to_owned()],
            affected_row_examples: Vec::new(),
            associations: vec![SchemaPromotionPackageTargetAssociation {
                package_hash: "sha256:package".to_owned(),
                destination: "warehouse".to_owned(),
                target: "prices".to_owned(),
                recorded_receipt_ids: vec!["receipt-1".to_owned()],
                availability: SchemaPromotionEvidenceAvailability::RetainedPackage,
            }],
        }];
        let lineage = || SnapshotCompilerLineage {
            normalizer_version: "namecase-v1",
            contract_policy_hash: "sha256:policy",
            validation_program_hash: Some("sha256:program"),
        };
        let first = promotion_snapshot_plan(&pinned, &proposed, &fresh, lineage(), &paths).unwrap();
        let second =
            promotion_snapshot_plan(&pinned, &proposed, &fresh, lineage(), &paths).unwrap();
        assert_eq!(first, second);
        assert!(first.path.contains(&first.schema_hash));
        first.artifact.validate_hash_input().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let store = crate::SchemaSnapshotStore::new(temp.path());
        let written = store.write(&first.artifact).unwrap();
        assert_eq!(written, temp.path().join(&first.path));
        let hydrated = store.read(&first.artifact.reference()).unwrap();
        assert_eq!(hydrated, first.artifact);
        assert_eq!(first.artifact.normalizer_version(), Some("namecase-v1"));
        assert_eq!(first.artifact.metadata.len(), 1);
        assert!(
            !first
                .artifact
                .metadata
                .contains_key("cdf:promotion_old_schema_hash")
        );
        assert!(
            !first
                .artifact
                .metadata
                .contains_key("cdf:promotion_lineage_version")
        );

        let valid_authority = first.artifact.promotion_authority.clone().unwrap();
        let mut invalid_authorities = Vec::new();
        let mut empty = valid_authority.clone();
        empty.selected_paths.clear();
        assert!(
            SchemaSnapshotArtifact::new_with_promotion(&resource_id, &proposed, empty.clone(),)
                .is_err()
        );
        invalid_authorities.push(empty);
        let mut wrong_resource = valid_authority.clone();
        wrong_resource.resource_id = "other.resource".to_owned();
        invalid_authorities.push(wrong_resource);
        let mut wrong_schema = valid_authority.clone();
        wrong_schema.proposed_schema.fields.pop();
        invalid_authorities.push(wrong_schema);
        let mut wrong_old_path = valid_authority.clone();
        wrong_old_path.old_snapshot.path = ".cdf/schemas/wrong.json".to_owned();
        invalid_authorities.push(wrong_old_path);
        for invalid_authority in invalid_authorities {
            let mut invalid_artifact = first.artifact.clone();
            invalid_artifact.promotion_authority = Some(invalid_authority);
            fs::write(
                &written,
                serde_json::to_vec_pretty(&invalid_artifact).unwrap(),
            )
            .unwrap();
            assert!(store.read(&first.artifact.reference()).is_err());
        }
        for (key, value) in [
            ("cdf:normalizer", "conflicting-normalizer"),
            ("cdf:promotion_old_schema_hash", "sha256:conflicting-old"),
            ("cdf:promotion_lineage_version", "999"),
        ] {
            let mut conflicting_metadata = first.artifact.clone();
            conflicting_metadata
                .metadata
                .insert(key.to_owned(), value.to_owned());
            fs::write(
                &written,
                serde_json::to_vec_pretty(&conflicting_metadata).unwrap(),
            )
            .unwrap();
            assert!(store.read(&first.artifact.reference()).is_err());
        }
        let mut arbitrary = serde_json::to_value(&first.artifact).unwrap();
        arbitrary["promotion_authority"]["arbitrary"] = serde_json::json!(true);
        fs::write(&written, serde_json::to_vec_pretty(&arbitrary).unwrap()).unwrap();
        assert!(store.read(&first.artifact.reference()).is_err());

        let mut reassociated = paths.clone();
        reassociated[0].associations[0].recorded_receipt_ids = vec!["receipt-2".to_owned()];
        let changed_association =
            promotion_snapshot_plan(&pinned, &proposed, &fresh, lineage(), &reassociated).unwrap();
        assert_ne!(first.schema_hash, changed_association.schema_hash);
        paths[0].affected_address_value_digest = "sha256:evidence-two".to_owned();
        let changed =
            promotion_snapshot_plan(&pinned, &proposed, &fresh, lineage(), &paths).unwrap();
        assert_ne!(first.schema_hash, changed.schema_hash);
    }
}
