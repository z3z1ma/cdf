use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use cdf_kernel::{
    CdfError, DiscoveryManifestHash, DiscoveryManifestReference, ResourceId, Result, SchemaHash,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const DISCOVERY_MANIFEST_ARTIFACT_VERSION: u16 = 1;
pub const DISCOVERY_MANIFEST_SUFFIX: &str = ".discovery.json";
pub const DEFAULT_DISCOVERY_MAX_METADATA_BYTES_PER_FILE: u64 = 64 * 1024 * 1024;
pub const DEFAULT_DISCOVERY_MAX_TOTAL_IN_FLIGHT_BYTES: u64 = 128 * 1024 * 1024;
pub const DEFAULT_DISCOVERY_MAX_CONCURRENT_PROBES: u32 = 8;

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "DiscoveryExecutorBudgetWire",
    into = "DiscoveryExecutorBudgetWire"
)]
pub struct DiscoveryExecutorBudget {
    max_metadata_bytes_per_file: u64,
    max_total_in_flight_bytes: u64,
    max_concurrent_probes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DiscoveryExecutorBudgetWire {
    max_metadata_bytes_per_file: u64,
    max_total_in_flight_bytes: u64,
    max_concurrent_probes: u32,
}

impl DiscoveryExecutorBudget {
    pub fn new(
        max_metadata_bytes_per_file: u64,
        max_total_in_flight_bytes: u64,
        max_concurrent_probes: u32,
    ) -> Result<Self> {
        if max_metadata_bytes_per_file == 0 {
            return Err(CdfError::contract(
                "discovery budget max_metadata_bytes_per_file must be greater than zero",
            ));
        }
        if max_total_in_flight_bytes == 0 {
            return Err(CdfError::contract(
                "discovery budget max_total_in_flight_bytes must be greater than zero",
            ));
        }
        if max_concurrent_probes == 0 {
            return Err(CdfError::contract(
                "discovery budget max_concurrent_probes must be greater than zero",
            ));
        }
        if max_metadata_bytes_per_file > max_total_in_flight_bytes {
            return Err(CdfError::contract(format!(
                "discovery budget max_metadata_bytes_per_file ({max_metadata_bytes_per_file}) cannot exceed max_total_in_flight_bytes ({max_total_in_flight_bytes})"
            )));
        }
        max_metadata_bytes_per_file
            .checked_mul(u64::from(max_concurrent_probes))
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "discovery budget overflows scheduled-byte accounting: {max_metadata_bytes_per_file} bytes per file times {max_concurrent_probes} probes"
                ))
            })?;
        Ok(Self {
            max_metadata_bytes_per_file,
            max_total_in_flight_bytes,
            max_concurrent_probes,
        })
    }

    pub fn max_metadata_bytes_per_file(&self) -> u64 {
        self.max_metadata_bytes_per_file
    }

    pub fn max_total_in_flight_bytes(&self) -> u64 {
        self.max_total_in_flight_bytes
    }

    pub fn max_concurrent_probes(&self) -> u32 {
        self.max_concurrent_probes
    }
}

impl Default for DiscoveryExecutorBudget {
    fn default() -> Self {
        Self::new(
            DEFAULT_DISCOVERY_MAX_METADATA_BYTES_PER_FILE,
            DEFAULT_DISCOVERY_MAX_TOTAL_IN_FLIGHT_BYTES,
            DEFAULT_DISCOVERY_MAX_CONCURRENT_PROBES,
        )
        .expect("the built-in discovery budget is valid")
    }
}

impl TryFrom<DiscoveryExecutorBudgetWire> for DiscoveryExecutorBudget {
    type Error = CdfError;

    fn try_from(value: DiscoveryExecutorBudgetWire) -> Result<Self> {
        Self::new(
            value.max_metadata_bytes_per_file,
            value.max_total_in_flight_bytes,
            value.max_concurrent_probes,
        )
    }
}

impl From<DiscoveryExecutorBudget> for DiscoveryExecutorBudgetWire {
    fn from(value: DiscoveryExecutorBudget) -> Self {
        Self {
            max_metadata_bytes_per_file: value.max_metadata_bytes_per_file,
            max_total_in_flight_bytes: value.max_total_in_flight_bytes,
            max_concurrent_probes: value.max_concurrent_probes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryCoverageMode {
    Exhaustive,
    Sampled,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryParticipation {
    Probed,
    Unprobed,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryIdentityStrength {
    StrongChecksum,
    StableEtag,
    WeakEtag,
    MultipartEtag,
    BoundedObservation,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DiscoveryBoundedIdentity {
    pub size_bytes: Option<u64>,
    pub modified_at_ms: Option<i64>,
    pub value: Option<String>,
    pub strength: DiscoveryIdentityStrength,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryMetadataScope {
    Schema,
    Field,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DiscoveryMetadataVariance {
    pub scope: DiscoveryMetadataScope,
    pub path: String,
    pub key: String,
    pub observed_values: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoverySchemaVerdictKind {
    Admitted,
    Incompatible,
    Quarantined,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DiscoverySchemaVerdict {
    pub kind: DiscoverySchemaVerdictKind,
    pub rule: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub details: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryCandidateEvidence {
    pub transport: String,
    pub canonical_location: String,
    pub identity: DiscoveryBoundedIdentity,
    pub participation: DiscoveryParticipation,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_variance: Vec<DiscoveryMetadataVariance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_schema_hash: Option<SchemaHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_verdict: Option<DiscoverySchemaVerdict>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoverySelectorSelection {
    pub canonical_location: String,
    pub score_sha256: String,
    pub candidate_identity: DiscoveryBoundedIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoverySelectorStratum {
    pub start_index_inclusive: u64,
    pub end_index_exclusive: u64,
    pub selected_location: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoverySelectorEvidence {
    pub selector: String,
    pub sample_files: u64,
    pub matched_count: u64,
    pub selected: Vec<DiscoverySelectorSelection>,
    pub interior_strata: Vec<DiscoverySelectorStratum>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryManifestInput {
    pub resource_id: String,
    pub baseline_schema_hash: Option<SchemaHash>,
    pub effective_schema_hash: Option<SchemaHash>,
    pub coverage: DiscoveryCoverageMode,
    pub selector: Option<DiscoverySelectorEvidence>,
    pub budget: DiscoveryExecutorBudget,
    pub normalizer_version: String,
    pub policy_version: String,
    pub candidates: Vec<DiscoveryCandidateEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryManifestHashInput {
    pub version: u16,
    pub resource_id: String,
    pub baseline_schema_hash: Option<SchemaHash>,
    pub effective_schema_hash: Option<SchemaHash>,
    pub coverage: DiscoveryCoverageMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<DiscoverySelectorEvidence>,
    pub budget: DiscoveryExecutorBudget,
    pub normalizer_version: String,
    pub policy_version: String,
    pub candidates: Vec<DiscoveryCandidateEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryManifestArtifact {
    pub version: u16,
    pub resource_id: String,
    pub manifest_hash: DiscoveryManifestHash,
    pub path: String,
    pub baseline_schema_hash: Option<SchemaHash>,
    pub effective_schema_hash: Option<SchemaHash>,
    pub coverage: DiscoveryCoverageMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<DiscoverySelectorEvidence>,
    pub budget: DiscoveryExecutorBudget,
    pub normalizer_version: String,
    pub policy_version: String,
    pub candidates: Vec<DiscoveryCandidateEvidence>,
    pub hash_input: serde_json::Value,
}

impl DiscoveryManifestArtifact {
    pub fn new(mut input: DiscoveryManifestInput) -> Result<Self> {
        input.candidates.sort_by(|left, right| {
            left.canonical_location
                .cmp(&right.canonical_location)
                .then_with(|| left.transport.cmp(&right.transport))
                .then_with(|| left.identity.cmp(&right.identity))
        });
        normalize_metadata_variance(&mut input.candidates);
        normalize_selector_evidence(input.selector.as_mut());
        validate_manifest_input(&input)?;
        let hash_input = DiscoveryManifestHashInput {
            version: DISCOVERY_MANIFEST_ARTIFACT_VERSION,
            resource_id: input.resource_id.clone(),
            baseline_schema_hash: input.baseline_schema_hash.clone(),
            effective_schema_hash: input.effective_schema_hash.clone(),
            coverage: input.coverage.clone(),
            selector: input.selector.clone(),
            budget: input.budget.clone(),
            normalizer_version: input.normalizer_version.clone(),
            policy_version: input.policy_version.clone(),
            candidates: input.candidates.clone(),
        };
        let hash_input = canonical_json_value(&hash_input)?;
        let manifest_hash = manifest_hash_for_canonical_value(&hash_input)?;
        let resource_id = ResourceId::new(input.resource_id.clone())?;
        let path = discovery_manifest_relative_path(&resource_id, &manifest_hash)?;
        Ok(Self {
            version: DISCOVERY_MANIFEST_ARTIFACT_VERSION,
            resource_id: input.resource_id,
            manifest_hash,
            path,
            baseline_schema_hash: input.baseline_schema_hash,
            effective_schema_hash: input.effective_schema_hash,
            coverage: input.coverage,
            selector: input.selector,
            budget: input.budget,
            normalizer_version: input.normalizer_version,
            policy_version: input.policy_version,
            candidates: input.candidates,
            hash_input,
        })
    }

    pub fn reference(&self) -> DiscoveryManifestReference {
        DiscoveryManifestReference {
            manifest_hash: self.manifest_hash.clone(),
            path: self.path.clone(),
        }
    }

    /// Returns whether two manifests record the same discovery observation.
    ///
    /// The baseline is deliberately excluded: it identifies the verified pinned
    /// snapshot against which a refresh ran, not a property of the observed file
    /// set. This lets an unchanged refresh retain the existing content-addressed
    /// snapshot instead of creating an identity chain whose only difference is
    /// the new baseline reference.
    pub fn has_same_observation(&self, other: &Self) -> bool {
        self.resource_id == other.resource_id
            && self.effective_schema_hash == other.effective_schema_hash
            && self.coverage == other.coverage
            && self.selector == other.selector
            && self.budget == other.budget
            && self.normalizer_version == other.normalizer_version
            && self.policy_version == other.policy_version
            && self.candidates == other.candidates
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != DISCOVERY_MANIFEST_ARTIFACT_VERSION {
            return Err(CdfError::data(format!(
                "discovery manifest uses unsupported artifact version {}; expected {}",
                self.version, DISCOVERY_MANIFEST_ARTIFACT_VERSION
            )));
        }
        let input = DiscoveryManifestInput {
            resource_id: self.resource_id.clone(),
            baseline_schema_hash: self.baseline_schema_hash.clone(),
            effective_schema_hash: self.effective_schema_hash.clone(),
            coverage: self.coverage.clone(),
            selector: self.selector.clone(),
            budget: self.budget.clone(),
            normalizer_version: self.normalizer_version.clone(),
            policy_version: self.policy_version.clone(),
            candidates: self.candidates.clone(),
        };
        validate_manifest_input(&input)?;
        if input
            .candidates
            .windows(2)
            .any(|pair| candidate_sort_key(&pair[0]) > candidate_sort_key(&pair[1]))
        {
            return Err(CdfError::data(
                "discovery manifest candidates are not in canonical transport-location order",
            ));
        }
        let expected_input = canonical_json_value(&DiscoveryManifestHashInput {
            version: self.version,
            resource_id: self.resource_id.clone(),
            baseline_schema_hash: self.baseline_schema_hash.clone(),
            effective_schema_hash: self.effective_schema_hash.clone(),
            coverage: self.coverage.clone(),
            selector: self.selector.clone(),
            budget: self.budget.clone(),
            normalizer_version: self.normalizer_version.clone(),
            policy_version: self.policy_version.clone(),
            candidates: self.candidates.clone(),
        })?;
        if self.hash_input != expected_input {
            return Err(CdfError::data(
                "discovery manifest hash_input does not match artifact evidence",
            ));
        }
        let expected_hash = manifest_hash_for_canonical_value(&expected_input)?;
        if self.manifest_hash != expected_hash {
            return Err(CdfError::data(format!(
                "discovery manifest hash {} does not match deterministic hash {}",
                self.manifest_hash, expected_hash
            )));
        }
        let resource_id = ResourceId::new(self.resource_id.clone())?;
        let expected_path = discovery_manifest_relative_path(&resource_id, &self.manifest_hash)?;
        if self.path != expected_path {
            return Err(CdfError::data(format!(
                "discovery manifest path {} does not match deterministic path {}",
                self.path, expected_path
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryManifestStore {
    project_root: PathBuf,
}

impl DiscoveryManifestStore {
    pub fn new(project_root: impl AsRef<Path>) -> Self {
        Self {
            project_root: project_root.as_ref().to_path_buf(),
        }
    }

    pub fn artifact_path(&self, reference: &DiscoveryManifestReference) -> Result<PathBuf> {
        validate_manifest_reference_path(&reference.path)?;
        Ok(self.project_root.join(&reference.path))
    }

    pub fn write(&self, artifact: &DiscoveryManifestArtifact) -> Result<PathBuf> {
        self.write_if_changed(artifact)?;
        Ok(self.project_root.join(&artifact.path))
    }

    pub fn write_if_changed(&self, artifact: &DiscoveryManifestArtifact) -> Result<bool> {
        artifact.validate()?;
        let path = self.project_root.join(&artifact.path);
        let encoded = canonical_json_bytes(artifact)?;
        match fs::read(&path) {
            Ok(existing) if existing == encoded => return Ok(false),
            Ok(_) => {
                return Err(CdfError::data(format!(
                    "discovery manifest content-addressed path {} already contains different bytes",
                    path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(CdfError::data(format!(
                    "read {} before discovery manifest write: {error}",
                    path.display()
                )));
            }
        }
        let parent = path.parent().ok_or_else(|| {
            CdfError::internal(format!(
                "discovery manifest path {} has no parent",
                path.display()
            ))
        })?;
        fs::create_dir_all(parent)
            .map_err(|error| CdfError::data(format!("create {}: {error}", parent.display())))?;
        Ok(matches!(
            atomic_write_new(&path, &encoded)?,
            AtomicInstallOutcome::Installed
        ))
    }

    pub fn read(
        &self,
        reference: &DiscoveryManifestReference,
    ) -> Result<DiscoveryManifestArtifact> {
        let path = self.artifact_path(reference)?;
        let bytes = fs::read(&path)
            .map_err(|error| CdfError::data(format!("read {}: {error}", path.display())))?;
        let artifact = serde_json::from_slice::<DiscoveryManifestArtifact>(&bytes)
            .map_err(|error| CdfError::data(format!("parse {}: {error}", path.display())))?;
        artifact.validate()?;
        if artifact.reference() != *reference {
            return Err(CdfError::data(format!(
                "discovery manifest {} does not match its hash/path reference",
                path.display()
            )));
        }
        Ok(artifact)
    }
}

pub fn discovery_manifest_relative_path(
    resource_id: &ResourceId,
    manifest_hash: &DiscoveryManifestHash,
) -> Result<String> {
    ensure_single_path_component(resource_id.as_str(), "resource id")?;
    ensure_single_path_component(manifest_hash.as_str(), "manifest hash")?;
    Ok(format!(
        "{}/{}@{}{}",
        crate::SCHEMA_SNAPSHOT_DIR,
        resource_id,
        manifest_hash,
        DISCOVERY_MANIFEST_SUFFIX
    ))
}

fn validate_manifest_input(input: &DiscoveryManifestInput) -> Result<()> {
    ResourceId::new(input.resource_id.clone())?;
    if input.normalizer_version.trim().is_empty() {
        return Err(CdfError::contract(
            "discovery manifest normalizer_version cannot be empty",
        ));
    }
    if input.policy_version.trim().is_empty() {
        return Err(CdfError::contract(
            "discovery manifest policy_version cannot be empty",
        ));
    }
    if input.candidates.is_empty() {
        return Err(CdfError::contract(
            "discovery manifest requires at least one matched candidate",
        ));
    }
    let mut locations = BTreeSet::new();
    for candidate in &input.candidates {
        validate_candidate(candidate)?;
        if !locations.insert(candidate.canonical_location.as_str()) {
            return Err(CdfError::contract(format!(
                "discovery manifest contains duplicate canonical location `{}`",
                candidate.canonical_location
            )));
        }
    }
    let probed_locations = input
        .candidates
        .iter()
        .filter(|candidate| candidate.participation == DiscoveryParticipation::Probed)
        .map(|candidate| candidate.canonical_location.as_str())
        .collect::<BTreeSet<_>>();
    match (&input.coverage, &input.selector) {
        (DiscoveryCoverageMode::Exhaustive, None) => {
            if probed_locations.len() != input.candidates.len() {
                return Err(CdfError::contract(
                    "exhaustive discovery manifest requires every candidate to be probed",
                ));
            }
        }
        (DiscoveryCoverageMode::Exhaustive, Some(_)) => {
            return Err(CdfError::contract(
                "exhaustive discovery manifest forbids sampled selector evidence",
            ));
        }
        (DiscoveryCoverageMode::Sampled, None) => {
            return Err(CdfError::contract(
                "sampled discovery manifest requires selector evidence",
            ));
        }
        (DiscoveryCoverageMode::Sampled, Some(selector)) => {
            validate_selector(selector, &input.candidates, &probed_locations)?;
            if probed_locations.len() == input.candidates.len() {
                return Err(CdfError::contract(
                    "sampled discovery manifest with every candidate probed must be recorded as exhaustive",
                ));
            }
        }
    }
    Ok(())
}

fn validate_candidate(candidate: &DiscoveryCandidateEvidence) -> Result<()> {
    if candidate.transport.trim().is_empty() {
        return Err(CdfError::contract(
            "discovery candidate transport cannot be empty",
        ));
    }
    if candidate.canonical_location.trim().is_empty() {
        return Err(CdfError::contract(
            "discovery candidate canonical_location cannot be empty",
        ));
    }
    match (&candidate.identity.strength, &candidate.identity.value) {
        (DiscoveryIdentityStrength::Unavailable, None) => {}
        (DiscoveryIdentityStrength::Unavailable, Some(_)) => {
            return Err(CdfError::contract(format!(
                "discovery candidate `{}` has an identity value with unavailable strength",
                candidate.canonical_location
            )));
        }
        (_, Some(value)) if !value.trim().is_empty() => {}
        _ => {
            return Err(CdfError::contract(format!(
                "discovery candidate `{}` requires a non-empty identity value for its declared strength",
                candidate.canonical_location
            )));
        }
    }
    let mut variance = candidate.metadata_variance.clone();
    variance.sort();
    if variance != candidate.metadata_variance {
        return Err(CdfError::contract(format!(
            "discovery candidate `{}` metadata variance must be canonically sorted",
            candidate.canonical_location
        )));
    }
    for item in &candidate.metadata_variance {
        if item.key.trim().is_empty() || item.observed_values.is_empty() {
            return Err(CdfError::contract(format!(
                "discovery candidate `{}` metadata variance requires a key and observed values",
                candidate.canonical_location
            )));
        }
        let mut values = item.observed_values.clone();
        values.sort();
        values.dedup();
        if values != item.observed_values {
            return Err(CdfError::contract(format!(
                "discovery candidate `{}` metadata variance values must be sorted and unique",
                candidate.canonical_location
            )));
        }
    }
    match candidate.participation {
        DiscoveryParticipation::Probed => {
            if candidate.physical_schema_hash.is_none()
                || candidate.probe_bytes.is_none()
                || candidate.schema_verdict.is_none()
            {
                return Err(CdfError::contract(format!(
                    "probed discovery candidate `{}` requires physical_schema_hash, probe_bytes, and schema_verdict",
                    candidate.canonical_location
                )));
            }
            if candidate.probe_bytes == Some(0) {
                return Err(CdfError::contract(format!(
                    "probed discovery candidate `{}` requires probe_bytes greater than zero",
                    candidate.canonical_location
                )));
            }
        }
        DiscoveryParticipation::Unprobed => {
            if candidate.physical_schema_hash.is_some()
                || candidate.probe_bytes.is_some()
                || candidate.schema_verdict.is_some()
            {
                return Err(CdfError::contract(format!(
                    "unprobed discovery candidate `{}` forbids physical_schema_hash, probe_bytes, and schema_verdict",
                    candidate.canonical_location
                )));
            }
        }
    }
    if let Some(verdict) = &candidate.schema_verdict
        && verdict.rule.trim().is_empty()
    {
        return Err(CdfError::contract(format!(
            "discovery candidate `{}` schema verdict requires a named rule",
            candidate.canonical_location
        )));
    }
    Ok(())
}

fn validate_selector(
    selector: &DiscoverySelectorEvidence,
    candidates: &[DiscoveryCandidateEvidence],
    probed_locations: &BTreeSet<&str>,
) -> Result<()> {
    if selector.selector != "stratified-hash-v1" {
        return Err(CdfError::contract(format!(
            "unsupported discovery selector `{}`; expected `stratified-hash-v1`",
            selector.selector
        )));
    }
    if selector.sample_files == 0 {
        return Err(CdfError::contract(
            "sampled discovery selector sample_files must be greater than zero",
        ));
    }
    let matched_count = u64::try_from(candidates.len())
        .map_err(|_| CdfError::contract("discovery candidate count exceeds u64"))?;
    if selector.matched_count != matched_count {
        return Err(CdfError::contract(format!(
            "sampled discovery selector matched_count {} does not match {} manifest candidates",
            selector.matched_count,
            candidates.len()
        )));
    }
    if selector.sample_files >= selector.matched_count {
        return Err(CdfError::contract(
            "sampled discovery selector covering every candidate must be recorded as exhaustive",
        ));
    }
    if selector.selected.len() as u64 != selector.sample_files {
        return Err(CdfError::contract(format!(
            "sampled discovery selector expected {} selected candidates but recorded {}",
            selector.sample_files,
            selector.selected.len()
        )));
    }
    if selector
        .selected
        .windows(2)
        .any(|pair| pair[0].canonical_location > pair[1].canonical_location)
    {
        return Err(CdfError::contract(
            "sampled discovery selector selections must be in canonical location order",
        ));
    }
    if selector.interior_strata.windows(2).any(|pair| {
        (
            pair[0].start_index_inclusive,
            pair[0].end_index_exclusive,
            &pair[0].selected_location,
        ) > (
            pair[1].start_index_inclusive,
            pair[1].end_index_exclusive,
            &pair[1].selected_location,
        )
    }) {
        return Err(CdfError::contract(
            "sampled discovery selector strata must be in canonical boundary order",
        ));
    }
    let selected_locations = selector
        .selected
        .iter()
        .map(|selected| selected.canonical_location.as_str())
        .collect::<BTreeSet<_>>();
    if selected_locations.len() != selector.selected.len() {
        return Err(CdfError::contract(
            "sampled discovery selector contains duplicate selected locations",
        ));
    }
    if &selected_locations != probed_locations {
        return Err(CdfError::contract(
            "sampled discovery selector selected locations do not match probed candidates",
        ));
    }
    for selected in &selector.selected {
        if !is_sha256_hex(&selected.score_sha256) {
            return Err(CdfError::contract(format!(
                "sampled discovery selector score for `{}` must be 64 lowercase hexadecimal characters",
                selected.canonical_location
            )));
        }
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.canonical_location == selected.canonical_location)
            .expect("selected/probed location sets were validated above");
        if selected.candidate_identity != candidate.identity {
            return Err(CdfError::contract(format!(
                "sampled discovery selector identity for `{}` does not match candidate evidence",
                selected.canonical_location
            )));
        }
    }
    for stratum in &selector.interior_strata {
        if stratum.start_index_inclusive >= stratum.end_index_exclusive
            || stratum.end_index_exclusive > selector.matched_count
            || !selected_locations.contains(stratum.selected_location.as_str())
        {
            return Err(CdfError::contract(
                "sampled discovery selector contains invalid interior stratum evidence",
            ));
        }
    }
    Ok(())
}

fn normalize_metadata_variance(candidates: &mut [DiscoveryCandidateEvidence]) {
    for candidate in candidates {
        for variance in &mut candidate.metadata_variance {
            variance.observed_values.sort();
            variance.observed_values.dedup();
        }
        candidate.metadata_variance.sort();
        candidate.metadata_variance.dedup();
    }
}

fn normalize_selector_evidence(selector: Option<&mut DiscoverySelectorEvidence>) {
    let Some(selector) = selector else {
        return;
    };
    selector
        .selected
        .sort_by(|left, right| left.canonical_location.cmp(&right.canonical_location));
    selector.interior_strata.sort_by(|left, right| {
        left.start_index_inclusive
            .cmp(&right.start_index_inclusive)
            .then_with(|| left.end_index_exclusive.cmp(&right.end_index_exclusive))
            .then_with(|| left.selected_location.cmp(&right.selected_location))
    });
}

fn candidate_sort_key(
    candidate: &DiscoveryCandidateEvidence,
) -> (&str, &str, &DiscoveryBoundedIdentity) {
    (
        &candidate.canonical_location,
        &candidate.transport,
        &candidate.identity,
    )
}

fn manifest_hash_for_canonical_value(value: &serde_json::Value) -> Result<DiscoveryManifestHash> {
    let bytes = serde_json::to_vec(value).map_err(|error| CdfError::internal(error.to_string()))?;
    DiscoveryManifestHash::new(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

fn canonical_json_bytes(value: &impl Serialize) -> Result<Vec<u8>> {
    let value = canonical_json_value(value)?;
    serde_json::to_vec_pretty(&value).map_err(|error| CdfError::internal(error.to_string()))
}

fn canonical_json_value(value: &impl Serialize) -> Result<serde_json::Value> {
    let mut value =
        serde_json::to_value(value).map_err(|error| CdfError::internal(error.to_string()))?;
    value.sort_all_objects();
    Ok(value)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AtomicInstallOutcome {
    Installed,
    IdenticalExisting,
}

fn atomic_write_new(path: &Path, bytes: &[u8]) -> Result<AtomicInstallOutcome> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| CdfError::internal(format!("invalid manifest path {}", path.display())))?;
    let parent = path.parent().ok_or_else(|| {
        CdfError::internal(format!("manifest path {} has no parent", path.display()))
    })?;
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(".{file_name}.{}.{}.tmp", process::id(), sequence));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| CdfError::data(format!("create {}: {error}", temporary.display())))?;
        file.write_all(bytes)
            .map_err(|error| CdfError::data(format!("write {}: {error}", temporary.display())))?;
        file.sync_all()
            .map_err(|error| CdfError::data(format!("sync {}: {error}", temporary.display())))?;
        match fs::hard_link(&temporary, path) {
            Ok(()) => {
                fs::remove_file(&temporary).map_err(|error| {
                    CdfError::data(format!("remove {}: {error}", temporary.display()))
                })?;
                sync_parent_directory(parent)?;
                Ok(AtomicInstallOutcome::Installed)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = fs::read(path).map_err(|read_error| {
                    CdfError::data(format!(
                        "read concurrently installed discovery manifest {}: {read_error}",
                        path.display()
                    ))
                })?;
                if existing != bytes {
                    return Err(CdfError::data(format!(
                        "discovery manifest content-addressed path {} was concurrently installed with different bytes",
                        path.display()
                    )));
                }
                Ok(AtomicInstallOutcome::IdenticalExisting)
            }
            Err(error) => Err(CdfError::data(format!(
                "atomically install discovery manifest {} without replacement: {error}; the target filesystem must support same-directory hard links",
                path.display()
            ))),
        }
    })();
    let _ = fs::remove_file(&temporary);
    result
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> Result<()> {
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| CdfError::data(format!("sync {}: {error}", parent.display())))
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> Result<()> {
    // std does not expose portable directory handles. The temporary file is
    // synced before publication, and hard-link creation remains no-clobber.
    Ok(())
}

fn ensure_single_path_component(value: &str, label: &str) -> Result<()> {
    if value.contains(['/', '\\']) {
        return Err(CdfError::contract(format!(
            "discovery manifest {label} `{value}` must be one path component"
        )));
    }
    let mut components = Path::new(value).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(CdfError::contract(format!(
            "discovery manifest {label} `{value}` must be one path component"
        ))),
    }
}

fn validate_manifest_reference_path(path: &str) -> Result<()> {
    let components = Path::new(path).components().collect::<Vec<_>>();
    if components.len() == 3
        && matches!(components[0], Component::Normal(root) if root == ".cdf")
        && matches!(components[1], Component::Normal(dir) if dir == "schemas")
        && matches!(components[2], Component::Normal(file) if file.to_string_lossy().ends_with(DISCOVERY_MANIFEST_SUFFIX))
    {
        return Ok(());
    }
    Err(CdfError::contract(format!(
        "discovery manifest reference path `{path}` must match {}/<resource>@<hash>{DISCOVERY_MANIFEST_SUFFIX}",
        crate::SCHEMA_SNAPSHOT_DIR
    )))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    #[test]
    fn concurrent_atomic_install_is_no_clobber_and_conflict_detecting() {
        let temp = tempfile::tempdir().unwrap();
        let identical_path = temp.path().join("identical.discovery.json");
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let path = identical_path.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                atomic_write_new(&path, b"identical")
            }));
        }
        barrier.wait();
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == AtomicInstallOutcome::Installed)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == AtomicInstallOutcome::IdenticalExisting)
                .count(),
            1
        );
        assert_eq!(fs::read(&identical_path).unwrap(), b"identical");

        let conflicting_path = temp.path().join("conflicting.discovery.json");
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for bytes in [b"first".as_slice(), b"second".as_slice()] {
            let path = conflicting_path.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                atomic_write_new(&path, bytes)
            }));
        }
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let error = results
            .iter()
            .find_map(|result| result.as_ref().err())
            .unwrap()
            .to_string();
        assert!(error.contains("concurrently installed with different bytes"));
        let installed = fs::read(&conflicting_path).unwrap();
        assert!(installed == b"first" || installed == b"second");
        assert!(fs::read_dir(temp.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
    }
}
