use std::collections::BTreeMap;

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{
    async_types::{BatchStream, BoxFuture},
    canonical_arrow::CanonicalArrowField,
    destination::DeliveryGuarantee,
    error::{CdfError, Result},
    ids::{ContractRef, PartitionId, PlanId, PredicateId, ResourceId, SchemaHash},
    position::SourcePosition,
    scope::{ScopeKey, ScopeKind},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDescriptor {
    pub resource_id: ResourceId,
    pub schema_source: SchemaSource,
    pub primary_key: Vec<String>,
    pub merge_key: Vec<String>,
    pub cursor: Option<CursorSpec>,
    pub write_disposition: WriteDisposition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deduplication: Option<DeduplicationSpec>,
    pub contract: Option<ContractRef>,
    pub state_scope: ScopeKey,
    pub freshness: Option<FreshnessSpec>,
    pub trust_level: TrustLevel,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeduplicationSpec {
    ExactRow,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaSource {
    Declared {
        schema_hash: SchemaHash,
        source: String,
    },
    Discover,
    Discovered {
        snapshot: SchemaSnapshotReference,
    },
    Hints {
        source: String,
        hints_hash: Option<SchemaHash>,
        snapshot: Option<SchemaSnapshotReference>,
    },
    Contract {
        contract: ContractRef,
        schema_hash: Option<SchemaHash>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshotReference {
    pub schema_hash: SchemaHash,
    pub path: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryManifestReference {
    pub manifest_hash: crate::DiscoveryManifestHash,
    pub path: String,
}

pub const DISCOVERY_MANIFEST_HASH_METADATA_KEY: &str = "cdf:discovery_manifest_hash";
pub const DISCOVERY_MANIFEST_PATH_METADATA_KEY: &str = "cdf:discovery_manifest_path";

impl SchemaSnapshotReference {
    pub fn discovery_manifest(&self) -> Result<Option<DiscoveryManifestReference>> {
        discovery_manifest_from_metadata(&self.metadata)
    }

    pub fn with_discovery_manifest(
        mut self,
        manifest: &DiscoveryManifestReference,
    ) -> Result<Self> {
        insert_discovery_manifest_metadata(&mut self.metadata, manifest)?;
        Ok(self)
    }
}

pub fn discovery_manifest_from_metadata(
    metadata: &BTreeMap<String, String>,
) -> Result<Option<DiscoveryManifestReference>> {
    match (
        metadata.get(DISCOVERY_MANIFEST_HASH_METADATA_KEY),
        metadata.get(DISCOVERY_MANIFEST_PATH_METADATA_KEY),
    ) {
        (None, None) => Ok(None),
        (Some(hash), Some(path)) => Ok(Some(DiscoveryManifestReference {
            manifest_hash: crate::DiscoveryManifestHash::new(hash.clone())?,
            path: path.clone(),
        })),
        _ => Err(crate::CdfError::contract(format!(
            "schema snapshot discovery manifest metadata requires both `{DISCOVERY_MANIFEST_HASH_METADATA_KEY}` and `{DISCOVERY_MANIFEST_PATH_METADATA_KEY}`"
        ))),
    }
}

pub fn insert_discovery_manifest_metadata(
    metadata: &mut BTreeMap<String, String>,
    manifest: &DiscoveryManifestReference,
) -> Result<()> {
    if metadata.contains_key(DISCOVERY_MANIFEST_HASH_METADATA_KEY)
        || metadata.contains_key(DISCOVERY_MANIFEST_PATH_METADATA_KEY)
    {
        return Err(crate::CdfError::contract(
            "schema snapshot metadata already contains a discovery manifest reference",
        ));
    }
    metadata.insert(
        DISCOVERY_MANIFEST_HASH_METADATA_KEY.to_owned(),
        manifest.manifest_hash.to_string(),
    );
    metadata.insert(
        DISCOVERY_MANIFEST_PATH_METADATA_KEY.to_owned(),
        manifest.path.clone(),
    );
    Ok(())
}

impl SchemaSource {
    pub fn pinned_snapshot(&self) -> Option<&SchemaSnapshotReference> {
        match self {
            SchemaSource::Discovered { snapshot } => Some(snapshot),
            SchemaSource::Hints {
                snapshot: Some(snapshot),
                ..
            } => Some(snapshot),
            SchemaSource::Declared { .. }
            | SchemaSource::Discover
            | SchemaSource::Hints { snapshot: None, .. }
            | SchemaSource::Contract { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorSpec {
    pub field: String,
    pub ordering: CursorOrderingClaim,
    pub lag_tolerance_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorOrderingClaim {
    Exact,
    Inexact,
    Unordered,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshnessSpec {
    pub max_age_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    Experimental,
    Governed,
    Financial,
    Serving,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteDisposition {
    Append,
    Replace,
    Merge,
    CdcApply,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceCapabilities {
    pub projection: CapabilitySupport,
    pub filters: FilterCapabilities,
    pub limits: CapabilitySupport,
    pub ordering: CapabilitySupport,
    pub partitioning: PartitioningCapabilities,
    pub incremental: IncrementalShape,
    pub replay: ReplaySupport,
    pub idempotent_reads: bool,
    pub backpressure: BackpressureSupport,
    pub estimates: EstimateSupport,
}

impl Default for ResourceCapabilities {
    fn default() -> Self {
        Self {
            projection: CapabilitySupport::Unsupported,
            filters: FilterCapabilities::default(),
            limits: CapabilitySupport::Unsupported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities::default(),
            incremental: IncrementalShape::Full,
            replay: ReplaySupport::None,
            idempotent_reads: false,
            backpressure: BackpressureSupport::CannotPause,
            estimates: EstimateSupport::None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySupport {
    Supported,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterCapabilities {
    pub default_fidelity: PushdownFidelity,
    pub supported_operators: Vec<String>,
}

impl Default for FilterCapabilities {
    fn default() -> Self {
        Self {
            default_fidelity: PushdownFidelity::Unsupported,
            supported_operators: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitioningCapabilities {
    pub parallel_partitions: bool,
    pub supported_scopes: Vec<ScopeKind>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PushdownFidelity {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncrementalShape {
    Full,
    Cursor,
    Log,
    File,
    PageToken,
    Cdc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplaySupport {
    None,
    FromPosition,
    ExactRecordedBatches,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureSupport {
    Pausable,
    SpillRequired,
    CannotPause,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EstimateSupport {
    None,
    Rows,
    Bytes,
    RowsAndBytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanRequest {
    pub resource_id: ResourceId,
    pub projection: Option<Vec<String>>,
    pub filters: Vec<ScanPredicate>,
    pub limit: Option<u64>,
    pub order_by: Vec<OrderBy>,
    pub scope: ScopeKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanPredicate {
    pub predicate_id: PredicateId,
    pub expression: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBy {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionPlan {
    pub partition_id: PartitionId,
    pub scope: ScopeKey,
    pub start_position: Option<SourcePosition>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanPlan {
    pub plan_id: PlanId,
    pub request: ScanRequest,
    pub partitions: Vec<PartitionPlan>,
    pub pushed_predicates: Vec<PushedPredicate>,
    pub unsupported_predicates: Vec<ScanPredicate>,
    pub estimated_rows: Option<u64>,
    pub estimated_bytes: Option<u64>,
    pub delivery_guarantee: DeliveryGuarantee,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushedPredicate {
    pub predicate: ScanPredicate,
    pub fidelity: PushdownFidelity,
}

pub const EFFECTIVE_SCHEMA_EVIDENCE_VERSION: u16 = 1;
pub const PLAN_SCHEMA_OBSERVATION_ID_KEY: &str = "cdf:schema_observation_id";
pub const PLAN_SCHEMA_OBSERVATION_BINDING_KEY: &str = "cdf:schema_observation_binding";
pub const PLAN_PHYSICAL_SCHEMA_HASH_KEY: &str = "cdf:physical_schema_hash";

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionAttestation {
    processed_position: SourcePosition,
    physical_schema_hash: Option<SchemaHash>,
}

impl PartitionAttestation {
    pub fn new(
        processed_position: SourcePosition,
        physical_schema_hash: Option<SchemaHash>,
    ) -> Self {
        Self {
            processed_position,
            physical_schema_hash,
        }
    }

    pub fn processed_position(&self) -> &SourcePosition {
        &self.processed_position
    }

    pub fn into_processed_position(self) -> SourcePosition {
        self.processed_position
    }

    pub fn physical_schema_hash(&self) -> Option<&SchemaHash> {
        self.physical_schema_hash.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ProcessedObservationOutcome {
    Admitted,
    Quarantined,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessedObservationPosition {
    pub observation_id: String,
    pub outcome: ProcessedObservationOutcome,
    pub source_position: SourcePosition,
}

impl ProcessedObservationPosition {
    pub fn new(
        observation_id: impl Into<String>,
        outcome: ProcessedObservationOutcome,
        source_position: SourcePosition,
    ) -> Result<Self> {
        let observation_id = observation_id.into();
        if observation_id.is_empty() {
            return Err(CdfError::contract(
                "processed observation identity cannot be empty",
            ));
        }
        Ok(Self {
            observation_id,
            outcome,
            source_position,
        })
    }
}

pub fn aggregate_processed_observation_positions(
    input: Option<&SourcePosition>,
    observations: &[ProcessedObservationPosition],
    disposition: &WriteDisposition,
) -> Result<SourcePosition> {
    let positions = observations
        .iter()
        .map(|observation| observation.source_position.clone())
        .collect::<Vec<_>>();
    crate::aggregate_position_set("processed observations", input, &positions, disposition)
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchemaObservationScope {
    FieldPath { path: Vec<String> },
    WholeSchema,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaObservationFieldQuarantine {
    scope: SchemaObservationScope,
    observed_field: Option<CanonicalArrowField>,
    effective_field: Option<CanonicalArrowField>,
    reason: String,
}

impl SchemaObservationFieldQuarantine {
    pub fn new_field_path(
        path: Vec<String>,
        observed_field: Option<CanonicalArrowField>,
        effective_field: Option<CanonicalArrowField>,
        reason: impl Into<String>,
    ) -> Result<Self> {
        Self::new(
            SchemaObservationScope::FieldPath { path },
            observed_field,
            effective_field,
            reason,
        )
    }

    pub fn whole_schema(reason: impl Into<String>) -> Result<Self> {
        Self::new(SchemaObservationScope::WholeSchema, None, None, reason)
    }

    fn new(
        scope: SchemaObservationScope,
        observed_field: Option<CanonicalArrowField>,
        effective_field: Option<CanonicalArrowField>,
        reason: impl Into<String>,
    ) -> Result<Self> {
        let fact = Self {
            scope,
            observed_field,
            effective_field,
            reason: reason.into(),
        };
        fact.validate()?;
        Ok(fact)
    }

    pub fn scope(&self) -> &SchemaObservationScope {
        &self.scope
    }

    pub fn observed_field(&self) -> Option<&CanonicalArrowField> {
        self.observed_field.as_ref()
    }

    pub fn effective_field(&self) -> Option<&CanonicalArrowField> {
        self.effective_field.as_ref()
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn validate(&self) -> Result<()> {
        if self.reason.trim().is_empty() {
            return Err(CdfError::contract(
                "schema-observation quarantine reason cannot be empty",
            ));
        }
        match &self.scope {
            SchemaObservationScope::FieldPath { path } => {
                if path.is_empty() || path.iter().any(|component| component.trim().is_empty()) {
                    return Err(CdfError::contract(
                        "schema-observation field quarantine requires a non-empty field path",
                    ));
                }
                if self.observed_field.is_none() && self.effective_field.is_none() {
                    return Err(CdfError::contract(
                        "field-scoped schema-observation quarantine requires an observed or effective exact Arrow field",
                    ));
                }
            }
            SchemaObservationScope::WholeSchema => {
                if self.observed_field.is_some() || self.effective_field.is_some() {
                    return Err(CdfError::contract(
                        "whole-schema observation quarantine cannot carry field-scoped evidence",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaObservationPolicy {
    Evolve,
    Freeze,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalSchemaObservationQuarantine {
    observation_id: String,
    physical_schema_hash: SchemaHash,
    rule_id: String,
    error_code: String,
    policy: SchemaObservationPolicy,
    remediation: String,
    fields: Vec<SchemaObservationFieldQuarantine>,
}

impl TerminalSchemaObservationQuarantine {
    pub fn new(
        observation_id: impl Into<String>,
        physical_schema_hash: SchemaHash,
        rule_id: impl Into<String>,
        error_code: impl Into<String>,
        policy: SchemaObservationPolicy,
        remediation: impl Into<String>,
        fields: Vec<SchemaObservationFieldQuarantine>,
    ) -> Result<Self> {
        let quarantine = Self {
            observation_id: observation_id.into(),
            physical_schema_hash,
            rule_id: rule_id.into(),
            error_code: error_code.into(),
            policy,
            remediation: remediation.into(),
            fields,
        };
        quarantine.validate()?;
        Ok(quarantine)
    }

    pub fn validate(&self) -> Result<()> {
        if self.observation_id.is_empty()
            || self.rule_id.is_empty()
            || self.error_code.is_empty()
            || self.remediation.is_empty()
            || self.fields.is_empty()
        {
            return Err(CdfError::contract(
                "terminal schema-observation quarantine requires identity, rule, policy, remediation, and field evidence",
            ));
        }
        for field in &self.fields {
            field.validate()?;
        }
        Ok(())
    }

    pub fn observation_id(&self) -> &str {
        &self.observation_id
    }

    pub fn physical_schema_hash(&self) -> &SchemaHash {
        &self.physical_schema_hash
    }

    pub fn rule_id(&self) -> &str {
        &self.rule_id
    }

    pub fn error_code(&self) -> &str {
        &self.error_code
    }

    pub fn policy(&self) -> &SchemaObservationPolicy {
        &self.policy
    }

    pub fn remediation(&self) -> &str {
        &self.remediation
    }

    pub fn fields(&self) -> &[SchemaObservationFieldQuarantine] {
        &self.fields
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveSchemaObservationEvidence {
    pub observation_id: String,
    pub physical_schema_hash: SchemaHash,
}

impl EffectiveSchemaObservationEvidence {
    pub fn new(observation_id: impl Into<String>, physical_schema_hash: SchemaHash) -> Self {
        Self {
            observation_id: observation_id.into(),
            physical_schema_hash,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectiveSchemaCatalogEntry {
    pub physical_schema_hash: SchemaHash,
    pub schema: SchemaRef,
}

impl EffectiveSchemaCatalogEntry {
    pub fn new(physical_schema_hash: SchemaHash, schema: SchemaRef) -> Self {
        Self {
            physical_schema_hash,
            schema,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveSchemaEvidence {
    pub version: u16,
    pub baseline_snapshot: SchemaSnapshotReference,
    pub effective_snapshot_schema_hash: SchemaHash,
    pub discovery_manifest: DiscoveryManifestReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_coverage: Option<DiscoveryCoverageEvidence>,
    pub observations: Vec<EffectiveSchemaObservationEvidence>,
}

impl EffectiveSchemaEvidence {
    pub fn new(
        baseline_snapshot: SchemaSnapshotReference,
        effective_snapshot_schema_hash: SchemaHash,
        discovery_manifest: DiscoveryManifestReference,
        mut observations: Vec<EffectiveSchemaObservationEvidence>,
    ) -> Result<Self> {
        observations.sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
        let evidence = Self {
            version: EFFECTIVE_SCHEMA_EVIDENCE_VERSION,
            baseline_snapshot,
            effective_snapshot_schema_hash,
            discovery_manifest,
            discovery_coverage: None,
            observations,
        };
        evidence.validate_intrinsic()?;
        Ok(evidence)
    }

    pub fn observations(&self) -> &[EffectiveSchemaObservationEvidence] {
        &self.observations
    }

    pub fn with_discovery_coverage(mut self, coverage: DiscoveryCoverageEvidence) -> Result<Self> {
        coverage.validate()?;
        self.discovery_coverage = Some(coverage);
        Ok(self)
    }

    pub fn validate_intrinsic(&self) -> Result<()> {
        if self.version != EFFECTIVE_SCHEMA_EVIDENCE_VERSION {
            return Err(CdfError::data(format!(
                "effective schema evidence uses unsupported version {}; expected {}",
                self.version, EFFECTIVE_SCHEMA_EVIDENCE_VERSION
            )));
        }
        if let Some(coverage) = &self.discovery_coverage {
            coverage.validate()?;
        }
        let mut previous = None::<&str>;
        for observation in &self.observations {
            if observation.observation_id.is_empty() {
                return Err(CdfError::data(
                    "effective schema evidence contains an empty observation identity",
                ));
            }
            if previous.is_some_and(|value| value >= observation.observation_id.as_str()) {
                return Err(CdfError::data(
                    "effective schema observations are not in unique identity order",
                ));
            }
            previous = Some(&observation.observation_id);
        }
        Ok(())
    }

    pub fn validate_for_resource(&self, descriptor: &ResourceDescriptor) -> Result<()> {
        self.validate_intrinsic()?;
        if descriptor.schema_source.pinned_snapshot() != Some(&self.baseline_snapshot) {
            return Err(CdfError::data(
                "effective schema evidence baseline does not match the resource's pinned snapshot",
            ));
        }
        Ok(())
    }

    pub fn observation(&self, identity: &str) -> Option<&EffectiveSchemaObservationEvidence> {
        self.observations
            .binary_search_by(|observation| observation.observation_id.as_str().cmp(identity))
            .ok()
            .map(|index| &self.observations[index])
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryCoverageEvidence {
    pub version: u16,
    pub coverage: String,
    pub selector: String,
    pub sample_files: u64,
    pub matched_files: u64,
    pub probed_files: u64,
    pub unprobed_files: u64,
}

impl DiscoveryCoverageEvidence {
    pub fn sampled(
        selector: impl Into<String>,
        sample_files: u64,
        matched_files: u64,
        probed_files: u64,
    ) -> Result<Self> {
        let unprobed_files = matched_files.checked_sub(probed_files).ok_or_else(|| {
            CdfError::contract("sampled discovery coverage probed count exceeds matched count")
        })?;
        let evidence = Self {
            version: 1,
            coverage: "sampled".to_owned(),
            selector: selector.into(),
            sample_files,
            matched_files,
            probed_files,
            unprobed_files,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.coverage != "sampled"
            || self.selector.trim().is_empty()
            || self.sample_files == 0
            || self.probed_files != self.sample_files
            || self.matched_files <= self.probed_files
            || self.probed_files.checked_add(self.unprobed_files) != Some(self.matched_files)
        {
            return Err(CdfError::data(
                "sampled discovery coverage evidence requires version 1, a selector, positive exact sample membership, and matched = probed + unprobed",
            ));
        }
        Ok(())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectiveSchemaRuntime {
    pub evidence: EffectiveSchemaEvidence,
    pub schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    pub terminal_quarantines: Vec<TerminalSchemaObservationQuarantine>,
    pub discovery_executor_budget: Option<DiscoveryExecutorBudgetEvidence>,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryExecutorBudgetEvidence {
    pub max_metadata_bytes_per_file: u64,
    pub max_total_in_flight_bytes: u64,
    pub max_concurrent_probes: u32,
}

impl DiscoveryExecutorBudgetEvidence {
    pub fn new(
        max_metadata_bytes_per_file: u64,
        max_total_in_flight_bytes: u64,
        max_concurrent_probes: u32,
    ) -> Result<Self> {
        if max_metadata_bytes_per_file == 0
            || max_total_in_flight_bytes == 0
            || max_concurrent_probes == 0
            || max_metadata_bytes_per_file > max_total_in_flight_bytes
        {
            return Err(CdfError::contract(
                "discovery executor budget requires positive limits and per-file bytes no greater than total in-flight bytes",
            ));
        }
        max_metadata_bytes_per_file
            .checked_mul(u64::from(max_concurrent_probes))
            .ok_or_else(|| {
                CdfError::contract("discovery executor budget byte accounting overflowed")
            })?;
        Ok(Self {
            max_metadata_bytes_per_file,
            max_total_in_flight_bytes,
            max_concurrent_probes,
        })
    }
}

impl EffectiveSchemaRuntime {
    pub fn new(
        evidence: EffectiveSchemaEvidence,
        mut schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    ) -> Result<Self> {
        schema_catalog
            .sort_by(|left, right| left.physical_schema_hash.cmp(&right.physical_schema_hash));
        let runtime = Self {
            evidence,
            schema_catalog,
            terminal_quarantines: Vec::new(),
            discovery_executor_budget: None,
        };
        runtime.validate_intrinsic()?;
        Ok(runtime)
    }

    pub fn with_terminal_quarantines(
        mut self,
        mut terminal_quarantines: Vec<TerminalSchemaObservationQuarantine>,
    ) -> Result<Self> {
        terminal_quarantines.sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
        self.terminal_quarantines = terminal_quarantines;
        self.validate_intrinsic()?;
        Ok(self)
    }

    pub fn with_discovery_executor_budget(
        mut self,
        budget: DiscoveryExecutorBudgetEvidence,
    ) -> Result<Self> {
        DiscoveryExecutorBudgetEvidence::new(
            budget.max_metadata_bytes_per_file,
            budget.max_total_in_flight_bytes,
            budget.max_concurrent_probes,
        )?;
        self.discovery_executor_budget = Some(budget);
        self.validate_intrinsic()?;
        Ok(self)
    }

    pub fn terminal_quarantine(
        &self,
        observation_id: &str,
    ) -> Option<&TerminalSchemaObservationQuarantine> {
        self.terminal_quarantines
            .binary_search_by(|item| item.observation_id.as_str().cmp(observation_id))
            .ok()
            .map(|index| &self.terminal_quarantines[index])
    }

    pub fn schema_catalog(&self) -> &[EffectiveSchemaCatalogEntry] {
        &self.schema_catalog
    }

    pub fn validate_intrinsic(&self) -> Result<()> {
        self.evidence.validate_intrinsic()?;
        let mut previous = None::<&SchemaHash>;
        for physical in &self.schema_catalog {
            if previous.is_some_and(|value| value >= &physical.physical_schema_hash) {
                return Err(CdfError::data(
                    "effective schema physical catalog is not in unique hash order",
                ));
            }
            previous = Some(&physical.physical_schema_hash);
        }
        for observation in &self.evidence.observations {
            if self
                .physical_schema(&observation.physical_schema_hash)
                .is_none()
            {
                return Err(CdfError::data(format!(
                    "effective schema observation {:?} references absent physical schema {}",
                    observation.observation_id, observation.physical_schema_hash
                )));
            }
        }
        let mut previous_quarantine = None::<&str>;
        for quarantine in &self.terminal_quarantines {
            quarantine.validate()?;
            if previous_quarantine
                .is_some_and(|previous| previous >= quarantine.observation_id.as_str())
            {
                return Err(CdfError::data(
                    "terminal schema-observation quarantines are not in unique identity order",
                ));
            }
            let observation = self
                .evidence
                .observation(&quarantine.observation_id)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "terminal quarantine references absent observation {:?}",
                        quarantine.observation_id
                    ))
                })?;
            if observation.physical_schema_hash != quarantine.physical_schema_hash {
                return Err(CdfError::data(format!(
                    "terminal quarantine physical schema {} does not match observation {}",
                    quarantine.physical_schema_hash, observation.physical_schema_hash
                )));
            }
            previous_quarantine = Some(&quarantine.observation_id);
        }
        Ok(())
    }

    pub fn validate_for_resource(&self, descriptor: &ResourceDescriptor) -> Result<()> {
        self.validate_intrinsic()?;
        self.evidence.validate_for_resource(descriptor)
    }

    pub fn physical_schema(&self, hash: &SchemaHash) -> Option<&SchemaRef> {
        self.schema_catalog
            .binary_search_by(|schema| schema.physical_schema_hash.cmp(hash))
            .ok()
            .map(|index| &self.schema_catalog[index].schema)
    }
}

pub trait ResourceStream {
    fn descriptor(&self) -> &ResourceDescriptor;
    fn schema(&self) -> SchemaRef;
    /// Validates adapter-owned runtime dependencies before orchestration starts.
    /// Generic orchestration calls this hook without knowing the source kind.
    fn validate_runtime_dependencies(&self) -> Result<()> {
        Ok(())
    }
    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>>;
    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>>;
    /// Revalidates a planned observation without opening its payload and returns
    /// the exact source position safe to mark processed. Adapters with mutable
    /// external identity MUST override this method.
    fn attest_partition(
        &self,
        _partition: &PartitionPlan,
    ) -> BoxFuture<'_, Result<Option<PartitionAttestation>>> {
        Box::pin(async { Ok(None) })
    }
    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        None
    }
    fn type_policy_allowances(&self) -> TypePolicyAllowances {
        TypePolicyAllowances::default()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypePolicyAllowances {
    pub coerce_types: bool,
    pub allow_lossy_mapping: bool,
}

pub trait QueryableResource: ResourceStream {
    fn capabilities(&self) -> &ResourceCapabilities;
    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan>;
}
