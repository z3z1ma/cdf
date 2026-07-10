use std::collections::BTreeMap;

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{
    async_types::{BatchStream, BoxFuture},
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
    pub contract: Option<ContractRef>,
    pub state_scope: ScopeKey,
    pub freshness: Option<FreshnessSpec>,
    pub trust_level: TrustLevel,
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
pub const PLAN_PHYSICAL_SCHEMA_HASH_KEY: &str = "cdf:physical_schema_hash";

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
#[derive(Clone, Debug, PartialEq)]
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
            observations,
        };
        evidence.validate_intrinsic()?;
        Ok(evidence)
    }

    pub fn observations(&self) -> &[EffectiveSchemaObservationEvidence] {
        &self.observations
    }

    pub fn validate_intrinsic(&self) -> Result<()> {
        if self.version != EFFECTIVE_SCHEMA_EVIDENCE_VERSION {
            return Err(CdfError::data(format!(
                "effective schema evidence uses unsupported version {}; expected {}",
                self.version, EFFECTIVE_SCHEMA_EVIDENCE_VERSION
            )));
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
#[derive(Clone, Debug, PartialEq)]
pub struct EffectiveSchemaRuntime {
    pub evidence: EffectiveSchemaEvidence,
    pub schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
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
        };
        runtime.validate_intrinsic()?;
        Ok(runtime)
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
    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>>;
    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>>;
    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        None
    }
}

pub trait QueryableResource: ResourceStream {
    fn capabilities(&self) -> &ResourceCapabilities;
    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan>;
}
