use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_schema::SchemaRef;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt, future::Shared, stream};
use serde::{Deserialize, Serialize};

use crate::{
    async_types::{BatchStream, BoxFuture},
    canonical_arrow::CanonicalArrowField,
    destination::DeliveryGuarantee,
    error::{CdfError, Result},
    ids::{
        ContentObjectKey, ContentProviderGeneration, ContentStoreNamespace, ContractRef,
        PartitionId, PlanId, PredicateId, ResourceId, SchemaHash,
    },
    position::{FilePosition, SourcePosition},
    retention::PayloadRetention,
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

impl ResourceDescriptor {
    pub fn validate(&self) -> Result<()> {
        ResourceId::new(self.resource_id.as_str())?;
        validate_schema_source(&self.schema_source)?;
        validate_resource_fields("primary key", &self.primary_key)?;
        validate_resource_fields("merge key", &self.merge_key)?;
        if self.write_disposition == WriteDisposition::Merge && self.merge_key.is_empty() {
            return Err(CdfError::contract(format!(
                "resource `{}` uses merge disposition without a merge key",
                self.resource_id
            )));
        }
        if self.deduplication.is_some() && self.write_disposition != WriteDisposition::Append {
            return Err(CdfError::contract(
                "exact-row deduplication is valid only for append disposition",
            ));
        }
        if let Some(cursor) = &self.cursor {
            validate_resource_token("cursor field", &cursor.field)?;
        }
        if let Some(contract) = &self.contract {
            ContractRef::new(contract.as_str())?;
        }
        validate_scope_key(&self.state_scope)?;
        if self
            .freshness
            .as_ref()
            .is_some_and(|freshness| freshness.max_age_ms == 0)
        {
            return Err(CdfError::contract(
                "resource freshness maximum age must be greater than zero",
            ));
        }
        Ok(())
    }
}

fn validate_schema_source(source: &SchemaSource) -> Result<()> {
    match source {
        SchemaSource::Declared {
            schema_hash,
            source,
        } => {
            SchemaHash::new(schema_hash.as_str())?;
            validate_resource_token("declared schema source", source)
        }
        SchemaSource::Discover => Ok(()),
        SchemaSource::Discovered { snapshot } => validate_schema_snapshot(snapshot),
        SchemaSource::Hints {
            source,
            hints_hash,
            snapshot,
        } => {
            validate_resource_token("schema hints source", source)?;
            if let Some(hash) = hints_hash {
                SchemaHash::new(hash.as_str())?;
            }
            if let Some(snapshot) = snapshot {
                validate_schema_snapshot(snapshot)?;
            }
            Ok(())
        }
        SchemaSource::Contract {
            contract,
            schema_hash,
        } => {
            ContractRef::new(contract.as_str())?;
            if let Some(hash) = schema_hash {
                SchemaHash::new(hash.as_str())?;
            }
            Ok(())
        }
    }
}

fn validate_schema_snapshot(snapshot: &SchemaSnapshotReference) -> Result<()> {
    SchemaHash::new(snapshot.schema_hash.as_str())?;
    validate_resource_token("schema snapshot path", &snapshot.path)?;
    for (key, value) in &snapshot.metadata {
        validate_resource_token("schema snapshot metadata key", key)?;
        validate_resource_token("schema snapshot metadata value", value)?;
    }
    snapshot.discovery_manifest()?;
    Ok(())
}

fn validate_resource_fields(label: &str, fields: &[String]) -> Result<()> {
    let mut unique = std::collections::BTreeSet::new();
    for field in fields {
        validate_resource_token(label, field)?;
        if !unique.insert(field) {
            return Err(CdfError::contract(format!(
                "resource {label} fields must be unique"
            )));
        }
    }
    Ok(())
}

fn validate_resource_token(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(CdfError::contract(format!(
            "resource {label} must be non-empty and control-free"
        )));
    }
    Ok(())
}

fn validate_scope_key(scope: &ScopeKey) -> Result<()> {
    match scope {
        ScopeKey::Resource => Ok(()),
        ScopeKey::Partition { partition_id } => PartitionId::new(partition_id.as_str()).map(drop),
        ScopeKey::Window { start, end } => {
            validate_resource_token("window start", start)?;
            validate_resource_token("window end", end)
        }
        ScopeKey::File { path } => validate_resource_token("file scope path", path),
        ScopeKey::Stream { name } => validate_resource_token("stream scope name", name),
        ScopeKey::SchemaContract { contract } => ContractRef::new(contract.as_str()).map(drop),
        ScopeKey::DestinationLoad {
            destination,
            target,
        } => {
            crate::DestinationId::new(destination.as_str())?;
            crate::TargetName::new(target.as_str()).map(drop)
        }
        ScopeKey::Composite { parts } => {
            if parts.is_empty() {
                return Err(CdfError::contract(
                    "composite resource scope requires at least one part",
                ));
            }
            for part in parts {
                validate_scope_key(part)?;
            }
            Ok(())
        }
    }
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
    pub fn with_pinned_snapshot(&self, snapshot: SchemaSnapshotReference) -> Option<SchemaSource> {
        match self {
            SchemaSource::Discover | SchemaSource::Discovered { .. } => {
                Some(SchemaSource::Discovered { snapshot })
            }
            SchemaSource::Hints {
                source, hints_hash, ..
            } => Some(SchemaSource::Hints {
                source: source.clone(),
                hints_hash: hints_hash.clone(),
                snapshot: Some(snapshot),
            }),
            SchemaSource::Declared { .. } | SchemaSource::Contract { .. } => None,
        }
    }

    pub fn without_pinned_snapshot(&self) -> Option<SchemaSource> {
        match self {
            SchemaSource::Discover | SchemaSource::Discovered { .. } => {
                Some(SchemaSource::Discover)
            }
            SchemaSource::Hints {
                source, hints_hash, ..
            } => Some(SchemaSource::Hints {
                source: source.clone(),
                hints_hash: hints_hash.clone(),
                snapshot: None,
            }),
            SchemaSource::Declared { .. } | SchemaSource::Contract { .. } => None,
        }
    }

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

    pub fn baseline_reference(&self) -> Option<SchemaBaselineReference> {
        match self {
            SchemaSource::Declared {
                schema_hash,
                source,
            } => Some(SchemaBaselineReference::Declared {
                schema_hash: schema_hash.clone(),
                source: source.clone(),
            }),
            SchemaSource::Discovered { snapshot }
            | SchemaSource::Hints {
                snapshot: Some(snapshot),
                ..
            } => Some(SchemaBaselineReference::Pinned {
                snapshot: snapshot.clone(),
            }),
            SchemaSource::Discover
            | SchemaSource::Hints { snapshot: None, .. }
            | SchemaSource::Contract { .. } => None,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaBaselineReference {
    Declared {
        schema_hash: SchemaHash,
        source: String,
    },
    Pinned {
        snapshot: SchemaSnapshotReference,
    },
}

impl SchemaBaselineReference {
    pub fn schema_hash(&self) -> &SchemaHash {
        match self {
            Self::Declared { schema_hash, .. } => schema_hash,
            Self::Pinned { snapshot } => &snapshot.schema_hash,
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

impl ResourceCapabilities {
    pub fn validate(&self) -> Result<()> {
        let mut operators = std::collections::BTreeSet::new();
        for operator in &self.filters.supported_operators {
            validate_resource_token("filter operator", operator)?;
            if !operators.insert(operator) {
                return Err(CdfError::contract(
                    "resource filter operators must be unique",
                ));
            }
        }
        if self.filters.default_fidelity == PushdownFidelity::Unsupported
            && !self.filters.supported_operators.is_empty()
        {
            return Err(CdfError::contract(
                "resource cannot advertise filter operators with unsupported pushdown fidelity",
            ));
        }
        let mut scopes = Vec::new();
        for scope in &self.partitioning.supported_scopes {
            if scopes.contains(scope) {
                return Err(CdfError::contract(
                    "resource partition scopes must be unique",
                ));
            }
            scopes.push(scope.clone());
        }
        if self.partitioning.parallel_partitions && self.partitioning.supported_scopes.is_empty() {
            return Err(CdfError::contract(
                "parallel resource partitioning requires at least one supported scope",
            ));
        }
        Ok(())
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
    TableSnapshot,
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
    /// Original operator text retained only for diagnostics and evidence.
    pub expression: String,
    /// Canonical source/engine execution authority parsed once at the trust boundary.
    pub canonical_expression: crate::Expression,
}

impl ScanPredicate {
    pub fn new(predicate_id: PredicateId, expression: impl Into<String>) -> Result<Self> {
        let expression = expression.into();
        let canonical_expression = crate::Expression::parse_comparison(&expression)?;
        Ok(Self {
            predicate_id,
            expression,
            canonical_expression,
        })
    }

    pub fn from_expression(
        predicate_id: PredicateId,
        diagnostic: impl Into<String>,
        canonical_expression: crate::Expression,
    ) -> Result<Self> {
        canonical_expression.validate()?;
        Ok(Self {
            predicate_id,
            expression: diagnostic.into(),
            canonical_expression,
        })
    }
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
    /// Position authority from which this exact planned source unit is opened.
    ///
    /// This is distinct from `start_position`: the latter is a resume boundary supplied by
    /// prior state, while this value identifies the complete unit selected by this plan.
    /// Completion may enrich it with terminal evidence but may not change its generation.
    #[serde(deserialize_with = "deserialize_planned_position")]
    pub planned_position: Option<SourcePosition>,
    pub start_position: Option<SourcePosition>,
    pub scan_intent: CompiledScanIntent,
    /// The strongest identity proof available for restarting this exact planned partition.
    pub retry_safety: PartitionRetrySafety,
    pub metadata: BTreeMap<String, String>,
}

fn deserialize_planned_position<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<SourcePosition>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<SourcePosition>::deserialize(deserializer)
}

impl PartitionPlan {
    /// Returns the single file selected by this partition when it is file-positioned.
    ///
    /// A file partition is one retry/checkpoint unit. Multi-file manifests belong to aggregate
    /// state after execution, never to one planned partition.
    pub fn planned_file(&self) -> Result<Option<&FilePosition>> {
        let Some(position) = self.planned_position.as_ref() else {
            return Ok(None);
        };
        let SourcePosition::FileManifest(manifest) = position else {
            return Ok(None);
        };
        if manifest.version != 1 {
            return Err(CdfError::contract(format!(
                "partition `{}` uses unsupported file-manifest version {}",
                self.partition_id, manifest.version
            )));
        }
        let [file] = manifest.files.as_slice() else {
            return Err(CdfError::contract(format!(
                "partition `{}` must plan exactly one file position, found {}",
                self.partition_id,
                manifest.files.len()
            )));
        };
        if self.scope
            != (ScopeKey::File {
                path: file.path.clone(),
            })
        {
            return Err(CdfError::contract(format!(
                "partition `{}` scope does not match planned file `{}`",
                self.partition_id, file.path
            )));
        }
        Ok(Some(file))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionRetrySafety {
    #[default]
    Forbidden,
    ImmutableContent,
    Snapshot,
}

pub const COMPILED_SCAN_INTENT_VERSION: u16 = 1;

/// Source-neutral physical work already negotiated by the engine and frozen
/// for one partition. Source and format adapters consume this artifact; they
/// never reconstruct projection, predicate, limit, or ordering intent from
/// metadata or from destination-specific behavior.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedCompiledScanIntent", deny_unknown_fields)]
pub struct CompiledScanIntent {
    pub version: u16,
    pub projection: Option<Vec<String>>,
    pub predicates: Vec<PushedPredicate>,
    pub limit: Option<u64>,
    pub order_by: Vec<OrderBy>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedCompiledScanIntent {
    version: u16,
    projection: Option<Vec<String>>,
    predicates: Vec<PushedPredicate>,
    limit: Option<u64>,
    order_by: Vec<OrderBy>,
}

impl TryFrom<UncheckedCompiledScanIntent> for CompiledScanIntent {
    type Error = CdfError;

    fn try_from(value: UncheckedCompiledScanIntent) -> Result<Self> {
        let intent = Self {
            version: value.version,
            projection: value.projection,
            predicates: value.predicates,
            limit: value.limit,
            order_by: value.order_by,
        };
        intent.validate()?;
        Ok(intent)
    }
}

impl CompiledScanIntent {
    pub const fn full_scan() -> Self {
        Self {
            version: COMPILED_SCAN_INTENT_VERSION,
            projection: None,
            predicates: Vec::new(),
            limit: None,
            order_by: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != COMPILED_SCAN_INTENT_VERSION {
            return Err(CdfError::contract(format!(
                "compiled scan intent version {} is unsupported; expected version {}",
                self.version, COMPILED_SCAN_INTENT_VERSION
            )));
        }
        if let Some(projection) = &self.projection
            && (projection.is_empty()
                || projection.iter().any(|field| field.trim().is_empty())
                || projection
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != projection.len())
        {
            return Err(CdfError::contract(
                "compiled scan projection requires unique non-empty fields",
            ));
        }
        let mut predicate_ids = std::collections::BTreeSet::new();
        for predicate in &self.predicates {
            if predicate.fidelity == PushdownFidelity::Unsupported {
                return Err(CdfError::contract(
                    "compiled scan intent cannot contain an unsupported pushed predicate",
                ));
            }
            predicate.predicate.canonical_expression.validate()?;
            if !predicate_ids.insert(predicate.predicate.predicate_id.as_str()) {
                return Err(CdfError::contract(
                    "compiled scan intent contains a duplicate predicate id",
                ));
            }
        }
        if self
            .order_by
            .iter()
            .any(|order| order.field.trim().is_empty())
        {
            return Err(CdfError::contract(
                "compiled scan ordering requires non-empty fields",
            ));
        }
        Ok(())
    }

    pub fn pushed_predicates(&self) -> Vec<ScanPredicate> {
        self.predicates
            .iter()
            .map(|pushed| pushed.predicate.clone())
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanPlan {
    pub plan_id: PlanId,
    pub request: ScanRequest,
    pub partitions: Vec<PartitionPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planned_task_set: Option<PlannedTaskSetReference>,
    pub pushed_predicates: Vec<PushedPredicate>,
    pub unsupported_predicates: Vec<ScanPredicate>,
    pub estimated_rows: Option<u64>,
    pub estimated_bytes: Option<u64>,
    pub delivery_guarantee: DeliveryGuarantee,
}

/// Invocation-local, bounded reader for one external canonical partition authority.
///
/// High-cardinality sources use this seam to translate their portable task records into ordinary
/// [`PartitionPlan`] values one canonical ordinal at a time. The engine remains opaque to the
/// task encoding, while the source remains unable to bypass the normal partition scheduler,
/// retry, attestation, schema-admission, and package paths.
pub trait PlannedPartitionReader: Send {
    /// Returns exactly the requested canonical partition, or `None` only at verified end-of-set.
    ///
    /// Calls are strictly increasing from zero. Implementations MUST reject skipped, repeated, or
    /// reordered ordinals and MUST retain no unbounded history of decoded tasks.
    fn next_partition(&mut self, expected_ordinal: u64) -> Result<Option<ExecutablePartition>>;
}

/// One ordinary partition plan paired with source-private, ledger-owned invocation state.
///
/// The opaque retention is never serialized or hashed into plan/package identity. It exists only
/// to carry already-decoded task authority through bounded scheduler lookahead without copying it
/// into generic metadata or forcing a second task-store read. Adapters recover their own type at
/// `open_executable`; all generic code remains limited to the canonical `PartitionPlan`.
#[derive(Clone, Debug)]
pub struct ExecutablePartition {
    plan: PartitionPlan,
    retention: Option<PayloadRetention>,
}

impl ExecutablePartition {
    pub fn inline(plan: PartitionPlan) -> Self {
        Self {
            plan,
            retention: None,
        }
    }

    pub fn retained(plan: PartitionPlan, retention: PayloadRetention) -> Self {
        Self {
            plan,
            retention: Some(retention),
        }
    }

    pub fn plan(&self) -> &PartitionPlan {
        &self.plan
    }

    pub fn into_plan(self) -> PartitionPlan {
        self.plan
    }

    pub fn retention(&self) -> Option<&PayloadRetention> {
        self.retention.as_ref()
    }
}

pub const PLANNED_TASK_SET_REFERENCE_VERSION: u16 = 1;

/// Source-neutral reference to an external canonical partition/task authority.
///
/// High-cardinality sources leave `ScanPlan::partitions` empty and name this artifact instead;
/// they may not retain an unbounded inline fallback. The execution host streams the artifact
/// through its registered content-store authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedTaskSetReference {
    pub version: u16,
    pub task_type: String,
    pub task_count: u64,
    pub store_namespace: ContentStoreNamespace,
    pub object_key: ContentObjectKey,
    pub byte_count: u64,
    pub content_sha256: String,
    pub provider_generation: ContentProviderGeneration,
}

impl PlannedTaskSetReference {
    pub fn validate(&self) -> Result<()> {
        if self.version != PLANNED_TASK_SET_REFERENCE_VERSION {
            return Err(CdfError::contract(format!(
                "planned task-set reference version {} is unsupported; expected {}",
                self.version, PLANNED_TASK_SET_REFERENCE_VERSION
            )));
        }
        if self.task_type.is_empty()
            || !self
                .task_type
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(CdfError::contract(
                "planned task-set type must be a canonical ASCII token",
            ));
        }
        ContentStoreNamespace::new(self.store_namespace.as_str())?;
        ContentObjectKey::new(self.object_key.as_str())?;
        ContentProviderGeneration::new(self.provider_generation.as_str())?;
        if self.byte_count == 0 {
            return Err(CdfError::contract(
                "planned task-set artifact byte count must be nonzero",
            ));
        }
        let path = std::path::Path::new(self.object_key.as_str());
        if path.is_absolute()
            || path.components().any(|component| {
                !matches!(
                    component,
                    std::path::Component::Normal(_) | std::path::Component::CurDir
                )
            })
        {
            return Err(CdfError::contract(
                "planned task-set object key must be a safe relative path",
            ));
        }
        validate_sha256_identity("planned task-set", &self.content_sha256)
    }
}

impl ScanPlan {
    pub fn validate_partition_authority(&self) -> Result<()> {
        if let Some(task_set) = &self.planned_task_set {
            task_set.validate()?;
            if !self.partitions.is_empty() {
                return Err(CdfError::contract(
                    "scan plan cannot carry both external task authority and inline partitions",
                ));
            }
        }
        Ok(())
    }

    pub fn partition_count(&self) -> Result<u64> {
        self.validate_partition_authority()?;
        self.planned_task_set.as_ref().map_or_else(
            || {
                u64::try_from(self.partitions.len())
                    .map_err(|_| CdfError::data("scan partition count exceeds u64"))
            },
            |task_set| Ok(task_set.task_count),
        )
    }
}

fn validate_sha256_identity(label: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CdfError::contract(format!(
            "{label} identity must use sha256:<64 lowercase hex>"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CdfError::contract(format!(
            "{label} identity must use sha256:<64 lowercase hex>"
        )));
    }
    Ok(())
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

pub fn partition_schema_observation_id(partition: &PartitionPlan) -> &str {
    partition
        .metadata
        .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
        .map_or_else(|| partition.partition_id.as_str(), String::as_str)
}

pub fn validate_scan_partition_observation_identities(scan: &ScanPlan) -> Result<()> {
    scan.validate_partition_authority()?;
    let mut partitions_by_observation = BTreeMap::new();
    for partition in &scan.partitions {
        let observation_id = partition_schema_observation_id(partition);
        if observation_id.is_empty() {
            return Err(CdfError::contract(format!(
                "planned partition {:?} carries an empty schema observation identity",
                partition.partition_id
            )));
        }
        if let Some(first_partition) =
            partitions_by_observation.insert(observation_id, partition.partition_id.as_str())
        {
            return Err(CdfError::contract(format!(
                "schema observation {observation_id:?} is assigned to planned partitions {first_partition:?} and {:?}; observation identities must be partition-scoped",
                partition.partition_id
            )));
        }
    }
    Ok(())
}

/// Validates that every partition carries the same compiled source work and
/// that the work is an exact subset of the canonical request classified by
/// the scan plan. This is intentionally source-neutral and is safe to repeat
/// when loading a recorded plan for execution or replay.
pub fn validate_compiled_scan_intents(scan: &ScanPlan) -> Result<()> {
    scan.validate_partition_authority()?;
    let mut expected: Option<&CompiledScanIntent> = None;
    for partition in &scan.partitions {
        let intent = &partition.scan_intent;
        intent.validate()?;
        if let Some(projection) = &intent.projection
            && scan.request.projection.as_ref() != Some(projection)
        {
            return Err(CdfError::contract(format!(
                "partition {} compiled a projection that differs from the canonical scan request",
                partition.partition_id
            )));
        }
        if intent.predicates != scan.pushed_predicates {
            return Err(CdfError::contract(format!(
                "partition {} compiled predicates that differ from source negotiation",
                partition.partition_id
            )));
        }
        if intent.limit.is_some() && intent.limit != scan.request.limit {
            return Err(CdfError::contract(format!(
                "partition {} compiled a limit that differs from the canonical scan request",
                partition.partition_id
            )));
        }
        if !intent.order_by.is_empty() && intent.order_by != scan.request.order_by {
            return Err(CdfError::contract(format!(
                "partition {} compiled ordering that differs from the canonical scan request",
                partition.partition_id
            )));
        }
        if let Some(expected) = expected
            && expected != intent
        {
            return Err(CdfError::contract(
                "source negotiation compiled inconsistent scan intent across partitions",
            ));
        }
        expected = Some(intent);
    }
    Ok(())
}

pub fn partition_source_identity_binding(partition: &PartitionPlan) -> Result<String> {
    if let Some(binding) = partition.metadata.get(PLAN_SCHEMA_OBSERVATION_BINDING_KEY) {
        if binding.is_empty() {
            return Err(CdfError::data(format!(
                "partition {:?} carries an empty source-identity binding",
                partition.partition_id
            )));
        }
        return Ok(binding.clone());
    }
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(&(
        &partition.partition_id,
        &partition.scope,
        &partition.planned_position,
        &partition.start_position,
        &partition.metadata,
    ))
    .map_err(|error| CdfError::internal(error.to_string()))?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionAttestation {
    processed_position: SourcePosition,
    physical_schema_hash: Option<SchemaHash>,
}

/// Invocation-local source I/O measurements. These values are operational
/// telemetry only: they never participate in plan, package, or manifest identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceReadMode {
    /// One forward byte stream feeds a streaming codec or one complete verifier.
    DirectStream,
    /// Seekable decode reads generation-bound byte extents without a full local copy.
    ExactRanges,
    /// Decode begins only after a finite, fully reserved spool is complete.
    FullSpool,
    /// Decode tails already-published extents while a finite, fully reserved spool grows.
    GrowingSpool,
    /// One finite generation streams through fixed disk residency reclaimed by
    /// codec-proven no-lookback frontiers.
    EvictingSpool,
    /// A generation-revalidated local payload whose hash was verified at cache promotion.
    PayloadCache,
    /// One invocation intentionally combines direct verification and exact-range decode.
    MixedAccess,
}

impl SourceReadMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DirectStream => "direct_stream",
            Self::ExactRanges => "exact_ranges",
            Self::FullSpool => "full_spool",
            Self::GrowingSpool => "growing_spool",
            Self::EvictingSpool => "evicting_spool",
            Self::PayloadCache => "payload_cache",
            Self::MixedAccess => "mixed_access",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourceIoMetrics {
    pub mode: Option<SourceReadMode>,
    /// Sum of time awaiting source opens/range responses/stream chunks. Consumer
    /// backpressure between polls is excluded; concurrent range waits may overlap.
    pub duration_ns: u64,
    pub logical_bytes: u64,
    pub useful_bytes: u64,
    pub physical_bytes: u64,
    pub requests: u64,
}

impl SourceIoMetrics {
    pub fn prefetch_waste_bytes(self) -> u64 {
        self.physical_bytes.saturating_sub(self.useful_bytes)
    }

    pub fn reused_bytes(self) -> u64 {
        self.logical_bytes.saturating_sub(self.useful_bytes)
    }

    pub fn is_empty(self) -> bool {
        self.mode.is_none()
            && self.duration_ns == 0
            && self.logical_bytes == 0
            && self.useful_bytes == 0
            && self.physical_bytes == 0
            && self.requests == 0
    }
}

/// EOF-bound outcome of one partition invocation.
///
/// Correctness evidence and operational measurements share the same lifecycle
/// barrier but remain separate values so telemetry cannot affect package identity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionCompletion {
    attestation: Option<PartitionAttestation>,
    source_io: Option<SourceIoMetrics>,
}

impl PartitionCompletion {
    pub fn new(
        attestation: Option<PartitionAttestation>,
        source_io: Option<SourceIoMetrics>,
    ) -> Self {
        Self {
            attestation,
            source_io,
        }
    }

    pub fn attestation(&self) -> Option<&PartitionAttestation> {
        self.attestation.as_ref()
    }

    pub fn into_attestation(self) -> Option<PartitionAttestation> {
        self.attestation
    }

    pub fn source_io(&self) -> Option<SourceIoMetrics> {
        self.source_io
    }
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

    /// Whether this terminal attestation only strengthens an earlier observation.
    ///
    /// File extraction may add a payload SHA-256 that metadata-only preflight could
    /// not know. Every pre-existing identity field and schema hash must remain exact;
    /// no terminal observation may remove or replace prior evidence.
    pub fn is_monotonic_refinement_of(&self, earlier: &Self) -> bool {
        if self.physical_schema_hash != earlier.physical_schema_hash {
            return false;
        }
        match (&self.processed_position, &earlier.processed_position) {
            (SourcePosition::FileManifest(current), SourcePosition::FileManifest(previous)) => {
                current.version == previous.version
                    && current.files.len() == previous.files.len()
                    && current
                        .files
                        .iter()
                        .zip(&previous.files)
                        .all(|(current, previous)| {
                            current.path == previous.path
                                && current.size_bytes == previous.size_bytes
                                && current.source_generation == previous.source_generation
                                && current.etag == previous.etag
                                && current.object_version == previous.object_version
                                && (current.sha256 == previous.sha256
                                    || (previous.sha256.is_none() && current.sha256.is_some()))
                        })
            }
            (current, previous) => current == previous,
        }
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
    source_position: Option<SourcePosition>,
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
            source_position: None,
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

    pub fn source_position(&self) -> Option<&SourcePosition> {
        self.source_position.as_ref()
    }

    pub fn bind_source_position(&mut self, source_position: SourcePosition) -> Result<()> {
        if self
            .source_position
            .as_ref()
            .is_some_and(|existing| existing != &source_position)
        {
            return Err(CdfError::data(format!(
                "schema quarantine {:?} carries conflicting source positions",
                self.observation_id
            )));
        }
        self.source_position = Some(source_position);
        Ok(())
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
    pub baseline: SchemaBaselineReference,
    pub effective_schema_hash: SchemaHash,
    pub discovery_manifest: DiscoveryManifestReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_coverage: Option<DiscoveryCoverageEvidence>,
    pub observations: Vec<EffectiveSchemaObservationEvidence>,
}

impl EffectiveSchemaEvidence {
    pub fn new(
        baseline: SchemaBaselineReference,
        effective_schema_hash: SchemaHash,
        discovery_manifest: DiscoveryManifestReference,
        mut observations: Vec<EffectiveSchemaObservationEvidence>,
    ) -> Result<Self> {
        observations.sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
        let evidence = Self {
            version: EFFECTIVE_SCHEMA_EVIDENCE_VERSION,
            baseline,
            effective_schema_hash,
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
        if descriptor.schema_source.baseline_reference().as_ref() != Some(&self.baseline) {
            return Err(CdfError::data(
                "effective schema evidence baseline does not match the resource schema constraint",
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
    pub file_coverage: String,
    pub within_file_coverage: String,
    pub selector: Option<String>,
    pub sample_files: Option<u64>,
    pub matched_files: u64,
    pub selected_files: u64,
    pub unobserved_files: u64,
    pub observed_bytes: u64,
    pub observed_records: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryCoverageEvidenceInput {
    pub file_coverage: String,
    pub within_file_coverage: String,
    pub selector: Option<String>,
    pub sample_files: Option<u64>,
    pub matched_files: u64,
    pub selected_files: u64,
    pub observed_bytes: u64,
    pub observed_records: u64,
}

impl DiscoveryCoverageEvidence {
    pub fn new(input: DiscoveryCoverageEvidenceInput) -> Result<Self> {
        let DiscoveryCoverageEvidenceInput {
            file_coverage,
            within_file_coverage,
            selector,
            sample_files,
            matched_files,
            selected_files,
            observed_bytes,
            observed_records,
        } = input;
        let unobserved_files = matched_files.checked_sub(selected_files).ok_or_else(|| {
            CdfError::contract("discovery coverage selected count exceeds matched count")
        })?;
        let evidence = Self {
            version: 1,
            file_coverage,
            within_file_coverage,
            selector,
            sample_files,
            matched_files,
            selected_files,
            unobserved_files,
            observed_bytes,
            observed_records,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    pub fn validate(&self) -> Result<()> {
        let file_coverage_valid = match self.file_coverage.as_str() {
            "all_files" => {
                self.selector.is_none()
                    && self.sample_files.is_none()
                    && self.selected_files == self.matched_files
                    && self.unobserved_files == 0
            }
            "sampled_files" => {
                self.selector
                    .as_deref()
                    .is_some_and(|selector| !selector.trim().is_empty())
                    && self.sample_files == Some(self.selected_files)
                    && self.selected_files > 0
                    && self.selected_files < self.matched_files
            }
            _ => false,
        };
        let within_file_coverage_valid = matches!(
            self.within_file_coverage.as_str(),
            "format_metadata" | "bounded_content" | "full_content"
        );
        if self.version != 1
            || !file_coverage_valid
            || !within_file_coverage_valid
            || self.matched_files == 0
            || self.selected_files.checked_add(self.unobserved_files) != Some(self.matched_files)
            || (self.within_file_coverage == "format_metadata" && self.observed_records != 0)
        {
            return Err(CdfError::data(
                "discovery coverage evidence requires valid independent file/within-file axes, exact selection counts, and zero data records for format_metadata",
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
    pub max_bytes_per_file: u64,
    pub max_records_per_file: u64,
    pub max_total_in_flight_bytes: u64,
    pub max_concurrent_probes: u32,
}

impl DiscoveryExecutorBudgetEvidence {
    pub fn new(
        max_bytes_per_file: u64,
        max_records_per_file: u64,
        max_total_in_flight_bytes: u64,
        max_concurrent_probes: u32,
    ) -> Result<Self> {
        if max_bytes_per_file == 0
            || max_records_per_file == 0
            || max_total_in_flight_bytes == 0
            || max_concurrent_probes == 0
            || max_bytes_per_file > max_total_in_flight_bytes
        {
            return Err(CdfError::contract(
                "discovery executor budget requires positive limits and per-file bytes no greater than total in-flight bytes",
            ));
        }
        max_bytes_per_file
            .checked_mul(u64::from(max_concurrent_probes))
            .ok_or_else(|| {
                CdfError::contract("discovery executor budget byte accounting overflowed")
            })?;
        Ok(Self {
            max_bytes_per_file,
            max_records_per_file,
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
            budget.max_bytes_per_file,
            budget.max_records_per_file,
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
            if crate::canonical_arrow_schema_hash(physical.schema.as_ref())?
                != physical.physical_schema_hash
            {
                return Err(CdfError::data(
                    "effective schema physical catalog hash does not match its Arrow schema",
                ));
            }
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
        if let Some(budget) = &self.discovery_executor_budget {
            DiscoveryExecutorBudgetEvidence::new(
                budget.max_bytes_per_file,
                budget.max_records_per_file,
                budget.max_total_in_flight_bytes,
                budget.max_concurrent_probes,
            )?;
        }
        Ok(())
    }

    pub fn validate_for_resource(&self, descriptor: &ResourceDescriptor) -> Result<()> {
        self.validate_intrinsic()?;
        self.evidence.validate_for_resource(descriptor)?;
        if let Some(snapshot) = descriptor.schema_source.pinned_snapshot()
            && snapshot.discovery_manifest()?.as_ref() != Some(&self.evidence.discovery_manifest)
        {
            return Err(CdfError::data(
                "effective schema discovery manifest does not match its pinned schema snapshot",
            ));
        }
        Ok(())
    }

    pub fn physical_schema(&self, hash: &SchemaHash) -> Option<&SchemaRef> {
        self.schema_catalog
            .binary_search_by(|schema| schema.physical_schema_hash.cmp(hash))
            .ok()
            .map(|index| &self.schema_catalog[index].schema)
    }
}

pub trait ResourceStream: Send + Sync {
    fn descriptor(&self) -> &ResourceDescriptor;
    fn schema(&self) -> SchemaRef;
    /// Returns the canonical compiler artifact that produced this executable resource.
    ///
    /// Engine plans compiled from a source driver require this external binding before crossing
    /// the source boundary. Keeping it on the resolved resource prevents a serialized engine plan
    /// from coherently rewriting every copy of its own source ceiling.
    fn compiled_source_plan_hash(&self) -> Option<&str> {
        None
    }
    /// Validates adapter-owned runtime dependencies before orchestration starts.
    /// Generic orchestration calls this hook without knowing the source kind.
    fn validate_runtime_dependencies(&self) -> Result<()> {
        Ok(())
    }
    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>>;
    /// Opens the source-owned decoder for an external canonical partition authority.
    ///
    /// Generic orchestration calls this only when `ScanPlan::planned_task_set` is present. The
    /// returned reader is invocation-local because task artifacts are sequential, integrity-
    /// checked streams; sharing one across runs would make ordering and cancellation ambiguous.
    fn planned_partition_reader(
        &self,
        _reference: &PlannedTaskSetReference,
    ) -> Result<Box<dyn PlannedPartitionReader>> {
        Err(CdfError::contract(format!(
            "resource `{}` uses an external planned task set but its source adapter does not provide a partition reader",
            self.descriptor().resource_id
        )))
    }
    /// Rebinds a recorded partition set to the last committed resource frontier before any
    /// partition is opened. The default covers the ubiquitous single-partition stream and a
    /// composite frontier keyed exactly by partition id; adapters with another partition-local
    /// checkpoint model must own that mapping here rather than leak source semantics into the
    /// engine or project orchestrator.
    fn rebind_scan_for_resume(
        &self,
        scan: &mut ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<()> {
        committed_frontier.validate()?;
        if scan.planned_task_set.is_some() {
            return Err(CdfError::contract(format!(
                "resource `{}` uses an external planned task set; its source adapter must implement resume binding",
                self.descriptor().resource_id
            )));
        }
        let partitions = &mut scan.partitions;
        if partitions.len() == 1 {
            partitions[0].start_position = Some(committed_frontier.clone());
            return Ok(());
        }
        let SourcePosition::Composite(composite) = committed_frontier else {
            return Err(CdfError::contract(format!(
                "resource `{}` has {} partitions but its committed frontier is not partition-keyed; the source adapter must implement resume binding",
                self.descriptor().resource_id,
                partitions.len()
            )));
        };
        let planned_ids = partitions
            .iter()
            .map(|partition| partition.partition_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if let Some(unknown) = composite
            .positions
            .keys()
            .find(|partition_id| !planned_ids.contains(partition_id.as_str()))
        {
            return Err(CdfError::data(format!(
                "resource `{}` committed composite frontier references absent partition `{unknown}`",
                self.descriptor().resource_id
            )));
        }
        for partition in partitions {
            if let Some(position) = composite.positions.get(partition.partition_id.as_str()) {
                partition.start_position = Some(position.clone());
            }
        }
        Ok(())
    }
    /// Opens one invocation-bound partition stream.
    ///
    /// The returned attempt exposes invocation termination before its opening future is polled.
    /// Generic orchestration can therefore cancel and await producer shutdown even when
    /// cancellation wins before the opened stream becomes visible.
    fn open(&self, partition: PartitionPlan) -> PartitionOpenAttempt<'_>;
    /// Opens a runtime-materialized partition. Inline sources inherit the ordinary `open` path;
    /// external-task sources override this to recover their private retained task state.
    fn open_executable(&self, partition: ExecutablePartition) -> PartitionOpenAttempt<'_> {
        self.open(partition.into_plan())
    }
    /// Revalidates a planned observation without opening its payload and returns
    /// the exact source position safe to mark processed. Adapters with mutable
    /// external identity MUST override this method.
    fn attest_partition(&self, _partition: PartitionPlan) -> PartitionAttestationAttempt<'_> {
        PartitionAttestationAttempt::materialized(Box::pin(async { Ok(None) }))
    }
    fn attest_executable(&self, partition: ExecutablePartition) -> PartitionAttestationAttempt<'_> {
        self.attest_partition(partition.into_plan())
    }
    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        None
    }
    /// Physical schemas admitted when the fixed schema snapshot was pinned.
    ///
    /// The complete catalog lets plan compilation derive projection-stable physical identities
    /// without reopening every source. Execution still recomputes the projected physical hash
    /// from the streamed Arrow schema before using the resulting allowlist.
    fn baseline_observation_schema_catalog(&self) -> &[EffectiveSchemaCatalogEntry] {
        &[]
    }
    fn type_policy_allowances(&self) -> TypePolicyAllowances {
        TypePolicyAllowances::default()
    }
    /// Runtime retention authority required by non-pausable unbounded sources.
    ///
    /// The source owns replay-unit encoding. Generic orchestration owns the checkpoint ordering:
    /// this authority is advanced only after the exact package receipt and checkpoint frontier
    /// have committed.
    fn replay_retention(&self) -> Option<&dyn SourceReplayRetention> {
        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceReplayRetentionStatus {
    pub maximum_bytes: u64,
    pub maximum_age_milliseconds: u64,
    pub maximum_units: u64,
    pub retained_bytes: u64,
    pub retained_units: u64,
    pub committed_low_watermark: Option<SourcePosition>,
}

impl SourceReplayRetentionStatus {
    pub fn validate(&self) -> Result<()> {
        if self.maximum_bytes == 0
            || self.maximum_age_milliseconds == 0
            || self.maximum_units == 0
            || self.retained_bytes > self.maximum_bytes
            || self.retained_units > self.maximum_units
        {
            return Err(CdfError::data(
                "source replay retention requires nonzero configured byte/time/unit bounds and retained bytes/units within those bounds",
            ));
        }
        if let Some(frontier) = &self.committed_low_watermark {
            frontier.validate()?;
        }
        Ok(())
    }
}

pub trait SourceReplayRetention: Send + Sync {
    fn status(&self) -> Result<SourceReplayRetentionStatus>;
    /// Proves before destination mutation that the proposed checkpoint boundary is durably
    /// retained or already committed.
    fn validate_checkpoint_frontier(&self, frontier: &SourcePosition) -> Result<()>;
    /// Idempotently reconciles replay eviction to an already committed checkpoint head.
    fn reconcile_committed_frontier(&self, frontier: &SourcePosition) -> Result<()>;
    fn commit_checkpoint_frontier(&self, frontier: &SourcePosition) -> Result<()>;
}

/// One lifecycle-bound partition attestation.
///
/// Unlike an open attempt, attestation has no continuing payload to own producer work. The future
/// therefore joins its invocation scope before returning the observation and preserves the primary
/// typed error if cleanup also fails.
pub struct PartitionAttestationAttempt<'a> {
    attesting: Option<BoxFuture<'a, Result<Option<PartitionAttestation>>>>,
    result: Option<Result<Option<PartitionAttestation>>>,
    joining: Option<BoxFuture<'static, Result<()>>>,
    termination: InvocationTermination,
    terminal: bool,
}

impl<'a> PartitionAttestationAttempt<'a> {
    pub fn materialized(attesting: BoxFuture<'a, Result<Option<PartitionAttestation>>>) -> Self {
        Self::with_termination(attesting, InvocationTermination::completed())
    }

    pub fn with_termination(
        attesting: BoxFuture<'a, Result<Option<PartitionAttestation>>>,
        termination: InvocationTermination,
    ) -> Self {
        Self {
            attesting: Some(attesting),
            result: None,
            joining: None,
            termination,
            terminal: false,
        }
    }

    /// Cancels the attestation and waits for every invocation-owned task to terminate.
    ///
    /// Callers that race attestation against run cancellation MUST use this barrier before they
    /// return. Drop remains a last-resort cancellation rail, not an asynchronous join primitive.
    pub async fn terminate_and_join(&mut self) -> Result<()> {
        drop(self.attesting.take());
        drop(self.result.take());
        drop(self.joining.take());
        self.terminal = true;
        self.termination.terminate_and_join().await
    }
}

impl Future for PartitionAttestationAttempt<'_> {
    type Output = Result<Option<PartitionAttestation>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.result.is_none() {
            let attesting = self
                .attesting
                .as_mut()
                .expect("partition attestation cannot be polled after completion");
            let Poll::Ready(result) = attesting.as_mut().poll(cx) else {
                return Poll::Pending;
            };
            self.attesting = None;
            self.result = Some(result);
            let termination = self.termination.clone();
            self.joining = Some(Box::pin(async move { termination.join().await }));
        }

        let joining = self
            .joining
            .as_mut()
            .expect("partition attestation join was initialized");
        let Poll::Ready(joined) = joining.as_mut().poll(cx) else {
            return Poll::Pending;
        };
        self.joining = None;
        self.terminal = true;
        let result = self
            .result
            .take()
            .expect("partition attestation result was initialized");
        match (result, joined) {
            (Ok(attestation), Ok(())) => Poll::Ready(Ok(attestation)),
            (Err(mut error), Err(cleanup)) => {
                error.message = format!(
                    "{}; source attestation termination also failed: {}",
                    error.message, cleanup.message
                );
                Poll::Ready(Err(error))
            }
            (Err(error), Ok(())) => Poll::Ready(Err(error)),
            (Ok(_), Err(error)) => Poll::Ready(Err(error)),
        }
    }
}

impl Drop for PartitionAttestationAttempt<'_> {
    fn drop(&mut self) {
        if !self.terminal {
            self.termination.cancel();
        }
    }
}

/// Cloneable cancel-and-join authority for all producer work in one source invocation.
#[derive(Clone)]
pub struct InvocationTermination {
    cancel: Arc<dyn Fn() + Send + Sync>,
    joined: Shared<BoxFuture<'static, Result<()>>>,
}

impl InvocationTermination {
    pub fn new(
        cancel: impl Fn() + Send + Sync + 'static,
        joined: BoxFuture<'static, Result<()>>,
    ) -> Self {
        Self {
            cancel: Arc::new(cancel),
            joined: joined.shared(),
        }
    }

    pub fn completed() -> Self {
        Self::new(|| {}, Box::pin(async { Ok(()) }))
    }

    pub async fn join(&self) -> Result<()> {
        self.joined.clone().await
    }

    pub fn cancel(&self) {
        (self.cancel)();
    }

    pub async fn terminate_and_join(&self) -> Result<()> {
        self.cancel();
        self.join().await
    }
}

/// An opening source invocation whose termination authority exists before polling begins.
///
/// An opening error is returned before cleanup so orchestration can record the primary failure
/// first. The caller MUST then call [`Self::terminate_and_join`] before retrying or returning;
/// dropping an unfinished attempt still cancels its producer as a final safety rail.
pub struct PartitionOpenAttempt<'a> {
    opening: Option<BoxFuture<'a, Result<PartitionStreamPayload>>>,
    termination: InvocationTermination,
    needs_termination: bool,
}

impl<'a> PartitionOpenAttempt<'a> {
    /// Creates an attempt whose opening work and payload are already materialized on the caller's
    /// task. Sources that spawn any producer work MUST use [`Self::with_termination`].
    pub fn materialized(opening: BoxFuture<'a, Result<PartitionStreamPayload>>) -> Self {
        Self::with_termination(opening, InvocationTermination::completed())
    }

    pub fn with_termination(
        opening: BoxFuture<'a, Result<PartitionStreamPayload>>,
        termination: InvocationTermination,
    ) -> Self {
        Self {
            opening: Some(opening),
            termination,
            needs_termination: true,
        }
    }

    pub async fn terminate_and_join(&mut self) -> Result<()> {
        drop(self.opening.take());
        self.needs_termination = false;
        self.termination.terminate_and_join().await
    }
}

impl Future for PartitionOpenAttempt<'_> {
    type Output = Result<OpenedPartitionStream>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let opening = self
            .opening
            .as_mut()
            .expect("partition opening attempt cannot be polled after completion");
        match opening.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(payload)) => {
                self.opening = None;
                self.needs_termination = false;
                Poll::Ready(Ok(OpenedPartitionStream::from_payload(
                    payload,
                    self.termination.clone(),
                )))
            }
            Poll::Ready(Err(error)) => {
                self.opening = None;
                Poll::Ready(Err(error))
            }
        }
    }
}

/// The payload published by a source invocation before its lifecycle authority is transferred.
///
/// This type deliberately carries no cancellation or join handle. [`PartitionOpenAttempt`] is the
/// sole owner of that authority and attaches it to [`OpenedPartitionStream`] on successful open,
/// making it impossible for an adapter to pair a payload with another invocation's termination.
pub struct PartitionStreamPayload {
    stream: BatchStream,
    completion: BoxFuture<'static, Result<PartitionCompletion>>,
}

impl PartitionStreamPayload {
    pub fn new(
        stream: BatchStream,
        completion: BoxFuture<'static, Result<PartitionCompletion>>,
    ) -> Self {
        Self { stream, completion }
    }

    pub fn batches(stream: BatchStream) -> Self {
        Self::new(
            stream,
            Box::pin(async { Ok(PartitionCompletion::default()) }),
        )
    }
}

impl Drop for PartitionOpenAttempt<'_> {
    fn drop(&mut self) {
        if self.needs_termination {
            self.termination.cancel();
        }
    }
}

/// One opened partition stream and its invocation-bound terminal evidence.
///
/// Completion may be consumed only after the batch stream reaches EOF. Binding the future to this
/// value prevents evidence from one preview, retry, or concurrent open being observed by another.
pub struct OpenedPartitionStream {
    stream: Option<BatchStream>,
    completion: Option<BoxFuture<'static, Result<PartitionCompletion>>>,
    termination: InvocationTermination,
    termination_consumed: bool,
    reached_eof: bool,
    terminal_error: Option<CdfError>,
}

impl OpenedPartitionStream {
    fn from_payload(payload: PartitionStreamPayload, termination: InvocationTermination) -> Self {
        Self {
            stream: Some(payload.stream),
            completion: Some(payload.completion),
            termination,
            termination_consumed: false,
            reached_eof: false,
            terminal_error: None,
        }
    }

    pub async fn join_failed_attempt(&mut self) -> Result<()> {
        let Some(primary) = self.terminal_error.clone() else {
            return Err(CdfError::contract(
                "failed-attempt join requires a terminal stream error",
            ));
        };
        match self.terminate_and_join().await {
            // Execution scopes surface the producer's failure through the stream and retain the
            // same task result in their join report. That repeated primary error proves the task
            // was joined; it is not a second cleanup failure and must not suppress retry.
            Err(joined) if joined == primary => Ok(()),
            result => result,
        }
    }

    /// Returns a probed batch to the front of this invocation-bound stream.
    ///
    /// Scheduler retry may need to observe the first stream item before it can publish a
    /// successful open. Re-inserting that item keeps the source frontier as the sole owner of
    /// ready payload and preserves the original completion and termination authority.
    pub fn prepend_batch(&mut self, batch: crate::Batch) -> Result<()> {
        if self.reached_eof || self.terminal_error.is_some() {
            return Err(CdfError::contract(
                "cannot prepend a batch after source stream termination",
            ));
        }
        let remaining = self.stream.take().ok_or_else(|| {
            CdfError::contract("cannot prepend a batch after source stream consumption")
        })?;
        self.stream = Some(Box::pin(
            stream::once(async move { Ok(batch) }).chain(remaining),
        ));
        Ok(())
    }

    pub async fn terminate_and_join(&mut self) -> Result<()> {
        drop(self.stream.take());
        if self.termination_consumed {
            return Ok(());
        }
        let result = self.termination.terminate_and_join().await;
        self.termination_consumed = true;
        result
    }

    pub async fn completion(&mut self) -> Result<PartitionCompletion> {
        if !self.reached_eof {
            return Err(CdfError::contract(
                "partition completion requires a fully consumed stream",
            ));
        }
        let completion = self.completion.take().ok_or_else(|| {
            CdfError::contract("partition completion attestation was already consumed")
        })?;
        let completion = completion.await;
        let termination = self.terminate_and_join().await;
        match (completion, termination) {
            (Ok(completion), Ok(())) => Ok(completion),
            (Err(mut error), Err(termination)) => {
                error.message = format!(
                    "{}; source invocation termination also failed: {}",
                    error.message, termination.message
                );
                Err(error)
            }
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }
}

impl Stream for OpenedPartitionStream {
    type Item = Result<crate::Batch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };
        let next = stream.as_mut().poll_next(cx);
        match &next {
            Poll::Ready(None) => self.reached_eof = true,
            Poll::Ready(Some(Err(error))) => self.terminal_error = Some(error.clone()),
            Poll::Ready(Some(Ok(_))) | Poll::Pending => {}
        }
        next
    }
}

impl Drop for OpenedPartitionStream {
    fn drop(&mut self) {
        if self.stream.is_some() && !self.termination_consumed {
            self.termination.cancel();
        }
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
