use std::collections::BTreeMap;

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{
    async_types::{BatchStream, BoxFuture},
    destination::DeliveryGuarantee,
    error::Result,
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

pub trait ResourceStream {
    fn descriptor(&self) -> &ResourceDescriptor;
    fn schema(&self) -> SchemaRef;
    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>>;
    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>>;
}

pub trait QueryableResource: ResourceStream {
    fn capabilities(&self) -> &ResourceCapabilities;
    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan>;
}
