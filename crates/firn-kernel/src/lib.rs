#![doc = "Core types, traits, and artifact contracts for firn."]

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fmt,
    future::Future,
    pin::Pin,
};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Field, SchemaRef};
use futures_core::Stream;
use serde::{Deserialize, Serialize};

pub const SEMANTIC_METADATA_KEY: &str = "firn:semantic";
pub const SOURCE_NAME_METADATA_KEY: &str = "firn:source_name";
pub const NULL_ORIGIN_METADATA_KEY: &str = "firn:null_origin";

pub type Result<T> = std::result::Result<T, FirnError>;
pub type BatchStream = Pin<Box<dyn Stream<Item = Result<Batch>> + Send + 'static>>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    Transient,
    RateLimited,
    Auth,
    Contract,
    Data,
    Destination,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirnError {
    pub kind: ErrorKind,
    pub message: String,
    pub retry_after_ms: Option<u64>,
}

impl FirnError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_after_ms: None,
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Transient, message)
    }

    pub fn rate_limited(message: impl Into<String>, retry_after_ms: Option<u64>) -> Self {
        Self {
            kind: ErrorKind::RateLimited,
            message: message.into(),
            retry_after_ms,
        }
    }

    pub fn auth(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Auth, message)
    }

    pub fn contract(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Contract, message)
    }

    pub fn data(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Data, message)
    }

    pub fn destination(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Destination, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }
}

impl fmt::Display for FirnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.retry_after_ms {
            Some(retry_after_ms) => write!(
                f,
                "{:?}: {} (retry after {} ms)",
                self.kind, self.message, retry_after_ms
            ),
            None => write!(f, "{:?}: {}", self.kind, self.message),
        }
    }
}

impl StdError for FirnError {}

impl From<ArrowError> for FirnError {
    fn from(error: ArrowError) -> Self {
        Self::data(error.to_string())
    }
}

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(FirnError::contract(concat!(
                        stringify!($name),
                        " cannot be empty"
                    )));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

string_id!(BatchId);
string_id!(CheckpointId);
string_id!(ContractRef);
string_id!(DestinationId);
string_id!(IdempotencyToken);
string_id!(PackageHash);
string_id!(PartitionId);
string_id!(PipelineId);
string_id!(PlanId);
string_id!(PredicateId);
string_id!(ReceiptId);
string_id!(ResourceId);
string_id!(RunId);
string_id!(SchemaHash);
string_id!(SegmentId);
string_id!(SourceId);
string_id!(TargetName);
string_id!(ValidationProgramHash);

pub fn with_source_name(field: Field, source_name: impl Into<String>) -> Field {
    with_metadata_value(field, SOURCE_NAME_METADATA_KEY, source_name)
}

pub fn source_name(field: &Field) -> Option<&str> {
    metadata_value(field, SOURCE_NAME_METADATA_KEY)
}

pub fn with_semantic(field: Field, semantic: impl Into<String>) -> Field {
    with_metadata_value(field, SEMANTIC_METADATA_KEY, semantic)
}

pub fn semantic(field: &Field) -> Option<&str> {
    metadata_value(field, SEMANTIC_METADATA_KEY)
}

pub fn with_null_origin(field: Field, null_origin: impl Into<String>) -> Field {
    with_metadata_value(field, NULL_ORIGIN_METADATA_KEY, null_origin)
}

pub fn null_origin(field: &Field) -> Option<&str> {
    metadata_value(field, NULL_ORIGIN_METADATA_KEY)
}

pub fn with_firn_metadata(
    field: Field,
    source_name: Option<impl Into<String>>,
    semantic: Option<impl Into<String>>,
    null_origin: Option<impl Into<String>>,
) -> Field {
    let field = match source_name {
        Some(value) => with_source_name(field, value),
        None => field,
    };
    let field = match semantic {
        Some(value) => with_semantic(field, value),
        None => field,
    };
    match null_origin {
        Some(value) => with_null_origin(field, value),
        None => field,
    }
}

fn with_metadata_value(field: Field, key: &'static str, value: impl Into<String>) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(key.to_owned(), value.into());
    field.with_metadata(metadata)
}

fn metadata_value<'a>(field: &'a Field, key: &'static str) -> Option<&'a str> {
    field.metadata().get(key).map(String::as_str)
}

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
    Discovered {
        schema_hash: Option<SchemaHash>,
    },
    Contract {
        contract: ContractRef,
        schema_hash: Option<SchemaHash>,
    },
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

#[derive(Clone, Debug)]
pub struct Batch {
    pub header: BatchHeader,
    pub payload: BatchPayload,
}

impl Batch {
    pub fn from_record_batch(
        batch_id: BatchId,
        resource_id: ResourceId,
        partition_id: PartitionId,
        observed_schema_hash: SchemaHash,
        record_batch: RecordBatch,
    ) -> Result<Self> {
        let row_count = record_batch.num_rows() as u64;
        let byte_count = record_batch.get_array_memory_size() as u64;
        Ok(Self {
            header: BatchHeader {
                batch_id,
                resource_id,
                partition_id,
                observed_schema_hash,
                row_count,
                byte_count,
                source_position: None,
                watermarks: Vec::new(),
                stats: BatchStats::default(),
                cdc: None,
            },
            payload: BatchPayload::RecordBatch(record_batch),
        })
    }

    pub fn from_reference(header: BatchHeader, reference: PayloadRef) -> Self {
        Self {
            header,
            payload: BatchPayload::Reference(reference),
        }
    }

    pub fn record_batch(&self) -> Option<&RecordBatch> {
        match &self.payload {
            BatchPayload::RecordBatch(record_batch) => Some(record_batch),
            BatchPayload::Reference(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum BatchPayload {
    RecordBatch(RecordBatch),
    Reference(PayloadRef),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchHeader {
    pub batch_id: BatchId,
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub observed_schema_hash: SchemaHash,
    pub row_count: u64,
    pub byte_count: u64,
    pub source_position: Option<SourcePosition>,
    pub watermarks: Vec<Watermark>,
    pub stats: BatchStats,
    pub cdc: Option<CdcMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadRef {
    pub uri: String,
    pub byte_count: u64,
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchStats {
    pub columns: BTreeMap<String, ColumnStats>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStats {
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
    pub min_lexical: Option<String>,
    pub max_lexical: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    pub name: String,
    pub position: SourcePosition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcMetadata {
    pub operation_field: String,
    pub position: SourcePosition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourcePosition {
    Cursor(CursorPosition),
    Log(LogPosition),
    FileManifest(FileManifest),
    PageToken(PageToken),
    Composite(CompositePosition),
    ForeignState(ForeignState),
}

impl SourcePosition {
    pub fn version(&self) -> u16 {
        match self {
            Self::Cursor(position) => position.version,
            Self::Log(position) => position.version,
            Self::FileManifest(position) => position.version,
            Self::PageToken(position) => position.version,
            Self::Composite(position) => position.version,
            Self::ForeignState(position) => position.version,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorPosition {
    pub version: u16,
    pub field: String,
    pub value: CursorValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CursorValue {
    String(String),
    I64(i64),
    U64(u64),
    DecimalString(String),
    TimestampMicros {
        micros: i64,
        timezone: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogPosition {
    pub version: u16,
    pub log: String,
    pub offset: i64,
    pub sequence: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileManifest {
    pub version: u16,
    pub files: Vec<FilePosition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilePosition {
    pub path: String,
    pub size_bytes: u64,
    pub etag: Option<String>,
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageToken {
    pub version: u16,
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositePosition {
    pub version: u16,
    pub positions: BTreeMap<String, SourcePosition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignState {
    pub version: u16,
    pub protocol: String,
    pub opaque_blob: Vec<u8>,
    pub blob_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopeKey {
    Resource,
    Partition {
        partition_id: PartitionId,
    },
    Window {
        start: String,
        end: String,
    },
    File {
        path: String,
    },
    Stream {
        name: String,
    },
    SchemaContract {
        contract: ContractRef,
    },
    DestinationLoad {
        destination: DestinationId,
        target: TargetName,
    },
    Composite {
        parts: Vec<ScopeKey>,
    },
}

impl ScopeKey {
    pub fn kind(&self) -> ScopeKind {
        match self {
            Self::Resource => ScopeKind::Resource,
            Self::Partition { .. } => ScopeKind::Partition,
            Self::Window { .. } => ScopeKind::Window,
            Self::File { .. } => ScopeKind::File,
            Self::Stream { .. } => ScopeKind::Stream,
            Self::SchemaContract { .. } => ScopeKind::SchemaContract,
            Self::DestinationLoad { .. } => ScopeKind::DestinationLoad,
            Self::Composite { .. } => ScopeKind::Composite,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeKind {
    Resource,
    Partition,
    Window,
    File,
    Stream,
    SchemaContract,
    DestinationLoad,
    Composite,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateDelta {
    pub checkpoint_id: CheckpointId,
    pub pipeline_id: PipelineId,
    pub resource_id: ResourceId,
    pub scope: ScopeKey,
    pub state_version: u16,
    pub parent_checkpoint_id: Option<CheckpointId>,
    pub input_position: Option<SourcePosition>,
    pub output_position: SourcePosition,
    pub package_hash: PackageHash,
    pub schema_hash: SchemaHash,
    pub segments: Vec<StateSegment>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSegment {
    pub segment_id: SegmentId,
    pub scope: ScopeKey,
    pub output_position: SourcePosition,
    pub row_count: u64,
    pub byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub receipt_id: ReceiptId,
    pub destination: DestinationId,
    pub target: TargetName,
    pub package_hash: PackageHash,
    pub segment_acks: Vec<SegmentAck>,
    pub disposition: WriteDisposition,
    pub idempotency_token: IdempotencyToken,
    pub transaction: Option<TransactionMetadata>,
    pub counts: CommitCounts,
    pub schema_hash: SchemaHash,
    pub migrations: Vec<MigrationRecord>,
    pub committed_at_ms: i64,
    pub verify: VerifyClause,
}

impl Receipt {
    pub fn covers_state_delta(&self, delta: &StateDelta) -> bool {
        if self.package_hash != delta.package_hash || self.schema_hash != delta.schema_hash {
            return false;
        }
        let acked_segments: BTreeSet<&SegmentId> = self
            .segment_acks
            .iter()
            .map(|ack| &ack.segment_id)
            .collect();
        delta
            .segments
            .iter()
            .all(|segment| acked_segments.contains(&segment.segment_id))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentAck {
    pub segment_id: SegmentId,
    pub row_count: u64,
    pub byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionMetadata {
    pub system: String,
    pub values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitCounts {
    pub rows_written: u64,
    pub rows_inserted: Option<u64>,
    pub rows_updated: Option<u64>,
    pub rows_deleted: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub migration_id: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyClause {
    pub kind: String,
    pub statement: String,
    pub parameters: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationSheet {
    pub destination: DestinationId,
    pub supported_dispositions: Vec<WriteDisposition>,
    pub transactions: TransactionSupport,
    pub idempotency: IdempotencySupport,
    pub type_mappings: Vec<TypeMapping>,
    pub identifier_rules: IdentifierRules,
    pub migration_support: CapabilitySupport,
    pub quarantine_tables: CapabilitySupport,
    pub concurrency: ConcurrencyLimit,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionSupport {
    None,
    AtomicTarget,
    AtomicPackage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencySupport {
    None,
    PackageToken,
    SegmentToken,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeMapping {
    pub arrow_type: String,
    pub destination_type: String,
    pub fidelity: TypeMappingFidelity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeMappingFidelity {
    Lossless,
    LossyRequiresContractAllowance,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentifierRules {
    pub normalizer: String,
    pub max_length: Option<u16>,
    pub allowed_pattern: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcurrencyLimit {
    pub max_writers: Option<u16>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationCommitRequest {
    pub package_hash: PackageHash,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub segments: Vec<StateSegment>,
    pub idempotency_token: IdempotencyToken,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlan {
    pub plan_id: PlanId,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub idempotency: IdempotencySupport,
    pub migrations: Vec<MigrationRecord>,
    pub delivery_guarantee: DeliveryGuarantee,
}

pub trait DestinationProtocol {
    fn sheet(&self) -> &DestinationSheet;
    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryGuarantee {
    AtLeastOnceDuplicateRisk,
    EffectivelyOncePerKey,
    EffectivelyOncePerPackage,
    EffectivelyOncePerTarget,
    EffectivelyOncePerPosition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationProgramRef {
    pub contract: ContractRef,
    pub program_hash: ValidationProgramHash,
    pub schema_hash: SchemaHash,
    pub policy: ContractPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractPolicy {
    Evolve,
    Freeze,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowDisposition {
    Accept,
    Quarantine,
    Reject,
    Fail,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    fn sample_state_delta_and_receipt() -> (StateDelta, Receipt) {
        let scope = ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        };
        let output_position = SourcePosition::Cursor(CursorPosition {
            version: 7,
            field: "updated_at".to_owned(),
            value: CursorValue::TimestampMicros {
                micros: 1_700_000_000_000_000,
                timezone: Some("America/Phoenix".to_owned()),
            },
        });
        let segment = StateSegment {
            segment_id: SegmentId::new("segment-1").unwrap(),
            scope: scope.clone(),
            output_position: output_position.clone(),
            row_count: 3,
            byte_count: 24,
        };
        let delta = StateDelta {
            checkpoint_id: CheckpointId::new("checkpoint-1").unwrap(),
            pipeline_id: PipelineId::new("pipeline-1").unwrap(),
            resource_id: ResourceId::new("orders").unwrap(),
            scope,
            state_version: 1,
            parent_checkpoint_id: None,
            input_position: None,
            output_position,
            package_hash: PackageHash::new("package-sha256").unwrap(),
            schema_hash: SchemaHash::new("schema-sha256").unwrap(),
            segments: vec![segment],
        };
        let receipt = Receipt {
            receipt_id: ReceiptId::new("receipt-1").unwrap(),
            destination: DestinationId::new("local-test").unwrap(),
            target: TargetName::new("orders").unwrap(),
            package_hash: PackageHash::new("package-sha256").unwrap(),
            segment_acks: vec![SegmentAck {
                segment_id: SegmentId::new("segment-1").unwrap(),
                row_count: 3,
                byte_count: 24,
            }],
            disposition: WriteDisposition::Merge,
            idempotency_token: IdempotencyToken::new("package-sha256").unwrap(),
            transaction: None,
            counts: CommitCounts {
                rows_written: 3,
                rows_inserted: Some(3),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: SchemaHash::new("schema-sha256").unwrap(),
            migrations: Vec::new(),
            committed_at_ms: 1_700_000_000_000,
            verify: VerifyClause {
                kind: "sql".to_owned(),
                statement: "select count(*) from orders where _firn_package = ?".to_owned(),
                parameters: BTreeMap::new(),
            },
        };

        (delta, receipt)
    }

    #[test]
    fn metadata_helpers_round_trip_firn_annotations() {
        let field = Field::new("normalized_name", DataType::Utf8, true);
        let field = with_firn_metadata(
            field,
            Some("Original Name"),
            Some("pii:email"),
            Some("source_absent"),
        );

        assert_eq!(source_name(&field), Some("Original Name"));
        assert_eq!(semantic(&field), Some("pii:email"));
        assert_eq!(null_origin(&field), Some("source_absent"));
        assert_eq!(
            field.metadata().get(SOURCE_NAME_METADATA_KEY),
            Some(&"Original Name".to_owned())
        );
    }

    #[test]
    fn batch_wraps_arrow_record_batch_and_reports_counts() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let column: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let record_batch = RecordBatch::try_new(schema, vec![column]).unwrap();

        let batch = Batch::from_record_batch(
            BatchId::new("batch-1").unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new("p0").unwrap(),
            SchemaHash::new("schema-sha256").unwrap(),
            record_batch,
        )
        .unwrap();

        assert_eq!(batch.header.row_count, 3);
        assert!(batch.header.byte_count > 0);
        assert!(batch.record_batch().is_some());
    }

    #[test]
    fn artifact_values_serde_round_trip() {
        let descriptor = ResourceDescriptor {
            resource_id: ResourceId::new("orders").unwrap(),
            schema_source: SchemaSource::Declared {
                schema_hash: SchemaHash::new("schema-sha256").unwrap(),
                source: "contract/orders.v1".to_owned(),
            },
            primary_key: vec!["id".to_owned()],
            merge_key: vec!["id".to_owned()],
            cursor: Some(CursorSpec {
                field: "updated_at".to_owned(),
                ordering: CursorOrderingClaim::Inexact,
                lag_tolerance_ms: 60_000,
            }),
            write_disposition: WriteDisposition::Merge,
            contract: Some(ContractRef::new("orders-contract").unwrap()),
            state_scope: ScopeKey::Partition {
                partition_id: PartitionId::new("p0").unwrap(),
            },
            freshness: Some(FreshnessSpec {
                max_age_ms: 300_000,
            }),
            trust_level: TrustLevel::Governed,
        };

        let json = serde_json::to_string(&descriptor).unwrap();
        assert_eq!(
            descriptor,
            serde_json::from_str::<ResourceDescriptor>(&json).unwrap()
        );

        let output_position = SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "updated_at".to_owned(),
            value: CursorValue::TimestampMicros {
                micros: 1_700_000_000_000_000,
                timezone: Some("America/Phoenix".to_owned()),
            },
        });
        let segment = StateSegment {
            segment_id: SegmentId::new("segment-1").unwrap(),
            scope: descriptor.state_scope.clone(),
            output_position: output_position.clone(),
            row_count: 3,
            byte_count: 24,
        };
        let delta = StateDelta {
            checkpoint_id: CheckpointId::new("checkpoint-1").unwrap(),
            pipeline_id: PipelineId::new("pipeline-1").unwrap(),
            resource_id: descriptor.resource_id.clone(),
            scope: descriptor.state_scope.clone(),
            state_version: 1,
            parent_checkpoint_id: None,
            input_position: None,
            output_position,
            package_hash: PackageHash::new("package-sha256").unwrap(),
            schema_hash: SchemaHash::new("schema-sha256").unwrap(),
            segments: vec![segment],
        };
        let receipt = Receipt {
            receipt_id: ReceiptId::new("receipt-1").unwrap(),
            destination: DestinationId::new("local-test").unwrap(),
            target: TargetName::new("orders").unwrap(),
            package_hash: PackageHash::new("package-sha256").unwrap(),
            segment_acks: vec![SegmentAck {
                segment_id: SegmentId::new("segment-1").unwrap(),
                row_count: 3,
                byte_count: 24,
            }],
            disposition: WriteDisposition::Merge,
            idempotency_token: IdempotencyToken::new("package-sha256").unwrap(),
            transaction: None,
            counts: CommitCounts {
                rows_written: 3,
                rows_inserted: Some(3),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: SchemaHash::new("schema-sha256").unwrap(),
            migrations: Vec::new(),
            committed_at_ms: 1_700_000_000_000,
            verify: VerifyClause {
                kind: "sql".to_owned(),
                statement: "select count(*) from orders where _firn_package = ?".to_owned(),
                parameters: BTreeMap::new(),
            },
        };

        assert!(receipt.covers_state_delta(&delta));
        let delta_json = serde_json::to_string(&delta).unwrap();
        assert_eq!(
            delta,
            serde_json::from_str::<StateDelta>(&delta_json).unwrap()
        );
        let receipt_json = serde_json::to_string(&receipt).unwrap();
        assert_eq!(
            receipt,
            serde_json::from_str::<Receipt>(&receipt_json).unwrap()
        );
    }

    #[test]
    fn error_taxonomy_contains_required_categories() {
        let kinds = [
            ErrorKind::Transient,
            ErrorKind::RateLimited,
            ErrorKind::Auth,
            ErrorKind::Contract,
            ErrorKind::Data,
            ErrorKind::Destination,
            ErrorKind::Internal,
        ];

        assert_eq!(kinds.len(), 7);
        assert_eq!(
            FirnError::rate_limited("slow down", Some(100)).kind,
            ErrorKind::RateLimited
        );
    }

    #[test]
    fn firn_error_display_includes_retry_context_when_present() {
        assert_eq!(
            FirnError::contract("schema drift").to_string(),
            "Contract: schema drift"
        );
        assert_eq!(
            FirnError::rate_limited("slow down", Some(250)).to_string(),
            "RateLimited: slow down (retry after 250 ms)"
        );
    }

    #[test]
    fn source_position_version_returns_embedded_variant_version() {
        let mut composite_parts = BTreeMap::new();
        composite_parts.insert(
            "cursor".to_owned(),
            SourcePosition::Cursor(CursorPosition {
                version: 2,
                field: "updated_at".to_owned(),
                value: CursorValue::I64(10),
            }),
        );

        let positions = [
            (
                SourcePosition::Cursor(CursorPosition {
                    version: 2,
                    field: "updated_at".to_owned(),
                    value: CursorValue::I64(10),
                }),
                2,
            ),
            (
                SourcePosition::Log(LogPosition {
                    version: 3,
                    log: "orders".to_owned(),
                    offset: 42,
                    sequence: Some("abc".to_owned()),
                }),
                3,
            ),
            (
                SourcePosition::FileManifest(FileManifest {
                    version: 4,
                    files: vec![FilePosition {
                        path: "orders.jsonl".to_owned(),
                        size_bytes: 1024,
                        etag: Some("etag-1".to_owned()),
                        sha256: Some("file-sha256".to_owned()),
                    }],
                }),
                4,
            ),
            (
                SourcePosition::PageToken(PageToken {
                    version: 5,
                    token: "next-page".to_owned(),
                }),
                5,
            ),
            (
                SourcePosition::Composite(CompositePosition {
                    version: 6,
                    positions: composite_parts,
                }),
                6,
            ),
            (
                SourcePosition::ForeignState(ForeignState {
                    version: 7,
                    protocol: "singer".to_owned(),
                    opaque_blob: b"state".to_vec(),
                    blob_sha256: "state-sha256".to_owned(),
                }),
                7,
            ),
        ];

        for (position, expected_version) in positions {
            assert_eq!(position.version(), expected_version);
        }
    }

    #[test]
    fn receipt_rejects_state_delta_when_identity_or_segments_do_not_match() {
        let (delta, receipt) = sample_state_delta_and_receipt();
        assert!(receipt.covers_state_delta(&delta));

        let mut wrong_package = receipt.clone();
        wrong_package.package_hash = PackageHash::new("other-package-sha256").unwrap();
        assert!(!wrong_package.covers_state_delta(&delta));

        let mut wrong_schema = receipt.clone();
        wrong_schema.schema_hash = SchemaHash::new("other-schema-sha256").unwrap();
        assert!(!wrong_schema.covers_state_delta(&delta));

        let mut missing_segment = receipt;
        missing_segment.segment_acks.clear();
        assert!(!missing_segment.covers_state_delta(&delta));
    }
}
