use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::{
    checkpoint::{Receipt, StateSegment},
    correction::{
        CorrectionCommitSession, DestinationCorrectionCommitPlan,
        DestinationCorrectionCommitRequest, DestinationProtocolCapabilities,
        DestinationResidualReadback, DestinationSheetArtifact, RowProvenanceAddress,
    },
    error::Result,
    ids::{DestinationId, IdempotencyToken, PackageHash, PlanId, ReceiptId, SegmentId, TargetName},
    resource::{CapabilitySupport, WriteDisposition},
};

#[derive(Clone, Debug)]
pub struct CommitSegment {
    pub state: StateSegment,
    pub package_byte_count: u64,
    pub batches: Vec<RecordBatch>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentAck {
    pub segment_id: SegmentId,
    pub row_count: u64,
    pub byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptVerification {
    pub verified: bool,
    pub receipt_id: ReceiptId,
    pub reason: Option<String>,
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

impl DestinationCommitRequest {
    /// A commit with no state segments still binds a package receipt, but MUST
    /// NOT mutate destination data or target replacement pointers.
    pub fn is_data_noop(&self) -> bool {
        self.segments.is_empty()
    }
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

pub trait CommitSession {
    fn apply_migrations(&mut self) -> Result<()>;

    fn write_segment(&mut self, segment: CommitSegment) -> Result<SegmentAck>;

    fn finalize(self: Box<Self>) -> Result<Receipt>;

    fn abort(self: Box<Self>) -> Result<()>;
}

pub trait DestinationProtocol {
    fn sheet(&self) -> &DestinationSheet;

    fn protocol_capabilities(&self) -> DestinationProtocolCapabilities {
        DestinationProtocolCapabilities::default()
    }

    fn sheet_artifact(&self) -> Result<DestinationSheetArtifact> {
        DestinationSheetArtifact::new(self.sheet().clone(), self.protocol_capabilities())
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan>;

    fn begin(
        &self,
        request: DestinationCommitRequest,
        plan: CommitPlan,
    ) -> Result<Box<dyn CommitSession + '_>>;

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification>;

    fn plan_correction(
        &self,
        _request: &DestinationCorrectionCommitRequest,
    ) -> Result<DestinationCorrectionCommitPlan> {
        Err(crate::CdfError::destination(format!(
            "destination {} does not support addressed corrections",
            self.sheet().destination
        )))
    }

    fn begin_correction(
        &self,
        _request: DestinationCorrectionCommitRequest,
        _plan: DestinationCorrectionCommitPlan,
    ) -> Result<Box<dyn CorrectionCommitSession + '_>> {
        Err(crate::CdfError::destination(format!(
            "destination {} does not support addressed corrections",
            self.sheet().destination
        )))
    }

    fn verify_correction(&self, _receipt: &Receipt) -> Result<ReceiptVerification> {
        Err(crate::CdfError::destination(format!(
            "destination {} does not support addressed corrections",
            self.sheet().destination
        )))
    }

    fn read_correction_residual(
        &self,
        _target: &TargetName,
        _original_row: &RowProvenanceAddress,
    ) -> Result<Option<DestinationResidualReadback>> {
        Err(crate::CdfError::destination(format!(
            "destination {} does not support correction residual readback",
            self.sheet().destination
        )))
    }
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
