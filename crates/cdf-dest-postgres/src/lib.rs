#![doc = "Postgres destination boundary for cdf."]

use std::collections::{BTreeMap, BTreeSet};

use cdf_kernel::{
    CapabilitySupport, CdfError, CommitCounts, CommitPlan, CommitSegment, CommitSession,
    ConcurrencyLimit, CorrectionCommitSession, CorrectionStrategy, CorrectionStrategyCapability,
    DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY, DeliveryGuarantee, DestinationCommitRequest,
    DestinationCorrectionCapabilities, DestinationCorrectionCommitPlan,
    DestinationCorrectionCommitRequest, DestinationCorrectionReceiptEvidence, DestinationId,
    DestinationProtocol, DestinationProtocolCapabilities, DestinationResidualReadback,
    DestinationSheet, IdempotencySupport, IdempotencyToken, IdentifierRules, MigrationRecord,
    PackageHash, PlanId, Receipt, ReceiptId, ReceiptVerification, ResourceId, Result,
    RowProvenanceAddress, RowProvenanceCapabilities, SchemaHash, SegmentAck, SegmentId, StateDelta,
    StateSegment, TargetName, TransactionMetadata, TransactionSupport, TypeMapping,
    TypeMappingFidelity, VerifyClause, WriteDisposition,
};
use serde::{Deserialize, Serialize};

pub use cdf_postgres::{PostgresIdentifier, PostgresTarget, quote_identifier};

pub const POSTGRES_DESTINATION_ID: &str = "postgres";
pub const CDF_LOADS_TABLE: &str = "_cdf_loads";
pub const CDF_STATE_TABLE: &str = "_cdf_state";
pub const CDF_QUARANTINE_TABLE: &str = "_cdf_quarantine";
pub const CDF_LOAD_COLUMN: &str = "_cdf_load";
pub const CDF_SEGMENT_COLUMN: &str = "_cdf_segment";
pub const CDF_ROW_COLUMN: &str = "_cdf_row";
pub const CDF_LOADED_AT_COLUMN: &str = "_cdf_loaded_at_ms";
pub const POSTGRES_XID_SQL: &str = "SELECT txid_current()::text AS xid";

mod api;
mod commit;
mod correction;
mod ddl;
mod dml;
mod identifiers;
#[cfg(test)]
mod live_tests;
mod mirrors;
mod package;
mod plan;
mod rows;
mod runtime;
mod sheet;
#[cfg(test)]
mod tests;
mod validate;

pub use api::*;
pub use correction::*;
pub use identifiers::*;
pub use plan::*;
pub use rows::{postgres_columns_for_schema, postgres_type_for_arrow};
pub use runtime::{PostgresRuntime, PostgresRuntimeDriver, validate_replay_target};
pub use sheet::*;
