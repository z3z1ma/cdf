#![doc = "Postgres destination boundary for cdf."]

#[cfg(test)]
use std::collections::BTreeMap;

#[cfg(test)]
use cdf_kernel::{
    CapabilitySupport, CdfError, CommitCounts, CommitSession, CorrectionStrategy,
    CorrectionStrategyCapability, DeliveryGuarantee, DestinationCommitRequest,
    DestinationCorrectionCommitRequest, DestinationProtocol, IdempotencySupport, IdempotencyToken,
    PackageHash, Receipt, ReceiptId, Result, SchemaHash, StateDelta, StateSegment, TargetName,
    TransactionSupport, WriteDisposition,
};

pub use cdf_postgres::{PostgresIdentifier, PostgresTarget, quote_identifier};

pub const POSTGRES_DESTINATION_ID: &str = "postgres";
pub const CDF_LOADS_TABLE: &str = "_cdf_loads";
pub const CDF_STATE_TABLE: &str = "_cdf_state";
pub const CDF_QUARANTINE_TABLE: &str = "_cdf_quarantine";
pub const CDF_ROW_KEY_ALLOCATOR_TABLE: &str = "_cdf_row_key_allocator";
pub const CDF_SEGMENTS_TABLE: &str = "_cdf_segments";
pub const CDF_ROW_KEY_COLUMN: &str = "_cdf_row_key";
pub const CDF_LOADED_AT_COLUMN: &str = "_cdf_loaded_at_ms";
pub const POSTGRES_XID_SQL: &str = "SELECT txid_current()::text AS xid";

mod api;
mod binary_copy;
mod commit;
mod corrections;
mod ddl;
mod dml;
mod identifiers;
#[cfg(test)]
mod live_tests;
mod mirrors;
mod models;
mod package;
mod plan;
mod rows;
mod runtime;
mod sheet;
#[cfg(test)]
mod tests;
mod validate;

pub use api::{PostgresReceiptVerification, build_receipt, plan_postgres_load};
pub use corrections::{plan_postgres_correction, postgres_correction_capabilities};
pub use identifiers::{PostgresColumn, PostgresExistingColumn, PostgresExistingTable};
pub use models::{
    PostgresCorrectionFieldPlan, PostgresCorrectionPlan, PostgresCorrectionPlanInput,
    PostgresDestination, PostgresDestinationSheet, PostgresTypeFidelity, PostgresTypeMapping,
};
pub use plan::{
    MergeDedupPolicy, PostgresDriftHooks, PostgresLoadPlan, PostgresLoadPlanInput,
    PostgresReceiptInput, PostgresStatement, StatementExpectation,
};
pub use rows::{postgres_columns_for_schema, postgres_type_for_arrow};
pub use runtime::{PostgresRuntime, PostgresRuntimeDriver, validate_replay_target};
pub use sheet::{postgres_destination_sheet, postgres_type_mappings};
