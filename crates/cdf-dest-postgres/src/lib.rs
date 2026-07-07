#![doc = "Postgres destination boundary for cdf."]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use cdf_kernel::{
    CapabilitySupport, CdfError, CommitCounts, CommitPlan, CommitSession, ConcurrencyLimit,
    DeliveryGuarantee, DestinationCommitRequest, DestinationId, DestinationProtocol,
    DestinationSheet, IdempotencySupport, IdempotencyToken, IdentifierRules, MigrationRecord,
    PackageHash, PlanId, Receipt, ReceiptId, ResourceId, Result, SchemaHash, SegmentAck,
    StateDelta, StateSegment, TargetName, TransactionMetadata, TransactionSupport, TypeMapping,
    TypeMappingFidelity, VerifyClause, WriteDisposition,
};
use serde::{Deserialize, Serialize};

pub const POSTGRES_DESTINATION_ID: &str = "postgres";
pub const CDF_LOADS_TABLE: &str = "_cdf_loads";
pub const CDF_STATE_TABLE: &str = "_cdf_state";
pub const CDF_LOAD_COLUMN: &str = "_cdf_load";
pub const CDF_SEGMENT_COLUMN: &str = "_cdf_segment";
pub const CDF_ROW_COLUMN: &str = "_cdf_row";
pub const CDF_LOADED_AT_COLUMN: &str = "_cdf_loaded_at_ms";
pub const POSTGRES_XID_SQL: &str = "SELECT txid_current()::text AS xid";

mod api;
mod commit;
mod ddl;
mod dml;
mod identifiers;
#[cfg(test)]
mod live_tests;
mod mirrors;
mod package;
mod plan;
mod rows;
mod sheet;
mod source;
#[cfg(test)]
mod tests;
mod validate;

pub use api::*;
pub use identifiers::*;
pub use plan::*;
pub use sheet::*;
pub use source::*;
