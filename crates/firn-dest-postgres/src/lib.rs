#![doc = "Postgres destination boundary for firn."]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use firn_kernel::{
    CapabilitySupport, CommitCounts, CommitPlan, ConcurrencyLimit, DeliveryGuarantee,
    DestinationCommitRequest, DestinationId, DestinationProtocol, DestinationSheet, FirnError,
    IdempotencySupport, IdempotencyToken, IdentifierRules, MigrationRecord, PackageHash, PlanId,
    Receipt, ReceiptId, ResourceId, Result, SchemaHash, SegmentAck, StateDelta, StateSegment,
    TargetName, TransactionMetadata, TransactionSupport, TypeMapping, TypeMappingFidelity,
    VerifyClause, WriteDisposition,
};
use serde::{Deserialize, Serialize};

pub const POSTGRES_DESTINATION_ID: &str = "postgres";
pub const FIRN_LOADS_TABLE: &str = "_firn_loads";
pub const FIRN_STATE_TABLE: &str = "_firn_state";
pub const FIRN_LOAD_COLUMN: &str = "_firn_load";
pub const FIRN_SEGMENT_COLUMN: &str = "_firn_segment";
pub const FIRN_ROW_COLUMN: &str = "_firn_row";
pub const FIRN_LOADED_AT_COLUMN: &str = "_firn_loaded_at_ms";
pub const POSTGRES_XID_SQL: &str = "SELECT txid_current()::text AS xid";

mod api;
mod ddl;
mod dml;
mod identifiers;
mod mirrors;
mod plan;
mod sheet;
#[cfg(test)]
mod tests;
mod validate;

pub use api::*;
pub use identifiers::*;
pub use plan::*;
pub use sheet::*;
