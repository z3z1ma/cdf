#![doc = "DuckDB destination boundary for cdf."]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_contract::is_framework_variant_field;
use cdf_kernel::{
    CapabilitySupport, CdfError, CommitCounts, CommitPlan, CommitSegment, CommitSession,
    ConcurrencyLimit, CorrectionCommitSession, CorrectionStrategy, CorrectionStrategyCapability,
    DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY, DeliveryGuarantee, DestinationCommitRequest,
    DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest,
    DestinationCorrectionOperation, DestinationCorrectionReceiptEvidence, DestinationId,
    DestinationProtocol, DestinationResidualReadback, DestinationSheet, IdempotencySupport,
    IdentifierRules, MigrationRecord, PlanId, Receipt, ReceiptId, Result, RowProvenanceAddress,
    RowProvenanceCapabilities, SchemaHash, SegmentAck, StateSegment, TargetName,
    TransactionMetadata, TransactionSupport, TypeMapping, TypeMappingFidelity, VerifyClause,
    WriteDisposition,
};
use cdf_package::PackageReader;
use duckdb::{
    AccessMode, Config, Connection, OptionalExt, params, params_from_iter,
    types::{TimeUnit as DuckTimeUnit, Value},
};
const DESTINATION_ID: &str = "duckdb";
const MAIN_SCHEMA: &str = "main";
const LOCK_SUFFIX: &str = "cdf.lock";
pub const CDF_ROW_KEY_COLUMN: &str = "_cdf_row_key";
const CDF_STAGE_ORDER_COLUMN: &str = "_cdf_stage_order";

static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

mod api;
mod arrow_bridge;
mod commit;
mod corrections;
mod mirrors;
mod package;
mod planning;
mod receipts;
mod rows;
mod runtime;
mod sheet;
mod sql;
mod table;
#[cfg(test)]
mod tests;

pub use api::*;
pub use runtime::DuckDbRuntimeDriver;
