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
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_contract::is_framework_variant_field;
use cdf_kernel::{
    CapabilitySupport, CdfError, CommitCounts, CommitPlan, ConcurrencyLimit,
    CorrectionCommitSession, CorrectionStrategy, CorrectionStrategyCapability,
    DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY, DeliveryGuarantee, DestinationCommitRequest,
    DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest,
    DestinationCorrectionOperation, DestinationCorrectionReceiptEvidence, DestinationId,
    DestinationProtocol, DestinationResidualReadback, DestinationSheet, IdempotencySupport,
    IdentifierRules, MigrationRecord, PlanId, Receipt, ReceiptId, Result, RowProvenanceAddress,
    RowProvenanceCapabilities, SchemaHash, SegmentAck, SegmentId, TargetName, TransactionMetadata,
    TransactionSupport, TypeMapping, TypeMappingFidelity, VerifyClause, WriteDisposition,
};
use duckdb::{
    AccessMode, Config, Connection, OptionalExt, params, params_from_iter,
    types::{TimeUnit as DuckTimeUnit, Value},
};
const DESTINATION_ID: &str = "duckdb";
const MAIN_SCHEMA: &str = "main";
const LOCK_SUFFIX: &str = "cdf.lock";
const DUCKDB_MINIMUM_NATIVE_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const DUCKDB_DEFAULT_NATIVE_MEMORY_LIMIT_CEILING_BYTES: u64 = 1024 * 1024 * 1024;
const DUCKDB_DEFAULT_TEMP_DIRECTORY_BUDGET_CEILING_BYTES: u64 = 1024 * 1024 * 1024;
const DUCKDB_DEFAULT_INTERNAL_THREADS: i64 = 1;
const DUCKDB_MEMORY_LIMIT_ENV: &str = "CDF_DUCKDB_MEMORY_LIMIT";
const DUCKDB_TEMP_BUDGET_ENV: &str = "CDF_DUCKDB_TEMP_BUDGET";
const DUCKDB_THREADS_ENV: &str = "CDF_DUCKDB_THREADS";
const DUCKDB_BULK_PATH_APPENDER: &str = "arrow_record_batch_appender";
const DUCKDB_BULK_PATH_STREAM_SCAN: &str = "arrow_stream_scan";
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
mod raw;
mod receipts;
mod rows;
mod runtime;
mod sheet;
mod sql;
mod stream_scan;
mod table;
#[cfg(test)]
mod tests;

pub use api::*;
pub use runtime::DuckDbRuntimeDriver;
