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

#[cfg(test)]
use arrow_array::RecordBatch;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
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
    RowProvenanceCapabilities, SchemaHash, SegmentAck, TargetName, TransactionMetadata,
    TransactionSupport, TypeMapping, TypeMappingFidelity, VerifyClause, WriteDisposition,
};
use duckdb::{
    AccessMode, Config, Connection, OptionalExt, params, params_from_iter,
    types::{TimeUnit as DuckTimeUnit, Value},
};
const DESTINATION_ID: &str = "duckdb";
const MAIN_SCHEMA: &str = "main";
const LOCK_SUFFIX: &str = "cdf.lock";
const DUCKDB_CONSERVATIVE_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const DUCKDB_DEFAULT_NATIVE_MEMORY_LIMIT_CEILING_BYTES: u64 = 1024 * 1024 * 1024;
const DUCKDB_DEFAULT_TEMP_DIRECTORY_BUDGET_CEILING_BYTES: u64 = 1024 * 1024 * 1024;
const DUCKDB_DEFAULT_INTERNAL_THREADS: i64 = 1;
const DUCKDB_DEFAULT_MAX_IN_FLIGHT_BYTES: u64 = 256 * 1024 * 1024;
const DUCKDB_MEMORY_LIMIT_ENV: &str = "CDF_DUCKDB_MEMORY_LIMIT";
const DUCKDB_TEMP_BUDGET_ENV: &str = "CDF_DUCKDB_TEMP_BUDGET";
const DUCKDB_THREADS_ENV: &str = "CDF_DUCKDB_THREADS";
const DUCKDB_SCAN_THREADS_ENV: &str = "CDF_DUCKDB_SCAN_THREADS";
const DUCKDB_MAX_IN_FLIGHT_BYTES_ENV: &str = "CDF_DUCKDB_MAX_IN_FLIGHT_BYTES";
const DUCKDB_PROFILE_DIRECTORY_ENV: &str = "CDF_DUCKDB_PROFILE_DIRECTORY";
const DUCKDB_BULK_PATH_SEGMENT_SCAN: &str = "canonical_segment_scan";
const DUCKDB_STAGED_INGRESS_LANE: &str = "duckdb.staged_ingress";
const DUCKDB_FINAL_BINDING_LANE: &str = "duckdb.final_binding";
pub const CDF_ROW_KEY_COLUMN: &str = "_cdf_row_key";
const CDF_STAGE_ORDER_COLUMN: &str = "_cdf_stage_order";

static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

mod api;
mod commit;
mod corrections;
mod ingest_envelope;
mod mirrors;
mod package;
mod planning;
mod profiling;
mod receipts;
mod rows;
mod runtime;
mod segment_scan;
mod sheet;
mod sql;
mod table;
#[cfg(test)]
mod tests;

pub use api::*;
pub use runtime::DuckDbRuntimeDriver;
