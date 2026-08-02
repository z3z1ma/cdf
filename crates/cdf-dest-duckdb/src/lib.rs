#![doc = "DuckDB destination boundary for cdf."]

#[cfg(test)]
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::atomic::Ordering,
};

#[cfg(test)]
use arrow_array::RecordBatch;
#[cfg(test)]
use arrow_array::{
    Time64NanosecondArray, TimestampMillisecondArray, TimestampSecondArray, UInt64Array,
};
#[cfg(test)]
use cdf_kernel::{
    CapabilitySupport, CdfError, CommitCounts, CommitPlan, DestinationCommitRequest,
    DestinationProtocol, IdempotencySupport, MigrationRecord, Receipt, Result, SchemaHash,
    TargetName, TransactionSupport, WriteDisposition,
};
#[cfg(test)]
use duckdb::{
    Connection, params,
    types::{TimeUnit as DuckTimeUnit, Value},
};
use std::sync::atomic::AtomicU64;
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
mod models;
mod package;
mod profiling;
mod receipts;
mod rows;
mod runtime;
#[allow(
    unsafe_code,
    reason = "DuckDB C API exception governed by .10x/decisions/compiler-enforced-rust-safety-walls.md"
)]
mod segment_scan;
mod sheet;
mod sql;
mod table;
#[cfg(test)]
mod tests;
mod writer_lock;

pub use models::{
    DuckDbDestination, DuckDbMirrorLoadRow, DuckDbMirrorSnapshot, DuckDbMirrorStateRow, IcuProbe,
    ReceiptVerification,
};
pub use runtime::DuckDbRuntimeDriver;
