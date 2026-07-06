#![doc = "DuckDB destination boundary for firn."]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
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
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use duckdb::{
    Connection, OptionalExt, appender_params_from_iter, params,
    types::{TimeUnit as DuckTimeUnit, Value},
};
use firn_kernel::{
    CapabilitySupport, CommitCounts, CommitPlan, ConcurrencyLimit, DeliveryGuarantee,
    DestinationCommitRequest, DestinationId, DestinationProtocol, DestinationSheet, FirnError,
    IdempotencySupport, IdentifierRules, MigrationRecord, PlanId, Receipt, ReceiptId, Result,
    SchemaHash, SegmentAck, StateSegment, TargetName, TransactionMetadata, TransactionSupport,
    TypeMapping, TypeMappingFidelity, VerifyClause, WriteDisposition,
};
use firn_package::{PackageReader, SegmentEntry};
use serde::{Deserialize, Serialize};

const DESTINATION_ID: &str = "duckdb";
const MAIN_SCHEMA: &str = "main";
const LOCK_SUFFIX: &str = "firn.lock";

static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

mod api;
mod commit;
mod mirrors;
mod package;
mod planning;
mod receipts;
mod rows;
mod sheet;
mod sql;
mod table;
#[cfg(test)]
mod tests;

pub use api::*;
