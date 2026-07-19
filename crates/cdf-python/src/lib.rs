#![doc = "Optional Python authoring and interchange boundary for cdf."]

use std::path::{Path, PathBuf};

use cdf_http::{HttpRequest, Redactor, SecretProvider, SecretUri, TraceEvent};
use cdf_kernel::{
    Batch, BatchId, CdfError, PartitionId, ResourceDescriptor, ResourceId, Result, SchemaHash,
    SchemaSnapshotReference, SchemaSource, ScopeKey, SourcePosition, TrustLevel, WriteDisposition,
};
use pyo3::{
    Bound, PyAny, Python,
    prelude::*,
    types::{PyDict, PyModule},
};
use pyo3_arrow::{PyRecordBatch, PyRecordBatchReader};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ARROW_C_ARRAY_METHOD: &str = "__arrow_c_array__";
pub const ARROW_C_STREAM_METHOD: &str = "__arrow_c_stream__";
pub const DEFAULT_DICT_BATCH_ROWS: usize = 8 * 1024;
pub const DEFAULT_BOUNDARY_CHANNEL_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_WATCHDOG_MS: u64 = 300_000;

mod bridge;
mod context;
mod dlt;
mod driver;
mod internal;
mod interpreter;
mod resource;
mod runtime;
#[cfg(test)]
mod tests;

pub use bridge::*;
pub use context::*;
pub use dlt::*;
pub use driver::*;
pub use interpreter::*;
pub use resource::*;
pub use runtime::*;
