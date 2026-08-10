#![doc = "Parquet object-store destination boundary for cdf."]

#[cfg(test)]
use std::{collections::BTreeMap, fs, path::Path, sync::Arc};

#[cfg(test)]
use arrow_array::RecordBatch;
#[cfg(test)]
use cdf_kernel::{
    CapabilitySupport, CdfError, DestinationCommitRequest, DestinationProtocol, IdempotencySupport,
    ObjectKeyRules, Receipt, Result, SchemaHash, TargetName, TransactionSupport, WriteDisposition,
};
#[cfg(test)]
use cdf_kernel::{CommitSegment, StateSegment};
#[cfg(test)]
use cdf_package::PackageReader;
#[cfg(test)]
use object_store::{ObjectStore, ObjectStoreExt};

const DESTINATION_ID: &str = "parquet_object_store";
const MANIFEST_VERSION: u16 = 5;
const REPLACE_POINTER_VERSION: u16 = 1;
const CORRECTION_SIDECAR_VERSION: u16 = 1;
const CORRECTION_SIDECAR_MANIFEST_VERSION: u16 = 1;

mod api;
mod compression;
mod corrections;
mod layout;
mod manifest;
mod models;
mod package;
mod publication;
mod receipts;
mod runtime;
mod sheet;
mod staging;
mod store;
#[cfg(test)]
mod tests;

pub use compression::ParquetCompression;
pub use corrections::{
    ParquetVersionedRematerializationPlan, ParquetVersionedRematerializationRequest,
};
pub use layout::ParquetObjectLayoutPolicy;
pub use models::{ParquetDestination, ParquetRowLocation, ReceiptVerification};
pub use runtime::{FilesystemParquetRuntime, ParquetRuntimeDriver};
