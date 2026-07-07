#![doc = "Parquet object-store destination boundary for cdf."]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use cdf_kernel::{
    CapabilitySupport, CdfError, CommitCounts, CommitPlan, CommitSegment, CommitSession,
    ConcurrencyLimit, DeliveryGuarantee, DestinationCommitRequest, DestinationId,
    DestinationProtocol, DestinationSheet, IdempotencySupport, IdentifierRules, PlanId, Receipt,
    ReceiptId, Result, SchemaHash, SegmentAck, SegmentId, StateSegment, TargetName,
    TransactionMetadata, TransactionSupport, TypeMapping, TypeMappingFidelity, VerifyClause,
    WriteDisposition,
};
use cdf_package::{PackageReader, SegmentEntry};
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, PutResult, local::LocalFileSystem,
    path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

const DESTINATION_ID: &str = "parquet_object_store";
const MANIFEST_VERSION: u16 = 1;
const REPLACE_POINTER_VERSION: u16 = 1;

mod api;
mod manifest;
mod package;
mod receipts;
mod sheet;
mod store;
#[cfg(test)]
mod tests;

pub use api::*;
