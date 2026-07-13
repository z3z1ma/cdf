#![doc = "Parquet object-store destination boundary for cdf."]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use cdf_kernel::{
    CapabilitySupport, CdfError, CommitCounts, CommitPlan, CommitSegment, CommitSession,
    ConcurrencyLimit, CorrectionCommitSession, CorrectionStrategy, CorrectionStrategyCapability,
    DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY,
    DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_KEY, DeliveryGuarantee,
    DestinationCommitRequest, DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest,
    DestinationCorrectionOperation, DestinationCorrectionReceiptEvidence,
    DestinationCorrectionSidecarObjectEvidence, DestinationCorrectionSidecarReceiptEvidence,
    DestinationId, DestinationProtocol, DestinationSheet, IdempotencySupport, IdentifierRules,
    ObjectKeyPolicy, ObjectKeyRules, PackageHash, PlanId, PromotionId, Receipt, ReceiptId, Result,
    RowProvenanceAddress, RowProvenanceCapabilities, SchemaHash, SegmentAck, SegmentId,
    StateSegment, TargetName, TransactionMetadata, TransactionSupport, TypeMapping,
    TypeMappingFidelity, VerifyClause, WriteDisposition,
};
#[cfg(test)]
use cdf_package::PackageReader;
use cdf_package::SegmentEntry;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, PutResult,
    local::LocalFileSystem, path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const DESTINATION_ID: &str = "parquet_object_store";
const MANIFEST_VERSION: u16 = 1;
const REPLACE_POINTER_VERSION: u16 = 1;
const CORRECTION_SIDECAR_VERSION: u16 = 1;
const CORRECTION_SIDECAR_MANIFEST_VERSION: u16 = 1;

mod api;
mod corrections;
mod manifest;
mod package;
mod receipts;
mod runtime;
mod sheet;
mod store;
#[cfg(test)]
mod tests;

pub use api::*;
pub use corrections::{
    ParquetVersionedRematerializationPlan, ParquetVersionedRematerializationRequest,
};
pub use runtime::{FilesystemParquetRuntime, ParquetRuntimeDriver};
