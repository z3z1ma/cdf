use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use cdf_kernel::{
    CdfError, CommitPlan, DestinationCommitRequest, DestinationCorrectionCommitPlan,
    DestinationCorrectionCommitRequest, DestinationSheet, PlanId, Receipt, Result, SchemaHash,
};

use crate::{
    compression::{PHYSICAL_PLAN_VERSION, ParquetCompression},
    layout::ParquetObjectLayoutPolicy,
    manifest::{
        ParquetCorrectionSidecarManifest, ParquetObjectManifest, ParquetReplacePointerReceipt,
    },
    store::{ObjectKeyEncoder, StoreClient},
};

pub(crate) const STAGING_METADATA_VERSION: u16 = 2;
pub(crate) const OBJECT_PUBLICATION_MODE: &str = "atomic_content_create_v1";

#[derive(Clone)]
pub struct ParquetDestination {
    pub(crate) store: StoreClient,
    pub(crate) execution: cdf_runtime::ExecutionServices,
    pub(crate) sheet: DestinationSheet,
    pub(crate) object_key_encoder: ObjectKeyEncoder,
    pub(crate) compression: ParquetCompression,
    pub(crate) object_layout: ParquetObjectLayoutPolicy,
    pub(crate) pending_corrections: Arc<Mutex<BTreeMap<PlanId, ParquetCorrectionContext>>>,
    #[cfg(test)]
    pub(crate) encode_probe: Option<Arc<ParquetEncodeConcurrencyProbe>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParquetRowLocation {
    pub object_key: String,
    pub row_ordinal: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ParquetCommitRequest {
    pub(crate) commit: DestinationCommitRequest,
    pub(crate) schema_hash: SchemaHash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParquetCommitPlan {
    pub kernel: CommitPlan,
    pub manifest_key: String,
    pub provenance_manifest_key: String,
    pub replace_pointer_key: Option<String>,
    pub current_pointer_key: Option<String>,
    pub duplicate: bool,
    pub rows_planned: u64,
    pub bytes_planned: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StagingAttemptMetadata {
    pub(crate) version: u16,
    pub(crate) target: String,
    pub(crate) attempt_id: String,
    pub(crate) physical_plan_path: String,
    pub(crate) physical_plan_version: u16,
    pub(crate) object_publication_mode: String,
    pub(crate) writers: u16,
    pub(crate) rows_per_batch: u64,
    pub(crate) bytes_per_batch: u64,
    pub(crate) object_layout: ParquetObjectLayoutPolicy,
    pub(crate) started_at_ms: i64,
    pub(crate) staging_lease: cdf_runtime::StagingLease,
}

impl StagingAttemptMetadata {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.version != STAGING_METADATA_VERSION
            || self.physical_plan_version != PHYSICAL_PLAN_VERSION
            || self.object_publication_mode != OBJECT_PUBLICATION_MODE
            || self.writers == 0
            || self.rows_per_batch == 0
            || self.bytes_per_batch == 0
            || self.target.is_empty()
            || self.attempt_id.is_empty()
        {
            return Err(CdfError::destination(
                "Parquet staging metadata contains unsupported or zero physical-plan authority",
            ));
        }
        ParquetCompression::from_path_id(&self.physical_plan_path).map_err(|_| {
            CdfError::destination("Parquet staging metadata names an unsupported physical path")
        })?;
        self.object_layout.validate().map(|_| ()).map_err(|_| {
            CdfError::destination("Parquet staging metadata contains invalid object-layout bounds")
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicationAttemptMetadata {
    pub(crate) version: u16,
    pub(crate) staging_lease: cdf_runtime::StagingLease,
    pub(crate) root_id: cdf_kernel::CommittedContentRootId,
    pub(crate) root_generation: u64,
    pub(crate) manifest_key: String,
    pub(crate) object_layout: ParquetObjectLayoutPolicy,
}

impl PublicationAttemptMetadata {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.version != STAGING_METADATA_VERSION
            || self.root_generation == 0
            || self.manifest_key.is_empty()
        {
            return Err(CdfError::destination(
                "Parquet publication metadata has an unsupported version",
            ));
        }
        self.object_layout.validate().map(|_| ()).map_err(|_| {
            CdfError::destination(
                "Parquet publication metadata contains invalid object-layout bounds",
            )
        })
    }
}

/// Proof that one exact immutable Parquet publication completed before checkpoint admission.
///
/// Construction is private to the publication protocol: callers cannot synthesize
/// commit-bound verification from a receipt alone.
pub(crate) struct CommittedParquetPublication {
    pub(crate) receipt: Receipt,
    pub(crate) verification: ReceiptVerification,
}

impl CommittedParquetPublication {
    pub(crate) fn into_parts(self) -> (Receipt, ReceiptVerification) {
        (self.receipt, self.verification)
    }
}

pub type ReceiptVerification = cdf_kernel::ReceiptVerification;

#[derive(Clone, Debug)]
pub(crate) struct LoadedManifest {
    pub(crate) manifest: ParquetObjectManifest,
    pub(crate) manifest_etag: Option<String>,
    pub(crate) replace_pointer: Option<ParquetReplacePointerReceipt>,
}

#[derive(Clone, Debug)]
pub(crate) struct ParquetCorrectionContext {
    pub(crate) request: DestinationCorrectionCommitRequest,
    pub(crate) plan: DestinationCorrectionCommitPlan,
    pub(crate) sidecar_bytes: Vec<u8>,
    pub(crate) manifest: ParquetCorrectionSidecarManifest,
    pub(crate) manifest_bytes: Vec<u8>,
    pub(crate) manifest_key: String,
    pub(crate) manifest_sha256: String,
    pub(crate) receipt_key: String,
    pub(crate) duplicate_receipt: Option<Receipt>,
}

#[cfg(test)]
pub(crate) struct ParquetEncodeConcurrencyProbe {
    expected: u16,
    state: std::sync::Mutex<(u16, u16)>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
impl ParquetEncodeConcurrencyProbe {
    pub(crate) fn new(expected: u16) -> Self {
        assert!(expected > 0);
        Self {
            expected,
            state: std::sync::Mutex::new((0, 0)),
            changed: std::sync::Condvar::new(),
        }
    }

    pub(crate) fn enter(self: &Arc<Self>) -> ParquetEncodeConcurrencyGuard {
        let mut state = self.state.lock().unwrap();
        state.0 += 1;
        state.1 = state.1.max(state.0);
        self.changed.notify_all();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while state.1 < self.expected {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (next, timeout) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if timeout.timed_out() {
                break;
            }
        }
        drop(state);
        ParquetEncodeConcurrencyGuard {
            probe: Arc::clone(self),
        }
    }

    pub(crate) fn peak(&self) -> u16 {
        self.state.lock().unwrap().1
    }
}

#[cfg(test)]
pub(crate) struct ParquetEncodeConcurrencyGuard {
    probe: Arc<ParquetEncodeConcurrencyProbe>,
}

#[cfg(test)]
impl Drop for ParquetEncodeConcurrencyGuard {
    fn drop(&mut self) {
        let mut state = self.probe.state.lock().unwrap();
        state.0 = state.0.saturating_sub(1);
        self.probe.changed.notify_all();
    }
}
