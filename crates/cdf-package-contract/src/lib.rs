#![doc = "Canonical package artifacts and verified-access contracts for cdf."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "foundational production code must propagate recoverable failures"
    )
)]

mod access;
mod artifacts;
mod late_data;
mod model;
mod provenance;
mod quarantine;
mod receipt;

pub use access::{SharedVerifiedPackageAccess, VerifiedPackageAccess};
pub use artifacts::{
    DEDUP_PROVENANCE_DIRECTORY, DEDUP_PROVENANCE_VERSION, DEDUP_SUMMARY_FILE,
    DEDUP_SUMMARY_VERSION, DESTINATION_COMMIT_PLAN_FILE, DESTINATION_COMMIT_PLAN_VERSION,
    DestinationCommitPlanPreimage, IdempotencyTokenSource,
    PARTITION_WATERMARK_STATE_ARTIFACT_VERSION, PARTITION_WATERMARK_STATE_FILE,
    PROCESSED_OBSERVATIONS_FILE, PROCESSED_OBSERVATIONS_VERSION, PackageDedupKeep,
    PackageDedupSummary, PackageReplayInputs, PartitionWatermarkStateArtifact,
    ProcessedObservationEvidenceArtifact, SCAN_PLAN_FILE, STATE_INPUT_CHECKPOINT_FILE,
    STATE_PROPOSED_DELTA_FILE, StateDeltaPreimage, dedup_provenance_shard_path,
};
pub use late_data::{
    LATE_DATA_EVIDENCE_FILE, LATE_DATA_EVIDENCE_VERSION, LATE_DATA_PAYLOAD_CATALOG_FILE,
    LATE_DATA_PAYLOAD_CATALOG_VERSION, LateDataBatchEvidence, LateDataEvidence,
    LateDataPayloadArtifact, LateDataPayloadCatalog, LateDataPayloadLocation, LateDataRowEvidence,
};
pub use model::{
    FileEntry, LifecycleState, MANIFEST_FILE, MANIFEST_VERSION, ManifestArchives, ManifestIdentity,
    PackageManifest, PackageStatus, ParquetArchiveMetadata, RECEIPTS_FILE, REQUIRED_DIRECTORIES,
    SegmentEntry, SignatureSlot, TRACE_FILE, TombstoneReport, VerificationReport,
};
pub use provenance::{
    CDF_INTERNAL_VISIBILITY, CDF_PACKAGE_ROW_ORD_FIELD, CDF_PACKAGE_ROW_ORDINAL_SEMANTIC,
    CDF_VISIBILITY_METADATA_KEY, PackageRowOrdinalValidator, append_package_row_ord,
    canonical_segment_schema, is_package_row_ord_field, logical_output_schema,
    package_row_ord_array, package_row_ord_field, strip_package_row_ord,
    validate_logical_output_schema, validate_package_row_ord_batches,
    validate_segment_ordinal_manifest,
};
pub use quarantine::{QuarantineObservedValue, QuarantineRecord};
pub use receipt::{ReceiptDraft, ReceiptEvidence};
