#![doc = "Package builder and reader boundary for cdf."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "foundational production code must propagate recoverable failures"
    )
)]

mod archive;
mod artifacts;
mod builder;
mod draft_index;
mod json;
mod manifest_stream;
mod ops;
mod package_fs;
mod parquet;
mod quarantine;
mod reader;
mod runtime_schema;
mod statistics_profile;
mod storage;

pub use archive::{
    ARCHIVE_FIDELITY_STATEMENT, PackageArchiveFidelityReport, PackageArchiveWriteStatus,
    PersistedPackageArchiveReport, persist_package_parquet_archive,
};
#[cfg(test)]
pub(crate) use archive::{
    ARCHIVE_SEGMENT_WINDOW_BYTES, ArchiveSegmentMetadata,
    write_streamed_archive_temp_tree_with_memory,
};
pub use builder::{
    EncodedPackageSegment, PackageBuilder, PackageSegmentEncoder, QuarantineArtifactWriter,
    RegisteredPackageSegment, SegmentWriteMetrics, StreamingIdentityArtifact,
};
pub use draft_index::{PackageBuilderResources, PackageDraftIndexLimits};
pub use json::{canonical_json_bytes, manifest_identity_hash};
pub use manifest_stream::{
    ManifestFileStream, ManifestIdentityHeader, ManifestSegmentStream, PackageManifestHeader,
    stored_manifest_identity_hash, visit_package_manifest,
};
pub use ops::{
    append_receipt, read_manifest, read_manifest_header, tombstone_package, update_package_status,
    verify_package, verify_package_identity, visit_manifest_entries,
};
pub use parquet::{transcode_record_batches_to_parquet_bytes, validate_parquet_schema};
pub use quarantine::{
    for_each_quarantine_record_in_parquet_file, quarantine_record_count_in_parquet_file,
};
#[cfg(test)]
pub(crate) use reader::SEGMENT_STREAM_MEMORY_CONSUMER;
pub use reader::{
    AccountedSegment, AccountedSegmentStream, DurableSegmentFile, PackageReader,
    VerifiedIdentityObject, VerifiedPackage, VerifiedPackageReader, VerifiedSegmentObject,
    VerifiedSegmentObjectStream,
};
pub use runtime_schema::RUNTIME_ARROW_SCHEMA_FILE;
pub(crate) use runtime_schema::{runtime_schema_bytes, runtime_schema_from_reader};
pub use statistics_profile::{
    STATISTICS_PROFILE_FILE, StatisticsProfileGrain, StatisticsProfileRow, StatisticsProfileWriter,
    VerifiedStatisticsProfileWindow,
};
pub use storage::encode_canonical_segment_ipc;

#[cfg(test)]
mod tests;
