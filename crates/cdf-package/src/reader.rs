use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, RecordBatch, UInt64Array};
use cdf_kernel::{
    CdfError, Checkpoint, CommitSegment, PackageHash, PayloadRetention, Receipt, Result, ScanPlan,
    SegmentId, StateSegment,
};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest,
    record_batch_retained_bytes, reserve_blocking,
};
use cdf_package_contract::{
    DEDUP_PROVENANCE_DIRECTORY, DEDUP_PROVENANCE_VERSION, DEDUP_SUMMARY_FILE,
    DEDUP_SUMMARY_VERSION, DESTINATION_COMMIT_PLAN_FILE, DestinationCommitPlanPreimage,
    LATE_DATA_EVIDENCE_FILE, LATE_DATA_PAYLOAD_CATALOG_FILE, LateDataEvidence,
    LateDataPayloadCatalog, PROCESSED_OBSERVATIONS_FILE, PackageManifest, PackageReplayInputs,
    PackageStatus, ProcessedObservationEvidenceArtifact, QuarantineRecord, ReplayView,
    SCAN_PLAN_FILE, STATE_INPUT_CHECKPOINT_FILE, STATE_PROPOSED_DELTA_FILE, SegmentEntry,
    StateDeltaPreimage, TombstoneReport, VerificationReport, VerifiedPackageAccess,
    dedup_provenance_shard_path,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

use crate::{
    artifacts::{read_json_artifact, read_optional_json_artifact},
    ops::{
        append_receipt, read_manifest_from_root, read_receipts, read_segment_file_from_root,
        tombstone_package, update_package_status, verify_package_from_root,
    },
    package_fs::PackageRoot,
    quarantine::{
        for_each_quarantine_record_in_package_file, quarantine_record_count_in_package_file,
    },
    storage::{io_error, normalize_artifact_path, package_path, sync_directory},
};

#[derive(Clone, Debug)]
pub struct PackageReader {
    package_dir: PathBuf,
    package_root: Arc<PackageRoot>,
    manifest: Arc<PackageManifest>,
}

/// Authority that one package identity was fully verified for a bounded
/// consumption operation. Its fields are private so callers cannot substitute
/// a hash-only assertion for package verification.
#[derive(Clone, Debug)]
pub struct VerifiedPackage {
    package_hash: String,
    _package_root: Arc<PackageRoot>,
}

impl PartialEq for VerifiedPackage {
    fn eq(&self, other: &Self) -> bool {
        self.package_hash == other.package_hash
            && self._package_root.same_object(&other._package_root)
    }
}

impl Eq for VerifiedPackage {}

#[derive(Clone, Debug)]
pub struct VerifiedPackageReader {
    reader: PackageReader,
    verified: VerifiedPackage,
}

impl VerifiedPackageAccess for VerifiedPackageReader {
    fn package_hash(&self) -> &str {
        self.verified.package_hash()
    }

    fn for_each_identity_segment(
        &self,
        visitor: &mut dyn FnMut(SegmentEntry) -> Result<()>,
    ) -> Result<()> {
        for segment in &self.reader.manifest.identity.segments {
            visitor(segment.clone())?;
        }
        Ok(())
    }

    fn recorded_scan_plan(&self) -> Result<ScanPlan> {
        self.reader.recorded_scan_plan_verified(&self.verified)
    }

    fn replay_inputs(&self) -> Result<PackageReplayInputs> {
        self.reader.replay_inputs_verified(&self.verified)
    }

    fn runtime_arrow_schema(&self) -> Result<arrow_schema::SchemaRef> {
        self.reader.runtime_arrow_schema_verified(&self.verified)
    }

    fn for_each_quarantine_record(
        &self,
        visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
    ) -> Result<()> {
        self.reader.require_verification(&self.verified)?;
        self.reader.for_each_quarantine_record(visitor)
    }
}

impl VerifiedPackageReader {
    pub fn reader(&self) -> &PackageReader {
        &self.reader
    }

    pub fn reader_mut(&mut self) -> &mut PackageReader {
        &mut self.reader
    }

    pub fn verification(&self) -> &VerifiedPackage {
        &self.verified
    }

    pub fn replay_inputs(&self) -> Result<PackageReplayInputs> {
        self.reader.replay_inputs_verified(&self.verified)
    }
}

impl VerifiedPackage {
    pub(crate) fn from_finalization(
        package_dir: &Path,
        manifest: &PackageManifest,
    ) -> Result<Self> {
        let package_root = Arc::new(PackageRoot::open(package_dir)?);
        Ok(Self {
            package_hash: manifest.package_hash.clone(),
            _package_root: package_root,
        })
    }

    pub fn package_hash(&self) -> &str {
        &self.package_hash
    }
}

#[derive(Debug)]
pub struct VerifiedSegment<T> {
    pub entry: SegmentEntry,
    pub authority: T,
    pub batches: Vec<RecordBatch>,
    window: Arc<VerifiedSegmentWindow>,
}

/// A fully verified canonical segment whose local bytes can be consumed without decoding.
///
/// `read()` remains available for destinations that need Arrow batches. Keeping verification and
/// the local object in one value prevents a hash-only path from being substituted for package
/// authority while allowing native consumers to avoid a redundant IPC decode.
#[derive(Debug)]
pub struct VerifiedSegmentObject<T> {
    pub entry: SegmentEntry,
    pub authority: T,
    package_root: Arc<PackageRoot>,
    display_path: PathBuf,
    _verification: Arc<VerifiedPackage>,
}

/// A verified identity-bearing package artifact retained beneath the package root capability.
#[derive(Debug)]
pub struct VerifiedIdentityObject {
    relative_path: String,
    byte_count: u64,
    sha256: String,
    package_root: Arc<PackageRoot>,
    _verification: Arc<VerifiedPackage>,
}

impl VerifiedIdentityObject {
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn open_file(&self) -> Result<std::fs::File> {
        self.package_root.open_std_file(&self.relative_path)
    }
}

impl<T> VerifiedSegmentObject<T> {
    /// Returns a pathname spelling for diagnostics only.
    pub fn display_path(&self) -> &Path {
        &self.display_path
    }

    /// Opens the exact segment beneath the retained package capability.
    pub fn open_file(&self) -> Result<std::fs::File> {
        self.package_root.open_std_file(&self.entry.path)
    }

    pub fn read(
        self,
        memory: Arc<dyn MemoryCoordinator>,
        maximum_segment_bytes: u64,
    ) -> Result<VerifiedSegment<T>> {
        load_verified_segment(
            self.package_root,
            self.entry,
            self.authority,
            memory,
            maximum_segment_bytes,
        )
    }
}

#[derive(Debug)]
struct VerifiedSegmentWindow {
    memory_lease: MemoryLease,
}

impl<T> VerifiedSegment<T> {
    pub fn accounted_bytes(&self) -> u64 {
        self.window.memory_lease.bytes()
    }

    pub fn into_commit_segment(self) -> Result<CommitSegment>
    where
        T: Into<StateSegment>,
    {
        let retained_bytes = self.accounted_bytes();
        let retention = PayloadRetention::new(self.window, retained_bytes)?;
        Ok(
            CommitSegment::new(self.authority.into(), self.entry.byte_count, self.batches)
                .with_retention(retention),
        )
    }
}

pub struct VerifiedSegmentStream<T> {
    package_root: Arc<PackageRoot>,
    segments: VerifiedSegmentItems<T>,
    memory: Arc<dyn MemoryCoordinator>,
    maximum_segment_bytes: u64,
    failed: bool,
}

pub struct VerifiedSegmentObjectStream<T> {
    package_dir: PathBuf,
    package_root: Arc<PackageRoot>,
    verified: Arc<VerifiedPackage>,
    segments: VerifiedSegmentItems<T>,
}

enum VerifiedSegmentItems<T> {
    Manifest {
        manifest: Arc<PackageManifest>,
        next_index: usize,
        authority: fn() -> T,
    },
    ManifestAuthorities {
        manifest: Arc<PackageManifest>,
        next_index: usize,
        authorities: std::vec::IntoIter<T>,
    },
}

impl<T> VerifiedSegmentItems<T> {
    fn manifest(manifest: Arc<PackageManifest>, authority: fn() -> T) -> Self {
        Self::Manifest {
            manifest,
            next_index: 0,
            authority,
        }
    }

    fn manifest_authorities(manifest: Arc<PackageManifest>, authorities: Vec<T>) -> Self {
        Self::ManifestAuthorities {
            manifest,
            next_index: 0,
            authorities: authorities.into_iter(),
        }
    }
}

impl<T> Iterator for VerifiedSegmentItems<T> {
    type Item = (SegmentEntry, T);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Manifest {
                manifest,
                next_index,
                authority,
            } => {
                let entry = manifest.identity.segments.get(*next_index)?.clone();
                *next_index = next_index.checked_add(1)?;
                Some((entry, authority()))
            }
            Self::ManifestAuthorities {
                manifest,
                next_index,
                authorities,
            } => {
                let authority = authorities.next()?;
                let entry = manifest.identity.segments.get(*next_index)?.clone();
                *next_index = next_index.checked_add(1)?;
                Some((entry, authority))
            }
        }
    }
}

impl<T> Iterator for VerifiedSegmentObjectStream<T> {
    type Item = VerifiedSegmentObject<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let (entry, authority) = self.segments.next()?;
        Some(VerifiedSegmentObject {
            display_path: package_path(&self.package_dir, &entry.path),
            package_root: Arc::clone(&self.package_root),
            entry,
            authority,
            _verification: self.verified.clone(),
        })
    }
}

fn verified_manifest_segment_stream(
    package_root: Arc<PackageRoot>,
    manifest: Arc<PackageManifest>,
    memory: Arc<dyn MemoryCoordinator>,
    maximum_segment_bytes: u64,
) -> Result<VerifiedSegmentStream<()>> {
    validate_verified_segment_window(memory.as_ref(), maximum_segment_bytes)?;
    Ok(VerifiedSegmentStream {
        package_root,
        segments: VerifiedSegmentItems::manifest(manifest, || ()),
        memory,
        maximum_segment_bytes,
        failed: false,
    })
}

fn verified_manifest_authority_segment_stream<T>(
    package_root: Arc<PackageRoot>,
    manifest: Arc<PackageManifest>,
    authorities: Vec<T>,
    memory: Arc<dyn MemoryCoordinator>,
    maximum_segment_bytes: u64,
) -> Result<VerifiedSegmentStream<T>> {
    validate_verified_segment_window(memory.as_ref(), maximum_segment_bytes)?;
    Ok(VerifiedSegmentStream {
        package_root,
        segments: VerifiedSegmentItems::manifest_authorities(manifest, authorities),
        memory,
        maximum_segment_bytes,
        failed: false,
    })
}

fn validate_verified_segment_window(
    memory: &dyn MemoryCoordinator,
    maximum_segment_bytes: u64,
) -> Result<()> {
    if maximum_segment_bytes == 0 {
        return Err(CdfError::contract(
            "verified segment stream window must be nonzero",
        ));
    }
    let budget_bytes = memory.snapshot().budget_bytes;
    if maximum_segment_bytes > budget_bytes {
        return Err(CdfError::data(format!(
            "verified segment stream window {maximum_segment_bytes} exceeds managed budget {budget_bytes}"
        )));
    }
    Ok(())
}

fn validate_commit_segment_authority(
    manifest: &[SegmentEntry],
    requested: &[StateSegment],
) -> Result<()> {
    for (index, state) in requested.iter().enumerate() {
        let Some(entry) = manifest.get(index) else {
            if requested[..index]
                .iter()
                .any(|prior| prior.segment_id == state.segment_id)
            {
                return Err(CdfError::data(format!(
                    "destination commit request contains duplicate segment {}",
                    state.segment_id
                )));
            }
            return Err(CdfError::data(format!(
                "destination commit request segment {} is not present in the package manifest",
                state.segment_id
            )));
        };
        if entry.segment_id != state.segment_id {
            if requested[..index]
                .iter()
                .any(|prior| prior.segment_id == state.segment_id)
            {
                return Err(CdfError::data(format!(
                    "destination commit request contains duplicate segment {}",
                    state.segment_id
                )));
            }
            if manifest
                .iter()
                .any(|candidate| candidate.segment_id == state.segment_id)
            {
                return Err(CdfError::data(format!(
                    "destination commit request segment {} is not in canonical package order at ordinal {index}",
                    state.segment_id
                )));
            }
            return Err(CdfError::data(format!(
                "destination commit request segment {} is not present in the package manifest",
                state.segment_id
            )));
        }
        if state.row_count != entry.row_count {
            return Err(CdfError::data(format!(
                "destination commit request segment {} has {} rows but package manifest has {} rows",
                state.segment_id, state.row_count, entry.row_count
            )));
        }
    }
    if let Some(entry) = manifest.get(requested.len()) {
        return Err(CdfError::data(format!(
            "package manifest segment {} is missing from destination commit request",
            entry.segment_id
        )));
    }
    Ok(())
}

impl<T> Iterator for VerifiedSegmentStream<T> {
    type Item = Result<VerifiedSegment<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let (entry, authority) = self.segments.next()?;
        let result = load_verified_segment(
            Arc::clone(&self.package_root),
            entry,
            authority,
            Arc::clone(&self.memory),
            self.maximum_segment_bytes,
        );
        if result.is_err() {
            self.failed = true;
        }
        Some(result)
    }
}

fn load_verified_segment<T>(
    package_root: Arc<PackageRoot>,
    entry: SegmentEntry,
    authority: T,
    memory: Arc<dyn MemoryCoordinator>,
    maximum_segment_bytes: u64,
) -> Result<VerifiedSegment<T>> {
    if maximum_segment_bytes == 0 || maximum_segment_bytes > memory.snapshot().budget_bytes {
        return Err(CdfError::data(format!(
            "verified segment stream window {maximum_segment_bytes} must be nonzero and no larger than managed budget {}",
            memory.snapshot().budget_bytes
        )));
    }
    let request = ReservationRequest::new(
        ConsumerKey::new("verified-segment-stream", MemoryClass::Package)?,
        maximum_segment_bytes,
    )?
    .as_minimum_working_set();
    let lease = reserve_blocking(Arc::clone(&memory), &request)?;
    let batches = read_segment_file_from_root(&package_root, &entry.path)?;
    let retained_bytes = batches.iter().try_fold(0u64, |total, batch| {
        total
            .checked_add(record_batch_retained_bytes(batch)?)
            .ok_or_else(|| CdfError::data("verified segment retained memory overflow"))
    })?;
    if retained_bytes > maximum_segment_bytes {
        return Err(CdfError::data(format!(
            "segment {} retains {retained_bytes} Arrow bytes above its {maximum_segment_bytes}-byte verified stream window; raise the stream window or rebuild with a smaller canonical segment maximum",
            entry.segment_id
        )));
    }
    let row_count = batches.iter().try_fold(0u64, |total, batch| {
        total
            .checked_add(
                u64::try_from(batch.num_rows())
                    .map_err(|_| CdfError::data("verified segment row count exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("verified segment row count overflow"))
    })?;
    if row_count != entry.row_count {
        return Err(CdfError::data(format!(
            "segment {} manifest row count {} differs from package data {row_count}",
            entry.segment_id, entry.row_count
        )));
    }
    cdf_package_contract::validate_package_row_ord_batches(
        &batches,
        entry.package_row_ord_start,
        entry.row_count,
    )?;
    lease.reconcile(retained_bytes.max(1))?;
    let window = Arc::new(VerifiedSegmentWindow {
        memory_lease: lease,
    });
    Ok(VerifiedSegment {
        entry,
        authority,
        batches,
        window,
    })
}

impl PackageReader {
    pub fn open(package_dir: impl AsRef<Path>) -> Result<Self> {
        let package_root = Arc::new(PackageRoot::open(package_dir.as_ref())?);
        let package_dir = package_root.path().to_path_buf();
        let manifest = read_manifest_from_root(&package_root)?;
        Ok(Self {
            package_dir,
            package_root,
            manifest: Arc::new(manifest),
        })
    }

    pub fn manifest(&self) -> &PackageManifest {
        self.manifest.as_ref()
    }

    /// Removes an incomplete owner-private construction so its deterministic identity can be
    /// re-driven. Replayable packages and any construction carrying a receipt are artifacts and
    /// cannot cross this deletion boundary.
    pub fn discard_incomplete_construction(self, expected_package_id: &str) -> Result<()> {
        if self.manifest.identity.package_id != expected_package_id {
            return Err(CdfError::data(format!(
                "incomplete package {} has identity {:?}, expected {:?}",
                self.package_dir.display(),
                self.manifest.identity.package_id,
                expected_package_id
            )));
        }
        if self.manifest.lifecycle.status.is_replayable() {
            return Err(CdfError::data(format!(
                "package {} is {} and must be recovered through verified replay, not discarded",
                self.package_dir.display(),
                self.manifest.lifecycle.status.as_str()
            )));
        }
        if !self.receipts()?.is_empty() {
            return Err(CdfError::data(format!(
                "incomplete package {} carries a durable destination receipt and cannot be discarded",
                self.package_dir.display()
            )));
        }
        let package_dir = self.package_dir.clone();
        let parent = package_dir.parent().map(Path::to_path_buf);
        drop(self);
        fs::remove_dir_all(&package_dir)
            .map_err(|error| io_error(format!("remove {}", package_dir.display()), error))?;
        if let Some(parent) = parent {
            sync_directory(&parent)?;
        }
        Ok(())
    }

    pub fn recorded_scan_plan_verified(&self, verified: &VerifiedPackage) -> Result<ScanPlan> {
        self.verified_json_artifact(verified, SCAN_PLAN_FILE)
    }

    pub fn identity_segments_verified(
        &self,
        verified: &VerifiedPackage,
    ) -> Result<&[SegmentEntry]> {
        self.require_verification(verified)?;
        Ok(&self.manifest.identity.segments)
    }

    pub fn verify(&self) -> Result<VerificationReport> {
        verify_package_from_root(&self.package_root, &self.manifest)
    }

    pub fn verify_for_consumption(&self) -> Result<VerifiedPackage> {
        let report = self.verify()?;
        Ok(VerifiedPackage {
            package_hash: report.package_hash,
            _package_root: Arc::clone(&self.package_root),
        })
    }

    pub fn into_verified(self) -> Result<VerifiedPackageReader> {
        let verified = self.verify_for_consumption()?;
        Ok(VerifiedPackageReader {
            reader: self,
            verified,
        })
    }

    pub fn with_verification(self, verified: VerifiedPackage) -> Result<VerifiedPackageReader> {
        self.require_verification(&verified)?;
        Ok(VerifiedPackageReader {
            reader: self,
            verified,
        })
    }

    fn require_verification(&self, verified: &VerifiedPackage) -> Result<()> {
        if !self.package_root.same_object(&verified._package_root)
            || verified.package_hash != self.manifest.package_hash
        {
            return Err(CdfError::data(
                "package verification authority does not bind this package identity",
            ));
        }
        Ok(())
    }

    /// Reads an identity-bearing artifact under package verification authority
    /// and revalidates its exact bytes at the point of consumption.
    pub fn verified_identity_bytes(
        &self,
        verified: &VerifiedPackage,
        relative_path: impl AsRef<Path>,
    ) -> Result<Vec<u8>> {
        self.require_verification(verified)?;
        let relative_path = normalize_artifact_path(relative_path.as_ref())?;
        let entry = self
            .manifest
            .identity
            .files
            .iter()
            .find(|entry| entry.path == relative_path)
            .ok_or_else(|| {
                CdfError::data(format!(
                    "verified package identity does not contain artifact {relative_path}"
                ))
            })?;
        let bytes = self.package_root.read(&relative_path)?;
        let byte_count = u64::try_from(bytes.len())
            .map_err(|_| CdfError::data("identity artifact byte count exceeds u64"))?;
        let sha256 = hex::encode(Sha256::digest(&bytes));
        if byte_count != entry.byte_count || sha256 != entry.sha256 {
            return Err(CdfError::data(format!(
                "identity artifact {relative_path} changed after package verification: expected {} bytes with sha256 {}, observed {byte_count} bytes with sha256 {sha256}",
                entry.byte_count, entry.sha256
            )));
        }
        Ok(bytes)
    }

    /// Retains a verified file capability for streaming consumers that must not buffer the whole
    /// identity artifact merely to cross the package boundary.
    pub fn verified_identity_object(
        &self,
        verified: Arc<VerifiedPackage>,
        relative_path: impl AsRef<Path>,
    ) -> Result<VerifiedIdentityObject> {
        self.require_verification(&verified)?;
        let relative_path = normalize_artifact_path(relative_path.as_ref())?;
        let entry = self
            .manifest
            .identity
            .files
            .iter()
            .find(|entry| entry.path == relative_path)
            .ok_or_else(|| {
                CdfError::data(format!(
                    "verified package identity does not contain artifact {relative_path}"
                ))
            })?;
        Ok(VerifiedIdentityObject {
            relative_path,
            byte_count: entry.byte_count,
            sha256: entry.sha256.clone(),
            package_root: Arc::clone(&self.package_root),
            _verification: verified,
        })
    }

    pub fn verified_json_artifact<T: DeserializeOwned>(
        &self,
        verified: &VerifiedPackage,
        relative_path: impl AsRef<Path>,
    ) -> Result<T> {
        let relative_path = relative_path.as_ref();
        serde_json::from_slice(&self.verified_identity_bytes(verified, relative_path)?).map_err(
            |error| {
                CdfError::data(format!(
                    "decode package artifact {}: {error}",
                    relative_path.display()
                ))
            },
        )
    }

    pub fn verified_optional_json_artifact<T: DeserializeOwned>(
        &self,
        verified: &VerifiedPackage,
        relative_path: impl AsRef<Path>,
    ) -> Result<Option<T>> {
        self.require_verification(verified)?;
        let relative_path = normalize_artifact_path(relative_path.as_ref())?;
        if !self
            .manifest
            .identity
            .files
            .iter()
            .any(|entry| entry.path == relative_path)
        {
            return Ok(None);
        }
        self.verified_json_artifact(verified, relative_path)
            .map(Some)
    }

    /// Reads and joins late-data evidence under the verified package identity.
    ///
    /// Deserialization validates each typed artifact independently; this join additionally proves
    /// that every referenced payload row and admitted package-row ordinal exists.
    pub fn late_data_evidence_verified(
        &self,
        verified: &VerifiedPackage,
    ) -> Result<Option<(LateDataEvidence, Option<LateDataPayloadCatalog>)>> {
        let Some(evidence) = self.verified_optional_json_artifact::<LateDataEvidence>(
            verified,
            LATE_DATA_EVIDENCE_FILE,
        )?
        else {
            return Ok(None);
        };
        let catalog = self.verified_optional_json_artifact::<LateDataPayloadCatalog>(
            verified,
            LATE_DATA_PAYLOAD_CATALOG_FILE,
        )?;
        let output_rows =
            self.manifest
                .identity
                .segments
                .iter()
                .try_fold(0_u64, |total, segment| {
                    total
                        .checked_add(segment.row_count)
                        .ok_or_else(|| CdfError::data("package segment row count overflow"))
                })?;
        evidence.validate_payloads(catalog.as_ref(), output_rows)?;
        Ok(Some((evidence, catalog)))
    }

    pub fn update_status(&mut self, status: PackageStatus) -> Result<&PackageManifest> {
        self.manifest = Arc::new(update_package_status(&self.package_dir, status)?);
        Ok(&self.manifest)
    }

    pub fn append_receipt(&self, receipt: Receipt) -> Result<Vec<Receipt>> {
        append_receipt(&self.package_dir, receipt)
    }

    pub fn receipts(&self) -> Result<Vec<Receipt>> {
        read_receipts(&self.package_dir)
    }

    pub fn replay_view(&self) -> Result<ReplayView> {
        if !self.manifest.lifecycle.status.is_replayable() {
            return Err(CdfError::data(format!(
                "package {} is not replayable at status {}",
                self.manifest.package_hash,
                self.manifest.lifecycle.status.as_str()
            )));
        }
        Ok(ReplayView {
            package_hash: PackageHash::new(self.manifest.package_hash.clone())?,
            status: self.manifest.lifecycle.status.clone(),
            segments: self.manifest.identity.segments.clone(),
            receipts: self.receipts()?,
        })
    }

    pub fn input_checkpoint(&self) -> Result<Option<Checkpoint>> {
        read_json_artifact(&self.package_dir, STATE_INPUT_CHECKPOINT_FILE)
    }

    pub fn state_delta_preimage(&self) -> Result<StateDeltaPreimage> {
        read_json_artifact(&self.package_dir, STATE_PROPOSED_DELTA_FILE)
    }

    pub fn destination_commit_plan_preimage(&self) -> Result<DestinationCommitPlanPreimage> {
        read_json_artifact(&self.package_dir, DESTINATION_COMMIT_PLAN_FILE)
    }

    pub fn processed_observation_evidence(
        &self,
    ) -> Result<Option<ProcessedObservationEvidenceArtifact>> {
        read_optional_json_artifact(&self.package_dir, PROCESSED_OBSERVATIONS_FILE)
    }

    pub fn runtime_arrow_schema(&self) -> Result<arrow_schema::SchemaRef> {
        let verified = self.verify_for_consumption()?;
        self.runtime_arrow_schema_verified(&verified)
    }

    pub fn runtime_arrow_schema_verified(
        &self,
        verified: &VerifiedPackage,
    ) -> Result<arrow_schema::SchemaRef> {
        let bytes = self.verified_identity_bytes(verified, crate::RUNTIME_ARROW_SCHEMA_FILE)?;
        crate::runtime_schema_from_bytes(bytes)
    }

    pub fn replay_inputs(&self) -> Result<PackageReplayInputs> {
        let verified = self.verify_for_consumption()?;
        self.replay_inputs_verified(&verified)
    }

    pub fn replay_inputs_verified(
        &self,
        verified: &VerifiedPackage,
    ) -> Result<PackageReplayInputs> {
        let replay = self.replay_view()?;
        PackageReplayInputs::from_preimages_with_processed(
            replay.package_hash,
            self.verified_json_artifact(verified, STATE_INPUT_CHECKPOINT_FILE)?,
            self.verified_json_artifact(verified, STATE_PROPOSED_DELTA_FILE)?,
            self.verified_json_artifact(verified, DESTINATION_COMMIT_PLAN_FILE)?,
            &replay.segments,
            self.verified_optional_json_artifact(verified, PROCESSED_OBSERVATIONS_FILE)?,
        )
    }

    pub fn read_segment(&self, segment_id: &SegmentId) -> Result<Vec<RecordBatch>> {
        let segment = self
            .manifest
            .identity
            .segments
            .iter()
            .find(|segment| &segment.segment_id == segment_id)
            .ok_or_else(|| {
                CdfError::data(format!(
                    "segment {} is not in manifest",
                    segment_id.as_str()
                ))
            })?;
        let batches = read_segment_file_from_root(&self.package_root, &segment.path)?;
        cdf_package_contract::validate_package_row_ord_batches(
            &batches,
            segment.package_row_ord_start,
            segment.row_count,
        )?;
        Ok(batches)
    }

    pub fn verified_segment_stream(
        &self,
        memory: Arc<dyn MemoryCoordinator>,
        maximum_segment_bytes: u64,
    ) -> Result<VerifiedSegmentStream<()>> {
        let verified = self.verify_for_consumption()?;
        self.verified_segment_stream_with(&verified, memory, maximum_segment_bytes)
    }

    pub fn verified_segment_stream_with(
        &self,
        verified: &VerifiedPackage,
        memory: Arc<dyn MemoryCoordinator>,
        maximum_segment_bytes: u64,
    ) -> Result<VerifiedSegmentStream<()>> {
        self.require_verification(verified)?;
        verified_manifest_segment_stream(
            Arc::clone(&self.package_root),
            Arc::clone(&self.manifest),
            memory,
            maximum_segment_bytes,
        )
    }

    pub fn verified_canonical_segment_stream(
        &self,
        memory: Arc<dyn MemoryCoordinator>,
        maximum_segment_bytes: u64,
    ) -> Result<VerifiedSegmentStream<()>> {
        let verified = self.verify_for_consumption()?;
        self.verified_canonical_segment_stream_with(&verified, memory, maximum_segment_bytes)
    }

    pub fn verified_canonical_segment_stream_with(
        &self,
        verified: &VerifiedPackage,
        memory: Arc<dyn MemoryCoordinator>,
        maximum_segment_bytes: u64,
    ) -> Result<VerifiedSegmentStream<()>> {
        self.require_verification(verified)?;
        verified_manifest_segment_stream(
            Arc::clone(&self.package_root),
            Arc::clone(&self.manifest),
            memory,
            maximum_segment_bytes,
        )
    }

    pub fn verified_canonical_segment_object_stream_with(
        &self,
        verified: &VerifiedPackage,
    ) -> Result<VerifiedSegmentObjectStream<()>> {
        self.require_verification(verified)?;
        Ok(VerifiedSegmentObjectStream {
            package_dir: self.package_dir.clone(),
            package_root: Arc::clone(&self.package_root),
            verified: Arc::new(verified.clone()),
            segments: VerifiedSegmentItems::manifest(Arc::clone(&self.manifest), || ()),
        })
    }

    pub fn verified_commit_segment_stream(
        &self,
        state_segments: &[StateSegment],
        memory: Arc<dyn MemoryCoordinator>,
        maximum_segment_bytes: u64,
    ) -> Result<VerifiedSegmentStream<StateSegment>> {
        let verified = self.verify_for_consumption()?;
        self.verified_commit_segment_stream_with(
            &verified,
            state_segments,
            memory,
            maximum_segment_bytes,
        )
    }

    pub fn verified_commit_segment_stream_with(
        &self,
        verified: &VerifiedPackage,
        state_segments: &[StateSegment],
        memory: Arc<dyn MemoryCoordinator>,
        maximum_segment_bytes: u64,
    ) -> Result<VerifiedSegmentStream<StateSegment>> {
        self.require_verification(verified)?;
        validate_commit_segment_authority(&self.manifest.identity.segments, state_segments)?;
        verified_manifest_authority_segment_stream(
            Arc::clone(&self.package_root),
            Arc::clone(&self.manifest),
            state_segments.to_vec(),
            memory,
            maximum_segment_bytes,
        )
    }

    pub fn for_each_quarantine_record(
        &self,
        visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
    ) -> Result<()> {
        for entry in &self.manifest.identity.files {
            if entry.path.starts_with("quarantine/") && entry.path.ends_with(".parquet") {
                for_each_quarantine_record_in_package_file(
                    &self.package_dir,
                    &entry.path,
                    visitor,
                )?;
            }
        }
        Ok(())
    }

    pub fn quarantine_record_count(&self) -> Result<u64> {
        self.manifest
            .identity
            .files
            .iter()
            .try_fold(0_u64, |count, entry| {
                if entry.path.starts_with("quarantine/") && entry.path.ends_with(".parquet") {
                    let file_count =
                        quarantine_record_count_in_package_file(&self.package_dir, &entry.path)?;
                    count
                        .checked_add(file_count)
                        .ok_or_else(|| CdfError::data("quarantine record count overflow"))
                } else {
                    Ok(count)
                }
            })
    }

    pub fn read_dedup_summary_json(&self) -> Result<Option<serde_json::Value>> {
        read_optional_json_artifact(&self.package_dir, DEDUP_SUMMARY_FILE)
    }

    pub fn for_each_dedup_dropped_provenance(
        &self,
        visitor: &mut dyn FnMut(u64, u64) -> Result<()>,
    ) -> Result<()> {
        let Some(summary) = self.read_dedup_summary_json()? else {
            return Ok(());
        };
        if required_json_u64(&summary, "version")? != u64::from(DEDUP_SUMMARY_VERSION) {
            return Err(CdfError::data(
                "dedup summary must use current external provenance version 3",
            ));
        }
        if required_json_string(&summary, "provenance_format")? != "parquet"
            || required_json_u64(&summary, "provenance_version")?
                != u64::from(DEDUP_PROVENANCE_VERSION)
            || required_json_string(&summary, "provenance_path")? != DEDUP_PROVENANCE_DIRECTORY
        {
            return Err(CdfError::data(
                "dedup summary declares unsupported provenance storage",
            ));
        }
        let expected_shard_count = required_json_u64(&summary, "shard_count")?;
        let expected_row_count = required_json_u64(&summary, "dropped_row_count")?;
        let mut shard_count = 0_u64;
        let mut row_count = 0_u64;
        let mut previous_dropped = None;
        for entry in &self.manifest.identity.files {
            if !entry.path.starts_with(DEDUP_PROVENANCE_DIRECTORY) {
                continue;
            }
            if !entry.path.ends_with(".parquet") {
                return Err(CdfError::data(format!(
                    "dedup provenance artifact is not Parquet: {}",
                    entry.path
                )));
            }
            shard_count = shard_count
                .checked_add(1)
                .ok_or_else(|| CdfError::data("dedup provenance shard count overflow"))?;
            let expected_path = dedup_provenance_shard_path(shard_count)?;
            if entry.path != expected_path {
                return Err(CdfError::data(format!(
                    "dedup provenance shard {} is not canonical path {expected_path}",
                    entry.path
                )));
            }
            let reader = ParquetRecordBatchReaderBuilder::try_new(
                self.package_root.open_std_file(&entry.path)?,
            )
            .map_err(|error| CdfError::data(format!("read dedup provenance metadata: {error}")))?
            .build()
            .map_err(|error| CdfError::data(format!("open dedup provenance rows: {error}")))?;
            for batch in reader {
                let batch = batch.map_err(|error| {
                    CdfError::data(format!("read dedup provenance rows: {error}"))
                })?;
                let dropped = dedup_u64_column(&batch, "package_row_ordinal")?;
                let kept = dedup_u64_column(&batch, "kept_package_row_ordinal")?;
                for row in 0..batch.num_rows() {
                    if dropped.is_null(row) || kept.is_null(row) {
                        return Err(CdfError::data("dedup provenance ordinals cannot be null"));
                    }
                    let dropped = dropped.value(row);
                    if previous_dropped.is_some_and(|previous| previous >= dropped) {
                        return Err(CdfError::data(
                            "dedup provenance shards are not in strict dropped-row order",
                        ));
                    }
                    previous_dropped = Some(dropped);
                    row_count = row_count
                        .checked_add(1)
                        .ok_or_else(|| CdfError::data("dedup provenance row count overflow"))?;
                    visitor(dropped, kept.value(row))?;
                }
            }
        }
        if shard_count != expected_shard_count || row_count != expected_row_count {
            return Err(CdfError::data(format!(
                "dedup provenance summary declares {expected_shard_count} shards and {expected_row_count} rows but package identity contains {shard_count} shards and {row_count} rows"
            )));
        }
        Ok(())
    }

    pub fn tombstone(&mut self) -> Result<TombstoneReport> {
        let report = tombstone_package(&self.package_dir)?;
        self.manifest = Arc::new(read_manifest_from_root(&self.package_root)?);
        Ok(report)
    }
}

fn required_json_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| CdfError::data(format!("dedup provenance row omits {field}")))
}

fn required_json_string<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| CdfError::data(format!("dedup summary omits string field {field:?}")))
}

fn dedup_u64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| CdfError::data(format!("dedup provenance omits uint64 column {name}")))
}
