use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use arrow_array::{Array, RecordBatch, UInt64Array};
use cdf_kernel::{
    CdfError, Checkpoint, CommitSegment, CommitSegmentRetention, PackageHash, Receipt, Result,
    SegmentId, StateSegment,
};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest,
    record_batch_retained_bytes, reserve_blocking,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{
    artifacts::{
        DEDUP_SUMMARY_FILE, DESTINATION_COMMIT_PLAN_FILE, DestinationCommitPlanPreimage,
        PROCESSED_OBSERVATIONS_FILE, PackageReplayInputs, ProcessedObservationEvidenceArtifact,
        STATE_INPUT_CHECKPOINT_FILE, STATE_PROPOSED_DELTA_FILE, StateDeltaPreimage,
        read_json_artifact, read_optional_json_artifact,
    },
    model::{
        PackageManifest, PackageStatus, ReplayView, SegmentEntry, TombstoneReport,
        VerificationReport,
    },
    ops::{
        append_receipt, read_manifest, read_receipts, read_segment_file, tombstone_package,
        update_package_status, verify_package,
    },
    quarantine::{QuarantineRecord, quarantine_records_from_package_file},
};

#[derive(Clone, Debug)]
pub struct PackageReader {
    package_dir: PathBuf,
    manifest: PackageManifest,
}

/// Authority that one package identity was fully verified for a bounded
/// consumption operation. Its fields are private so callers cannot substitute
/// a hash-only assertion for package verification.
#[derive(Clone, Debug)]
pub struct VerifiedPackage {
    package_dir: PathBuf,
    package_hash: String,
}

impl PartialEq for VerifiedPackage {
    fn eq(&self, other: &Self) -> bool {
        self.package_hash == other.package_hash
    }
}

impl Eq for VerifiedPackage {}

#[derive(Clone, Debug)]
pub struct VerifiedPackageReader {
    reader: PackageReader,
    verified: VerifiedPackage,
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
    pub(crate) fn from_finalization(package_dir: &Path, manifest: &PackageManifest) -> Self {
        Self {
            package_dir: package_dir.to_path_buf(),
            package_hash: manifest.package_hash.clone(),
        }
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

#[derive(Debug)]
struct VerifiedSegmentWindow {
    memory_lease: MemoryLease,
    in_flight: Arc<AtomicBool>,
}

impl Drop for VerifiedSegmentWindow {
    fn drop(&mut self) {
        self.in_flight.store(false, Ordering::Release);
    }
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
        let retention = CommitSegmentRetention::new(self.window, retained_bytes)?;
        Ok(
            CommitSegment::new(self.authority.into(), self.entry.byte_count, self.batches)
                .with_retention(retention),
        )
    }
}

pub struct VerifiedSegmentStream<T> {
    package_dir: PathBuf,
    segments: std::vec::IntoIter<(SegmentEntry, T)>,
    memory: Arc<dyn MemoryCoordinator>,
    maximum_segment_bytes: u64,
    window_in_flight: Arc<AtomicBool>,
    failed: bool,
}

fn verified_segment_stream<T>(
    package_dir: &Path,
    segments: Vec<(SegmentEntry, T)>,
    memory: Arc<dyn MemoryCoordinator>,
    maximum_segment_bytes: u64,
) -> Result<VerifiedSegmentStream<T>> {
    if maximum_segment_bytes == 0 {
        return Err(CdfError::contract(
            "verified segment stream window must be nonzero",
        ));
    }
    if maximum_segment_bytes > memory.snapshot().budget_bytes {
        return Err(CdfError::data(format!(
            "verified segment stream window {maximum_segment_bytes} exceeds managed budget {}",
            memory.snapshot().budget_bytes
        )));
    }
    Ok(VerifiedSegmentStream {
        package_dir: package_dir.to_path_buf(),
        segments: segments.into_iter(),
        memory,
        maximum_segment_bytes,
        window_in_flight: Arc::new(AtomicBool::new(false)),
        failed: false,
    })
}

impl<T> Iterator for VerifiedSegmentStream<T> {
    type Item = Result<VerifiedSegment<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let (entry, authority) = self.segments.next()?;
        if self
            .window_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.failed = true;
            return Some(Err(CdfError::contract(
                "verified segment stream requires the previous accounted segment to be dropped before advancing",
            )));
        }
        let result = (|| {
            let request = ReservationRequest::new(
                ConsumerKey::new("verified-segment-stream", MemoryClass::Package)?,
                self.maximum_segment_bytes,
            )?
            .as_minimum_working_set();
            let lease = reserve_blocking(Arc::clone(&self.memory), &request)?;
            let batches = read_segment_file(&self.package_dir, &entry.path)?;
            let retained_bytes = batches.iter().try_fold(0u64, |total, batch| {
                total
                    .checked_add(record_batch_retained_bytes(batch)?)
                    .ok_or_else(|| CdfError::data("verified segment retained memory overflow"))
            })?;
            if retained_bytes > self.maximum_segment_bytes {
                return Err(CdfError::data(format!(
                    "segment {} retains {retained_bytes} Arrow bytes above its {}-byte verified stream window; raise the stream window or rebuild with a smaller canonical segment maximum",
                    entry.segment_id, self.maximum_segment_bytes
                )));
            }
            let row_count =
                batches.iter().try_fold(0u64, |total, batch| {
                    total
                        .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                            CdfError::data("verified segment row count exceeds u64")
                        })?)
                        .ok_or_else(|| CdfError::data("verified segment row count overflow"))
                })?;
            if row_count != entry.row_count {
                return Err(CdfError::data(format!(
                    "segment {} manifest row count {} differs from package data {row_count}",
                    entry.segment_id, entry.row_count
                )));
            }
            lease.reconcile(retained_bytes.max(1))?;
            let window = Arc::new(VerifiedSegmentWindow {
                memory_lease: lease,
                in_flight: Arc::clone(&self.window_in_flight),
            });
            Ok(VerifiedSegment {
                entry,
                authority,
                batches,
                window,
            })
        })();
        if result.is_err() {
            self.window_in_flight.store(false, Ordering::Release);
            self.failed = true;
        }
        Some(result)
    }
}

impl PackageReader {
    pub fn open(package_dir: impl AsRef<Path>) -> Result<Self> {
        let package_dir = package_dir.as_ref().to_path_buf();
        let manifest = read_manifest(&package_dir)?;
        Ok(Self {
            package_dir,
            manifest,
        })
    }

    pub fn manifest(&self) -> &PackageManifest {
        &self.manifest
    }

    pub fn verify(&self) -> Result<VerificationReport> {
        verify_package(&self.package_dir)
    }

    pub fn verify_for_consumption(&self) -> Result<VerifiedPackage> {
        let report = self.verify()?;
        Ok(VerifiedPackage {
            package_dir: self.package_dir.clone(),
            package_hash: report.package_hash,
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
        if verified.package_dir != self.package_dir
            || verified.package_hash != self.manifest.package_hash
        {
            return Err(CdfError::data(
                "package verification authority does not bind this package identity",
            ));
        }
        Ok(())
    }

    pub fn update_status(&mut self, status: PackageStatus) -> Result<&PackageManifest> {
        self.manifest = update_package_status(&self.package_dir, status)?;
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
        self.require_verification(verified)?;
        let path = self.package_dir.join(crate::RUNTIME_ARROW_SCHEMA_FILE);
        let bytes = std::fs::read(&path)
            .map_err(|error| CdfError::data(format!("read {}: {error}", path.display())))?;
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
        self.require_verification(verified)?;
        let replay = self.replay_view()?;
        PackageReplayInputs::from_preimages_with_processed(
            replay.package_hash,
            self.input_checkpoint()?,
            self.state_delta_preimage()?,
            self.destination_commit_plan_preimage()?,
            &replay.segments,
            self.processed_observation_evidence()?,
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
        read_segment_file(&self.package_dir, &segment.path)
    }

    pub fn read_all_segments(&self) -> Result<Vec<(SegmentEntry, Vec<RecordBatch>)>> {
        self.manifest
            .identity
            .segments
            .iter()
            .map(|segment| {
                Ok((
                    segment.clone(),
                    read_segment_file(&self.package_dir, &segment.path)?,
                ))
            })
            .collect()
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
        verified_segment_stream(
            &self.package_dir,
            self.manifest
                .identity
                .segments
                .iter()
                .cloned()
                .map(|entry| (entry, ()))
                .collect(),
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
        verified_segment_stream(
            &self.package_dir,
            self.manifest
                .identity
                .segments
                .iter()
                .cloned()
                .map(|entry| (entry, ()))
                .collect(),
            memory,
            maximum_segment_bytes,
        )
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
        let mut manifest_by_id = self
            .manifest
            .identity
            .segments
            .iter()
            .map(|entry| (entry.segment_id.clone(), entry.clone()))
            .collect::<BTreeMap<_, _>>();
        if manifest_by_id.len() != self.manifest.identity.segments.len() {
            return Err(CdfError::data(
                "package manifest contains duplicate segment ids",
            ));
        }
        let mut ordered = Vec::with_capacity(state_segments.len());
        for state in state_segments {
            let entry = manifest_by_id.remove(&state.segment_id).ok_or_else(|| {
                CdfError::data(format!(
                    "destination commit request segment {} is not present in the package manifest",
                    state.segment_id
                ))
            })?;
            if state.row_count != entry.row_count {
                return Err(CdfError::data(format!(
                    "destination commit request segment {} has {} rows but package manifest has {} rows",
                    state.segment_id, state.row_count, entry.row_count
                )));
            }
            ordered.push((entry, state.clone()));
        }
        if let Some(segment_id) = manifest_by_id.keys().next() {
            return Err(CdfError::data(format!(
                "package manifest segment {segment_id} is missing from destination commit request"
            )));
        }
        verified_segment_stream(&self.package_dir, ordered, memory, maximum_segment_bytes)
    }

    pub fn read_quarantine_records(&self) -> Result<Vec<QuarantineRecord>> {
        let mut records = Vec::new();
        for entry in &self.manifest.identity.files {
            if entry.path.starts_with("quarantine/") && entry.path.ends_with(".parquet") {
                records.extend(quarantine_records_from_package_file(
                    &self.package_dir,
                    &entry.path,
                )?);
            }
        }
        Ok(records)
    }

    pub fn read_dedup_summary_json(&self) -> Result<Option<serde_json::Value>> {
        read_optional_json_artifact(&self.package_dir, DEDUP_SUMMARY_FILE)
    }

    pub fn read_dedup_dropped_provenance(&self) -> Result<Vec<(u64, u64)>> {
        let Some(summary) = self.read_dedup_summary_json()? else {
            return Ok(Vec::new());
        };
        if summary.get("version").and_then(serde_json::Value::as_u64) != Some(2) {
            return summary
                .get("dropped_rows")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .map(|row| {
                    Ok((
                        required_json_u64(row, "package_row_ordinal")?,
                        required_json_u64(row, "kept_package_row_ordinal")?,
                    ))
                })
                .collect();
        }
        let shards = summary
            .get("shards")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| CdfError::data("dedup v2 summary omits shards"))?;
        let mut output = Vec::new();
        for shard in shards {
            let relative = shard
                .get("path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| CdfError::data("dedup v2 shard omits path"))?;
            if !relative.starts_with("stats/dedup-dropped/") || !relative.ends_with(".parquet") {
                return Err(CdfError::data(format!(
                    "dedup provenance path is outside its artifact directory: {relative}"
                )));
            }
            if !self
                .manifest
                .identity
                .files
                .iter()
                .any(|entry| entry.path == relative)
            {
                return Err(CdfError::data(format!(
                    "dedup provenance shard is absent from package identity: {relative}"
                )));
            }
            let path = self.package_dir.join(relative);
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&path).map_err(|error| {
                    CdfError::data(format!("open {}: {error}", path.display()))
                })?)
                .map_err(|error| {
                    CdfError::data(format!("read dedup provenance metadata: {error}"))
                })?
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
                    output.push((dropped.value(row), kept.value(row)));
                }
            }
        }
        if output.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(CdfError::data(
                "dedup provenance shards are not in strict dropped-row order",
            ));
        }
        Ok(output)
    }

    pub fn read_commit_segments(
        &self,
        state_segments: &[StateSegment],
    ) -> Result<Vec<CommitSegment>> {
        let mut manifest_by_id = BTreeMap::new();
        for segment in &self.manifest.identity.segments {
            if manifest_by_id
                .insert(segment.segment_id.clone(), segment)
                .is_some()
            {
                return Err(CdfError::data(format!(
                    "package manifest contains duplicate segment {}",
                    segment.segment_id
                )));
            }
        }

        let mut requested_ids = BTreeSet::new();
        let mut commit_segments = Vec::with_capacity(state_segments.len());
        for state in state_segments {
            if !requested_ids.insert(state.segment_id.clone()) {
                return Err(CdfError::data(format!(
                    "destination commit request contains duplicate segment {}",
                    state.segment_id
                )));
            }
            let manifest_segment = manifest_by_id.get(&state.segment_id).ok_or_else(|| {
                CdfError::data(format!(
                    "destination commit request segment {} is not present in the package manifest",
                    state.segment_id
                ))
            })?;
            let batches = read_segment_file(&self.package_dir, &manifest_segment.path)?;
            let batch_rows = batches
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum::<u64>();
            if batch_rows != manifest_segment.row_count {
                return Err(CdfError::data(format!(
                    "segment {} manifest row count {} differs from package data {}",
                    state.segment_id, manifest_segment.row_count, batch_rows
                )));
            }
            if state.row_count != manifest_segment.row_count {
                return Err(CdfError::data(format!(
                    "destination commit request segment {} has {} rows but package manifest has {} rows",
                    state.segment_id, state.row_count, manifest_segment.row_count
                )));
            }
            commit_segments.push(CommitSegment::new(
                state.clone(),
                manifest_segment.byte_count,
                batches,
            ));
        }

        for segment_id in manifest_by_id.keys() {
            if !requested_ids.contains(segment_id) {
                return Err(CdfError::data(format!(
                    "package manifest segment {} is missing from destination commit request",
                    segment_id
                )));
            }
        }

        Ok(commit_segments)
    }

    pub fn tombstone(&mut self) -> Result<TombstoneReport> {
        let report = tombstone_package(&self.package_dir)?;
        self.manifest = read_manifest(&self.package_dir)?;
        Ok(report)
    }
}

fn required_json_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| CdfError::data(format!("dedup provenance row omits {field}")))
}

fn dedup_u64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| CdfError::data(format!("dedup provenance omits uint64 column {name}")))
}
