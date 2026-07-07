use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use arrow_array::RecordBatch;
use cdf_kernel::{
    CdfError, Checkpoint, CommitSegment, PackageHash, Receipt, Result, SegmentId, StateSegment,
};

use crate::{
    artifacts::{
        DESTINATION_COMMIT_PLAN_FILE, DestinationCommitPlanPreimage, PackageReplayInputs,
        STATE_INPUT_CHECKPOINT_FILE, STATE_PROPOSED_DELTA_FILE, StateDeltaPreimage,
        read_json_artifact,
    },
    model::{
        PackageManifest, PackageStatus, ReplayView, SegmentEntry, TombstoneReport,
        VerificationReport,
    },
    ops::{
        append_receipt, read_manifest, read_receipts, read_segment_file, tombstone_package,
        update_package_status, verify_package,
    },
};

#[derive(Clone, Debug)]
pub struct PackageReader {
    package_dir: PathBuf,
    manifest: PackageManifest,
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

    pub fn replay_inputs(&self) -> Result<PackageReplayInputs> {
        self.verify()?;
        let replay = self.replay_view()?;
        PackageReplayInputs::from_preimages(
            replay.package_hash,
            self.input_checkpoint()?,
            self.state_delta_preimage()?,
            self.destination_commit_plan_preimage()?,
            &replay.segments,
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
            commit_segments.push(CommitSegment {
                state: state.clone(),
                package_byte_count: manifest_segment.byte_count,
                batches,
            });
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
