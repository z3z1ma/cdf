use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use firn_kernel::{Checkpoint, FirnError, PackageHash, Receipt, Result, SegmentId};

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
            return Err(FirnError::data(format!(
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
                FirnError::data(format!(
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

    pub fn tombstone(&mut self) -> Result<TombstoneReport> {
        let report = tombstone_package(&self.package_dir)?;
        self.manifest = read_manifest(&self.package_dir)?;
        Ok(report)
    }
}
