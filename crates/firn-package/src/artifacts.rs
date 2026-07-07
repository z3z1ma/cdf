use std::{collections::BTreeSet, fs, path::Path};

use firn_kernel::{
    CHECKPOINT_STATE_VERSION, Checkpoint, CheckpointId, CheckpointStatus, DestinationCommitRequest,
    FirnError, IdempotencyToken, PackageHash, PipelineId, ResourceId, Result, SchemaHash, ScopeKey,
    SourcePosition, StateDelta, StateSegment, TargetName, WriteDisposition,
};
use serde::{Deserialize, Serialize};

use crate::{
    json::json_error,
    model::SegmentEntry,
    storage::{io_error, package_path},
};

pub const STATE_INPUT_CHECKPOINT_FILE: &str = "state/input_checkpoint.json";
pub const STATE_PROPOSED_DELTA_FILE: &str = "state/proposed_delta.json";
pub const DESTINATION_COMMIT_PLAN_FILE: &str = "destination/commit_plan.json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateDeltaPreimage {
    pub checkpoint_id: CheckpointId,
    pub pipeline_id: PipelineId,
    pub resource_id: ResourceId,
    pub scope: ScopeKey,
    pub state_version: u16,
    pub parent_checkpoint_id: Option<CheckpointId>,
    pub input_position: Option<SourcePosition>,
    pub output_position: SourcePosition,
    pub schema_hash: SchemaHash,
    pub segments: Vec<StateSegment>,
}

impl StateDeltaPreimage {
    pub fn into_state_delta(self, package_hash: PackageHash) -> StateDelta {
        StateDelta {
            checkpoint_id: self.checkpoint_id,
            pipeline_id: self.pipeline_id,
            resource_id: self.resource_id,
            scope: self.scope,
            state_version: self.state_version,
            parent_checkpoint_id: self.parent_checkpoint_id,
            input_position: self.input_position,
            output_position: self.output_position,
            package_hash,
            schema_hash: self.schema_hash,
            segments: self.segments,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationCommitPlanPreimage {
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub merge_keys: Vec<String>,
    pub schema_hash: SchemaHash,
    pub segments: Vec<StateSegment>,
    pub idempotency_token_source: IdempotencyTokenSource,
}

impl DestinationCommitPlanPreimage {
    pub fn package_hash_token(
        target: TargetName,
        disposition: WriteDisposition,
        merge_keys: Vec<String>,
        schema_hash: SchemaHash,
        segments: Vec<StateSegment>,
    ) -> Self {
        Self {
            target,
            disposition,
            merge_keys,
            schema_hash,
            segments,
            idempotency_token_source: IdempotencyTokenSource::PackageHash,
        }
    }

    pub fn commit_request(&self, package_hash: PackageHash) -> Result<DestinationCommitRequest> {
        match self.idempotency_token_source {
            IdempotencyTokenSource::PackageHash => Ok(DestinationCommitRequest {
                package_hash: package_hash.clone(),
                target: self.target.clone(),
                disposition: self.disposition.clone(),
                segments: self.segments.clone(),
                idempotency_token: IdempotencyToken::new(package_hash.as_str())?,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyTokenSource {
    PackageHash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageReplayInputs {
    pub input_checkpoint: Option<Checkpoint>,
    pub state_delta: StateDelta,
    pub destination_commit: DestinationCommitRequest,
    pub merge_keys: Vec<String>,
    pub schema_hash: SchemaHash,
}

impl PackageReplayInputs {
    pub fn from_preimages(
        package_hash: PackageHash,
        input_checkpoint: Option<Checkpoint>,
        state_delta: StateDeltaPreimage,
        commit_plan: DestinationCommitPlanPreimage,
        package_segments: &[SegmentEntry],
    ) -> Result<Self> {
        validate_input_checkpoint(&input_checkpoint, &state_delta)?;
        if commit_plan.schema_hash != state_delta.schema_hash {
            return Err(FirnError::data(format!(
                "destination commit plan schema hash {} does not match state delta schema hash {}",
                commit_plan.schema_hash, state_delta.schema_hash
            )));
        }
        if commit_plan.segments != state_delta.segments {
            return Err(FirnError::data(
                "destination commit plan segments do not match state delta segments",
            ));
        }
        validate_package_segments(package_segments, &state_delta.segments)?;

        let schema_hash = state_delta.schema_hash.clone();
        let merge_keys = commit_plan.merge_keys.clone();
        let destination_commit = commit_plan.commit_request(package_hash.clone())?;
        let state_delta = state_delta.into_state_delta(package_hash);
        Ok(Self {
            input_checkpoint,
            state_delta,
            destination_commit,
            merge_keys,
            schema_hash,
        })
    }
}

pub(crate) fn read_json_artifact<T: for<'de> Deserialize<'de>>(
    package_dir: &Path,
    relative_path: &str,
) -> Result<T> {
    let path = package_path(package_dir, relative_path);
    let bytes =
        fs::read(&path).map_err(|error| io_error(format!("read {}", path.display()), error))?;
    serde_json::from_slice(&bytes).map_err(json_error)
}

fn validate_input_checkpoint(
    input_checkpoint: &Option<Checkpoint>,
    state_delta: &StateDeltaPreimage,
) -> Result<()> {
    if state_delta.state_version != CHECKPOINT_STATE_VERSION {
        return Err(FirnError::data(format!(
            "unsupported state delta preimage version {}",
            state_delta.state_version
        )));
    }
    match input_checkpoint {
        Some(checkpoint) => {
            if checkpoint.status != CheckpointStatus::Committed || !checkpoint.is_head {
                return Err(FirnError::data(
                    "state input checkpoint must be the committed head",
                ));
            }
            if checkpoint.delta.pipeline_id != state_delta.pipeline_id
                || checkpoint.delta.resource_id != state_delta.resource_id
                || checkpoint.delta.scope != state_delta.scope
            {
                return Err(FirnError::data(
                    "state input checkpoint tuple does not match state delta tuple",
                ));
            }
            if state_delta.parent_checkpoint_id.as_ref() != Some(&checkpoint.delta.checkpoint_id) {
                return Err(FirnError::data(
                    "state delta parent checkpoint does not match input checkpoint",
                ));
            }
            if state_delta.input_position.as_ref() != Some(&checkpoint.delta.output_position) {
                return Err(FirnError::data(
                    "state delta input position does not match input checkpoint output position",
                ));
            }
        }
        None => {
            if state_delta.parent_checkpoint_id.is_some() || state_delta.input_position.is_some() {
                return Err(FirnError::data(
                    "state delta cannot reference an input checkpoint when input checkpoint artifact is null",
                ));
            }
        }
    }
    Ok(())
}

fn validate_package_segments(
    package_segments: &[SegmentEntry],
    state_segments: &[StateSegment],
) -> Result<()> {
    if state_segments.is_empty() {
        return Err(FirnError::data(
            "state delta preimage must include at least one state segment",
        ));
    }
    if package_segments.len() != state_segments.len() {
        return Err(FirnError::data(format!(
            "package has {} segment(s) but state delta preimage has {} segment(s)",
            package_segments.len(),
            state_segments.len()
        )));
    }
    let mut seen_state_segments = BTreeSet::<&firn_kernel::SegmentId>::new();
    for state_segment in state_segments {
        if !seen_state_segments.insert(&state_segment.segment_id) {
            return Err(FirnError::data(format!(
                "state delta preimage contains duplicate segment {}",
                state_segment.segment_id
            )));
        }
        let Some(package_segment) = package_segments
            .iter()
            .find(|segment| segment.segment_id == state_segment.segment_id)
        else {
            return Err(FirnError::data(format!(
                "state delta segment {} is not present in the package manifest",
                state_segment.segment_id
            )));
        };
        if package_segment.row_count != state_segment.row_count
            || package_segment.byte_count != state_segment.byte_count
        {
            return Err(FirnError::data(format!(
                "state delta segment {} has {} rows/{} bytes but package manifest has {} rows/{} bytes",
                state_segment.segment_id,
                state_segment.row_count,
                state_segment.byte_count,
                package_segment.row_count,
                package_segment.byte_count
            )));
        }
    }
    Ok(())
}
