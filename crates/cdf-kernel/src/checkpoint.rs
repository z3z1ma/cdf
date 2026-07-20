use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    WatermarkClaim,
    destination::{CommitCounts, MigrationRecord, SegmentAck, TransactionMetadata, VerifyClause},
    error::{CdfError, Result},
    ids::{
        CheckpointId, DestinationId, IdempotencyToken, PackageHash, PipelineId, ReceiptId,
        ResourceId, SchemaHash, SegmentId, TargetName,
    },
    position::SourcePosition,
    resource::WriteDisposition,
    scope::ScopeKey,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateDelta {
    pub checkpoint_id: CheckpointId,
    pub pipeline_id: PipelineId,
    pub resource_id: ResourceId,
    pub scope: ScopeKey,
    pub state_version: u16,
    pub parent_checkpoint_id: Option<CheckpointId>,
    pub input_position: Option<SourcePosition>,
    pub output_position: SourcePosition,
    /// Receipt-gated global event-time completeness emitted by this state transition.
    pub output_watermark: Option<WatermarkClaim>,
    /// Exact source-local restart authority when the resource output position is an aggregate.
    ///
    /// Multi-partition drains commonly expose a useful aggregate cursor as `output_position`
    /// while requiring partition-keyed positions to resume without loss. This field preserves
    /// that distinction in the checkpoint instead of relying on command-local executor state.
    pub source_continuation: Option<SourcePosition>,
    pub package_hash: PackageHash,
    pub schema_hash: SchemaHash,
    pub segments: Vec<StateSegment>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSegment {
    pub segment_id: SegmentId,
    pub scope: ScopeKey,
    pub output_position: SourcePosition,
    pub row_count: u64,
    pub byte_count: u64,
}

pub const CHECKPOINT_STATE_VERSION: u16 = 1;

impl StateDelta {
    /// Exact source restart authority, or the aggregate output when that position is sufficient.
    pub fn source_resume_position(&self) -> &SourcePosition {
        self.source_continuation
            .as_ref()
            .unwrap_or(&self.output_position)
    }

    /// Validates the complete typed position authority before persistence or replay.
    pub fn validate(&self) -> Result<()> {
        if self.state_version != CHECKPOINT_STATE_VERSION {
            return Err(CdfError::contract(format!(
                "unsupported checkpoint state version {}",
                self.state_version
            )));
        }
        if let Some(position) = &self.input_position {
            position.validate()?;
        }
        self.output_position.validate()?;
        if let Some(watermark) = &self.output_watermark {
            watermark.validate()?;
        }
        if let Some(position) = &self.source_continuation {
            position.validate()?;
        }
        for segment in &self.segments {
            segment.output_position.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub receipt_id: ReceiptId,
    pub destination: DestinationId,
    pub target: TargetName,
    pub package_hash: PackageHash,
    pub segment_acks: Vec<SegmentAck>,
    pub disposition: WriteDisposition,
    pub idempotency_token: IdempotencyToken,
    pub transaction: Option<TransactionMetadata>,
    pub counts: CommitCounts,
    pub schema_hash: SchemaHash,
    pub migrations: Vec<MigrationRecord>,
    pub committed_at_ms: i64,
    pub verify: VerifyClause,
}

impl Receipt {
    pub fn covers_state_delta(&self, delta: &StateDelta) -> bool {
        if self.package_hash != delta.package_hash || self.schema_hash != delta.schema_hash {
            return false;
        }
        let acked_segments: BTreeSet<&SegmentId> = self
            .segment_acks
            .iter()
            .map(|ack| &ack.segment_id)
            .collect();
        delta
            .segments
            .iter()
            .all(|segment| acked_segments.contains(&segment.segment_id))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStatus {
    Proposed,
    Committed,
    Abandoned,
    Rewound,
}

impl CheckpointStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Proposed => "proposed",
            Self::Committed => "committed",
            Self::Abandoned => "abandoned",
            Self::Rewound => "rewound",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "proposed" => Ok(Self::Proposed),
            "committed" => Ok(Self::Committed),
            "abandoned" => Ok(Self::Abandoned),
            "rewound" => Ok(Self::Rewound),
            other => Err(CdfError::data(format!(
                "unknown checkpoint status {other:?}"
            ))),
        }
    }
}

impl TryFrom<&str> for CheckpointStatus {
    type Error = CdfError;

    fn try_from(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub delta: StateDelta,
    pub status: CheckpointStatus,
    pub receipt: Option<Receipt>,
    pub is_head: bool,
    pub created_at_ms: i64,
    pub committed_at_ms: Option<i64>,
    pub rewind_target_checkpoint_id: Option<CheckpointId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewindRequest {
    pub marker_checkpoint_id: CheckpointId,
    pub pipeline_id: PipelineId,
    pub resource_id: ResourceId,
    pub scope: ScopeKey,
    pub target_checkpoint_id: CheckpointId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewindReport {
    pub marker: Checkpoint,
    pub head: Checkpoint,
    pub packages_ahead: Vec<PackageHash>,
}

pub trait CheckpointStore: Send + Sync {
    fn propose(&self, delta: StateDelta) -> Result<Checkpoint>;

    fn commit(&self, checkpoint_id: &CheckpointId, receipt: Receipt) -> Result<Checkpoint>;

    fn abandon(&self, checkpoint_id: &CheckpointId) -> Result<Checkpoint>;

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>>;

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>>;

    /// Returns the length of the newest contiguous committed suffix carrying `schema_hash`,
    /// saturated at `limit`. Stores should override this with a bounded projection so hot-path
    /// policy checks do not decode unbounded checkpoint history.
    fn committed_schema_streak(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
        schema_hash: &SchemaHash,
        limit: u32,
    ) -> Result<u32> {
        let history = self.history(pipeline_id, resource_id, scope)?;
        let mut count = 0_u32;
        for checkpoint in history
            .iter()
            .rev()
            .filter(|checkpoint| checkpoint.status == CheckpointStatus::Committed)
        {
            if checkpoint.delta.schema_hash != *schema_hash || count == limit {
                break;
            }
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    fn rewind(&self, request: RewindRequest) -> Result<RewindReport>;
}
