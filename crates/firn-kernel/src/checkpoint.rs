use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    destination::{CommitCounts, MigrationRecord, SegmentAck, TransactionMetadata, VerifyClause},
    error::{FirnError, Result},
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
            other => Err(FirnError::data(format!(
                "unknown checkpoint status {other:?}"
            ))),
        }
    }
}

impl TryFrom<&str> for CheckpointStatus {
    type Error = FirnError;

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

    fn rewind(&self, request: RewindRequest) -> Result<RewindReport>;
}
