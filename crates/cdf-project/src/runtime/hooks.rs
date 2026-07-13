use super::prelude::*;

pub type ReceiptVerifiedHook<'a> = &'a dyn Fn(&Receipt) -> Result<()>;
pub type RuntimeStageHook<'a> = &'a dyn Fn(RuntimeStage<'_>) -> Result<()>;

pub enum RuntimeStage<'a> {
    PackageReplayVerified,
    CheckpointProposed {
        delta: &'a StateDelta,
    },
    DestinationWriteReady,
    DestinationCommitStarted {
        plan_id: &'a PlanId,
        segment_count: usize,
        bulk_path: &'a cdf_runtime::PreparedBulkPath,
    },
    DestinationSegmentAcknowledged {
        ack: &'a SegmentAck,
    },
    DestinationReceiptRecorded {
        receipt: &'a Receipt,
    },
    CheckpointCommitted {
        checkpoint: &'a Checkpoint,
    },
    PackageStatusUpdated {
        status: &'a PackageStatus,
    },
}
