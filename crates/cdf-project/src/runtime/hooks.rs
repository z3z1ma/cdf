use super::prelude::*;

pub type ReceiptVerifiedHook<'a> = &'a dyn Fn(&Receipt) -> Result<()>;
pub type RuntimeStageHook<'a> = &'a dyn Fn(RuntimeStage<'_>) -> Result<()>;

pub enum RuntimeStage<'a> {
    PackageReplayVerified,
    CheckpointProposed { delta: &'a StateDelta },
    DestinationWriteReady,
    DestinationCommitStarted { plan_id: &'a PlanId },
    DestinationReceiptRecorded { receipt: &'a Receipt },
    CheckpointCommitted { checkpoint: &'a Checkpoint },
    PackageStatusUpdated { status: &'a PackageStatus },
}
