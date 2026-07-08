use super::prelude::*;

pub type ReceiptVerifiedHook<'a> = &'a dyn Fn(&Receipt) -> Result<()>;
pub type LocalDuckDbLifecycleFailpointHook<'a> =
    &'a dyn Fn(LocalDuckDbLifecycleFailpoint, Option<&Receipt>) -> Result<()>;
pub type RuntimeStageHook<'a> = &'a dyn Fn(RuntimeStage<'_>) -> Result<()>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalDuckDbLifecycleFailpoint {
    AfterPackagedBeforeDestinationWrite,
    AfterCheckpointProposalBeforeDestinationWrite,
    AfterReceiptVerifiedBeforeCheckpointCommit,
    AfterCheckpointCommitBeforePackageStatusCheckpointed,
}

pub enum RuntimeStage<'a> {
    CheckpointProposed { delta: &'a StateDelta },
    DestinationCommitStarted { plan_id: &'a PlanId },
    DestinationReceiptRecorded { receipt: &'a Receipt },
    CheckpointCommitted { checkpoint: &'a Checkpoint },
    PackageStatusUpdated { status: &'a PackageStatus },
}
