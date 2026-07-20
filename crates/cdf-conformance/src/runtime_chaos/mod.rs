use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChaosDestination(String);

impl ChaosDestination {
    pub fn new(value: impl Into<String>) -> cdf_kernel::Result<Self> {
        let destination = crate::run_matrix::MatrixDestination::new(value)?;
        Ok(Self(destination.as_str().to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChaosCrashWindow {
    PackageReplayVerifiedBeforeDestinationWrite,
    CheckpointProposedBeforeDestinationWrite,
    DestinationReceiptRecordedVerifiedBeforeCheckpointCommit,
    CheckpointCommittedBeforePackageStatusCheckpointed,
}

impl ChaosCrashWindow {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PackageReplayVerifiedBeforeDestinationWrite => {
                "package_replay_verified_before_destination_write"
            }
            Self::CheckpointProposedBeforeDestinationWrite => {
                "checkpoint_proposed_before_destination_write"
            }
            Self::DestinationReceiptRecordedVerifiedBeforeCheckpointCommit => {
                "destination_receipt_recorded_verified_before_checkpoint_commit"
            }
            Self::CheckpointCommittedBeforePackageStatusCheckpointed => {
                "checkpoint_committed_before_package_status_checkpointed"
            }
        }
    }

    #[cfg(test)]
    fn fixture_suffix(self) -> &'static str {
        match self {
            Self::PackageReplayVerifiedBeforeDestinationWrite => "pkg",
            Self::CheckpointProposedBeforeDestinationWrite => "prop",
            Self::DestinationReceiptRecordedVerifiedBeforeCheckpointCommit => "rcpt",
            Self::CheckpointCommittedBeforePackageStatusCheckpointed => "ckpt",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutedChaosCase {
    pub destination: ChaosDestination,
    pub crash_window: ChaosCrashWindow,
    pub package_id: String,
    pub crashed_checkpoint_id: String,
    pub recovery_checkpoint_id: String,
    pub recovery_receipt_id: String,
    pub crash_left_durable_receipt: bool,
    pub crash_left_checkpoint_head: bool,
    pub crash_left_destination_write: bool,
    pub recovery_path: String,
    pub recovery_without_source_contact: bool,
    pub checkpoint_not_ahead_of_durable_data: bool,
    pub receipt_recovery_avoided_second_destination_write: bool,
    pub duplicate_retry_no_second_destination_write: bool,
    pub duplicate_retry_behavior: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeChaosOutput {
    pub executed_cases: Vec<ExecutedChaosCase>,
}

pub fn cross_destination_chaos_cases() -> Vec<(ChaosDestination, ChaosCrashWindow)> {
    let mut cases = Vec::new();
    for destination in crate::destination_catalog::conformance_destinations()
        .into_iter()
        .map(|destination| ChaosDestination::new(destination.as_str()).unwrap())
    {
        for window in [
            ChaosCrashWindow::PackageReplayVerifiedBeforeDestinationWrite,
            ChaosCrashWindow::CheckpointProposedBeforeDestinationWrite,
            ChaosCrashWindow::DestinationReceiptRecordedVerifiedBeforeCheckpointCommit,
            ChaosCrashWindow::CheckpointCommittedBeforePackageStatusCheckpointed,
        ] {
            cases.push((destination.clone(), window));
        }
    }
    cases
}

#[cfg(test)]
mod destinations;
#[cfg(test)]
mod fixture;
#[cfg(test)]
mod helper;
#[cfg(test)]
mod tests;
