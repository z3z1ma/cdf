use cdf_kernel::{Checkpoint, CheckpointStatus, Receipt};
use serde::Serialize;

use crate::{
    commands::report_output,
    context::ProjectContext,
    output::{CliError, CommandOutput},
};

use super::model::ResumePackageFacts;

pub(super) fn finish_resume_report(report: ResumeReport) -> Result<CommandOutput, CliError> {
    let human = report.human_message();
    let exit_code = report.exit_code();
    report_output("resume", human, report, exit_code)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct ResumeReport {
    pub(super) command: &'static str,
    pub(super) run_id: String,
    pub(super) state: String,
    pub(super) action: String,
    pub(super) source_contact: bool,
    pub(super) mutation_required: bool,
    pub(super) mutated: bool,
    pub(super) package: ResumePackagePointer,
    pub(super) checkpoint: ResumeCheckpointPointer,
    pub(super) receipt: ResumeReceiptPointer,
    pub(super) destination: ResumeDestinationPointer,
    pub(super) recovery: ResumeRecoveryReport,
    pub(super) ledger_event_count_before: usize,
    pub(super) ledger_event_count_after: usize,
}

impl ResumeReport {
    fn exit_code(&self) -> i32 {
        if self.recovery.result == "success" {
            0
        } else {
            1
        }
    }

    fn human_message(&self) -> String {
        format!(
            "resume run {} action {} result {} mutated={} guidance: {}",
            self.run_id, self.action, self.recovery.result, self.mutated, self.recovery.guidance
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(super) struct ResumePackagePointer {
    pub(super) path: Option<String>,
    pub(super) package_id: Option<String>,
    pub(super) package_hash: Option<String>,
    pub(super) status: Option<String>,
    pub(super) receipt_count: usize,
}

impl ResumePackagePointer {
    pub(super) fn from_facts(facts: &ResumePackageFacts) -> Self {
        Self {
            path: Some(facts.path.display().to_string()),
            package_id: Some(facts.reader.manifest().identity.package_id.clone()),
            package_hash: Some(facts.reader.manifest().package_hash.clone()),
            status: Some(facts.status.as_str().to_owned()),
            receipt_count: facts.receipts.len(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(super) struct ResumeCheckpointPointer {
    checkpoint_id: Option<String>,
    status: Option<String>,
    committed: bool,
    is_head: bool,
}

impl ResumeCheckpointPointer {
    pub(super) fn from_checkpoint(checkpoint: Option<&Checkpoint>) -> Self {
        match checkpoint {
            Some(checkpoint) => Self {
                checkpoint_id: Some(checkpoint.delta.checkpoint_id.to_string()),
                status: Some(checkpoint.status.as_str().to_owned()),
                committed: checkpoint.status == CheckpointStatus::Committed,
                is_head: checkpoint.is_head,
            },
            None => Self::default(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(super) struct ResumeReceiptPointer {
    receipt_id: Option<String>,
    destination_id: Option<String>,
    target: Option<String>,
    package_hash: Option<String>,
    pub(super) source: Option<String>,
}

impl ResumeReceiptPointer {
    pub(super) fn from_receipt(receipt: Option<&Receipt>) -> Self {
        match receipt {
            Some(receipt) => Self {
                receipt_id: Some(receipt.receipt_id.to_string()),
                destination_id: Some(receipt.destination.to_string()),
                target: Some(receipt.target.to_string()),
                package_hash: Some(receipt.package_hash.to_string()),
                source: None,
            },
            None => Self::default(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(super) struct ResumeDestinationPointer {
    kind: String,
    uri: String,
}

impl ResumeDestinationPointer {
    pub(super) fn from_context(context: &ProjectContext) -> Self {
        let uri = context.environment.destination.clone();
        let kind = uri
            .split_once("://")
            .map(|(scheme, _)| scheme.to_owned())
            .unwrap_or_else(|| "unsupported".to_owned());
        Self { kind, uri }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct ResumeRecoveryReport {
    pub(super) result: String,
    pub(super) guidance: String,
}
