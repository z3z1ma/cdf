use cdf_kernel::{Checkpoint, CheckpointStatus, Receipt};
use serde::Serialize;

use crate::{
    context::ProjectContext,
    output::{CliError, CommandOutput},
    progress::ProgressSnapshot,
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine},
        redaction::redact_uri_userinfo,
    },
};

use super::model::ResumePackageFacts;

pub(super) fn finish_resume_report(
    report: ResumeReport,
    progress: Option<ProgressSnapshot>,
) -> Result<CommandOutput, CliError> {
    let exit_code = report.exit_code();
    let document = report.render_document();
    match progress {
        Some(progress) => CommandOutput::rendered_with_progress_and_exit_code(
            "resume", document, report, progress, exit_code,
        ),
        None => CommandOutput::rendered_with_exit_code("resume", document, report, exit_code),
    }
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

    fn render_document(&self) -> RenderDocument {
        let status = if self.recovery.result == "success" {
            StatusKind::Success
        } else {
            StatusKind::Error
        };
        RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                status,
                format!(
                    "resume run {} {}",
                    self.run_id,
                    if self.recovery.result == "success" {
                        "completed"
                    } else {
                        "failed closed"
                    }
                ),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Recovery")
                    .row("failed phase", self.state.clone())
                    .row("action", self.action.clone())
                    .row("result", self.recovery.result.clone())
                    .row("source contact", yes_no(self.source_contact))
                    .row("mutation required", yes_no(self.mutation_required))
                    .row("mutation performed", yes_no(self.mutated))
                    .row("guidance", self.recovery.guidance.clone())
                    .row("next command", self.next_command()),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Durable artifacts")
                    .row("package", optional_ref(self.package.package_id.as_deref()))
                    .row(
                        "package path",
                        optional_display(self.package.path.as_deref()),
                    )
                    .row(
                        "package hash",
                        optional_ref(self.package.package_hash.as_deref()),
                    )
                    .row(
                        "package status",
                        optional_ref(self.package.status.as_deref()),
                    )
                    .row("package receipts", self.package.receipt_count.to_string())
                    .row(
                        "checkpoint",
                        optional_ref(self.checkpoint.checkpoint_id.as_deref()),
                    )
                    .row(
                        "checkpoint status",
                        optional_ref(self.checkpoint.status.as_deref()),
                    )
                    .row("receipt", optional_ref(self.receipt.receipt_id.as_deref())),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("State")
                    .row("checkpoint committed", yes_no(self.checkpoint.committed))
                    .row("checkpoint is head", yes_no(self.checkpoint.is_head))
                    .row(
                        "receipt destination",
                        optional_ref(self.receipt.destination_id.as_deref()),
                    )
                    .row(
                        "receipt target",
                        optional_ref(self.receipt.target.as_deref()),
                    )
                    .row(
                        "receipt source",
                        optional_ref(self.receipt.source.as_deref()),
                    )
                    .row("destination kind", self.destination.kind.clone())
                    .row("destination", safe_display_value(&self.destination.uri)),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Run ledger")
                    .row("events before", self.ledger_event_count_before.to_string())
                    .row("events after", self.ledger_event_count_after.to_string()),
            )
            .blank_line()
            .push(NextCommand::new(self.next_command()))
    }

    fn next_command(&self) -> String {
        match self.action.as_str() {
            "rerun_extraction_from_last_committed_checkpoint" => "cdf run <resource>".to_owned(),
            "inspect_missing_artifacts" | "inspect_destination" => {
                format!("cdf inspect run {}", self.run_id)
            }
            _ => format!("cdf inspect run {}", self.run_id),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(super) struct ResumePackagePointer {
    pub(super) path: Option<String>,
    pub(super) package_id: Option<String>,
    pub(super) package_hash: Option<String>,
    pub(super) status: Option<String>,
    pub(super) receipt_count: u64,
}

impl ResumePackagePointer {
    pub(super) fn from_facts(facts: &ResumePackageFacts) -> Self {
        Self {
            path: Some(facts.path.display().to_string()),
            package_id: Some(facts.reader.manifest().identity.package_id.clone()),
            package_hash: Some(facts.reader.manifest().package_hash.clone()),
            status: Some(facts.status.as_str().to_owned()),
            receipt_count: facts.receipt_count,
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

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn optional_ref(value: Option<&str>) -> String {
    value.unwrap_or("none").to_owned()
}

fn optional_display(value: Option<&str>) -> String {
    value
        .map(safe_display_value)
        .unwrap_or_else(|| "none".to_owned())
}

fn safe_display_value(value: &str) -> String {
    redact_uri_userinfo(value)
}
