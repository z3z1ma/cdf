use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
    redaction::redact_uri_userinfo,
};

impl InspectRunReport {
    pub(super) fn render_document(&self) -> RenderDocument {
        let missing_package_count = self
            .artifacts
            .packages
            .iter()
            .filter(|package| package.status != "available")
            .count();
        let missing_receipts = self.artifacts.receipt.missing_receipt_ids.len();
        let mut document = RenderDocument::new()
            .push(StatusLine::new(
                StatusKind::Success,
                format!("run {} terminal {}", self.run_id, self.terminal_state),
            ))
            .blank_line()
            .push(
                KeyValuePanel::recovery()
                    .row("state", self.recovery.state.clone())
                    .row("action", self.recovery.action.clone())
                    .row("source contact", yes_no(self.recovery.source_contact))
                    .row("mutation required", yes_no(self.recovery.mutation_required))
                    .row("guidance", self.recovery.guidance.clone())
                    .row("next command", self.recovery_next_command()),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Artifacts")
                    .row("package status", self.artifacts.package_status.clone())
                    .row("packages", self.artifacts.packages.len().to_string())
                    .row("missing packages", missing_package_count.to_string())
                    .row("receipt status", self.artifacts.receipt.status.clone())
                    .row("missing receipts", missing_receipts.to_string())
                    .row(
                        "checkpoint status",
                        self.artifacts.checkpoint.status.clone(),
                    )
                    .row(
                        "checkpoint committed",
                        yes_no(self.artifacts.checkpoint.committed),
                    ),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Pointers")
                    .row("resources", list_or_none(&self.pointers.resource_ids))
                    .row("packages", list_or_none(&self.pointers.package_ids))
                    .row("checkpoints", list_or_none(&self.pointers.checkpoint_ids))
                    .row("receipts", list_or_none(&self.pointers.receipt_ids))
                    .row("destinations", list_or_none(&self.pointers.destination_ids)),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Duplicate")
                    .row("status", self.duplicate.status.clone())
                    .row("duplicate", optional_bool(self.duplicate.duplicate))
                    .row("no-op", optional_bool(self.duplicate.no_op))
                    .row(
                        "source event",
                        self.duplicate
                            .source_event_sequence
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "none".to_owned()),
                    ),
            );

        if self.artifacts.packages.is_empty() {
            document = document.blank_line().push(
                KeyValuePanel::new("Package artifacts")
                    .row("status", "not recorded")
                    .row("missing", "no package path is recorded in the run ledger"),
            );
        } else {
            let first_path = self
                .artifacts
                .packages
                .first()
                .map(|package| safe_display_value(&package.path))
                .unwrap_or_else(|| "none".to_owned());
            let first_issue = self
                .artifacts
                .packages
                .iter()
                .find_map(|package| package.reason.clone())
                .unwrap_or_else(|| "none".to_owned());
            document = document.blank_line().push(
                KeyValuePanel::new("Package artifacts")
                    .row("recorded", self.artifacts.packages.len().to_string())
                    .row("missing", missing_package_count.to_string())
                    .row("first path", first_path)
                    .row("first issue", first_issue),
            );
            let table = self.artifacts.packages.iter().fold(
                Table::new(["path", "status", "receipt artifact", "reason"]),
                |table, package| {
                    table.row([
                        safe_display_value(&package.path),
                        package.status.clone(),
                        package.receipt_artifact_status.clone(),
                        package
                            .reason
                            .clone()
                            .unwrap_or_else(|| "available".to_owned()),
                    ])
                },
            );
            document = document.blank_line().push(table);
        }

        let table = self.events.iter().fold(
            Table::new(["seq", "kind", "package", "checkpoint", "receipt"]),
            |table, event| {
                table.row([
                    event.sequence.to_string(),
                    event.kind.clone(),
                    event.package_id.clone().unwrap_or_else(|| "-".to_owned()),
                    event
                        .checkpoint_id
                        .clone()
                        .unwrap_or_else(|| "-".to_owned()),
                    event.receipt_id.clone().unwrap_or_else(|| "-".to_owned()),
                ])
            },
        );

        document
            .blank_line()
            .push(table)
            .blank_line()
            .push(NextCommand::new(self.recovery_next_command()))
    }

    fn recovery_next_command(&self) -> String {
        match self.recovery.action.as_str() {
            "rerun_extraction_from_last_committed_checkpoint" => self
                .pointers
                .resource_ids
                .first()
                .map(|resource| format!("cdf run {resource}"))
                .unwrap_or_else(|| "cdf run <resource>".to_owned()),
            "replay_package_without_source_contact"
            | "verify_receipt_then_commit_checkpoint"
            | "update_package_status" => format!("cdf run --resume {}", self.run_id),
            _ => format!("cdf inspect run {}", self.run_id),
        }
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn optional_bool(value: Option<bool>) -> String {
    value.map(yes_no).unwrap_or("unknown").to_owned()
}

fn list_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_owned()
    } else {
        values
            .iter()
            .map(|value| safe_display_value(value))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn safe_display_value(value: &str) -> String {
    redact_uri_userinfo(value)
}
