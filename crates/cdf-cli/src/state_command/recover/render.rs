use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
    redaction::redact_uri_userinfo,
};

impl StateRecoverCliReport {
    pub(super) fn render_document(&self) -> RenderDocument {
        let limit_table = self
            .evidence_limits
            .iter()
            .fold(Table::new(["evidence limit"]), |table, limit| {
                table.row([(*limit).to_owned()])
            });

        RenderDocument::new()
            .push(StatusLine::new(
                StatusKind::Success,
                format!("recovered checkpoint {}", self.checkpoint_id),
            ))
            .blank_line()
            .push(
                KeyValuePanel::recovery()
                    .row("package", self.package_id.clone())
                    .row("package dir", safe_display_value(&self.package_dir))
                    .row("package hash", self.package_hash.clone())
                    .row("selected receipt", self.selected_receipt_id.clone())
                    .row(
                        "receipt selection",
                        receipt_selection_name(self.receipt_selection),
                    )
                    .row("receipt source", self.receipt_source)
                    .row(
                        "next command",
                        format!(
                            "cdf inspect package {}",
                            safe_display_value(&self.package_dir)
                        ),
                    ),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Checkpoint")
                    .row("checkpoint", self.checkpoint_id.clone())
                    .row("package status", self.package_status.clone())
                    .row("receipt", self.receipt_id.clone())
                    .row(
                        "mutation performed",
                        "checkpoint committed from durable receipt",
                    ),
            )
            .blank_line()
            .push(
                KeyValuePanel::effects()
                    .row("package status", yes_no(self.writes.package_status))
                    .row("destination rows", yes_no(self.writes.destination_rows))
                    .row("checkpoint", yes_no(self.writes.checkpoint))
                    .row(
                        "destination",
                        "verified receipt only; destination rows were not written",
                    ),
            )
            .blank_line()
            .push(limit_table)
            .blank_line()
            .push(NextCommand::new(format!(
                "cdf inspect package {}",
                safe_display_value(&self.package_dir)
            )))
    }
}

fn receipt_selection_name(selection: RecoveryReceiptSelection) -> &'static str {
    match selection {
        RecoveryReceiptSelection::Explicit => "explicit",
        RecoveryReceiptSelection::SingleDurableReceipt => "single_durable_receipt",
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn safe_display_value(value: &str) -> String {
    redact_uri_userinfo(value)
}
