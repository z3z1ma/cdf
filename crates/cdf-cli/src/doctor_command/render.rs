use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
};

impl DoctorReport {
    pub(super) fn render_document(&self) -> RenderDocument {
        let table = self.checks.iter().fold(
            Table::new(["check", "status", "message"]),
            |table, check| {
                table.row([
                    check.name.clone(),
                    check.status.name().to_owned(),
                    redact_uri_userinfo(&check.message),
                ])
            },
        );

        RenderDocument::new()
            .push(StatusLine::new(
                if self.failed > 0 {
                    StatusKind::Error
                } else if self.unsupported > 0 {
                    StatusKind::Warning
                } else {
                    StatusKind::Success
                },
                if self.failed == 0 {
                    format!(
                        "doctor completed with {} unsupported check(s)",
                        self.unsupported
                    )
                } else {
                    format!("doctor found {} failed check(s)", self.failed)
                },
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Doctor")
                    .row("checks", self.checks.len().to_string())
                    .row("failed", self.failed.to_string())
                    .row("unsupported", self.unsupported.to_string())
                    .row("passed", self.passed_count().to_string())
                    .row("skipped", self.skipped_count().to_string()),
            )
            .blank_line()
            .push(table)
            .blank_line()
            .push(NextCommand::new("cdf status"))
    }
}
