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
                if self.counts.failed > 0 {
                    StatusKind::Error
                } else if self.counts.warned > 0 {
                    StatusKind::Warning
                } else {
                    StatusKind::Success
                },
                if self.counts.failed == 0 {
                    format!("doctor {} completed", self.scope)
                } else {
                    format!(
                        "doctor {} found {} failed check(s)",
                        self.scope, self.counts.failed
                    )
                },
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Doctor")
                    .row("scope", self.scope.clone())
                    .row("effect ceiling", self.effect_ceiling)
                    .row("attempted", self.counts.attempted.to_string())
                    .row("passed", self.counts.passed.to_string())
                    .row("warned", self.counts.warned.to_string())
                    .row("failed", self.counts.failed.to_string())
                    .row("skipped", self.counts.skipped.to_string())
                    .row(
                        "external authorities",
                        if self.external_authorities_contacted.is_empty() {
                            "none".to_owned()
                        } else {
                            self.external_authorities_contacted.join(", ")
                        },
                    ),
            )
            .blank_line()
            .push(table)
            .blank_line()
            .push(NextCommand::new("cdf status"))
    }
}
