use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine},
};

use super::{CompileReport, CompileResourceStatus};

pub(super) fn document(report: &CompileReport) -> RenderDocument {
    let status = if report.counts.failed == 0 {
        StatusKind::Success
    } else {
        StatusKind::Error
    };
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            status,
            format!(
                "compiled {}/{} selected resource(s)",
                report.counts.compiled, report.counts.selected
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Project compilation")
                .row("project", &report.project)
                .row("environment", &report.environment)
                .row("locked", report.locked.to_string())
                .row("index", &report.index_path),
        );
    for resource in &report.resources {
        let (kind, label) = match resource.status {
            CompileResourceStatus::Compiled => (StatusKind::Success, "compiled"),
            CompileResourceStatus::Failed => (StatusKind::Error, "failed"),
        };
        let mut message = format!("{label} {}", resource.resource_id);
        if let Some(error) = &resource.error {
            message.push_str(&format!(": {}", error.message));
        }
        document = document.push(StatusLine::new(kind, message));
    }
    document
        .blank_line()
        .push(NextCommand::new(&report.next_command))
}
