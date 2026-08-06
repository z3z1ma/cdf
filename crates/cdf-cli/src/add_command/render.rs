use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine},
};

pub(super) fn document(report: &AddReport) -> RenderDocument {
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            if report.writes.resource_sql {
                format!("added query resource {}", report.resource_id)
            } else {
                format!("prepared query resource {} (dry run)", report.resource_id)
            },
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Resource")
                .row("id", report.resource_id.clone())
                .row("namespace", report.namespace.clone())
                .row("resource", report.resource.clone())
                .row("configured source", report.configured_source.clone())
                .row("driver", report.source_driver.clone())
                .row("SQL", report.resource_path.clone())
                .row("location", report.location.clone())
                .row("selection", report.selection.clone())
                .row("policy", report.policy.to_owned()),
        )
        .blank_line()
        .push(NextCommand::new(report.next_command.clone()))
}
