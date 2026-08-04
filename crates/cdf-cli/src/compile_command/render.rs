use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine},
};

use super::CompileReport;

pub(super) fn document(report: &CompileReport) -> RenderDocument {
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "compiled {} resource(s) into {}",
                report.resources, report.manifest_path
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Project compilation")
                .row("project", &report.project)
                .row("environment", &report.environment)
                .row(
                    "mode",
                    match report.mode {
                        cdf_project::ProjectCompilationMode::LockedOffline => "locked_offline",
                        cdf_project::ProjectCompilationMode::Refresh => "refresh",
                    },
                )
                .row("manifest hash", &report.manifest_hash)
                .row(
                    "semantic definitions",
                    report.semantic_definitions.to_string(),
                )
                .row(
                    "semantic references",
                    report.semantic_references.to_string(),
                )
                .row(
                    "source observations",
                    report.source_observations.to_string(),
                ),
        )
        .blank_line()
        .push(NextCommand::new(&report.next_command))
}
