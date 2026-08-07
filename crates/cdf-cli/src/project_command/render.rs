use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
};

pub(super) fn init_document(report: &ProjectScaffoldReport) -> RenderDocument {
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!("initialized project {}", report.project_name),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Project")
                .row("name", report.project_name.clone())
                .row("root", report.root.clone())
                .row("force", yes_no(report.force))
                .row("created", path_list(&report.created))
                .row("replaced", path_list(&report.replaced))
                .row("skipped", path_list(&report.skipped)),
        )
        .blank_line()
        .push(NextCommand::new("cdf validate"))
}

pub(super) fn validate_document(
    report: &cdf_project::ProjectStaticValidationReport,
) -> RenderDocument {
    let counts = &report.counts;
    let status = if counts.errors != 0 {
        StatusKind::Error
    } else if counts.warnings != 0 {
        StatusKind::Warning
    } else {
        StatusKind::Success
    };
    let selected = if report.selection.positive.is_empty() {
        "all authored resources".to_owned()
    } else {
        report.selection.positive.join(", ")
    };
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            status,
            format!(
                "validated {} resource(s): {} valid, {} error(s), {} warning(s)",
                counts.selected_resources, counts.valid_resources, counts.errors, counts.warnings
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::summary()
                .row("project", report.project.clone())
                .row("environment", report.environment.clone())
                .row("selection", selected)
                .row("environments", counts.environments.to_string())
                .row("configured sources", counts.configured_sources.to_string())
                .row("selected resources", counts.selected_resources.to_string())
                .row("valid resources", counts.valid_resources.to_string())
                .row("warnings", counts.warnings.to_string())
                .row("errors", counts.errors.to_string())
                .row("authority current", counts.authority_current.to_string())
                .row("authority stale", counts.authority_stale.to_string())
                .row("authority missing", counts.authority_missing.to_string()),
        );

    if !report.resources.is_empty() {
        let resources = report.resources.iter().fold(
            Table::new(["resource", "source", "valid", "authority", "diagnostics"]),
            |table, resource| {
                table.row([
                    resource.resource_id.clone(),
                    resource
                        .configured_source
                        .clone()
                        .unwrap_or_else(|| "unresolved".to_owned()),
                    yes_no(resource.valid).to_owned(),
                    format!("{:?}", resource.authority).to_lowercase(),
                    resource.diagnostics.len().to_string(),
                ])
            },
        );
        document = document.blank_line().push(resources);
    }

    let all_diagnostics = report
        .diagnostics
        .iter()
        .chain(
            report
                .resources
                .iter()
                .flat_map(|resource| &resource.diagnostics),
        )
        .collect::<Vec<_>>();
    if !all_diagnostics.is_empty() {
        let diagnostics = all_diagnostics.iter().fold(
            Table::new(["scope", "severity", "code", "message"]),
            |table, diagnostic| {
                table.row([
                    diagnostic
                        .resource_id
                        .clone()
                        .or_else(|| diagnostic.path.clone())
                        .unwrap_or_else(|| "project".to_owned()),
                    format!("{:?}", diagnostic.severity).to_lowercase(),
                    diagnostic.code.clone(),
                    diagnostic.message.clone(),
                ])
            },
        );
        document = document.blank_line().push(diagnostics);
    }

    document = document.blank_line().push(
        KeyValuePanel::effects()
            .row("writes", report.effects.writes.clone())
            .row("checked", report.effects.checked.join("; "))
            .row("not checked", report.effects.skipped.join("; ")),
    );
    if counts.errors == 0
        && let Some(resource_id) = report.selection.resolved.first()
    {
        document = document
            .blank_line()
            .push(NextCommand::new(format!("cdf plan {resource_id}")));
    }
    document
}

fn path_list(paths: &[String]) -> String {
    if paths.is_empty() {
        "none".to_owned()
    } else {
        paths.join(", ")
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
