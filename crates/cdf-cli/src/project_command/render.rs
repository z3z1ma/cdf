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

pub(super) fn validate_document(report: &ProjectValidationCliReport) -> RenderDocument {
    let validation = &report.validation;
    let secret_table = validation.checked_secrets.iter().fold(
        Table::new(["secret reference", "status"]),
        |table, secret| {
            table.row([
                secret.uri.as_str().to_owned(),
                format!("{:?}", secret.status).to_lowercase(),
            ])
        },
    );

    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!("validated project {}", report.project_name),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Project")
                .row("name", report.project_name.clone())
                .row("environment", validation.environment.name.clone())
                .row(
                    "declarative resources",
                    validation.declarative_resources.to_string(),
                )
                .row(
                    "external resources",
                    validation.external_resources.to_string(),
                )
                .row(
                    "secret references",
                    validation.checked_secrets.len().to_string(),
                ),
        );

    if !validation.checked_secrets.is_empty() {
        document = document.blank_line().push(secret_table);
    }

    document.blank_line().push(NextCommand::new("cdf plan"))
}

pub(super) fn diff_schema_document(report: &DiffSchemaCliReport) -> RenderDocument {
    let table = report.diffs.iter().fold(
        Table::new(["kind", "path", "before", "after"]),
        |table, diff| {
            table.row([
                format!("{:?}", diff.kind).to_lowercase(),
                diff.path.clone(),
                diff.before.clone().unwrap_or_else(|| "none".to_owned()),
                diff.after.clone().unwrap_or_else(|| "none".to_owned()),
            ])
        },
    );

    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            if report.diffs.is_empty() {
                StatusKind::Success
            } else {
                StatusKind::Warning
            },
            format!("{} lock diff(s)", report.diffs.len()),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Schema diff")
                .row("diffs", report.diffs.len().to_string())
                .row(
                    "status",
                    if report.diffs.is_empty() {
                        "lockfile matches project"
                    } else {
                        "lockfile drift detected"
                    },
                ),
        );

    if !report.diffs.is_empty() {
        document = document.blank_line().push(table);
    }

    document
        .blank_line()
        .push(NextCommand::new(if report.diffs.is_empty() {
            "cdf validate"
        } else {
            "cdf contract freeze"
        }))
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
