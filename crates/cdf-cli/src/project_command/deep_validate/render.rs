use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
};

pub(super) fn document(report: &DeepValidateReport) -> RenderDocument {
    let status = if report.summary.failed == 0 {
        StatusKind::Success
    } else {
        StatusKind::Error
    };
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            status,
            format!(
                "deep validated project {} ({} passed, {} failed)",
                report.project, report.summary.passed, report.summary.failed
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Deep validate")
                .row("mode", report.mode.clone())
                .row("environment", report.environment.clone())
                .row("resources", report.summary.resources.to_string())
                .row("partitions", report.summary.partitions.to_string())
                .row(
                    "discovery probes",
                    report.summary.discovery_probes.to_string(),
                )
                .row("warnings", report.summary.warnings.to_string())
                .row("writes", "none"),
        );

    let table = report.resources.iter().fold(
        Table::new([
            "resource",
            "status",
            "kind",
            "execution",
            "schema",
            "partitions",
            "destination",
        ]),
        |table, resource| {
            table.row([
                resource.resource_id.clone(),
                resource.status.clone(),
                resource.source_kind.clone(),
                resource.execution_extent.clone(),
                resource.schema_source.clone(),
                resource.partitions.count.to_string(),
                resource
                    .destination
                    .target
                    .clone()
                    .unwrap_or_else(|| resource.destination.status.clone()),
            ])
        },
    );
    document = document.blank_line().push(table);

    let diagnostics = report
        .resources
        .iter()
        .flat_map(|resource| {
            resource
                .diagnostics
                .iter()
                .map(move |diagnostic| (resource.resource_id.as_str(), diagnostic))
        })
        .collect::<Vec<_>>();
    if !diagnostics.is_empty() {
        let table = diagnostics.into_iter().fold(
            Table::new(["resource", "severity", "check", "message", "remediation"]),
            |table, (resource_id, diagnostic)| {
                table.row([
                    resource_id.to_owned(),
                    diagnostic.severity.clone(),
                    diagnostic.check.clone(),
                    diagnostic.message.clone(),
                    diagnostic.remediation.clone(),
                ])
            },
        );
        document = document.blank_line().push(table);
    }

    document
        .blank_line()
        .push(NextCommand::new(if report.summary.failed == 0 {
            "cdf plan <resource>"
        } else {
            "cdf inspect resources"
        }))
}
