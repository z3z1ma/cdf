use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
};

pub(super) fn show_document(report: &ContractShowCliReport) -> RenderDocument {
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!("contract policy {}", report.policy),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Policy")
                .row("name", report.policy.clone())
                .row(
                    "schema review",
                    yes_no(report.contract.schema.review_artifact_required),
                )
                .row(
                    "receipts required",
                    yes_no(report.contract.receipts_required),
                )
                .row(
                    "reconciliation",
                    yes_no(report.contract.reconciliation_counts),
                )
                .row("retention", format!("{:?}", report.contract.retention)),
        )
        .blank_line()
        .push(NextCommand::new("cdf contract freeze"))
}

pub(super) fn freeze_document(report: &ContractFreezeReport) -> RenderDocument {
    let table = report.snapshots.iter().fold(
        Table::new(["resource", "schema", "policy", "program"]),
        |table, (resource, snapshot)| {
            table.row([
                resource.clone(),
                optional_string(snapshot.schema_hash.clone()),
                optional_string(snapshot.policy_hash.clone()),
                optional_string(snapshot.validation_program_hash.clone()),
            ])
        },
    );

    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!("froze {} contract snapshot(s)", report.counts.frozen),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Contract registry")
                .row("file", LOCK_FILE_NAME)
                .row("registry", report.registry.clone())
                .row("resources", report.resource_ids.len().to_string())
                .row("frozen", report.counts.frozen.to_string())
                .row("missing", report.counts.missing.to_string())
                .row("drifted", report.counts.drifted.to_string()),
        );

    if !report.snapshots.is_empty() {
        document = document.blank_line().push(table);
    }

    document
        .blank_line()
        .push(NextCommand::new("cdf contract test"))
}

pub(super) fn test_document(report: &ContractTestReport) -> RenderDocument {
    let drifted = report.counts.drifted > 0;
    let table = report.snapshots.iter().fold(
        Table::new(["resource", "verdict", "drift fields"]),
        |table, comparison| {
            table.row([
                comparison.resource_id.clone(),
                format!("{:?}", comparison.verdict).to_lowercase(),
                if comparison.drift_details.is_empty() {
                    "none".to_owned()
                } else {
                    comparison
                        .drift_details
                        .iter()
                        .map(|detail| detail.field.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                },
            ])
        },
    );

    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            if drifted {
                StatusKind::Warning
            } else {
                StatusKind::Success
            },
            format!(
                "contract test: {} passed, {} drifted",
                report.counts.passed, report.counts.drifted
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Contract registry")
                .row("registry", report.registry.clone())
                .row("resources", report.resource_ids.len().to_string())
                .row("passed", report.counts.passed.to_string())
                .row("drifted", report.counts.drifted.to_string())
                .row("missing", report.counts.missing.to_string()),
        );

    if !report.snapshots.is_empty() {
        document = document.blank_line().push(table);
    }

    document.blank_line().push(NextCommand::new(if drifted {
        "cdf contract freeze"
    } else {
        "cdf plan"
    }))
}

fn optional_string(value: Option<String>) -> String {
    value.unwrap_or_else(|| "none".to_owned())
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
