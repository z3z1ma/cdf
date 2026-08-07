use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, StatusKind, StatusLine},
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
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
