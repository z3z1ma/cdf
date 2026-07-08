use crate::{
    args::Cli,
    context::ProjectContext,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
    },
    status_freshness,
    status_freshness::{
        FreshnessState, NonEvaluableReason, ReceiptFreshnessState, StatusReport, StatusResource,
    },
};

pub(crate) fn status(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let report = status_freshness::evaluate(&context)?;
    let exit_code = report.exit_code();
    CommandOutput::rendered_with_exit_code("status", status_document(&report), report, exit_code)
}

fn status_document(report: &StatusReport) -> RenderDocument {
    let summary = status_freshness::human_summary(report);
    let kind = if report.summary.stale > 0 {
        StatusKind::Error
    } else if report.summary.non_evaluable > 0 {
        StatusKind::Warning
    } else {
        StatusKind::Success
    };
    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(kind, summary))
        .blank_line()
        .push(
            KeyValuePanel::new("Freshness")
                .row("total", report.summary.total.to_string())
                .row("fresh", report.summary.fresh.to_string())
                .row("stale", report.summary.stale.to_string())
                .row("non-evaluable", report.summary.non_evaluable.to_string()),
        );

    if !report.freshness_resources.is_empty() {
        let table = report.freshness_resources.iter().fold(
            Table::new(["resource", "state", "age", "max age", "receipt"]),
            |table, resource| {
                table.row([
                    resource.resource_id.clone(),
                    freshness_state_name(&resource.freshness_state).to_owned(),
                    optional_ms(resource.age_ms),
                    format_ms(resource.max_age_ms),
                    receipt_status(resource),
                ])
            },
        );
        document = document.blank_line().push(table);
    }

    document.blank_line().push(NextCommand::new("cdf doctor"))
}

fn freshness_state_name(state: &FreshnessState) -> &'static str {
    match state {
        FreshnessState::Fresh => "fresh",
        FreshnessState::Stale => "stale",
        FreshnessState::NonEvaluable => "non-evaluable",
    }
}

fn optional_ms(value: Option<u64>) -> String {
    value.map(format_ms).unwrap_or_else(|| "none".to_owned())
}

fn format_ms(value: u64) -> String {
    if value < 1_000 {
        return format!("{value} ms");
    }
    let seconds = value / 1_000;
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;
    if minutes < 60 {
        return format!("{minutes}m {remaining_seconds:02}s");
    }
    let hours = minutes / 60;
    let remaining_minutes = minutes % 60;
    format!("{hours}h {remaining_minutes:02}m")
}

fn receipt_status(resource: &StatusResource) -> String {
    resource
        .receipt_freshness
        .as_ref()
        .map(|receipt| receipt_freshness_state_name(&receipt.state).to_owned())
        .or_else(|| {
            resource
                .non_evaluable_reason
                .as_ref()
                .map(non_evaluable_reason_name)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "n/a".to_owned())
}

fn receipt_freshness_state_name(state: &ReceiptFreshnessState) -> &'static str {
    match state {
        ReceiptFreshnessState::MissingRunLedger => "missing_run_ledger",
        ReceiptFreshnessState::MissingReceipt => "missing_receipt",
        ReceiptFreshnessState::FreshReceipt => "fresh_receipt",
        ReceiptFreshnessState::StaleReceipt => "stale_receipt",
        ReceiptFreshnessState::CorruptReceipt => "corrupt_receipt",
    }
}

fn non_evaluable_reason_name(reason: &NonEvaluableReason) -> &'static str {
    match reason {
        NonEvaluableReason::StateDatabaseMissing => "state_database_missing",
        NonEvaluableReason::CheckpointTableMissing => "checkpoint_table_missing",
        NonEvaluableReason::RunLedgerMissing => "run_ledger_missing",
        NonEvaluableReason::CommittedHeadMissing => "committed_head_missing",
        NonEvaluableReason::AmbiguousCommittedHeads => "ambiguous_committed_heads",
        NonEvaluableReason::ReceiptMissing => "receipt_missing",
        NonEvaluableReason::ReceiptCorrupt => "receipt_corrupt",
    }
}
