use super::*;
use crate::render::{
    RenderDocument,
    humanize::humanize_rows,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
};

pub(super) fn document(report: &BackfillCliReport) -> RenderDocument {
    let executed = report.mode == "execute";
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "{} backfill {} -> {}",
                if executed { "executed" } else { "planned" },
                report.resource_id,
                report.target
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Backfill")
                .row("mode", report.mode)
                .row("resource", report.resource_id.clone())
                .row("target", report.target.clone())
                .row("pipeline", report.pipeline_id.clone())
                .row("from", report.requested.from.clone())
                .row("to", report.requested.to.clone())
                .row("slice size", optional_u64(report.requested.slice_size))
                .row("slices", report.slices.len().to_string()),
        )
        .blank_line()
        .push(
            KeyValuePanel::new("Writes")
                .row("package", yes_no(executed))
                .row("destination", yes_no(executed))
                .row("checkpoint", yes_no(executed))
                .row(
                    "mutation",
                    if executed {
                        "ran each slice through the run spine"
                    } else {
                        "dry plan only; no package, destination, checkpoint, or run-ledger writes"
                    },
                ),
        );

    let table = report.slices.iter().fold(
        Table::new(["slice", "window", "status", "rows"]),
        |table, slice| {
            table.row([
                slice.ordinal.to_string(),
                format!("{}..{}", slice.start, slice.end),
                slice.status.to_owned(),
                slice
                    .executed
                    .as_ref()
                    .map(|executed| humanize_rows(executed.row_count))
                    .unwrap_or_else(|| "-".to_owned()),
            ])
        },
    );
    document = document.blank_line().push(table);

    if executed {
        document = document.blank_line().push(
            KeyValuePanel::new("Summary")
                .row(
                    "slices succeeded",
                    format!(
                        "{}/{}",
                        report
                            .slices
                            .iter()
                            .filter(|slice| slice.status == "succeeded")
                            .count(),
                        report.slices.len()
                    ),
                )
                .row("rows", humanize_rows(executed_row_count(report)))
                .row("segments", executed_segment_count(report).to_string()),
        );
        document = document
            .blank_line()
            .push(NextCommand::new("cdf state history <resource>"));
    } else {
        document = document.blank_line().push(NextCommand::new(format!(
            "cdf backfill {} --from {} --to {} --target {} --execute",
            report.resource_id, report.requested.from, report.requested.to, report.target
        )));
    }

    document
}

fn executed_row_count(report: &BackfillCliReport) -> u64 {
    report
        .slices
        .iter()
        .filter_map(|slice| slice.executed.as_ref())
        .map(|executed| executed.row_count)
        .sum()
}

fn executed_segment_count(report: &BackfillCliReport) -> u64 {
    report
        .slices
        .iter()
        .filter_map(|slice| slice.executed.as_ref())
        .map(|executed| executed.segment_count)
        .sum()
}

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned())
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
