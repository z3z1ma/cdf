use super::*;
use crate::{
    render::{
        RenderDocument,
        humanize::{humanize_bytes, humanize_rows},
        primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
    reports::discovery_coverage_panel,
};

pub(super) fn plan_report_document(report: &PlanReport) -> RenderDocument {
    let status = if report.counts.failed == 0 {
        StatusKind::Success
    } else {
        StatusKind::Error
    };
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            status,
            format!(
                "planned {}/{} selected resource(s)",
                report.counts.ready, report.counts.selected
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Plan readiness")
                .row("project", &report.project)
                .row("environment", &report.environment)
                .row("selected", report.counts.selected.to_string())
                .row("ready", report.counts.ready.to_string())
                .row("failed", report.counts.failed.to_string()),
        );
    for outcome in &report.resources {
        document = match outcome {
            PlanResourceOutcome::Ready { report } => {
                document.blank_line().append(scan_report_document(report))
            }
            PlanResourceOutcome::Failed { resource_id, error } => {
                document.blank_line().push(StatusLine::new(
                    StatusKind::Error,
                    format!("failed {resource_id} [{}]: {}", error.code, error.message),
                ))
            }
        };
    }
    if let Some(artifact) = &report.artifact {
        let status = match artifact.status {
            crate::portable_plan_command::PortablePlanWriteStatus::Created => "created",
            crate::portable_plan_command::PortablePlanWriteStatus::Unchanged => "unchanged",
        };
        document = document.blank_line().push(
            KeyValuePanel::new("Portable plan")
                .row("status", status)
                .row("path", artifact.path.clone())
                .row("hash", artifact.plan_hash.clone())
                .row("resources", artifact.resources.to_string())
                .row("size", humanize_bytes(artifact.bytes)),
        );
    }
    document
}

pub(super) fn scan_report_document(report: &ScanPlanReport) -> RenderDocument {
    let pushed = report.pushdown.pushed.len();
    let inexact = report.pushdown.inexact.len();
    let unsupported = report.pushdown.unsupported.len();
    let migrations = report.ddl_preview.migrations.len();
    let scheduler_jobs = report
        .scheduler
        .as_ref()
        .map(|scheduler| scheduler.effective_jobs.jobs.to_string())
        .unwrap_or_else(|| "source-owned".to_owned());
    let scheduler_limits = report
        .scheduler
        .as_ref()
        .map(|scheduler| scheduler.effective_jobs.limiting_factors.join(", "))
        .unwrap_or_else(|| "not declared".to_owned());
    let scheduler_memory = report
        .scheduler
        .as_ref()
        .map(|scheduler| humanize_bytes(scheduler.managed_memory_available_bytes))
        .unwrap_or_else(|| "not declared".to_owned());
    let summary = KeyValuePanel::new("Plan")
        .row("resource", report.resource_id.clone())
        .row("destination", report.destination.destination_id.clone())
        .row("target", report.destination.target.clone())
        .row("location", safe_display_value(&report.destination.label))
        .row(
            "execution",
            execution_extent_name(&report.explain.execution_extent),
        )
        .row("partitions", report.will_fetch.partition_count.to_string())
        .row("jobs", scheduler_jobs.clone())
        .row(
            "projection",
            list_or_default(&report.will_fetch.projection, "all fields"),
        )
        .row(
            "filters",
            list_or_default(&report.will_fetch.filters, "none"),
        )
        .row("limit", optional_u64(report.will_fetch.limit))
        .row("disposition", report.destination.disposition.clone())
        .row("guarantee", report.delivery_guarantee.clone())
        .row(
            "schema fields",
            report.resource_schema.fields.len().to_string(),
        )
        .row("migrations", migrations.to_string());
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "{} {} -> {}",
                report.human_command, report.resource_id, report.destination.target
            ),
        ))
        .blank_line()
        .push(summary);
    if inexact > 0 || unsupported > 0 {
        document = document.blank_line().push(
            KeyValuePanel::attention()
                .row("inexact pushdowns", inexact.to_string())
                .row("unsupported pushdowns", unsupported.to_string())
                .row(
                    "effect",
                    "CDF evaluates these operations after source extraction",
                ),
        );
    }
    let mut document = document
        .blank_line()
        .push_verbose(
            KeyValuePanel::new("Fetch")
                .row("project", report.project.clone())
                .row("environment", report.environment.clone())
                .row("package", report.package_id.clone())
                .row(
                    "execution",
                    execution_extent_name(&report.explain.execution_extent),
                )
                .row("partitions", report.will_fetch.partition_count.to_string())
                .row("effective jobs", scheduler_jobs)
                .row("job ceiling", scheduler_limits)
                .row("managed memory available", scheduler_memory)
                .row(
                    "projection",
                    list_or_default(&report.will_fetch.projection, "all fields"),
                )
                .row(
                    "filters",
                    list_or_default(&report.will_fetch.filters, "none"),
                )
                .row("limit", optional_u64(report.will_fetch.limit)),
        )
        .blank_line()
        .push_verbose(
            KeyValuePanel::new("Pushdown")
                .row("pushed", pushed.to_string())
                .row("inexact", inexact.to_string())
                .row("unsupported", unsupported.to_string())
                .row("projection", yes_no(report.explain.projection_pushed))
                .row("limit", yes_no(report.explain.limit_pushed)),
        )
        .blank_line()
        .push_verbose(
            KeyValuePanel::new("Destination")
                .row("destination", report.destination.destination_id.clone())
                .row("target", report.destination.target.clone())
                .row("label", safe_display_value(&report.destination.label))
                .row("schemes", report.destination.schemes.join(", "))
                .row("disposition", report.destination.disposition.clone())
                .row("idempotency", report.destination.idempotency.clone()),
        )
        .blank_line()
        .push_verbose(
            KeyValuePanel::new("Guarantee")
                .row("guarantee", report.delivery_guarantee.clone())
                .row(
                    "qualifier",
                    report.delivery_guarantee_detail.qualifier.clone(),
                )
                .row("basis", report.delivery_guarantee_detail.basis.clone()),
        )
        .blank_line()
        .push_verbose(
            KeyValuePanel::new("Contract")
                .row("schema", report.resource_schema.schema_hash.clone())
                .row("normalizer", report.normalization.version.clone())
                .row(
                    "schema source",
                    report.resource_schema.schema_source.clone(),
                )
                .row(
                    "schema snapshot",
                    report
                        .resource_schema
                        .snapshot_path
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                )
                .row("fields", report.resource_schema.fields.len().to_string())
                .row("state scope", report.state_advancement.scope.to_string())
                .row(
                    "cursor",
                    report
                        .state_advancement
                        .cursor
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                )
                .row(
                    "advances after",
                    report.state_advancement.advances_after.clone(),
                ),
        );
    if let Some(boundary) = &report.explain.source_boundary {
        document = document.blank_line().push_verbose(
            KeyValuePanel::new("Source Boundary")
                .row(
                    "transfer modes",
                    boundary
                        .transfer_modes
                        .iter()
                        .map(|mode| source_transfer_mode_name(*mode))
                        .collect::<Vec<_>>()
                        .join(", "),
                )
                .row(
                    "execution lane",
                    source_execution_lane_name(boundary.execution_lane),
                )
                .row(
                    "internal parallelism",
                    boundary.maximum_internal_parallelism.to_string(),
                ),
        );
    }
    let mut document = if let Some(snapshot) = &report.schema_snapshot {
        let document = document.blank_line().push_verbose(
            KeyValuePanel::new("Schema Snapshot")
                .row("outcome", snapshot.outcome)
                .row("hash", snapshot.schema_hash.clone())
                .row("path", snapshot.path.clone())
                .row("snapshot written", yes_no(snapshot.snapshot_written))
                .row("lockfile written", yes_no(snapshot.lockfile_written)),
        );
        if let Some(discovery) = &snapshot.discovery {
            document
                .blank_line()
                .push_verbose(discovery_coverage_panel(discovery))
        } else {
            document
        }
    } else {
        document
    };
    document = document.blank_line().push_verbose(
        KeyValuePanel::new("Migration")
            .row("supported", yes_no(report.ddl_preview.supported))
            .row("support", report.ddl_preview.migration_support.clone())
            .row("items", migrations.to_string())
            .row("target", report.ddl_preview.target.clone()),
    );

    if !report.ddl_preview.migrations.is_empty() {
        let table = report.ddl_preview.migrations.iter().fold(
            Table::new(["migration", "description"]),
            |table, migration| {
                table.row([
                    migration.migration_id.clone(),
                    safe_display_value(&migration.description),
                ])
            },
        );
        document = document.blank_line().push_verbose(table);
    }

    document
        .blank_line()
        .push(NextCommand::new(next_run_command(
            &report.resource_id,
            report.human_destination_uri.as_deref(),
        )))
}

fn execution_extent_name(extent: &cdf_kernel::ExecutionExtent) -> &'static str {
    match extent {
        cdf_kernel::ExecutionExtent::Bounded { .. } => "bounded",
        cdf_kernel::ExecutionExtent::Drain { .. } => "drain",
        cdf_kernel::ExecutionExtent::Resident { .. } => "resident",
    }
}

fn source_transfer_mode_name(mode: cdf_kernel::SourceTransferMode) -> &'static str {
    match mode {
        cdf_kernel::SourceTransferMode::ArrowCData => "arrow_c_data",
        cdf_kernel::SourceTransferMode::ArrowIpcStream => "arrow_ipc_stream",
        cdf_kernel::SourceTransferMode::RowCompat => "row_compat",
    }
}

fn source_execution_lane_name(lane: cdf_kernel::SourceExecutionLane) -> &'static str {
    match lane {
        cdf_kernel::SourceExecutionLane::Cpu => "cpu",
        cdf_kernel::SourceExecutionLane::Blocking => "blocking",
        cdf_kernel::SourceExecutionLane::IsolatedProcess => "isolated_process",
        cdf_kernel::SourceExecutionLane::Sandbox => "sandbox",
    }
}

fn list_or_default(values: &[String], default: &str) -> String {
    if values.is_empty() {
        default.to_owned()
    } else {
        values.join(", ")
    }
}

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned())
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn safe_display_value(value: &str) -> String {
    redact_uri_userinfo(value)
}

pub(super) fn next_run_command(resource_id: &str, destination_uri: Option<&str>) -> String {
    let mut command = format!("cdf run {resource_id}");
    if let Some(destination_uri) = destination_uri {
        command.push_str(" --to ");
        command.push_str(&safe_display_value(destination_uri));
    }
    command
}

pub(super) fn preview_document(report: &PreviewReport) -> RenderDocument {
    let mut summary = KeyValuePanel::summary()
        .row("resource", report.resource.clone())
        .row("rows", humanize_rows(report.row_count))
        .row("data", humanize_bytes(report.byte_count))
        .row("partitions", report.selected_partition_count.to_string())
        .row("batches", report.inspected_batch_count.to_string())
        .row("fields", report.fields.len().to_string())
        .row("truncated", yes_no(report.truncated))
        .row("writes", "none");
    if report.quarantined_row_count > 0
        || report.residual_row_count > 0
        || report.terminal_quarantine_count > 0
    {
        summary = summary
            .row("quarantined rows", report.quarantined_row_count.to_string())
            .row("rows with residuals", report.residual_row_count.to_string())
            .row(
                "quarantined files",
                report.terminal_quarantine_count.to_string(),
            );
    }
    let document = RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "Previewed {} rows from {}",
                humanize_rows(report.row_count),
                report.resource_id
            ),
        ))
        .blank_line()
        .push(summary)
        .blank_line()
        .push_verbose(
            KeyValuePanel::new("Preview")
                .row("resource", report.resource.clone())
                .row("partition", report.partition.clone())
                .row("batch", report.batch.clone())
                .row(
                    "partitions planned",
                    report.planned_partition_count.to_string(),
                )
                .row(
                    "payload eligible",
                    report.payload_eligible_partition_count.to_string(),
                )
                .row(
                    "payload selected",
                    report.selected_partition_count.to_string(),
                )
                .row(
                    "payload partitions opened",
                    report.payload_opened_partition_count.to_string(),
                )
                .row(
                    "partitions attested",
                    report.attested_partition_count.to_string(),
                )
                .row(
                    "partitions inspected",
                    report.inspected_partition_count.to_string(),
                )
                .row(
                    "partitions partial",
                    report.partially_inspected_partition_count.to_string(),
                )
                .row(
                    "payload uninspected",
                    report.payload_uninspected_partition_count.to_string(),
                )
                .row(
                    "batches inspected",
                    report.inspected_batch_count.to_string(),
                )
                .row("rows", humanize_rows(report.row_count))
                .row("decoded bytes inspected", humanize_bytes(report.byte_count))
                .row("rendered bytes", humanize_bytes(report.output_byte_count))
                .row("row limit", humanize_rows(report.limits.max_rows))
                .row("byte limit", humanize_bytes(report.limits.max_bytes))
                .row("global batch limit", report.limits.max_batches.to_string())
                .row("policy", report.selection.policy.clone())
                .row("selector", report.selection.selector.clone())
                .row("truncated", yes_no(report.truncated))
                .row("rows quarantined", report.quarantined_row_count.to_string())
                .row("rows with residuals", report.residual_row_count.to_string())
                .row(
                    "files quarantined",
                    report.terminal_quarantine_count.to_string(),
                )
                .row("normalizer", report.normalization.version.clone())
                .row("fields", report.fields.join(", ")),
        );
    let document = if let Some(snapshot) = &report.schema_snapshot {
        let document = document.blank_line().push_verbose(
            KeyValuePanel::new("Schema Snapshot")
                .row("outcome", snapshot.outcome)
                .row("hash", snapshot.schema_hash.clone())
                .row("path", snapshot.path.clone())
                .row("snapshot written", yes_no(snapshot.snapshot_written))
                .row("lockfile written", yes_no(snapshot.lockfile_written)),
        );
        if let Some(discovery) = &snapshot.discovery {
            document
                .blank_line()
                .push_verbose(discovery_coverage_panel(discovery))
        } else {
            document
        }
    } else {
        document
    };
    document
        .blank_line()
        .push_verbose(
            KeyValuePanel::effects()
                .row("package", yes_no(report.writes.package()))
                .row("destination", yes_no(report.writes.destination()))
                .row("checkpoint", yes_no(report.writes.checkpoint())),
        )
        .blank_line()
        .push(NextCommand::new(format!("cdf plan {}", report.resource_id)))
}
