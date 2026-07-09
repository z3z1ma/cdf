use cdf_kernel::{CdfError, PipelineId, RunEventSink, TargetName};
use cdf_project::{
    BackfillPlan, BackfillPlanRequest, BackfillSlice, ProjectRunRequest, ProjectRunSource,
    WindowScopedResource, backfill_pipeline_id, plan_backfill, run_project,
};
use serde::Serialize;

use crate::{
    args::{BackfillArgs, Cli},
    context::ProjectContext,
    destination_uri::{
        destination_error_suggestions, redact_error_value, resolve_environment_destination,
    },
    error_catalog,
    output::{CliError, CommandOutput},
    progress::{ProgressSnapshot, human_progress_sink},
    project_run_resource::build_project_run_resource,
    render::{
        RenderDocument,
        humanize::humanize_rows,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
    reports::{RunDestinationReport, WriteEffects},
    scan_command::default_target_for_resource,
};

pub(crate) fn backfill(cli: &Cli, args: BackfillArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let resource = context.resource(&args.resource_id)?;
    let target = TargetName::new(
        args.target
            .clone()
            .unwrap_or_else(|| default_target_for_resource(&args.resource_id)),
    )?;
    let plan = plan_backfill(
        resource,
        BackfillPlanRequest {
            target: target.clone(),
            from: args.from.clone(),
            to: args.to.clone(),
            slice_size: args.slice_size,
        },
    )?;

    if !args.execute {
        let report = BackfillCliReport::planned(&plan, args.slice_size);
        return CommandOutput::rendered("backfill", report.render_document(), report);
    }

    let run_resource = build_project_run_resource(&context, resource)?;
    let source = run_resource.as_project_resource();
    source.validate_supported().map_err(CliError::from)?;
    let pipeline_id = backfill_pipeline_id()?;
    let progress = human_progress_sink(cli.json, cli.no_color);
    let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);
    let mut reports = Vec::with_capacity(plan.slices.len());
    for slice in &plan.slices {
        let report = execute_slice(&context, &target, source, &pipeline_id, slice, event_sink)
            .map_err(|error| {
                annotate_backfill_slice_error(
                    error,
                    slice,
                    progress.as_ref().map(|progress| progress.snapshot()),
                )
            })?;
        reports.push(report);
    }
    let report = BackfillCliReport::executed(&plan, args.slice_size, reports);
    match progress {
        Some(progress) => CommandOutput::rendered_with_progress(
            "backfill",
            report.render_document(),
            report,
            progress.snapshot(),
        ),
        None => CommandOutput::rendered("backfill", report.render_document(), report),
    }
}

fn execute_slice(
    context: &ProjectContext,
    target: &TargetName,
    source: ProjectRunSource<'_>,
    pipeline_id: &PipelineId,
    slice: &BackfillSlice,
    event_sink: Option<&dyn RunEventSink>,
) -> Result<BackfillSliceExecutionReport, CliError> {
    let resolved = resolve_environment_destination(context, target)
        .map_err(|error| backfill_destination_resolution_error(context, error))?;
    let destination = resolved.destination;
    let destination_report =
        RunDestinationReport::from_project(&destination.describe(), destination.target());
    let scoped = WindowScopedResource::new(source.queryable(), slice.scope.clone());
    let run = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&scoped),
        plan: slice.engine_plan.clone(),
        package_root: context.package_root(),
        state_store_path: context.state_store_path()?,
        pipeline_id: pipeline_id.clone(),
        package_id: slice.package_id.clone(),
        checkpoint_id: slice.checkpoint_id()?,
        destination,
        run_id: None,
        event_sink,
        after_receipt_verified: None,
    }))
    .map_err(|error| redact_error_value(error, resolved.secret_redaction.as_deref()))?;
    Ok(BackfillSliceExecutionReport {
        run_id: run.run_id.to_string(),
        package_id: run.package_id,
        package_dir: run.package_dir.display().to_string(),
        package_hash: run.package_hash.to_string(),
        checkpoint_id: run.checkpoint.delta.checkpoint_id.to_string(),
        receipt_id: run.receipt.receipt_id.to_string(),
        row_count: run.row_count,
        segment_count: run.segment_count,
        destination: destination_report
            .with_receipt_destination(run.receipt.destination.to_string()),
    })
}

fn annotate_backfill_slice_error(
    mut error: CliError,
    slice: &BackfillSlice,
    progress: Option<ProgressSnapshot>,
) -> CliError {
    let recovery_command = progress
        .as_ref()
        .and_then(|progress| progress.latest_run_id_for_package(&slice.package_id))
        .map(|run_id| format!("cdf resume {run_id}"));
    let mutation_status = if recovery_command.is_some() {
        "run ledger recorded this slice; package, destination, receipt, or checkpoint artifacts may need recovery"
    } else {
        "no run id was recorded; destination and checkpoint mutation were not started"
    };
    let next_recovery = recovery_command
        .as_deref()
        .unwrap_or("not available before a run id is recorded");
    let original_message = error.message;
    error.message = format!(
        "backfill slice {} ({}..{}) failed; package {}; checkpoint {}; mutation status: {}; next recovery command: {}; lower-layer error: {}",
        slice.ordinal,
        safe_display_value(&slice.start),
        safe_display_value(&slice.end),
        safe_display_value(&slice.package_id),
        safe_display_value(&slice.checkpoint_id),
        mutation_status,
        next_recovery,
        original_message
    );

    if let Some(command) = recovery_command {
        let mut suggestions = error.suggestions.to_vec();
        if !suggestions.iter().any(|suggestion| suggestion == &command) {
            suggestions.push(command);
        }
        error = error.with_suggestions(suggestions);
    }
    if let Some(progress) = progress {
        error = error.with_progress(progress);
    }
    error
}

fn backfill_destination_resolution_error(context: &ProjectContext, error: CdfError) -> CliError {
    let error = redact_error_value(error, None);
    if error
        .message
        .contains("no project destination driver registered")
        || error.message.contains("malformed or non-local")
        || error.message.contains("is missing a scheme")
    {
        CliError::not_supported_with(
            "backfill",
            error.message,
            "registered project destination driver",
            error_catalog::DESTINATION_NOT_SUPPORTED,
        )
        .with_suggestions(destination_error_suggestions(context, None))
    } else {
        error.into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct BackfillCliReport {
    mode: &'static str,
    resource_id: String,
    target: String,
    requested: BackfillRequestedBoundsReport,
    slices: Vec<BackfillSliceReport>,
    pipeline_id: String,
    writes: WriteEffects,
}

impl BackfillCliReport {
    fn planned(plan: &BackfillPlan, slice_size: Option<u64>) -> Self {
        Self {
            mode: "dry_plan",
            resource_id: plan.resource_id.clone(),
            target: plan.target.clone(),
            requested: BackfillRequestedBoundsReport::from_plan(plan, slice_size),
            slices: plan
                .slices
                .iter()
                .map(|slice| {
                    BackfillSliceReport::from_planned_slice(slice, "planned", "dry_plan_only")
                })
                .collect(),
            pipeline_id: plan.pipeline_id.clone(),
            writes: WriteEffects::none(),
        }
    }

    fn executed(
        plan: &BackfillPlan,
        slice_size: Option<u64>,
        reports: Vec<BackfillSliceExecutionReport>,
    ) -> Self {
        Self {
            mode: "execute",
            resource_id: plan.resource_id.clone(),
            target: plan.target.clone(),
            requested: BackfillRequestedBoundsReport::from_plan(plan, slice_size),
            slices: plan
                .slices
                .iter()
                .zip(reports)
                .map(|(slice, report)| BackfillSliceReport::from_executed_slice(slice, report))
                .collect(),
            pipeline_id: plan.pipeline_id.clone(),
            writes: WriteEffects::all(),
        }
    }

    fn render_document(&self) -> RenderDocument {
        let executed = self.mode == "execute";
        let mut document = RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!(
                    "{} backfill {} -> {}",
                    if executed { "executed" } else { "planned" },
                    self.resource_id,
                    self.target
                ),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Backfill")
                    .row("mode", self.mode)
                    .row("resource", self.resource_id.clone())
                    .row("target", self.target.clone())
                    .row("pipeline", self.pipeline_id.clone())
                    .row("from", self.requested.from.clone())
                    .row("to", self.requested.to.clone())
                    .row("slice size", optional_u64(self.requested.slice_size))
                    .row("slices", self.slices.len().to_string()),
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

        let table = self.slices.iter().fold(
            Table::new(["slice", "window", "status", "package", "checkpoint", "rows"]),
            |table, slice| {
                table.row([
                    slice.ordinal.to_string(),
                    format!("{}..{}", slice.start, slice.end),
                    slice.status.to_owned(),
                    safe_display_value(&slice.package_id),
                    safe_display_value(&slice.checkpoint_id),
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
                            self.slices
                                .iter()
                                .filter(|slice| slice.status == "succeeded")
                                .count(),
                            self.slices.len()
                        ),
                    )
                    .row("rows", humanize_rows(self.executed_row_count()))
                    .row("segments", self.executed_segment_count().to_string()),
            );
            document = document
                .blank_line()
                .push(NextCommand::new("cdf state history <resource>"));
        } else {
            document = document.blank_line().push(NextCommand::new(format!(
                "cdf backfill {} --from {} --to {} --target {} --execute",
                self.resource_id, self.requested.from, self.requested.to, self.target
            )));
        }

        document
    }

    fn executed_row_count(&self) -> u64 {
        self.slices
            .iter()
            .filter_map(|slice| slice.executed.as_ref())
            .map(|executed| executed.row_count)
            .sum()
    }

    fn executed_segment_count(&self) -> usize {
        self.slices
            .iter()
            .filter_map(|slice| slice.executed.as_ref())
            .map(|executed| executed.segment_count)
            .sum()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct BackfillRequestedBoundsReport {
    from: String,
    to: String,
    slice_size: Option<u64>,
}

impl BackfillRequestedBoundsReport {
    fn from_plan(plan: &BackfillPlan, slice_size: Option<u64>) -> Self {
        Self {
            from: plan.from.clone(),
            to: plan.to.clone(),
            slice_size,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct BackfillSliceReport {
    ordinal: usize,
    start: String,
    end: String,
    filters: Vec<String>,
    package_id: String,
    checkpoint_id: String,
    scope: cdf_kernel::ScopeKey,
    status: &'static str,
    reason: &'static str,
    executed: Option<BackfillSliceExecutionReport>,
}

impl BackfillSliceReport {
    fn from_planned_slice(
        slice: &BackfillSlice,
        status: &'static str,
        reason: &'static str,
    ) -> Self {
        Self {
            ordinal: slice.ordinal,
            start: slice.start.clone(),
            end: slice.end.clone(),
            filters: slice.filters.clone(),
            package_id: slice.package_id.clone(),
            checkpoint_id: slice.checkpoint_id.clone(),
            scope: slice.scope.clone(),
            status,
            reason,
            executed: None,
        }
    }

    fn from_executed_slice(slice: &BackfillSlice, executed: BackfillSliceExecutionReport) -> Self {
        Self {
            executed: Some(executed),
            ..Self::from_planned_slice(slice, "succeeded", "executed")
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct BackfillSliceExecutionReport {
    run_id: String,
    package_id: String,
    package_dir: String,
    package_hash: String,
    checkpoint_id: String,
    receipt_id: String,
    row_count: u64,
    segment_count: usize,
    destination: RunDestinationReport,
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
