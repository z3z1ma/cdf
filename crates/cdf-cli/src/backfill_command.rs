use cdf_kernel::{CdfError, PipelineId, TargetName};
use cdf_project::{
    BackfillPlan, BackfillPlanRequest, BackfillSlice, ProjectRunRequest, ProjectRunSource,
    WindowScopedResource, backfill_pipeline_id, plan_backfill, run_project,
};
use serde::Serialize;

use crate::{
    args::{BackfillArgs, Cli},
    commands::output,
    context::ProjectContext,
    destination_uri::{redact_error_value, resolve_environment_destination},
    output::{CliError, CommandOutput},
    project_run_resource::build_project_run_resource,
    reports::{RunDestinationReport, WriteEffects},
};

pub(crate) fn backfill(cli: &Cli, args: BackfillArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let resource = context.resource(&args.resource_id)?;
    let target = TargetName::new(args.target.clone())?;
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
        return output("backfill", report.human_message(), report);
    }

    let run_resource = build_project_run_resource(&context, resource)?;
    let source = run_resource.as_project_resource();
    source.validate_supported().map_err(CliError::from)?;
    let pipeline_id = backfill_pipeline_id()?;
    let mut reports = Vec::with_capacity(plan.slices.len());
    for slice in &plan.slices {
        reports.push(execute_slice(
            &context,
            &target,
            source,
            &pipeline_id,
            slice,
        )?);
    }
    let report = BackfillCliReport::executed(&plan, args.slice_size, reports);
    output("backfill", report.human_message(), report)
}

fn execute_slice(
    context: &ProjectContext,
    target: &TargetName,
    source: ProjectRunSource<'_>,
    pipeline_id: &PipelineId,
    slice: &BackfillSlice,
) -> Result<BackfillSliceExecutionReport, CliError> {
    let resolved = resolve_environment_destination(context, target)
        .map_err(backfill_destination_resolution_error)?;
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

fn backfill_destination_resolution_error(error: CdfError) -> CliError {
    if error
        .message
        .contains("no project destination driver registered")
        || error.message.contains("malformed or non-local")
        || error.message.contains("is missing a scheme")
    {
        CliError::not_supported(
            "backfill",
            error.message,
            "registered project destination driver",
        )
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

    fn human_message(&self) -> String {
        match self.mode {
            "execute" => format!(
                "executed backfill for {} to {}: {} slice(s) through the run spine",
                self.resource_id,
                self.target,
                self.slices.len()
            ),
            _ => format!(
                "planned backfill for {} to {}: {} slice(s); wrote no package, destination data, checkpoint rows, or run-ledger events",
                self.resource_id,
                self.target,
                self.slices.len()
            ),
        }
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
