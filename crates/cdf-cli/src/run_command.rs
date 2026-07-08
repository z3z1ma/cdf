use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{CdfError, CheckpointId, PipelineId, TargetName};
use cdf_project::{ProjectRunRequest, run_project};

use crate::{
    args::{Cli, RunArgs, ScanArgs},
    context::ProjectContext,
    destination_uri::{redact_error_value, resolve_selected_destination},
    error_catalog,
    output::{CliError, CommandOutput},
    project_run_resource::build_project_run_resource,
    reports::{RunCliReport, RunDestinationReport},
    scan_command::{build_engine_plan, default_target_for_resource},
};

pub(crate) const DEFAULT_RUN_PIPELINE_ID: &str = "cdf-run";

pub(crate) fn run(cli: &Cli, args: RunArgs) -> Result<CommandOutput, CliError> {
    if args.loop_mode {
        return Err(CliError::not_supported_with(
            "run --loop",
            "the local development loop supervisor is excluded from this explicit one-package run slice",
            "later loop/streaming supervisor",
            error_catalog::RUN_LOOP_NOT_SUPPORTED,
        ));
    }
    let explicit = resolved_run_args(args)?;
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let resource = context.resource(&explicit.resource_id)?;
    let run_resource = build_project_run_resource(&context, resource)?;
    let state_store_path = context.state_store_path()?;
    let plan = build_engine_plan(
        &context,
        &ScanArgs {
            resource_id: explicit.resource_id.clone(),
            destination_uri: None,
            target: None,
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            package_id: Some(explicit.package_id.clone()),
        },
    )?;
    let resolved = resolve_selected_destination(
        &context,
        &explicit.target,
        explicit.destination_uri.as_deref(),
    )
    .map_err(run_destination_resolution_error)?;
    let destination = resolved.destination;
    let destination_report =
        RunDestinationReport::from_project(&destination.describe(), destination.target());
    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: run_resource.as_project_resource(),
        plan,
        package_root: context.package_root(),
        state_store_path,
        pipeline_id: explicit.pipeline_id.clone(),
        package_id: explicit.package_id.clone(),
        checkpoint_id: explicit.checkpoint_id.clone(),
        destination,
        run_id: None,
        event_sink: None,
        after_receipt_verified: None,
    }))
    .map_err(|error| redact_error_value(error, resolved.secret_redaction.as_deref()))?;
    let cli_report = RunCliReport::from_report(&report, destination_report);
    CommandOutput::rendered("run", cli_report.render_document(), cli_report)
}

fn run_destination_resolution_error(error: CdfError) -> CliError {
    if error
        .message
        .contains("no project destination driver registered")
        || error.message.contains("malformed or non-local")
        || error.message.contains("is missing a scheme")
    {
        CliError::not_supported_with(
            "run",
            error.message,
            "registered project destination driver",
            error_catalog::DESTINATION_NOT_SUPPORTED,
        )
    } else {
        error.into()
    }
}

fn resolved_run_args(args: RunArgs) -> Result<ResolvedRunArgs, CliError> {
    let resource_id = args.resource_id.ok_or_else(|| {
        CliError::usage_with(
            "run requires RESOURCE or --resource",
            error_catalog::RUN_ARGUMENT,
        )
    })?;
    let suffix = minted_run_suffix(&resource_id);
    let package_id = args.package_id.unwrap_or_else(|| format!("pkg-{suffix}"));
    let checkpoint_id = args
        .checkpoint_id
        .unwrap_or_else(|| format!("checkpoint-{suffix}"));
    Ok(ResolvedRunArgs {
        resource_id: resource_id.clone(),
        pipeline_id: PipelineId::new(
            args.pipeline_id
                .unwrap_or_else(|| DEFAULT_RUN_PIPELINE_ID.to_owned()),
        )?,
        destination_uri: args.destination_uri,
        target: TargetName::new(
            args.target
                .unwrap_or_else(|| default_target_for_resource(&resource_id)),
        )?,
        package_id,
        checkpoint_id: CheckpointId::new(checkpoint_id)?,
    })
}

fn minted_run_suffix(resource_id: &str) -> String {
    let resource = resource_id.replace(|character: char| !character.is_ascii_alphanumeric(), "-");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{resource}-{}-{nanos}", std::process::id())
}

struct ResolvedRunArgs {
    resource_id: String,
    pipeline_id: PipelineId,
    destination_uri: Option<String>,
    target: TargetName,
    package_id: String,
    checkpoint_id: CheckpointId,
}

pub(crate) fn ensure_parent_directory(path: &std::path::Path) -> Result<(), CliError> {
    let Some(parent) = path.parent() else {
        return Err(CliError::mapped(
            CdfError::internal(format!("{} has no parent directory", path.display())),
            error_catalog::RUN_ARTIFACT_INTERNAL,
        ));
    };
    fs::create_dir_all(parent).map_err(|error| {
        CliError::mapped(
            CdfError::data(format!("create {}: {error}", parent.display())),
            error_catalog::RUN_ARTIFACT_PATH,
        )
    })
}
