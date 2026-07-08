mod migrate;
mod recover;

use cdf_kernel::{CheckpointId, CheckpointStore, PipelineId, ResourceId, ScopeKey};
use serde_json::json;

use crate::{
    args::{Cli, RewindArgs, StateCommand, StateScopeArgs},
    commands::output,
    context::ProjectContext,
    output::{CliError, CommandOutput},
};

use self::{migrate::migrate, recover::recover};

pub(crate) fn state(cli: &Cli, command: StateCommand) -> Result<CommandOutput, CliError> {
    match command {
        StateCommand::Show(args) => show(cli, args),
        StateCommand::History(args) => history(cli, args),
        StateCommand::Rewind(args) => rewind(cli, args),
        StateCommand::Migrate => migrate(cli),
        StateCommand::Recover(args) => recover(cli, args),
    }
}

fn show(cli: &Cli, args: StateScopeArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let store = context.state_store()?;
    let scope = scope_key(args.scope_json.as_deref())?;
    let head = store.head(
        &PipelineId::new(args.pipeline_id)?,
        &ResourceId::new(args.resource_id)?,
        &scope,
    )?;
    output(
        "state show",
        if head.is_some() {
            "state head found".to_owned()
        } else {
            "no committed state head".to_owned()
        },
        json!({ "scope": scope, "head": head }),
    )
}

fn history(cli: &Cli, args: StateScopeArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let store = context.state_store()?;
    let scope = scope_key(args.scope_json.as_deref())?;
    let history = store.history(
        &PipelineId::new(args.pipeline_id)?,
        &ResourceId::new(args.resource_id)?,
        &scope,
    )?;
    output(
        "state history",
        format!("{} checkpoint(s)", history.len()),
        json!({ "scope": scope, "history": history }),
    )
}

fn rewind(cli: &Cli, args: RewindArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let store = context.state_store()?;
    let report = store.rewind(cdf_kernel::RewindRequest {
        marker_checkpoint_id: CheckpointId::new(args.marker_checkpoint_id)?,
        pipeline_id: PipelineId::new(args.scope.pipeline_id)?,
        resource_id: ResourceId::new(args.scope.resource_id)?,
        scope: scope_key(args.scope.scope_json.as_deref())?,
        target_checkpoint_id: CheckpointId::new(args.target_checkpoint_id)?,
    })?;
    output(
        "state rewind",
        format!(
            "rewound to {}; {} package(s) ahead of state",
            report.head.delta.checkpoint_id,
            report.packages_ahead.len()
        ),
        report,
    )
}

fn scope_key(scope_json: Option<&str>) -> Result<ScopeKey, CliError> {
    match scope_json {
        Some(json) => serde_json::from_str(json).map_err(|error| {
            CliError::usage(format!("--scope-json must encode a ScopeKey: {error}"))
        }),
        None => Ok(ScopeKey::Resource),
    }
}
