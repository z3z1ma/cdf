mod migrate;
mod recover;

use cdf_kernel::{CheckpointId, CheckpointStore, PipelineId, ResourceId, ScopeKey};
use serde_json::{Map, Value, json};

use crate::{
    args::{Cli, RewindArgs, StateCommand, StateScopeArgs},
    commands::output,
    context::ProjectContext,
    output::{CliError, CommandOutput},
    run_command::DEFAULT_RUN_PIPELINE_ID,
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
    let pipeline_id = state_pipeline_id(&args)?;
    let resource_id = ResourceId::new(args.resource_id.clone())?;
    let scope = scope_key(&args)?;
    let head = store.head(&pipeline_id, &resource_id, &scope)?;
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
    let pipeline_id = state_pipeline_id(&args)?;
    let resource_id = ResourceId::new(args.resource_id.clone())?;
    let scope = scope_key(&args)?;
    let history = store.history(&pipeline_id, &resource_id, &scope)?;
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
        pipeline_id: state_pipeline_id(&args.scope)?,
        resource_id: ResourceId::new(args.scope.resource_id.clone())?,
        scope: scope_key(&args.scope)?,
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

fn state_pipeline_id(args: &StateScopeArgs) -> Result<PipelineId, CliError> {
    PipelineId::new(
        args.pipeline_id
            .clone()
            .unwrap_or_else(|| DEFAULT_RUN_PIPELINE_ID.to_owned()),
    )
    .map_err(CliError::from)
}

fn scope_key(args: &StateScopeArgs) -> Result<ScopeKey, CliError> {
    match (args.scope_json.as_deref(), args.scope.is_empty()) {
        (Some(_), false) => Err(CliError::usage(
            "state command accepts either --scope-json or --scope key=value, not both",
        )),
        (Some(scope_json), true) => serde_json::from_str(scope_json).map_err(|error| {
            CliError::usage(format!("--scope-json must encode a ScopeKey: {error}"))
        }),
        (None, false) => scope_key_from_pairs(&args.scope),
        (None, true) => Ok(ScopeKey::Resource),
    }
}

fn scope_key_from_pairs(pairs: &[String]) -> Result<ScopeKey, CliError> {
    let mut scope = Map::new();
    for pair in pairs {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| CliError::usage("--scope values must be key=value pairs"))?;
        if key.is_empty() {
            return Err(CliError::usage("--scope key must not be empty"));
        }
        scope.insert(key.to_owned(), Value::String(value.to_owned()));
    }
    serde_json::from_value(Value::Object(scope))
        .map_err(|error| CliError::usage(format!("--scope must encode a ScopeKey: {error}")))
}
