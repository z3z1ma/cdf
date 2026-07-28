mod recover;
mod render;

use cdf_kernel::{CheckpointId, CheckpointStore, PipelineId, ResourceId, ScopeKey};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::{
    args::{Cli, RewindArgs, StateCommand, StateScopeArgs},
    context::ProjectContext,
    error_catalog,
    output::{CliError, CommandOutput},
    run_command::DEFAULT_RUN_PIPELINE_ID,
};

use self::recover::recover;

pub(crate) fn state(
    cli: &Cli,
    command: StateCommand,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    match command {
        StateCommand::Show(args) => show(cli, args),
        StateCommand::History(args) => history(cli, args),
        StateCommand::Rewind(args) => rewind(cli, args),
        StateCommand::Recover(args) => recover(cli, args, execution, destinations),
    }
}

fn show(cli: &Cli, args: StateScopeArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let store = context.state_store()?;
    let pipeline_id = state_pipeline_id(&args)?;
    let resource_id = ResourceId::new(args.resource_id.clone())?;
    let scope = scope_key(&args)?;
    let head = store.head(&pipeline_id, &resource_id, &scope)?;
    let report = StateShowReport {
        scope,
        head,
        args,
        pipeline_id,
    };
    CommandOutput::rendered("state show", render::show_document(&report), report)
}

fn history(cli: &Cli, args: StateScopeArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let store = context.state_store()?;
    let pipeline_id = state_pipeline_id(&args)?;
    let resource_id = ResourceId::new(args.resource_id.clone())?;
    let scope = scope_key(&args)?;
    let history = store.history(&pipeline_id, &resource_id, &scope)?;
    let report = StateHistoryReport {
        scope,
        history,
        args,
        pipeline_id,
    };
    CommandOutput::rendered("state history", render::history_document(&report), report)
}

fn rewind(cli: &Cli, args: RewindArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let store = context.state_store()?;
    let outcome = store.rewind(cdf_kernel::RewindRequest {
        marker_checkpoint_id: CheckpointId::new(args.marker_checkpoint_id)?,
        pipeline_id: state_pipeline_id(&args.scope)?,
        resource_id: ResourceId::new(args.scope.resource_id.clone())?,
        scope: scope_key(&args.scope)?,
        target_checkpoint_id: CheckpointId::new(args.target_checkpoint_id)?,
    })?;
    let report = StateRewindReport {
        outcome,
        args: args.scope,
    };
    CommandOutput::rendered("state rewind", render::rewind_document(&report), report)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateShowReport {
    scope: ScopeKey,
    head: Option<cdf_kernel::Checkpoint>,
    #[serde(skip)]
    args: StateScopeArgs,
    #[serde(skip)]
    pipeline_id: PipelineId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateHistoryReport {
    scope: ScopeKey,
    history: Vec<cdf_kernel::Checkpoint>,
    #[serde(skip)]
    args: StateScopeArgs,
    #[serde(skip)]
    pipeline_id: PipelineId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateRewindReport {
    #[serde(flatten)]
    outcome: cdf_kernel::RewindReport,
    #[serde(skip)]
    args: StateScopeArgs,
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
        (Some(_), false) => Err(CliError::usage_with(
            "state command accepts either --scope-json or --scope key=value, not both",
            error_catalog::STATE_SCOPE_ARGUMENT,
        )),
        (Some(scope_json), true) => serde_json::from_str(scope_json).map_err(|error| {
            CliError::usage_with(
                format!("--scope-json must encode a ScopeKey: {error}"),
                error_catalog::STATE_SCOPE_ARGUMENT,
            )
        }),
        (None, false) => scope_key_from_pairs(&args.scope),
        (None, true) => Ok(ScopeKey::Resource),
    }
}

fn scope_key_from_pairs(pairs: &[String]) -> Result<ScopeKey, CliError> {
    let mut scope = Map::new();
    for pair in pairs {
        let (key, value) = pair.split_once('=').ok_or_else(|| {
            CliError::usage_with(
                "--scope values must be key=value pairs",
                error_catalog::STATE_SCOPE_ARGUMENT,
            )
        })?;
        if key.is_empty() {
            return Err(CliError::usage_with(
                "--scope key must not be empty",
                error_catalog::STATE_SCOPE_ARGUMENT,
            ));
        }
        scope.insert(key.to_owned(), Value::String(value.to_owned()));
    }
    serde_json::from_value(Value::Object(scope)).map_err(|error| {
        CliError::usage_with(
            format!("--scope must encode a ScopeKey: {error}"),
            error_catalog::STATE_SCOPE_ARGUMENT,
        )
    })
}
