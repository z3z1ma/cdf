mod recover;

use cdf_kernel::{CheckpointId, CheckpointStore, PipelineId, ResourceId, ScopeKey};
use serde_json::{Map, Value, json};

use crate::{
    args::{Cli, RewindArgs, StateCommand, StateScopeArgs},
    context::ProjectContext,
    error_catalog,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
    },
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
    let document = state_show_document(&args, &pipeline_id, &scope, head.as_ref());
    CommandOutput::rendered(
        "state show",
        document,
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
    let document = state_history_document(&args, &pipeline_id, &scope, &history);
    CommandOutput::rendered(
        "state history",
        document,
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
    let document = state_rewind_document(&args.scope, &report);
    CommandOutput::rendered("state rewind", document, report)
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

fn state_show_document(
    args: &StateScopeArgs,
    pipeline_id: &PipelineId,
    scope: &ScopeKey,
    head: Option<&cdf_kernel::Checkpoint>,
) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            if head.is_some() {
                StatusKind::Success
            } else {
                StatusKind::Warning
            },
            if head.is_some() {
                "state head found"
            } else {
                "no committed state head"
            },
        ))
        .blank_line()
        .push(scope_panel("Scope", args, pipeline_id, scope));

    document = match head {
        Some(head) => document.blank_line().push(checkpoint_panel("Head", head)),
        None => document.blank_line().push(
            KeyValuePanel::new("Head")
                .row("checkpoint", "none")
                .row("status", "missing")
                .row("mutation performed", "none"),
        ),
    };

    document
        .blank_line()
        .push(NextCommand::new(state_scope_command(
            "cdf state history",
            args,
        )))
}

fn state_history_document(
    args: &StateScopeArgs,
    pipeline_id: &PipelineId,
    scope: &ScopeKey,
    history: &[cdf_kernel::Checkpoint],
) -> RenderDocument {
    let table = history.iter().fold(
        Table::new(["checkpoint", "status", "head", "package", "receipt"]),
        |table, checkpoint| {
            table.row([
                checkpoint.delta.checkpoint_id.to_string(),
                checkpoint.status.as_str().to_owned(),
                yes_no(checkpoint.is_head).to_owned(),
                checkpoint.delta.package_hash.to_string(),
                checkpoint
                    .receipt
                    .as_ref()
                    .map(|receipt| receipt.receipt_id.to_string())
                    .unwrap_or_else(|| "none".to_owned()),
            ])
        },
    );

    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!("{} checkpoint(s)", history.len()),
        ))
        .blank_line()
        .push(scope_panel("Scope", args, pipeline_id, scope))
        .blank_line()
        .push(history_panel(history))
        .blank_line()
        .push(table)
        .blank_line()
        .push(NextCommand::new(state_scope_command(
            "cdf state show",
            args,
        )))
}

fn state_rewind_document(
    args: &StateScopeArgs,
    report: &cdf_kernel::RewindReport,
) -> RenderDocument {
    let table = report
        .packages_ahead
        .iter()
        .fold(Table::new(["package ahead of state"]), |table, package| {
            table.row([package.to_string()])
        });

    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!("rewound to {}", report.head.delta.checkpoint_id),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Rewind")
                .row("marker", report.marker.delta.checkpoint_id.to_string())
                .row(
                    "target",
                    report
                        .marker
                        .rewind_target_checkpoint_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| report.head.delta.checkpoint_id.to_string()),
                )
                .row("new head", report.head.delta.checkpoint_id.to_string())
                .row("marker status", report.marker.status.as_str())
                .row("head status", report.head.status.as_str())
                .row("packages ahead", report.packages_ahead.len().to_string())
                .row("mutation performed", "rewind marker checkpoint appended"),
        )
        .blank_line()
        .push(table)
        .blank_line()
        .push(NextCommand::new(state_scope_command(
            "cdf state show",
            args,
        )))
}

fn scope_panel(
    title: &str,
    args: &StateScopeArgs,
    pipeline_id: &PipelineId,
    scope: &ScopeKey,
) -> KeyValuePanel {
    KeyValuePanel::new(title)
        .row("pipeline", pipeline_id.to_string())
        .row("resource", args.resource_id.clone())
        .row(
            "scope",
            serde_json::to_string(scope).unwrap_or_else(|_| "<unavailable>".to_owned()),
        )
}

fn checkpoint_panel(title: &str, checkpoint: &cdf_kernel::Checkpoint) -> KeyValuePanel {
    KeyValuePanel::new(title)
        .row("checkpoint", checkpoint.delta.checkpoint_id.to_string())
        .row("status", checkpoint.status.as_str())
        .row("is head", yes_no(checkpoint.is_head))
        .row("package", checkpoint.delta.package_hash.to_string())
        .row(
            "receipt",
            checkpoint
                .receipt
                .as_ref()
                .map(|receipt| receipt.receipt_id.to_string())
                .unwrap_or_else(|| "none".to_owned()),
        )
}

fn history_panel(history: &[cdf_kernel::Checkpoint]) -> KeyValuePanel {
    KeyValuePanel::new("History")
        .row("checkpoints", history.len().to_string())
        .row(
            "oldest",
            history
                .first()
                .map(|checkpoint| checkpoint.delta.checkpoint_id.to_string())
                .unwrap_or_else(|| "none".to_owned()),
        )
        .row(
            "newest",
            history
                .last()
                .map(|checkpoint| checkpoint.delta.checkpoint_id.to_string())
                .unwrap_or_else(|| "none".to_owned()),
        )
        .row(
            "head",
            history
                .iter()
                .find(|checkpoint| checkpoint.is_head)
                .map(|checkpoint| checkpoint.delta.checkpoint_id.to_string())
                .unwrap_or_else(|| "none".to_owned()),
        )
}

fn state_scope_command(prefix: &str, args: &StateScopeArgs) -> String {
    let mut command = format!("{prefix} {}", args.resource_id);
    if let Some(pipeline_id) = &args.pipeline_id {
        command.push_str(" --pipeline ");
        command.push_str(pipeline_id);
    }
    if let Some(scope_json) = &args.scope_json {
        append_scope_json_as_command_args(&mut command, scope_json);
    }
    for pair in &args.scope {
        command.push_str(" --scope ");
        command.push_str(pair);
    }
    command
}

fn append_scope_json_as_command_args(command: &mut String, scope_json: &str) {
    let Ok(Value::Object(scope)) = serde_json::from_str::<Value>(scope_json) else {
        command.push_str(" --scope-json ");
        command.push_str(scope_json);
        return;
    };

    let mut pairs = Vec::new();
    for (key, value) in scope {
        let Value::String(value) = value else {
            command.push_str(" --scope-json ");
            command.push_str(scope_json);
            return;
        };
        pairs.push(format!("{key}={value}"));
    }

    for pair in pairs {
        command.push_str(" --scope ");
        command.push_str(&pair);
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
