use crate::{
    args::{BackfillArgs, Cli},
    context::ProjectContext,
    output::{CliError, CommandOutput},
};

pub(crate) fn backfill(cli: &Cli, args: BackfillArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    if let Some(resource_id) = &args.resource_id {
        context.resource(resource_id)?;
    }
    Err(CliError::not_supported(
        "backfill",
        "bounded historical planning and checkpoint-safe replay windows are not exposed by lower crates",
        "backfill planner/orchestrator",
    ))
}
