use crate::{
    args::{Cli, ResumeArgs},
    context::ProjectContext,
    output::{CliError, CommandOutput},
};

pub(crate) fn resume(cli: &Cli, args: ResumeArgs) -> Result<CommandOutput, CliError> {
    let _context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    Err(CliError::not_supported(
        "resume",
        format!(
            "run recovery orchestration is not exposed by lower crates{}",
            args.run_id
                .as_ref()
                .map(|id| format!(" for run `{id}`"))
                .unwrap_or_default()
        ),
        "run ledger/recovery orchestrator",
    ))
}
