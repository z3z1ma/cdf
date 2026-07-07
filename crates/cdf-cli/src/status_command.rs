use crate::{
    args::Cli,
    commands::report_output,
    context::ProjectContext,
    output::{CliError, CommandOutput},
    status_freshness,
};

pub(crate) fn status(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let report = status_freshness::evaluate(&context)?;
    let exit_code = report.exit_code();
    let human = status_freshness::human_summary(&report);
    report_output("status", human, report, exit_code)
}
