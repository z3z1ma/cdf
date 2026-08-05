mod render;

use crate::{
    args::Cli,
    context::ProjectContext,
    output::{CliError, CommandOutput},
    status_freshness,
};

pub(crate) fn status(
    cli: &Cli,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load_with_destination_registry(
        cli.project.as_ref(),
        cli.env.as_deref(),
        destinations,
    )?;
    let report = status_freshness::evaluate(&context)?;
    let exit_code = report.exit_code();
    CommandOutput::rendered_with_exit_code("status", render::document(&report), report, exit_code)
}
