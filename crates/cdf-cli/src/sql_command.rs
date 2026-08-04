mod render;

use crate::{
    args::{Cli, SqlArgs},
    context::ProjectManifestContext,
    output::{CliError, CommandOutput},
    system_sql,
};

pub(crate) fn sql(cli: &Cli, args: SqlArgs) -> Result<CommandOutput, CliError> {
    let query = system_sql::read_only_query(&args.query)?;
    let context = ProjectManifestContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let report = system_sql::run(&context, query)?;
    CommandOutput::rendered("sql", render::document(&report), report)
}
