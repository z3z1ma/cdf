use crate::{
    args::{Cli, SqlArgs},
    commands::output,
    context::ProjectContext,
    output::{CliError, CommandOutput},
    system_sql,
};

pub(crate) fn sql(cli: &Cli, args: SqlArgs) -> Result<CommandOutput, CliError> {
    let query = system_sql::read_only_query(&args.query)?;
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let report = system_sql::run(&context, query)?;
    output(
        "sql",
        format!(
            "sql returned {} row(s) from local system history",
            report.row_count()
        ),
        report,
    )
}
