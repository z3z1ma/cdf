use cdf_state_sqlite::migrate_sqlite_state;
use serde::Serialize;

use crate::{
    args::Cli,
    commands::output,
    context::ProjectContext,
    output::{CliError, CommandOutput},
    run_command::ensure_parent_directory,
};

pub(super) fn migrate(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let state_store_path = context.state_store_path()?;
    ensure_parent_directory(&state_store_path)?;
    let report = migrate_sqlite_state(&state_store_path)?;
    let applied_count = report.applied_count();
    let cli_report = StateMigrateCliReport {
        command: "state migrate",
        state_store_path: state_store_path.display().to_string(),
        applied_count,
        components: report.components,
    };
    output(
        "state migrate",
        format!(
            "migrated local SQLite state at {}; {} component(s) applied",
            cli_report.state_store_path, applied_count
        ),
        cli_report,
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateMigrateCliReport {
    command: &'static str,
    state_store_path: String,
    applied_count: usize,
    components: Vec<cdf_state_sqlite::SqliteStateComponentMigration>,
}
