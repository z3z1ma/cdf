use cdf_state_sqlite::migrate_sqlite_state;
use serde::Serialize;

use crate::{
    args::Cli,
    context::ProjectContext,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, SectionRule, StatusKind, StatusLine, Table},
    },
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
    CommandOutput::rendered("state migrate", cli_report.render_document(), cli_report)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateMigrateCliReport {
    command: &'static str,
    state_store_path: String,
    applied_count: usize,
    components: Vec<cdf_state_sqlite::SqliteStateComponentMigration>,
}

impl StateMigrateCliReport {
    fn render_document(&self) -> RenderDocument {
        let table = self.components.iter().fold(
            Table::new(["component", "before", "after", "target", "action"]),
            |table, component| {
                table.row([
                    component.component.to_owned(),
                    optional_i64(component.before_version),
                    component.after_version.to_string(),
                    component.target_version.to_string(),
                    migration_action_name(component.action).to_owned(),
                ])
            },
        );

        RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!(
                    "state migration checked {} component(s)",
                    self.components.len()
                ),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("State store")
                    .row("path", self.state_store_path.clone())
                    .row("applied", self.applied_count.to_string())
                    .row("mutation performed", mutation_summary(self.applied_count)),
            )
            .blank_line()
            .push(table)
    }
}

fn optional_i64(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned())
}

fn mutation_summary(applied_count: usize) -> &'static str {
    if applied_count == 0 {
        "none; all SQLite state components were current"
    } else {
        "SQLite state schema migration applied"
    }
}

fn migration_action_name(action: cdf_state_sqlite::SqliteStateMigrationAction) -> &'static str {
    match action {
        cdf_state_sqlite::SqliteStateMigrationAction::Current => "current",
        cdf_state_sqlite::SqliteStateMigrationAction::Initialized => "initialized",
        cdf_state_sqlite::SqliteStateMigrationAction::Migrated => "migrated",
    }
}
