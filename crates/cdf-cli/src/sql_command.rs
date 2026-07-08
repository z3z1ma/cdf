use crate::{
    args::{Cli, SqlArgs},
    context::ProjectContext,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
    },
    system_sql,
    system_sql::SystemSqlReport,
};
use serde_json::Value;

pub(crate) fn sql(cli: &Cli, args: SqlArgs) -> Result<CommandOutput, CliError> {
    let query = system_sql::read_only_query(&args.query)?;
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let report = system_sql::run(&context, query)?;
    CommandOutput::rendered("sql", sql_document(&report), report)
}

fn sql_document(report: &SystemSqlReport) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "sql returned {} row(s) from local system history",
                report.row_count()
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("System SQL")
                .row("tables", report.tables.len().to_string())
                .row("columns", report.columns.len().to_string())
                .row("rows", report.row_count().to_string()),
        );

    if !report.columns.is_empty() {
        let table = report
            .rows
            .iter()
            .fold(Table::from_headers(report.columns.clone()), |table, row| {
                table.row_values(row.iter().map(sql_value))
            });
        document = document.blank_line().push(table);
    }

    document.blank_line().push(NextCommand::new(
        "cdf sql \"select * from packages limit 5\"",
    ))
}

fn sql_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "<json>".to_owned())
        }
    }
}
