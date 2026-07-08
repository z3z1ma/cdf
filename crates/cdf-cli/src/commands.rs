use cdf_kernel::CdfError;
use serde::Serialize;
use serde_json::json;

use crate::{
    args::{Cli, Command},
    output::{CliError, CommandOutput, InvocationResult},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn execute(cli: Cli) -> InvocationResult {
    let json_mode = cli.json;
    match dispatch(cli) {
        Ok(output) => InvocationResult::from_output(json_mode, output),
        Err(error) => InvocationResult::from_error(json_mode, error),
    }
}

fn dispatch(cli: Cli) -> Result<CommandOutput, CliError> {
    let command = cli.command.clone();
    match command {
        Command::Help => Ok(output(
            "help",
            HELP_TEXT.to_owned(),
            json!({ "help": HELP_TEXT }),
        )?),
        Command::Version => Ok(output(
            "version",
            format!("cdf {VERSION}"),
            json!({ "version": VERSION }),
        )?),
        Command::Init(args) => crate::project_command::init(args),
        Command::Validate => crate::project_command::validate(&cli),
        Command::Plan(args) => crate::scan_command::plan_or_explain(&cli, args, "plan"),
        Command::Explain(args) => crate::scan_command::plan_or_explain(&cli, args, "explain"),
        Command::Run(args) => crate::run_command::run(&cli, args),
        Command::Preview(args) => crate::scan_command::preview(&cli, args),
        Command::Sql(args) => crate::sql_command::sql(&cli, args),
        Command::Inspect(args) => crate::inspect_command::inspect(&cli, args),
        Command::DiffSchema => crate::project_command::diff_schema(&cli),
        Command::Contract(command) => crate::contract_command::contract(&cli, command),
        Command::State(command) => crate::state_command::state(&cli, command),
        Command::Resume(args) => crate::resume_command::resume(&cli, args),
        Command::ReplayPackage(args) => crate::replay_command::replay_package(&cli, args),
        Command::Backfill(args) => crate::backfill_command::backfill(&cli, args),
        Command::Package(command) => crate::package_command::package(&cli, command),
        Command::Doctor => crate::doctor_command::doctor(&cli),
        Command::Status => crate::status_command::status(&cli),
    }
}

pub(crate) fn output<T: Serialize>(
    command: &'static str,
    human: String,
    value: T,
) -> Result<CommandOutput, CliError> {
    report_output(command, human, value, 0)
}

pub(crate) fn report_output<T: Serialize>(
    command: &'static str,
    human: String,
    value: T,
    exit_code: i32,
) -> Result<CommandOutput, CliError> {
    Ok(CommandOutput {
        command,
        exit_code,
        human,
        json: serde_json::to_value(value).map_err(json_cli_error)?,
    })
}

pub(crate) fn json_cli_error(error: serde_json::Error) -> CliError {
    CliError::from(CdfError::internal(error.to_string()))
}

const HELP_TEXT: &str = r#"cdf 0.1.0

Usage:
  cdf [--project PATH] [--env NAME] [--json] <command>

Commands:
  init [DIR] [--name NAME] [--force]
  validate
  plan <RESOURCE> --target TARGET [--select a,b] [--filter EXPR] [--limit N]
  explain <RESOURCE> --target TARGET [--select a,b] [--filter EXPR] [--limit N]
  run --resource RESOURCE --pipeline ID --target TARGET --package-id ID --checkpoint-id ID [--loop]
  preview <RESOURCE> [--select a,b] [--filter EXPR] [--limit N]
  sql <QUERY>
  inspect project|resources|resource <ID>|lock|destinations|package <DIR>|run <ID>
  diff schema
  contract freeze|show|test
  state show|history --pipeline ID --resource ID [--scope-json JSON]
  state rewind --pipeline ID --resource ID --target-checkpoint ID --marker-checkpoint ID [--scope-json JSON]
  state migrate
  state recover --package DIR --to DEST [--receipt ID] [--target schema.table --merge-dedup fail]
  resume [RUN_ID]
  replay package <DIR>
  backfill [RESOURCE]
  package ls [DIR]
  package gc [DIR]
  package verify <DIR>
  package archive <DIR> [--format parquet] [--force]
  doctor
  status
"#;
