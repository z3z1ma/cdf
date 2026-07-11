use cdf_kernel::CdfError;
use serde::Serialize;
use serde_json::json;

use crate::{
    args::{Cli, Command},
    error_catalog,
    output::{CliError, CommandOutput, InvocationResult},
    render::{RenderConfig, RenderDocument},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn execute(cli: Cli) -> InvocationResult {
    let json_mode = cli.json;
    let render_config = RenderConfig::detect(cli.no_color);
    match dispatch(cli) {
        Ok(output) => InvocationResult::from_output(json_mode, &render_config, output),
        Err(error) => InvocationResult::from_error_with_config(json_mode, &render_config, error),
    }
}

fn dispatch(cli: Cli) -> Result<CommandOutput, CliError> {
    let command = cli.command.clone();
    match command {
        Command::Help(help_text) => Ok(output(
            "help",
            help_text.clone(),
            json!({ "help": help_text }),
        )?),
        Command::Version => Ok(output(
            "version",
            format!("cdf {VERSION}"),
            json!({ "version": VERSION }),
        )?),
        Command::Init(args) => crate::project_command::init(args),
        Command::Add(args) => crate::add_command::add(&cli, args),
        Command::Validate(args) => crate::project_command::validate(&cli, args),
        Command::Plan(args) => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::scan_command::plan_or_explain(&cli, args, "plan", &services)
        }
        Command::Explain(args) => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::scan_command::plan_or_explain(&cli, args, "explain", &services)
        }
        Command::Run(args) => {
            let managed = cdf_memory_budget()?;
            let (host, services) = cdf_engine::StandaloneExecutionHost::default_services(managed)?;
            crate::run_command::run(&cli, args, host.as_ref(), &services)
        }
        Command::Preview(args) => {
            let (host, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::scan_command::preview(&cli, args, host.as_ref(), &services)
        }
        Command::Sql(args) => crate::sql_command::sql(&cli, args),
        Command::Inspect(args) => crate::inspect_command::inspect(&cli, args),
        Command::DiffSchema => crate::project_command::diff_schema(&cli),
        Command::Schema(command) => crate::schema_command::schema(&cli, command),
        Command::Contract(command) => crate::contract_command::contract(&cli, command),
        Command::State(command) => crate::state_command::state(&cli, command),
        Command::Resume(args) => crate::resume_command::resume(&cli, args),
        Command::ReplayPackage(args) => crate::replay_command::replay_package(&cli, args),
        Command::Backfill(args) if args.execute => {
            let (host, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::backfill_command::backfill(&cli, args, Some((host.as_ref(), &services)))
        }
        Command::Backfill(args) => crate::backfill_command::backfill(&cli, args, None),
        Command::Package(command) => crate::package_command::package(&cli, command),
        Command::Doctor => crate::doctor_command::doctor(&cli),
        Command::Status => crate::status_command::status(&cli),
    }
}

fn cdf_memory_budget() -> Result<u64, CliError> {
    let default_authority = cdf_memory::DEFAULT_PROCESS_BUDGET_BYTES;
    let effective_authority = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_authority);
    let resolution = cdf_memory::resolve_memory_budget(
        None,
        effective_authority,
        64 * 1024 * 1024,
        8 * 1024 * 1024 * 1024,
    )?;
    Ok(resolution.managed_pool_bytes)
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
    // 10x: compatibility shim for parser-generated help/version text; command
    // families should return structured RenderDocument values directly.
    CommandOutput::rendered_with_exit_code(command, RenderDocument::text(human), value, exit_code)
}

pub(crate) fn json_cli_error(error: serde_json::Error) -> CliError {
    CliError::mapped(
        CdfError::internal(error.to_string()),
        error_catalog::CLI_JSON,
    )
}
