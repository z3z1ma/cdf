use cdf_kernel::CdfError;
use serde_json::json;

use crate::{
    args::{Cli, Command},
    error_catalog,
    output::{CliError, CommandOutput, InvocationResult},
    render::{RenderConfig, RenderDocument},
    terminal::OutputChannel,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn execute(cli: Cli, destinations: &cdf_runtime::DestinationRegistry) -> InvocationResult {
    let json_mode = cli.json;
    let stdout_config = RenderConfig::detect(&cli.terminal, OutputChannel::Stdout);
    let stderr_config = RenderConfig::detect(&cli.terminal, OutputChannel::Stderr);
    match dispatch(cli, destinations) {
        Ok(output) => InvocationResult::from_output_with_configs(
            json_mode,
            &stdout_config,
            &stderr_config,
            output,
        ),
        Err(error) => InvocationResult::from_error_with_config(json_mode, &stderr_config, error),
    }
}

fn dispatch(
    cli: Cli,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let command = cli.command.clone();
    match command {
        Command::Help(help_text) => CommandOutput::rendered(
            "help",
            RenderDocument::text(help_text.clone()),
            json!({ "help": help_text }),
        ),
        Command::Version => CommandOutput::rendered(
            "version",
            RenderDocument::text(format!("cdf {VERSION}")),
            json!({ "version": VERSION }),
        ),
        Command::Init(args) => crate::project_command::init(args),
        Command::Add(args) => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::add_command::add(&cli, args, &services, destinations)
        }
        Command::Validate(args) => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::project_command::validate(&cli, args, &services, destinations)
        }
        Command::Plan(args) => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::scan_command::plan_or_explain(&cli, args, "plan", &services, destinations)
        }
        Command::Explain(args) => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::scan_command::plan_or_explain(&cli, args, "explain", &services, destinations)
        }
        Command::Run(args) => {
            let managed = cdf_memory_budget()?;
            let (host, services) = cdf_engine::StandaloneExecutionHost::default_services(managed)?;
            crate::run_command::run(&cli, args, host.as_ref(), &services, destinations)
        }
        Command::Preview(args) => {
            let (host, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::scan_command::preview(&cli, args, host.as_ref(), &services, destinations)
        }
        Command::Sql(args) => crate::sql_command::sql(&cli, args),
        Command::Inspect(args) => crate::inspect_command::inspect(&cli, args, destinations),
        Command::DiffSchema => crate::project_command::diff_schema(&cli),
        Command::Schema(command) => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::schema_command::schema(&cli, command, &services, destinations)
        }
        Command::Contract(command) => {
            crate::contract_command::contract(&cli, command, destinations)
        }
        Command::State(command) => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::state_command::state(&cli, command, &services, destinations)
        }
        Command::Resume(args) => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::resume_command::resume(&cli, args, &services, destinations)
        }
        Command::ReplayPackage(args) => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::replay_command::replay_package(&cli, args, &services, destinations)
        }
        Command::Backfill(args) if args.execute => {
            let (host, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::backfill_command::backfill(
                &cli,
                args,
                Some((host.as_ref(), &services)),
                destinations,
            )
        }
        Command::Backfill(args) => {
            crate::backfill_command::backfill(&cli, args, None, destinations)
        }
        Command::Package(command) => crate::package_command::package(&cli, command),
        Command::Doctor => {
            let (_, services) =
                cdf_engine::StandaloneExecutionHost::default_services(cdf_memory_budget()?)?;
            crate::doctor_command::doctor(&cli, &services, destinations)
        }
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
        cdf_memory::DEFAULT_SPILL_BUDGET_BYTES,
    )?;
    Ok(resolution.managed_pool_bytes)
}

pub(crate) fn json_cli_error(error: serde_json::Error) -> CliError {
    CliError::mapped(
        CdfError::internal(error.to_string()),
        error_catalog::CLI_JSON,
    )
}
