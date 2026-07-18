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
            let (_, services) = default_services(&cli)?;
            crate::add_command::add(&cli, args, &services, destinations)
        }
        Command::Validate(args) => {
            let (_, services) = default_services(&cli)?;
            crate::project_command::validate(&cli, args, &services, destinations)
        }
        Command::Plan(args) => {
            let (_, services) = default_services(&cli)?;
            crate::scan_command::plan_or_explain(&cli, args, "plan", &services, destinations)
        }
        Command::Explain(args) => {
            let (_, services) = default_services(&cli)?;
            crate::scan_command::plan_or_explain(&cli, args, "explain", &services, destinations)
        }
        Command::Run(args) => {
            let (host, services) = default_services(&cli)?;
            crate::run_command::run(&cli, args, host.as_ref(), &services, destinations)
        }
        Command::Preview(args) => {
            let (host, services) = default_services(&cli)?;
            crate::scan_command::preview(&cli, args, host.as_ref(), &services, destinations)
        }
        Command::Sql(args) => crate::sql_command::sql(&cli, args),
        Command::Inspect(args) => crate::inspect_command::inspect(&cli, args, destinations),
        Command::DiffSchema => crate::project_command::diff_schema(&cli),
        Command::Schema(command) => {
            let (_, services) = default_services(&cli)?;
            crate::schema_command::schema(&cli, command, &services, destinations)
        }
        Command::Contract(command) => {
            crate::contract_command::contract(&cli, command, destinations)
        }
        Command::State(command) => {
            let (_, services) = default_services(&cli)?;
            crate::state_command::state(&cli, command, &services, destinations)
        }
        Command::Resume(args) => {
            let (_, services) = default_services(&cli)?;
            crate::resume_command::resume(&cli, args, &services, destinations)
        }
        Command::ReplayPackage(args) => {
            let (_, services) = default_services(&cli)?;
            crate::replay_command::replay_package(&cli, args, &services, destinations)
        }
        Command::Backfill(args) => {
            let (host, services) = default_services(&cli)?;
            crate::backfill_command::backfill(&cli, args, (host.as_ref(), &services), destinations)
        }
        Command::Package(command) => crate::package_command::package(&cli, command),
        Command::Doctor => {
            let (_, services) = default_services(&cli)?;
            crate::doctor_command::doctor(&cli, &services, destinations)
        }
        Command::Status => crate::status_command::status(&cli),
    }
}

fn default_services(
    cli: &Cli,
) -> Result<
    (
        std::sync::Arc<cdf_engine::StandaloneExecutionHost>,
        cdf_runtime::ExecutionServices,
    ),
    CliError,
> {
    let budgets = crate::runtime_budget::resolve(cli)?.resolution;
    cdf_engine::StandaloneExecutionHost::default_services_with_spill(
        budgets.managed_pool_bytes,
        budgets.spill_budget_bytes,
    )
    .map_err(Into::into)
}

pub(crate) fn json_cli_error(error: serde_json::Error) -> CliError {
    CliError::mapped(
        CdfError::internal(error.to_string()),
        error_catalog::CLI_JSON,
    )
}
