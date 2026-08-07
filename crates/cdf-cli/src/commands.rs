mod render;

use cdf_kernel::CdfError;
use serde::Serialize;

use crate::{
    args::{Cli, Command},
    error_catalog,
    output::{CliError, CommandOutput, InvocationResult},
    progress::ProgressDelivery,
    render::RenderConfig,
    terminal::OutputChannel,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn execute(
    cli: Cli,
    destinations: &cdf_runtime::DestinationRegistry,
    progress_delivery: ProgressDelivery,
) -> InvocationResult {
    let json_mode = cli.json;
    let stdout_config = RenderConfig::detect(&cli.terminal, OutputChannel::Stdout);
    let stderr_config = RenderConfig::detect(&cli.terminal, OutputChannel::Stderr);
    match dispatch(cli, destinations, progress_delivery) {
        Ok(output) => InvocationResult::from_output_with_configs(
            json_mode,
            &stdout_config,
            &stderr_config,
            output,
        ),
        Err(error) => InvocationResult::from_error_with_config(json_mode, &stderr_config, error),
    }
}

pub fn execute_without_destination_registry(cli: Cli) -> InvocationResult {
    let json_mode = cli.json;
    let stdout_config = RenderConfig::detect(&cli.terminal, OutputChannel::Stdout);
    let stderr_config = RenderConfig::detect(&cli.terminal, OutputChannel::Stderr);
    let result = match cli.command.clone() {
        Command::Sql(args) => crate::sql_command::sql(&cli, args),
        _ => Err(CdfError::internal(
            "registry-free dispatch received a command that requires destination composition",
        )
        .into()),
    };
    match result {
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
    progress_delivery: ProgressDelivery,
) -> Result<CommandOutput, CliError> {
    let command = cli.command.clone();
    match command {
        Command::Help(help) => {
            let report = HelpReport { help };
            CommandOutput::rendered("help", render::help_document(&report), report)
        }
        Command::Version => {
            let report = VersionReport { version: VERSION };
            CommandOutput::rendered("version", render::version_document(&report), report)
        }
        Command::Init(args) => crate::project_command::init(args),
        Command::Add(args) => {
            let (_, services) = default_services(&cli)?;
            crate::add_command::add(&cli, args, &services, destinations)
        }
        Command::Discover(command) => {
            let (_, services) = default_services(&cli)?;
            crate::discover_command::discover(&cli, command, &services, destinations)
        }
        Command::Compile(args) => crate::compile_command::compile(&cli, args, destinations),
        Command::Sql(_) => {
            Err(CdfError::internal("sql must use registry-free command dispatch").into())
        }
        Command::Validate(args) => crate::project_command::validate(&cli, args),
        Command::Plan(args) => {
            let (_, services) = default_services(&cli)?;
            crate::scan_command::plan(&cli, args, &services, destinations)
        }
        Command::Explain(args) => {
            let (_, services) = default_services(&cli)?;
            crate::scan_command::plan_or_explain(&cli, args, "explain", &services, destinations)
        }
        Command::Run(args) => {
            let (host, services) = default_services(&cli)?;
            crate::run_command::run(
                &cli,
                args,
                host.as_ref(),
                &services,
                destinations,
                progress_delivery,
            )
        }
        Command::Preview(args) => {
            let (host, services) = default_services(&cli)?;
            crate::scan_command::preview(&cli, args, host.as_ref(), &services, destinations)
        }
        Command::Inspect(args) => crate::inspect_command::inspect(&cli, args, destinations),
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
        Command::Backfill(args) => {
            let (host, services) = default_services(&cli)?;
            crate::backfill_command::backfill(
                &cli,
                args,
                (host.as_ref(), &services),
                destinations,
                progress_delivery,
            )
        }
        Command::Package(command) => crate::package_command::package(&cli, command, destinations),
        Command::Doctor(scope) => {
            let (_, services) = default_services(&cli)?;
            crate::doctor_command::doctor(&cli, scope, &services, destinations)
        }
        Command::Status => crate::status_command::status(&cli),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct HelpReport {
    help: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct VersionReport {
    version: &'static str,
}

pub(crate) fn default_services(
    cli: &Cli,
) -> Result<
    (
        std::sync::Arc<cdf_engine::StandaloneExecutionHost>,
        cdf_runtime::ExecutionServices,
    ),
    CliError,
> {
    let budgets = crate::runtime_budget::resolve(cli)?.resolution;
    cdf_engine::StandaloneExecutionHost::default_services_with_budget_resolution(budgets)
        .map_err(Into::into)
}

pub(crate) fn json_cli_error(error: serde_json::Error) -> CliError {
    CliError::mapped(
        CdfError::internal(error.to_string()),
        error_catalog::CLI_JSON,
    )
}
