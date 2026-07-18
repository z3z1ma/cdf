use cdf_kernel::CdfError;
use serde_json::json;

use crate::{
    args::{Cli, Command, parse_byte_size},
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
    let budgets = cdf_runtime_budgets(cli)?;
    cdf_engine::StandaloneExecutionHost::default_services_with_spill(
        budgets.managed_pool_bytes,
        budgets.spill_budget_bytes,
    )
    .map_err(Into::into)
}

fn cdf_runtime_budgets(cli: &Cli) -> Result<cdf_memory::MemoryBudgetResolution, CliError> {
    cdf_runtime_budgets_from(
        cli.memory_budget,
        cli.spill_budget,
        env_byte_size("CDF_MEMORY_BUDGET")?,
        env_byte_size("CDF_SPILL_BUDGET")?,
        cgroup_memory_authority(),
    )
}

fn cdf_runtime_budgets_from(
    cli_memory_budget: Option<u64>,
    cli_spill_budget: Option<u64>,
    env_memory_budget: Option<u64>,
    env_spill_budget: Option<u64>,
    cgroup_authority: Option<u64>,
) -> Result<cdf_memory::MemoryBudgetResolution, CliError> {
    let requested_process_bytes = cli_memory_budget.or(env_memory_budget);
    let spill_budget_bytes = cli_spill_budget
        .or(env_spill_budget)
        .unwrap_or(cdf_memory::DEFAULT_SPILL_BUDGET_BYTES);
    let effective_authority = cgroup_authority.unwrap_or_else(|| {
        requested_process_bytes.unwrap_or(cdf_memory::DEFAULT_PROCESS_BUDGET_BYTES)
    });
    let resolution = cdf_memory::resolve_memory_budget(
        requested_process_bytes,
        effective_authority,
        64 * 1024 * 1024,
        spill_budget_bytes,
    )?;
    Ok(resolution)
}

fn env_byte_size(name: &str) -> Result<Option<u64>, CliError> {
    match std::env::var(name) {
        Ok(value) => parse_byte_size(name, &value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(CliError::usage(format!(
            "{name} must be valid UTF-8 when set"
        ))),
    }
}

fn cgroup_memory_authority() -> Option<u64> {
    std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
}

pub(crate) fn json_cli_error(error: serde_json::Error) -> CliError {
    CliError::mapped(
        CdfError::internal(error.to_string()),
        error_catalog::CLI_JSON,
    )
}

#[cfg(test)]
mod runtime_budget_tests {
    use super::cdf_runtime_budgets_from;

    #[test]
    fn cli_budgets_override_environment_without_default_ceiling() {
        let gib = 1024 * 1024 * 1024;
        let resolution =
            cdf_runtime_budgets_from(Some(8 * gib), None, Some(2 * gib), Some(32 * gib), None)
                .unwrap();

        assert_eq!(resolution.process_budget_bytes, 8 * gib);
        assert_eq!(resolution.spill_budget_bytes, 32 * gib);
        assert!(resolution.managed_pool_bytes > 7 * gib / 2);
    }

    #[test]
    fn cgroup_authority_remains_a_real_ceiling() {
        let gib = 1024 * 1024 * 1024;
        let error =
            cdf_runtime_budgets_from(Some(8 * gib), None, None, None, Some(4 * gib)).unwrap_err();

        assert!(
            error
                .message
                .contains("requested process memory budget 8589934592 exceeds effective authority")
        );
    }
}
