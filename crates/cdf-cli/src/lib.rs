#![doc = "Command-line surface for cdf."]

use std::ffi::OsString;

use cdf_cli_core::{args, error_catalog, output, progress, render, suggestions, terminal};

mod add_command;
mod backfill_command;
mod commands;
mod context;
mod contract_command;
mod destination_registry;
#[cfg(test)]
mod destination_registry_test_support;
mod destination_uri;
mod doctor_command;
mod doctor_drift;
mod http_transport;
mod inspect_command;
mod inspect_run_command;
mod package_command;
mod project_command;
mod project_run_resource;
mod replay_command;
mod reports;
mod resume_command;
mod run_command;
mod runtime_budget;
mod scan_command;
mod schema_command;
mod source_registry;
mod sql_command;
mod state_command;
mod status_command;
mod status_freshness;
mod system_sql;

pub fn invoke(args: impl IntoIterator<Item = OsString>) -> cdf_cli_core::output::InvocationResult {
    let args = args.into_iter().collect::<Vec<_>>();
    let json_mode = args.iter().any(|arg| arg == "--json");
    match cdf_cli_core::args::Cli::parse(args) {
        Ok(cli) => match destination_registry::builtin_destination_registry() {
            Ok(registry) => commands::execute(cli, &registry),
            Err(error) => {
                cdf_cli_core::output::InvocationResult::from_error(json_mode, error.into())
            }
        },
        Err(error) => cdf_cli_core::output::InvocationResult::from_error(json_mode, error),
    }
}

pub fn invoke_with_destination_registry(
    args: impl IntoIterator<Item = OsString>,
    registry: &cdf_runtime::DestinationRegistry,
) -> cdf_cli_core::output::InvocationResult {
    let args = args.into_iter().collect::<Vec<_>>();
    let json_mode = args.iter().any(|arg| arg == "--json");
    match cdf_cli_core::args::Cli::parse(args) {
        Ok(cli) => commands::execute(cli, registry),
        Err(error) => cdf_cli_core::output::InvocationResult::from_error(json_mode, error),
    }
}

#[cfg(test)]
mod tests;
