#![doc = "Command-line surface for cdf."]

use std::ffi::OsString;

mod args;
mod backfill_command;
#[cfg(feature = "cli-artifacts")]
pub mod cli_artifacts;
mod commands;
mod context;
mod contract_command;
mod destination_uri;
mod doctor_command;
mod doctor_drift;
mod http_transport;
mod inspect_command;
mod inspect_run_command;
mod output;
mod package_command;
mod project_command;
mod project_run_resource;
mod render;
mod replay_command;
mod reports;
mod resume_command;
mod run_command;
mod scan_command;
mod sql_command;
mod state_command;
mod status_command;
mod status_freshness;
mod system_sql;

pub use output::InvocationResult;

pub fn invoke(args: impl IntoIterator<Item = OsString>) -> InvocationResult {
    let args = args.into_iter().collect::<Vec<_>>();
    let json_mode = args.iter().any(|arg| arg == "--json");
    match args::Cli::parse(args) {
        Ok(cli) => commands::execute(cli),
        Err(error) => output::InvocationResult::from_error(json_mode, error),
    }
}

#[cfg(test)]
mod tests;
