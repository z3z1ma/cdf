#![doc = "Command-line surface for cdf."]

use std::ffi::OsString;

mod args;
mod commands;
mod context;
mod destination_uri;
mod doctor_drift;
mod http_transport;
mod output;
mod run_command;
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
