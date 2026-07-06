#![doc = "Command-line surface for firn."]

use std::ffi::OsString;

mod args;
mod commands;
mod context;
mod output;
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
