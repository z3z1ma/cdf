#![doc = "Subprocess adapter boundary for firn."]

mod command;
mod runner;
#[cfg(test)]
mod tests;

pub use command::{
    CommandSpec, DEFAULT_STDERR_LINE_LIMIT, StderrTrace, StdoutFormat, SubprocessOutput,
    SupervisionOptions,
};
pub use runner::run_stdout_adapter;
