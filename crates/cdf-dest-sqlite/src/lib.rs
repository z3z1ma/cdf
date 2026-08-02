#![doc = "SQLite destination adapter for cdf."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "destination adapters must propagate recoverable failures"
    )
)]

mod error;
mod identifier;
mod mapping;
mod mirrors;
mod models;
mod package;
mod plan;
mod receipts;
mod runtime;
mod sheet;
mod transaction;

#[cfg(test)]
mod tests;

pub use models::SqliteDestination;
pub use runtime::SqliteRuntimeDriver;

pub const SQLITE_DESTINATION_ID: &str = "sqlite";
