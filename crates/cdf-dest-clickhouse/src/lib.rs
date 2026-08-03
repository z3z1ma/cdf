#![doc = "Verified ClickHouse destination adapter for cdf."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "destination adapters must propagate recoverable failures"
    )
)]

mod client;
mod error;
mod identifier;
mod mapping;
mod models;
mod package;
mod plan;
mod receipt;
mod runtime;
mod session;
mod sheet;

#[cfg(test)]
mod tests;

pub use models::ClickHouseDestination;
pub use runtime::ClickHouseRuntimeDriver;

pub const CLICKHOUSE_DESTINATION_ID: &str = "clickhouse";
