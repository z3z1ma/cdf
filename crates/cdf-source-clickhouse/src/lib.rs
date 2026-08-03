#![doc = "Finite, read-only ClickHouse table source adapter for cdf."]

mod catalog;
mod client;
mod driver;
mod error;
mod execution;
mod identifier;
mod memory;
mod query;
mod resource;
mod types;

pub use driver::ClickHouseSourceDriver;

#[cfg(test)]
mod tests;
