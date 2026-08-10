#![doc = "MySQL finite and CDC source adapter for cdf."]

mod add;
mod catalog;
mod driver;
mod error;
mod identifier;
mod native;
mod query;
mod resource;
mod schema;

pub use driver::MySqlSourceDriver;
pub use identifier::{MySqlIdentifier, MySqlTarget};
