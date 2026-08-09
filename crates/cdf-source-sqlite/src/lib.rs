#![doc = "Finite, read-only SQLite table and native-query source adapter for cdf."]

mod catalog;
mod driver;
mod error;
mod identifier;
mod native;
mod source;

pub use driver::SqliteSourceDriver;
