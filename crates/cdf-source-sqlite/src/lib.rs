#![doc = "Finite, read-only SQLite table source adapter for cdf."]

mod catalog;
mod driver;
mod error;
mod identifier;
mod source;

pub use driver::SqliteSourceDriver;
