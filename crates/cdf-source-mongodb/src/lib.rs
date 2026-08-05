#![doc = "Finite, read-only MongoDB collection source adapter for cdf."]

mod driver;
mod error;
mod execution;
mod identifier;
mod query;
mod resource;
mod schema;

pub use driver::MongoDbSourceDriver;

#[cfg(test)]
mod tests;
