#![doc = "SQLite checkpoint store boundary for firn."]

mod in_memory;
mod sqlite;
mod support;

#[cfg(test)]
mod tests;

pub use in_memory::InMemoryCheckpointStore;
pub use sqlite::SqliteCheckpointStore;
