#![doc = "SQLite checkpoint store boundary for cdf."]

mod in_memory;
mod lease;
mod migration;
mod run_ledger;
mod sqlite;
mod support;

#[cfg(test)]
mod tests;

pub use cdf_kernel::{
    RunEvent, RunEventAppend, RunEventDetails, RunEventKind, RunEventValue, SecretReference,
};
pub use in_memory::InMemoryCheckpointStore;
pub use lease::{InMemoryScopeLeaseStore, SqliteScopeLeaseStore};
pub use migration::{
    SqliteStateComponentMigration, SqliteStateMigrationAction, SqliteStateMigrationReport,
    migrate_sqlite_state,
};
pub use run_ledger::{RunLedgerSnapshot, RunRecord, SqliteRunLedger};
pub use sqlite::SqliteCheckpointStore;
