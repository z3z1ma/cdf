#![doc = "SQLite checkpoint store boundary for cdf."]

mod content_reachability;
mod in_memory;
mod lease;
mod run_ledger;
mod settlement;
mod sqlite;
mod support;

#[cfg(test)]
mod tests;

pub use cdf_kernel::{
    RunEvent, RunEventAppend, RunEventDetails, RunEventKind, RunEventValue, SecretReference,
};
pub use content_reachability::SqliteContentReachabilityStore;
pub use in_memory::InMemoryCheckpointStore;
pub use lease::{InMemoryScopeLeaseStore, SqliteScopeLeaseStore};
pub use run_ledger::{RunLedgerSnapshot, RunRecord, SqliteRunLedger};
pub use settlement::SqlitePromotionSettlementStore;
pub use sqlite::SqliteCheckpointStore;
pub use support::{
    SqliteErrorContext, StateStorePathOwnership, classify_sqlite_error, database_open_path,
    database_path_exists, managed_database_open_path, managed_database_path_exists,
};
