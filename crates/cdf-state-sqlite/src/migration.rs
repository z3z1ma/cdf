use std::path::Path;

use cdf_kernel::{CdfError, Result};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

use crate::{
    run_ledger::{RUN_LEDGER_COMPONENT, RUN_LEDGER_SCHEMA_VERSION, SqliteRunLedger},
    sqlite::{CHECKPOINT_STORE_COMPONENT, CHECKPOINT_STORE_SCHEMA_VERSION, SqliteCheckpointStore},
    support::{read_component_schema_version, sqlite_error},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SqliteStateMigrationReport {
    pub components: Vec<SqliteStateComponentMigration>,
}

impl SqliteStateMigrationReport {
    pub fn applied_count(&self) -> usize {
        self.components
            .iter()
            .filter(|component| component.applied)
            .count()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SqliteStateComponentMigration {
    pub component: &'static str,
    pub before_version: Option<i64>,
    pub after_version: i64,
    pub target_version: i64,
    pub applied: bool,
    pub action: SqliteStateMigrationAction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SqliteStateMigrationAction {
    Current,
    Initialized,
    Migrated,
}

pub fn migrate_sqlite_state(path: impl AsRef<Path>) -> Result<SqliteStateMigrationReport> {
    let path = path.as_ref();
    let before = read_state_versions(path)?;
    SqliteCheckpointStore::open(path)?;
    SqliteRunLedger::open(path)?;
    let after = read_state_versions(path)?;

    Ok(SqliteStateMigrationReport {
        components: vec![
            component_report(
                CHECKPOINT_STORE_COMPONENT,
                before.checkpoint_store,
                after.checkpoint_store,
                CHECKPOINT_STORE_SCHEMA_VERSION,
            )?,
            component_report(
                RUN_LEDGER_COMPONENT,
                before.run_ledger,
                after.run_ledger,
                RUN_LEDGER_SCHEMA_VERSION,
            )?,
        ],
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StateVersions {
    checkpoint_store: Option<i64>,
    run_ledger: Option<i64>,
}

fn read_state_versions(path: &Path) -> Result<StateVersions> {
    if !path.exists() {
        return Ok(StateVersions {
            checkpoint_store: None,
            run_ledger: None,
        });
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(sqlite_error)?;
    Ok(StateVersions {
        checkpoint_store: read_component_schema_version(&conn, CHECKPOINT_STORE_COMPONENT)?,
        run_ledger: read_component_schema_version(&conn, RUN_LEDGER_COMPONENT)?,
    })
}

fn component_report(
    component: &'static str,
    before_version: Option<i64>,
    after_version: Option<i64>,
    target_version: i64,
) -> Result<SqliteStateComponentMigration> {
    let after_version = after_version.ok_or_else(|| {
        CdfError::internal(format!(
            "SQLite state migration did not record {component} schema version"
        ))
    })?;
    if after_version != target_version {
        return Err(CdfError::internal(format!(
            "SQLite state migration left {component} at version {after_version}; expected {target_version}"
        )));
    }
    let action = match before_version {
        None => SqliteStateMigrationAction::Initialized,
        Some(before) if before != after_version => SqliteStateMigrationAction::Migrated,
        Some(_) => SqliteStateMigrationAction::Current,
    };
    Ok(SqliteStateComponentMigration {
        component,
        before_version,
        after_version,
        target_version,
        applied: action != SqliteStateMigrationAction::Current,
        action,
    })
}
