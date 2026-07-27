use std::{path::Path, sync::Mutex};

use cdf_kernel::{
    CdfError, CheckpointId, DestinationId, PackageHash, PartitionId, PlanId, PromotionId,
    PromotionPublicationEvent, ReceiptId, ResourceId, Result, RunEvent, RunEventAppend,
    RunEventDetails, RunEventKind, RunId, ScopeKey,
};
use rusqlite::{
    Connection, ErrorCode, OptionalExtension, Row, Transaction, TransactionBehavior, params,
};
use serde::{Deserialize, Serialize};

use crate::support::{
    SqliteConnectionGuard, SqliteErrorContext, StateStorePathOwnership, database_open_path,
    database_path_exists, decode_private_promotion_publication_row, encode_json,
    ensure_schema_version_table, lock_sqlite_connection, managed_sqlite_open_flags, now_ms,
    prepare_managed_database_path, private_state_decode, read_component_schema_version,
    require_sqlite_tables, sqlite_error, sqlite_table_exists,
    validate_private_promotion_publications, with_sqlite_error_context,
    write_component_schema_version,
};

pub(crate) const RUN_LEDGER_COMPONENT: &str = "run_ledger";
pub(crate) const RUN_LEDGER_SCHEMA_VERSION: i64 = 1;
const RUN_EVENT_SELECT: &str = "SELECT run_id, sequence, timestamp_ms, kind, resource_id, scope_json, partition_id, package_id, package_hash, package_path, checkpoint_id, receipt_id, destination_id, plan_id, details_json FROM cdf_run_events";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: RunId,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLedgerSnapshot {
    pub run: RunRecord,
    pub events: Vec<RunEvent>,
}

pub struct SqliteRunLedger {
    conn: Mutex<Connection>,
    error_context: SqliteErrorContext,
}

impl SqliteRunLedger {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_path_ownership(path, StateStorePathOwnership::CdfManaged)
    }

    pub fn open_with_path_ownership(
        path: impl AsRef<Path>,
        ownership: StateStorePathOwnership,
    ) -> Result<Self> {
        let path = path.as_ref();
        let open_path = prepare_managed_database_path(path, ownership)?;
        let error_context = SqliteErrorContext::ManagedState;
        let conn = with_sqlite_error_context(error_context, || {
            let conn = Connection::open_with_flags(&open_path, managed_sqlite_open_flags(false))
                .map_err(sqlite_error)?;
            initialize_run_schema(&conn)?;
            Ok(conn)
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
            error_context,
        })
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_read_only_with_path_ownership(path, StateStorePathOwnership::CdfManaged)
    }

    pub fn open_read_only_with_path_ownership(
        path: impl AsRef<Path>,
        ownership: StateStorePathOwnership,
    ) -> Result<Self> {
        let path = path.as_ref();
        if !database_path_exists(path, ownership)? {
            return Err(CdfError::data(format!(
                "run ledger state database {} is missing",
                path.display()
            )));
        }
        let open_path = database_open_path(path, ownership)?;
        let error_context = SqliteErrorContext::ManagedReadOnly;
        let conn = with_sqlite_error_context(error_context, || {
            let conn = Connection::open_with_flags(&open_path, managed_sqlite_open_flags(true))
                .map_err(sqlite_error)?;
            validate_run_schema_version(&conn)?;
            Ok(conn)
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
            error_context,
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        let error_context = SqliteErrorContext::EphemeralWorkspace;
        let conn = with_sqlite_error_context(error_context, || {
            let conn = Connection::open_in_memory().map_err(sqlite_error)?;
            initialize_run_schema(&conn)?;
            Ok(conn)
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
            error_context,
        })
    }

    pub fn create_run(&self, run_id: Option<RunId>) -> Result<RunRecord> {
        let mut conn = self.lock_conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let created_at_ms = now_ms()?;
        let run = match run_id {
            Some(run_id) => insert_supplied_run(&tx, run_id, created_at_ms)?,
            None => insert_minted_run(&tx, created_at_ms)?,
        };
        tx.commit().map_err(sqlite_error)?;
        Ok(run)
    }

    pub fn run(&self, run_id: &RunId) -> Result<Option<RunRecord>> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT run_id, created_at_ms FROM cdf_runs WHERE run_id = ?",
            params![run_id.as_str()],
            row_to_run_record,
        )
        .optional()
        .map_err(sqlite_error)
    }

    pub fn append_event(&self, run_id: &RunId, event: RunEventAppend) -> Result<RunEvent> {
        event.details.validate()?;
        let mut conn = self.lock_conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        fetch_run_record_tx(&tx, run_id)?.ok_or_else(|| {
            CdfError::contract(format!("run {} does not exist in the run ledger", run_id))
        })?;
        let sequence = next_sequence_tx(&tx, run_id)?;
        let timestamp_ms = now_ms()?;
        insert_event_tx(&tx, run_id, sequence, timestamp_ms, &event)?;
        let stored = fetch_event_tx(&tx, run_id, sequence)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(stored)
    }

    pub fn events(&self, run_id: &RunId) -> Result<Vec<RunEvent>> {
        let conn = self.lock_conn()?;
        let sql = format!("{RUN_EVENT_SELECT} WHERE run_id = ? ORDER BY sequence");
        let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
        let rows = stmt
            .query_map(params![run_id.as_str()], row_to_run_event)
            .map_err(sqlite_error)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)
    }

    pub fn snapshot(&self, run_id: &RunId) -> Result<Option<RunLedgerSnapshot>> {
        let Some(run) = self.run(run_id)? else {
            return Ok(None);
        };
        Ok(Some(RunLedgerSnapshot {
            run,
            events: self.events(run_id)?,
        }))
    }

    pub fn promotion_publication(
        &self,
        promotion_id: &PromotionId,
    ) -> Result<Option<PromotionPublicationEvent>> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT promotion_id, published_at_ms, event_json FROM cdf_promotion_publications WHERE promotion_id = ?",
            params![promotion_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
        .map(|(promotion_id, published_at_ms, json)| {
            decode_private_promotion_publication_row(promotion_id, published_at_ms, json)
        })
        .transpose()
    }

    pub fn publish_promotion(
        &self,
        event: PromotionPublicationEvent,
    ) -> Result<PromotionPublicationEvent> {
        event.validate()?;
        let mut conn = self.lock_conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let existing = tx
            .query_row(
                "SELECT promotion_id, published_at_ms, event_json FROM cdf_promotion_publications WHERE promotion_id = ?",
                params![event.promotion_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(sqlite_error)?
            .map(|(promotion_id, published_at_ms, json)| {
                decode_private_promotion_publication_row(promotion_id, published_at_ms, json)
            })
            .transpose()?;
        if let Some(existing) = existing {
            if !existing.same_authority(&event) {
                return Err(CdfError::contract(format!(
                    "promotion publication {} conflicts with existing ledger authority",
                    event.promotion_id
                )));
            }
            return Ok(existing);
        }
        let json = encode_json(&event)?;
        tx.execute(
            "INSERT INTO cdf_promotion_publications (promotion_id, published_at_ms, event_json) VALUES (?, ?, ?)",
            params![event.promotion_id.as_str(), event.published_at_ms, json],
        )
        .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(event)
    }

    fn lock_conn(&self) -> Result<SqliteConnectionGuard<'_>> {
        lock_sqlite_connection(&self.conn, self.error_context)
    }

    /// Validates every CDF-owned run-ledger row before a raw or diagnostic reader bypasses the
    /// typed ledger API.
    ///
    /// Ordinary store opens intentionally validate only schema authority. Typed reads validate
    /// each row they consume so run startup remains independent of total historical state.
    pub fn validate_integrity(&self) -> Result<()> {
        let conn = self.lock_conn()?;
        validate_private_run_ledger_rows(&conn)
    }
}

#[cfg(test)]
impl SqliteRunLedger {
    pub(crate) fn execute_for_test<P>(&self, sql: &str, params: P) -> rusqlite::Result<usize>
    where
        P: rusqlite::Params,
    {
        self.conn.lock().unwrap().execute(sql, params)
    }

    pub(crate) fn query_row_for_test<T, P, F>(
        &self,
        sql: &str,
        params: P,
        f: F,
    ) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.conn.lock().unwrap().query_row(sql, params, f)
    }
}

pub(crate) fn initialize_run_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        ",
    )
    .map_err(sqlite_error)?;
    let existing_version = read_run_schema_version(conn)?;
    match existing_version {
        Some(RUN_LEDGER_SCHEMA_VERSION) => validate_run_schema_structure(conn)?,
        Some(version) => return Err(unsupported_run_schema_version(version)),
        None if sqlite_table_exists(conn, "cdf_runs")?
            || sqlite_table_exists(conn, "cdf_run_events")? =>
        {
            return Err(CdfError::internal(format!(
                "run ledger SQLite schema is unversioned; expected current version {RUN_LEDGER_SCHEMA_VERSION}"
            )));
        }
        None => {}
    }
    ensure_schema_version_table(conn)?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS cdf_runs (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL UNIQUE,
            created_at_ms INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cdf_run_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL REFERENCES cdf_runs(run_id) ON DELETE RESTRICT,
            sequence INTEGER NOT NULL CHECK (sequence > 0),
            timestamp_ms INTEGER NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN (
                'run_started',
                'plan_recorded',
                'package_started',
                'package_segment_recorded',
                'package_finalized',
                'destination_commit_started',
                'destination_segment_acknowledged',
                'destination_receipt_recorded',
                'checkpoint_proposed',
                'checkpoint_committed',
                'package_status_updated',
                'run_succeeded',
                'run_failed',
                'run_resumed',
                'replay_recorded',
                'validation_depth_transition_recorded',
                'source_retry_recorded',
                'phase_measured'
            )),
            resource_id TEXT,
            scope_json TEXT,
            partition_id TEXT,
            package_id TEXT,
            package_hash TEXT,
            package_path TEXT,
            checkpoint_id TEXT,
            receipt_id TEXT,
            destination_id TEXT,
            plan_id TEXT,
            details_json TEXT NOT NULL,
            UNIQUE(run_id, sequence)
        );

        CREATE INDEX IF NOT EXISTS cdf_run_events_run_sequence
            ON cdf_run_events (run_id, sequence);

        CREATE INDEX IF NOT EXISTS cdf_run_events_checkpoint
            ON cdf_run_events (checkpoint_id)
            WHERE checkpoint_id IS NOT NULL;

        CREATE INDEX IF NOT EXISTS cdf_run_events_receipt
            ON cdf_run_events (receipt_id)
            WHERE receipt_id IS NOT NULL;

        CREATE INDEX IF NOT EXISTS cdf_run_events_package_hash
            ON cdf_run_events (package_hash)
            WHERE package_hash IS NOT NULL;
        ",
    )
    .map_err(sqlite_error)?;

    create_run_event_indexes(conn)?;
    create_promotion_publication_table(conn)?;

    conn.execute_batch(
        "

        CREATE TRIGGER IF NOT EXISTS cdf_runs_no_update
        BEFORE UPDATE ON cdf_runs
        BEGIN
            SELECT RAISE(ABORT, 'cdf_runs is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS cdf_runs_no_delete
        BEFORE DELETE ON cdf_runs
        BEGIN
            SELECT RAISE(ABORT, 'cdf_runs is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS cdf_run_events_no_update
        BEFORE UPDATE ON cdf_run_events
        BEGIN
            SELECT RAISE(ABORT, 'cdf_run_events is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS cdf_run_events_no_delete
        BEFORE DELETE ON cdf_run_events
        BEGIN
            SELECT RAISE(ABORT, 'cdf_run_events is append-only');
        END;
        ",
    )
    .map_err(sqlite_error)?;

    write_component_schema_version(conn, RUN_LEDGER_COMPONENT, RUN_LEDGER_SCHEMA_VERSION)?;

    Ok(())
}

fn validate_run_schema_version(conn: &Connection) -> Result<()> {
    let existing_version = read_run_schema_version(conn)?;
    match existing_version {
        Some(RUN_LEDGER_SCHEMA_VERSION) => validate_run_schema_structure(conn),
        Some(version) => Err(unsupported_run_schema_version(version)),
        None => Err(CdfError::internal(format!(
            "run ledger SQLite schema version is missing; expected {RUN_LEDGER_SCHEMA_VERSION}"
        ))),
    }
}

fn validate_run_schema_structure(conn: &Connection) -> Result<()> {
    require_sqlite_tables(
        conn,
        "run ledger",
        &["cdf_runs", "cdf_run_events", "cdf_promotion_publications"],
    )
}

fn validate_private_run_ledger_rows(conn: &Connection) -> Result<()> {
    let mut runs = conn
        .prepare("SELECT run_id, created_at_ms FROM cdf_runs")
        .map_err(sqlite_error)?;
    let run_rows = runs
        .query_map([], row_to_run_record)
        .map_err(sqlite_error)?;
    for run in run_rows {
        run.map_err(sqlite_error)?;
    }

    let mut events = conn.prepare(RUN_EVENT_SELECT).map_err(sqlite_error)?;
    let event_rows = events
        .query_map([], row_to_run_event)
        .map_err(sqlite_error)?;
    for event in event_rows {
        event.map_err(sqlite_error)?;
    }

    validate_private_promotion_publications(conn)
}

fn read_run_schema_version(conn: &Connection) -> Result<Option<i64>> {
    read_component_schema_version(conn, RUN_LEDGER_COMPONENT)
}

fn unsupported_run_schema_version(version: i64) -> CdfError {
    CdfError::internal(format!(
        "unsupported run ledger SQLite schema version {version}"
    ))
}

fn create_run_event_indexes(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS cdf_run_events_run_sequence
            ON cdf_run_events (run_id, sequence);

        CREATE INDEX IF NOT EXISTS cdf_run_events_checkpoint
            ON cdf_run_events (checkpoint_id)
            WHERE checkpoint_id IS NOT NULL;

        CREATE INDEX IF NOT EXISTS cdf_run_events_receipt
            ON cdf_run_events (receipt_id)
            WHERE receipt_id IS NOT NULL;

        CREATE INDEX IF NOT EXISTS cdf_run_events_package_hash
            ON cdf_run_events (package_hash)
            WHERE package_hash IS NOT NULL;
        ",
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn create_promotion_publication_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS cdf_promotion_publications (
            promotion_id TEXT PRIMARY KEY,
            published_at_ms INTEGER NOT NULL,
            event_json TEXT NOT NULL
        );

        CREATE TRIGGER IF NOT EXISTS cdf_promotion_publications_no_update
        BEFORE UPDATE ON cdf_promotion_publications
        BEGIN
            SELECT RAISE(ABORT, 'cdf_promotion_publications is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS cdf_promotion_publications_no_delete
        BEFORE DELETE ON cdf_promotion_publications
        BEGIN
            SELECT RAISE(ABORT, 'cdf_promotion_publications is append-only');
        END;
        ",
    )
    .map_err(sqlite_error)
}

fn insert_supplied_run(
    tx: &Transaction<'_>,
    run_id: RunId,
    created_at_ms: i64,
) -> Result<RunRecord> {
    match insert_run_tx(tx, &run_id, created_at_ms) {
        Ok(()) => Ok(RunRecord {
            run_id,
            created_at_ms,
        }),
        Err(error) if is_constraint_violation(&error) => Err(CdfError::contract(format!(
            "run {} already exists in the run ledger",
            run_id
        ))),
        Err(error) => Err(sqlite_error(error)),
    }
}

fn insert_minted_run(tx: &Transaction<'_>, created_at_ms: i64) -> Result<RunRecord> {
    for _ in 0..16 {
        let value: String = tx
            .query_row("SELECT 'run-' || lower(hex(randomblob(16)))", [], |row| {
                row.get(0)
            })
            .map_err(sqlite_error)?;
        let run_id = RunId::new(value)?;
        match insert_run_tx(tx, &run_id, created_at_ms) {
            Ok(()) => {
                return Ok(RunRecord {
                    run_id,
                    created_at_ms,
                });
            }
            Err(error) if is_constraint_violation(&error) => continue,
            Err(error) => return Err(sqlite_error(error)),
        }
    }
    Err(CdfError::internal("failed to mint a unique run id"))
}

fn insert_run_tx(tx: &Transaction<'_>, run_id: &RunId, created_at_ms: i64) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO cdf_runs (run_id, created_at_ms) VALUES (?, ?)",
        params![run_id.as_str(), created_at_ms],
    )?;
    Ok(())
}

fn fetch_run_record_tx(tx: &Transaction<'_>, run_id: &RunId) -> Result<Option<RunRecord>> {
    tx.query_row(
        "SELECT run_id, created_at_ms FROM cdf_runs WHERE run_id = ?",
        params![run_id.as_str()],
        row_to_run_record,
    )
    .optional()
    .map_err(sqlite_error)
}

fn next_sequence_tx(tx: &Transaction<'_>, run_id: &RunId) -> Result<u64> {
    let current: Option<i64> = tx
        .query_row(
            "SELECT MAX(sequence) FROM cdf_run_events WHERE run_id = ?",
            params![run_id.as_str()],
            |row| row.get(0),
        )
        .map_err(sqlite_error)?;
    let next = current
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| CdfError::internal("CDF-managed run event sequence overflow"))?;
    u64::try_from(next).map_err(|error| CdfError::internal(error.to_string()))
}

fn insert_event_tx(
    tx: &Transaction<'_>,
    run_id: &RunId,
    sequence: u64,
    timestamp_ms: i64,
    event: &RunEventAppend,
) -> Result<()> {
    tx.execute(
        "INSERT INTO cdf_run_events (run_id, sequence, timestamp_ms, kind, resource_id, scope_json, partition_id, package_id, package_hash, package_path, checkpoint_id, receipt_id, destination_id, plan_id, details_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            run_id.as_str(),
            i64::try_from(sequence).map_err(|error| CdfError::internal(error.to_string()))?,
            timestamp_ms,
            event.kind.as_str(),
            event.resource_id.as_ref().map(ResourceId::as_str),
            event.scope.as_ref().map(encode_json).transpose()?,
            event.partition_id.as_ref().map(PartitionId::as_str),
            event.package_id.as_deref(),
            event.package_hash.as_ref().map(PackageHash::as_str),
            event.package_path.as_deref(),
            event.checkpoint_id.as_ref().map(CheckpointId::as_str),
            event.receipt_id.as_ref().map(ReceiptId::as_str),
            event.destination_id.as_ref().map(DestinationId::as_str),
            event.plan_id.as_ref().map(PlanId::as_str),
            encode_json(&event.details)?,
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn fetch_event_tx(tx: &Transaction<'_>, run_id: &RunId, sequence: u64) -> Result<RunEvent> {
    let sql = format!("{RUN_EVENT_SELECT} WHERE run_id = ? AND sequence = ?");
    tx.query_row(
        &sql,
        params![
            run_id.as_str(),
            i64::try_from(sequence).map_err(|error| CdfError::internal(error.to_string()))?,
        ],
        row_to_run_event,
    )
    .map_err(sqlite_error)
}

fn row_to_run_record(row: &Row<'_>) -> rusqlite::Result<RunRecord> {
    row_to_run_record_result(row).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn row_to_run_record_result(row: &Row<'_>) -> Result<RunRecord> {
    private_state_decode(
        "decode CDF-managed run ledger row",
        (|| {
            let created_at_ms = row_get(row, "created_at_ms")?;
            if created_at_ms < 0 {
                return Err(CdfError::internal(
                    "CDF-managed run creation timestamp is negative",
                ));
            }
            Ok(RunRecord {
                run_id: RunId::new(row_get::<String>(row, "run_id")?)?,
                created_at_ms,
            })
        })(),
    )
}

fn row_to_run_event(row: &Row<'_>) -> rusqlite::Result<RunEvent> {
    row_to_run_event_result(row).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn row_to_run_event_result(row: &Row<'_>) -> Result<RunEvent> {
    private_state_decode(
        "decode CDF-managed run event row",
        (|| {
            let sequence = row_get::<i64>(row, "sequence")?;
            if sequence < 1 {
                return Err(CdfError::internal(
                    "CDF-managed run event sequence is not positive",
                ));
            }
            let timestamp_ms = row_get(row, "timestamp_ms")?;
            if timestamp_ms < 0 {
                return Err(CdfError::internal(
                    "CDF-managed run event timestamp is negative",
                ));
            }
            let scope = row_get::<Option<String>>(row, "scope_json")?
                .map(|json| serde_json::from_str::<ScopeKey>(&json))
                .transpose()
                .map_err(|error| {
                    CdfError::internal(format!("decode CDF-managed run event scope JSON: {error}"))
                })?;
            let details =
                serde_json::from_str::<RunEventDetails>(&row_get::<String>(row, "details_json")?)
                    .map_err(|error| {
                    CdfError::internal(format!(
                        "decode CDF-managed run event details JSON: {error}"
                    ))
                })?;
            details.validate()?;

            Ok(RunEvent {
                run_id: RunId::new(row_get::<String>(row, "run_id")?)?,
                sequence: u64::try_from(sequence)
                    .map_err(|error| CdfError::internal(error.to_string()))?,
                timestamp_ms,
                kind: RunEventKind::parse(&row_get::<String>(row, "kind")?)?,
                resource_id: row_get::<Option<String>>(row, "resource_id")?
                    .map(ResourceId::new)
                    .transpose()?,
                scope,
                partition_id: row_get::<Option<String>>(row, "partition_id")?
                    .map(PartitionId::new)
                    .transpose()?,
                package_id: row_get(row, "package_id")?,
                package_hash: row_get::<Option<String>>(row, "package_hash")?
                    .map(PackageHash::new)
                    .transpose()?,
                package_path: row_get(row, "package_path")?,
                checkpoint_id: row_get::<Option<String>>(row, "checkpoint_id")?
                    .map(CheckpointId::new)
                    .transpose()?,
                receipt_id: row_get::<Option<String>>(row, "receipt_id")?
                    .map(ReceiptId::new)
                    .transpose()?,
                destination_id: row_get::<Option<String>>(row, "destination_id")?
                    .map(DestinationId::new)
                    .transpose()?,
                plan_id: row_get::<Option<String>>(row, "plan_id")?
                    .map(PlanId::new)
                    .transpose()?,
                details,
            })
        })(),
    )
}

fn is_constraint_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation
    )
}

fn row_get<T: rusqlite::types::FromSql>(row: &Row<'_>, column: &str) -> Result<T> {
    row.get(column).map_err(sqlite_error)
}

#[cfg(test)]
mod private_row_tests {
    use cdf_kernel::ErrorKind;

    use super::*;

    #[test]
    fn malformed_private_run_identifier_is_internal() {
        let conn = Connection::open_in_memory().unwrap();
        let error = conn
            .query_row(
                "SELECT '' AS run_id, 1 AS created_at_ms",
                [],
                row_to_run_record,
            )
            .unwrap_err();

        let classified = sqlite_error(error);

        assert_eq!(classified.kind, ErrorKind::Internal);
        assert!(classified.message.contains("run ledger row"));
    }
}
