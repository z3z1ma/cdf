use std::{
    collections::BTreeMap,
    fmt,
    path::Path,
    sync::{Mutex, MutexGuard},
};

use cdf_kernel::{
    CdfError, CheckpointId, DestinationId, PackageHash, PartitionId, PlanId, ReceiptId, ResourceId,
    Result, RunId, ScopeKey,
};
use rusqlite::{
    Connection, ErrorCode, OptionalExtension, Row, Transaction, TransactionBehavior, params,
};
use serde::{Deserialize, Serialize};

use crate::support::{encode_json, lock_error, now_ms, sqlite_error};

const RUN_LEDGER_SCHEMA_VERSION: i64 = 1;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEvent {
    pub run_id: RunId,
    pub sequence: u64,
    pub timestamp_ms: i64,
    pub kind: RunEventKind,
    pub resource_id: Option<ResourceId>,
    pub scope: Option<ScopeKey>,
    pub partition_id: Option<PartitionId>,
    pub package_id: Option<String>,
    pub package_hash: Option<PackageHash>,
    pub package_path: Option<String>,
    pub checkpoint_id: Option<CheckpointId>,
    pub receipt_id: Option<ReceiptId>,
    pub destination_id: Option<DestinationId>,
    pub plan_id: Option<PlanId>,
    pub details: RunEventDetails,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEventAppend {
    pub kind: RunEventKind,
    pub resource_id: Option<ResourceId>,
    pub scope: Option<ScopeKey>,
    pub partition_id: Option<PartitionId>,
    pub package_id: Option<String>,
    pub package_hash: Option<PackageHash>,
    pub package_path: Option<String>,
    pub checkpoint_id: Option<CheckpointId>,
    pub receipt_id: Option<ReceiptId>,
    pub destination_id: Option<DestinationId>,
    pub plan_id: Option<PlanId>,
    pub details: RunEventDetails,
}

impl RunEventAppend {
    pub fn new(kind: RunEventKind) -> Self {
        Self {
            kind,
            resource_id: None,
            scope: None,
            partition_id: None,
            package_id: None,
            package_hash: None,
            package_path: None,
            checkpoint_id: None,
            receipt_id: None,
            destination_id: None,
            plan_id: None,
            details: RunEventDetails::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunEventKind {
    RunStarted,
    PlanRecorded,
    PackageStarted,
    PackageFinalized,
    DestinationCommitStarted,
    DestinationReceiptRecorded,
    CheckpointProposed,
    CheckpointCommitted,
    PackageStatusUpdated,
    RunSucceeded,
    RunFailed,
    RunResumed,
    ReplayRecorded,
}

impl RunEventKind {
    pub const ALL: [Self; 13] = [
        Self::RunStarted,
        Self::PlanRecorded,
        Self::PackageStarted,
        Self::PackageFinalized,
        Self::DestinationCommitStarted,
        Self::DestinationReceiptRecorded,
        Self::CheckpointProposed,
        Self::CheckpointCommitted,
        Self::PackageStatusUpdated,
        Self::RunSucceeded,
        Self::RunFailed,
        Self::RunResumed,
        Self::ReplayRecorded,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RunStarted => "run_started",
            Self::PlanRecorded => "plan_recorded",
            Self::PackageStarted => "package_started",
            Self::PackageFinalized => "package_finalized",
            Self::DestinationCommitStarted => "destination_commit_started",
            Self::DestinationReceiptRecorded => "destination_receipt_recorded",
            Self::CheckpointProposed => "checkpoint_proposed",
            Self::CheckpointCommitted => "checkpoint_committed",
            Self::PackageStatusUpdated => "package_status_updated",
            Self::RunSucceeded => "run_succeeded",
            Self::RunFailed => "run_failed",
            Self::RunResumed => "run_resumed",
            Self::ReplayRecorded => "replay_recorded",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "run_started" => Ok(Self::RunStarted),
            "plan_recorded" => Ok(Self::PlanRecorded),
            "package_started" => Ok(Self::PackageStarted),
            "package_finalized" => Ok(Self::PackageFinalized),
            "destination_commit_started" => Ok(Self::DestinationCommitStarted),
            "destination_receipt_recorded" => Ok(Self::DestinationReceiptRecorded),
            "checkpoint_proposed" => Ok(Self::CheckpointProposed),
            "checkpoint_committed" => Ok(Self::CheckpointCommitted),
            "package_status_updated" => Ok(Self::PackageStatusUpdated),
            "run_succeeded" => Ok(Self::RunSucceeded),
            "run_failed" => Ok(Self::RunFailed),
            "run_resumed" => Ok(Self::RunResumed),
            "replay_recorded" => Ok(Self::ReplayRecorded),
            other => Err(CdfError::data(format!("unknown run event kind {other:?}"))),
        }
    }
}

impl fmt::Display for RunEventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEventDetails {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, RunEventValue>,
}

impl RunEventDetails {
    pub fn new(attributes: impl IntoIterator<Item = (impl Into<String>, RunEventValue)>) -> Self {
        Self {
            attributes: attributes
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        }
    }

    fn validate(&self) -> Result<()> {
        for (key, value) in &self.attributes {
            validate_event_value(key, value)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum RunEventValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    String(String),
    SecretRef(SecretReference),
    List(Vec<RunEventValue>),
    Object(BTreeMap<String, RunEventValue>),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SecretReference(String);

impl SecretReference {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let rest = value
            .strip_prefix("secret://")
            .ok_or_else(|| CdfError::contract("secret reference must use the secret:// scheme"))?;
        let (provider, key) = rest
            .split_once('/')
            .ok_or_else(|| CdfError::contract("secret reference must use secret://provider/key"))?;
        if provider.trim().is_empty() || key.trim().is_empty() {
            return Err(CdfError::contract(
                "secret reference must use secret://provider/key",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for SecretReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for SecretReference {
    type Error = CdfError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<SecretReference> for String {
    fn from(value: SecretReference) -> Self {
        value.0
    }
}

pub struct SqliteRunLedger {
    conn: Mutex<Connection>,
}

impl SqliteRunLedger {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path.as_ref()).map_err(sqlite_error)?;
        initialize_run_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(sqlite_error)?;
        initialize_run_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
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

    fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(lock_error)
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

fn initialize_run_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;

        CREATE TABLE IF NOT EXISTS cdf_sqlite_schema_migrations (
            component TEXT PRIMARY KEY,
            version INTEGER NOT NULL,
            applied_at_ms INTEGER NOT NULL
        );
        ",
    )
    .map_err(sqlite_error)?;

    let existing_version = conn
        .query_row(
            "SELECT version FROM cdf_sqlite_schema_migrations WHERE component = 'run_ledger'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    if let Some(version) = existing_version
        && version != RUN_LEDGER_SCHEMA_VERSION
    {
        return Err(CdfError::internal(format!(
            "unsupported run ledger SQLite schema version {version}"
        )));
    }

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
                'package_finalized',
                'destination_commit_started',
                'destination_receipt_recorded',
                'checkpoint_proposed',
                'checkpoint_committed',
                'package_status_updated',
                'run_succeeded',
                'run_failed',
                'run_resumed',
                'replay_recorded'
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

    if existing_version.is_none() {
        conn.execute(
            "INSERT INTO cdf_sqlite_schema_migrations (component, version, applied_at_ms) VALUES ('run_ledger', ?, ?)",
            params![RUN_LEDGER_SCHEMA_VERSION, now_ms()?],
        )
        .map_err(sqlite_error)?;
    }

    Ok(())
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
    let next = current.unwrap_or(0) + 1;
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
    Ok(RunRecord {
        run_id: RunId::new(row_get::<String>(row, "run_id")?)?,
        created_at_ms: row_get(row, "created_at_ms")?,
    })
}

fn row_to_run_event(row: &Row<'_>) -> rusqlite::Result<RunEvent> {
    row_to_run_event_result(row).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn row_to_run_event_result(row: &Row<'_>) -> Result<RunEvent> {
    let sequence = row_get::<i64>(row, "sequence")?;
    if sequence < 1 {
        return Err(CdfError::data("run event sequence must be positive"));
    }
    let scope = row_get::<Option<String>>(row, "scope_json")?
        .map(|json| serde_json::from_str::<ScopeKey>(&json))
        .transpose()
        .map_err(|error| CdfError::data(error.to_string()))?;
    let details = serde_json::from_str::<RunEventDetails>(&row_get::<String>(row, "details_json")?)
        .map_err(|error| CdfError::data(error.to_string()))?;
    details.validate()?;

    Ok(RunEvent {
        run_id: RunId::new(row_get::<String>(row, "run_id")?)?,
        sequence: u64::try_from(sequence).map_err(|error| CdfError::internal(error.to_string()))?,
        timestamp_ms: row_get(row, "timestamp_ms")?,
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
}

fn validate_event_value(key: &str, value: &RunEventValue) -> Result<()> {
    if key.trim().is_empty() {
        return Err(CdfError::contract("run event detail keys cannot be empty"));
    }
    if is_sensitive_key(key) && !value_contains_only_secret_refs(value) {
        return Err(CdfError::contract(format!(
            "run event detail {key:?} must use secret references"
        )));
    }
    match value {
        RunEventValue::String(value) => {
            if value.contains("secret://") {
                return Err(CdfError::contract(
                    "run event detail strings must use SecretRef for secret references",
                ));
            }
            Ok(())
        }
        RunEventValue::List(values) => {
            for value in values {
                validate_event_value(key, value)?;
            }
            Ok(())
        }
        RunEventValue::Object(values) => {
            for (nested_key, value) in values {
                validate_event_value(nested_key, value)?;
            }
            Ok(())
        }
        RunEventValue::Bool(_)
        | RunEventValue::I64(_)
        | RunEventValue::U64(_)
        | RunEventValue::SecretRef(_) => Ok(()),
    }
}

fn value_contains_only_secret_refs(value: &RunEventValue) -> bool {
    match value {
        RunEventValue::SecretRef(_) => true,
        RunEventValue::List(values) => values.iter().all(value_contains_only_secret_refs),
        RunEventValue::Object(values) => values.values().all(value_contains_only_secret_refs),
        RunEventValue::Bool(_)
        | RunEventValue::I64(_)
        | RunEventValue::U64(_)
        | RunEventValue::String(_) => false,
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("secret")
        || key.contains("token")
        || key.contains("password")
        || key.contains("credential")
        || key.contains("authorization")
        || key.contains("api_key")
        || key.contains("apikey")
        || key.contains("connection_string")
        || key.contains("dsn")
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
