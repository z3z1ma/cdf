use std::{collections::BTreeSet, path::Path, sync::Mutex};

use cdf_kernel::{
    CdfError, Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore, PackageHash, PipelineId,
    Receipt, ResourceId, Result, RewindReport, RewindRequest, SchemaHash, ScopeKey, SourcePosition,
    StateDelta,
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};

use crate::support::{
    SqliteConnectionGuard, SqliteErrorContext, StateStorePathOwnership, database_open_path,
    database_path_exists, decode_json, encode_json, ensure_schema_version_table,
    lock_sqlite_connection, managed_sqlite_open_flags, missing_checkpoint, now_ms,
    packages_ahead_of_state, prepare_managed_database_path, private_state_decode,
    read_component_schema_version, require_sqlite_tables, rewind_marker, same_tuple, sqlite_error,
    sqlite_table_exists, validate_state_version, verify_receipt, with_sqlite_error_context,
    write_component_schema_version,
};

pub(crate) const CHECKPOINT_STORE_COMPONENT: &str = "checkpoint_store";
pub(crate) const CHECKPOINT_STORE_SCHEMA_VERSION: i64 = 2;
const CHECKPOINT_SELECT: &str = "SELECT sequence, checkpoint_id, pipeline_id, resource_id, scope_json, state_version, parent_checkpoint_id, input_position_json, output_position_json, package_hash, schema_hash, receipt_id, status, is_head, created_at_ms, committed_at_ms, delta_json, receipt_json, rewind_target_checkpoint_id FROM cdf_checkpoints";
pub struct SqliteCheckpointStore {
    conn: Mutex<Connection>,
    error_context: SqliteErrorContext,
}

impl SqliteCheckpointStore {
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
            initialize_schema(&conn)?;
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

    pub(crate) fn open_existing_with_path_ownership(
        path: impl AsRef<Path>,
        ownership: StateStorePathOwnership,
    ) -> Result<Self> {
        let path = path.as_ref();
        database_path_exists(path, ownership)?;
        let open_path = database_open_path(path, ownership)?;
        let error_context = SqliteErrorContext::ManagedState;
        let conn = with_sqlite_error_context(error_context, || {
            let conn = Connection::open_with_flags(&open_path, managed_sqlite_open_flags(false))
                .map_err(sqlite_error)?;
            validate_schema_version(&conn)?;
            Ok(conn)
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
            error_context,
        })
    }

    pub fn open_read_only_with_path_ownership(
        path: impl AsRef<Path>,
        ownership: StateStorePathOwnership,
    ) -> Result<Self> {
        let path = path.as_ref();
        if !database_path_exists(path, ownership)? {
            return Err(CdfError::data(format!(
                "checkpoint state database {} is missing",
                path.display()
            )));
        }
        let open_path = database_open_path(path, ownership)?;
        let error_context = SqliteErrorContext::ManagedReadOnly;
        let conn = with_sqlite_error_context(error_context, || {
            let conn = Connection::open_with_flags(&open_path, managed_sqlite_open_flags(true))
                .map_err(sqlite_error)?;
            validate_schema_version(&conn)?;
            Ok(conn)
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
            error_context,
        })
    }

    pub fn is_initialized(
        path: impl AsRef<Path>,
        ownership: StateStorePathOwnership,
    ) -> Result<bool> {
        let path = path.as_ref();
        if !database_path_exists(path, ownership)? {
            return Ok(false);
        }
        let open_path = database_open_path(path, ownership)?;
        let error_context = SqliteErrorContext::ManagedReadOnly;
        with_sqlite_error_context(error_context, || {
            let conn = Connection::open_with_flags(&open_path, managed_sqlite_open_flags(true))
                .map_err(sqlite_error)?;
            match read_component_schema_version(&conn, CHECKPOINT_STORE_COMPONENT)? {
                Some(CHECKPOINT_STORE_SCHEMA_VERSION) => {
                    validate_schema_structure(&conn)?;
                    Ok(true)
                }
                Some(version) => Err(unsupported_checkpoint_schema_version(version)),
                None if sqlite_table_exists(&conn, "cdf_checkpoints")? => {
                    Err(CdfError::internal(format!(
                        "checkpoint store SQLite schema is unversioned; expected current version {CHECKPOINT_STORE_SCHEMA_VERSION}"
                    )))
                }
                None => Ok(false),
            }
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        let error_context = SqliteErrorContext::EphemeralWorkspace;
        let conn = with_sqlite_error_context(error_context, || {
            let conn = Connection::open_in_memory().map_err(sqlite_error)?;
            initialize_schema(&conn)?;
            Ok(conn)
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
            error_context,
        })
    }

    pub fn committed_package_hashes(&self) -> Result<BTreeSet<PackageHash>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT package_hash FROM cdf_checkpoints \
                 WHERE status = 'committed' ORDER BY package_hash",
            )
            .map_err(sqlite_error)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sqlite_error)?;
        rows.map(|row| {
            private_state_decode(
                "decode CDF-managed committed package hash",
                row.map_err(sqlite_error).and_then(PackageHash::new),
            )
        })
        .collect()
    }

    /// Returns every committed checkpoint in canonical store sequence order.
    ///
    /// Retention and recovery planners consume the complete typed rows so package eligibility is
    /// derived from receipt/checkpoint authority rather than filesystem age or hash presence.
    pub fn committed_checkpoints(&self) -> Result<Vec<Checkpoint>> {
        let conn = self.lock_conn()?;
        let sql = format!("{CHECKPOINT_SELECT} WHERE status = 'committed' ORDER BY sequence");
        let mut statement = conn.prepare(&sql).map_err(sqlite_error)?;
        let rows = statement
            .query_map([], row_to_checkpoint)
            .map_err(sqlite_error)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)
    }

    fn lock_conn(&self) -> Result<SqliteConnectionGuard<'_>> {
        lock_sqlite_connection(&self.conn, self.error_context)
    }

    /// Validates every CDF-owned checkpoint row before a raw or diagnostic reader bypasses the
    /// typed store API.
    ///
    /// Ordinary store opens intentionally validate only schema authority. Typed reads validate
    /// each row they consume so run startup remains independent of total historical state.
    pub fn validate_integrity(&self) -> Result<()> {
        let conn = self.lock_conn()?;
        validate_private_checkpoint_rows(&conn)
    }

    pub(crate) fn fetch_by_id_tx(
        tx: &Transaction<'_>,
        checkpoint_id: &CheckpointId,
    ) -> Result<Option<Checkpoint>> {
        let sql = format!("{CHECKPOINT_SELECT} WHERE checkpoint_id = ?");
        tx.query_row(&sql, params![checkpoint_id.as_str()], row_to_checkpoint)
            .optional()
            .map_err(sqlite_error)
    }

    pub(crate) fn head_tx(
        tx: &Transaction<'_>,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        let scope_json = encode_json(scope)?;
        let sql = format!(
            "{CHECKPOINT_SELECT} WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? AND status = 'committed' AND is_head = 1"
        );
        tx.query_row(
            &sql,
            params![pipeline_id.as_str(), resource_id.as_str(), scope_json],
            row_to_checkpoint,
        )
        .optional()
        .map_err(sqlite_error)
    }

    fn history_tx(
        tx: &Transaction<'_>,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        let scope_json = encode_json(scope)?;
        let sql = format!(
            "{CHECKPOINT_SELECT} WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? ORDER BY sequence"
        );
        let mut stmt = tx.prepare(&sql).map_err(sqlite_error)?;
        let rows = stmt
            .query_map(
                params![pipeline_id.as_str(), resource_id.as_str(), scope_json],
                row_to_checkpoint,
            )
            .map_err(sqlite_error)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)
    }
}

#[cfg(test)]
impl SqliteCheckpointStore {
    pub(crate) fn execute_for_test<P>(&self, sql: &str, params: P) -> rusqlite::Result<usize>
    where
        P: rusqlite::Params,
    {
        self.conn.lock().unwrap().execute(sql, params)
    }

    pub(crate) fn execute_classified_for_test<P>(&self, sql: &str, params: P) -> Result<usize>
    where
        P: rusqlite::Params,
    {
        let conn = self.lock_conn()?;
        conn.execute(sql, params).map_err(sqlite_error)
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

impl CheckpointStore for SqliteCheckpointStore {
    fn propose(&self, delta: StateDelta) -> Result<Checkpoint> {
        delta.validate()?;
        let checkpoint = Checkpoint {
            delta,
            status: CheckpointStatus::Proposed,
            receipt: None,
            is_head: false,
            created_at_ms: now_ms()?,
            committed_at_ms: None,
            rewind_target_checkpoint_id: None,
        };
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        if Self::fetch_by_id_tx(&tx, &checkpoint.delta.checkpoint_id)?.is_some() {
            return Err(CdfError::contract(format!(
                "checkpoint {} already exists",
                checkpoint.delta.checkpoint_id
            )));
        }
        let head = Self::head_tx(
            &tx,
            &checkpoint.delta.pipeline_id,
            &checkpoint.delta.resource_id,
            &checkpoint.delta.scope,
        )?;
        checkpoint
            .delta
            .validate_transition_from_head(head.as_ref().map(|head| &head.delta))?;
        insert_checkpoint(&tx, &checkpoint)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(checkpoint)
    }

    fn commit(&self, checkpoint_id: &CheckpointId, receipt: Receipt) -> Result<Checkpoint> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        let committed = commit_checkpoint_tx(&tx, checkpoint_id, &receipt)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(committed)
    }

    fn abandon(&self, checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        let checkpoint = Self::fetch_by_id_tx(&tx, checkpoint_id)?
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?;
        if checkpoint.status != CheckpointStatus::Proposed {
            return Err(CdfError::contract(format!(
                "checkpoint {checkpoint_id} is not proposed"
            )));
        }
        tx.execute(
            "UPDATE cdf_checkpoints SET status = 'abandoned' WHERE checkpoint_id = ? AND status = 'proposed'",
            params![checkpoint_id.as_str()],
        )
        .map_err(sqlite_error)?;
        let abandoned = Self::fetch_by_id_tx(&tx, checkpoint_id)?
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?;
        tx.commit().map_err(sqlite_error)?;
        Ok(abandoned)
    }

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        let conn = self.lock_conn()?;
        let scope_json = encode_json(scope)?;
        let sql = format!(
            "{CHECKPOINT_SELECT} WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? AND status = 'committed' AND is_head = 1"
        );
        conn.query_row(
            &sql,
            params![pipeline_id.as_str(), resource_id.as_str(), scope_json],
            row_to_checkpoint,
        )
        .optional()
        .map_err(sqlite_error)
    }

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        let conn = self.lock_conn()?;
        let scope_json = encode_json(scope)?;
        let sql = format!(
            "{CHECKPOINT_SELECT} WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? ORDER BY sequence"
        );
        let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
        let rows = stmt
            .query_map(
                params![pipeline_id.as_str(), resource_id.as_str(), scope_json],
                row_to_checkpoint,
            )
            .map_err(sqlite_error)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)
    }

    fn committed_schema_streak(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
        schema_hash: &SchemaHash,
        limit: u32,
    ) -> Result<u32> {
        if limit == 0 {
            return Ok(0);
        }
        let conn = self.lock_conn()?;
        let scope_json = encode_json(scope)?;
        let sql = format!(
            "{CHECKPOINT_SELECT} \
             WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? \
               AND status = 'committed' \
             ORDER BY sequence DESC LIMIT ?"
        );
        let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
        let rows = stmt
            .query_map(
                params![
                    pipeline_id.as_str(),
                    resource_id.as_str(),
                    scope_json,
                    i64::from(limit)
                ],
                row_to_checkpoint,
            )
            .map_err(sqlite_error)?;
        let mut count = 0_u32;
        for row in rows {
            if row.map_err(sqlite_error)?.delta.schema_hash != *schema_hash {
                break;
            }
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    fn rewind(&self, request: RewindRequest) -> Result<RewindReport> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        if Self::fetch_by_id_tx(&tx, &request.marker_checkpoint_id)?.is_some() {
            return Err(CdfError::contract(format!(
                "checkpoint {} already exists",
                request.marker_checkpoint_id
            )));
        }
        let target = Self::fetch_by_id_tx(&tx, &request.target_checkpoint_id)?
            .ok_or_else(|| missing_checkpoint(&request.target_checkpoint_id))?;
        if target.status != CheckpointStatus::Committed
            || !same_tuple(
                &target.delta,
                &request.pipeline_id,
                &request.resource_id,
                &request.scope,
            )
        {
            return Err(CdfError::contract(
                "rewind target must be a committed checkpoint for the requested scope",
            ));
        }
        let current_head = Self::head_tx(
            &tx,
            &request.pipeline_id,
            &request.resource_id,
            &request.scope,
        )?
        .ok_or_else(|| CdfError::contract("cannot rewind without a committed head"))?;
        let history = Self::history_tx(
            &tx,
            &request.pipeline_id,
            &request.resource_id,
            &request.scope,
        )?;
        let packages_ahead = packages_ahead_of_state(
            &history,
            &current_head.delta.checkpoint_id,
            &target.delta.checkpoint_id,
        );

        let scope_json = encode_json(&request.scope)?;
        tx.execute(
            "UPDATE cdf_checkpoints SET is_head = 0 WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? AND is_head = 1",
            params![request.pipeline_id.as_str(), request.resource_id.as_str(), scope_json],
        )
        .map_err(sqlite_error)?;
        tx.execute(
            "UPDATE cdf_checkpoints SET is_head = 1 WHERE checkpoint_id = ? AND status = 'committed'",
            params![request.target_checkpoint_id.as_str()],
        )
        .map_err(sqlite_error)?;

        let marker = rewind_marker(&request, &current_head, &target, now_ms()?);
        insert_checkpoint(&tx, &marker)?;
        let head = Self::fetch_by_id_tx(&tx, &request.target_checkpoint_id)?
            .ok_or_else(|| missing_checkpoint(&request.target_checkpoint_id))?;
        tx.commit().map_err(sqlite_error)?;

        Ok(RewindReport {
            marker,
            head,
            packages_ahead,
        })
    }
}

pub(crate) fn commit_checkpoint_tx(
    tx: &Transaction<'_>,
    checkpoint_id: &CheckpointId,
    receipt: &Receipt,
) -> Result<Checkpoint> {
    let checkpoint = SqliteCheckpointStore::fetch_by_id_tx(tx, checkpoint_id)?
        .ok_or_else(|| missing_checkpoint(checkpoint_id))?;
    if checkpoint.status == CheckpointStatus::Committed {
        if checkpoint.receipt.as_ref() == Some(receipt) {
            return Ok(checkpoint);
        }
        return Err(CdfError::contract(format!(
            "checkpoint {checkpoint_id} is committed with conflicting receipt authority"
        )));
    }
    if checkpoint.status != CheckpointStatus::Proposed {
        return Err(CdfError::contract(format!(
            "checkpoint {checkpoint_id} is not proposed"
        )));
    }
    verify_receipt(receipt, &checkpoint.delta)?;
    let head = SqliteCheckpointStore::head_tx(
        tx,
        &checkpoint.delta.pipeline_id,
        &checkpoint.delta.resource_id,
        &checkpoint.delta.scope,
    )?;
    checkpoint
        .delta
        .validate_transition_from_head(head.as_ref().map(|head| &head.delta))?;
    let scope_json = encode_json(&checkpoint.delta.scope)?;
    tx.execute(
        "UPDATE cdf_checkpoints SET is_head = 0 WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? AND is_head = 1",
        params![
            checkpoint.delta.pipeline_id.as_str(),
            checkpoint.delta.resource_id.as_str(),
            scope_json,
        ],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "UPDATE cdf_checkpoints SET status = 'committed', receipt_id = ?, receipt_json = ?, is_head = 1, committed_at_ms = ? WHERE checkpoint_id = ? AND status = 'proposed'",
        params![
            receipt.receipt_id.as_str(),
            encode_json(receipt)?,
            receipt.committed_at_ms,
            checkpoint_id.as_str(),
        ],
    )
    .map_err(sqlite_error)?;
    SqliteCheckpointStore::fetch_by_id_tx(tx, checkpoint_id)?
        .ok_or_else(|| missing_checkpoint(checkpoint_id))
}

pub(crate) fn initialize_schema(conn: &Connection) -> Result<()> {
    match read_component_schema_version(conn, CHECKPOINT_STORE_COMPONENT)? {
        Some(CHECKPOINT_STORE_SCHEMA_VERSION) => validate_schema_structure(conn)?,
        Some(version) => return Err(unsupported_checkpoint_schema_version(version)),
        None if sqlite_table_exists(conn, "cdf_checkpoints")? => {
            return Err(CdfError::internal(format!(
                "checkpoint store SQLite schema is unversioned; expected current version {CHECKPOINT_STORE_SCHEMA_VERSION}"
            )));
        }
        None => {}
    }
    ensure_schema_version_table(conn)?;

    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;

        CREATE TABLE IF NOT EXISTS cdf_checkpoints (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            checkpoint_id TEXT NOT NULL UNIQUE,
            pipeline_id TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            scope_json TEXT NOT NULL,
            state_version INTEGER NOT NULL,
            parent_checkpoint_id TEXT,
            input_position_json TEXT,
            output_position_json TEXT NOT NULL,
            package_hash TEXT NOT NULL,
            schema_hash TEXT NOT NULL,
            receipt_id TEXT,
            status TEXT NOT NULL CHECK (status IN ('proposed', 'committed', 'abandoned', 'rewound')),
            is_head INTEGER NOT NULL CHECK (is_head IN (0, 1)),
            created_at_ms INTEGER NOT NULL,
            committed_at_ms INTEGER,
            delta_json TEXT NOT NULL,
            receipt_json TEXT,
            rewind_target_checkpoint_id TEXT,
            CHECK (state_version = 2),
            CHECK (is_head = 0 OR status = 'committed'),
            CHECK ((status = 'committed') = (receipt_id IS NOT NULL AND receipt_json IS NOT NULL AND committed_at_ms IS NOT NULL))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS cdf_checkpoints_one_committed_head
            ON cdf_checkpoints (pipeline_id, resource_id, scope_json)
            WHERE is_head = 1 AND status = 'committed';

        CREATE INDEX IF NOT EXISTS cdf_checkpoints_history
            ON cdf_checkpoints (pipeline_id, resource_id, scope_json, sequence);
        ",
    )
    .map_err(sqlite_error)?;
    write_component_schema_version(
        conn,
        CHECKPOINT_STORE_COMPONENT,
        CHECKPOINT_STORE_SCHEMA_VERSION,
    )
}

fn validate_schema_version(conn: &Connection) -> Result<()> {
    match read_component_schema_version(conn, CHECKPOINT_STORE_COMPONENT)? {
        Some(CHECKPOINT_STORE_SCHEMA_VERSION) => validate_schema_structure(conn),
        Some(version) => Err(unsupported_checkpoint_schema_version(version)),
        None => Err(CdfError::internal(format!(
            "checkpoint store SQLite schema version is missing; expected {CHECKPOINT_STORE_SCHEMA_VERSION}"
        ))),
    }
}

fn validate_schema_structure(conn: &Connection) -> Result<()> {
    require_sqlite_tables(conn, "checkpoint store", &["cdf_checkpoints"])
}

fn validate_private_checkpoint_rows(conn: &Connection) -> Result<()> {
    let mut statement = conn.prepare(CHECKPOINT_SELECT).map_err(sqlite_error)?;
    let rows = statement
        .query_map([], row_to_checkpoint)
        .map_err(sqlite_error)?;
    for checkpoint in rows {
        checkpoint.map_err(sqlite_error)?;
    }
    Ok(())
}

fn unsupported_checkpoint_schema_version(version: i64) -> CdfError {
    CdfError::internal(format!(
        "unsupported checkpoint store SQLite schema version {version}; current schema version {CHECKPOINT_STORE_SCHEMA_VERSION} is required, so recreate this pre-production state store"
    ))
}

fn insert_checkpoint(tx: &Transaction<'_>, checkpoint: &Checkpoint) -> Result<()> {
    let receipt_id = checkpoint
        .receipt
        .as_ref()
        .map(|receipt| receipt.receipt_id.as_str());
    let receipt_json = checkpoint.receipt.as_ref().map(encode_json).transpose()?;
    let input_position_json = checkpoint
        .delta
        .input_position
        .as_ref()
        .map(encode_json)
        .transpose()?;
    tx.execute(
        "INSERT INTO cdf_checkpoints (checkpoint_id, pipeline_id, resource_id, scope_json, state_version, parent_checkpoint_id, input_position_json, output_position_json, package_hash, schema_hash, receipt_id, status, is_head, created_at_ms, committed_at_ms, delta_json, receipt_json, rewind_target_checkpoint_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            checkpoint.delta.checkpoint_id.as_str(),
            checkpoint.delta.pipeline_id.as_str(),
            checkpoint.delta.resource_id.as_str(),
            encode_json(&checkpoint.delta.scope)?,
            checkpoint.delta.state_version,
            checkpoint
                .delta
                .parent_checkpoint_id
                .as_ref()
                .map(CheckpointId::as_str),
            input_position_json,
            encode_json(&checkpoint.delta.output_position)?,
            checkpoint.delta.package_hash.as_str(),
            checkpoint.delta.schema_hash.as_str(),
            receipt_id,
            checkpoint.status.as_str(),
            i64::from(checkpoint.is_head),
            checkpoint.created_at_ms,
            checkpoint.committed_at_ms,
            encode_json(&checkpoint.delta)?,
            receipt_json,
            checkpoint
                .rewind_target_checkpoint_id
                .as_ref()
                .map(CheckpointId::as_str),
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn row_to_checkpoint(row: &Row<'_>) -> rusqlite::Result<Checkpoint> {
    row_to_checkpoint_result(row).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn row_to_checkpoint_result(row: &Row<'_>) -> Result<Checkpoint> {
    private_state_decode(
        "decode CDF-managed checkpoint row",
        (|| {
            let sequence = row_get::<i64>(row, "sequence")?;
            if sequence < 1 {
                return Err(CdfError::internal(
                    "checkpoint row sequence must be a positive CDF-managed identifier",
                ));
            }
            let checkpoint_id = CheckpointId::new(row_get::<String>(row, "checkpoint_id")?)?;
            let pipeline_id = PipelineId::new(row_get::<String>(row, "pipeline_id")?)?;
            let resource_id = ResourceId::new(row_get::<String>(row, "resource_id")?)?;
            let scope_json: String = row_get(row, "scope_json")?;
            let state_version = row_get::<u16>(row, "state_version")?;
            validate_state_version(state_version)?;
            let parent_checkpoint_id = row_get::<Option<String>>(row, "parent_checkpoint_id")?
                .map(CheckpointId::new)
                .transpose()?;
            let input_position = row_get::<Option<String>>(row, "input_position_json")?
                .map(|json| decode_json::<SourcePosition>(&json, state_version))
                .transpose()?;
            let output_position = decode_json::<SourcePosition>(
                &row_get::<String>(row, "output_position_json")?,
                state_version,
            )?;
            let package_hash = PackageHash::new(row_get::<String>(row, "package_hash")?)?;
            let schema_hash = cdf_kernel::SchemaHash::new(row_get::<String>(row, "schema_hash")?)?;
            let status = CheckpointStatus::parse(&row_get::<String>(row, "status")?)?;
            let delta =
                decode_json::<StateDelta>(&row_get::<String>(row, "delta_json")?, state_version)?;
            delta.validate()?;
            let receipt_id = row_get::<Option<String>>(row, "receipt_id")?
                .map(cdf_kernel::ReceiptId::new)
                .transpose()?;
            if delta.checkpoint_id != checkpoint_id
                || delta.pipeline_id != pipeline_id
                || delta.resource_id != resource_id
                || delta.scope != decode_json::<ScopeKey>(&scope_json, state_version)?
                || delta.state_version != state_version
                || delta.parent_checkpoint_id != parent_checkpoint_id
                || delta.input_position != input_position
                || delta.output_position != output_position
                || delta.package_hash != package_hash
                || delta.schema_hash != schema_hash
            {
                return Err(CdfError::internal(
                    "checkpoint row columns do not match serialized state delta",
                ));
            }

            let receipt = row_get::<Option<String>>(row, "receipt_json")?
                .map(|json| decode_json::<Receipt>(&json, state_version))
                .transpose()?;
            let is_head = row_get::<i64>(row, "is_head")?;
            if !matches!(is_head, 0 | 1) {
                return Err(CdfError::internal(
                    "checkpoint row is_head must be encoded as 0 or 1",
                ));
            }
            if is_head == 1 && status != CheckpointStatus::Committed {
                return Err(CdfError::internal(
                    "checkpoint row marks a non-committed checkpoint as the current head",
                ));
            }
            let created_at_ms = row_get::<i64>(row, "created_at_ms")?;
            let committed_at_ms = row_get::<Option<i64>>(row, "committed_at_ms")?;
            let rewind_target_checkpoint_id =
                row_get::<Option<String>>(row, "rewind_target_checkpoint_id")?
                    .map(CheckpointId::new)
                    .transpose()?;
            if created_at_ms < 0 || committed_at_ms.is_some_and(|committed| committed < 0) {
                return Err(CdfError::internal(
                    "checkpoint row timestamps violate the private lifecycle invariant",
                ));
            }
            match (&status, &receipt, &receipt_id) {
                (CheckpointStatus::Committed, Some(receipt), Some(receipt_id))
                    if receipt.receipt_id == *receipt_id =>
                {
                    verify_receipt(receipt, &delta)?;
                    if committed_at_ms != Some(receipt.committed_at_ms) {
                        return Err(CdfError::internal(
                            "committed checkpoint timestamp does not match receipt JSON",
                        ));
                    }
                }
                (CheckpointStatus::Committed, Some(_), Some(_)) => {
                    return Err(CdfError::internal(
                        "committed checkpoint row receipt id does not match receipt JSON",
                    ));
                }
                (CheckpointStatus::Committed, None, _) => {
                    return Err(CdfError::internal(
                        "committed checkpoint row is missing receipt JSON",
                    ));
                }
                (CheckpointStatus::Committed, Some(_), None) => {
                    return Err(CdfError::internal(
                        "committed checkpoint row is missing receipt id",
                    ));
                }
                (_, Some(_), _) | (_, _, Some(_)) => {
                    return Err(CdfError::internal(
                        "non-committed checkpoint row unexpectedly has a receipt",
                    ));
                }
                (_, None, None) => {}
            }
            if status != CheckpointStatus::Committed && committed_at_ms.is_some() {
                return Err(CdfError::internal(
                    "non-committed checkpoint row unexpectedly has a committed timestamp",
                ));
            }
            match (&status, &rewind_target_checkpoint_id) {
                (CheckpointStatus::Rewound, Some(_)) => {}
                (CheckpointStatus::Rewound, None) => {
                    return Err(CdfError::internal(
                        "rewound checkpoint row is missing its rewind target",
                    ));
                }
                (_, Some(_)) => {
                    return Err(CdfError::internal(
                        "non-rewound checkpoint row unexpectedly has a rewind target",
                    ));
                }
                (_, None) => {}
            }

            Ok(Checkpoint {
                delta,
                status,
                receipt,
                is_head: is_head == 1,
                created_at_ms,
                committed_at_ms,
                rewind_target_checkpoint_id,
            })
        })(),
    )
}

fn row_get<T: rusqlite::types::FromSql>(row: &Row<'_>, column: &str) -> Result<T> {
    row.get(column).map_err(sqlite_error)
}
