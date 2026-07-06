#![doc = "SQLite checkpoint store boundary for firn."]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use firn_kernel::{
    CHECKPOINT_STATE_VERSION, Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore,
    FirnError, PackageHash, PipelineId, Receipt, ResourceId, Result, RewindReport, RewindRequest,
    ScopeKey, SourcePosition, StateDelta, StateSegment,
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use serde::de::DeserializeOwned;

const CHECKPOINT_SELECT: &str = "SELECT checkpoint_id, pipeline_id, resource_id, scope_json, state_version, parent_checkpoint_id, input_position_json, output_position_json, package_hash, schema_hash, receipt_id, status, is_head, created_at_ms, committed_at_ms, delta_json, receipt_json, rewind_target_checkpoint_id FROM firn_checkpoints";

#[derive(Default)]
pub struct InMemoryCheckpointStore {
    inner: Mutex<InMemoryCheckpointState>,
}

#[derive(Default)]
struct InMemoryCheckpointState {
    checkpoints: BTreeMap<CheckpointId, Checkpoint>,
    order: Vec<CheckpointId>,
}

impl InMemoryCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    fn propose(&self, delta: StateDelta) -> Result<Checkpoint> {
        validate_state_version(delta.state_version)?;
        let mut state = self.lock_inner()?;
        if state.checkpoints.contains_key(&delta.checkpoint_id) {
            return Err(FirnError::contract(format!(
                "checkpoint {} already exists",
                delta.checkpoint_id
            )));
        }

        let checkpoint = Checkpoint {
            delta,
            status: CheckpointStatus::Proposed,
            receipt: None,
            is_head: false,
            created_at_ms: now_ms()?,
            committed_at_ms: None,
            rewind_target_checkpoint_id: None,
        };
        state.order.push(checkpoint.delta.checkpoint_id.clone());
        state
            .checkpoints
            .insert(checkpoint.delta.checkpoint_id.clone(), checkpoint.clone());
        Ok(checkpoint)
    }

    fn commit(&self, checkpoint_id: &CheckpointId, receipt: Receipt) -> Result<Checkpoint> {
        let mut state = self.lock_inner()?;
        let checkpoint = state
            .checkpoints
            .get(checkpoint_id)
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?
            .clone();
        if checkpoint.status != CheckpointStatus::Proposed {
            return Err(FirnError::contract(format!(
                "checkpoint {checkpoint_id} is not proposed"
            )));
        }
        verify_receipt(&receipt, &checkpoint.delta)?;

        for existing in state.checkpoints.values_mut() {
            if same_tuple(
                &existing.delta,
                &checkpoint.delta.pipeline_id,
                &checkpoint.delta.resource_id,
                &checkpoint.delta.scope,
            ) {
                existing.is_head = false;
            }
        }

        let committed = state
            .checkpoints
            .get_mut(checkpoint_id)
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?;
        committed.status = CheckpointStatus::Committed;
        committed.receipt = Some(receipt.clone());
        committed.is_head = true;
        committed.committed_at_ms = Some(receipt.committed_at_ms);
        Ok(committed.clone())
    }

    fn abandon(&self, checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        let mut state = self.lock_inner()?;
        let checkpoint = state
            .checkpoints
            .get_mut(checkpoint_id)
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?;
        if checkpoint.status != CheckpointStatus::Proposed {
            return Err(FirnError::contract(format!(
                "checkpoint {checkpoint_id} is not proposed"
            )));
        }
        checkpoint.status = CheckpointStatus::Abandoned;
        Ok(checkpoint.clone())
    }

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        let state = self.lock_inner()?;
        Ok(in_memory_head(&state, pipeline_id, resource_id, scope))
    }

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        let state = self.lock_inner()?;
        Ok(in_memory_history(&state, pipeline_id, resource_id, scope))
    }

    fn rewind(&self, request: RewindRequest) -> Result<RewindReport> {
        let mut state = self.lock_inner()?;
        if state
            .checkpoints
            .contains_key(&request.marker_checkpoint_id)
        {
            return Err(FirnError::contract(format!(
                "checkpoint {} already exists",
                request.marker_checkpoint_id
            )));
        }

        let target = state
            .checkpoints
            .get(&request.target_checkpoint_id)
            .ok_or_else(|| missing_checkpoint(&request.target_checkpoint_id))?
            .clone();
        if target.status != CheckpointStatus::Committed
            || !same_tuple(
                &target.delta,
                &request.pipeline_id,
                &request.resource_id,
                &request.scope,
            )
        {
            return Err(FirnError::contract(
                "rewind target must be a committed checkpoint for the requested scope",
            ));
        }
        let current_head = in_memory_head(
            &state,
            &request.pipeline_id,
            &request.resource_id,
            &request.scope,
        )
        .ok_or_else(|| FirnError::contract("cannot rewind without a committed head"))?;
        let history = in_memory_history(
            &state,
            &request.pipeline_id,
            &request.resource_id,
            &request.scope,
        );
        let packages_ahead = packages_ahead_of_state(
            &history,
            &current_head.delta.checkpoint_id,
            &target.delta.checkpoint_id,
        );

        for checkpoint in state.checkpoints.values_mut() {
            if same_tuple(
                &checkpoint.delta,
                &request.pipeline_id,
                &request.resource_id,
                &request.scope,
            ) {
                checkpoint.is_head = false;
            }
        }
        let head = state
            .checkpoints
            .get_mut(&request.target_checkpoint_id)
            .ok_or_else(|| missing_checkpoint(&request.target_checkpoint_id))?;
        head.is_head = true;
        let head = head.clone();

        let marker = rewind_marker(&request, &current_head, &target, now_ms()?);
        state.order.push(marker.delta.checkpoint_id.clone());
        state
            .checkpoints
            .insert(marker.delta.checkpoint_id.clone(), marker.clone());

        Ok(RewindReport {
            marker,
            head,
            packages_ahead,
        })
    }
}

impl InMemoryCheckpointStore {
    fn lock_inner(&self) -> Result<MutexGuard<'_, InMemoryCheckpointState>> {
        self.inner.lock().map_err(lock_error)
    }
}

fn in_memory_head(
    state: &InMemoryCheckpointState,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
) -> Option<Checkpoint> {
    state
        .checkpoints
        .values()
        .find(|checkpoint| {
            checkpoint.status == CheckpointStatus::Committed
                && checkpoint.is_head
                && same_tuple(&checkpoint.delta, pipeline_id, resource_id, scope)
        })
        .cloned()
}

fn in_memory_history(
    state: &InMemoryCheckpointState,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
) -> Vec<Checkpoint> {
    state
        .order
        .iter()
        .filter_map(|checkpoint_id| state.checkpoints.get(checkpoint_id))
        .filter(|checkpoint| same_tuple(&checkpoint.delta, pipeline_id, resource_id, scope))
        .cloned()
        .collect()
}

pub struct SqliteCheckpointStore {
    conn: Mutex<Connection>,
}

impl SqliteCheckpointStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path.as_ref()).map_err(sqlite_error)?;
        initialize_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(sqlite_error)?;
        initialize_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(lock_error)
    }

    fn fetch_by_id_tx(
        tx: &Transaction<'_>,
        checkpoint_id: &CheckpointId,
    ) -> Result<Option<Checkpoint>> {
        let sql = format!("{CHECKPOINT_SELECT} WHERE checkpoint_id = ?");
        tx.query_row(&sql, params![checkpoint_id.as_str()], row_to_checkpoint)
            .optional()
            .map_err(sqlite_error)
    }

    fn head_tx(
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

impl CheckpointStore for SqliteCheckpointStore {
    fn propose(&self, delta: StateDelta) -> Result<Checkpoint> {
        validate_state_version(delta.state_version)?;
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
        insert_checkpoint(&tx, &checkpoint)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(checkpoint)
    }

    fn commit(&self, checkpoint_id: &CheckpointId, receipt: Receipt) -> Result<Checkpoint> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        let checkpoint = Self::fetch_by_id_tx(&tx, checkpoint_id)?
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?;
        if checkpoint.status != CheckpointStatus::Proposed {
            return Err(FirnError::contract(format!(
                "checkpoint {checkpoint_id} is not proposed"
            )));
        }
        verify_receipt(&receipt, &checkpoint.delta)?;

        let scope_json = encode_json(&checkpoint.delta.scope)?;
        tx.execute(
            "UPDATE firn_checkpoints SET is_head = 0 WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? AND is_head = 1",
            params![
                checkpoint.delta.pipeline_id.as_str(),
                checkpoint.delta.resource_id.as_str(),
                scope_json,
            ],
        )
        .map_err(sqlite_error)?;
        tx.execute(
            "UPDATE firn_checkpoints SET status = 'committed', receipt_id = ?, receipt_json = ?, is_head = 1, committed_at_ms = ? WHERE checkpoint_id = ? AND status = 'proposed'",
            params![
                receipt.receipt_id.as_str(),
                encode_json(&receipt)?,
                receipt.committed_at_ms,
                checkpoint_id.as_str(),
            ],
        )
        .map_err(sqlite_error)?;
        let committed = Self::fetch_by_id_tx(&tx, checkpoint_id)?
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?;
        tx.commit().map_err(sqlite_error)?;
        Ok(committed)
    }

    fn abandon(&self, checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        let checkpoint = Self::fetch_by_id_tx(&tx, checkpoint_id)?
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?;
        if checkpoint.status != CheckpointStatus::Proposed {
            return Err(FirnError::contract(format!(
                "checkpoint {checkpoint_id} is not proposed"
            )));
        }
        tx.execute(
            "UPDATE firn_checkpoints SET status = 'abandoned' WHERE checkpoint_id = ? AND status = 'proposed'",
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

    fn rewind(&self, request: RewindRequest) -> Result<RewindReport> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        if Self::fetch_by_id_tx(&tx, &request.marker_checkpoint_id)?.is_some() {
            return Err(FirnError::contract(format!(
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
            return Err(FirnError::contract(
                "rewind target must be a committed checkpoint for the requested scope",
            ));
        }
        let current_head = Self::head_tx(
            &tx,
            &request.pipeline_id,
            &request.resource_id,
            &request.scope,
        )?
        .ok_or_else(|| FirnError::contract("cannot rewind without a committed head"))?;
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
            "UPDATE firn_checkpoints SET is_head = 0 WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? AND is_head = 1",
            params![request.pipeline_id.as_str(), request.resource_id.as_str(), scope_json],
        )
        .map_err(sqlite_error)?;
        tx.execute(
            "UPDATE firn_checkpoints SET is_head = 1 WHERE checkpoint_id = ? AND status = 'committed'",
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

fn initialize_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;

        CREATE TABLE IF NOT EXISTS firn_checkpoints (
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
            CHECK (state_version = 1),
            CHECK (is_head = 0 OR status = 'committed'),
            CHECK ((status = 'committed') = (receipt_id IS NOT NULL AND receipt_json IS NOT NULL AND committed_at_ms IS NOT NULL))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS firn_checkpoints_one_committed_head
            ON firn_checkpoints (pipeline_id, resource_id, scope_json)
            WHERE is_head = 1 AND status = 'committed';

        CREATE INDEX IF NOT EXISTS firn_checkpoints_history
            ON firn_checkpoints (pipeline_id, resource_id, scope_json, sequence);
        ",
    )
    .map_err(sqlite_error)
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
        "INSERT INTO firn_checkpoints (checkpoint_id, pipeline_id, resource_id, scope_json, state_version, parent_checkpoint_id, input_position_json, output_position_json, package_hash, schema_hash, receipt_id, status, is_head, created_at_ms, committed_at_ms, delta_json, receipt_json, rewind_target_checkpoint_id)
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
    let schema_hash = firn_kernel::SchemaHash::new(row_get::<String>(row, "schema_hash")?)?;
    let status = CheckpointStatus::parse(&row_get::<String>(row, "status")?)?;
    let delta = decode_json::<StateDelta>(&row_get::<String>(row, "delta_json")?, state_version)?;
    let receipt_id = row_get::<Option<String>>(row, "receipt_id")?
        .map(firn_kernel::ReceiptId::new)
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
        return Err(FirnError::data(
            "checkpoint row columns do not match serialized state delta",
        ));
    }

    let receipt = row_get::<Option<String>>(row, "receipt_json")?
        .map(|json| decode_json::<Receipt>(&json, state_version))
        .transpose()?;
    match (&status, &receipt, &receipt_id) {
        (CheckpointStatus::Committed, Some(receipt), Some(receipt_id))
            if receipt.receipt_id == *receipt_id =>
        {
            verify_receipt(receipt, &delta)?;
        }
        (CheckpointStatus::Committed, Some(_), Some(_)) => {
            return Err(FirnError::data(
                "committed checkpoint row receipt id does not match receipt JSON",
            ));
        }
        (CheckpointStatus::Committed, None, _) => {
            return Err(FirnError::data(
                "committed checkpoint row is missing receipt JSON",
            ));
        }
        (CheckpointStatus::Committed, Some(_), None) => {
            return Err(FirnError::data(
                "committed checkpoint row is missing receipt id",
            ));
        }
        (_, Some(_), _) | (_, _, Some(_)) => {
            return Err(FirnError::data(
                "non-committed checkpoint row unexpectedly has a receipt",
            ));
        }
        (_, None, None) => {}
    }

    Ok(Checkpoint {
        delta,
        status,
        receipt,
        is_head: row_get::<i64>(row, "is_head")? == 1,
        created_at_ms: row_get(row, "created_at_ms")?,
        committed_at_ms: row_get(row, "committed_at_ms")?,
        rewind_target_checkpoint_id: row_get::<Option<String>>(row, "rewind_target_checkpoint_id")?
            .map(CheckpointId::new)
            .transpose()?,
    })
}

fn row_get<T: rusqlite::types::FromSql>(row: &Row<'_>, column: &str) -> Result<T> {
    row.get(column).map_err(sqlite_error)
}

fn rewind_marker(
    request: &RewindRequest,
    current_head: &Checkpoint,
    target: &Checkpoint,
    created_at_ms: i64,
) -> Checkpoint {
    let mut delta = target.delta.clone();
    delta.checkpoint_id = request.marker_checkpoint_id.clone();
    delta.parent_checkpoint_id = Some(current_head.delta.checkpoint_id.clone());
    delta.input_position = Some(current_head.delta.output_position.clone());

    Checkpoint {
        delta,
        status: CheckpointStatus::Rewound,
        receipt: None,
        is_head: false,
        created_at_ms,
        committed_at_ms: None,
        rewind_target_checkpoint_id: Some(target.delta.checkpoint_id.clone()),
    }
}

fn packages_ahead_of_state(
    history: &[Checkpoint],
    current_head_id: &CheckpointId,
    target_id: &CheckpointId,
) -> Vec<PackageHash> {
    let by_id: BTreeMap<CheckpointId, &Checkpoint> = history
        .iter()
        .map(|checkpoint| (checkpoint.delta.checkpoint_id.clone(), checkpoint))
        .collect();
    let target_lineage = lineage_ids(&by_id, target_id);
    let mut packages = Vec::new();
    let mut cursor = Some(current_head_id.clone());

    while let Some(checkpoint_id) = cursor {
        if target_lineage.contains(&checkpoint_id) {
            break;
        }
        let Some(checkpoint) = by_id.get(&checkpoint_id) else {
            break;
        };
        if checkpoint.status == CheckpointStatus::Committed {
            packages.push(checkpoint.delta.package_hash.clone());
        }
        cursor = checkpoint.delta.parent_checkpoint_id.clone();
    }

    packages
}

fn lineage_ids(
    by_id: &BTreeMap<CheckpointId, &Checkpoint>,
    start_id: &CheckpointId,
) -> BTreeSet<CheckpointId> {
    let mut lineage = BTreeSet::new();
    let mut cursor = Some(start_id.clone());
    while let Some(checkpoint_id) = cursor {
        if !lineage.insert(checkpoint_id.clone()) {
            break;
        }
        cursor = by_id
            .get(&checkpoint_id)
            .and_then(|checkpoint| checkpoint.delta.parent_checkpoint_id.clone());
    }
    lineage
}

fn verify_receipt(receipt: &Receipt, delta: &StateDelta) -> Result<()> {
    if !receipt.covers_state_delta(delta)
        || !receipt_matches_segment_counts(receipt, &delta.segments)
    {
        return Err(FirnError::contract(format!(
            "receipt {} does not cover checkpoint {}",
            receipt.receipt_id, delta.checkpoint_id
        )));
    }
    Ok(())
}

fn receipt_matches_segment_counts(receipt: &Receipt, segments: &[StateSegment]) -> bool {
    let acks: BTreeMap<_, _> = receipt
        .segment_acks
        .iter()
        .map(|ack| (&ack.segment_id, ack))
        .collect();
    segments.iter().all(|segment| {
        acks.get(&segment.segment_id).is_some_and(|ack| {
            ack.row_count == segment.row_count && ack.byte_count == segment.byte_count
        })
    })
}

fn same_tuple(
    delta: &StateDelta,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
) -> bool {
    delta.pipeline_id == *pipeline_id && delta.resource_id == *resource_id && delta.scope == *scope
}

fn validate_state_version(state_version: u16) -> Result<()> {
    if state_version == CHECKPOINT_STATE_VERSION {
        Ok(())
    } else {
        Err(FirnError::contract(format!(
            "unsupported checkpoint state version {state_version}"
        )))
    }
}

fn encode_json<T: serde::Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|error| FirnError::data(error.to_string()))
}

fn decode_json<T: DeserializeOwned>(json: &str, state_version: u16) -> Result<T> {
    validate_state_version(state_version)?;
    serde_json::from_str(json).map_err(|error| FirnError::data(error.to_string()))
}

fn missing_checkpoint(checkpoint_id: &CheckpointId) -> FirnError {
    FirnError::contract(format!("checkpoint {checkpoint_id} does not exist"))
}

fn now_ms() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| FirnError::internal(error.to_string()))?;
    i64::try_from(elapsed.as_millis()).map_err(|error| FirnError::internal(error.to_string()))
}

fn sqlite_error(error: rusqlite::Error) -> FirnError {
    FirnError::internal(error.to_string())
}

fn lock_error<T>(error: std::sync::PoisonError<T>) -> FirnError {
    FirnError::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use firn_kernel::{
        CommitCounts, CompositePosition, ContractRef, CursorPosition, CursorValue, DestinationId,
        FileManifest, FilePosition, ForeignState, IdempotencyToken, LogPosition, MigrationRecord,
        PageToken, PartitionId, ReceiptId, SchemaHash, SegmentAck, SegmentId, TargetName,
        VerifyClause, WriteDisposition,
    };
    use tempfile::tempdir;

    use super::*;

    fn pipeline_id() -> PipelineId {
        PipelineId::new("pipeline-1").unwrap()
    }

    fn resource_id() -> ResourceId {
        ResourceId::new("orders").unwrap()
    }

    fn other_resource_id() -> ResourceId {
        ResourceId::new("customers").unwrap()
    }

    fn partition_scope() -> ScopeKey {
        ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        }
    }

    fn other_partition_scope() -> ScopeKey {
        ScopeKey::Partition {
            partition_id: PartitionId::new("p1").unwrap(),
        }
    }

    fn cursor_position(value: i64) -> SourcePosition {
        SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(value),
        })
    }

    fn delta(
        checkpoint_id: &str,
        parent_checkpoint_id: Option<&CheckpointId>,
        scope: ScopeKey,
        output_position: SourcePosition,
        package_hash: &str,
    ) -> StateDelta {
        delta_for(
            checkpoint_id,
            parent_checkpoint_id,
            pipeline_id(),
            resource_id(),
            scope,
            output_position,
            package_hash,
        )
    }

    fn delta_for(
        checkpoint_id: &str,
        parent_checkpoint_id: Option<&CheckpointId>,
        pipeline_id: PipelineId,
        resource_id: ResourceId,
        scope: ScopeKey,
        output_position: SourcePosition,
        package_hash: &str,
    ) -> StateDelta {
        let segment = StateSegment {
            segment_id: SegmentId::new(format!("{checkpoint_id}-segment")).unwrap(),
            scope: scope.clone(),
            output_position: output_position.clone(),
            row_count: 10,
            byte_count: 80,
        };
        StateDelta {
            checkpoint_id: CheckpointId::new(checkpoint_id).unwrap(),
            pipeline_id,
            resource_id,
            scope,
            state_version: CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: parent_checkpoint_id.cloned(),
            input_position: None,
            output_position,
            package_hash: PackageHash::new(package_hash).unwrap(),
            schema_hash: SchemaHash::new("schema-sha256").unwrap(),
            segments: vec![segment],
        }
    }

    fn receipt(delta: &StateDelta) -> Receipt {
        Receipt {
            receipt_id: ReceiptId::new(format!("receipt-{}", delta.checkpoint_id)).unwrap(),
            destination: DestinationId::new("local-test").unwrap(),
            target: TargetName::new("orders").unwrap(),
            package_hash: delta.package_hash.clone(),
            segment_acks: delta
                .segments
                .iter()
                .map(|segment| SegmentAck {
                    segment_id: segment.segment_id.clone(),
                    row_count: segment.row_count,
                    byte_count: segment.byte_count,
                })
                .collect(),
            disposition: WriteDisposition::Merge,
            idempotency_token: IdempotencyToken::new(delta.package_hash.as_str()).unwrap(),
            transaction: None,
            counts: CommitCounts {
                rows_written: 10,
                rows_inserted: Some(10),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: delta.schema_hash.clone(),
            migrations: Vec::<MigrationRecord>::new(),
            committed_at_ms: 1_700_000_000_000,
            verify: VerifyClause {
                kind: "sql".to_owned(),
                statement: "select count(*) from orders where _firn_package = ?".to_owned(),
                parameters: BTreeMap::new(),
            },
        }
    }

    fn assert_plausible_created_at(checkpoint: &Checkpoint) {
        assert!(
            checkpoint.created_at_ms > 1_600_000_000_000,
            "checkpoint timestamp should be a plausible positive epoch millisecond"
        );
    }

    fn commit_delta<S: CheckpointStore>(store: &S, delta: StateDelta) -> Checkpoint {
        let checkpoint_id = delta.checkpoint_id.clone();
        let receipt = receipt(&delta);
        let receipt_committed_at_ms = receipt.committed_at_ms;
        let proposed = store.propose(delta).unwrap();
        assert_plausible_created_at(&proposed);
        assert_eq!(proposed.committed_at_ms, None);
        let committed = store.commit(&checkpoint_id, receipt).unwrap();
        assert_plausible_created_at(&committed);
        assert_eq!(committed.committed_at_ms, Some(receipt_committed_at_ms));
        committed
    }

    fn assert_store_rejects_bad_receipts<S: CheckpointStore>(store: &S) {
        let delta = delta(
            "checkpoint-bad-receipt",
            None,
            partition_scope(),
            cursor_position(1),
            "package-sha256",
        );
        let checkpoint_id = delta.checkpoint_id.clone();
        let proposed = store.propose(delta.clone()).unwrap();
        assert_plausible_created_at(&proposed);

        let mut wrong_package = receipt(&delta);
        wrong_package.package_hash = PackageHash::new("other-package-sha256").unwrap();
        assert!(store.commit(&checkpoint_id, wrong_package).is_err());

        let mut missing_segment = receipt(&delta);
        missing_segment.segment_acks.clear();
        assert!(store.commit(&checkpoint_id, missing_segment).is_err());

        let mut wrong_counts = receipt(&delta);
        wrong_counts.segment_acks[0].row_count += 1;
        assert!(store.commit(&checkpoint_id, wrong_counts).is_err());

        assert!(
            store
                .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
                .unwrap()
                .is_none()
        );
    }

    fn assert_abandon_keeps_proposed_checkpoint_out_of_head<S: CheckpointStore>(store: &S) {
        let delta = delta(
            "checkpoint-abandon",
            None,
            partition_scope(),
            cursor_position(1),
            "package-abandon",
        );
        let checkpoint_id = delta.checkpoint_id.clone();
        let proposed = store.propose(delta.clone()).unwrap();
        assert_plausible_created_at(&proposed);

        let abandoned = store.abandon(&checkpoint_id).unwrap();
        assert_eq!(abandoned.status, CheckpointStatus::Abandoned);
        assert_plausible_created_at(&abandoned);
        assert_eq!(abandoned.committed_at_ms, None);
        assert!(store.commit(&checkpoint_id, receipt(&delta)).is_err());
        assert!(
            store
                .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
                .unwrap(),
            vec![abandoned]
        );
    }

    fn assert_scope_and_resource_isolation<S: CheckpointStore>(store: &S) {
        let scope = partition_scope();
        let other_scope = other_partition_scope();
        let first = commit_delta(
            store,
            delta(
                "checkpoint-isolation-main",
                None,
                scope.clone(),
                cursor_position(1),
                "package-isolation-main",
            ),
        );
        let other_resource = commit_delta(
            store,
            delta_for(
                "checkpoint-isolation-resource",
                None,
                pipeline_id(),
                other_resource_id(),
                scope.clone(),
                cursor_position(2),
                "package-isolation-resource",
            ),
        );
        let other_scope_checkpoint = commit_delta(
            store,
            delta_for(
                "checkpoint-isolation-scope",
                None,
                pipeline_id(),
                resource_id(),
                other_scope.clone(),
                cursor_position(3),
                "package-isolation-scope",
            ),
        );

        assert_eq!(
            store
                .head(&pipeline_id(), &resource_id(), &scope)
                .unwrap()
                .unwrap()
                .delta
                .checkpoint_id,
            first.delta.checkpoint_id
        );
        assert_eq!(
            store
                .head(&pipeline_id(), &other_resource_id(), &scope)
                .unwrap()
                .unwrap()
                .delta
                .checkpoint_id,
            other_resource.delta.checkpoint_id
        );
        assert_eq!(
            store
                .head(&pipeline_id(), &resource_id(), &other_scope)
                .unwrap()
                .unwrap()
                .delta
                .checkpoint_id,
            other_scope_checkpoint.delta.checkpoint_id
        );
        assert!(
            store
                .head(&pipeline_id(), &other_resource_id(), &other_scope)
                .unwrap()
                .is_none()
        );

        let main_history = store
            .history(&pipeline_id(), &resource_id(), &scope)
            .unwrap();
        assert_eq!(main_history.len(), 1);
        assert_eq!(
            main_history[0].delta.checkpoint_id,
            first.delta.checkpoint_id
        );
        assert_eq!(
            store
                .history(&pipeline_id(), &other_resource_id(), &scope)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .history(&pipeline_id(), &resource_id(), &other_scope)
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .history(&pipeline_id(), &other_resource_id(), &other_scope)
                .unwrap()
                .is_empty()
        );

        assert!(
            store
                .rewind(RewindRequest {
                    marker_checkpoint_id: CheckpointId::new("rewind-wrong-resource").unwrap(),
                    pipeline_id: pipeline_id(),
                    resource_id: resource_id(),
                    scope: scope.clone(),
                    target_checkpoint_id: other_resource.delta.checkpoint_id,
                })
                .is_err()
        );
        assert!(
            store
                .rewind(RewindRequest {
                    marker_checkpoint_id: CheckpointId::new("rewind-wrong-scope").unwrap(),
                    pipeline_id: pipeline_id(),
                    resource_id: resource_id(),
                    scope,
                    target_checkpoint_id: other_scope_checkpoint.delta.checkpoint_id,
                })
                .is_err()
        );
    }

    fn assert_rewind_appends_marker_and_reports_packages_ahead<S: CheckpointStore>(store: &S) {
        let scope = partition_scope();
        let first = commit_delta(
            store,
            delta(
                "checkpoint-1",
                None,
                scope.clone(),
                cursor_position(1),
                "package-1",
            ),
        );
        let second = commit_delta(
            store,
            delta(
                "checkpoint-2",
                Some(&first.delta.checkpoint_id),
                scope.clone(),
                cursor_position(2),
                "package-2",
            ),
        );
        let third = commit_delta(
            store,
            delta(
                "checkpoint-3",
                Some(&second.delta.checkpoint_id),
                scope.clone(),
                cursor_position(3),
                "package-3",
            ),
        );

        let report = store
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-marker-1").unwrap(),
                pipeline_id: pipeline_id(),
                resource_id: resource_id(),
                scope: scope.clone(),
                target_checkpoint_id: first.delta.checkpoint_id.clone(),
            })
            .unwrap();

        assert_eq!(report.marker.status, CheckpointStatus::Rewound);
        assert_plausible_created_at(&report.marker);
        assert_eq!(report.marker.committed_at_ms, None);
        assert_eq!(
            report.marker.rewind_target_checkpoint_id,
            Some(first.delta.checkpoint_id.clone())
        );
        assert_eq!(report.head.delta.checkpoint_id, first.delta.checkpoint_id);
        assert_eq!(
            report.packages_ahead,
            vec![third.delta.package_hash, second.delta.package_hash]
        );

        let history = store
            .history(&pipeline_id(), &resource_id(), &scope)
            .unwrap();
        assert_eq!(history.len(), 4);
        let historical_old_head = history
            .iter()
            .find(|checkpoint| checkpoint.delta.checkpoint_id == third.delta.checkpoint_id)
            .unwrap();
        assert_eq!(historical_old_head.status, CheckpointStatus::Committed);
        assert!(
            !historical_old_head.is_head,
            "rewind preserves later committed checkpoints as non-head history"
        );
        let target_history = history
            .iter()
            .find(|checkpoint| checkpoint.delta.checkpoint_id == first.delta.checkpoint_id)
            .unwrap();
        assert!(
            target_history.is_head,
            "rewind target becomes the committed head"
        );
        assert_eq!(
            store
                .head(&pipeline_id(), &resource_id(), &scope)
                .unwrap()
                .unwrap()
                .delta
                .checkpoint_id,
            first.delta.checkpoint_id
        );
    }

    fn assert_rewind_validation<S: CheckpointStore>(store: &S) {
        let scope = partition_scope();
        let first = commit_delta(
            store,
            delta(
                "checkpoint-rewind-validation-head",
                None,
                scope.clone(),
                cursor_position(1),
                "package-rewind-validation-head",
            ),
        );
        let proposed_delta = delta(
            "checkpoint-rewind-validation-proposed",
            Some(&first.delta.checkpoint_id),
            scope.clone(),
            cursor_position(2),
            "package-rewind-validation-proposed",
        );
        let proposed_id = proposed_delta.checkpoint_id.clone();
        store.propose(proposed_delta).unwrap();

        assert!(
            store
                .rewind(RewindRequest {
                    marker_checkpoint_id: CheckpointId::new("rewind-to-proposed").unwrap(),
                    pipeline_id: pipeline_id(),
                    resource_id: resource_id(),
                    scope: scope.clone(),
                    target_checkpoint_id: proposed_id,
                })
                .is_err()
        );
        assert!(
            store
                .rewind(RewindRequest {
                    marker_checkpoint_id: CheckpointId::new("rewind-to-missing").unwrap(),
                    pipeline_id: pipeline_id(),
                    resource_id: resource_id(),
                    scope,
                    target_checkpoint_id: CheckpointId::new("checkpoint-missing").unwrap(),
                })
                .is_err()
        );
    }

    fn assert_branch_rewind_reports_only_current_branch_ahead<S: CheckpointStore>(store: &S) {
        let scope = partition_scope();
        let base = commit_delta(
            store,
            delta(
                "checkpoint-branch-base",
                None,
                scope.clone(),
                cursor_position(1),
                "package-branch-base",
            ),
        );
        let target_branch = commit_delta(
            store,
            delta(
                "checkpoint-branch-target",
                Some(&base.delta.checkpoint_id),
                scope.clone(),
                cursor_position(2),
                "package-branch-target",
            ),
        );
        let current_branch_parent = commit_delta(
            store,
            delta(
                "checkpoint-branch-current-parent",
                Some(&base.delta.checkpoint_id),
                scope.clone(),
                cursor_position(3),
                "package-branch-current-parent",
            ),
        );
        let current_branch_head = commit_delta(
            store,
            delta(
                "checkpoint-branch-current-head",
                Some(&current_branch_parent.delta.checkpoint_id),
                scope.clone(),
                cursor_position(4),
                "package-branch-current-head",
            ),
        );

        let report = store
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-branch-marker").unwrap(),
                pipeline_id: pipeline_id(),
                resource_id: resource_id(),
                scope,
                target_checkpoint_id: target_branch.delta.checkpoint_id.clone(),
            })
            .unwrap();

        assert_eq!(
            report.head.delta.checkpoint_id,
            target_branch.delta.checkpoint_id
        );
        assert_eq!(
            report.packages_ahead,
            vec![
                current_branch_head.delta.package_hash,
                current_branch_parent.delta.package_hash
            ]
        );
        assert!(
            !report.packages_ahead.contains(&base.delta.package_hash),
            "common ancestors of the rewind target are not ahead of state"
        );
        assert!(
            !report
                .packages_ahead
                .contains(&target_branch.delta.package_hash),
            "the target package itself is not ahead of state"
        );
    }

    fn assert_checkpoint_store_send_sync<T: CheckpointStore + Send + Sync>() {}

    #[test]
    fn store_types_implement_thread_safe_checkpoint_store() {
        assert_checkpoint_store_send_sync::<InMemoryCheckpointStore>();
        assert_checkpoint_store_send_sync::<SqliteCheckpointStore>();
    }

    #[test]
    fn commit_requires_receipt_covering_package_and_segments() {
        assert_store_rejects_bad_receipts(&InMemoryCheckpointStore::new());
        assert_store_rejects_bad_receipts(&SqliteCheckpointStore::open_in_memory().unwrap());
    }

    #[test]
    fn abandon_keeps_proposed_checkpoint_out_of_head() {
        assert_abandon_keeps_proposed_checkpoint_out_of_head(&InMemoryCheckpointStore::new());
        assert_abandon_keeps_proposed_checkpoint_out_of_head(
            &SqliteCheckpointStore::open_in_memory().unwrap(),
        );
    }

    #[test]
    fn head_history_and_rewind_target_are_isolated_by_resource_and_scope() {
        assert_scope_and_resource_isolation(&InMemoryCheckpointStore::new());
        assert_scope_and_resource_isolation(&SqliteCheckpointStore::open_in_memory().unwrap());
    }

    #[test]
    fn rewind_rejects_non_committed_wrong_tuple_and_missing_targets() {
        assert_rewind_validation(&InMemoryCheckpointStore::new());
        assert_rewind_validation(&SqliteCheckpointStore::open_in_memory().unwrap());
    }

    #[test]
    fn rewind_rejects_committed_target_when_scope_has_no_head() {
        let in_memory = InMemoryCheckpointStore::new();
        let committed = commit_delta(
            &in_memory,
            delta(
                "checkpoint-no-head-memory",
                None,
                partition_scope(),
                cursor_position(1),
                "package-no-head-memory",
            ),
        );
        {
            let mut state = in_memory.inner.lock().unwrap();
            state
                .checkpoints
                .get_mut(&committed.delta.checkpoint_id)
                .unwrap()
                .is_head = false;
        }
        assert!(
            in_memory
                .rewind(RewindRequest {
                    marker_checkpoint_id: CheckpointId::new("rewind-no-head-memory").unwrap(),
                    pipeline_id: committed.delta.pipeline_id.clone(),
                    resource_id: committed.delta.resource_id.clone(),
                    scope: committed.delta.scope.clone(),
                    target_checkpoint_id: committed.delta.checkpoint_id,
                })
                .is_err()
        );

        let sqlite = SqliteCheckpointStore::open_in_memory().unwrap();
        let committed = commit_delta(
            &sqlite,
            delta(
                "checkpoint-no-head-sqlite",
                None,
                partition_scope(),
                cursor_position(1),
                "package-no-head-sqlite",
            ),
        );
        sqlite
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE firn_checkpoints SET is_head = 0 WHERE checkpoint_id = ?",
                params![committed.delta.checkpoint_id.as_str()],
            )
            .unwrap();
        assert!(
            sqlite
                .rewind(RewindRequest {
                    marker_checkpoint_id: CheckpointId::new("rewind-no-head-sqlite").unwrap(),
                    pipeline_id: committed.delta.pipeline_id.clone(),
                    resource_id: committed.delta.resource_id.clone(),
                    scope: committed.delta.scope.clone(),
                    target_checkpoint_id: committed.delta.checkpoint_id,
                })
                .is_err()
        );
    }

    #[test]
    fn sqlite_uses_wal_and_single_committed_head_index() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let store = SqliteCheckpointStore::open(&db_path).unwrap();

        let conn = store.conn.lock().unwrap();
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode, "wal");

        let index_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'firn_checkpoints_one_committed_head'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(index_sql.contains("WHERE is_head = 1 AND status = 'committed'"));
    }

    #[test]
    fn sqlite_head_move_remains_transactionally_unique_across_connections() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let first = SqliteCheckpointStore::open(&db_path).unwrap();
        let second = SqliteCheckpointStore::open(&db_path).unwrap();
        let scope = partition_scope();
        let first_delta = delta(
            "checkpoint-1",
            None,
            scope.clone(),
            cursor_position(1),
            "package-1",
        );
        let first_id = first_delta.checkpoint_id.clone();
        let second_delta = delta(
            "checkpoint-2",
            Some(&first_id),
            scope.clone(),
            cursor_position(2),
            "package-2",
        );
        first.propose(first_delta.clone()).unwrap();
        second.propose(second_delta.clone()).unwrap();

        first
            .commit(&first_delta.checkpoint_id, receipt(&first_delta))
            .unwrap();
        second
            .commit(&second_delta.checkpoint_id, receipt(&second_delta))
            .unwrap();

        let head_count: i64 = {
            let conn = second.conn.lock().unwrap();
            conn.query_row(
                    "SELECT COUNT(*) FROM firn_checkpoints WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? AND status = 'committed' AND is_head = 1",
                    params![
                        pipeline_id().as_str(),
                        resource_id().as_str(),
                        encode_json(&scope).unwrap(),
                    ],
                    |row| row.get(0),
                )
                .unwrap()
        };
        assert_eq!(head_count, 1);
        assert_eq!(
            second
                .head(&pipeline_id(), &resource_id(), &scope)
                .unwrap()
                .unwrap()
                .delta
                .checkpoint_id,
            second_delta.checkpoint_id
        );
    }

    #[test]
    fn rewind_appends_marker_and_reports_packages_ahead() {
        assert_rewind_appends_marker_and_reports_packages_ahead(&InMemoryCheckpointStore::new());
        assert_rewind_appends_marker_and_reports_packages_ahead(
            &SqliteCheckpointStore::open_in_memory().unwrap(),
        );
    }

    #[test]
    fn branch_rewind_reports_only_current_branch_packages_ahead() {
        assert_branch_rewind_reports_only_current_branch_ahead(&InMemoryCheckpointStore::new());
        assert_branch_rewind_reports_only_current_branch_ahead(
            &SqliteCheckpointStore::open_in_memory().unwrap(),
        );
    }

    fn assert_sqlite_row_corruption_is_rejected<F>(checkpoint_id: &str, mutate: F)
    where
        F: FnOnce(&SqliteCheckpointStore, &StateDelta) -> (PipelineId, ResourceId, ScopeKey),
    {
        let store = SqliteCheckpointStore::open_in_memory().unwrap();
        let committed = commit_delta(
            &store,
            delta(
                checkpoint_id,
                None,
                partition_scope(),
                cursor_position(1),
                &format!("package-{checkpoint_id}"),
            ),
        );
        let (pipeline_id, resource_id, scope) = mutate(&store, &committed.delta);
        assert!(
            store.head(&pipeline_id, &resource_id, &scope).is_err(),
            "corrupt scalar checkpoint row should be rejected during read"
        );
    }

    #[test]
    fn sqlite_rejects_rows_when_scalar_columns_disagree_with_delta_json() {
        assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-id", |store, delta| {
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE firn_checkpoints SET checkpoint_id = ? WHERE checkpoint_id = ?",
                    params!["checkpoint-corrupt-id-scalar", delta.checkpoint_id.as_str()],
                )
                .unwrap();
            (
                delta.pipeline_id.clone(),
                delta.resource_id.clone(),
                delta.scope.clone(),
            )
        });
        assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-pipeline", |store, delta| {
            let corrupt_pipeline = PipelineId::new("pipeline-corrupt").unwrap();
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE firn_checkpoints SET pipeline_id = ? WHERE checkpoint_id = ?",
                    params![corrupt_pipeline.as_str(), delta.checkpoint_id.as_str()],
                )
                .unwrap();
            (
                corrupt_pipeline,
                delta.resource_id.clone(),
                delta.scope.clone(),
            )
        });
        assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-resource", |store, delta| {
            let corrupt_resource = ResourceId::new("resource-corrupt").unwrap();
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE firn_checkpoints SET resource_id = ? WHERE checkpoint_id = ?",
                    params![corrupt_resource.as_str(), delta.checkpoint_id.as_str()],
                )
                .unwrap();
            (
                delta.pipeline_id.clone(),
                corrupt_resource,
                delta.scope.clone(),
            )
        });
        assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-scope", |store, delta| {
            let corrupt_scope = other_partition_scope();
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE firn_checkpoints SET scope_json = ? WHERE checkpoint_id = ?",
                    params![
                        encode_json(&corrupt_scope).unwrap(),
                        delta.checkpoint_id.as_str()
                    ],
                )
                .unwrap();
            (
                delta.pipeline_id.clone(),
                delta.resource_id.clone(),
                corrupt_scope,
            )
        });
        assert_sqlite_row_corruption_is_rejected(
            "checkpoint-corrupt-state-version",
            |store, delta| {
                let mut corrupt_delta = delta.clone();
                corrupt_delta.state_version = CHECKPOINT_STATE_VERSION + 1;
                store
                    .conn
                    .lock()
                    .unwrap()
                    .execute(
                        "UPDATE firn_checkpoints SET delta_json = ? WHERE checkpoint_id = ?",
                        params![
                            encode_json(&corrupt_delta).unwrap(),
                            delta.checkpoint_id.as_str()
                        ],
                    )
                    .unwrap();
                (
                    delta.pipeline_id.clone(),
                    delta.resource_id.clone(),
                    delta.scope.clone(),
                )
            },
        );
        assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-parent", |store, delta| {
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE firn_checkpoints SET parent_checkpoint_id = ? WHERE checkpoint_id = ?",
                    params!["checkpoint-other-parent", delta.checkpoint_id.as_str()],
                )
                .unwrap();
            (
                delta.pipeline_id.clone(),
                delta.resource_id.clone(),
                delta.scope.clone(),
            )
        });
        assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-input", |store, delta| {
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE firn_checkpoints SET input_position_json = ? WHERE checkpoint_id = ?",
                    params![
                        encode_json(&cursor_position(99)).unwrap(),
                        delta.checkpoint_id.as_str()
                    ],
                )
                .unwrap();
            (
                delta.pipeline_id.clone(),
                delta.resource_id.clone(),
                delta.scope.clone(),
            )
        });
        assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-output", |store, delta| {
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE firn_checkpoints SET output_position_json = ? WHERE checkpoint_id = ?",
                    params![
                        encode_json(&cursor_position(99)).unwrap(),
                        delta.checkpoint_id.as_str()
                    ],
                )
                .unwrap();
            (
                delta.pipeline_id.clone(),
                delta.resource_id.clone(),
                delta.scope.clone(),
            )
        });
        assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-package", |store, delta| {
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE firn_checkpoints SET package_hash = ? WHERE checkpoint_id = ?",
                    params!["package-corrupt", delta.checkpoint_id.as_str()],
                )
                .unwrap();
            (
                delta.pipeline_id.clone(),
                delta.resource_id.clone(),
                delta.scope.clone(),
            )
        });
        assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-schema", |store, delta| {
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE firn_checkpoints SET schema_hash = ? WHERE checkpoint_id = ?",
                    params!["schema-corrupt", delta.checkpoint_id.as_str()],
                )
                .unwrap();
            (
                delta.pipeline_id.clone(),
                delta.resource_id.clone(),
                delta.scope.clone(),
            )
        });
    }

    #[test]
    fn sqlite_rejects_committed_rows_when_receipt_id_disagrees_with_receipt_json() {
        let store = SqliteCheckpointStore::open_in_memory().unwrap();
        let committed = commit_delta(
            &store,
            delta(
                "checkpoint-corrupt-receipt-id",
                None,
                partition_scope(),
                cursor_position(1),
                "package-corrupt-receipt-id",
            ),
        );
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE firn_checkpoints SET receipt_id = ? WHERE checkpoint_id = ?",
                params!["receipt-other", committed.delta.checkpoint_id.as_str()],
            )
            .unwrap();

        assert!(
            store
                .head(
                    &committed.delta.pipeline_id,
                    &committed.delta.resource_id,
                    &committed.delta.scope,
                )
                .is_err()
        );
    }

    #[test]
    fn sqlite_round_trips_position_scope_and_state_json() {
        let mut composite_parts = BTreeMap::new();
        composite_parts.insert("cursor".to_owned(), cursor_position(1));
        composite_parts.insert(
            "log".to_owned(),
            SourcePosition::Log(LogPosition {
                version: 1,
                log: "orders".to_owned(),
                offset: 7,
                sequence: Some("abc".to_owned()),
            }),
        );

        let positions = vec![
            cursor_position(1),
            SourcePosition::Log(LogPosition {
                version: 1,
                log: "orders".to_owned(),
                offset: 42,
                sequence: Some("def".to_owned()),
            }),
            SourcePosition::FileManifest(FileManifest {
                version: 1,
                files: vec![FilePosition {
                    path: "orders-1.jsonl".to_owned(),
                    size_bytes: 1024,
                    etag: Some("etag-1".to_owned()),
                    sha256: Some("file-sha256".to_owned()),
                }],
            }),
            SourcePosition::PageToken(PageToken {
                version: 1,
                token: "next-page".to_owned(),
            }),
            SourcePosition::Composite(CompositePosition {
                version: 1,
                positions: composite_parts,
            }),
            SourcePosition::ForeignState(ForeignState {
                version: 1,
                protocol: "singer".to_owned(),
                opaque_blob: b"{\"bookmarks\":{}}".to_vec(),
                blob_sha256: "state-sha256".to_owned(),
            }),
        ];
        let scopes = vec![
            ScopeKey::Resource,
            partition_scope(),
            ScopeKey::Window {
                start: "2026-07-01T00:00:00Z".to_owned(),
                end: "2026-07-02T00:00:00Z".to_owned(),
            },
            ScopeKey::File {
                path: "orders-1.jsonl".to_owned(),
            },
            ScopeKey::Stream {
                name: "orders".to_owned(),
            },
            ScopeKey::SchemaContract {
                contract: ContractRef::new("orders-contract").unwrap(),
            },
            ScopeKey::DestinationLoad {
                destination: DestinationId::new("duckdb-local").unwrap(),
                target: TargetName::new("orders").unwrap(),
            },
            ScopeKey::Composite {
                parts: vec![
                    partition_scope(),
                    ScopeKey::Stream {
                        name: "orders".to_owned(),
                    },
                ],
            },
        ];

        let store = SqliteCheckpointStore::open_in_memory().unwrap();
        for (index, position) in positions.into_iter().enumerate() {
            let scope = scopes[index].clone();
            let delta = delta(
                &format!("checkpoint-roundtrip-{index}"),
                None,
                scope.clone(),
                position,
                &format!("package-roundtrip-{index}"),
            );
            let checkpoint = commit_delta(&store, delta.clone());
            let head = store
                .head(&delta.pipeline_id, &delta.resource_id, &scope)
                .unwrap()
                .unwrap();
            assert_eq!(head.delta, checkpoint.delta);
            assert_eq!(head.delta.scope, scope);
            assert_eq!(head.delta.state_version, CHECKPOINT_STATE_VERSION);
        }

        for (index, scope) in scopes.into_iter().enumerate().skip(positions_count()) {
            let delta = delta(
                &format!("checkpoint-scope-roundtrip-{index}"),
                None,
                scope.clone(),
                cursor_position(index as i64),
                &format!("package-scope-roundtrip-{index}"),
            );
            commit_delta(&store, delta.clone());
            assert_eq!(
                store
                    .head(&delta.pipeline_id, &delta.resource_id, &scope)
                    .unwrap()
                    .unwrap()
                    .delta
                    .scope,
                scope
            );
        }

        let mut unsupported = delta(
            "checkpoint-unsupported-state",
            None,
            partition_scope(),
            cursor_position(99),
            "package-unsupported-state",
        );
        unsupported.state_version = CHECKPOINT_STATE_VERSION + 1;
        assert!(store.propose(unsupported).is_err());
    }

    #[test]
    fn in_memory_rejects_unsupported_state_version_without_sqlite_constraints() {
        let store = InMemoryCheckpointStore::new();
        let mut unsupported = delta(
            "checkpoint-memory-unsupported-state",
            None,
            partition_scope(),
            cursor_position(99),
            "package-memory-unsupported-state",
        );
        unsupported.state_version = CHECKPOINT_STATE_VERSION + 1;
        assert!(store.propose(unsupported).is_err());
        assert!(
            store
                .history(&pipeline_id(), &resource_id(), &partition_scope())
                .unwrap()
                .is_empty()
        );
    }

    fn positions_count() -> usize {
        6
    }
}
