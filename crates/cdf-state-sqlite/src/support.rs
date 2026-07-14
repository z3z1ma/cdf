use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, Checkpoint, CheckpointId, CheckpointStatus, PackageHash,
    PipelineId, Receipt, ResourceId, Result, RewindRequest, ScopeKey, StateDelta, StateSegment,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::de::DeserializeOwned;

pub(crate) fn rewind_marker(
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

pub(crate) fn packages_ahead_of_state(
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

pub(crate) fn verify_receipt(receipt: &Receipt, delta: &StateDelta) -> Result<()> {
    if !receipt.covers_state_delta(delta)
        || !receipt_matches_segment_counts(receipt, &delta.segments)
    {
        return Err(CdfError::contract(format!(
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

pub(crate) fn same_tuple(
    delta: &StateDelta,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
) -> bool {
    delta.pipeline_id == *pipeline_id && delta.resource_id == *resource_id && delta.scope == *scope
}

pub(crate) fn validate_state_version(state_version: u16) -> Result<()> {
    if state_version == CHECKPOINT_STATE_VERSION {
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "unsupported checkpoint state version {state_version}"
        )))
    }
}

pub(crate) fn encode_json<T: serde::Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|error| CdfError::data(error.to_string()))
}

pub(crate) fn decode_json<T: DeserializeOwned>(json: &str, state_version: u16) -> Result<T> {
    validate_state_version(state_version)?;
    serde_json::from_str(json).map_err(|error| CdfError::data(error.to_string()))
}

pub(crate) fn missing_checkpoint(checkpoint_id: &CheckpointId) -> CdfError {
    CdfError::contract(format!("checkpoint {checkpoint_id} does not exist"))
}

pub(crate) fn now_ms() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CdfError::internal(error.to_string()))?;
    i64::try_from(elapsed.as_millis()).map_err(|error| CdfError::internal(error.to_string()))
}

pub(crate) fn ensure_schema_version_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS cdf_sqlite_schema_versions (
            component TEXT PRIMARY KEY,
            version INTEGER NOT NULL,
            recorded_at_ms INTEGER NOT NULL
        );
        ",
    )
    .map_err(sqlite_error)
}

fn schema_version_table_exists(conn: &Connection) -> Result<bool> {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cdf_sqlite_schema_versions'",
        [],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(sqlite_error)
}

pub(crate) fn read_component_schema_version(
    conn: &Connection,
    component: &str,
) -> Result<Option<i64>> {
    if !schema_version_table_exists(conn)? {
        return Ok(None);
    }
    conn.query_row(
        "SELECT version FROM cdf_sqlite_schema_versions WHERE component = ?",
        params![component],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(sqlite_error)
}

pub(crate) fn write_component_schema_version(
    conn: &Connection,
    component: &str,
    version: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO cdf_sqlite_schema_versions (component, version, recorded_at_ms)
         VALUES (?, ?, ?)
         ON CONFLICT(component) DO NOTHING",
        params![component, version, now_ms()?],
    )
    .map(|_| ())
    .map_err(sqlite_error)
}

pub(crate) fn sqlite_table_exists(conn: &Connection, table: &str) -> Result<bool> {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        params![table],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(sqlite_error)
}

pub(crate) fn require_sqlite_tables(
    conn: &Connection,
    component: &str,
    tables: &[&str],
) -> Result<()> {
    for table in tables {
        if !sqlite_table_exists(conn, table)? {
            return Err(CdfError::internal(format!(
                "{component} SQLite schema is incomplete; required table {table} is missing"
            )));
        }
    }
    Ok(())
}

pub(crate) fn sqlite_error(error: rusqlite::Error) -> CdfError {
    CdfError::internal(error.to_string())
}

pub(crate) fn lock_error<T>(error: std::sync::PoisonError<T>) -> CdfError {
    CdfError::internal(error.to_string())
}
