use crate::*;
use crate::{api::*, sql::*};

pub(crate) fn ensure_mirror_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS _firn_loads (
            target VARCHAR NOT NULL,
            idempotency_token VARCHAR NOT NULL,
            package_hash VARCHAR NOT NULL,
            destination VARCHAR NOT NULL,
            disposition VARCHAR NOT NULL,
            schema_hash VARCHAR NOT NULL,
            rows_written UBIGINT NOT NULL,
            rows_inserted UBIGINT,
            rows_updated UBIGINT,
            rows_deleted UBIGINT,
            receipt_id VARCHAR NOT NULL,
            receipt_json VARCHAR NOT NULL,
            committed_at_ms BIGINT NOT NULL,
            PRIMARY KEY (target, idempotency_token)
        );
        CREATE TABLE IF NOT EXISTS _firn_state (
            target VARCHAR NOT NULL,
            package_hash VARCHAR NOT NULL,
            segment_id VARCHAR NOT NULL,
            idempotency_token VARCHAR NOT NULL,
            scope_json VARCHAR,
            output_position_json VARCHAR,
            row_count UBIGINT NOT NULL,
            byte_count UBIGINT NOT NULL,
            committed_at_ms BIGINT NOT NULL,
            PRIMARY KEY (target, package_hash, segment_id)
        );
        "#,
    )
    .map_err(|error| duckdb_error("create DuckDB firn mirror tables", error))
}

pub(crate) fn find_duplicate_receipt(
    conn: &Connection,
    request: &DestinationCommitRequest,
) -> Result<Option<Receipt>> {
    let receipt_json: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM _firn_loads WHERE target = ? AND idempotency_token = ?",
            params![request.target.as_str(), request.idempotency_token.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| duckdb_error("query DuckDB idempotency mirror", error))?;
    receipt_json
        .map(|json| serde_json::from_str(&json).map_err(json_error))
        .transpose()
}

pub(crate) fn insert_mirrors(
    conn: &Connection,
    request: &DuckDbCommitRequest,
    segment_acks: &[SegmentAck],
    receipt: &Receipt,
) -> Result<()> {
    let receipt_json = serde_json::to_string(receipt).map_err(json_error)?;
    conn.execute(
        "INSERT INTO _firn_loads \
         (target, idempotency_token, package_hash, destination, disposition, schema_hash, rows_written, rows_inserted, rows_updated, rows_deleted, receipt_id, receipt_json, committed_at_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            receipt.target.as_str(),
            receipt.idempotency_token.as_str(),
            receipt.package_hash.as_str(),
            receipt.destination.as_str(),
            disposition_name(&receipt.disposition),
            receipt.schema_hash.as_str(),
            receipt.counts.rows_written,
            receipt.counts.rows_inserted,
            receipt.counts.rows_updated,
            receipt.counts.rows_deleted,
            receipt.receipt_id.as_str(),
            receipt_json,
            receipt.committed_at_ms,
        ],
    )
    .map_err(|error| duckdb_error("insert DuckDB _firn_loads row", error))?;

    let state_by_segment = request
        .commit
        .segments
        .iter()
        .map(|segment| (segment.segment_id.as_str(), segment))
        .collect::<BTreeMap<_, _>>();
    for ack in segment_acks {
        let state = state_by_segment.get(ack.segment_id.as_str()).copied();
        let scope_json = state
            .map(|segment| serde_json::to_string(&segment.scope).map_err(json_error))
            .transpose()?;
        let position_json = state
            .map(|segment| serde_json::to_string(&segment.output_position).map_err(json_error))
            .transpose()?;
        conn.execute(
            "INSERT INTO _firn_state \
             (target, package_hash, segment_id, idempotency_token, scope_json, output_position_json, row_count, byte_count, committed_at_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                receipt.target.as_str(),
                receipt.package_hash.as_str(),
                ack.segment_id.as_str(),
                receipt.idempotency_token.as_str(),
                scope_json,
                position_json,
                ack.row_count,
                ack.byte_count,
                receipt.committed_at_ms,
            ],
        )
        .map_err(|error| duckdb_error("insert DuckDB _firn_state row", error))?;
    }
    Ok(())
}
