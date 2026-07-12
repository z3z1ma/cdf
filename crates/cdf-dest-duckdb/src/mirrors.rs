use crate::*;
use crate::{api::*, sql::*};

pub(crate) fn ensure_mirror_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS _cdf_row_key_allocator (
            singleton BOOLEAN PRIMARY KEY CHECK (singleton),
            next_key UBIGINT NOT NULL
        );
        INSERT OR IGNORE INTO _cdf_row_key_allocator VALUES (true, 1);
        CREATE TABLE IF NOT EXISTS _cdf_loads (
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
        CREATE TABLE IF NOT EXISTS _cdf_state (
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
        CREATE TABLE IF NOT EXISTS _cdf_segments (
            row_key_start UBIGINT NOT NULL,
            row_key_end UBIGINT NOT NULL,
            target VARCHAR NOT NULL,
            package_hash VARCHAR NOT NULL,
            segment_id VARCHAR NOT NULL,
            PRIMARY KEY (row_key_start),
            UNIQUE (target, package_hash, segment_id)
        );
        "#,
    )
    .map_err(|error| duckdb_error("create DuckDB cdf mirror tables", error))
}

pub(crate) fn allocate_row_keys(conn: &Connection, row_count: u64) -> Result<Option<u64>> {
    if row_count == 0 {
        return Ok(None);
    }
    let start: u64 = conn
        .query_row(
            "SELECT next_key FROM _cdf_row_key_allocator WHERE singleton",
            [],
            |row| row.get(0),
        )
        .map_err(|error| duckdb_error("read DuckDB row-key allocator", error))?;
    let end = start
        .checked_add(row_count)
        .ok_or_else(|| CdfError::data("DuckDB row-key allocator overflowed u64"))?;
    conn.execute(
        "UPDATE _cdf_row_key_allocator SET next_key = ? WHERE singleton",
        params![end],
    )
    .map_err(|error| duckdb_error("advance DuckDB row-key allocator", error))?;
    Ok(Some(start))
}

pub(crate) fn find_duplicate_receipt(
    conn: &Connection,
    request: &DestinationCommitRequest,
) -> Result<Option<Receipt>> {
    let receipt_json: Option<String> = conn
        .query_row(
            "SELECT receipt_json FROM _cdf_loads WHERE target = ? AND idempotency_token = ?",
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
    commit: &DestinationCommitRequest,
    segment_acks: &[SegmentAck],
    receipt: &Receipt,
    first_row_key: Option<u64>,
) -> Result<()> {
    let receipt_json = serde_json::to_string(receipt).map_err(json_error)?;
    conn.execute(
        "INSERT INTO _cdf_loads \
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
    .map_err(|error| duckdb_error("insert DuckDB _cdf_loads row", error))?;

    let state_by_segment = commit
        .segments
        .iter()
        .map(|segment| (segment.segment_id.as_str(), segment))
        .collect::<BTreeMap<_, _>>();
    let mut next_row_key = first_row_key;
    for ack in segment_acks {
        let state = state_by_segment.get(ack.segment_id.as_str()).copied();
        let scope_json = state
            .map(|segment| serde_json::to_string(&segment.scope).map_err(json_error))
            .transpose()?;
        let position_json = state
            .map(|segment| serde_json::to_string(&segment.output_position).map_err(json_error))
            .transpose()?;
        conn.execute(
            "INSERT INTO _cdf_state \
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
        .map_err(|error| duckdb_error("insert DuckDB _cdf_state row", error))?;
        if let Some(row_key_start) = next_row_key {
            let row_key_end = row_key_start
                .checked_add(ack.row_count)
                .ok_or_else(|| CdfError::data("DuckDB segment row-key range overflowed"))?;
            conn.execute(
                "INSERT INTO _cdf_segments (row_key_start, row_key_end, target, package_hash, segment_id) VALUES (?, ?, ?, ?, ?)",
                params![
                    row_key_start,
                    row_key_end,
                    receipt.target.as_str(),
                    receipt.package_hash.as_str(),
                    ack.segment_id.as_str(),
                ],
            )
            .map_err(|error| duckdb_error("insert DuckDB _cdf_segments row", error))?;
            next_row_key = Some(row_key_end);
        }
    }
    Ok(())
}

pub(crate) fn read_mirror_snapshot(conn: &Connection) -> Result<DuckDbMirrorSnapshot> {
    let loads_table_present = table_exists(conn, "_cdf_loads")?;
    let state_table_present = table_exists(conn, "_cdf_state")?;
    Ok(DuckDbMirrorSnapshot {
        loads_table_present,
        state_table_present,
        loads: if loads_table_present {
            read_load_rows(conn)?
        } else {
            Vec::new()
        },
        state: if state_table_present {
            read_state_rows(conn)?
        } else {
            Vec::new()
        },
    })
}

fn table_exists(conn: &Connection, table_name: &str) -> Result<bool> {
    conn.query_row(
        "SELECT count(*) > 0 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
        params![table_name],
        |row| row.get(0),
    )
    .map_err(|error| duckdb_error(format!("query DuckDB mirror table presence for {table_name}"), error))
}

fn read_load_rows(conn: &Connection) -> Result<Vec<DuckDbMirrorLoadRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT target, idempotency_token, package_hash, receipt_id, receipt_json \
             FROM _cdf_loads ORDER BY target, idempotency_token",
        )
        .map_err(|error| duckdb_error("prepare DuckDB _cdf_loads snapshot query", error))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(DuckDbMirrorLoadRow {
                target: row.get(0)?,
                idempotency_token: row.get(1)?,
                package_hash: row.get(2)?,
                receipt_id: row.get(3)?,
                receipt_json: row.get(4)?,
            })
        })
        .map_err(|error| duckdb_error("query DuckDB _cdf_loads snapshot", error))?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| duckdb_error("read DuckDB _cdf_loads snapshot row", error))
}

fn read_state_rows(conn: &Connection) -> Result<Vec<DuckDbMirrorStateRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT target, package_hash, segment_id, scope_json, output_position_json, row_count, byte_count \
             FROM _cdf_state ORDER BY target, package_hash, segment_id",
        )
        .map_err(|error| duckdb_error("prepare DuckDB _cdf_state snapshot query", error))?;
    let rows = stmt
        .query_map([], |row| {
            Ok(DuckDbMirrorStateRow {
                target: row.get(0)?,
                package_hash: row.get(1)?,
                segment_id: row.get(2)?,
                scope_json: row.get(3)?,
                output_position_json: row.get(4)?,
                row_count: row.get(5)?,
                byte_count: row.get(6)?,
            })
        })
        .map_err(|error| duckdb_error("query DuckDB _cdf_state snapshot", error))?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| duckdb_error("read DuckDB _cdf_state snapshot row", error))
}
