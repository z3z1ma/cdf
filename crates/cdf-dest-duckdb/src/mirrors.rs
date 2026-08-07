use cdf_dest_sql::{
    LoadMirrorKey, LoadMirrorMutation, LoadMirrorRow, MirrorCommit, MirrorInsertOutcome,
    QuarantineMirrorKey, QuarantineMirrorMutation, QuarantineMirrorRow, SegmentMirrorMutation,
    SegmentMirrorPolicy, SegmentMirrorRow, SegmentRowRange, StateMirrorKey, StateMirrorMutation,
    StateMirrorRow, TransactionalMirrorBackend, TransactionalMirrorManager,
};
use cdf_kernel::{
    CdfError, CommitCounts, CommitPlan, DestinationCommitRequest, DestinationId, IdempotencyToken,
    MigrationRecord, Receipt, ReceiptId, Result, SchemaHash, SegmentAck, WriteDisposition,
};
use duckdb::{Connection, OptionalExt, params};

use crate::{
    DESTINATION_ID,
    models::{DuckDbMirrorLoadRow, DuckDbMirrorSnapshot, DuckDbMirrorStateRow},
    sql::{disposition_name, duckdb_error, json_error},
};

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
            segment_count UBIGINT NOT NULL,
            migrations_json VARCHAR NOT NULL,
            receipt_id VARCHAR NOT NULL,
            receipt_json VARCHAR NOT NULL,
            committed_at_ms BIGINT NOT NULL,
            PRIMARY KEY (target, idempotency_token)
        );
        ALTER TABLE _cdf_loads ADD COLUMN IF NOT EXISTS segment_count UBIGINT;
        ALTER TABLE _cdf_loads ADD COLUMN IF NOT EXISTS migrations_json VARCHAR;
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

pub(crate) fn next_row_key(conn: &Connection) -> Result<u64> {
    conn.query_row(
        "SELECT next_key FROM _cdf_row_key_allocator WHERE singleton",
        [],
        |row| row.get(0),
    )
    .map_err(|error| duckdb_error("read DuckDB row-key allocator", error))
}

pub(crate) fn advance_row_key_allocator(
    conn: &Connection,
    expected_start: u64,
    next_key: u64,
) -> Result<()> {
    if next_key < expected_start {
        return Err(CdfError::data("DuckDB row-key allocator moved backwards"));
    }
    let changed = conn
        .execute(
            "UPDATE _cdf_row_key_allocator SET next_key = ? WHERE singleton AND next_key = ?",
            params![next_key, expected_start],
        )
        .map_err(|error| duckdb_error("advance DuckDB row-key allocator", error))?;
    if changed != 1 {
        return Err(CdfError::destination(
            "DuckDB row-key allocator changed during an exclusive staged transaction",
        ));
    }
    Ok(())
}

pub(crate) fn find_duplicate_receipt(
    conn: &Connection,
    request: &DestinationCommitRequest,
    plan: &CommitPlan,
    schema_hash: &SchemaHash,
    segment_acks: &[SegmentAck],
) -> Result<Option<Receipt>> {
    find_duplicate_receipt_with(
        conn,
        &LoadMirrorKey {
            target: request.target.clone(),
            package_hash: request.package_hash.clone(),
            idempotency_token: request.idempotency_token.clone(),
        },
        |stored| expected_duckdb_duplicate(stored, request, plan, schema_hash, segment_acks),
    )
}

pub(crate) fn find_duplicate_receipt_with<F>(
    conn: &Connection,
    key: &LoadMirrorKey,
    expected_logical_receipt: F,
) -> Result<Option<Receipt>>
where
    F: FnOnce(&Receipt) -> Result<Receipt>,
{
    let mut backend = DuckDbMirrorBackend { conn };
    TransactionalMirrorManager::new(&mut backend).find_duplicate(key, expected_logical_receipt)
}

struct DuckDbMirrorBackend<'a> {
    conn: &'a Connection,
}

impl TransactionalMirrorBackend for DuckDbMirrorBackend<'_> {
    fn read_load(&mut self, key: &LoadMirrorKey) -> Result<Option<LoadMirrorRow>> {
        read_load(self.conn, key)
    }

    fn insert_load(
        &mut self,
        mutation: &LoadMirrorMutation,
    ) -> Result<MirrorInsertOutcome<LoadMirrorRow>> {
        insert_load(self.conn, mutation)?;
        read_load(self.conn, &mutation.key())?
            .map(MirrorInsertOutcome::Inserted)
            .ok_or_else(|| CdfError::destination("DuckDB load mirror is absent after insertion"))
    }

    fn read_state(&mut self, _key: &StateMirrorKey) -> Result<Option<StateMirrorRow>> {
        Ok(None)
    }

    fn upsert_state(
        &mut self,
        _mutation: &StateMirrorMutation,
    ) -> Result<MirrorInsertOutcome<StateMirrorRow>> {
        Err(CdfError::internal(
            "DuckDB mirror backend received unsupported checkpoint-head state mutation",
        ))
    }

    fn insert_segment(
        &mut self,
        mutation: &SegmentMirrorMutation,
    ) -> Result<MirrorInsertOutcome<SegmentMirrorRow>> {
        insert_segment(self.conn, mutation)?;
        read_mirror_segment(self.conn, mutation)?
            .map(MirrorInsertOutcome::Inserted)
            .ok_or_else(|| CdfError::destination("DuckDB segment mirror is absent after insertion"))
    }

    fn read_mirror_segment(
        &mut self,
        mutation: &SegmentMirrorMutation,
    ) -> Result<Option<SegmentMirrorRow>> {
        read_mirror_segment(self.conn, mutation)
    }

    fn insert_quarantine(
        &mut self,
        _mutation: &QuarantineMirrorMutation,
    ) -> Result<MirrorInsertOutcome<QuarantineMirrorRow>> {
        Err(CdfError::internal(
            "DuckDB mirror backend received unsupported quarantine mutation",
        ))
    }

    fn read_quarantine(
        &mut self,
        _key: &QuarantineMirrorKey,
    ) -> Result<Option<QuarantineMirrorRow>> {
        Err(CdfError::internal(
            "DuckDB mirror backend received unsupported quarantine readback",
        ))
    }
}

fn expected_duckdb_duplicate(
    stored: &Receipt,
    request: &DestinationCommitRequest,
    plan: &CommitPlan,
    schema_hash: &SchemaHash,
    segment_acks: &[SegmentAck],
) -> Result<Receipt> {
    if plan.target != request.target || plan.disposition != request.disposition {
        return Err(CdfError::contract(
            "DuckDB duplicate request differs from its typed commit plan",
        ));
    }
    if !plan.migrations.is_empty() && plan.migrations != stored.migrations {
        return Err(CdfError::destination(
            "DuckDB duplicate receipt migrations differ from the applicable commit plan",
        ));
    }
    validate_duckdb_duplicate_counts(stored, segment_acks)?;
    let mut expected = stored.clone();
    expected.receipt_id = ReceiptId::new(format!(
        "duckdb:{}:{}",
        request.target.as_str(),
        request.idempotency_token.as_str()
    ))?;
    expected.destination = DestinationId::new(DESTINATION_ID)?;
    expected.target = request.target.clone();
    expected.package_hash = request.package_hash.clone();
    expected.segment_acks = segment_acks.to_vec();
    expected.disposition = request.disposition.clone();
    expected.idempotency_token = request.idempotency_token.clone();
    expected.schema_hash = schema_hash.clone();
    // A replay plans against the already-migrated target, so its current dry plan legitimately
    // omits the historical DDL recorded by the first physical commit. The stored receipt remains
    // the migration authority; generic package receipt reconciliation compares it to the
    // package-recorded logical receipt before checkpointing.
    expected.migrations = stored.migrations.clone();
    Ok(expected)
}

fn validate_duckdb_duplicate_counts(stored: &Receipt, segment_acks: &[SegmentAck]) -> Result<()> {
    let rows = segment_acks.iter().try_fold(0_u64, |total, ack| {
        total
            .checked_add(ack.row_count)
            .ok_or_else(|| CdfError::data("DuckDB duplicate row count overflowed"))
    })?;
    let counts = &stored.counts;
    let valid = if segment_acks.is_empty() {
        match (&stored.content, counts) {
            (
                cdf_kernel::PackageContentAuthority::Rows { .. },
                CommitCounts::Rows {
                    rows_written: 0,
                    rows_inserted: None,
                    rows_updated: None,
                    rows_deleted: None,
                },
            ) => true,
            (
                cdf_kernel::PackageContentAuthority::KeyedChanges { reduction, .. },
                CommitCounts::KeyedChanges {
                    intent,
                    rows_inserted: Some(0),
                    rows_updated: Some(0),
                    hard_deletes: None,
                    soft_deletes: None,
                    missing_delete_keys: None,
                    ignored_deletes: None,
                },
            ) => *intent == reduction.surviving,
            _ => false,
        }
    } else {
        match (&stored.disposition, counts) {
            (
                WriteDisposition::Append | WriteDisposition::Replace,
                CommitCounts::Rows {
                    rows_written,
                    rows_inserted,
                    rows_updated,
                    rows_deleted,
                },
            ) => {
                *rows_written == rows
                    && *rows_inserted == Some(rows)
                    && *rows_updated == Some(0)
                    && *rows_deleted == Some(0)
            }
            (
                WriteDisposition::Merge,
                CommitCounts::KeyedChanges {
                    intent,
                    rows_inserted,
                    rows_updated,
                    hard_deletes,
                    soft_deletes,
                    missing_delete_keys,
                    ignored_deletes,
                },
            ) => {
                intent.upserts == rows
                    && intent.deletes == 0
                    && rows_inserted
                        .zip(*rows_updated)
                        .is_some_and(|(inserted, updated)| {
                            inserted.checked_add(updated) == Some(rows)
                        })
                    && hard_deletes.is_none()
                    && soft_deletes.is_none()
                    && missing_delete_keys.is_none()
                    && ignored_deletes.is_none()
            }
            _ => false,
        }
    };
    if valid {
        Ok(())
    } else {
        Err(CdfError::destination(
            "DuckDB duplicate receipt counts contradict its segment acknowledgements",
        ))
    }
}

type DuckDbLoadEvidenceRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    u64,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<String>,
    i64,
);

fn read_load(conn: &Connection, key: &LoadMirrorKey) -> Result<Option<LoadMirrorRow>> {
    let row: Option<DuckDbLoadEvidenceRow> = conn
        .query_row(
            "SELECT receipt_json, receipt_id, destination, target, package_hash, idempotency_token, disposition, schema_hash, rows_written, rows_inserted, rows_updated, rows_deleted, segment_count, migrations_json, committed_at_ms \
             FROM _cdf_loads WHERE target = ? AND package_hash = ? AND idempotency_token = ?",
            params![
                key.target.as_str(),
                key.package_hash.as_str(),
                key.idempotency_token.as_str()
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                ))
            },
        )
        .optional()
        .map_err(|error| duckdb_error("query DuckDB idempotency mirror", error))?;
    row.map(decode_duckdb_load_row).transpose()
}

fn decode_duckdb_load_row(row: DuckDbLoadEvidenceRow) -> Result<LoadMirrorRow> {
    let (
        receipt_json,
        receipt_id,
        destination,
        target,
        package_hash,
        idempotency_token,
        disposition,
        schema_hash,
        rows_written,
        rows_inserted,
        rows_updated,
        rows_deleted,
        segment_count,
        migrations_json,
        committed_at_ms,
    ) = row;
    let receipt: Receipt = serde_json::from_str(&receipt_json).map_err(json_error)?;
    let migrations_json = migrations_json.ok_or_else(|| {
        CdfError::data(
            "DuckDB load mirror predates independent migration evidence; replay fails closed",
        )
    })?;
    let migrations: Vec<MigrationRecord> =
        serde_json::from_str(&migrations_json).map_err(json_error)?;
    let segment_count = segment_count.ok_or_else(|| {
        CdfError::data(
            "DuckDB load mirror predates independent segment-count evidence; replay fails closed",
        )
    })?;
    if receipt.receipt_id.as_str() != receipt_id
        || receipt.destination.as_str() != destination
        || receipt.target.as_str() != target
        || receipt.package_hash.as_str() != package_hash
        || receipt.idempotency_token.as_str() != idempotency_token
        || disposition_name(&receipt.disposition) != disposition
        || receipt.schema_hash.as_str() != schema_hash
        || indexed_counts(&receipt.counts)
            != (rows_written, rows_inserted, rows_updated, rows_deleted)
        || receipt.segment_acks.len() as u64 != segment_count
        || receipt.migrations != migrations
        || receipt.committed_at_ms != committed_at_ms
    {
        return Err(CdfError::data(
            "DuckDB receipt JSON differs from independently stored load evidence",
        ));
    }
    Ok(LoadMirrorRow { receipt })
}

pub(crate) fn insert_mirrors(
    conn: &Connection,
    commit: &DestinationCommitRequest,
    segment_acks: &[SegmentAck],
    receipt: &Receipt,
    first_row_key: Option<u64>,
    segment_identities: Option<&[cdf_runtime::StagedSegmentIdentity]>,
) -> Result<()> {
    let row_ranges = segment_row_ranges(segment_acks, first_row_key, segment_identities)?;
    let commit = MirrorCommit::new(
        receipt.clone(),
        None,
        None,
        &commit.segments,
        row_ranges,
        SegmentMirrorPolicy::Persist {
            require_row_ranges: false,
        },
    )?;
    let mut backend = DuckDbMirrorBackend { conn };
    TransactionalMirrorManager::new(&mut backend)
        .apply(commit)
        .map(|_| ())
}

fn insert_load(conn: &Connection, mutation: &LoadMirrorMutation) -> Result<()> {
    let receipt = &mutation.receipt;
    let receipt_json = serde_json::to_string(receipt).map_err(json_error)?;
    let migrations_json = serde_json::to_string(&receipt.migrations).map_err(json_error)?;
    let (rows_written, rows_inserted, rows_updated, rows_deleted) = indexed_counts(&receipt.counts);
    conn.execute(
        "INSERT INTO _cdf_loads \
         (target, idempotency_token, package_hash, destination, disposition, schema_hash, rows_written, rows_inserted, rows_updated, rows_deleted, segment_count, migrations_json, receipt_id, receipt_json, committed_at_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            receipt.target.as_str(),
            receipt.idempotency_token.as_str(),
            receipt.package_hash.as_str(),
            receipt.destination.as_str(),
            disposition_name(&receipt.disposition),
            receipt.schema_hash.as_str(),
            rows_written,
            rows_inserted,
            rows_updated,
            rows_deleted,
            receipt.segment_acks.len() as u64,
            migrations_json,
            receipt.receipt_id.as_str(),
            receipt_json,
            receipt.committed_at_ms,
        ],
    )
    .map_err(|error| duckdb_error("insert DuckDB _cdf_loads row", error))?;
    Ok(())
}

fn indexed_counts(counts: &CommitCounts) -> (u64, Option<u64>, Option<u64>, Option<u64>) {
    match counts {
        CommitCounts::Rows {
            rows_written,
            rows_inserted,
            rows_updated,
            rows_deleted,
        } => (*rows_written, *rows_inserted, *rows_updated, *rows_deleted),
        CommitCounts::KeyedChanges {
            intent,
            rows_inserted,
            rows_updated,
            hard_deletes,
            soft_deletes,
            ..
        } => (
            intent.upserts,
            *rows_inserted,
            *rows_updated,
            hard_deletes.or(*soft_deletes),
        ),
    }
}

fn insert_segment(conn: &Connection, mutation: &SegmentMirrorMutation) -> Result<()> {
    let scope_json = mutation
        .scope
        .as_ref()
        .map(|scope| serde_json::to_string(scope).map_err(json_error))
        .transpose()?;
    let position_json = mutation
        .output_position
        .as_ref()
        .map(|position| serde_json::to_string(position).map_err(json_error))
        .transpose()?;
    conn.execute(
        "INSERT INTO _cdf_state \
         (target, package_hash, segment_id, idempotency_token, scope_json, output_position_json, row_count, byte_count, committed_at_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            mutation.target.as_str(),
            mutation.package_hash.as_str(),
            mutation.segment_id.as_str(),
            mutation.idempotency_token.as_str(),
            scope_json,
            position_json,
            mutation.row_count,
            mutation.byte_count,
            mutation.committed_at_ms,
        ],
    )
    .map_err(|error| duckdb_error("insert DuckDB _cdf_state row", error))?;
    if let Some(range) = &mutation.row_range {
        let changed = conn
            .execute(
            "INSERT INTO _cdf_segments (row_key_start, row_key_end, target, package_hash, segment_id) \
             SELECT ?, ?, ?, ?, ? \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM _cdf_segments \
                 WHERE row_key_start < ? AND row_key_end > ? \
             )",
            params![
                range.row_key_start,
                range.row_key_end,
                mutation.target.as_str(),
                mutation.package_hash.as_str(),
                mutation.segment_id.as_str(),
                range.row_key_end,
                range.row_key_start,
            ],
        )
        .map_err(|error| duckdb_error("insert DuckDB _cdf_segments row", error))?;
        if changed != 1 {
            return Err(CdfError::data(
                "DuckDB segment row range overlaps committed provenance",
            ));
        }
    }
    Ok(())
}

fn read_mirror_segment(
    conn: &Connection,
    mutation: &SegmentMirrorMutation,
) -> Result<Option<SegmentMirrorRow>> {
    type Row = (
        String,
        Option<String>,
        Option<String>,
        u64,
        u64,
        i64,
        Option<u64>,
        Option<u64>,
    );
    let row: Option<Row> = conn
        .query_row(
            "SELECT \"state\".idempotency_token, \"state\".scope_json, \"state\".output_position_json, \
             \"state\".row_count, \"state\".byte_count, \"state\".committed_at_ms, \
             \"range\".row_key_start, \"range\".row_key_end \
             FROM _cdf_state AS \"state\" \
             LEFT JOIN _cdf_segments AS \"range\" \
               ON \"range\".target = \"state\".target \
              AND \"range\".package_hash = \"state\".package_hash \
              AND \"range\".segment_id = \"state\".segment_id \
             WHERE \"state\".target = ? AND \"state\".package_hash = ? AND \"state\".segment_id = ?",
            params![
                mutation.target.as_str(),
                mutation.package_hash.as_str(),
                mutation.segment_id.as_str()
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .optional()
        .map_err(|error| duckdb_error("read DuckDB segment mirror", error))?;
    row.map(
        |(
            idempotency_token,
            scope_json,
            output_position_json,
            row_count,
            byte_count,
            committed_at_ms,
            row_key_start,
            row_key_end,
        )| {
            let scope = scope_json
                .map(|json| serde_json::from_str(&json).map_err(json_error))
                .transpose()?;
            let output_position = output_position_json
                .map(|json| serde_json::from_str(&json).map_err(json_error))
                .transpose()?;
            let row_range = match (row_key_start, row_key_end) {
                (Some(row_key_start), Some(row_key_end)) => Some(SegmentRowRange {
                    segment_id: mutation.segment_id.clone(),
                    row_key_start,
                    row_key_end,
                }),
                (None, None) => None,
                _ => {
                    return Err(CdfError::data(
                        "DuckDB segment mirror contains a partial row range",
                    ));
                }
            };
            Ok(SegmentMirrorRow {
                mutation: SegmentMirrorMutation {
                    target: mutation.target.clone(),
                    package_hash: mutation.package_hash.clone(),
                    idempotency_token: IdempotencyToken::new(idempotency_token)?,
                    segment_id: mutation.segment_id.clone(),
                    scope,
                    output_position,
                    row_count,
                    byte_count,
                    committed_at_ms,
                    row_range,
                },
            })
        },
    )
    .transpose()
}

fn segment_row_ranges(
    segment_acks: &[SegmentAck],
    first_row_key: Option<u64>,
    segment_identities: Option<&[cdf_runtime::StagedSegmentIdentity]>,
) -> Result<Vec<SegmentRowRange>> {
    let Some(first_row_key) = first_row_key else {
        return Ok(Vec::new());
    };
    let identities = segment_identities.ok_or_else(|| {
        CdfError::internal("DuckDB row-key mirror requires canonical segment identities")
    })?;
    segment_acks
        .iter()
        .map(|ack| {
            let identity = identities
                .iter()
                .find(|identity| identity.segment_id == ack.segment_id)
                .ok_or_else(|| {
                    CdfError::internal(format!(
                        "DuckDB segment {} is missing canonical ordinal identity",
                        ack.segment_id
                    ))
                })?;
            if identity.row_count != ack.row_count {
                return Err(CdfError::internal(format!(
                    "DuckDB segment {} acknowledgement and ordinal identity row counts differ",
                    ack.segment_id
                )));
            }
            let row_key_start = first_row_key
                .checked_add(identity.package_row_ord_start)
                .ok_or_else(|| CdfError::data("DuckDB segment row-key range overflowed"))?;
            let row_key_end = row_key_start
                .checked_add(ack.row_count)
                .ok_or_else(|| CdfError::data("DuckDB segment row-key range overflowed"))?;
            Ok(SegmentRowRange {
                segment_id: ack.segment_id.clone(),
                row_key_start,
                row_key_end,
            })
        })
        .collect()
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
