use std::collections::{BTreeMap, BTreeSet};

use cdf_dest_sql::{LoadMirrorKey, TransactionalMirrorManager};
use cdf_kernel::{
    CdfError, CommitCounts, CommitSegment, Receipt, Result, SegmentId, WriteDisposition,
};
use rusqlite::{Connection, OptionalExtension, params_from_iter, types::Value};

use crate::{
    error::{
        classify_sqlite_error, classify_sqlite_execution_error, classify_sqlite_payload_error,
    },
    mapping::{sqlite_value, validate_schema_matches_columns},
    mirrors::SqliteMirrorBackend,
    models::{SqliteExpectedSegment, SqliteLoadPlan},
    receipts::{build_receipt, duplicate_receipt_input, expected_counts},
};

pub(super) const ROW_KEY_COLUMN: &str = "_cdf_row_key";
const STAGE_TABLE: &str = "_cdf_sqlite_stage";
const SQLITE_PROGRESS_VM_OPERATIONS: i32 = 8 * 1024;
#[cfg(test)]
pub(crate) const TEST_EXIT_BEFORE_COMMIT_ENV: &str = "CDF_SQLITE_TEST_EXIT_BEFORE_COMMIT";
#[cfg(test)]
pub(crate) const TEST_EXIT_DURING_PAYLOAD_CODE: i32 = 87;
#[cfg(test)]
pub(crate) const TEST_EXIT_DURING_MIRRORS_CODE: i32 = 88;

pub(crate) fn install_progress_handler(
    connection: &Connection,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<()> {
    let cancellation = cancellation.clone();
    connection
        .progress_handler(
            SQLITE_PROGRESS_VM_OPERATIONS,
            Some(move || cancellation.is_cancelled()),
        )
        .map_err(|error| {
            classify_sqlite_error("install SQLite destination cancellation hook", error)
        })
}

pub(super) fn find_duplicate_receipt(
    connection: &Connection,
    plan: &SqliteLoadPlan,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<Option<Receipt>> {
    let mut backend = SqliteMirrorBackend::new(connection, cancellation.clone());
    TransactionalMirrorManager::new(&mut backend).find_duplicate(
        &LoadMirrorKey {
            target: plan.kernel.target.clone(),
            package_hash: plan.package_hash.clone(),
            idempotency_token: plan.idempotency_token.clone(),
        },
        |stored| {
            expected_counts(plan, stored)?;
            build_receipt(plan, duplicate_receipt_input(stored)?)
        },
    )
}

pub(super) fn package_row_count(
    expected: &BTreeMap<SegmentId, SqliteExpectedSegment>,
) -> Result<u64> {
    expected.values().try_fold(0_u64, |total, segment| {
        total
            .checked_add(segment.state.row_count)
            .ok_or_else(|| CdfError::data("SQLite package row count overflowed"))
    })
}

pub(super) fn allocate_row_keys(connection: &Connection, row_count: u64) -> Result<u64> {
    let count = i64::try_from(row_count)
        .map_err(|_| CdfError::data("SQLite package row count exceeds INTEGER range"))?;
    let first: i64 = connection
        .query_row(
            "UPDATE _cdf_row_key_allocator
             SET next_row_key = next_row_key + ?1
             WHERE singleton = 1
             RETURNING next_row_key - ?1",
            [count],
            |row| row.get(0),
        )
        .map_err(|error| classify_sqlite_error("allocate SQLite row-key range", error))?;
    u64::try_from(first)
        .map_err(|_| CdfError::destination("SQLite row-key allocator contains a negative value"))
}

pub(super) fn prepare_target(connection: &Connection, plan: &SqliteLoadPlan) -> Result<()> {
    let existing = table_columns(connection, plan.target.as_str())?;
    if existing.is_empty() {
        let mut definitions = plan
            .columns
            .iter()
            .map(|column| {
                format!(
                    "{} {}{}",
                    column.name.quoted(),
                    column.sqlite_type,
                    if column.nullable { "" } else { " NOT NULL" }
                )
            })
            .collect::<Vec<_>>();
        definitions.push(format!("\"{ROW_KEY_COLUMN}\" INTEGER"));
        connection
            .execute_batch(&format!(
                "CREATE TABLE {} ({}) STRICT;",
                plan.target.quoted(),
                definitions.join(", "),
            ))
            .map_err(|error| classify_sqlite_error("create SQLite target", error))?;
    } else {
        for column in &plan.columns {
            match existing.get(column.name.as_str()) {
                Some((stored_type, not_null)) => {
                    if !stored_type.eq_ignore_ascii_case(&column.sqlite_type)
                        || column.nullable == *not_null
                    {
                        return Err(CdfError::destination(format!(
                            "SQLite target column {} has incompatible type/nullability",
                            column.name
                        )));
                    }
                }
                None if column.nullable => {
                    connection
                        .execute_batch(&format!(
                            "ALTER TABLE {} ADD COLUMN {} {}",
                            plan.target.quoted(),
                            column.name.quoted(),
                            column.sqlite_type
                        ))
                        .map_err(|error| {
                            classify_sqlite_error("add SQLite target column", error)
                        })?;
                }
                None => {
                    return Err(CdfError::contract(format!(
                        "SQLite cannot add required column {} to an existing target without inventing a default",
                        column.name
                    )));
                }
            }
        }
        if !existing.contains_key(ROW_KEY_COLUMN) {
            connection
                .execute_batch(&format!(
                    "ALTER TABLE {} ADD COLUMN \"{ROW_KEY_COLUMN}\" INTEGER;",
                    plan.target.quoted(),
                ))
                .map_err(|error| classify_sqlite_error("add SQLite row provenance", error))?;
        }
    }
    connection
        .execute_batch(&format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}(\"{ROW_KEY_COLUMN}\")
             WHERE \"{ROW_KEY_COLUMN}\" IS NOT NULL;",
            crate::identifier::quote_identifier(&row_key_index_name(plan.target.as_str())),
            plan.target.quoted(),
        ))
        .map_err(|error| classify_sqlite_error("guarantee SQLite row provenance", error))?;
    Ok(())
}

pub(super) fn row_key_index_name(target: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(target.as_bytes());
    format!("_cdf_row_key_{digest:x}")
}

pub(super) fn target_exists(connection: &Connection, target: &str) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
            [target],
            |row| row.get::<_, bool>(0),
        )
        .map_err(|error| classify_sqlite_error("inspect SQLite target existence", error))
}

pub(super) fn prepare_stage(connection: &Connection, plan: &SqliteLoadPlan) -> Result<()> {
    let mut definitions = plan
        .columns
        .iter()
        .map(|column| format!("{} {}", column.name.quoted(), column.sqlite_type))
        .collect::<Vec<_>>();
    definitions.push(format!("\"{ROW_KEY_COLUMN}\" INTEGER NOT NULL"));
    connection
        .execute_batch(&format!(
            "DROP TABLE IF EXISTS temp.\"{STAGE_TABLE}\";
             CREATE TEMP TABLE \"{STAGE_TABLE}\" ({}) STRICT;",
            definitions.join(", ")
        ))
        .map_err(|error| classify_sqlite_error("create SQLite merge stage", error))
}

pub(super) fn table_columns(
    connection: &Connection,
    table: &str,
) -> Result<BTreeMap<String, (String, bool)>> {
    let mut statement = connection
        .prepare(&format!(
            "PRAGMA table_info({})",
            crate::identifier::quote_identifier(table)
        ))
        .map_err(|error| classify_sqlite_error("inspect SQLite target schema", error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                (row.get::<_, String>(2)?, row.get::<_, i64>(3)? != 0),
            ))
        })
        .map_err(|error| classify_sqlite_error("query SQLite target schema", error))?;
    rows.map(|row| row.map_err(|error| classify_sqlite_error("decode SQLite target schema", error)))
        .collect()
}

pub(super) fn validate_package_segment<'a>(
    segment: &CommitSegment,
    expected: &'a BTreeMap<SegmentId, SqliteExpectedSegment>,
    plan: &SqliteLoadPlan,
    accepted: &mut BTreeSet<SegmentId>,
) -> Result<&'a SqliteExpectedSegment> {
    let id = &segment.state.segment_id;
    if accepted.contains(id) {
        return Err(CdfError::data(format!(
            "SQLite commit received duplicate segment {id}"
        )));
    }
    let expected = expected
        .get(id)
        .ok_or_else(|| CdfError::data(format!("SQLite commit received unplanned segment {id}")))?;
    if segment.state != expected.state || segment.package_byte_count != expected.package_byte_count
    {
        return Err(CdfError::data(format!(
            "SQLite commit segment {id} differs from finalized package authority"
        )));
    }
    let mut rows = 0_u64;
    for batch in &segment.batches {
        let logical = cdf_package_contract::logical_output_schema(batch.schema().as_ref())?;
        validate_schema_matches_columns(&logical, &plan.columns)?;
        rows = rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| CdfError::data("SQLite segment row count overflowed"))?;
    }
    if rows != expected.state.row_count {
        return Err(CdfError::data(format!(
            "SQLite segment {id} payload row count differs from its manifest"
        )));
    }
    cdf_package_contract::validate_package_row_ord_batches(
        &segment.batches,
        expected.package_row_ord_start,
        expected.state.row_count,
    )?;
    accepted.insert(id.clone());
    Ok(expected)
}

pub(super) fn write_segment(
    connection: &Connection,
    plan: &SqliteLoadPlan,
    segment: &CommitSegment,
    first_row_key: u64,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<()> {
    cancellation.check()?;
    let table = if plan.kernel.disposition == WriteDisposition::Merge {
        format!("\"{STAGE_TABLE}\"")
    } else {
        plan.target.quoted()
    };
    let mut column_names = plan
        .columns
        .iter()
        .map(|column| column.name.quoted())
        .collect::<Vec<_>>();
    column_names.push(format!("\"{ROW_KEY_COLUMN}\""));
    let placeholders = (1..=column_names.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut statement = connection
        .prepare(&format!(
            "INSERT INTO {table} ({}) VALUES ({placeholders})",
            column_names.join(", ")
        ))
        .map_err(|error| classify_sqlite_error("prepare SQLite payload insert", error))?;
    for batch in &segment.batches {
        cancellation.check()?;
        for row in 0..batch.num_rows() {
            cancellation.check()?;
            let mut values = Vec::with_capacity(plan.columns.len() + 1);
            for (index, field) in batch.schema().fields()[..plan.columns.len()]
                .iter()
                .enumerate()
            {
                values.push(sqlite_value(
                    batch.column(index).as_ref(),
                    field.data_type(),
                    row,
                )?);
            }
            let package_ordinal = expected_package_ordinal(batch, row)?;
            let row_key = first_row_key
                .checked_add(package_ordinal)
                .ok_or_else(|| CdfError::data("SQLite row key overflowed"))?;
            values.push(Value::Integer(i64::try_from(row_key).map_err(|_| {
                CdfError::data("SQLite row key exceeds INTEGER range")
            })?));
            statement
                .execute(params_from_iter(values))
                .map_err(|error| {
                    if cancellation.is_cancelled() {
                        classify_sqlite_execution_error(
                            "insert SQLite payload row",
                            error,
                            cancellation,
                        )
                    } else {
                        classify_sqlite_payload_error("insert SQLite payload row", error)
                    }
                })?;
            #[cfg(test)]
            exit_before_commit_for_test("payload", TEST_EXIT_DURING_PAYLOAD_CODE);
        }
    }
    Ok(())
}

pub(super) fn expected_package_ordinal(
    batch: &arrow_array::RecordBatch,
    row: usize,
) -> Result<u64> {
    use arrow_array::UInt64Array;
    let index = batch
        .num_columns()
        .checked_sub(1)
        .ok_or_else(|| CdfError::data("SQLite canonical package batch has no ordinal column"))?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .map(|array| array.value(row))
        .ok_or_else(|| CdfError::data("SQLite package ordinal column has wrong Arrow type"))
}

pub(super) fn require_complete_segments(
    accepted: &BTreeSet<SegmentId>,
    expected: &BTreeMap<SegmentId, SqliteExpectedSegment>,
) -> Result<()> {
    if accepted.len() == expected.len() {
        return Ok(());
    }
    let missing = expected
        .keys()
        .find(|id| !accepted.contains(*id))
        .ok_or_else(|| CdfError::internal("SQLite package segment cardinality is inconsistent"))?;
    Err(CdfError::data(format!(
        "SQLite finalized package stream omitted segment {missing}"
    )))
}

pub(super) fn finish_payload(
    connection: &Connection,
    plan: &SqliteLoadPlan,
    rows: u64,
    deleted_rows: u64,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<CommitCounts> {
    cancellation.check()?;
    match plan.kernel.disposition {
        WriteDisposition::Append | WriteDisposition::Replace => Ok(CommitCounts::rows(
            rows,
            Some(rows),
            Some(0),
            Some(deleted_rows),
        )),
        WriteDisposition::Merge => finish_merge(connection, plan, rows, cancellation),
        WriteDisposition::CdcApply => Err(CdfError::internal(
            "unsupported SQLite CDC disposition reached payload finalization",
        )),
    }
}

pub(super) fn finish_merge(
    connection: &Connection,
    plan: &SqliteLoadPlan,
    rows: u64,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<CommitCounts> {
    cancellation.check()?;
    let keys = merge_predicate(plan, "target", "stage");
    let group = plan
        .merge_keys
        .iter()
        .map(|key| key.quoted())
        .collect::<Vec<_>>()
        .join(", ");
    let duplicate: Option<i64> = connection
        .query_row(
            &format!(
                "SELECT 1 FROM \"{STAGE_TABLE}\" GROUP BY {group} HAVING COUNT(*) > 1 LIMIT 1"
            ),
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| {
            classify_sqlite_execution_error(
                "check SQLite merge package duplicates",
                error,
                cancellation,
            )
        })?;
    if duplicate.is_some() {
        return Err(CdfError::data(
            "SQLite merge package contains duplicate merge keys and dedup policy is fail",
        ));
    }
    let ambiguous: Option<i64> = connection
        .query_row(
            &format!(
                "SELECT 1 FROM \"{STAGE_TABLE}\" AS stage
                 JOIN {} AS target ON {keys}
                 GROUP BY stage.\"{ROW_KEY_COLUMN}\" HAVING COUNT(*) > 1 LIMIT 1",
                plan.target.quoted()
            ),
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| {
            classify_sqlite_execution_error(
                "check SQLite merge target ambiguity",
                error,
                cancellation,
            )
        })?;
    if ambiguous.is_some() {
        return Err(CdfError::destination(
            "SQLite merge keys match more than one existing target row",
        ));
    }
    let updated = query_count(
        connection,
        &format!(
            "SELECT COUNT(*) FROM \"{STAGE_TABLE}\" AS stage
             WHERE EXISTS (SELECT 1 FROM {} AS target WHERE {keys})",
            plan.target.quoted()
        ),
        "count SQLite merge updates",
    )?;
    let assignments = plan
        .columns
        .iter()
        .map(|column| {
            format!(
                "{} = (SELECT stage.{} FROM \"{STAGE_TABLE}\" AS stage WHERE {})",
                column.name.quoted(),
                column.name.quoted(),
                merge_predicate(plan, "target", "stage")
            )
        })
        .chain(std::iter::once(format!(
            "\"{ROW_KEY_COLUMN}\" = (SELECT stage.\"{ROW_KEY_COLUMN}\" FROM \"{STAGE_TABLE}\" AS stage WHERE {})",
            merge_predicate(plan, "target", "stage")
        )))
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute(
            &format!(
                "UPDATE {} AS target SET {assignments}
                 WHERE EXISTS (SELECT 1 FROM \"{STAGE_TABLE}\" AS stage WHERE {})",
                plan.target.quoted(),
                merge_predicate(plan, "target", "stage")
            ),
            [],
        )
        .map_err(|error| {
            classify_sqlite_execution_error("update SQLite merge matches", error, cancellation)
        })?;
    let all_columns = plan
        .columns
        .iter()
        .map(|column| column.name.quoted())
        .chain(std::iter::once(format!("\"{ROW_KEY_COLUMN}\"")))
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute(
            &format!(
                "INSERT INTO {} ({all_columns})
                 SELECT {all_columns} FROM \"{STAGE_TABLE}\" AS stage
                 WHERE NOT EXISTS (SELECT 1 FROM {} AS target WHERE {})",
                plan.target.quoted(),
                plan.target.quoted(),
                merge_predicate(plan, "target", "stage")
            ),
            [],
        )
        .map_err(|error| {
            classify_sqlite_execution_error("insert SQLite merge misses", error, cancellation)
        })?;
    let inserted = rows
        .checked_sub(updated)
        .ok_or_else(|| CdfError::internal("SQLite merge update count exceeds source rows"))?;
    Ok(CommitCounts::keyed_changes(
        cdf_kernel::KeyedEffectCounts {
            upserts: rows,
            deletes: 0,
        },
        Some(inserted),
        Some(updated),
        None,
        None,
        None,
        None,
    ))
}

pub(super) fn merge_predicate(
    plan: &SqliteLoadPlan,
    target_alias: &str,
    stage_alias: &str,
) -> String {
    plan.merge_keys
        .iter()
        .map(|key| {
            format!(
                "{target_alias}.{} IS {stage_alias}.{}",
                key.quoted(),
                key.quoted()
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

pub(super) fn count_target_rows(connection: &Connection, plan: &SqliteLoadPlan) -> Result<u64> {
    query_count(
        connection,
        &format!("SELECT COUNT(*) FROM {}", plan.target.quoted()),
        "count SQLite target rows",
    )
}

pub(super) fn query_count(connection: &Connection, sql: &str, action: &str) -> Result<u64> {
    let count: i64 = connection
        .query_row(sql, [], |row| row.get(0))
        .map_err(|error| classify_sqlite_error(action, error))?;
    u64::try_from(count).map_err(|_| CdfError::destination(format!("{action}: negative count")))
}

#[cfg(test)]
pub(super) fn exit_before_commit_for_test(phase: &str, code: i32) {
    if std::env::var_os(TEST_EXIT_BEFORE_COMMIT_ENV).as_deref() == Some(std::ffi::OsStr::new(phase))
    {
        std::process::exit(code);
    }
}
