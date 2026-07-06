use crate::*;
use crate::{api::*, sql::*, table::*};

pub(crate) fn apply_table_plan(
    conn: &Connection,
    plan: &TablePlan,
    disposition: WriteDisposition,
) -> Result<()> {
    for ddl in &plan.ddl {
        conn.execute_batch(ddl)
            .map_err(|error| duckdb_error(format!("apply DDL {ddl}"), error))?;
    }
    if disposition == WriteDisposition::Replace && plan.ddl.is_empty() {
        return Err(FirnError::internal(
            "replace disposition must plan a table rebuild",
        ));
    }
    Ok(())
}

pub(crate) fn append_rows(
    conn: &Connection,
    target: &TargetRef,
    fields: &[FieldPlan],
    rows: &[RowValues],
) -> Result<CommitCounts> {
    append_rows_to_table(conn, target, fields, rows)?;
    Ok(CommitCounts {
        rows_written: rows.len() as u64,
        rows_inserted: Some(rows.len() as u64),
        rows_updated: Some(0),
        rows_deleted: Some(0),
    })
}

pub(crate) fn append_rows_to_table(
    conn: &Connection,
    target: &TargetRef,
    fields: &[FieldPlan],
    rows: &[RowValues],
) -> Result<()> {
    let column_names = fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    let mut appender = if target.schema == MAIN_SCHEMA {
        conn.appender_with_columns(&target.table, &column_names)
    } else {
        conn.appender_with_columns_to_db(&target.table, &target.schema, &column_names)
    }
    .map_err(|error| duckdb_error(format!("open appender for {}", target.sql_name()), error))?;

    for row in rows {
        let values = row
            .iter()
            .map(|cell| cell.value.clone())
            .collect::<Vec<_>>();
        appender
            .append_row(appender_params_from_iter(values))
            .map_err(|error| {
                duckdb_error(format!("append row into {}", target.sql_name()), error)
            })?;
    }
    appender
        .flush()
        .map_err(|error| duckdb_error(format!("flush appender for {}", target.sql_name()), error))
}

pub(crate) fn merge_rows(
    conn: &Connection,
    target: &TargetRef,
    fields: &[FieldPlan],
    merge_keys: &[String],
    rows: &[RowValues],
) -> Result<CommitCounts> {
    let staging = TargetRef {
        schema: MAIN_SCHEMA.to_owned(),
        table: staging_table_name(),
    };
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE {} ({})",
        quote_ident(&staging.table),
        create_columns_sql(fields)
    ))
    .map_err(|error| duckdb_error("create DuckDB merge staging table", error))?;
    append_rows_to_table(conn, &staging, fields, rows)?;

    let predicate = merge_predicate(merge_keys)?;
    let updated: u64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM {} AS target WHERE EXISTS (SELECT 1 FROM {} AS stage WHERE {})",
                target.sql_name(),
                quote_ident(&staging.table),
                predicate
            ),
            [],
            |row| row.get(0),
        )
        .map_err(|error| duckdb_error("count DuckDB merge updates", error))?;
    conn.execute_batch(&format!(
        "DELETE FROM {} AS target USING {} AS stage WHERE {}",
        target.sql_name(),
        quote_ident(&staging.table),
        predicate
    ))
    .map_err(|error| duckdb_error("delete DuckDB merge matches", error))?;

    let column_list = fields
        .iter()
        .map(|field| quote_ident(&field.name))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!(
        "INSERT INTO {} ({}) SELECT {} FROM {}",
        target.sql_name(),
        column_list,
        column_list,
        quote_ident(&staging.table)
    ))
    .map_err(|error| duckdb_error("insert DuckDB merge rows", error))?;

    let written = rows.len() as u64;
    Ok(CommitCounts {
        rows_written: written,
        rows_inserted: Some(written.saturating_sub(updated)),
        rows_updated: Some(updated),
        rows_deleted: Some(0),
    })
}

pub(crate) fn staging_table_name() -> String {
    let counter = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("_firn_stage_{}_{}", std::process::id(), counter)
}

pub(crate) fn merge_predicate(merge_keys: &[String]) -> Result<String> {
    if merge_keys.is_empty() {
        return Err(FirnError::contract(
            "DuckDB merge requires at least one merge key",
        ));
    }
    for key in merge_keys {
        validate_ident(key)?;
    }
    Ok(merge_keys
        .iter()
        .map(|key| {
            format!(
                "target.{} IS NOT DISTINCT FROM stage.{}",
                quote_ident(key),
                quote_ident(key)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND "))
}

pub(crate) fn merge_key_indexes(fields: &[FieldPlan], merge_keys: &[String]) -> Result<Vec<usize>> {
    if merge_keys.is_empty() {
        return Err(FirnError::contract(
            "DuckDB merge requires at least one merge key",
        ));
    }
    merge_keys
        .iter()
        .map(|key| {
            fields
                .iter()
                .position(|field| &field.name == key)
                .ok_or_else(|| {
                    FirnError::contract(format!("merge key {key} is not in package schema"))
                })
        })
        .collect()
}

pub(crate) fn dedup_merge_rows(
    rows: &[RowValues],
    key_indexes: &[usize],
) -> Result<Vec<RowValues>> {
    let mut key_to_index = BTreeMap::<Vec<CellKey>, usize>::new();
    let mut deduped = Vec::<RowValues>::new();

    for row in rows {
        let key = key_indexes
            .iter()
            .map(|index| row[*index].key.clone())
            .collect::<Vec<_>>();
        if key.iter().any(|cell| matches!(cell, CellKey::Null)) {
            return Err(FirnError::data("DuckDB merge key values cannot be NULL"));
        }

        match key_to_index.get(&key) {
            Some(existing_index) if same_row(&deduped[*existing_index], row) => {}
            Some(_) => {
                return Err(FirnError::data(
                    "DuckDB merge package contains conflicting duplicate merge keys; no winner policy is ratified",
                ));
            }
            None => {
                key_to_index.insert(key, deduped.len());
                deduped.push(row.clone());
            }
        }
    }

    Ok(deduped)
}

pub(crate) fn same_row(left: &RowValues, right: &RowValues) -> bool {
    left.iter()
        .map(|cell| &cell.key)
        .eq(right.iter().map(|cell| &cell.key))
}
