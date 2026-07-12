use crate::*;
use crate::{api::*, arrow_bridge::into_duckdb_batch, sql::*};

pub(crate) fn append_arrow_batch_to_table(
    conn: &Connection,
    target: &TargetRef,
    batch: RecordBatch,
) -> Result<()> {
    let schema = batch.schema();
    let column_names = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    let mut appender = if target.schema == MAIN_SCHEMA {
        conn.appender_with_columns(&target.table, &column_names)
    } else {
        conn.appender_with_columns_to_db(&target.table, &target.schema, &column_names)
    }
    .map_err(|error| duckdb_error(format!("open appender for {}", target.sql_name()), error))?;
    appender
        .append_record_batch(into_duckdb_batch(batch)?)
        .map_err(|error| {
            duckdb_error(
                format!("append Arrow batch into {}", target.sql_name()),
                error,
            )
        })?;
    appender
        .flush()
        .map_err(|error| duckdb_error(format!("flush appender for {}", target.sql_name()), error))
}

pub(crate) struct IngressSegmentTransfer<'a> {
    pub ingress: &'a TargetRef,
    pub target: &'a TargetRef,
    pub persisted_fields: &'a [FieldPlan],
    pub user_field_count: usize,
    pub package_hash: &'a cdf_kernel::PackageHash,
    pub segment_id: &'a cdf_kernel::SegmentId,
    pub include_stage_order: bool,
}

pub(crate) fn transfer_ingress_segment(
    conn: &Connection,
    transfer: IngressSegmentTransfer<'_>,
) -> Result<()> {
    if transfer.user_field_count + 3 != transfer.persisted_fields.len() {
        return Err(CdfError::internal(
            "DuckDB persistence schema does not contain three provenance fields",
        ));
    }
    let user_columns = transfer.persisted_fields[..transfer.user_field_count]
        .iter()
        .map(|field| quote_ident(&field.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut target_columns = transfer
        .persisted_fields
        .iter()
        .map(|field| quote_ident(&field.name))
        .collect::<Vec<_>>();
    let mut selected = if user_columns.is_empty() {
        Vec::new()
    } else {
        vec![user_columns]
    };
    selected.extend(["?".to_owned(), "?".to_owned(), quote_ident(CDF_ROW_COLUMN)]);
    if transfer.include_stage_order {
        target_columns.push(quote_ident(CDF_STAGE_ORDER_COLUMN));
        selected.push(quote_ident(CDF_STAGE_ORDER_COLUMN));
    }
    conn.execute(
        &format!(
            "INSERT INTO {} ({}) SELECT {} FROM {}",
            transfer.target.sql_name(),
            target_columns.join(", "),
            selected.join(", "),
            transfer.ingress.sql_name(),
        ),
        params![transfer.package_hash.as_str(), transfer.segment_id.as_str()],
    )
    .map_err(|error| duckdb_error("transfer DuckDB Arrow ingress segment", error))?;
    conn.execute_batch(&format!("DELETE FROM {}", transfer.ingress.sql_name()))
        .map_err(|error| duckdb_error("clear DuckDB Arrow ingress segment", error))
}

pub(crate) fn transfer_package_ingress(
    conn: &Connection,
    ingress: &TargetRef,
    ranges: &TargetRef,
    target: &TargetRef,
    persisted_fields: &[FieldPlan],
    user_field_count: usize,
    package_hash: &cdf_kernel::PackageHash,
) -> Result<()> {
    if user_field_count + 3 != persisted_fields.len() {
        return Err(CdfError::internal(
            "DuckDB persistence schema does not contain three provenance fields",
        ));
    }
    let user_columns = persisted_fields[..user_field_count]
        .iter()
        .map(|field| format!("ingress.{}", quote_ident(&field.name)))
        .collect::<Vec<_>>();
    let target_columns = persisted_fields
        .iter()
        .map(|field| quote_ident(&field.name))
        .collect::<Vec<_>>();
    let mut selected = user_columns;
    selected.extend([
        "?".to_owned(),
        "ranges.segment_id".to_owned(),
        format!("ingress.{} - ranges.start_row", quote_ident(CDF_ROW_COLUMN)),
    ]);
    conn.execute(
        &format!(
            "INSERT INTO {} ({}) SELECT {} FROM {} AS ingress JOIN {} AS ranges ON ingress.{} >= ranges.start_row AND ingress.{} < ranges.end_row ORDER BY ingress.{}",
            target.sql_name(),
            target_columns.join(", "),
            selected.join(", "),
            ingress.sql_name(),
            ranges.sql_name(),
            quote_ident(CDF_ROW_COLUMN),
            quote_ident(CDF_ROW_COLUMN),
            quote_ident(CDF_ROW_COLUMN),
        ),
        params![package_hash.as_str()],
    )
    .map_err(|error| duckdb_error("transfer DuckDB package ingress", error))?;
    Ok(())
}

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
        return Err(CdfError::internal(
            "replace disposition must plan a table rebuild",
        ));
    }
    Ok(())
}

pub(crate) fn finalize_arrow_merge(
    conn: &Connection,
    target: &TargetRef,
    staging: &TargetRef,
    fields: &[FieldPlan],
    user_field_count: usize,
    merge_keys: &[String],
) -> Result<CommitCounts> {
    merge_key_indexes(fields, merge_keys)?;
    if user_field_count > fields.len() {
        return Err(CdfError::internal(
            "DuckDB merge user-field count exceeds persistence schema",
        ));
    }
    let null_keys = merge_keys
        .iter()
        .map(|key| format!("{} IS NULL", quote_ident(key)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let has_null: bool = conn
        .query_row(
            &format!(
                "SELECT EXISTS (SELECT 1 FROM {} WHERE {})",
                staging.sql_name(),
                null_keys
            ),
            [],
            |row| row.get(0),
        )
        .map_err(|error| duckdb_error("validate DuckDB Arrow merge null keys", error))?;
    if has_null {
        return Err(CdfError::data("DuckDB merge key values cannot be NULL"));
    }

    let same_key = merge_keys
        .iter()
        .map(|key| {
            format!(
                "left_stage.{} IS NOT DISTINCT FROM right_stage.{}",
                quote_ident(key),
                quote_ident(key)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let different_user_value = fields[..user_field_count]
        .iter()
        .map(|field| {
            format!(
                "left_stage.{} IS DISTINCT FROM right_stage.{}",
                quote_ident(&field.name),
                quote_ident(&field.name)
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let different_user_value = if different_user_value.is_empty() {
        "FALSE".to_owned()
    } else {
        different_user_value
    };
    let conflicting: bool = conn
        .query_row(
            &format!(
                "SELECT EXISTS (SELECT 1 FROM {stage} AS left_stage JOIN {stage} AS right_stage ON {same_key} AND left_stage.{order} < right_stage.{order} WHERE {different_user_value})",
                stage = staging.sql_name(),
                order = quote_ident(CDF_STAGE_ORDER_COLUMN),
            ),
            [],
            |row| row.get(0),
        )
        .map_err(|error| duckdb_error("validate DuckDB Arrow merge duplicate keys", error))?;
    if conflicting {
        return Err(CdfError::data(
            "DuckDB merge package contains conflicting duplicate merge keys; no winner policy is ratified",
        ));
    }

    let dedup = TargetRef {
        schema: MAIN_SCHEMA.to_owned(),
        table: staging_table_name(),
    };
    let column_list = fields
        .iter()
        .map(|field| quote_ident(&field.name))
        .collect::<Vec<_>>()
        .join(", ");
    let key_list = merge_keys
        .iter()
        .map(|key| quote_ident(key))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE {dedup} AS SELECT {columns} FROM (SELECT {columns}, row_number() OVER (PARTITION BY {keys} ORDER BY {order}) AS _cdf_rank FROM {stage}) WHERE _cdf_rank = 1",
        dedup = dedup.sql_name(),
        columns = column_list,
        keys = key_list,
        order = quote_ident(CDF_STAGE_ORDER_COLUMN),
        stage = staging.sql_name(),
    ))
    .map_err(|error| duckdb_error("deduplicate DuckDB Arrow merge staging", error))?;

    let written: u64 = conn
        .query_row(
            &format!("SELECT count(*) FROM {}", dedup.sql_name()),
            [],
            |row| row.get(0),
        )
        .map_err(|error| duckdb_error("count DuckDB Arrow merge rows", error))?;
    let predicate = merge_predicate(merge_keys)?;
    let updated: u64 = conn
        .query_row(
            &format!(
                "SELECT count(*) FROM {} AS target WHERE EXISTS (SELECT 1 FROM {} AS stage WHERE {})",
                target.sql_name(),
                dedup.sql_name(),
                predicate
            ),
            [],
            |row| row.get(0),
        )
        .map_err(|error| duckdb_error("count DuckDB Arrow merge updates", error))?;
    conn.execute_batch(&format!(
        "DELETE FROM {} AS target USING {} AS stage WHERE {}; INSERT INTO {} ({}) SELECT {} FROM {}",
        target.sql_name(),
        dedup.sql_name(),
        predicate,
        target.sql_name(),
        column_list,
        column_list,
        dedup.sql_name(),
    ))
    .map_err(|error| duckdb_error("apply DuckDB Arrow merge", error))?;
    Ok(CommitCounts {
        rows_written: written,
        rows_inserted: Some(written.saturating_sub(updated)),
        rows_updated: Some(updated),
        rows_deleted: Some(0),
    })
}

pub(crate) fn staging_table_name() -> String {
    let counter = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("_cdf_stage_{}_{}", std::process::id(), counter)
}

pub(crate) fn merge_predicate(merge_keys: &[String]) -> Result<String> {
    if merge_keys.is_empty() {
        return Err(CdfError::contract(
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
        return Err(CdfError::contract(
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
                    CdfError::contract(format!("merge key {key} is not in package schema"))
                })
        })
        .collect()
}
