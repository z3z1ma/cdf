use crate::*;
use crate::{api::*, sql::*};

pub(crate) fn plan_table(
    conn: &Connection,
    target: TargetRef,
    fields: &[FieldPlan],
    disposition: WriteDisposition,
) -> Result<TablePlan> {
    let existing = existing_columns(conn, &target)?;
    let mut ddl = Vec::new();
    if target.schema != MAIN_SCHEMA {
        ddl.push(format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_ident(&target.schema)
        ));
    }

    match disposition {
        WriteDisposition::Replace => {
            ddl.push(format!(
                "DROP TABLE IF EXISTS {}; CREATE TABLE {} ({})",
                target.sql_name(),
                target.sql_name(),
                create_target_columns_sql(fields)
            ));
        }
        WriteDisposition::Append | WriteDisposition::Merge => {
            if existing.is_empty() {
                ddl.push(format!(
                    "CREATE TABLE {} ({})",
                    target.sql_name(),
                    create_target_columns_sql(fields)
                ));
            } else {
                require_targetable_provenance(conn, &target, &existing)?;
                for field in fields {
                    match existing.get(&field.name) {
                        Some(column) if same_type(&column.data_type, &field.sql_type) => {}
                        Some(column) => {
                            return Err(CdfError::contract(format!(
                                "DuckDB column {}.{} has type {}, package requires {}",
                                target.table, field.name, column.data_type, field.sql_type
                            )));
                        }
                        None => ddl.push(format!(
                            "ALTER TABLE {} ADD COLUMN {} {}",
                            target.sql_name(),
                            quote_ident(&field.name),
                            field.sql_type
                        )),
                    }
                }
            }
        }
        WriteDisposition::CdcApply => {
            return Err(CdfError::contract(
                "DuckDB destination does not support cdc_apply in the MVP sheet",
            ));
        }
    }

    Ok(TablePlan { target, ddl })
}

pub(crate) fn plan_absent_table(
    target: TargetRef,
    fields: &[FieldPlan],
    disposition: WriteDisposition,
) -> Result<TablePlan> {
    if disposition == WriteDisposition::CdcApply {
        return Err(CdfError::contract(
            "DuckDB destination does not support cdc_apply in the MVP sheet",
        ));
    }

    let mut ddl = Vec::new();
    if target.schema != MAIN_SCHEMA {
        ddl.push(format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_ident(&target.schema)
        ));
    }
    if disposition == WriteDisposition::Replace {
        ddl.push(format!(
            "DROP TABLE IF EXISTS {}; CREATE TABLE {} ({})",
            target.sql_name(),
            target.sql_name(),
            create_target_columns_sql(fields)
        ));
    } else {
        ddl.push(format!(
            "CREATE TABLE {} ({})",
            target.sql_name(),
            create_target_columns_sql(fields)
        ));
    }

    Ok(TablePlan { target, ddl })
}

pub(crate) fn existing_columns(
    conn: &Connection,
    target: &TargetRef,
) -> Result<BTreeMap<String, ExistingColumn>> {
    let mut stmt = conn
        .prepare(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        )
        .map_err(|error| duckdb_error("prepare information_schema column query", error))?;
    let rows = stmt
        .query_map(
            params![target.schema.as_str(), target.table.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    ExistingColumn {
                        data_type: row.get::<_, String>(1)?,
                        nullable: row.get::<_, String>(2)? == "YES",
                    },
                ))
            },
        )
        .map_err(|error| duckdb_error("query information_schema columns", error))?;

    let mut columns = BTreeMap::new();
    for row in rows {
        let (name, column) =
            row.map_err(|error| duckdb_error("read information_schema row", error))?;
        columns.insert(name, column);
    }
    Ok(columns)
}

pub(crate) fn same_type(existing: &str, required: &str) -> bool {
    normalize_type(existing) == normalize_type(required)
}

pub(crate) fn normalize_type(value: &str) -> String {
    match value.to_ascii_uppercase().as_str() {
        "INT" => "INTEGER".to_owned(),
        "UINT" => "UINTEGER".to_owned(),
        "DOUBLE PRECISION" => "DOUBLE".to_owned(),
        other => other.to_owned(),
    }
}

pub(crate) fn create_columns_sql(fields: &[FieldPlan]) -> String {
    fields
        .iter()
        .map(|field| {
            let nullable = if field.nullable { "" } else { " NOT NULL" };
            format!(
                "{} {}{}",
                quote_ident(&field.name),
                field.sql_type,
                nullable
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn create_target_columns_sql(fields: &[FieldPlan]) -> String {
    create_columns_sql(fields)
}

pub(crate) fn require_targetable_provenance(
    conn: &Connection,
    target: &TargetRef,
    existing: &BTreeMap<String, ExistingColumn>,
) -> Result<()> {
    if !existing.contains_key(CDF_ROW_KEY_COLUMN) {
        return Err(CdfError::contract(format!(
            "DuckDB target {} does not have the current compact {} provenance column; use replace to rebuild it from verified packages",
            target.sql_name(),
            CDF_ROW_KEY_COLUMN,
        )));
    }
    let column = &existing[CDF_ROW_KEY_COLUMN];
    if !same_type(&column.data_type, "UBIGINT") {
        return Err(CdfError::contract(format!(
            "DuckDB target {} provenance column {} has type {}; expected UBIGINT",
            target.sql_name(),
            CDF_ROW_KEY_COLUMN,
            column.data_type
        )));
    }
    if column.nullable {
        return Err(CdfError::contract(format!(
            "DuckDB target {} provenance column {} is nullable; row keys must be NOT NULL before addressed correction is safe",
            target.sql_name(),
            CDF_ROW_KEY_COLUMN,
        )));
    }
    let has_duplicate: bool = conn
        .query_row(
            &format!(
                "SELECT EXISTS (SELECT 1 FROM {} GROUP BY {} HAVING count(*) > 1)",
                target.sql_name(),
                quote_ident(CDF_ROW_KEY_COLUMN),
            ),
            [],
            |row| row.get(0),
        )
        .map_err(|error| duckdb_error("verify DuckDB provenance uniqueness", error))?;
    if has_duplicate {
        return Err(CdfError::contract(format!(
            "DuckDB target {} contains duplicate compact row-provenance addresses; use replace to rebuild it from verified packages",
            target.sql_name()
        )));
    }
    Ok(())
}
