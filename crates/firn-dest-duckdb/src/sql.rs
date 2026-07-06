use crate::api::*;
use crate::*;

pub(crate) fn parse_target(target: &TargetName) -> Result<TargetRef> {
    let parts = target.as_str().split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [table] => {
            validate_ident(table)?;
            Ok(TargetRef {
                schema: MAIN_SCHEMA.to_owned(),
                table: (*table).to_owned(),
            })
        }
        [schema, table] => {
            validate_ident(schema)?;
            validate_ident(table)?;
            Ok(TargetRef {
                schema: (*schema).to_owned(),
                table: (*table).to_owned(),
            })
        }
        _ => Err(FirnError::contract(format!(
            "DuckDB target {} must be a namecase-v1 table or schema.table identifier",
            target.as_str()
        ))),
    }
}

pub(crate) fn validate_ident(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return Err(FirnError::contract("DuckDB identifier cannot be empty"));
    };
    if !(first == '_' || first.is_ascii_lowercase()) {
        return Err(FirnError::contract(format!(
            "DuckDB identifier {identifier:?} must start with a lowercase letter or underscore"
        )));
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit()) {
        return Err(FirnError::contract(format!(
            "DuckDB identifier {identifier:?} must contain only lowercase letters, digits, and underscores"
        )));
    }
    Ok(())
}

pub(crate) fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(crate) fn disposition_name(disposition: &WriteDisposition) -> &'static str {
    match disposition {
        WriteDisposition::Append => "append",
        WriteDisposition::Replace => "replace",
        WriteDisposition::Merge => "merge",
        WriteDisposition::CdcApply => "cdc_apply",
    }
}

pub(crate) fn duckdb_version(conn: &Connection) -> Result<String> {
    conn.query_row("PRAGMA version", [], |row| row.get(0))
        .map_err(|error| duckdb_error("query DuckDB version", error))
}

pub(crate) fn now_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            FirnError::internal(format!("system clock is before UNIX_EPOCH: {error}"))
        })?;
    i64::try_from(duration.as_millis())
        .map_err(|_| FirnError::internal("system time milliseconds exceed i64"))
}

pub(crate) fn duckdb_error(context: impl Into<String>, error: duckdb::Error) -> FirnError {
    FirnError::destination(format!("{}: {}", context.into(), error))
}

pub(crate) fn io_error(context: impl Into<String>, error: std::io::Error) -> FirnError {
    FirnError::destination(format!("{}: {}", context.into(), error))
}

pub(crate) fn json_error(error: serde_json::Error) -> FirnError {
    FirnError::data(error.to_string())
}
