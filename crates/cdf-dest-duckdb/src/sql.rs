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
        _ => Err(CdfError::contract(format!(
            "DuckDB target {} must be a namecase-v1 table or schema.table identifier",
            target.as_str()
        ))),
    }
}

pub(crate) fn validate_ident(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return Err(CdfError::contract("DuckDB identifier cannot be empty"));
    };
    if !(first == '_' || first.is_ascii_lowercase()) {
        return Err(CdfError::contract(format!(
            "DuckDB identifier {identifier:?} must start with a lowercase letter or underscore"
        )));
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit()) {
        return Err(CdfError::contract(format!(
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
            CdfError::internal(format!("system clock is before UNIX_EPOCH: {error}"))
        })?;
    i64::try_from(duration.as_millis())
        .map_err(|_| CdfError::internal("system time milliseconds exceed i64"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DuckDbExceptionType {
    OutOfMemory,
    Other,
}

#[derive(Debug)]
pub(crate) struct DuckDbFailure {
    pub(crate) exception_type: DuckDbExceptionType,
    pub(crate) error: CdfError,
}

impl DuckDbFailure {
    pub(crate) fn other(error: CdfError) -> Self {
        Self {
            exception_type: DuckDbExceptionType::Other,
            error,
        }
    }
}

pub(crate) fn duckdb_failure(context: impl Into<String>, error: duckdb::Error) -> DuckDbFailure {
    let context = context.into();
    let (exception_type, message) = match &error {
        duckdb::Error::DuckDBFailure(_, Some(message)) => {
            let structured = serde_json::from_str::<serde_json::Value>(message)
                .ok()
                .and_then(|value| {
                    Some((
                        value.get("exception_type")?.as_str()?.to_owned(),
                        value.get("exception_message")?.as_str()?.to_owned(),
                    ))
                });
            structured.map_or_else(
                || (DuckDbExceptionType::Other, error.to_string()),
                |(exception_type, exception_message)| {
                    (
                        if exception_type == "Out of Memory" {
                            DuckDbExceptionType::OutOfMemory
                        } else {
                            DuckDbExceptionType::Other
                        },
                        exception_message,
                    )
                },
            )
        }
        _ => (DuckDbExceptionType::Other, error.to_string()),
    };
    DuckDbFailure {
        exception_type,
        error: CdfError::destination(format!("{context}: {message}")),
    }
}

pub(crate) fn duckdb_error(context: impl Into<String>, error: duckdb::Error) -> CdfError {
    duckdb_failure(context, error).error
}

pub(crate) fn io_error(context: impl Into<String>, error: std::io::Error) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}

pub(crate) fn json_error(error: serde_json::Error) -> CdfError {
    CdfError::data(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_duckdb_out_of_memory_is_typed_without_message_matching() {
        let error = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(duckdb::ffi::DuckDBError),
            Some(
                r#"{"exception_type":"Out of Memory","exception_message":"failed to allocate block"}"#
                    .to_owned(),
            ),
        );
        let failure = duckdb_failure("ingest package", error);
        assert_eq!(failure.exception_type, DuckDbExceptionType::OutOfMemory);
        assert_eq!(
            failure.error.message,
            "ingest package: failed to allocate block"
        );
    }

    #[test]
    fn structured_non_memory_exception_never_enters_memory_retry() {
        let error = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(duckdb::ffi::DuckDBError),
            Some(
                r#"{"exception_type":"Conversion","exception_message":"invalid cast"}"#.to_owned(),
            ),
        );
        let failure = duckdb_failure("ingest package", error);
        assert_eq!(failure.exception_type, DuckDbExceptionType::Other);
        assert_eq!(failure.error.message, "ingest package: invalid cast");
    }

    #[test]
    fn configured_duckdb_runtime_emits_a_typed_out_of_memory_exception() {
        let config = duckdb::Config::default()
            .with("memory_limit", "8MB")
            .unwrap()
            .with("max_temp_directory_size", "1B")
            .unwrap()
            .with("errors_as_json", "true")
            .unwrap();
        let connection = duckdb::Connection::open_in_memory_with_flags(config).unwrap();
        let error = connection
            .execute_batch("SELECT list(i) FROM range(10000000) AS values(i)")
            .unwrap_err();
        let failure = duckdb_failure("exercise bounded DuckDB runtime", error);
        assert_eq!(failure.exception_type, DuckDbExceptionType::OutOfMemory);
    }
}
