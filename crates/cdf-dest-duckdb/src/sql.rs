use cdf_kernel::{CdfError, Result, TargetName, WriteDisposition};
use duckdb::Connection;

use crate::{MAIN_SCHEMA, models::TargetRef};

pub(crate) fn parse_target(target: &TargetName) -> Result<TargetRef> {
    let parts = target.as_str().split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [table] => Ok(TargetRef {
            schema: validate_system_ident(MAIN_SCHEMA)?,
            table: validate_ident(table)?,
        }),
        [schema, table] => Ok(TargetRef {
            schema: validate_ident(schema)?,
            table: validate_ident(table)?,
        }),
        _ => Err(CdfError::contract(format!(
            "DuckDB target {} must be a namecase-v1 table or schema.table identifier",
            target.as_str()
        ))),
    }
}

impl TargetRef {
    pub(crate) fn sql_name(&self) -> String {
        if self.schema.as_str() == MAIN_SCHEMA {
            quote_ident(&self.table)
        } else {
            format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table))
        }
    }
}

pub(crate) fn validate_ident(identifier: &str) -> Result<cdf_dest_sql::ValidatedSqlIdentifier> {
    cdf_dest_sql::ValidatedSqlIdentifier::user(&crate::sheet::duckdb_identifier_rules(), identifier)
}

pub(crate) fn validate_system_ident(
    identifier: &str,
) -> Result<cdf_dest_sql::ValidatedSqlIdentifier> {
    cdf_dest_sql::ValidatedSqlIdentifier::system(
        &crate::sheet::duckdb_identifier_rules(),
        identifier,
    )
}

pub(crate) fn framework_ident(identifier: &'static str) -> cdf_dest_sql::ValidatedSqlIdentifier {
    validate_system_ident(identifier).expect("framework identifier must satisfy DuckDB sheet rules")
}

pub(crate) fn quote_ident(identifier: &cdf_dest_sql::ValidatedSqlIdentifier) -> String {
    format!("\"{}\"", identifier.as_str().replace('"', "\"\""))
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
    CdfError::environment(format!(
        "{}: {error}; check the local path, permissions, device health, free space, and process file limits",
        context.into()
    ))
}

pub(crate) fn json_error(error: serde_json::Error) -> CdfError {
    CdfError::data(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_filesystem_failure_is_environment_not_destination() {
        let error = io_error(
            "create DuckDB sidecar",
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        );
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Environment);
        assert!(error.message.contains("permissions"));
        assert!(error.message.contains("process file limits"));
    }

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
