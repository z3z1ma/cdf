use std::{error::Error as StdError, path::Path};

use cdf_kernel::{CdfError, ErrorKind};
use rusqlite::{Error, ffi::ErrorCode};

pub(crate) fn validate_source_file(path: &Path) -> Result<(), CdfError> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_) => Err(CdfError::data(
            "SQLite source location is not a regular database file",
        )),
        Err(error) => Err(classify_source_io("inspect SQLite source database", &error)),
    }
}

pub(crate) fn classify_source_io(action: &str, error: &std::io::Error) -> CdfError {
    if let Some(embedded) = cdf_kernel::embedded_cdf_error(error) {
        return with_context(action, embedded);
    }
    let kind = if error.kind() == std::io::ErrorKind::NotFound
        || error.kind() == std::io::ErrorKind::UnexpectedEof
        || error.kind() == std::io::ErrorKind::InvalidData
        || cdf_kernel::is_filesystem_loop(error)
    {
        ErrorKind::Data
    } else {
        ErrorKind::Environment
    };
    CdfError::new(kind, format!("{action}: {error}"))
}

pub(crate) fn classify_sqlite_error(action: &str, error: Error) -> CdfError {
    if let Some(embedded) = embedded_cdf_error(&error) {
        return with_context(action, embedded);
    }
    if matches!(&error, Error::InvalidPath(..)) {
        return CdfError::contract(format!("{action}: SQLite source database path is invalid"));
    }
    let kind = match &error {
        Error::SqliteFailure(failure, _) => match failure.code {
            ErrorCode::DatabaseBusy
            | ErrorCode::DatabaseLocked
            | ErrorCode::FileLockingProtocolFailed
            | ErrorCode::SchemaChanged
            | ErrorCode::OperationAborted => ErrorKind::Transient,
            ErrorCode::PermissionDenied
            | ErrorCode::OutOfMemory
            | ErrorCode::SystemIoFailure
            | ErrorCode::DiskFull
            | ErrorCode::CannotOpen
            | ErrorCode::NoLargeFileSupport => ErrorKind::Environment,
            ErrorCode::DatabaseCorrupt
            | ErrorCode::NotADatabase
            | ErrorCode::TooBig
            | ErrorCode::TypeMismatch
            | ErrorCode::NotFound
            | ErrorCode::Unknown => ErrorKind::Data,
            ErrorCode::ReadOnly
            | ErrorCode::OperationInterrupted
            | ErrorCode::ConstraintViolation
            | ErrorCode::InternalMalfunction
            | ErrorCode::ApiMisuse
            | ErrorCode::AuthorizationForStatementDenied
            | ErrorCode::ParameterOutOfRange => ErrorKind::Internal,
            _ => ErrorKind::Data,
        },
        Error::FromSqlConversionFailure(..)
        | Error::IntegralValueOutOfRange(..)
        | Error::Utf8Error(..)
        | Error::QueryReturnedNoRows
        | Error::QueryReturnedMoreThanOneRow
        | Error::InvalidColumnType(..) => ErrorKind::Data,
        Error::InvalidPath(..) | Error::NulError(..) => ErrorKind::Contract,
        Error::SqliteSingleThreadedMode
        | Error::InvalidParameterName(..)
        | Error::ExecuteReturnedResults
        | Error::InvalidColumnIndex(..)
        | Error::InvalidColumnName(..)
        | Error::StatementChangedRows(..)
        | Error::InvalidQuery
        | Error::UnwindingPanic
        | Error::MultipleStatement
        | Error::InvalidParameterCount(..) => ErrorKind::Internal,
        _ => ErrorKind::Internal,
    };
    CdfError::new(kind, format!("{action}: {error}"))
}

pub(crate) fn classify_sqlite_open_error(action: &str, path: &Path, error: Error) -> CdfError {
    if matches!(
        &error,
        Error::SqliteFailure(failure, _) if failure.code == ErrorCode::CannotOpen
    ) {
        match std::fs::metadata(path) {
            Ok(metadata) if !metadata.is_file() => {
                return CdfError::data(format!(
                    "{action}: SQLite source location is not a regular database file"
                ));
            }
            Err(io_error) => return classify_source_io(action, &io_error),
            Ok(_) => {}
        }
    }
    classify_sqlite_error(action, error)
}

fn embedded_cdf_error(error: &(dyn StdError + 'static)) -> Option<CdfError> {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(error) = source.downcast_ref::<CdfError>() {
            return Some(error.clone());
        }
        if let Some(error) = source.downcast_ref::<std::io::Error>()
            && let Some(error) = cdf_kernel::embedded_cdf_error(error)
        {
            return Some(error);
        }
        current = source.source();
    }
    None
}

fn with_context(action: &str, mut error: CdfError) -> CdfError {
    error.message = format!("{action}: {}", error.message);
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_typed_error_kind_and_retry_through_rusqlite_wrapper() {
        let error = Error::ToSqlConversionFailure(Box::new(CdfError::rate_limited(
            "provider throttled",
            Some(275),
        )));
        let classified = classify_sqlite_error("bind SQLite value", error);
        assert_eq!(classified.kind, ErrorKind::RateLimited);
        assert_eq!(classified.retry_after_ms, Some(275));
        assert!(classified.message.contains("bind SQLite value"));
        assert!(classified.message.contains("provider throttled"));

        let nested = Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
            CdfError::data("source row is malformed"),
        )));
        let classified = classify_sqlite_error("bind SQLite value", nested);
        assert_eq!(classified.kind, ErrorKind::Data);
        assert_eq!(classified.retry_after_ms, None);
        assert!(classified.message.contains("source row is malformed"));
    }

    #[test]
    fn distinguishes_corrupt_busy_and_host_owned_failures() {
        let sqlite = |code| Error::SqliteFailure(rusqlite::ffi::Error::new(code), None);
        assert_eq!(
            classify_sqlite_error("read", sqlite(rusqlite::ffi::SQLITE_CORRUPT)).kind,
            ErrorKind::Data
        );
        assert_eq!(
            classify_sqlite_error("read", sqlite(rusqlite::ffi::SQLITE_BUSY)).kind,
            ErrorKind::Transient
        );
        assert_eq!(
            classify_sqlite_error("read", sqlite(rusqlite::ffi::SQLITE_IOERR)).kind,
            ErrorKind::Environment
        );
        assert_eq!(
            classify_source_io(
                "read",
                &std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied")
            )
            .kind,
            ErrorKind::Environment
        );
        assert_eq!(
            classify_source_io(
                "read",
                &std::io::Error::new(std::io::ErrorKind::NotFound, "missing")
            )
            .kind,
            ErrorKind::Data
        );
    }

    #[test]
    fn redacts_invalid_database_path() {
        let error = classify_sqlite_error(
            "open SQLite source database",
            Error::InvalidPath("/private/credentials/customer.sqlite".into()),
        );
        assert_eq!(error.kind, ErrorKind::Contract);
        assert!(!error.message.contains("credentials"));
        assert!(!error.message.contains("customer.sqlite"));
    }
}
