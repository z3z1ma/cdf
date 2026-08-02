use std::{error::Error as StdError, io, path::Path};

use cdf_kernel::{CdfError, ErrorKind};
use rusqlite::{Error, ffi::ErrorCode};

pub(crate) fn classify_destination_io(action: &str, error: &io::Error) -> CdfError {
    if let Some(embedded) = cdf_kernel::embedded_cdf_error(error) {
        return with_context(action, embedded);
    }
    let kind = if matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
    ) {
        ErrorKind::Destination
    } else {
        ErrorKind::Environment
    };
    CdfError::new(kind, format!("{action}: {error}"))
}

pub(crate) fn classify_sqlite_error(action: &str, error: Error) -> CdfError {
    classify_sqlite_error_in(action, error, SqliteErrorContext::General)
}

pub(crate) fn classify_sqlite_payload_error(action: &str, error: Error) -> CdfError {
    classify_sqlite_error_in(action, error, SqliteErrorContext::Payload)
}

#[derive(Clone, Copy)]
enum SqliteErrorContext {
    General,
    Payload,
}

fn classify_sqlite_error_in(action: &str, error: Error, context: SqliteErrorContext) -> CdfError {
    if let Some(embedded) = embedded_cdf_error(&error) {
        return with_context(action, embedded);
    }
    if matches!(&error, Error::InvalidPath(..)) {
        return CdfError::contract(format!(
            "{action}: SQLite destination database path is invalid"
        ));
    }
    let kind = match &error {
        Error::SqliteFailure(failure, _) => match failure.code {
            ErrorCode::DatabaseBusy
            | ErrorCode::DatabaseLocked
            | ErrorCode::FileLockingProtocolFailed
            | ErrorCode::SchemaChanged
            | ErrorCode::OperationAborted => ErrorKind::Transient,
            ErrorCode::PermissionDenied
            | ErrorCode::ReadOnly
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
            | ErrorCode::Unknown => ErrorKind::Destination,
            ErrorCode::ConstraintViolation => match context {
                SqliteErrorContext::Payload
                    if matches!(
                        failure.extended_code,
                        rusqlite::ffi::SQLITE_CONSTRAINT_NOTNULL
                            | rusqlite::ffi::SQLITE_CONSTRAINT_DATATYPE
                    ) =>
                {
                    ErrorKind::Data
                }
                SqliteErrorContext::General | SqliteErrorContext::Payload => ErrorKind::Destination,
            },
            ErrorCode::OperationInterrupted
            | ErrorCode::InternalMalfunction
            | ErrorCode::ApiMisuse
            | ErrorCode::AuthorizationForStatementDenied
            | ErrorCode::ParameterOutOfRange => ErrorKind::Internal,
            _ => ErrorKind::Destination,
        },
        Error::FromSqlConversionFailure(..)
        | Error::IntegralValueOutOfRange(..)
        | Error::Utf8Error(..)
        | Error::QueryReturnedNoRows
        | Error::QueryReturnedMoreThanOneRow
        | Error::InvalidColumnType(..) => ErrorKind::Destination,
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

pub(crate) fn classify_sqlite_execution_error(
    action: &str,
    error: Error,
    cancellation: &cdf_runtime::RunCancellation,
) -> CdfError {
    if cancellation.is_cancelled()
        && matches!(
            &error,
            Error::SqliteFailure(failure, _)
                if failure.code == ErrorCode::OperationInterrupted
        )
    {
        CdfError::internal(format!(
            "{action}: SQLite destination execution was cancelled"
        ))
    } else {
        classify_sqlite_error(action, error)
    }
}

pub(crate) fn classify_sqlite_open_error(action: &str, path: &Path, error: Error) -> CdfError {
    if matches!(
        &error,
        Error::SqliteFailure(failure, _) if failure.code == ErrorCode::CannotOpen
    ) && let Some(parent) = path.parent()
        && let Err(io_error) = std::fs::metadata(parent)
    {
        return classify_destination_io(action, &io_error);
    }
    classify_sqlite_error(action, error)
}

fn embedded_cdf_error(error: &(dyn StdError + 'static)) -> Option<CdfError> {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(error) = source.downcast_ref::<CdfError>() {
            return Some(error.clone());
        }
        if let Some(error) = source.downcast_ref::<io::Error>()
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
