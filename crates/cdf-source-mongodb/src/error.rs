use std::error::Error as StdError;

use cdf_kernel::{CdfError, ErrorKind};
use mongodb::error::{Error, ErrorKind as MongoErrorKind};

pub(crate) fn classify_mongodb_error(action: &str, error: Error) -> CdfError {
    if let Some(embedded) = embedded_cdf_error(&error) {
        return with_context(action, embedded);
    }
    let kind = match error.kind.as_ref() {
        MongoErrorKind::Authentication { .. } => ErrorKind::Auth,
        MongoErrorKind::InvalidArgument { .. } => ErrorKind::Contract,
        MongoErrorKind::Bson(_) | MongoErrorKind::InvalidResponse { .. } => ErrorKind::Data,
        MongoErrorKind::Command(command) if matches!(command.code, 2 | 14 | 20) => {
            ErrorKind::Contract
        }
        MongoErrorKind::Command(command) if command.code == 26 => ErrorKind::Data,
        MongoErrorKind::Command(command) if matches!(command.code, 13 | 18) => ErrorKind::Auth,
        MongoErrorKind::Command(command)
            if matches!(command.code, 6 | 7 | 89 | 91 | 189 | 262 | 9001) =>
        {
            ErrorKind::Transient
        }
        MongoErrorKind::Command(command) if command.code == 50 => ErrorKind::Transient,
        MongoErrorKind::Command(command) if command.code == 16500 => ErrorKind::RateLimited,
        MongoErrorKind::Io(error) => classify_io(error),
        MongoErrorKind::DnsResolve { .. } | MongoErrorKind::InvalidTlsConfig { .. } => {
            ErrorKind::Environment
        }
        MongoErrorKind::ConnectionPoolCleared { .. } | MongoErrorKind::ServerSelection { .. } => {
            ErrorKind::Transient
        }
        MongoErrorKind::Shutdown => ErrorKind::Internal,
        MongoErrorKind::IncompatibleServer { .. } | MongoErrorKind::SessionsNotSupported => {
            ErrorKind::Contract
        }
        MongoErrorKind::Internal { .. } => ErrorKind::Internal,
        _ => ErrorKind::Data,
    };
    let detail = match kind {
        ErrorKind::Auth => "MongoDB authentication failed",
        ErrorKind::Contract => "MongoDB rejected the compiled request",
        ErrorKind::Environment => "the host could not provide MongoDB transport facilities",
        ErrorKind::Transient | ErrorKind::RateLimited => "MongoDB is temporarily unavailable",
        ErrorKind::Internal => "the MongoDB driver reported an internal invariant failure",
        _ => "MongoDB returned invalid or incompatible source data",
    };
    CdfError::new(kind, format!("{action}: {detail}"))
}

fn embedded_cdf_error(error: &Error) -> Option<CdfError> {
    let mut source: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(current) = source {
        if let Some(error) = current.downcast_ref::<CdfError>() {
            return Some(error.clone());
        }
        if let Some(error) = current.downcast_ref::<Error>() {
            if let Some(embedded) = error.get_custom::<CdfError>() {
                return Some(embedded.clone());
            }
            if let MongoErrorKind::Io(error) = error.kind.as_ref()
                && let Some(embedded) = cdf_kernel::embedded_cdf_error(error)
            {
                return Some(embedded);
            }
        }
        source = match current.downcast_ref::<std::io::Error>() {
            Some(error) => error
                .get_ref()
                .map(|source| source as &(dyn StdError + 'static)),
            None => current.source(),
        };
    }
    None
}

fn classify_io(error: &std::io::Error) -> ErrorKind {
    use std::io::ErrorKind as Io;

    if let Some(embedded) = cdf_kernel::embedded_cdf_error(error) {
        return embedded.kind;
    }
    match error.kind() {
        Io::TimedOut
        | Io::WouldBlock
        | Io::Interrupted
        | Io::ConnectionAborted
        | Io::ConnectionRefused
        | Io::ConnectionReset
        | Io::NotConnected
        | Io::BrokenPipe => ErrorKind::Transient,
        Io::UnexpectedEof | Io::InvalidData => ErrorKind::Data,
        _ => ErrorKind::Environment,
    }
}

fn with_context(action: &str, mut error: CdfError) -> CdfError {
    error.message = format!("{action}: {}", error.message);
    error
}
