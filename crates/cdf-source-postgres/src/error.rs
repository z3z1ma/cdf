use std::error::Error as StdError;

use cdf_kernel::{CdfError, ErrorKind};

pub(crate) fn classify_postgres_error(action: &str, error: postgres::Error) -> CdfError {
    classify_postgres_error_ref(action, &error)
}

pub(crate) fn classify_postgres_io_error(action: &str, error: std::io::Error) -> CdfError {
    if let Some(error) = cdf_kernel::embedded_cdf_error(&error) {
        return with_context(action, error);
    }
    if let Some(error) = nested_postgres_error(&error) {
        return classify_postgres_error_ref(action, error);
    }
    let kind = classify_transport_io(&error);
    CdfError::new(
        kind,
        format!("{action}: PostgreSQL source transport failed"),
    )
}

fn classify_postgres_error_ref(action: &str, error: &postgres::Error) -> CdfError {
    if let Some(error) = embedded_cdf_error(error) {
        return with_context(action, error);
    }
    if let Some(database_error) = error.as_db_error() {
        let code = database_error.code().code();
        let kind = classify_sqlstate(code);
        let detail = if kind == ErrorKind::Auth {
            "authentication or authorization failed".to_owned()
        } else {
            database_error.message().to_owned()
        };
        return CdfError::new(
            kind,
            format!(
                "{action}: PostgreSQL server rejected the source request (SQLSTATE {code}): {detail}"
            ),
        );
    }
    if let Some(error) = nested_io_error(error) {
        return CdfError::new(
            classify_transport_io(error),
            format!("{action}: PostgreSQL source transport failed"),
        );
    }
    CdfError::transient(format!(
        "{action}: PostgreSQL source request failed before the server returned a SQLSTATE"
    ))
}

fn classify_sqlstate(code: &str) -> ErrorKind {
    if code.starts_with("28") || code == "42501" {
        ErrorKind::Auth
    } else if code.starts_with("08")
        || code.starts_with("40")
        || matches!(code, "57014" | "57P01" | "57P02" | "57P03")
    {
        ErrorKind::Transient
    } else if code.starts_with("53") || code == "55P03" {
        ErrorKind::RateLimited
    } else {
        ErrorKind::Data
    }
}

fn classify_transport_io(error: &std::io::Error) -> ErrorKind {
    use std::io::ErrorKind as Io;

    match error.kind() {
        Io::TimedOut
        | Io::WouldBlock
        | Io::Interrupted
        | Io::UnexpectedEof
        | Io::ConnectionAborted
        | Io::ConnectionRefused
        | Io::ConnectionReset
        | Io::NotConnected
        | Io::BrokenPipe => ErrorKind::Transient,
        Io::InvalidData => ErrorKind::Data,
        _ => ErrorKind::Environment,
    }
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

fn nested_postgres_error<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a postgres::Error> {
    if let Some(error) = error.downcast_ref::<postgres::Error>() {
        return Some(error);
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>()
        && let Some(source) = error.get_ref()
        && let Some(error) = nested_postgres_error(source)
    {
        return Some(error);
    }
    error.source().and_then(nested_postgres_error)
}

fn nested_io_error<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a std::io::Error> {
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        if let Some(source) = error.get_ref()
            && let Some(error) = nested_io_error(source)
        {
            return Some(error);
        }
        return Some(error);
    }
    error.source().and_then(nested_io_error)
}

fn with_context(action: &str, mut error: CdfError) -> CdfError {
    error.message = format!("{action}: {}", error.message);
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstate_owner_distinguishes_auth_transport_capacity_and_data() {
        assert_eq!(classify_sqlstate("28P01"), ErrorKind::Auth);
        assert_eq!(classify_sqlstate("42501"), ErrorKind::Auth);
        assert_eq!(classify_sqlstate("08006"), ErrorKind::Transient);
        assert_eq!(classify_sqlstate("40001"), ErrorKind::Transient);
        assert_eq!(classify_sqlstate("53300"), ErrorKind::RateLimited);
        assert_eq!(classify_sqlstate("22P02"), ErrorKind::Data);
    }

    #[test]
    fn copy_io_preserves_typed_owner_and_classifies_transport() {
        let typed = std::io::Error::other(std::io::Error::other(CdfError::rate_limited(
            "server capacity",
            Some(250),
        )));
        let classified = classify_postgres_io_error("read COPY", typed);
        assert_eq!(classified.kind, ErrorKind::RateLimited);
        assert_eq!(classified.retry_after_ms, Some(250));
        assert!(classified.message.contains("server capacity"));

        let reset = classify_postgres_io_error(
            "read COPY",
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "secret server detail"),
        );
        assert_eq!(reset.kind, ErrorKind::Transient);
        assert!(!reset.message.contains("secret server detail"));

        let invalid = classify_postgres_io_error(
            "read COPY",
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid frame"),
        );
        assert_eq!(invalid.kind, ErrorKind::Data);
    }
}
