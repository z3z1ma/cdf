use std::error::Error as StdError;

use cdf_kernel::{CdfError, ErrorKind};
use clickhouse::error::Error;

pub(crate) fn classify_clickhouse_error(action: &str, error: Error) -> CdfError {
    if let Some(embedded) = embedded_cdf_error(&error) {
        return with_context(action, embedded);
    }
    let (kind, detail) = match &error {
        Error::Network(_) => (
            nested_io_error(&error)
                .map(classify_network_io_kind)
                .unwrap_or(ErrorKind::Transient),
            "transport failure",
        ),
        Error::TimedOut => (ErrorKind::Transient, "transport timeout"),
        Error::Compression(_) => (ErrorKind::Internal, "payload compression invariant failed"),
        Error::Decompression(_) => (
            ErrorKind::Destination,
            "destination response compression was invalid",
        ),
        Error::ResponseTooLarge { .. } => (
            ErrorKind::Destination,
            "server response exceeded its admitted memory bound",
        ),
        Error::BadResponse(message) => {
            let code = clickhouse_server_code(message);
            let kind = match code {
                Some(192 | 193 | 194 | 516) => ErrorKind::Auth,
                Some(202 | 203 | 204 | 241 | 242) => ErrorKind::RateLimited,
                Some(159 | 209 | 210 | 279 | 319) => ErrorKind::Transient,
                Some(62) => ErrorKind::Internal,
                Some(50 | 117 | 386) => ErrorKind::Data,
                _ => ErrorKind::Destination,
            };
            return CdfError::new(
                kind,
                code.map_or_else(
                    || format!("{action}: ClickHouse server rejected the destination request"),
                    |code| {
                        format!(
                            "{action}: ClickHouse server rejected the destination request (code {code})"
                        )
                    },
                ),
            );
        }
        Error::InvalidParams(_) => (ErrorKind::Internal, "generated request was invalid"),
        Error::Unsupported(_) => (
            ErrorKind::Environment,
            "client transport or TLS support is unavailable",
        ),
        Error::InvalidColumnsHeader(_)
        | Error::SchemaMismatch(_)
        | Error::NotEnoughData
        | Error::InvalidUtf8Encoding(_)
        | Error::InvalidTagEncoding(_)
        | Error::VariantDiscriminatorIsOutOfBound(_)
        | Error::RowNotFound => (
            ErrorKind::Destination,
            "destination schema or response contradicted the plan",
        ),
        Error::SequenceMustHaveLength | Error::DeserializeAnyNotSupported | Error::Custom(_) => {
            (ErrorKind::Internal, "official client invariant failed")
        }
        Error::Other(_) => (
            nested_io_error(&error)
                .map(classify_network_io_kind)
                .unwrap_or(ErrorKind::Data),
            "Arrow destination request failed",
        ),
        _ => (ErrorKind::Destination, "destination request failed"),
    };
    CdfError::new(kind, format!("{action}: {detail}"))
}

fn classify_network_io_kind(error: &std::io::Error) -> ErrorKind {
    use std::io::ErrorKind as Io;
    match error.kind() {
        Io::TimedOut
        | Io::WouldBlock
        | Io::Interrupted
        | Io::ConnectionAborted
        | Io::ConnectionRefused
        | Io::ConnectionReset
        | Io::NotConnected
        | Io::BrokenPipe => ErrorKind::Transient,
        _ => ErrorKind::Environment,
    }
}

fn nested_io_error<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a std::io::Error> {
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        return error
            .get_ref()
            .and_then(|source| nested_io_error(source))
            .or(Some(error));
    }
    if let Some(error) = error.downcast_ref::<Error>()
        && let Some(source) = clickhouse_boxed_source(error)
        && let Some(error) = nested_io_error(source)
    {
        return Some(error);
    }
    error.source().and_then(nested_io_error)
}

fn clickhouse_server_code(message: &str) -> Option<u32> {
    let remainder = message.split_once("Code:")?.1.trim_start();
    let digits = remainder
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
}

fn embedded_cdf_error(error: &(dyn StdError + 'static)) -> Option<CdfError> {
    if let Some(error) = error.downcast_ref::<Error>()
        && let Some(source) = clickhouse_boxed_source(error)
        && let Some(error) = embedded_cdf_error(source)
    {
        return Some(error);
    }
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

fn clickhouse_boxed_source(error: &Error) -> Option<&(dyn StdError + 'static)> {
    match error {
        Error::InvalidParams(error)
        | Error::Network(error)
        | Error::Compression(error)
        | Error::Decompression(error)
        | Error::InvalidColumnsHeader(error)
        | Error::Other(error) => Some(error.as_ref()),
        _ => None,
    }
}

fn with_context(action: &str, mut error: CdfError) -> CdfError {
    error.message = format!("{action}: {}", error.message);
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_cdf_errors_survive_direct_and_nested_client_wrappers() {
        let direct = classify_clickhouse_error(
            "write segment",
            Error::Other(Box::new(CdfError::auth("credential owner"))),
        );
        assert_eq!(direct.kind, ErrorKind::Auth);
        assert_eq!(direct.message, "write segment: credential owner");

        let nested = std::io::Error::other(std::io::Error::other(CdfError::rate_limited(
            "provider throttle",
            Some(875),
        )));
        let nested = classify_clickhouse_error("flush segment", Error::Network(Box::new(nested)));
        assert_eq!(nested.kind, ErrorKind::RateLimited);
        assert_eq!(nested.retry_after_ms, Some(875));
        assert_eq!(nested.message, "flush segment: provider throttle");
    }

    #[test]
    fn transport_and_server_failures_keep_their_repair_owner() {
        let permission = classify_clickhouse_error(
            "connect",
            Error::Network(Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "socket denied",
            ))),
        );
        assert_eq!(permission.kind, ErrorKind::Environment);

        let timeout = classify_clickhouse_error(
            "connect",
            Error::Network(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "late",
            ))),
        );
        assert_eq!(timeout.kind, ErrorKind::Transient);
        assert_eq!(
            classify_clickhouse_error(
                "query",
                Error::BadResponse("Code: 516. AUTH secret-password".to_owned())
            )
            .kind,
            ErrorKind::Auth
        );
        assert_eq!(
            classify_clickhouse_error(
                "query",
                Error::BadResponse("Code: 242. TOO_MANY_SIMULTANEOUS_QUERIES".to_owned())
            )
            .kind,
            ErrorKind::RateLimited
        );
        assert_eq!(
            classify_clickhouse_error(
                "query",
                Error::BadResponse("Code: 241. MEMORY_LIMIT_EXCEEDED".to_owned())
            )
            .kind,
            ErrorKind::RateLimited
        );
        assert_eq!(
            classify_clickhouse_error(
                "query",
                Error::BadResponse("Code: 209. SOCKET_TIMEOUT".to_owned())
            )
            .kind,
            ErrorKind::Transient
        );
        let generated = classify_clickhouse_error(
            "query",
            Error::BadResponse("Code: 62. SYNTAX_ERROR secret-password".to_owned()),
        );
        assert_eq!(generated.kind, ErrorKind::Internal);
        assert!(!generated.message.contains("secret-password"));
        assert_eq!(
            generated.message,
            "query: ClickHouse server rejected the destination request (code 62)"
        );
        assert_eq!(
            classify_clickhouse_error(
                "query",
                Error::BadResponse("Code: 60. UNKNOWN_TABLE secret-password".to_owned())
            )
            .kind,
            ErrorKind::Destination
        );
        assert_eq!(
            classify_clickhouse_error(
                "query",
                Error::BadResponse("unstructured secret-password".to_owned())
            )
            .kind,
            ErrorKind::Destination
        );
    }

    #[test]
    fn generated_request_invariants_are_internal_unless_a_typed_owner_is_embedded() {
        let internal = classify_clickhouse_error(
            "bind query",
            Error::InvalidParams(Box::new(std::fmt::Error)),
        );
        assert_eq!(internal.kind, ErrorKind::Internal);

        let contract = classify_clickhouse_error(
            "bind query",
            Error::InvalidParams(Box::new(CdfError::contract("invalid caller bound"))),
        );
        assert_eq!(contract.kind, ErrorKind::Contract);
        assert_eq!(contract.message, "bind query: invalid caller bound");
    }
}
