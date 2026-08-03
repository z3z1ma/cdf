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
        Error::Compression(_) | Error::Decompression(_) => {
            (ErrorKind::Data, "invalid compressed source response")
        }
        Error::ResponseTooLarge { .. } => (
            ErrorKind::Data,
            "source response exceeded its admitted memory bound",
        ),
        Error::BadResponse(message) => {
            let code = clickhouse_server_code(message);
            let kind = match code {
                Some(192 | 193 | 194 | 516) => ErrorKind::Auth,
                Some(202 | 203 | 204 | 242) => ErrorKind::RateLimited,
                Some(159 | 209 | 210 | 279 | 319) => ErrorKind::Transient,
                Some(47 | 50 | 60 | 62 | 81 | 117 | 122 | 386) | None => ErrorKind::Data,
                Some(_) => ErrorKind::Data,
            };
            return CdfError::new(
                kind,
                code.map_or_else(
                    || format!("{action}: ClickHouse server rejected the source request"),
                    |code| {
                        format!(
                            "{action}: ClickHouse server rejected the source request (code {code})"
                        )
                    },
                ),
            );
        }
        Error::InvalidParams(_) => (
            ErrorKind::Internal,
            "generated query parameters were invalid",
        ),
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
            ErrorKind::Data,
            "source response contradicted its pinned schema",
        ),
        Error::SequenceMustHaveLength | Error::DeserializeAnyNotSupported | Error::Custom(_) => {
            (ErrorKind::Internal, "official client invariant failed")
        }
        Error::Other(_) => (
            nested_io_error(&error)
                .map(classify_source_io_kind)
                .unwrap_or(ErrorKind::Data),
            "Arrow source response could not be decoded",
        ),
        _ => (ErrorKind::Data, "source request failed"),
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

fn classify_source_io_kind(error: &std::io::Error) -> ErrorKind {
    use std::io::ErrorKind as Io;

    match error.kind() {
        Io::UnexpectedEof | Io::InvalidData => ErrorKind::Data,
        _ => classify_network_io_kind(error),
    }
}

fn nested_io_error<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a std::io::Error> {
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        if let Some(nested) = error.get_ref().and_then(|source| nested_io_error(source)) {
            return Some(nested);
        }
        return Some(error);
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
    fn classifies_stable_server_codes_without_echoing_server_text() {
        let secret = "password=hunter2";
        let auth = classify_clickhouse_error(
            "discover ClickHouse schema",
            Error::BadResponse(format!("Code: 516. authentication failed; {secret}")),
        );
        assert_eq!(auth.kind, ErrorKind::Auth);
        assert!(!auth.message.contains(secret));

        let quota = classify_clickhouse_error(
            "stream ClickHouse table",
            Error::BadResponse("Code: 202. too many queries".to_owned()),
        );
        assert_eq!(quota.kind, ErrorKind::RateLimited);
    }

    #[test]
    fn preserves_nested_typed_error_provenance() {
        let wrapped = Error::Other(Box::new(std::io::Error::other(CdfError::rate_limited(
            "provider throttled",
            Some(250),
        ))));
        let classified = classify_clickhouse_error("read Arrow stream", wrapped);
        assert_eq!(classified.kind, ErrorKind::RateLimited);
        assert_eq!(classified.retry_after_ms, Some(250));
        assert!(classified.message.contains("provider throttled"));
    }

    #[test]
    fn response_limits_are_data_failures_without_remote_detail() {
        let classified = classify_clickhouse_error(
            "read Arrow stream",
            Error::ResponseTooLarge {
                stage: "secret remote stage",
                limit: 42,
            },
        );
        assert_eq!(classified.kind, ErrorKind::Data);
        assert_eq!(
            classified.message,
            "read Arrow stream: source response exceeded its admitted memory bound"
        );
    }

    #[test]
    fn recursively_distinguishes_host_transport_and_malformed_io() {
        let host = Error::Network(Box::new(std::io::Error::other(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "private host detail",
        ))));
        assert_eq!(
            classify_clickhouse_error("connect", host).kind,
            ErrorKind::Environment
        );

        let timeout = Error::Network(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "late",
        )));
        assert_eq!(
            classify_clickhouse_error("connect", timeout).kind,
            ErrorKind::Transient
        );

        let malformed = Error::Other(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad Arrow",
        )));
        assert_eq!(
            classify_clickhouse_error("decode", malformed).kind,
            ErrorKind::Data
        );

        let malformed_tls = Error::Network(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid certificate",
        )));
        assert_eq!(
            classify_clickhouse_error("construct TLS", malformed_tls).kind,
            ErrorKind::Environment
        );

        assert_eq!(
            classify_clickhouse_error(
                "construct TLS",
                Error::Unsupported("secret resolver detail".to_owned()),
            )
            .kind,
            ErrorKind::Environment
        );
    }
}
