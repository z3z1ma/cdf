//! HTTP status, identity, timeout, and sanitized transport error classification.

use std::future::Future;
use std::time::Duration;

use cdf_kernel::{CdfError, Result};
use cdf_object_access::FileIdentityMetadata;

pub(crate) fn validate_response(
    response: &reqwest::Response,
    expected_status: u16,
    expected: &FileIdentityMetadata,
) -> Result<()> {
    let status = response.status().as_u16();
    if status != expected_status {
        let retry_after_ms = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(|seconds| seconds.saturating_mul(1_000));
        return Err(classify_http_byte_source_status(
            status,
            expected_status,
            retry_after_ms,
        ));
    }
    if let Some(expected_etag) = expected.etag.as_deref() {
        let etag = response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok());
        if etag != Some(expected_etag) {
            return Err(CdfError::data(
                "HTTP object generation changed (ETag mismatch)",
            ));
        }
    } else if let Some(expected_modified) = expected.modified.as_deref() {
        let modified = response
            .headers()
            .get("last-modified")
            .and_then(|value| value.to_str().ok());
        if modified != Some(expected_modified) {
            return Err(CdfError::data(
                "HTTP object generation changed (Last-Modified mismatch)",
            ));
        }
    }
    if expected_status == 200 {
        let content_length = response
            .headers()
            .get("content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        if content_length != expected.size_bytes {
            return Err(CdfError::data(format!(
                "HTTP response Content-Length {:?} does not match planned {:?}",
                content_length, expected.size_bytes
            )));
        }
    }
    Ok(())
}

pub(crate) fn classify_http_byte_source_status(
    status: u16,
    expected_status: u16,
    retry_after_ms: Option<u64>,
) -> CdfError {
    let message = || format!("HTTP byte source expected status {expected_status}, got {status}");
    match status {
        401 | 403 => CdfError::auth(message()),
        408 | 425 | 500..=599 => CdfError::transient(message()),
        429 => CdfError::rate_limited(message(), retry_after_ms),
        412 => CdfError::data("HTTP object generation changed (precondition failed)"),
        200 if expected_status == 206 => {
            CdfError::data("HTTP byte source ignored the planned exact byte range")
        }
        300..=499 => CdfError::data(message()),
        _ => CdfError::data(message()),
    }
}

pub(crate) fn http_send_error(error: reqwest::Error) -> CdfError {
    CdfError::transient(format!(
        "send HTTP byte-source request: {}",
        sanitized_reqwest_error(error)
    ))
}

pub(crate) fn http_body_error(error: reqwest::Error) -> CdfError {
    CdfError::transient(format!(
        "stream HTTP byte-source response: {}",
        sanitized_reqwest_error(error)
    ))
}

pub(crate) async fn with_file_progress_deadline<T, F>(
    operation: &str,
    timeout: Duration,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| http_file_progress_timeout(operation, timeout))?
}

pub(crate) fn http_file_progress_timeout(operation: &str, timeout: Duration) -> CdfError {
    CdfError::transient(format!(
        "{operation} made no progress for {} ms",
        timeout.as_millis()
    ))
}

pub(crate) fn sanitized_reqwest_error(error: reqwest::Error) -> String {
    error.without_url().to_string()
}
