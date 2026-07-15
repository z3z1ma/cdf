use cdf_kernel::CdfError;

use crate::{HttpResponse, support::retry_after_ms};

/// Maps one HTTP response into CDF's typed error taxonomy.
///
/// This function deliberately owns no attempt, delay, or budget policy. The
/// runtime scheduler consumes the typed result and is the sole retry authority.
pub fn classify_response(response: &HttpResponse) -> Option<CdfError> {
    match response.status {
        200..=399 => None,
        401 | 403 => Some(CdfError::auth(format!(
            "HTTP {} requires authentication refresh or credential review",
            response.status
        ))),
        408 | 500..=599 => Some(CdfError::transient(format!(
            "HTTP {} from upstream",
            response.status
        ))),
        429 => Some(CdfError::rate_limited(
            "HTTP 429 rate limit",
            retry_after_ms(response),
        )),
        400..=499 => Some(CdfError::data(format!(
            "HTTP {} response is not retryable as a request",
            response.status
        ))),
        _ => Some(CdfError::internal(format!(
            "unexpected HTTP status {}",
            response.status
        ))),
    }
}
