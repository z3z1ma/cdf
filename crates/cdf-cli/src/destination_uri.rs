use cdf_kernel::CdfError;

pub(crate) fn redact_error_value(mut error: CdfError, secret: Option<&str>) -> CdfError {
    if let Some(secret) = secret
        && !secret.is_empty()
    {
        error.message = error.message.replace(secret, "[REDACTED]");
    }
    error
}
