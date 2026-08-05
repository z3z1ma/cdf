use std::{error::Error as StdError, fmt};

use arrow_schema::ArrowError;
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, CdfError>;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    Transient,
    RateLimited,
    Auth,
    Contract,
    Data,
    Destination,
    Environment,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdfError {
    pub kind: ErrorKind,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub retry_after_ms: Option<u64>,
}

impl CdfError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            code: None,
            retry_after_ms: None,
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Transient, message)
    }

    pub fn rate_limited(message: impl Into<String>, retry_after_ms: Option<u64>) -> Self {
        Self {
            kind: ErrorKind::RateLimited,
            message: message.into(),
            code: None,
            retry_after_ms,
        }
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn auth(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Auth, message)
    }

    pub fn contract(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Contract, message)
    }

    pub fn data(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Data, message)
    }

    pub fn destination(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Destination, message)
    }

    pub fn environment(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Environment, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }
}

impl fmt::Display for CdfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.retry_after_ms {
            Some(retry_after_ms) => write!(
                f,
                "{:?}: {} (retry after {} ms)",
                self.kind, self.message, retry_after_ms
            ),
            None => write!(f, "{:?}: {}", self.kind, self.message),
        }
    }
}

impl StdError for CdfError {}

/// Returns whether an I/O error reports an exhausted symbolic-link traversal.
///
/// `std::io::ErrorKind::FilesystemLoop` remains unstable on the supported Rust
/// toolchain, so trust-boundary classifiers use the stable platform error code.
pub fn is_filesystem_loop(error: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::ELOOP)
    }
    #[cfg(windows)]
    {
        // ERROR_TOO_MANY_LINKS and ERROR_CANT_RESOLVE_FILENAME.
        matches!(error.raw_os_error(), Some(1142 | 1921))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = error;
        false
    }
}

pub fn embedded_cdf_error(error: &std::io::Error) -> Option<CdfError> {
    let mut source: Option<&(dyn StdError + 'static)> = error
        .get_ref()
        .map(|source| source as &(dyn StdError + 'static));
    while let Some(current) = source {
        if let Some(error) = current.downcast_ref::<CdfError>() {
            return Some(error.clone());
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

impl From<ArrowError> for CdfError {
    fn from(error: ArrowError) -> Self {
        Self::data(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_cdf_error_walks_nested_io_wrappers() {
        let expected = CdfError::rate_limited("nested owner", Some(125));
        let nested = std::io::Error::other(std::io::Error::other(expected.clone()));

        assert_eq!(embedded_cdf_error(&nested), Some(expected));
    }
}
