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
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdfError {
    pub kind: ErrorKind,
    pub message: String,
    pub retry_after_ms: Option<u64>,
}

impl CdfError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
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
            retry_after_ms,
        }
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

impl From<ArrowError> for CdfError {
    fn from(error: ArrowError) -> Self {
        Self::data(error.to_string())
    }
}
