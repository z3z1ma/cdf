use firn_kernel::{ErrorKind, FirnError};

use crate::{
    message::{HttpMethod, HttpResponse},
    redaction::Redactor,
    support::{is_retryable_kind, retry_after_ms, retry_exhausted_error},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub budget_ms: u64,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            budget_ms: 30_000,
            base_delay_ms: 100,
            max_delay_ms: 5_000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetryUnit {
    Request {
        method: HttpMethod,
        idempotency_key: bool,
    },
    Partition {
        replayable: bool,
    },
    Run,
}

impl RetryUnit {
    pub fn is_retry_safe(&self) -> bool {
        match self {
            Self::Request {
                method,
                idempotency_key,
            } => method.is_safe_read() || *idempotency_key,
            Self::Partition { replayable } => *replayable,
            Self::Run => true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetryDecision {
    Retry { delay_ms: u64 },
    GiveUp { error: FirnError },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryBudget {
    policy: RetryPolicy,
    attempts: u32,
    spent_ms: u64,
}

impl RetryBudget {
    pub fn new(policy: RetryPolicy) -> Self {
        Self {
            policy,
            attempts: 0,
            spent_ms: 0,
        }
    }

    pub fn classify_response(response: &HttpResponse) -> Option<FirnError> {
        match response.status {
            200..=399 => None,
            401 | 403 => Some(FirnError::auth(format!(
                "HTTP {} requires authentication refresh or credential review",
                response.status
            ))),
            408 | 500..=599 => Some(FirnError::transient(format!(
                "HTTP {} from upstream",
                response.status
            ))),
            429 => Some(FirnError::rate_limited(
                "HTTP 429 rate limit",
                retry_after_ms(response),
            )),
            400..=499 => Some(FirnError::data(format!(
                "HTTP {} response is not retryable as a request",
                response.status
            ))),
            _ => Some(FirnError::internal(format!(
                "unexpected HTTP status {}",
                response.status
            ))),
        }
    }

    pub fn classify_transport_error(message: &str, redactor: &Redactor) -> FirnError {
        FirnError::transient(redactor.redact_text(message))
    }

    pub fn next_retry(&mut self, error: &FirnError, unit: &RetryUnit) -> RetryDecision {
        if !is_retryable_kind(&error.kind) {
            return RetryDecision::GiveUp {
                error: error.clone(),
            };
        }
        if !unit.is_retry_safe() {
            return RetryDecision::GiveUp {
                error: FirnError::new(
                    error.kind.clone(),
                    format!("not retrying unsafe unit after {}", error.kind_label()),
                ),
            };
        }
        if self.attempts >= self.policy.max_attempts {
            return RetryDecision::GiveUp {
                error: retry_exhausted_error(error, "attempt budget exhausted"),
            };
        }

        let delay_ms = error.retry_after_ms.unwrap_or_else(|| {
            let shift = self.attempts.min(16);
            let exponent = 1_u64 << shift;
            let base = self.policy.base_delay_ms.max(1);
            let jitter = ((self.attempts as u64 + 1) * 17) % base;
            base.saturating_mul(exponent)
                .saturating_add(jitter)
                .min(self.policy.max_delay_ms)
        });
        if self.spent_ms.saturating_add(delay_ms) > self.policy.budget_ms {
            return RetryDecision::GiveUp {
                error: retry_exhausted_error(error, "time budget exhausted"),
            };
        }

        self.attempts = self.attempts.saturating_add(1);
        self.spent_ms = self.spent_ms.saturating_add(delay_ms);
        RetryDecision::Retry { delay_ms }
    }
}

trait ErrorKindLabel {
    fn kind_label(&self) -> &'static str;
}

impl ErrorKindLabel for FirnError {
    fn kind_label(&self) -> &'static str {
        match self.kind {
            ErrorKind::Transient => "transient error",
            ErrorKind::RateLimited => "rate limit",
            ErrorKind::Auth => "auth error",
            ErrorKind::Contract => "contract error",
            ErrorKind::Data => "data error",
            ErrorKind::Destination => "destination error",
            ErrorKind::Internal => "internal error",
        }
    }
}
