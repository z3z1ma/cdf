use crate::{
    message::HttpResponse,
    support::{header_value, parse_u64, retry_after_ms},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RateLimitPolicy {
    pub requests_per_minute: Option<u32>,
    pub quota_headers: Vec<QuotaHeaderPolicy>,
}

impl RateLimitPolicy {
    pub fn unrestricted() -> Self {
        Self {
            requests_per_minute: None,
            quota_headers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuotaHeaderPolicy {
    pub remaining_header: String,
    pub reset_header: String,
    pub reset: ResetHeaderSemantics,
}

impl QuotaHeaderPolicy {
    pub fn remaining_until_reset(
        remaining_header: impl Into<String>,
        reset_header: impl Into<String>,
        reset: ResetHeaderSemantics,
    ) -> Self {
        Self {
            remaining_header: remaining_header.into(),
            reset_header: reset_header.into(),
            reset,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetHeaderSemantics {
    DelaySeconds,
    EpochSeconds,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RateLimitDecision {
    pub allowed: bool,
    pub wait_ms: u64,
    pub reason: Option<String>,
}

impl RateLimitDecision {
    pub fn allowed() -> Self {
        Self {
            allowed: true,
            wait_ms: 0,
            reason: None,
        }
    }

    pub fn wait(wait_ms: u64, reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            wait_ms,
            reason: Some(reason.into()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RateLimiter {
    policy: RateLimitPolicy,
    capacity: f64,
    tokens: f64,
    last_refill_ms: u64,
    blocked_until_ms: u64,
}

impl RateLimiter {
    pub fn new(policy: RateLimitPolicy, now_ms: u64) -> Self {
        let capacity = policy.requests_per_minute.unwrap_or(u32::MAX) as f64;
        Self {
            policy,
            capacity,
            tokens: capacity,
            last_refill_ms: now_ms,
            blocked_until_ms: now_ms,
        }
    }

    pub fn before_request(&mut self, now_ms: u64) -> RateLimitDecision {
        if now_ms < self.blocked_until_ms {
            return RateLimitDecision::wait(self.blocked_until_ms - now_ms, "server quota window");
        }

        let Some(requests_per_minute) = self.policy.requests_per_minute else {
            return RateLimitDecision::allowed();
        };

        let elapsed_ms = now_ms.saturating_sub(self.last_refill_ms);
        let refill = elapsed_ms as f64 * requests_per_minute as f64 / 60_000.0;
        self.tokens = self.capacity.min(self.tokens + refill);
        self.last_refill_ms = now_ms;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            RateLimitDecision::allowed()
        } else {
            let missing = 1.0 - self.tokens;
            let wait_ms = (missing * 60_000.0 / requests_per_minute as f64).ceil() as u64;
            RateLimitDecision::wait(wait_ms.max(1), "token bucket")
        }
    }

    pub fn observe_response(
        &mut self,
        response: &HttpResponse,
        monotonic_now_ms: u64,
        unix_now_ms: u64,
    ) -> RateLimitDecision {
        if let Some(wait_ms) = retry_after_ms(response) {
            self.blocked_until_ms = monotonic_now_ms.saturating_add(wait_ms);
            return RateLimitDecision::wait(wait_ms, "Retry-After");
        }

        for quota in &self.policy.quota_headers {
            let Some(remaining) =
                header_value(&response.headers, &quota.remaining_header).and_then(parse_u64)
            else {
                continue;
            };
            if remaining > 0 {
                continue;
            }

            let wait_ms = header_value(&response.headers, &quota.reset_header)
                .and_then(parse_u64)
                .map(|value| match quota.reset {
                    ResetHeaderSemantics::DelaySeconds => value.saturating_mul(1_000),
                    ResetHeaderSemantics::EpochSeconds => {
                        value.saturating_mul(1_000).saturating_sub(unix_now_ms)
                    }
                })
                .unwrap_or(1_000);
            self.blocked_until_ms = monotonic_now_ms.saturating_add(wait_ms);
            return RateLimitDecision::wait(wait_ms, quota.reset_header.clone());
        }

        if response.status == 429 {
            self.blocked_until_ms = monotonic_now_ms.saturating_add(1_000);
            return RateLimitDecision::wait(1_000, "429");
        }

        RateLimitDecision::allowed()
    }
}
