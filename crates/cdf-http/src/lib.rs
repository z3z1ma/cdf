#![doc = "HTTP resource toolkit boundary for cdf."]

mod auth;
mod egress;
mod message;
mod pagination;
mod rate_limit;
mod redaction;
mod response;
mod support;
mod trace;

#[cfg(test)]
mod tests;

pub use auth::{
    AuthRefreshHook, AuthScheme, AuthSession, ProviderRefreshHook, SecretProvider, SecretUri,
    SecretValue,
};
pub use egress::{EgressAllowlist, HttpTransport, send_with_policy};
pub use message::{
    HeaderMap, HttpCancellation, HttpMethod, HttpRequest, HttpResponse, HttpResponseBudget,
    ResponsePage,
};
pub use pagination::{
    AutoDetectionResult, PageRequest, PaginationConfig, PaginationKind, Paginator,
    detect_pagination,
};
pub use rate_limit::{
    QuotaHeaderPolicy, RateLimitDecision, RateLimitPolicy, RateLimiter, ResetHeaderSemantics,
};
pub use redaction::Redactor;
pub use response::classify_response;
pub use trace::TraceEvent;
