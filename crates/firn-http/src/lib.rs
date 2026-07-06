#![doc = "HTTP resource toolkit boundary for firn."]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, str,
};

use firn_kernel::{ErrorKind, FirnError, Result};

pub type HeaderMap = BTreeMap<String, String>;

#[derive(Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: HeaderMap,
}

impl HttpRequest {
    pub fn new(method: HttpMethod, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            headers: HeaderMap::new(),
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        set_header(&mut self.headers, name, value);
        self
    }
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redactor = Redactor::default();
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &redactor.redact_url(&self.url))
            .field("headers", &redactor.redact_headers(&self.headers))
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub page: ResponsePage,
}

impl HttpResponse {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            page: ResponsePage::default(),
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        set_header(&mut self.headers, name, value);
        self
    }

    pub fn with_field(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.page.fields.insert(name.into(), value.into());
        self
    }

    pub fn with_item_count(mut self, item_count: usize) -> Self {
        self.page.item_count = item_count;
        self
    }
}

impl fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redactor = Redactor::default();
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("headers", &redactor.redact_headers(&self.headers))
            .field("page", &self.page)
            .finish()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResponsePage {
    pub fields: BTreeMap<String, String>,
    pub item_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Head,
    Options,
    Trace,
    Post,
    Put,
    Patch,
    Delete,
    Other(String),
}

impl HttpMethod {
    pub fn is_safe_read(&self) -> bool {
        matches!(self, Self::Get | Self::Head | Self::Options | Self::Trace)
    }
}

impl fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
            Self::Trace => "TRACE",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
            Self::Other(value) => value,
        };
        f.write_str(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaginationKind {
    Cursor,
    Page,
    Offset,
    LinkHeader,
    NextToken,
}

impl fmt::Display for PaginationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Cursor => "cursor",
            Self::Page => "page",
            Self::Offset => "offset",
            Self::LinkHeader => "link_header",
            Self::NextToken => "next_token",
        };
        f.write_str(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaginationConfig {
    Cursor {
        query_param: String,
        response_field: String,
        initial: Option<String>,
    },
    Page {
        query_param: String,
        start_page: u64,
    },
    Offset {
        offset_param: String,
        limit_param: String,
        start_offset: u64,
        limit: u64,
    },
    LinkHeader,
    NextToken {
        query_param: String,
        response_field: String,
        initial: Option<String>,
    },
}

impl PaginationConfig {
    pub fn kind(&self) -> PaginationKind {
        match self {
            Self::Cursor { .. } => PaginationKind::Cursor,
            Self::Page { .. } => PaginationKind::Page,
            Self::Offset { .. } => PaginationKind::Offset,
            Self::LinkHeader => PaginationKind::LinkHeader,
            Self::NextToken { .. } => PaginationKind::NextToken,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageRequest {
    pub url: String,
    pub kind: PaginationKind,
    pub plan_note: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Paginator {
    config: PaginationConfig,
    current_page: u64,
    current_offset: u64,
    last_marker: Option<String>,
}

impl Paginator {
    pub fn new(config: PaginationConfig) -> Self {
        let current_page = match &config {
            PaginationConfig::Page { start_page, .. } => *start_page,
            _ => 0,
        };
        let current_offset = match &config {
            PaginationConfig::Offset { start_offset, .. } => *start_offset,
            _ => 0,
        };
        Self {
            config,
            current_page,
            current_offset,
            last_marker: None,
        }
    }

    pub fn first_request(&self, base_url: &str) -> PageRequest {
        let url = match &self.config {
            PaginationConfig::Cursor {
                query_param,
                initial: Some(initial),
                ..
            }
            | PaginationConfig::NextToken {
                query_param,
                initial: Some(initial),
                ..
            } => set_query_param(base_url, query_param, initial),
            PaginationConfig::Page {
                query_param,
                start_page,
            } => set_query_param(base_url, query_param, &start_page.to_string()),
            PaginationConfig::Offset {
                offset_param,
                limit_param,
                start_offset,
                limit,
            } => {
                let url = set_query_param(base_url, offset_param, &start_offset.to_string());
                set_query_param(&url, limit_param, &limit.to_string())
            }
            PaginationConfig::Cursor { .. }
            | PaginationConfig::NextToken { .. }
            | PaginationConfig::LinkHeader => base_url.to_owned(),
        };
        PageRequest {
            url,
            kind: self.config.kind(),
            plan_note: format!("pagination={}", self.config.kind()),
        }
    }

    pub fn next_request(
        &mut self,
        current_url: &str,
        response: &HttpResponse,
    ) -> Option<PageRequest> {
        let next_url = match &self.config {
            PaginationConfig::Cursor {
                query_param,
                response_field,
                ..
            }
            | PaginationConfig::NextToken {
                query_param,
                response_field,
                ..
            } => {
                let marker = response.page.fields.get(response_field)?.trim();
                if marker.is_empty() || self.last_marker.as_deref() == Some(marker) {
                    return None;
                }
                self.last_marker = Some(marker.to_owned());
                set_query_param(current_url, query_param, marker)
            }
            PaginationConfig::Page { query_param, .. } => {
                if response.page.item_count == 0 {
                    return None;
                }
                self.current_page = self.current_page.saturating_add(1);
                set_query_param(current_url, query_param, &self.current_page.to_string())
            }
            PaginationConfig::Offset {
                offset_param,
                limit,
                ..
            } => {
                if response.page.item_count == 0 || response.page.item_count < *limit as usize {
                    return None;
                }
                self.current_offset = self.current_offset.saturating_add(*limit);
                set_query_param(current_url, offset_param, &self.current_offset.to_string())
            }
            PaginationConfig::LinkHeader => {
                parse_next_link(header_value(&response.headers, "link")?)?
            }
        };

        Some(PageRequest {
            url: next_url,
            kind: self.config.kind(),
            plan_note: format!("pagination={}", self.config.kind()),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoDetectionResult {
    pub kind: Option<PaginationKind>,
    pub evidence: Vec<String>,
}

impl AutoDetectionResult {
    pub fn plan_summary(&self) -> String {
        let kind = self
            .kind
            .map(|kind| kind.to_string())
            .unwrap_or_else(|| "none".to_owned());
        format!("pagination={kind}; evidence={}", self.evidence.join(", "))
    }
}

pub fn detect_pagination(response: &HttpResponse) -> AutoDetectionResult {
    if response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("link"))
        .and_then(|(_, value)| parse_next_link(value))
        .is_some()
    {
        return AutoDetectionResult {
            kind: Some(PaginationKind::LinkHeader),
            evidence: vec!["Link header contains rel=next".to_owned()],
        };
    }

    let field_specs = [
        ("next_token", PaginationKind::NextToken),
        ("nextToken", PaginationKind::NextToken),
        ("next_cursor", PaginationKind::Cursor),
        ("nextCursor", PaginationKind::Cursor),
        ("next_page", PaginationKind::Page),
        ("nextPage", PaginationKind::Page),
        ("next_offset", PaginationKind::Offset),
        ("nextOffset", PaginationKind::Offset),
    ];
    for (field, kind) in field_specs {
        if response
            .page
            .fields
            .get(field)
            .is_some_and(|value| !value.trim().is_empty())
        {
            return AutoDetectionResult {
                kind: Some(kind),
                evidence: vec![format!("response field `{field}` is present")],
            };
        }
    }

    AutoDetectionResult {
        kind: None,
        evidence: vec!["no supported pagination marker found".to_owned()],
    }
}

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

    pub fn observe_response(&mut self, response: &HttpResponse, now_ms: u64) -> RateLimitDecision {
        if let Some(wait_ms) = retry_after_ms(response) {
            self.blocked_until_ms = now_ms.saturating_add(wait_ms);
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
                        value.saturating_mul(1_000).saturating_sub(now_ms)
                    }
                })
                .unwrap_or(1_000);
            self.blocked_until_ms = now_ms.saturating_add(wait_ms);
            return RateLimitDecision::wait(wait_ms, quota.reset_header.clone());
        }

        if response.status == 429 {
            self.blocked_until_ms = now_ms.saturating_add(1_000);
            return RateLimitDecision::wait(1_000, "429");
        }

        RateLimitDecision::allowed()
    }
}

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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretUri(String);

impl SecretUri {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if !value.starts_with("secret://") {
            return Err(FirnError::contract(
                "secret reference must use the secret:// scheme",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for SecretUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue {
    bytes: Vec<u8>,
}

impl SecretValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            bytes: value.into().into_bytes(),
        }
    }

    pub fn as_str(&self) -> Result<&str> {
        str::from_utf8(&self.bytes).map_err(|_| FirnError::auth("secret value is not valid UTF-8"))
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        self.bytes.fill(0);
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl fmt::Display for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

pub trait SecretProvider {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue>;
}

pub trait AuthRefreshHook {
    fn refresh(&mut self, uri: &SecretUri, provider: &dyn SecretProvider) -> Result<SecretValue>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProviderRefreshHook;

impl AuthRefreshHook for ProviderRefreshHook {
    fn refresh(&mut self, uri: &SecretUri, provider: &dyn SecretProvider) -> Result<SecretValue> {
        provider.resolve(uri)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthScheme {
    Bearer { token_uri: SecretUri },
    Header { name: String, value_uri: SecretUri },
}

impl AuthScheme {
    fn uri(&self) -> &SecretUri {
        match self {
            Self::Bearer { token_uri } => token_uri,
            Self::Header { value_uri, .. } => value_uri,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthSession {
    scheme: AuthScheme,
    value: Option<SecretValue>,
    refreshed: bool,
}

impl AuthSession {
    pub fn new(scheme: AuthScheme) -> Self {
        Self {
            scheme,
            value: None,
            refreshed: false,
        }
    }

    pub fn apply(
        &mut self,
        provider: &dyn SecretProvider,
        request: &mut HttpRequest,
    ) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(provider.resolve(self.scheme.uri())?);
        }
        let value = self.value.as_ref().expect("secret value was just resolved");
        match &self.scheme {
            AuthScheme::Bearer { .. } => {
                set_header(
                    &mut request.headers,
                    "authorization",
                    format!("Bearer {}", value.as_str()?),
                );
            }
            AuthScheme::Header { name, .. } => {
                set_header(&mut request.headers, name, value.as_str()?);
            }
        }
        Ok(())
    }

    pub fn refresh_once(
        &mut self,
        provider: &dyn SecretProvider,
        hook: &mut dyn AuthRefreshHook,
    ) -> Result<()> {
        if self.refreshed {
            return Err(FirnError::auth(
                "auth refresh was already attempted for this session",
            ));
        }
        self.value = Some(hook.refresh(self.scheme.uri(), provider)?);
        self.refreshed = true;
        Ok(())
    }
}

impl fmt::Debug for AuthSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthSession")
            .field("scheme", &self.scheme)
            .field("value", &self.value.as_ref().map(|_| "[REDACTED]"))
            .field("refreshed", &self.refreshed)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EgressAllowlist {
    AllowAny,
    AllowHosts(BTreeSet<String>),
}

impl EgressAllowlist {
    pub fn allow_any() -> Self {
        Self::AllowAny
    }

    pub fn from_hosts(hosts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::AllowHosts(hosts.into_iter().map(normalize_host).collect())
    }

    pub fn check(&self, request: &HttpRequest) -> Result<()> {
        let host = host_from_url(&request.url)?;
        match self {
            Self::AllowAny => Ok(()),
            Self::AllowHosts(hosts) if hosts.iter().any(|allowed| host_matches(&host, allowed)) => {
                Ok(())
            }
            Self::AllowHosts(_) => Err(FirnError::auth(format!(
                "egress to host `{host}` is denied by allowlist"
            ))),
        }
    }
}

pub trait HttpTransport {
    fn send(&mut self, request: HttpRequest) -> Result<HttpResponse>;
}

pub fn send_with_policy(
    transport: &mut dyn HttpTransport,
    allowlist: &EgressAllowlist,
    request: HttpRequest,
) -> Result<HttpResponse> {
    allowlist.check(&request)?;
    transport.send(request)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEvent {
    pub method: String,
    pub url: String,
    pub headers: HeaderMap,
    pub status: Option<u16>,
    pub error_kind: Option<ErrorKind>,
    pub note: Option<String>,
}

impl TraceEvent {
    pub fn from_request(request: &HttpRequest, redactor: &Redactor) -> Self {
        Self {
            method: request.method.to_string(),
            url: redactor.redact_url(&request.url),
            headers: redactor.redact_headers(&request.headers),
            status: None,
            error_kind: None,
            note: None,
        }
    }

    pub fn from_exchange(
        request: &HttpRequest,
        response: &HttpResponse,
        redactor: &Redactor,
        note: Option<String>,
    ) -> Self {
        let error_kind = RetryBudget::classify_response(response).map(|error| error.kind);
        Self {
            method: request.method.to_string(),
            url: redactor.redact_url(&request.url),
            headers: redactor.redact_headers(&request.headers),
            status: Some(response.status),
            error_kind,
            note: note.map(|value| redactor.redact_text(&value)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Redactor {
    secrets: Vec<String>,
    sensitive_headers: BTreeSet<String>,
}

impl Default for Redactor {
    fn default() -> Self {
        Self {
            secrets: Vec::new(),
            sensitive_headers: [
                "authorization",
                "proxy-authorization",
                "x-api-key",
                "api-key",
                "cookie",
                "set-cookie",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        }
    }
}

impl Redactor {
    pub fn register_secret_value(&mut self, value: &SecretValue) -> Result<()> {
        self.register_secret(value.as_str()?)
    }

    pub fn register_secret(&mut self, value: &str) -> Result<()> {
        if value.is_empty() {
            return Ok(());
        }
        self.secrets.push(value.to_owned());
        Ok(())
    }

    pub fn redact_text(&self, value: &str) -> String {
        self.secrets
            .iter()
            .fold(value.to_owned(), |redacted, secret| {
                redacted.replace(secret, "[REDACTED]")
            })
    }

    pub fn redact_headers(&self, headers: &HeaderMap) -> HeaderMap {
        headers
            .iter()
            .map(|(name, value)| {
                let canonical = canonical_header_name(name);
                let value = if self.sensitive_headers.contains(&canonical)
                    || is_sensitive_name(&canonical)
                {
                    "[REDACTED]".to_owned()
                } else {
                    self.redact_text(value)
                };
                (name.clone(), value)
            })
            .collect()
    }

    pub fn redact_url(&self, url: &str) -> String {
        let Some((base, query_and_fragment)) = url.split_once('?') else {
            return self.redact_text(url);
        };
        let (query, fragment) = query_and_fragment
            .split_once('#')
            .map_or((query_and_fragment, ""), |(query, fragment)| {
                (query, fragment)
            });
        let redacted_query = query
            .split('&')
            .filter(|part| !part.is_empty())
            .map(|part| {
                let (name, value) = part.split_once('=').unwrap_or((part, ""));
                let redacted_value = if is_sensitive_name(name) {
                    "[REDACTED]".to_owned()
                } else {
                    self.redact_text(value)
                };
                format!("{name}={redacted_value}")
            })
            .collect::<Vec<_>>()
            .join("&");
        let mut redacted = format!("{}?{}", self.redact_text(base), redacted_query);
        if !fragment.is_empty() {
            redacted.push('#');
            redacted.push_str(&self.redact_text(fragment));
        }
        redacted
    }
}

fn set_header(headers: &mut HeaderMap, name: impl Into<String>, value: impl Into<String>) {
    headers.insert(canonical_header_name(&name.into()), value.into());
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn canonical_header_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn parse_next_link(link_header: &str) -> Option<String> {
    link_header.split(',').find_map(|part| {
        let part = part.trim();
        let rel_next = part.split(';').skip(1).any(|param| {
            param.trim().eq_ignore_ascii_case("rel=\"next\"")
                || param.trim().eq_ignore_ascii_case("rel=next")
        });
        if !rel_next {
            return None;
        }
        let start = part.find('<')?;
        let end = part[start + 1..].find('>')?;
        Some(part[start + 1..start + 1 + end].to_owned())
    })
}

fn set_query_param(url: &str, name: &str, value: &str) -> String {
    let (without_fragment, fragment) = url
        .split_once('#')
        .map_or((url, ""), |(url, fragment)| (url, fragment));
    let (base, query) = without_fragment
        .split_once('?')
        .map_or((without_fragment, ""), |(base, query)| (base, query));
    let mut params = query
        .split('&')
        .filter(|part| !part.is_empty())
        .filter(|part| {
            let candidate = part
                .split_once('=')
                .map_or(*part, |(candidate, _)| candidate);
            candidate != name
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    params.push(format!("{name}={value}"));
    let mut next_url = format!("{base}?{}", params.join("&"));
    if !fragment.is_empty() {
        next_url.push('#');
        next_url.push_str(fragment);
    }
    next_url
}

fn retry_after_ms(response: &HttpResponse) -> Option<u64> {
    header_value(&response.headers, "retry-after")
        .and_then(parse_u64)
        .map(|seconds| seconds.saturating_mul(1_000))
}

fn parse_u64(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

fn is_retryable_kind(kind: &ErrorKind) -> bool {
    matches!(kind, ErrorKind::Transient | ErrorKind::RateLimited)
}

fn retry_exhausted_error(error: &FirnError, reason: &str) -> FirnError {
    let message = format!("{reason}: {}", error.message);
    match error.kind {
        ErrorKind::RateLimited => FirnError::rate_limited(message, error.retry_after_ms),
        _ => FirnError::new(error.kind.clone(), message),
    }
}

fn normalize_host(host: impl Into<String>) -> String {
    let value = host.into();
    let host = value
        .trim()
        .trim_end_matches('.')
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let host = host.split('/').next().unwrap_or(host);
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.split_once(']'))
        .map_or(host.split(':').next().unwrap_or(host), |(inside, _)| inside);
    host.to_ascii_lowercase()
}

fn host_from_url(url: &str) -> Result<String> {
    let (_, rest) = url
        .split_once("://")
        .ok_or_else(|| FirnError::contract("HTTP request URL must include a scheme"))?;
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .ok_or_else(|| FirnError::contract("HTTP request URL must include a host"))?;
    let host_port = authority.rsplit('@').next().unwrap_or(authority);
    let host = if let Some(stripped) = host_port.strip_prefix('[') {
        stripped
            .split_once(']')
            .map(|(inside, _)| inside)
            .ok_or_else(|| FirnError::contract("IPv6 host is missing closing bracket"))?
    } else {
        host_port.split(':').next().unwrap_or(host_port)
    };
    if host.trim().is_empty() {
        return Err(FirnError::contract("HTTP request URL must include a host"));
    }
    Ok(normalize_host(host))
}

fn host_matches(host: &str, allowed: &str) -> bool {
    host == allowed
        || allowed
            .strip_prefix("*.")
            .is_some_and(|suffix| host.ends_with(&format!(".{suffix}")))
}

fn is_sensitive_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "authorization",
        "api_key",
        "apikey",
    ]
    .iter()
    .any(|needle| name.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paginators_cover_cursor_page_offset_link_and_next_token() {
        let mut cursor = Paginator::new(PaginationConfig::Cursor {
            query_param: "cursor".to_owned(),
            response_field: "next_cursor".to_owned(),
            initial: None,
        });
        let current = cursor.first_request("https://api.example.com/items");
        assert_eq!(current.url, "https://api.example.com/items");
        let response = HttpResponse::new(200).with_field("next_cursor", "abc");
        assert_eq!(
            cursor.next_request(&current.url, &response).unwrap().url,
            "https://api.example.com/items?cursor=abc"
        );

        let mut pages = Paginator::new(PaginationConfig::Page {
            query_param: "page".to_owned(),
            start_page: 1,
        });
        let first = pages.first_request("https://api.example.com/items");
        assert_eq!(first.url, "https://api.example.com/items?page=1");
        let response = HttpResponse::new(200).with_item_count(10);
        assert_eq!(
            pages.next_request(&first.url, &response).unwrap().url,
            "https://api.example.com/items?page=2"
        );

        let mut offsets = Paginator::new(PaginationConfig::Offset {
            offset_param: "offset".to_owned(),
            limit_param: "limit".to_owned(),
            start_offset: 0,
            limit: 50,
        });
        let first = offsets.first_request("https://api.example.com/items");
        assert_eq!(first.url, "https://api.example.com/items?offset=0&limit=50");
        let response = HttpResponse::new(200).with_item_count(50);
        assert_eq!(
            offsets.next_request(&first.url, &response).unwrap().url,
            "https://api.example.com/items?limit=50&offset=50"
        );

        let mut links = Paginator::new(PaginationConfig::LinkHeader);
        let response = HttpResponse::new(200).with_header(
            "Link",
            r#"<https://api.example.com/items?page=2>; rel="next""#,
        );
        assert_eq!(
            links
                .next_request("https://api.example.com/items?page=1", &response)
                .unwrap()
                .url,
            "https://api.example.com/items?page=2"
        );

        let mut tokens = Paginator::new(PaginationConfig::NextToken {
            query_param: "page_token".to_owned(),
            response_field: "next_token".to_owned(),
            initial: None,
        });
        let response = HttpResponse::new(200).with_field("next_token", "tok-2");
        assert_eq!(
            tokens
                .next_request("https://api.example.com/items", &response)
                .unwrap()
                .url,
            "https://api.example.com/items?page_token=tok-2"
        );
    }

    #[test]
    fn auto_detection_is_plan_visible() {
        let response = HttpResponse::new(200).with_field("next_token", "tok-2");
        let detected = detect_pagination(&response);
        assert_eq!(detected.kind, Some(PaginationKind::NextToken));
        assert_eq!(
            detected.plan_summary(),
            "pagination=next_token; evidence=response field `next_token` is present"
        );
    }

    #[test]
    fn rate_limiter_respects_retry_after_and_quota_headers() {
        let mut limiter = RateLimiter::new(
            RateLimitPolicy {
                requests_per_minute: Some(60),
                quota_headers: vec![QuotaHeaderPolicy::remaining_until_reset(
                    "X-RateLimit-Remaining",
                    "X-RateLimit-Reset",
                    ResetHeaderSemantics::DelaySeconds,
                )],
            },
            0,
        );

        assert!(limiter.before_request(0).allowed);
        let retry_after = HttpResponse::new(429).with_header("Retry-After", "2");
        let decision = limiter.observe_response(&retry_after, 10);
        assert_eq!(decision.wait_ms, 2_000);
        assert!(!limiter.before_request(1_000).allowed);

        let quota = HttpResponse::new(200)
            .with_header("X-RateLimit-Remaining", "0")
            .with_header("X-RateLimit-Reset", "3");
        let decision = limiter.observe_response(&quota, 5_000);
        assert_eq!(decision.wait_ms, 3_000);
    }

    #[test]
    fn retry_budget_maps_taxonomy_and_retries_only_safe_units() {
        let response = HttpResponse::new(500);
        let error = RetryBudget::classify_response(&response).unwrap();
        assert_eq!(error.kind, ErrorKind::Transient);

        let mut budget = RetryBudget::new(RetryPolicy {
            max_attempts: 1,
            budget_ms: 1_000,
            base_delay_ms: 100,
            max_delay_ms: 500,
        });
        let decision = budget.next_retry(
            &error,
            &RetryUnit::Request {
                method: HttpMethod::Get,
                idempotency_key: false,
            },
        );
        assert!(matches!(decision, RetryDecision::Retry { .. }));
        let exhausted = budget.next_retry(
            &error,
            &RetryUnit::Request {
                method: HttpMethod::Get,
                idempotency_key: false,
            },
        );
        assert!(matches!(
            exhausted,
            RetryDecision::GiveUp { error }
                if error.kind == ErrorKind::Transient
                    && error.message.contains("attempt budget exhausted")
        ));

        let mut budget = RetryBudget::new(RetryPolicy::default());
        let decision = budget.next_retry(
            &error,
            &RetryUnit::Request {
                method: HttpMethod::Post,
                idempotency_key: false,
            },
        );
        assert!(matches!(decision, RetryDecision::GiveUp { .. }));

        let rate_limited = HttpResponse::new(429).with_header("Retry-After", "4");
        let error = RetryBudget::classify_response(&rate_limited).unwrap();
        assert_eq!(error.kind, ErrorKind::RateLimited);
        assert_eq!(error.retry_after_ms, Some(4_000));
    }

    #[test]
    fn auth_refresh_hooks_and_traces_do_not_format_secrets() {
        struct Provider {
            value: String,
        }

        impl SecretProvider for Provider {
            fn resolve(&self, _uri: &SecretUri) -> Result<SecretValue> {
                Ok(SecretValue::new(self.value.clone()))
            }
        }

        let uri = SecretUri::new("secret://env/API_TOKEN").unwrap();
        let provider = Provider {
            value: "super-secret-token".to_owned(),
        };
        let mut session = AuthSession::new(AuthScheme::Bearer {
            token_uri: uri.clone(),
        });
        let mut request = HttpRequest::new(
            HttpMethod::Get,
            "https://api.example.com/items?token=super-secret-token",
        );
        session.apply(&provider, &mut request).unwrap();
        assert_eq!(
            request.headers.get("authorization").unwrap(),
            "Bearer super-secret-token"
        );

        let mut redactor = Redactor::default();
        redactor.register_secret("super-secret-token").unwrap();
        let trace = TraceEvent::from_request(&request, &redactor);
        let trace_text = format!("{trace:?}");
        assert!(!trace_text.contains("super-secret-token"));
        assert!(trace_text.contains("[REDACTED]"));
        assert!(!format!("{request:?}").contains("super-secret-token"));
        assert!(!format!("{session:?}").contains("super-secret-token"));

        let refreshed_provider = Provider {
            value: "rotated-secret-token".to_owned(),
        };
        let mut hook = ProviderRefreshHook;
        session
            .refresh_once(&refreshed_provider, &mut hook)
            .unwrap();
        let mut refreshed = HttpRequest::new(HttpMethod::Get, "https://api.example.com/items");
        session.apply(&refreshed_provider, &mut refreshed).unwrap();
        assert_eq!(
            refreshed.headers.get("authorization").unwrap(),
            "Bearer rotated-secret-token"
        );
        assert!(
            session
                .refresh_once(&refreshed_provider, &mut hook)
                .is_err()
        );
    }

    #[test]
    fn allowlist_denies_before_transport_send() {
        #[derive(Default)]
        struct CountingTransport {
            sends: usize,
        }

        impl HttpTransport for CountingTransport {
            fn send(&mut self, _request: HttpRequest) -> Result<HttpResponse> {
                self.sends += 1;
                Ok(HttpResponse::new(200))
            }
        }

        let allowlist = EgressAllowlist::from_hosts(["api.example.com"]);
        let mut transport = CountingTransport::default();
        let denied = send_with_policy(
            &mut transport,
            &allowlist,
            HttpRequest::new(HttpMethod::Get, "https://evil.example.net/items"),
        )
        .unwrap_err();
        assert_eq!(denied.kind, ErrorKind::Auth);
        assert_eq!(transport.sends, 0);

        send_with_policy(
            &mut transport,
            &allowlist,
            HttpRequest::new(HttpMethod::Get, "https://api.example.com/items"),
        )
        .unwrap();
        assert_eq!(transport.sends, 1);
    }
}
