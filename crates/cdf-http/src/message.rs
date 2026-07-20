use std::{collections::BTreeMap, fmt, sync::Arc};

use bytes::Bytes;
use cdf_kernel::{CdfError, Result};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest,
    reserve,
};

use crate::{redaction::Redactor, support::set_header};

pub type HeaderMap = BTreeMap<String, String>;

pub trait HttpCancellation: Send + Sync {
    fn check(&self) -> Result<()>;
}

impl<F> HttpCancellation for F
where
    F: Fn() -> Result<()> + Send + Sync,
{
    fn check(&self) -> Result<()> {
        self()
    }
}

#[derive(Clone)]
pub struct HttpResponseBudget {
    maximum_body_bytes: u64,
    memory: Arc<dyn MemoryCoordinator>,
    cancellation: Arc<dyn HttpCancellation>,
}

impl HttpResponseBudget {
    pub fn new(
        maximum_body_bytes: u64,
        memory: Arc<dyn MemoryCoordinator>,
        cancellation: Arc<dyn HttpCancellation>,
    ) -> Result<Self> {
        if maximum_body_bytes == 0 {
            return Err(CdfError::contract(
                "HTTP response budget requires a nonzero body limit",
            ));
        }
        Ok(Self {
            maximum_body_bytes,
            memory,
            cancellation,
        })
    }

    pub fn maximum_body_bytes(&self) -> u64 {
        self.maximum_body_bytes
    }

    pub fn check_cancellation(&self) -> Result<()> {
        self.cancellation.check()
    }

    pub async fn reserve_body(&self, bytes: u64) -> Result<Option<MemoryLease>> {
        self.check_cancellation()?;
        if bytes > self.maximum_body_bytes {
            return Err(CdfError::data(format!(
                "HTTP response declares {bytes} body bytes above its {}-byte limit",
                self.maximum_body_bytes
            )));
        }
        if bytes == 0 {
            return Ok(None);
        }
        reserve(
            Arc::clone(&self.memory),
            ReservationRequest::new(
                ConsumerKey::new("http-response-body", MemoryClass::Source)?,
                bytes,
            )?,
        )
        .await
        .map(Some)
    }

    pub async fn account_body(&self, body: impl Into<Bytes>) -> Result<Option<AccountedBytes>> {
        let body = body.into();
        let bytes = u64::try_from(body.len())
            .map_err(|_| CdfError::data("HTTP response body length exceeds u64"))?;
        let Some(lease) = self.reserve_body(bytes).await? else {
            return Ok(None);
        };
        self.account_reserved_body(body, lease).map(Some)
    }

    pub fn account_reserved_body(&self, body: Bytes, lease: MemoryLease) -> Result<AccountedBytes> {
        self.check_cancellation()?;
        let bytes = u64::try_from(body.len())
            .map_err(|_| CdfError::data("HTTP response body length exceeds u64"))?;
        if bytes == 0 || bytes > self.maximum_body_bytes {
            return Err(CdfError::data(format!(
                "HTTP response contains {bytes} body bytes outside its 1..={}-byte limit",
                self.maximum_body_bytes
            )));
        }
        AccountedBytes::new_conservative(body, lease)
    }
}

impl fmt::Debug for HttpResponseBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpResponseBudget")
            .field("maximum_body_bytes", &self.maximum_body_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: HeaderMap,
    body: Option<Bytes>,
}

impl HttpRequest {
    pub fn new(method: HttpMethod, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            headers: HeaderMap::new(),
            body: None,
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        set_header(&mut self.headers, name, value);
        self
    }

    pub fn with_body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }

    pub fn body(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redactor = Redactor::default();
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &redactor.redact_url(&self.url))
            .field("headers", &redactor.redact_headers(&self.headers))
            .field("body_bytes", &self.body.as_ref().map(Bytes::len))
            .finish()
    }
}

#[derive(Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub page: ResponsePage,
    body: Option<AccountedBytes>,
}

impl HttpResponse {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            page: ResponsePage::default(),
            body: None,
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        set_header(&mut self.headers, name, value);
        self
    }

    pub fn with_body(mut self, body: Option<AccountedBytes>) -> Self {
        self.body = body;
        self
    }

    pub fn body(&self) -> Option<&[u8]> {
        self.body.as_ref().map(AccountedBytes::payload)
    }

    pub fn accounted_body(&self) -> Option<&AccountedBytes> {
        self.body.as_ref()
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

impl PartialEq for HttpResponse {
    fn eq(&self, other: &Self) -> bool {
        self.status == other.status
            && self.headers == other.headers
            && self.page == other.page
            && self.body() == other.body()
    }
}

impl Eq for HttpResponse {}

impl fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redactor = Redactor::default();
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("headers", &redactor.redact_headers(&self.headers))
            .field("body_len", &self.body().map_or(0, <[u8]>::len))
            .field("page", &self.page)
            .finish()
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct ResponsePage {
    pub fields: BTreeMap<String, String>,
    pub item_count: usize,
}

impl fmt::Debug for ResponsePage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponsePage")
            .field("fields", &self.fields)
            .field("item_count", &self.item_count)
            .finish()
    }
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
