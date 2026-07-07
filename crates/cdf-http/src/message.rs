use std::{collections::BTreeMap, fmt};

use crate::{redaction::Redactor, support::set_header};

pub type HeaderMap = BTreeMap<String, String>;
const RESPONSE_BODY_FIELD: &str = "__cdf_response_body";

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

    pub fn with_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.page.fields.insert(
            RESPONSE_BODY_FIELD.to_owned(),
            String::from_utf8_lossy(&body.into()).into_owned(),
        );
        self
    }

    pub fn body(&self) -> Option<&[u8]> {
        self.page
            .fields
            .get(RESPONSE_BODY_FIELD)
            .map(String::as_bytes)
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
        let mut fields = self.fields.clone();
        if let Some(body) = fields.get_mut(RESPONSE_BODY_FIELD) {
            *body = format!("<{} bytes>", body.len());
        }
        f.debug_struct("ResponsePage")
            .field("fields", &fields)
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
