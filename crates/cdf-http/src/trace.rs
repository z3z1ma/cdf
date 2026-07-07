use cdf_kernel::ErrorKind;

use crate::{
    message::{HeaderMap, HttpRequest, HttpResponse},
    redaction::Redactor,
    retry::RetryBudget,
};

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
