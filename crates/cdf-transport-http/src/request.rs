//! Reqwest request/response wire conversion.

use cdf_http::HttpMethod;
use cdf_kernel::{CdfError, Result};
use cdf_memory::AccountedBytes;

pub(crate) struct RawHttpResponse {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Option<AccountedBytes>,
}

pub(crate) fn response_headers(headers: &reqwest::header::HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_owned(), value.to_owned()))
        })
        .collect()
}

pub(crate) fn reqwest_method(method: &HttpMethod) -> Result<reqwest::Method> {
    match method {
        HttpMethod::Get => Ok(reqwest::Method::GET),
        HttpMethod::Head => Ok(reqwest::Method::HEAD),
        HttpMethod::Options => Ok(reqwest::Method::OPTIONS),
        HttpMethod::Trace => Ok(reqwest::Method::TRACE),
        HttpMethod::Post => Ok(reqwest::Method::POST),
        HttpMethod::Put => Ok(reqwest::Method::PUT),
        HttpMethod::Patch => Ok(reqwest::Method::PATCH),
        HttpMethod::Delete => Ok(reqwest::Method::DELETE),
        HttpMethod::Other(value) => reqwest::Method::from_bytes(value.as_bytes())
            .map_err(|error| CdfError::contract(format!("invalid HTTP method `{value}`: {error}"))),
    }
}
