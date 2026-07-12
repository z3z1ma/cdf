use cdf_declarative::{HttpFileRequest, HttpFileResponse, HttpFileTransport};
use cdf_http::{HeaderMap, HttpMethod, HttpRequest, HttpResponse, HttpTransport};
use cdf_kernel::{CdfError, Result};
use std::{fs::File, io::Write, path::Path};

pub(crate) struct ReqwestHttpTransport {
    client: reqwest::blocking::Client,
}

impl ReqwestHttpTransport {
    pub(crate) fn new() -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .build()
            .map_err(|error| CdfError::internal(format!("build REST HTTP client: {error}")))?;
        Ok(Self { client })
    }
}

impl HttpTransport for ReqwestHttpTransport {
    fn send(&mut self, request: HttpRequest) -> Result<HttpResponse> {
        let raw = self.send_raw(&request.method, &request.url, &request.headers, "REST")?;
        let mut response = HttpResponse::new(raw.status).with_body(raw.body);
        for (name, value) in raw.headers {
            response = response.with_header(name, value);
        }
        Ok(response)
    }
}

impl HttpFileTransport for ReqwestHttpTransport {
    fn send(&mut self, request: HttpFileRequest) -> Result<HttpFileResponse> {
        let raw = self.send_raw(
            &request.method,
            &request.url,
            &request.headers,
            "file transport",
        )?;
        let mut response = HttpFileResponse::new(raw.status).with_body(raw.body);
        for (name, value) in raw.headers {
            response = response.with_header(name, value);
        }
        Ok(response)
    }

    fn download(
        &mut self,
        request: HttpFileRequest,
        destination: &Path,
    ) -> Result<(HttpFileResponse, u64)> {
        let method = reqwest_method(&request.method)?;
        let mut builder = self.client.request(method, &request.url);
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        let mut response = builder.send().map_err(|error| {
            CdfError::transient(format!("send file transport HTTP request: {error}"))
        })?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect::<Vec<_>>();
        if !(200..=399).contains(&status) {
            let mut metadata = HttpFileResponse::new(status);
            for (name, value) in headers {
                metadata = metadata.with_header(name, value);
            }
            return Ok((metadata, 0));
        }
        let mut file = File::create(destination).map_err(|error| {
            CdfError::data(format!(
                "create HTTP file spool {}: {error}",
                destination.display()
            ))
        })?;
        let bytes_written = std::io::copy(&mut response, &mut file).map_err(|error| {
            CdfError::transient(format!("stream HTTP response into spool: {error}"))
        })?;
        file.flush().map_err(|error| {
            CdfError::data(format!(
                "flush HTTP file spool {}: {error}",
                destination.display()
            ))
        })?;
        let mut metadata = HttpFileResponse::new(status);
        for (name, value) in headers {
            metadata = metadata.with_header(name, value);
        }
        Ok((metadata, bytes_written))
    }
}

impl ReqwestHttpTransport {
    fn send_raw(
        &mut self,
        method: &HttpMethod,
        url: &str,
        headers: &HeaderMap,
        context: &str,
    ) -> Result<RawHttpResponse> {
        let method = reqwest_method(method)?;
        let mut builder = self.client.request(method, url);
        for (name, value) in headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        let response = builder.send().map_err(|error| {
            CdfError::transient(format!("send {context} HTTP request: {error}"))
        })?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect::<Vec<_>>();
        let body = response.bytes().map_err(|error| {
            CdfError::transient(format!("read {context} HTTP response body: {error}"))
        })?;
        Ok(RawHttpResponse {
            status,
            headers,
            body: body.to_vec(),
        })
    }
}

struct RawHttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn reqwest_method(method: &HttpMethod) -> Result<reqwest::Method> {
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
