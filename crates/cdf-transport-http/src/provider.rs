//! Reqwest-backed control-plane and file-transport provider.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cdf_http::{
    HeaderMap, HttpMethod, HttpRequest, HttpResponse, HttpResponseBudget, HttpTransport,
};
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_memory::MemoryCoordinator;
use cdf_object_access::{
    FileIdentityMetadata, FileTransportResource, HttpFileRequest, HttpFileResponse,
    HttpFileTransport, ResolvedHttpAuth,
};
use cdf_runtime::ByteSource;

use crate::byte_source::HttpByteSource;
use crate::errors::sanitized_reqwest_error;
use crate::policy::{FILE_READ_IDLE_TIMEOUT, FILE_RESPONSE_TIMEOUT};
use crate::request::{RawHttpResponse, reqwest_method, response_headers};
use crate::response_body::read_bounded_response_body;

#[derive(Clone)]
pub struct ReqwestHttpProvider {
    asynchronous: reqwest::Client,
    files: reqwest::Client,
    file_response_timeout: Duration,
    file_read_idle_timeout: Duration,
}

impl ReqwestHttpProvider {
    pub fn new() -> Result<Self> {
        Self::with_file_timeouts(FILE_RESPONSE_TIMEOUT, FILE_READ_IDLE_TIMEOUT)
    }

    pub(crate) fn with_file_timeouts(
        file_response_timeout: Duration,
        file_read_idle_timeout: Duration,
    ) -> Result<Self> {
        let asynchronous = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| {
                CdfError::environment(format!(
                    "build async HTTP client: {error}; verify the host TLS, resolver, and network runtime facilities"
                ))
            })?;
        let files = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .http1_only()
            .pool_max_idle_per_host(32)
            .build()
            .map_err(|error| {
                CdfError::environment(format!(
                    "build file HTTP client: {error}; verify the host TLS, resolver, and network runtime facilities"
                ))
            })?;
        Ok(Self {
            asynchronous,
            files,
            file_response_timeout,
            file_read_idle_timeout,
        })
    }
}

impl HttpTransport for ReqwestHttpProvider {
    fn send(
        &self,
        request: HttpRequest,
        budget: HttpResponseBudget,
    ) -> BoxFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let raw = self
                .send_raw(
                    &request.method,
                    &request.url,
                    &request.headers,
                    request.body(),
                    "REST",
                    &budget,
                )
                .await?;
            let mut response = HttpResponse::new(raw.status).with_body(raw.body);
            for (name, value) in raw.headers {
                response = response.with_header(name, value);
            }
            Ok(response)
        })
    }
}

impl HttpFileTransport for ReqwestHttpProvider {
    fn send_headers(
        &self,
        request: HttpFileRequest,
    ) -> BoxFuture<'static, Result<HttpFileResponse>> {
        let client = self.files.clone();
        Box::pin(async move {
            let method = reqwest_method(&request.method)?;
            let mut builder = client.request(method, &request.url);
            for (name, value) in &request.headers {
                builder = builder.header(name.as_str(), value.as_str());
            }
            let response = builder.send().await.map_err(|error| {
                CdfError::transient(format!(
                    "send file transport HTTP metadata request: {}",
                    sanitized_reqwest_error(error)
                ))
            })?;
            let mut observed = HttpFileResponse::new(response.status().as_u16());
            for (name, value) in response_headers(response.headers()) {
                observed = observed.with_header(name, value);
            }
            Ok(observed)
        })
    }

    fn open_byte_source(
        &self,
        resource: &FileTransportResource,
        expected: &FileIdentityMetadata,
        auth: Option<ResolvedHttpAuth>,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Arc<dyn ByteSource>> {
        let url = match &resource.location {
            cdf_object_access::FileTransportLocation::HttpUrl { url } => url.clone(),
            _ => {
                return Err(CdfError::contract(
                    "HTTP byte source requires an HTTP(S) file resource",
                ));
            }
        };
        resource
            .egress_allowlist
            .check(&HttpRequest::new(HttpMethod::Get, url.clone()))?;
        Ok(Arc::new(HttpByteSource::new(
            self.files.clone(),
            url,
            expected.clone(),
            auth,
            memory,
            self.file_response_timeout,
            self.file_read_idle_timeout,
        )?))
    }
}

impl ReqwestHttpProvider {
    async fn send_raw(
        &self,
        method: &HttpMethod,
        url: &str,
        headers: &HeaderMap,
        body: Option<&Bytes>,
        context: &str,
        budget: &HttpResponseBudget,
    ) -> Result<RawHttpResponse> {
        budget.check_cancellation()?;
        let method = reqwest_method(method)?;
        let mut builder = self.asynchronous.request(method, url);
        for (name, value) in headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        if let Some(body) = body {
            builder = builder.body(body.clone());
        }
        let response = builder.send().await.map_err(|error| {
            CdfError::transient(format!(
                "send {context} HTTP request: {}",
                sanitized_reqwest_error(error)
            ))
        })?;
        let status = response.status().as_u16();
        let headers = response_headers(response.headers());
        let declared_length = response.content_length();
        let body = read_bounded_response_body(response, declared_length, context, budget).await?;
        Ok(RawHttpResponse {
            status,
            headers,
            body,
        })
    }
}
