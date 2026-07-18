#![doc = "Pooled HTTP transport provider for cdf."]

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use bytes::Bytes;
use cdf_http::{
    HeaderMap, HttpMethod, HttpRequest, HttpResponse, HttpResponseBudget, HttpTransport,
};
use cdf_kernel::{BoxFuture, CdfError, ErrorKind, Result};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    ExactRangeCoalescingPolicy, GenerationStrength, REMOTE_RANGE_COALESCING_POLICY,
    RunCancellation, SequentialReadRequest,
};
use cdf_source_files::{
    FileIdentityMetadata, FileTransportResource, HttpFileRequest, HttpFileResponse,
    HttpFileTransport, ResolvedHttpAuth,
};
use futures_util::{Stream, TryStreamExt, stream};
use sha2::{Digest, Sha256};

const MINIMUM_CHUNK_BYTES: u64 = 8 * 1024;
const MAXIMUM_CHUNK_BYTES: u64 = 32 * 1024 * 1024;
const FILE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const FILE_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

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

    fn with_file_timeouts(
        file_response_timeout: Duration,
        file_read_idle_timeout: Duration,
    ) -> Result<Self> {
        let asynchronous = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| CdfError::internal(format!("build async HTTP client: {error}")))?;
        let files = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .http1_only()
            .pool_max_idle_per_host(32)
            .build()
            .map_err(|error| CdfError::internal(format!("build file HTTP client: {error}")))?;
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
            cdf_source_files::FileTransportLocation::HttpUrl { url } => url.clone(),
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
        context: &str,
        budget: &HttpResponseBudget,
    ) -> Result<RawHttpResponse> {
        budget.check_cancellation()?;
        let method = reqwest_method(method)?;
        let mut builder = self.asynchronous.request(method, url);
        for (name, value) in headers {
            builder = builder.header(name.as_str(), value.as_str());
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

async fn read_bounded_response_body(
    response: reqwest::Response,
    declared_length: Option<u64>,
    context: &str,
    budget: &HttpResponseBudget,
) -> Result<Option<AccountedBytes>> {
    let error_context = context.to_owned();
    let stream = response.bytes_stream().map_err(move |error| {
        CdfError::transient(format!(
            "read {error_context} HTTP response body: {}",
            sanitized_reqwest_error(error)
        ))
    });
    collect_bounded_response_body(Box::pin(stream), declared_length, context, budget).await
}

type BoundedBodyStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'static>>;

async fn collect_bounded_response_body(
    mut stream: BoundedBodyStream,
    declared_length: Option<u64>,
    context: &str,
    budget: &HttpResponseBudget,
) -> Result<Option<AccountedBytes>> {
    if let Some(bytes) = declared_length
        && bytes > budget.maximum_body_bytes()
    {
        return Err(CdfError::data(format!(
            "{context} HTTP response declares {bytes} body bytes above its {}-byte limit",
            budget.maximum_body_bytes()
        )));
    }
    let reservation_bytes = declared_length.unwrap_or(budget.maximum_body_bytes());
    let lease = budget.reserve_body(reservation_bytes).await?;
    let capacity = usize::try_from(reservation_bytes)
        .map_err(|_| CdfError::data("HTTP response body limit exceeds usize"))?;
    let mut body = Vec::with_capacity(capacity);
    let mut remaining = reservation_bytes;
    while let Some(chunk) = stream.try_next().await? {
        budget.check_cancellation()?;
        let chunk_bytes = u64::try_from(chunk.len())
            .map_err(|_| CdfError::data("HTTP response chunk exceeds u64"))?;
        if chunk_bytes > remaining {
            return Err(CdfError::data(format!(
                "{context} HTTP response exceeds its {}-byte body limit",
                budget.maximum_body_bytes()
            )));
        }
        body.extend_from_slice(&chunk);
        remaining -= chunk_bytes;
    }
    budget.check_cancellation()?;
    if let Some(declared) = declared_length
        && declared != body.len() as u64
    {
        return Err(CdfError::data(format!(
            "{context} HTTP response declared {declared} body bytes but transferred {}",
            body.len()
        )));
    }
    match (lease, body.is_empty()) {
        (Some(lease), false) => budget
            .account_reserved_body(Bytes::from(body), lease)
            .map(Some),
        (Some(_), true) | (None, true) => Ok(None),
        (None, false) => Err(CdfError::internal(
            "nonempty HTTP response body has no memory reservation",
        )),
    }
}

struct HttpByteSource {
    client: reqwest::Client,
    url: String,
    expected: FileIdentityMetadata,
    auth: Option<ResolvedHttpAuth>,
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    memory: Arc<dyn MemoryCoordinator>,
    file_response_timeout: Duration,
    file_read_idle_timeout: Duration,
}

impl HttpByteSource {
    fn new(
        client: reqwest::Client,
        url: String,
        expected: FileIdentityMetadata,
        auth: Option<ResolvedHttpAuth>,
        memory: Arc<dyn MemoryCoordinator>,
        file_response_timeout: Duration,
        file_read_idle_timeout: Duration,
    ) -> Result<Self> {
        let size_bytes = expected
            .size_bytes
            .ok_or_else(|| CdfError::data("HTTP byte source requires Content-Length"))?;
        let checksum = expected.sha256().map(str::to_owned);
        let generation = expected
            .etag
            .clone()
            .or_else(|| {
                expected
                    .modified
                    .as_ref()
                    .map(|modified| format!("last-modified:{modified};size:{size_bytes}"))
            })
            .or_else(|| Some(format!("unversioned-size:{size_bytes}")));
        let exact_ranges = expected.etag.is_some() && expected.exact_ranges;
        let identity = ContentIdentity {
            stable_id: expected.location.clone(),
            size_bytes: Some(size_bytes),
            generation,
            checksum: checksum.clone(),
            strength: if checksum.is_some() {
                GenerationStrength::ContentAddressed
            } else if expected.etag.is_some() {
                GenerationStrength::Strong
            } else {
                GenerationStrength::Weak
            },
        };
        identity.validate()?;
        let capabilities = ByteSourceCapabilities {
            known_length: true,
            reopenable: true,
            seekable: exact_ranges,
            exact_ranges,
            useful_range_concurrency: if exact_ranges { 16 } else { 0 },
            minimum_chunk_bytes: MINIMUM_CHUNK_BYTES,
            maximum_chunk_bytes: MAXIMUM_CHUNK_BYTES,
        };
        capabilities.validate()?;
        Ok(Self {
            client,
            url,
            expected,
            auth,
            identity,
            capabilities,
            memory,
            file_response_timeout,
            file_read_idle_timeout,
        })
    }

    fn request(&self) -> Result<reqwest::RequestBuilder> {
        let mut logical = HttpFileRequest::new(HttpMethod::Get, self.url.clone());
        if let Some(auth) = &self.auth {
            auth.apply(&mut logical)?;
        }
        let mut request = self.client.get(&self.url);
        for (name, value) in logical.headers {
            request = request.header(name, value);
        }
        if let Some(etag) = &self.expected.etag {
            request = request.header("if-match", etag);
        }
        Ok(request)
    }
}

impl ByteSource for HttpByteSource {
    fn identity(&self) -> &ContentIdentity {
        &self.identity
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        &self.capabilities
    }

    fn exact_range_coalescing_policy(&self) -> ExactRangeCoalescingPolicy {
        REMOTE_RANGE_COALESCING_POLICY
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        Box::pin(async move {
            request.cancellation.check()?;
            validate_chunk_target(request.preferred_chunk_bytes, &self.capabilities)?;
            let response = request
                .cancellation
                .await_or_cancel(with_file_progress_deadline(
                    "receive HTTP byte-source response",
                    self.file_response_timeout,
                    async { self.request()?.send().await.map_err(http_send_error) },
                ))
                .await?;
            validate_response(&response, 200, &self.expected)?;
            let state = HttpSequentialState {
                stream: Box::pin(response.bytes_stream()),
                expected_size: self.expected.size_bytes.unwrap_or_default(),
                memory: Arc::clone(&self.memory),
                cancellation: request.cancellation,
                maximum_chunk_bytes: request.preferred_chunk_bytes,
                transferred_bytes: 0,
                pending: None,
                expected_checksum: self.expected.sha256().map(str::to_owned),
                hasher: self.expected.sha256().map(|_| Sha256::new()),
                read_idle_timeout: self.file_read_idle_timeout,
            };
            Ok(Box::pin(stream::try_unfold(state, http_sequential_next)) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<AccountedBytes>> {
        Box::pin(async move {
            cancellation.check()?;
            if !self.capabilities.exact_ranges {
                return Err(CdfError::contract(
                    "weakly versioned HTTP objects require sequential verified spooling",
                ));
            }
            let end = extent
                .start
                .checked_add(extent.length)
                .ok_or_else(|| CdfError::contract("HTTP byte range overflowed"))?;
            if end > self.expected.size_bytes.unwrap_or_default() {
                return Err(CdfError::data("HTTP byte range exceeds planned generation"));
            }
            let lease = cancellation
                .await_or_cancel(reserve(
                    Arc::clone(&self.memory),
                    ReservationRequest::new(
                        ConsumerKey::new("http-byte-source-range", MemoryClass::Source)?,
                        extent.length,
                    )?,
                ))
                .await?;
            let response = cancellation
                .await_or_cancel(with_file_progress_deadline(
                    "receive HTTP byte-source range response",
                    self.file_response_timeout,
                    async {
                        self.request()?
                            .header("range", format!("bytes={}-{}", extent.start, end - 1))
                            .send()
                            .await
                            .map_err(http_send_error)
                    },
                ))
                .await?;
            validate_response(&response, 206, &self.expected)?;
            let content_range = response
                .headers()
                .get("content-range")
                .and_then(|value| value.to_str().ok());
            let expected_content_range = format!(
                "bytes {}-{}/{expected_size}",
                extent.start,
                end - 1,
                expected_size = self.expected.size_bytes.unwrap_or_default()
            );
            if content_range != Some(expected_content_range.as_str()) {
                return Err(CdfError::data(format!(
                    "HTTP range response Content-Range {:?} does not match `{expected_content_range}`",
                    content_range
                )));
            }
            let bytes = read_exact_range_body_with_idle(
                response,
                extent.length,
                self.file_read_idle_timeout,
                cancellation.clone(),
            )
            .await?;
            cancellation.check()?;
            AccountedBytes::new(bytes, lease)
        })
    }
}

type HttpBodyStream =
    Pin<Box<dyn Stream<Item = std::result::Result<Bytes, reqwest::Error>> + Send + 'static>>;

struct HttpSequentialState {
    stream: HttpBodyStream,
    expected_size: u64,
    memory: Arc<dyn MemoryCoordinator>,
    cancellation: RunCancellation,
    maximum_chunk_bytes: u64,
    transferred_bytes: u64,
    pending: Option<AccountedBytes>,
    expected_checksum: Option<String>,
    hasher: Option<Sha256>,
    read_idle_timeout: Duration,
}

async fn http_sequential_next(
    mut state: HttpSequentialState,
) -> Result<Option<(AccountedBytes, HttpSequentialState)>> {
    state.cancellation.check()?;
    if let Some(chunk) = take_http_sequential_chunk(&mut state)? {
        return Ok(Some((chunk, state)));
    }
    let lease = state
        .cancellation
        .await_or_cancel(reserve(
            Arc::clone(&state.memory),
            ReservationRequest::new(
                ConsumerKey::new("http-byte-source-sequential", MemoryClass::Source)?,
                state.maximum_chunk_bytes,
            )?,
        ))
        .await?;
    loop {
        state.cancellation.check()?;
        let cancellation = state.cancellation.clone();
        let next = cancellation
            .await_or_cancel(with_file_progress_deadline(
                "stream HTTP byte-source response",
                state.read_idle_timeout,
                async { state.stream.try_next().await.map_err(http_body_error) },
            ))
            .await?;
        let Some(bytes) = next else {
            drop(lease);
            if state.transferred_bytes != state.expected_size {
                return Err(CdfError::data(format!(
                    "HTTP sequential response returned {} bytes for planned {}-byte generation",
                    state.transferred_bytes, state.expected_size
                )));
            }
            if let Some(expected) = &state.expected_checksum {
                let observed = format!(
                    "{:x}",
                    state
                        .hasher
                        .take()
                        .ok_or_else(|| {
                            CdfError::internal(
                                "HTTP checksum expectation omitted its streaming hasher",
                            )
                        })?
                        .finalize()
                );
                if observed
                    != expected
                        .strip_prefix("sha256:")
                        .unwrap_or(expected.as_str())
                {
                    return Err(CdfError::data(
                        "HTTP sequential response checksum does not match planned content identity",
                    ));
                }
            }
            return Ok(None);
        };
        let length = u64::try_from(bytes.len())
            .map_err(|_| CdfError::data("HTTP response chunk exceeds u64"))?;
        if length == 0 {
            continue;
        }
        lease.reconcile(length)?;
        state.transferred_bytes = state
            .transferred_bytes
            .checked_add(length)
            .ok_or_else(|| CdfError::data("HTTP transfer byte count overflowed"))?;
        if state.transferred_bytes > state.expected_size {
            return Err(CdfError::data(
                "HTTP sequential response exceeded planned generation length",
            ));
        }
        if let Some(hasher) = &mut state.hasher {
            hasher.update(&bytes);
        }
        state.cancellation.check()?;
        state.pending = Some(AccountedBytes::new(bytes, lease)?);
        let chunk = take_http_sequential_chunk(&mut state)?.ok_or_else(|| {
            CdfError::internal("nonempty HTTP frame produced no sequential chunk")
        })?;
        return Ok(Some((chunk, state)));
    }
}

async fn read_exact_range_body_with_idle(
    response: reqwest::Response,
    expected_length: u64,
    read_idle_timeout: Duration,
    cancellation: RunCancellation,
) -> Result<Bytes> {
    let capacity = usize::try_from(expected_length)
        .map_err(|_| CdfError::data("HTTP exact range length exceeds usize"))?;
    let mut stream = response.bytes_stream();
    let mut body = Vec::with_capacity(capacity);
    while body.len() < capacity {
        cancellation.check()?;
        let next = cancellation
            .await_or_cancel(with_file_progress_deadline(
                "stream HTTP byte-source range response",
                read_idle_timeout,
                async { stream.try_next().await.map_err(http_body_error) },
            ))
            .await?;
        let Some(chunk) = next else {
            break;
        };
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| CdfError::data("HTTP exact range body length overflowed"))?;
        if next_len > capacity {
            return Err(CdfError::data("HTTP exact range returned too many bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    if body.len() != capacity {
        return Err(CdfError::data("HTTP exact range returned a short body"));
    }
    cancellation.check()?;
    Ok(Bytes::from(body))
}

fn take_http_sequential_chunk(state: &mut HttpSequentialState) -> Result<Option<AccountedBytes>> {
    let Some(pending) = state.pending.take() else {
        return Ok(None);
    };
    let target = usize::try_from(state.maximum_chunk_bytes)
        .map_err(|_| CdfError::data("HTTP chunk target exceeds usize"))?;
    if pending.payload().len() <= target {
        return Ok(Some(pending));
    }
    let chunk = pending.slice(0..target)?;
    state.pending = Some(pending.slice(target..pending.payload().len())?);
    Ok(Some(chunk))
}

fn validate_chunk_target(target: u64, capabilities: &ByteSourceCapabilities) -> Result<()> {
    if target < capabilities.minimum_chunk_bytes || target > capabilities.maximum_chunk_bytes {
        return Err(CdfError::contract(format!(
            "HTTP sequential chunk target {target} is outside {}..={} bytes",
            capabilities.minimum_chunk_bytes, capabilities.maximum_chunk_bytes
        )));
    }
    Ok(())
}

fn validate_response(
    response: &reqwest::Response,
    expected_status: u16,
    expected: &FileIdentityMetadata,
) -> Result<()> {
    let status = response.status().as_u16();
    if status != expected_status {
        let retry_after_ms = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(|seconds| seconds.saturating_mul(1_000));
        return Err(classify_http_byte_source_status(
            status,
            expected_status,
            retry_after_ms,
        ));
    }
    if let Some(expected_etag) = expected.etag.as_deref() {
        let etag = response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok());
        if etag != Some(expected_etag) {
            return Err(CdfError::data(
                "HTTP object generation changed (ETag mismatch)",
            ));
        }
    } else if let Some(expected_modified) = expected.modified.as_deref() {
        let modified = response
            .headers()
            .get("last-modified")
            .and_then(|value| value.to_str().ok());
        if modified != Some(expected_modified) {
            return Err(CdfError::data(
                "HTTP object generation changed (Last-Modified mismatch)",
            ));
        }
    }
    if expected_status == 200 {
        let content_length = response
            .headers()
            .get("content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        if content_length != expected.size_bytes {
            return Err(CdfError::data(format!(
                "HTTP response Content-Length {:?} does not match planned {:?}",
                content_length, expected.size_bytes
            )));
        }
    }
    Ok(())
}

fn classify_http_byte_source_status(
    status: u16,
    expected_status: u16,
    retry_after_ms: Option<u64>,
) -> CdfError {
    let message = || format!("HTTP byte source expected status {expected_status}, got {status}");
    match status {
        401 | 403 => CdfError::auth(message()),
        408 | 425 | 500..=599 => CdfError::transient(message()),
        429 => CdfError::rate_limited(message(), retry_after_ms),
        412 => CdfError::data("HTTP object generation changed (precondition failed)"),
        200 if expected_status == 206 => {
            CdfError::data("HTTP byte source ignored the planned exact byte range")
        }
        300..=499 => CdfError::data(message()),
        _ => CdfError::new(ErrorKind::Internal, message()),
    }
}

struct RawHttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Option<AccountedBytes>,
}

fn response_headers(headers: &reqwest::header::HeaderMap) -> Vec<(String, String)> {
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

fn http_send_error(error: reqwest::Error) -> CdfError {
    CdfError::transient(format!(
        "send HTTP byte-source request: {}",
        sanitized_reqwest_error(error)
    ))
}

fn http_body_error(error: reqwest::Error) -> CdfError {
    CdfError::transient(format!(
        "stream HTTP byte-source response: {}",
        sanitized_reqwest_error(error)
    ))
}

async fn with_file_progress_deadline<T, F>(
    operation: &str,
    timeout: Duration,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| http_file_progress_timeout(operation, timeout))?
}

fn http_file_progress_timeout(operation: &str, timeout: Duration) -> CdfError {
    CdfError::transient(format!(
        "{operation} made no progress for {} ms",
        timeout.as_millis()
    ))
}

fn sanitized_reqwest_error(error: reqwest::Error) -> String {
    error.without_url().to_string()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::TcpListener,
        sync::{Mutex, mpsc},
        thread,
        time::Duration,
    };

    use cdf_memory::DeterministicMemoryCoordinator;
    use futures_util::TryStreamExt;

    use super::*;

    fn rest_response_budget(
        maximum_body_bytes: u64,
        coordinator: Arc<DeterministicMemoryCoordinator>,
    ) -> HttpResponseBudget {
        let memory: Arc<dyn MemoryCoordinator> = coordinator;
        HttpResponseBudget::new(maximum_body_bytes, memory, Arc::new(|| Ok(()))).unwrap()
    }

    #[test]
    fn byte_source_statuses_preserve_scheduler_retry_taxonomy() {
        let rate = classify_http_byte_source_status(429, 200, Some(7_000));
        let transient = classify_http_byte_source_status(503, 200, None);
        let auth = classify_http_byte_source_status(401, 200, None);
        let changed = classify_http_byte_source_status(412, 206, None);
        let ignored_range = classify_http_byte_source_status(200, 206, None);

        assert_eq!(rate.kind, ErrorKind::RateLimited);
        assert_eq!(rate.retry_after_ms, Some(7_000));
        assert_eq!(transient.kind, ErrorKind::Transient);
        assert_eq!(auth.kind, ErrorKind::Auth);
        assert_eq!(changed.kind, ErrorKind::Data);
        assert_eq!(ignored_range.kind, ErrorKind::Data);
    }

    #[tokio::test]
    async fn rest_rejects_oversized_content_length_before_body_allocation() {
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap());
        let body: BoundedBodyStream = Box::pin(stream::iter([Ok(Bytes::from_static(b"12345678"))]));
        let error = collect_bounded_response_body(
            body,
            Some(8),
            "REST",
            &rest_response_budget(4, Arc::clone(&coordinator)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("declares 8 body bytes"));
        assert_eq!(coordinator.snapshot().peak_bytes, 0);
    }

    #[tokio::test]
    async fn rest_stops_chunked_body_exactly_at_the_accounted_limit() {
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap());
        let body: BoundedBodyStream = Box::pin(stream::iter([Ok(Bytes::from_static(b"abcdef"))]));
        let error = collect_bounded_response_body(
            body,
            None,
            "REST",
            &rest_response_budget(4, Arc::clone(&coordinator)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("exceeds its 4-byte body limit"));
        assert_eq!(coordinator.snapshot().peak_bytes, 4);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_source_streams_once_and_ranges_with_generation_preconditions() {
        let body = b"0123456789abcdef";
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&requests);
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = socket.read(&mut buffer).unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                }
                let request = String::from_utf8(request).unwrap();
                observed.lock().unwrap().push(request.clone());
                if request.to_ascii_lowercase().contains("range: bytes=4-9") {
                    socket
                        .write_all(
                            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\nContent-Range: bytes 4-9/16\r\nETag: \"generation-1\"\r\nConnection: close\r\n\r\n456789",
                        )
                        .unwrap();
                } else {
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\nETag: \"generation-1\"\r\nConnection: close\r\n\r\n0123456789abcdef",
                        )
                        .unwrap();
                }
            }
        });
        let url = format!("http://{address}/events.bin");
        let resource = FileTransportResource::http_url(url.clone());
        let expected = FileIdentityMetadata {
            location: url,
            size_bytes: Some(16),
            checksum: None,
            etag: Some("\"generation-1\"".to_owned()),
            version: None,
            modified: None,
            exact_ranges: true,
        };
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let transport = ReqwestHttpProvider::new().unwrap();
        let source = transport
            .open_byte_source(&resource, &expected, None, memory)
            .unwrap();

        let chunks = source
            .open_sequential(SequentialReadRequest {
                preferred_chunk_bytes: MINIMUM_CHUNK_BYTES,
                cancellation: RunCancellation::default(),
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let streamed = chunks
            .iter()
            .flat_map(|chunk| chunk.payload().iter().copied())
            .collect::<Vec<_>>();
        drop(chunks);
        let ranged = source
            .read_exact_range(ByteExtent::new(4, 6).unwrap(), RunCancellation::default())
            .await
            .unwrap();

        assert_eq!(streamed, body);
        assert_eq!(ranged.payload(), b"456789");
        drop(ranged);
        server.join().unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| {
            request
                .to_ascii_lowercase()
                .contains("if-match: \"generation-1\"")
        }));
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.to_ascii_lowercase().contains("range:"))
                .count(),
            1
        );
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_source_cancellation_interrupts_a_pending_request() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_sender, request_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            request_sender.send(()).unwrap();
            let _ = release_receiver.recv_timeout(Duration::from_secs(3));
        });
        let url = format!("http://{address}/stalled.bin");
        let resource = FileTransportResource::http_url(url.clone());
        let expected = FileIdentityMetadata {
            location: url,
            size_bytes: Some(16),
            checksum: None,
            etag: None,
            version: None,
            modified: None,
            exact_ranges: false,
        };
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let source = ReqwestHttpProvider::new()
            .unwrap()
            .open_byte_source(&resource, &expected, None, memory)
            .unwrap();
        let cancellation = RunCancellation::default();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: MINIMUM_CHUNK_BYTES,
                    cancellation: task_cancellation,
                })
                .await
        });
        tokio::task::spawn_blocking(move || request_receiver.recv_timeout(Duration::from_secs(2)))
            .await
            .unwrap()
            .unwrap();
        cancellation.cancel();
        let outcome = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("pending HTTP send ignored run cancellation")
            .unwrap();
        let error = match outcome {
            Ok(_) => panic!("pending HTTP send completed after cancellation"),
            Err(error) => error,
        };
        assert!(error.message.contains("cancelled"));
        release_sender.send(()).unwrap();
        server.join().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_source_fails_transiently_when_a_response_body_stops_making_progress() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (headers_sender, headers_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\nETag: \"generation-1\"\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            socket.flush().unwrap();
            headers_sender.send(()).unwrap();
            let _ = release_receiver.recv_timeout(Duration::from_secs(3));
        });
        let url = format!("http://{address}/stalled-body.bin");
        let resource = FileTransportResource::http_url(url.clone());
        let expected = FileIdentityMetadata {
            location: url,
            size_bytes: Some(16),
            checksum: None,
            etag: Some("\"generation-1\"".to_owned()),
            version: None,
            modified: None,
            exact_ranges: true,
        };
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let source = ReqwestHttpProvider::with_file_timeouts(
            Duration::from_secs(1),
            Duration::from_millis(50),
        )
        .unwrap()
        .open_byte_source(&resource, &expected, None, memory)
        .unwrap();
        let mut stream = source
            .open_sequential(SequentialReadRequest {
                preferred_chunk_bytes: MINIMUM_CHUNK_BYTES,
                cancellation: RunCancellation::default(),
            })
            .await
            .unwrap();
        tokio::task::spawn_blocking(move || headers_receiver.recv_timeout(Duration::from_secs(2)))
            .await
            .unwrap()
            .unwrap();

        let error = tokio::time::timeout(Duration::from_secs(1), stream.try_next())
            .await
            .expect("HTTP body idle timeout did not terminate the stalled response")
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Transient);
        assert!(error.message.contains("stream HTTP byte-source response"));
        drop(stream);
        release_sender.send(()).unwrap();
        server.join().unwrap();
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_source_idle_deadline_resets_after_progress() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\nETag: \"generation-1\"\r\nConnection: close\r\n\r\n")
                .unwrap();
            for chunk in [
                b"abc".as_slice(),
                b"def".as_slice(),
                b"ghi".as_slice(),
                b"jkl".as_slice(),
            ] {
                thread::sleep(Duration::from_millis(30));
                socket.write_all(chunk).unwrap();
                socket.flush().unwrap();
            }
        });
        let url = format!("http://{address}/slow-progress.bin");
        let resource = FileTransportResource::http_url(url.clone());
        let expected = FileIdentityMetadata {
            location: url,
            size_bytes: Some(12),
            checksum: None,
            etag: Some("\"generation-1\"".to_owned()),
            version: None,
            modified: None,
            exact_ranges: true,
        };
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let source = ReqwestHttpProvider::with_file_timeouts(
            Duration::from_secs(1),
            Duration::from_millis(50),
        )
        .unwrap()
        .open_byte_source(&resource, &expected, None, memory)
        .unwrap();

        let chunks = source
            .open_sequential(SequentialReadRequest {
                preferred_chunk_bytes: MINIMUM_CHUNK_BYTES,
                cancellation: RunCancellation::default(),
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let streamed = chunks
            .iter()
            .flat_map(|chunk| chunk.payload().iter().copied())
            .collect::<Vec<_>>();

        assert_eq!(streamed, b"abcdefghijkl");
        drop(chunks);
        server.join().unwrap();
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rest_transport_does_not_inherit_file_idle_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            thread::sleep(Duration::from_millis(120));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let budget = rest_response_budget(1024, coordinator);
        let transport = ReqwestHttpProvider::with_file_timeouts(
            Duration::from_millis(20),
            Duration::from_millis(20),
        )
        .unwrap();

        let response = transport
            .send(
                HttpRequest::new(HttpMethod::Get, format!("http://{address}/rest")),
                budget,
            )
            .await
            .unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(response.body().unwrap(), b"ok");
        server.join().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_range_batch_coalesces_requests_and_preserves_logical_order() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&requests);
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = socket.read(&mut buffer).unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                }
                let request = String::from_utf8(request).unwrap();
                observed.lock().unwrap().push(request.clone());
                let response = if request.to_ascii_lowercase().contains("range: bytes=0-7") {
                    b"HTTP/1.1 206 Partial Content\r\nContent-Length: 8\r\nContent-Range: bytes 0-7/16\r\nETag: \"generation-1\"\r\nConnection: close\r\n\r\n01234567".as_slice()
                } else if request.to_ascii_lowercase().contains("range: bytes=12-15") {
                    b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 12-15/16\r\nETag: \"generation-1\"\r\nConnection: close\r\n\r\ncdef".as_slice()
                } else {
                    panic!("unexpected HTTP range request: {request}");
                };
                socket.write_all(response).unwrap();
            }
        });
        let url = format!("http://{address}/events.bin");
        let resource = FileTransportResource::http_url(url.clone());
        let expected = FileIdentityMetadata {
            location: url,
            size_bytes: Some(16),
            checksum: None,
            etag: Some("\"generation-1\"".to_owned()),
            version: None,
            modified: None,
            exact_ranges: true,
        };
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let source = ReqwestHttpProvider::new()
            .unwrap()
            .open_byte_source(&resource, &expected, None, memory)
            .unwrap();

        let batch = source
            .read_exact_ranges(
                vec![
                    ByteExtent::new(4, 4).unwrap(),
                    ByteExtent::new(0, 4).unwrap(),
                    ByteExtent::new(12, 4).unwrap(),
                ],
                RunCancellation::default(),
            )
            .await
            .unwrap();
        server.join().unwrap();

        assert_eq!(batch.logical()[0].payload(), b"4567");
        assert_eq!(batch.logical()[1].payload(), b"0123");
        assert_eq!(batch.logical()[2].payload(), b"cdef");
        assert_eq!(batch.logical_bytes(), 12);
        assert_eq!(batch.useful_bytes(), 12);
        assert_eq!(batch.physical_bytes(), 12);
        assert_eq!(batch.prefetch_waste_bytes(), 0);
        assert_eq!(batch.request_count(), 2);
        assert_eq!(requests.lock().unwrap().len(), 2);
        assert_eq!(coordinator.snapshot().current_bytes, 12);
        drop(batch);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_get_returns_after_headers_without_draining_the_object_body() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (release_sender, release_receiver) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nContent-Length: 1048576\r\nContent-Range: bytes 0-0/1048576\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            socket.flush().unwrap();
            let _ = release_receiver.recv_timeout(Duration::from_secs(3));
        });
        let transport = ReqwestHttpProvider::new().unwrap();
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            transport.send_headers(HttpFileRequest::new(
                HttpMethod::Get,
                format!("http://{address}/large.parquet"),
            )),
        )
        .await
        .expect("header-only metadata request tried to drain the object body")
        .unwrap();
        assert_eq!(response.status, 206);
        assert!(response.headers.contains_key("content-length"));
        release_sender.send(()).unwrap();
        server.join().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reqwest_failures_remove_signed_urls_from_error_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let transport = ReqwestHttpProvider::new().unwrap();
        let secret = "must-not-leak";

        let error = transport
            .send_headers(HttpFileRequest::new(
                HttpMethod::Get,
                format!("http://{address}/file?X-Amz-Signature={secret}"),
            ))
            .await
            .unwrap_err();

        assert!(!error.to_string().contains(secret));
        assert!(!error.to_string().contains("X-Amz-Signature"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_sequential_source_slices_oversized_transport_frames_under_one_lease() {
        const WINDOW_BYTES: u64 = 2;
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(3, BTreeMap::new()).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let stream: HttpBodyStream = Box::pin(stream::iter([
            Ok::<Bytes, reqwest::Error>(Bytes::new()),
            Ok::<Bytes, reqwest::Error>(Bytes::new()),
            Ok::<Bytes, reqwest::Error>(Bytes::from_static(b"abc")),
        ]));
        let state = HttpSequentialState {
            stream,
            expected_size: 3,
            memory,
            cancellation: RunCancellation::default(),
            maximum_chunk_bytes: WINDOW_BYTES,
            transferred_bytes: 0,
            pending: None,
            expected_checksum: None,
            hasher: None,
            read_idle_timeout: Duration::from_secs(1),
        };

        let (chunk, state) = http_sequential_next(state).await.unwrap().unwrap();
        assert_eq!(chunk.payload(), b"ab");
        assert_eq!(chunk.lease().bytes(), 3);
        assert_eq!(coordinator.snapshot().peak_bytes, 3);
        drop(chunk);
        let (chunk, state) = http_sequential_next(state).await.unwrap().unwrap();
        assert_eq!(chunk.payload(), b"c");
        drop(chunk);
        assert!(http_sequential_next(state).await.unwrap().is_none());
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn weak_http_identity_selects_sequential_verified_spool_instead_of_ranges() {
        let transport = ReqwestHttpProvider::new().unwrap();
        let resource = FileTransportResource::http_url("https://example.test/events.bin");
        let expected = FileIdentityMetadata {
            location: "https://example.test/events.bin".to_owned(),
            size_bytes: Some(16),
            checksum: None,
            etag: None,
            version: None,
            modified: Some("Wed, 08 Jul 2026 12:00:00 GMT".to_owned()),
            exact_ranges: false,
        };
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());

        let source = transport
            .open_byte_source(&resource, &expected, None, memory)
            .unwrap();
        assert!(!source.capabilities().seekable);
        assert!(!source.capabilities().exact_ranges);
        assert_eq!(source.capabilities().useful_range_concurrency, 0);
        assert_eq!(source.identity().strength, GenerationStrength::Weak);
    }

    #[test]
    fn unversioned_http_identity_remains_sequential_and_attestable() {
        let transport = ReqwestHttpProvider::new().unwrap();
        let resource = FileTransportResource::http_url("https://example.test/events.bin");
        let expected = FileIdentityMetadata {
            location: "https://example.test/events.bin".to_owned(),
            size_bytes: Some(16),
            checksum: None,
            etag: None,
            version: None,
            modified: None,
            exact_ranges: false,
        };
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());

        let source = transport
            .open_byte_source(&resource, &expected, None, memory)
            .unwrap();

        assert_eq!(
            source.identity().generation.as_deref(),
            Some("unversioned-size:16")
        );
        assert_eq!(source.identity().strength, GenerationStrength::Weak);
        assert!(!source.capabilities().seekable);
        assert!(!source.capabilities().exact_ranges);
    }

    #[test]
    fn strong_http_identity_without_range_attestation_uses_sequential_access() {
        let transport = ReqwestHttpProvider::new().unwrap();
        let resource = FileTransportResource::http_url("https://example.test/events.bin");
        let expected = FileIdentityMetadata {
            location: "https://example.test/events.bin".to_owned(),
            size_bytes: Some(16),
            checksum: None,
            etag: Some("\"generation-1\"".to_owned()),
            version: None,
            modified: None,
            exact_ranges: false,
        };
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());

        let source = transport
            .open_byte_source(&resource, &expected, None, memory)
            .unwrap();

        assert_eq!(source.identity().strength, GenerationStrength::Strong);
        assert!(!source.capabilities().seekable);
        assert!(!source.capabilities().exact_ranges);
    }
}
