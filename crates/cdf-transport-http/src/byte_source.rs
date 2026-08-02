//! Generation-bound sequential and exact-range HTTP byte source.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cdf_http::HttpMethod;
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use cdf_object_access::{FileIdentityMetadata, HttpFileRequest, ResolvedHttpAuth};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    ExactRangeCoalescingPolicy, GenerationStrength, REMOTE_RANGE_COALESCING_POLICY,
    RunCancellation, SequentialReadRequest,
};
use futures_util::{Stream, TryStreamExt, stream};
use sha2::{Digest, Sha256};

use crate::errors::{
    http_body_error, http_send_error, validate_response, with_file_progress_deadline,
};
use crate::policy::{MAXIMUM_CHUNK_BYTES, MINIMUM_CHUNK_BYTES};

pub(crate) struct HttpByteSource {
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
    pub(crate) fn new(
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

pub(crate) type HttpBodyStream =
    Pin<Box<dyn Stream<Item = std::result::Result<Bytes, reqwest::Error>> + Send + 'static>>;

pub(crate) struct HttpSequentialState {
    pub(crate) stream: HttpBodyStream,
    pub(crate) expected_size: u64,
    pub(crate) memory: Arc<dyn MemoryCoordinator>,
    pub(crate) cancellation: RunCancellation,
    pub(crate) maximum_chunk_bytes: u64,
    pub(crate) transferred_bytes: u64,
    pub(crate) pending: Option<AccountedBytes>,
    pub(crate) expected_checksum: Option<String>,
    pub(crate) hasher: Option<Sha256>,
    pub(crate) read_idle_timeout: Duration,
}

pub(crate) async fn http_sequential_next(
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
