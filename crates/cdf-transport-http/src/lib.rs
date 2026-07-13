#![doc = "Pooled HTTP transport provider for cdf."]

use std::{pin::Pin, sync::Arc, thread};

use bytes::Bytes;
use cdf_http::{HeaderMap, HttpMethod, HttpRequest, HttpResponse, HttpTransport};
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    GenerationStrength, RunCancellation, SequentialReadRequest,
};
use cdf_source_files::{
    FileIdentityMetadata, FileTransportResource, HttpFileRequest, HttpFileResponse,
    HttpFileTransport,
};
use futures_util::{Stream, TryStreamExt, stream};
use sha2::{Digest, Sha256};

const MINIMUM_CHUNK_BYTES: u64 = 8 * 1024;
const MAXIMUM_CHUNK_BYTES: u64 = 32 * 1024 * 1024;

pub struct ReqwestHttpTransport {
    blocking: Option<reqwest::blocking::Client>,
    asynchronous: reqwest::Client,
}

impl ReqwestHttpTransport {
    pub fn new() -> Result<Self> {
        let blocking = thread::spawn(|| {
            reqwest::blocking::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
        })
        .join()
        .map_err(|_| CdfError::internal("blocking HTTP client builder panicked"))?
        .map_err(|error| CdfError::internal(format!("build blocking HTTP client: {error}")))?;
        let asynchronous = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| CdfError::internal(format!("build async HTTP client: {error}")))?;
        Ok(Self {
            blocking: Some(blocking),
            asynchronous,
        })
    }
}

impl Drop for ReqwestHttpTransport {
    fn drop(&mut self) {
        if let Some(client) = self.blocking.take() {
            let _ = thread::spawn(move || drop(client)).join();
        }
    }
}

impl HttpTransport for ReqwestHttpTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse> {
        let raw = self.send_raw(&request.method, &request.url, &request.headers, "REST")?;
        let mut response = HttpResponse::new(raw.status).with_body(raw.body);
        for (name, value) in raw.headers {
            response = response.with_header(name, value);
        }
        Ok(response)
    }
}

impl HttpFileTransport for ReqwestHttpTransport {
    fn send(&self, request: HttpFileRequest) -> Result<HttpFileResponse> {
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

    fn open_byte_source(
        &self,
        resource: &FileTransportResource,
        expected: &FileIdentityMetadata,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Arc<dyn ByteSource>> {
        if resource.auth.is_some() {
            return Err(CdfError::auth(
                "HTTP byte-source auth must be resolved by the transport provider before open",
            ));
        }
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
            self.asynchronous.clone(),
            url,
            expected.clone(),
            memory,
        )?))
    }
}

impl ReqwestHttpTransport {
    fn send_raw(
        &self,
        method: &HttpMethod,
        url: &str,
        headers: &HeaderMap,
        context: &str,
    ) -> Result<RawHttpResponse> {
        let method = reqwest_method(method)?;
        let mut builder = self.blocking()?.request(method, url);
        for (name, value) in headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        let response = builder.send().map_err(|error| {
            CdfError::transient(format!("send {context} HTTP request: {error}"))
        })?;
        let status = response.status().as_u16();
        let headers = response_headers(response.headers());
        let body = response.bytes().map_err(|error| {
            CdfError::transient(format!("read {context} HTTP response body: {error}"))
        })?;
        Ok(RawHttpResponse {
            status,
            headers,
            body: body.to_vec(),
        })
    }

    fn blocking(&self) -> Result<&reqwest::blocking::Client> {
        self.blocking
            .as_ref()
            .ok_or_else(|| CdfError::internal("blocking HTTP client unavailable during teardown"))
    }
}

struct HttpByteSource {
    client: reqwest::Client,
    url: String,
    expected: FileIdentityMetadata,
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    memory: Arc<dyn MemoryCoordinator>,
}

impl HttpByteSource {
    fn new(
        client: reqwest::Client,
        url: String,
        expected: FileIdentityMetadata,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Self> {
        let size_bytes = expected
            .size_bytes
            .ok_or_else(|| CdfError::data("HTTP byte source requires Content-Length"))?;
        let checksum = expected.sha256().map(str::to_owned);
        let generation = expected.etag.clone().or_else(|| {
            expected
                .modified
                .as_ref()
                .map(|modified| format!("last-modified:{modified};size:{size_bytes}"))
        });
        if generation.is_none() && checksum.is_none() {
            return Err(CdfError::data(
                "HTTP byte source requires ETag, Last-Modified, or SHA-256 content identity",
            ));
        }
        let exact_ranges = expected.etag.is_some();
        let identity = ContentIdentity {
            stable_id: url.clone(),
            size_bytes: Some(size_bytes),
            generation,
            checksum: checksum.clone(),
            strength: if checksum.is_some() {
                GenerationStrength::ContentAddressed
            } else if exact_ranges {
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
            identity,
            capabilities,
            memory,
        })
    }

    fn request(&self) -> reqwest::RequestBuilder {
        let request = self.client.get(&self.url);
        if let Some(etag) = &self.expected.etag {
            request.header("if-match", etag)
        } else {
            request
        }
    }
}

impl ByteSource for HttpByteSource {
    fn identity(&self) -> &ContentIdentity {
        &self.identity
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        &self.capabilities
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        Box::pin(async move {
            request.cancellation.check()?;
            validate_chunk_target(request.preferred_chunk_bytes, &self.capabilities)?;
            let response = self.request().send().await.map_err(http_send_error)?;
            validate_response(&response, 200, &self.expected)?;
            let state = HttpSequentialState {
                stream: Box::pin(response.bytes_stream()),
                expected_size: self.expected.size_bytes.unwrap_or_default(),
                memory: Arc::clone(&self.memory),
                cancellation: request.cancellation,
                maximum_chunk_bytes: request.preferred_chunk_bytes,
                transferred_bytes: 0,
                expected_checksum: self.expected.sha256().map(str::to_owned),
                hasher: Sha256::new(),
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
            let lease = reserve(
                Arc::clone(&self.memory),
                ReservationRequest::new(
                    ConsumerKey::new("http-byte-source-range", MemoryClass::Source)?,
                    extent.length,
                )?,
            )
            .await?;
            let response = self
                .request()
                .header("range", format!("bytes={}-{}", extent.start, end - 1))
                .send()
                .await
                .map_err(http_send_error)?;
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
            let bytes = response.bytes().await.map_err(http_body_error)?;
            if u64::try_from(bytes.len()).ok() != Some(extent.length) {
                return Err(CdfError::data("HTTP exact range returned a short body"));
            }
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
    expected_checksum: Option<String>,
    hasher: Sha256,
}

async fn http_sequential_next(
    mut state: HttpSequentialState,
) -> Result<Option<(AccountedBytes, HttpSequentialState)>> {
    state.cancellation.check()?;
    let lease = reserve(
        Arc::clone(&state.memory),
        ReservationRequest::new(
            ConsumerKey::new("http-byte-source-sequential", MemoryClass::Source)?,
            state.maximum_chunk_bytes,
        )?,
    )
    .await?;
    loop {
        state.cancellation.check()?;
        let Some(bytes) = state.stream.try_next().await.map_err(http_body_error)? else {
            drop(lease);
            if state.transferred_bytes != state.expected_size {
                return Err(CdfError::data(format!(
                    "HTTP sequential response returned {} bytes for planned {}-byte generation",
                    state.transferred_bytes, state.expected_size
                )));
            }
            if let Some(expected) = &state.expected_checksum {
                let observed = format!("{:x}", state.hasher.finalize());
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
        if length > state.maximum_chunk_bytes {
            return Err(CdfError::data(format!(
                "HTTP response chunk {length} exceeds its pre-admitted {}-byte envelope",
                state.maximum_chunk_bytes
            )));
        }
        state.transferred_bytes = state
            .transferred_bytes
            .checked_add(length)
            .ok_or_else(|| CdfError::data("HTTP transfer byte count overflowed"))?;
        if state.transferred_bytes > state.expected_size {
            return Err(CdfError::data(
                "HTTP sequential response exceeded planned generation length",
            ));
        }
        state.hasher.update(&bytes);
        state.cancellation.check()?;
        return Ok(Some((AccountedBytes::new(bytes, lease)?, state)));
    }
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
        return Err(if status == 412 {
            CdfError::data("HTTP object generation changed (If-Match precondition failed)")
        } else {
            CdfError::transient(format!(
                "HTTP byte source expected status {expected_status}, got {status}"
            ))
        });
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

struct RawHttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
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
    CdfError::transient(format!("send HTTP byte-source request: {error}"))
}

fn http_body_error(error: reqwest::Error) -> CdfError {
    CdfError::transient(format!("stream HTTP byte-source response: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::TcpListener,
        sync::Mutex,
        thread,
    };

    use cdf_memory::DeterministicMemoryCoordinator;
    use futures_util::TryStreamExt;

    use super::*;

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
        };
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let transport = ReqwestHttpTransport::new().unwrap();
        let source = transport
            .open_byte_source(&resource, &expected, memory)
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
    async fn http_sequential_source_skips_empty_transport_frames_under_one_lease() {
        const WINDOW_BYTES: u64 = 4 * 1024 * 1024;
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(WINDOW_BYTES, BTreeMap::new()).unwrap());
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
            expected_checksum: None,
            hasher: Sha256::new(),
        };

        let (chunk, state) = http_sequential_next(state).await.unwrap().unwrap();
        assert_eq!(chunk.payload(), b"abc");
        assert_eq!(chunk.lease().bytes(), 3);
        assert_eq!(coordinator.snapshot().peak_bytes, WINDOW_BYTES);
        drop(chunk);
        assert!(http_sequential_next(state).await.unwrap().is_none());
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn weak_http_identity_selects_sequential_verified_spool_instead_of_ranges() {
        let transport = ReqwestHttpTransport::new().unwrap();
        let resource = FileTransportResource::http_url("https://example.test/events.bin");
        let expected = FileIdentityMetadata {
            location: "https://example.test/events.bin".to_owned(),
            size_bytes: Some(16),
            checksum: None,
            etag: None,
            version: None,
            modified: Some("Wed, 08 Jul 2026 12:00:00 GMT".to_owned()),
        };
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());

        let source = transport
            .open_byte_source(&resource, &expected, memory)
            .unwrap();
        assert!(!source.capabilities().seekable);
        assert!(!source.capabilities().exact_ranges);
        assert_eq!(source.capabilities().useful_range_concurrency, 0);
        assert_eq!(source.identity().strength, GenerationStrength::Weak);
    }
}
