use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Duration,
};

use bytes::Bytes;
use cdf_http::{HttpMethod, HttpRequest, HttpResponseBudget, HttpTransport};
use cdf_kernel::ErrorKind;
use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
use cdf_object_access::{
    FileIdentityMetadata, FileTransportResource, HttpFileRequest, HttpFileTransport,
};
use cdf_runtime::{ByteExtent, GenerationStrength, RunCancellation, SequentialReadRequest};
use futures_util::{TryStreamExt, stream};

use crate::byte_source::{HttpBodyStream, HttpSequentialState, http_sequential_next};
use crate::errors::classify_http_byte_source_status;
use crate::policy::MINIMUM_CHUNK_BYTES;
use crate::provider::ReqwestHttpProvider;
use crate::response_body::{BoundedBodyStream, collect_bounded_response_body};

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
    let unexpected_success = classify_http_byte_source_status(204, 200, None);

    assert_eq!(rate.kind, ErrorKind::RateLimited);
    assert_eq!(rate.retry_after_ms, Some(7_000));
    assert_eq!(transient.kind, ErrorKind::Transient);
    assert_eq!(auth.kind, ErrorKind::Auth);
    assert_eq!(changed.kind, ErrorKind::Data);
    assert_eq!(ignored_range.kind, ErrorKind::Data);
    assert_eq!(unexpected_success.kind, ErrorKind::Data);
}

#[tokio::test]
async fn rest_rejects_oversized_content_length_before_body_allocation() {
    let coordinator = Arc::new(DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap());
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
    let coordinator = Arc::new(DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap());
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
    let source =
        ReqwestHttpProvider::with_file_timeouts(Duration::from_secs(1), Duration::from_millis(50))
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
    let source =
        ReqwestHttpProvider::with_file_timeouts(Duration::from_secs(1), Duration::from_millis(50))
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
async fn control_transport_sends_post_body_without_copying_it_into_diagnostics() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while request.len() < 7
            || request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .is_none_or(|header_end| request.len() < header_end + 4 + 7)
        {
            let read = socket.read(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
        }
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap();
        assert!(String::from_utf8_lossy(&request[..header_end]).starts_with("POST /glue "));
        assert_eq!(&request[header_end + 4..], br#"{"x":1}"#);
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .unwrap();
    });
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
    let transport = ReqwestHttpProvider::new().unwrap();
    let request = HttpRequest::new(HttpMethod::Post, format!("http://{address}/glue"))
        .with_body(br#"{"x":1}"#.as_slice());
    assert!(!format!("{request:?}").contains(r#"{"x":1}"#));

    let response = transport
        .send(request, rest_response_budget(1024, coordinator))
        .await
        .unwrap();

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
    let coordinator = Arc::new(DeterministicMemoryCoordinator::new(3, BTreeMap::new()).unwrap());
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
