use std::sync::Arc;

use bytes::Bytes;
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    ExactRangeCoalescingPolicy, GenerationStrength, REMOTE_RANGE_COALESCING_POLICY,
    RunCancellation, SequentialReadRequest,
};
use futures_util::{TryStreamExt, stream};
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use sha2::{Digest, Sha256};

use crate::{
    FileIdentityMetadata,
    transport::{object_store_error, verify_generation_identity},
};

const MINIMUM_CHUNK_BYTES: u64 = 8 * 1024;
const MAXIMUM_CHUNK_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Clone)]
pub struct ObjectStoreByteSource {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    expected: FileIdentityMetadata,
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    memory: Arc<dyn MemoryCoordinator>,
}

impl std::fmt::Debug for ObjectStoreByteSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreByteSource")
            .field("identity", &self.identity)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreByteSource {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        expected: FileIdentityMetadata,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Self> {
        let size_bytes = expected.size_bytes.ok_or_else(|| {
            CdfError::data("object-store byte source requires planned content length")
        })?;
        let checksum = expected.sha256().map(str::to_owned);
        let (generation, strength) = if checksum.is_some() {
            (
                expected.etag.clone().or_else(|| checksum.clone()),
                GenerationStrength::ContentAddressed,
            )
        } else if let Some(token) = expected.etag.as_ref().or(expected.version.as_ref()) {
            (Some(token.clone()), GenerationStrength::Strong)
        } else {
            (
                expected
                    .modified
                    .as_ref()
                    .map(|modified| format!("object-v1:{size_bytes}:{modified}")),
                GenerationStrength::Weak,
            )
        };
        let identity = ContentIdentity {
            stable_id: expected.location.clone(),
            size_bytes: Some(size_bytes),
            generation,
            checksum,
            strength,
        };
        identity.validate()?;
        let exact_ranges = strength != GenerationStrength::Weak
            && (expected.etag.is_some() || expected.version.is_some());
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
            store,
            path,
            expected,
            identity,
            capabilities,
            memory,
        })
    }

    fn get_options(&self) -> GetOptions {
        GetOptions::new()
            .with_if_match(self.expected.etag.clone())
            .with_version(self.expected.version.clone())
    }
}

impl ByteSource for ObjectStoreByteSource {
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
            if request.preferred_chunk_bytes < self.capabilities.minimum_chunk_bytes
                || request.preferred_chunk_bytes > self.capabilities.maximum_chunk_bytes
            {
                return Err(CdfError::contract(format!(
                    "object-store sequential chunk target {} is outside {}..={} bytes",
                    request.preferred_chunk_bytes,
                    self.capabilities.minimum_chunk_bytes,
                    self.capabilities.maximum_chunk_bytes
                )));
            }
            let result = request
                .cancellation
                .await_or_cancel(async {
                    self.store
                        .get_opts(&self.path, self.get_options())
                        .await
                        .map_err(|error| object_store_error("open object stream", error))
                })
                .await?;
            let observed = crate::transport::object_identity(
                self.expected.location.clone(),
                result.meta.clone(),
            );
            verify_generation_identity(
                &self.expected,
                &observed,
                observed.size_bytes.ok_or_else(|| {
                    CdfError::data("object stream metadata omitted content length")
                })?,
            )?;
            let expected_size = self.expected.size_bytes.unwrap_or_default();
            if result.range != (0..expected_size) {
                return Err(CdfError::data(format!(
                    "object-store sequential response range {:?} does not match complete generation 0..{expected_size}",
                    result.range
                )));
            }
            let expected_checksum = self.expected.sha256().map(str::to_owned);
            if expected_size == 0 {
                if let Some(expected) = expected_checksum {
                    let observed = format!("{:x}", Sha256::digest([]));
                    if observed != expected.strip_prefix("sha256:").unwrap_or(&expected) {
                        return Err(CdfError::data(
                            "empty object-store response checksum does not match planned content identity",
                        ));
                    }
                }
                return Ok(Box::pin(stream::empty()) as AccountedByteStream);
            }
            let state = ObjectSequentialState {
                stream: result.into_stream(),
                store: Arc::clone(&self.store),
                path: self.path.clone(),
                expected: self.expected.clone(),
                memory: Arc::clone(&self.memory),
                cancellation: request.cancellation,
                preferred_chunk_bytes: request.preferred_chunk_bytes,
                maximum_provider_chunk_bytes: self.capabilities.maximum_chunk_bytes,
                transferred_bytes: 0,
                pending: None,
                expected_checksum: expected_checksum.clone(),
                hasher: expected_checksum.map(|_| Sha256::new()),
            };
            Ok(Box::pin(stream::try_unfold(state, object_sequential_next)) as AccountedByteStream)
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
                    "weak object generation cannot perform independent ranged reads; use sequential verified spool",
                ));
            }
            let end = extent
                .start
                .checked_add(extent.length)
                .ok_or_else(|| CdfError::contract("object-store byte range overflowed"))?;
            if end > self.expected.size_bytes.unwrap_or_default() {
                return Err(CdfError::data(
                    "object-store byte range exceeds planned generation",
                ));
            }
            let lease = cancellation
                .await_or_cancel(reserve(
                    Arc::clone(&self.memory),
                    ReservationRequest::new(
                        ConsumerKey::new("object-store-byte-source-range", MemoryClass::Source)?,
                        extent.length,
                    )?,
                ))
                .await?;
            let result = cancellation
                .await_or_cancel(async {
                    self.store
                        .get_opts(
                            &self.path,
                            self.get_options().with_range(Some(extent.start..end)),
                        )
                        .await
                        .map_err(|error| object_store_error("read object range", error))
                })
                .await?;
            if result.range != (extent.start..end) {
                return Err(CdfError::data(format!(
                    "object-store range response {:?} does not match requested {}..{end}",
                    result.range, extent.start
                )));
            }
            let observed = crate::transport::object_identity(
                self.expected.location.clone(),
                result.meta.clone(),
            );
            verify_generation_identity(
                &self.expected,
                &observed,
                observed.size_bytes.ok_or_else(|| {
                    CdfError::data("object range metadata omitted content length")
                })?,
            )?;
            let bytes = cancellation
                .await_or_cancel(async {
                    result
                        .bytes()
                        .await
                        .map_err(|error| object_store_error("collect exact object range", error))
                })
                .await?;
            if u64::try_from(bytes.len()).ok() != Some(extent.length) {
                return Err(CdfError::data(
                    "object-store exact range returned a short body",
                ));
            }
            cancellation.check()?;
            AccountedBytes::new(bytes, lease)
        })
    }
}

struct ObjectSequentialState {
    stream: futures_util::stream::BoxStream<'static, object_store::Result<Bytes>>,
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    expected: FileIdentityMetadata,
    memory: Arc<dyn MemoryCoordinator>,
    cancellation: RunCancellation,
    preferred_chunk_bytes: u64,
    maximum_provider_chunk_bytes: u64,
    transferred_bytes: u64,
    pending: Option<AccountedBytes>,
    expected_checksum: Option<String>,
    hasher: Option<Sha256>,
}

fn take_sequential_chunk(state: &mut ObjectSequentialState) -> Result<Option<AccountedBytes>> {
    let Some(pending) = state.pending.take() else {
        return Ok(None);
    };
    let preferred = usize::try_from(state.preferred_chunk_bytes)
        .map_err(|_| CdfError::data("object-store chunk target exceeds usize"))?;
    if pending.payload().len() <= preferred {
        return Ok(Some(pending));
    }
    let chunk = pending.slice(0..preferred)?;
    state.pending = Some(pending.slice(preferred..pending.payload().len())?);
    Ok(Some(chunk))
}

async fn object_sequential_next(
    mut state: ObjectSequentialState,
) -> Result<Option<(AccountedBytes, ObjectSequentialState)>> {
    state.cancellation.check()?;
    if let Some(chunk) = take_sequential_chunk(&mut state)? {
        return Ok(Some((chunk, state)));
    }
    let remaining = state
        .expected
        .size_bytes
        .unwrap_or_default()
        .saturating_sub(state.transferred_bytes);
    let admitted_frame_bytes = remaining.clamp(1, state.maximum_provider_chunk_bytes);
    let lease = state
        .cancellation
        .await_or_cancel(reserve(
            Arc::clone(&state.memory),
            ReservationRequest::new(
                ConsumerKey::new("object-store-byte-source-sequential", MemoryClass::Source)?,
                admitted_frame_bytes,
            )?,
        ))
        .await?;
    loop {
        state.cancellation.check()?;
        let Some(bytes) = state
            .cancellation
            .await_or_cancel(async {
                state
                    .stream
                    .try_next()
                    .await
                    .map_err(|error| object_store_error("stream object body", error))
            })
            .await?
        else {
            drop(lease);
            verify_generation_identity(&state.expected, &state.expected, state.transferred_bytes)?;
            if state.expected.etag.is_none() && state.expected.version.is_none() {
                let metadata = state
                    .cancellation
                    .await_or_cancel(async {
                        state.store.head(&state.path).await.map_err(|error| {
                            object_store_error("reattest weak object generation", error)
                        })
                    })
                    .await?;
                let observed =
                    crate::transport::object_identity(state.expected.location.clone(), metadata);
                verify_generation_identity(&state.expected, &observed, state.transferred_bytes)?;
            }
            if let Some(expected) = &state.expected_checksum {
                let observed = format!(
                    "{:x}",
                    state
                        .hasher
                        .take()
                        .ok_or_else(|| {
                            CdfError::internal(
                                "object checksum expectation omitted its streaming hasher",
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
                        "object-store sequential response checksum does not match planned content identity",
                    ));
                }
            }
            return Ok(None);
        };
        let length = u64::try_from(bytes.len())
            .map_err(|_| CdfError::data("object-store response chunk exceeds u64"))?;
        if length == 0 {
            continue;
        }
        lease.reconcile(length)?;
        state.transferred_bytes = state
            .transferred_bytes
            .checked_add(length)
            .ok_or_else(|| CdfError::data("object-store transfer byte count overflowed"))?;
        if state.transferred_bytes > state.expected.size_bytes.unwrap_or_default() {
            return Err(CdfError::data(
                "object-store sequential response exceeded planned generation length",
            ));
        }
        if let Some(hasher) = &mut state.hasher {
            hasher.update(&bytes);
        }
        state.cancellation.check()?;
        state.pending = Some(AccountedBytes::new(bytes, lease)?);
        let chunk = take_sequential_chunk(&mut state)?.ok_or_else(|| {
            CdfError::internal("nonempty object-store frame produced no sequential chunk")
        })?;
        return Ok(Some((chunk, state)));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cdf_memory::DeterministicMemoryCoordinator;
    use futures_util::{StreamExt, TryStreamExt};
    use object_store::{
        ObjectStoreExt, PutPayload,
        memory::InMemory,
        throttle::{ThrottleConfig, ThrottledStore},
    };

    use super::*;

    #[test]
    fn object_store_source_streams_and_ranges_one_generation() {
        let store = Arc::new(InMemory::new());
        let path = ObjectPath::from("events/data.bin");
        let body = Bytes::from_static(b"0123456789abcdef");
        futures_executor::block_on(store.put(&path, PutPayload::from(body.clone()))).unwrap();
        let metadata = futures_executor::block_on(store.head(&path)).unwrap();
        assert!(metadata.e_tag.is_some());
        let expected =
            crate::transport::object_identity("s3://bucket/events/data.bin".to_owned(), metadata);
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let source = ObjectStoreByteSource::new(store, path, expected, memory).unwrap();

        let (streamed, ranged) = futures_executor::block_on(async {
            let stream = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: MINIMUM_CHUNK_BYTES,
                    cancellation: RunCancellation::default(),
                })
                .await
                .unwrap();
            let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
            let streamed = chunks
                .iter()
                .flat_map(|chunk| chunk.payload().iter().copied())
                .collect::<Vec<_>>();
            drop(chunks);
            let ranged = source
                .read_exact_range(ByteExtent::new(4, 6).unwrap(), RunCancellation::default())
                .await
                .unwrap();
            (streamed, ranged)
        });

        assert_eq!(streamed, body);
        assert_eq!(ranged.payload(), b"456789");
        drop(ranged);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_cancellation_interrupts_pending_provider_get() {
        let inner = Arc::new(InMemory::new());
        let path = ObjectPath::from("events/pending.bin");
        inner
            .put(&path, PutPayload::from_static(b"payload"))
            .await
            .unwrap();
        let metadata = inner.head(&path).await.unwrap();
        let expected = crate::transport::object_identity(
            "s3://bucket/events/pending.bin".to_owned(),
            metadata,
        );
        let store: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
            inner,
            ThrottleConfig {
                wait_get_per_call: std::time::Duration::from_secs(60),
                ..ThrottleConfig::default()
            },
        ));
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let source = ObjectStoreByteSource::new(store, path, expected, memory).unwrap();
        let cancellation = RunCancellation::default();
        let cancel = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            cancel.cancel();
        });
        let started = std::time::Instant::now();
        let error = match source
            .open_sequential(SequentialReadRequest {
                preferred_chunk_bytes: MINIMUM_CHUNK_BYTES,
                cancellation,
            })
            .await
        {
            Ok(_) => panic!("pending provider GET ignored cancellation"),
            Err(error) => error,
        };

        assert!(error.message.contains("cancelled"), "{error}");
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn object_store_sequential_source_slices_oversized_provider_frames_under_one_lease() {
        const WINDOW_BYTES: u64 = 2;
        let store = Arc::new(InMemory::new());
        let path = ObjectPath::from("events/empty-frames.bin");
        futures_executor::block_on(store.put(&path, PutPayload::from(Bytes::from_static(b"abc"))))
            .unwrap();
        let metadata = futures_executor::block_on(store.head(&path)).unwrap();
        let expected = crate::transport::object_identity(
            "s3://bucket/events/empty-frames.bin".to_owned(),
            metadata,
        );
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(3, BTreeMap::new()).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let stream = futures_util::stream::iter([
            Ok::<Bytes, object_store::Error>(Bytes::new()),
            Ok::<Bytes, object_store::Error>(Bytes::new()),
            Ok::<Bytes, object_store::Error>(Bytes::from_static(b"abc")),
        ])
        .boxed();
        let state = ObjectSequentialState {
            stream,
            store,
            path,
            expected,
            memory,
            cancellation: RunCancellation::default(),
            preferred_chunk_bytes: WINDOW_BYTES,
            maximum_provider_chunk_bytes: WINDOW_BYTES,
            transferred_bytes: 0,
            pending: None,
            expected_checksum: None,
            hasher: None,
        };

        let (chunk, state) = futures_executor::block_on(object_sequential_next(state))
            .unwrap()
            .unwrap();
        assert_eq!(chunk.payload(), b"ab");
        assert_eq!(chunk.lease().bytes(), 3);
        assert_eq!(coordinator.snapshot().peak_bytes, 3);
        drop(chunk);
        let (chunk, state) = futures_executor::block_on(object_sequential_next(state))
            .unwrap()
            .unwrap();
        assert_eq!(chunk.payload(), b"c");
        drop(chunk);
        assert!(
            futures_executor::block_on(object_sequential_next(state))
                .unwrap()
                .is_none()
        );
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn strong_object_store_sequential_source_owns_chunk_shape() {
        const WINDOW_BYTES: u64 = 1024 * 1024;
        let store = Arc::new(InMemory::new());
        let path = ObjectPath::from("events/provider-oversized.bin");
        let body = Bytes::from(vec![b'x'; usize::try_from(WINDOW_BYTES * 2 + 137).unwrap()]);
        futures_executor::block_on(store.put(&path, PutPayload::from(body.clone()))).unwrap();
        let metadata = futures_executor::block_on(store.head(&path)).unwrap();
        assert!(metadata.e_tag.is_some());
        let expected = crate::transport::object_identity(
            "s3://bucket/events/provider-oversized.bin".to_owned(),
            metadata,
        );
        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(MAXIMUM_CHUNK_BYTES, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let source = ObjectStoreByteSource::new(store, path, expected, memory).unwrap();

        let (observed, chunk_lengths) = futures_executor::block_on(async {
            let mut stream = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: WINDOW_BYTES,
                    cancellation: RunCancellation::default(),
                })
                .await
                .unwrap();
            let mut observed = Vec::with_capacity(body.len());
            let mut chunk_lengths = Vec::new();
            while let Some(chunk) = stream.try_next().await.unwrap() {
                chunk_lengths.push(chunk.payload().len());
                observed.extend_from_slice(chunk.payload());
                drop(chunk);
            }
            (observed, chunk_lengths)
        });

        assert_eq!(observed, body);
        assert_eq!(chunk_lengths, vec![1024 * 1024, 1024 * 1024, 137]);
        assert!(coordinator.snapshot().peak_bytes <= MAXIMUM_CHUNK_BYTES);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn object_store_source_rejects_generation_change() {
        let store = Arc::new(InMemory::new());
        let path = ObjectPath::from("events/data.bin");
        futures_executor::block_on(store.put(&path, PutPayload::from_static(b"first"))).unwrap();
        let metadata = futures_executor::block_on(store.head(&path)).unwrap();
        let expected =
            crate::transport::object_identity("s3://bucket/events/data.bin".to_owned(), metadata);
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let source =
            ObjectStoreByteSource::new(store.clone(), path.clone(), expected, memory).unwrap();
        futures_executor::block_on(store.put(&path, PutPayload::from_static(b"second"))).unwrap();

        let error = futures_executor::block_on(
            source.read_exact_range(ByteExtent::new(0, 1).unwrap(), RunCancellation::default()),
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert_eq!(
            error.message,
            "read object range: object-store provider request failed"
        );
    }

    #[test]
    fn empty_strong_object_is_reattested_without_a_range() {
        let store = Arc::new(InMemory::new());
        let path = ObjectPath::from("events/empty.bin");
        futures_executor::block_on(store.put(&path, PutPayload::from(Bytes::new()))).unwrap();
        let metadata = futures_executor::block_on(store.head(&path)).unwrap();
        let expected =
            crate::transport::object_identity("s3://bucket/events/empty.bin".to_owned(), metadata);
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let source =
            ObjectStoreByteSource::new(store.clone(), path.clone(), expected, memory).unwrap();
        futures_executor::block_on(store.put(&path, PutPayload::from_static(b"changed"))).unwrap();

        let error =
            match futures_executor::block_on(source.open_sequential(SequentialReadRequest {
                preferred_chunk_bytes: MINIMUM_CHUNK_BYTES,
                cancellation: RunCancellation::default(),
            })) {
                Ok(_) => panic!("mutated empty object unexpectedly opened"),
                Err(error) => error,
            };

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(
            error.message.contains("generation")
                || error
                    .message
                    .contains("object-store provider request failed"),
            "{error}"
        );
    }
}
