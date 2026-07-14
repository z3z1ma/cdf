use std::sync::Arc;

use bytes::Bytes;
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    GenerationStrength, RunCancellation, SequentialReadRequest,
};
use futures_util::{TryStreamExt, stream};
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};

use crate::{FileIdentityMetadata, transport::verify_generation_identity};

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
            let result = self
                .store
                .get_opts(&self.path, self.get_options())
                .await
                .map_err(|error| object_error("open object stream", error))?;
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
            let state = SequentialState {
                stream: result.into_stream(),
                store: Arc::clone(&self.store),
                path: self.path.clone(),
                expected: self.expected.clone(),
                memory: Arc::clone(&self.memory),
                cancellation: request.cancellation,
                maximum_chunk_bytes: request.preferred_chunk_bytes,
                transferred_bytes: 0,
            };
            Ok(Box::pin(stream::try_unfold(state, sequential_next)) as AccountedByteStream)
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
            let lease = reserve(
                Arc::clone(&self.memory),
                ReservationRequest::new(
                    ConsumerKey::new("object-store-byte-source-range", MemoryClass::Source)?,
                    extent.length,
                )?,
            )
            .await?;
            let result = self
                .store
                .get_opts(
                    &self.path,
                    self.get_options().with_range(Some(extent.start..end)),
                )
                .await
                .map_err(|error| object_error("read object range", error))?;
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
            let bytes = result
                .bytes()
                .await
                .map_err(|error| object_error("collect exact object range", error))?;
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

struct SequentialState {
    stream: futures_util::stream::BoxStream<'static, object_store::Result<Bytes>>,
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    expected: FileIdentityMetadata,
    memory: Arc<dyn MemoryCoordinator>,
    cancellation: RunCancellation,
    maximum_chunk_bytes: u64,
    transferred_bytes: u64,
}

async fn sequential_next(
    mut state: SequentialState,
) -> Result<Option<(AccountedBytes, SequentialState)>> {
    state.cancellation.check()?;
    let lease = reserve(
        Arc::clone(&state.memory),
        ReservationRequest::new(
            ConsumerKey::new("object-store-byte-source-sequential", MemoryClass::Source)?,
            state.maximum_chunk_bytes,
        )?,
    )
    .await?;
    loop {
        state.cancellation.check()?;
        let Some(bytes) = state
            .stream
            .try_next()
            .await
            .map_err(|error| object_error("stream object body", error))?
        else {
            drop(lease);
            verify_generation_identity(&state.expected, &state.expected, state.transferred_bytes)?;
            if state.expected.etag.is_none() && state.expected.version.is_none() {
                let metadata = state
                    .store
                    .head(&state.path)
                    .await
                    .map_err(|error| object_error("reattest weak object generation", error))?;
                let observed =
                    crate::transport::object_identity(state.expected.location.clone(), metadata);
                verify_generation_identity(&state.expected, &observed, state.transferred_bytes)?;
            }
            return Ok(None);
        };
        let length = u64::try_from(bytes.len())
            .map_err(|_| CdfError::data("object-store response chunk exceeds u64"))?;
        if length == 0 {
            continue;
        }
        if length > state.maximum_chunk_bytes {
            return Err(CdfError::data(format!(
                "object-store response chunk {length} exceeds its pre-admitted {}-byte envelope",
                state.maximum_chunk_bytes
            )));
        }
        state.transferred_bytes = state
            .transferred_bytes
            .checked_add(length)
            .ok_or_else(|| CdfError::data("object-store transfer byte count overflowed"))?;
        if state.transferred_bytes > state.expected.size_bytes.unwrap_or_default() {
            return Err(CdfError::data(
                "object-store sequential response exceeded planned generation length",
            ));
        }
        state.cancellation.check()?;
        return Ok(Some((AccountedBytes::new(bytes, lease)?, state)));
    }
}

fn object_error(action: &str, error: object_store::Error) -> CdfError {
    CdfError::data(format!("{action}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cdf_memory::DeterministicMemoryCoordinator;
    use futures_util::{StreamExt, TryStreamExt};
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};

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

    #[test]
    fn object_store_sequential_source_skips_empty_provider_frames_under_one_lease() {
        const WINDOW_BYTES: u64 = 4 * 1024 * 1024;
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
            Arc::new(DeterministicMemoryCoordinator::new(WINDOW_BYTES, BTreeMap::new()).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let stream = futures_util::stream::iter([
            Ok::<Bytes, object_store::Error>(Bytes::new()),
            Ok::<Bytes, object_store::Error>(Bytes::new()),
            Ok::<Bytes, object_store::Error>(Bytes::from_static(b"abc")),
        ])
        .boxed();
        let state = SequentialState {
            stream,
            store,
            path,
            expected,
            memory,
            cancellation: RunCancellation::default(),
            maximum_chunk_bytes: WINDOW_BYTES,
            transferred_bytes: 0,
        };

        let (chunk, state) = futures_executor::block_on(sequential_next(state))
            .unwrap()
            .unwrap();
        assert_eq!(chunk.payload(), b"abc");
        assert_eq!(chunk.lease().bytes(), 3);
        assert_eq!(coordinator.snapshot().peak_bytes, WINDOW_BYTES);
        drop(chunk);
        assert!(
            futures_executor::block_on(sequential_next(state))
                .unwrap()
                .is_none()
        );
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

        assert!(
            error.message.contains("precondition") || error.message.contains("Precondition"),
            "{error}"
        );
    }
}
