use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use cdf_kernel::{BoxFuture, CdfError, Result, SourceIoMetrics};
use futures_util::TryStreamExt;

use crate::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    ExactRangeReadBatch, RunCancellation, SequentialReadRequest,
};

#[derive(Default)]
struct SourceIoCounters {
    duration_ns: AtomicU64,
    logical_bytes: AtomicU64,
    physical_bytes: AtomicU64,
    requests: AtomicU64,
}

fn saturating_add(target: &AtomicU64, value: u64) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Cloneable invocation-local view over byte-source I/O counters.
///
/// The snapshot is operational telemetry and is deliberately detached from all
/// identity-bearing source and package structures.
#[derive(Clone, Default)]
pub struct SourceIoObserver {
    counters: Arc<SourceIoCounters>,
}

impl SourceIoObserver {
    fn observe(&self, duration_ns: u64, logical_bytes: u64, physical_bytes: u64, requests: u64) {
        saturating_add(&self.counters.duration_ns, duration_ns);
        saturating_add(&self.counters.logical_bytes, logical_bytes);
        saturating_add(&self.counters.physical_bytes, physical_bytes);
        saturating_add(&self.counters.requests, requests);
    }

    pub fn snapshot(&self) -> SourceIoMetrics {
        SourceIoMetrics {
            duration_ns: self.counters.duration_ns.load(Ordering::Relaxed),
            logical_bytes: self.counters.logical_bytes.load(Ordering::Relaxed),
            physical_bytes: self.counters.physical_bytes.load(Ordering::Relaxed),
            requests: self.counters.requests.load(Ordering::Relaxed),
        }
    }
}

/// Transport-neutral observation wrapper for one byte-source invocation.
///
/// It preserves source capabilities and exact logical payloads. Batched ranges
/// delegate to the wrapped source so provider/controller coalescing remains the
/// sole physical-request authority.
pub struct ObservedByteSource {
    inner: Arc<dyn ByteSource>,
    observer: SourceIoObserver,
}

impl ObservedByteSource {
    pub fn new(inner: Arc<dyn ByteSource>) -> Self {
        Self {
            inner,
            observer: SourceIoObserver::default(),
        }
    }

    pub fn observer(&self) -> SourceIoObserver {
        self.observer.clone()
    }
}

impl ByteSource for ObservedByteSource {
    fn identity(&self) -> &ContentIdentity {
        self.inner.identity()
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        self.inner.capabilities()
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        let inner = Arc::clone(&self.inner);
        let observer = self.observer.clone();
        Box::pin(async move {
            let open_started = Instant::now();
            let stream = match inner.open_sequential(request).await {
                Ok(stream) => stream,
                Err(error) => {
                    observer.observe(elapsed_ns(open_started), 0, 0, 1);
                    return Err(error);
                }
            };
            let state = (stream, observer, elapsed_ns(open_started), 0_u64);
            let observed = futures_util::stream::try_unfold(
                state,
                |(mut stream, observer, duration_ns, bytes)| async move {
                    let poll_started = Instant::now();
                    let next = stream.try_next().await;
                    let duration_ns = duration_ns.saturating_add(elapsed_ns(poll_started));
                    match next {
                        Ok(Some(chunk)) => {
                            let length = u64::try_from(chunk.payload().len()).map_err(|_| {
                                CdfError::data("sequential source chunk length exceeds u64")
                            })?;
                            let bytes = bytes.saturating_add(length);
                            Ok(Some((chunk, (stream, observer, duration_ns, bytes))))
                        }
                        Ok(None) => {
                            observer.observe(duration_ns, bytes, bytes, 1);
                            Ok(None)
                        }
                        Err(error) => {
                            observer.observe(duration_ns, bytes, bytes, 1);
                            Err(error)
                        }
                    }
                },
            );
            Ok(Box::pin(observed) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
        let inner = Arc::clone(&self.inner);
        let observer = self.observer.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.read_exact_range(extent, cancellation).await;
            match &result {
                Ok(bytes) => {
                    let length = u64::try_from(bytes.payload().len())
                        .map_err(|_| CdfError::data("exact source range length exceeds u64"))?;
                    observer.observe(elapsed_ns(started), length, length, 1);
                }
                Err(_) => observer.observe(elapsed_ns(started), 0, 0, 1),
            }
            result
        })
    }

    fn read_exact_ranges(
        &self,
        extents: Vec<ByteExtent>,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<ExactRangeReadBatch>> {
        let inner = Arc::clone(&self.inner);
        let observer = self.observer.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.read_exact_ranges(extents, cancellation).await;
            if let Ok(batch) = &result {
                observer.observe(
                    elapsed_ns(started),
                    batch.logical_bytes(),
                    batch.physical_bytes(),
                    u64::from(batch.request_count()),
                );
            }
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use cdf_kernel::Result;
    use cdf_memory::{
        ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator,
        ReservationRequest,
    };
    use futures_util::TryStreamExt;

    use super::*;

    struct MemorySource {
        payload: Bytes,
        identity: ContentIdentity,
        capabilities: ByteSourceCapabilities,
        memory: Arc<dyn MemoryCoordinator>,
    }

    impl MemorySource {
        fn new(payload: &'static [u8]) -> Result<Self> {
            Ok(Self {
                payload: Bytes::from_static(payload),
                identity: ContentIdentity {
                    stable_id: "memory://observed".to_owned(),
                    generation: Some("fixed".to_owned()),
                    checksum: None,
                    size_bytes: Some(payload.len() as u64),
                    strength: crate::GenerationStrength::Strong,
                },
                capabilities: ByteSourceCapabilities {
                    known_length: true,
                    reopenable: true,
                    seekable: true,
                    exact_ranges: true,
                    useful_range_concurrency: 2,
                    minimum_chunk_bytes: 1,
                    maximum_chunk_bytes: 64,
                },
                memory: Arc::new(DeterministicMemoryCoordinator::new(
                    1024,
                    std::collections::BTreeMap::new(),
                )?),
            })
        }

        fn accounted(&self, payload: Bytes) -> Result<cdf_memory::AccountedBytes> {
            let request = ReservationRequest::new(
                ConsumerKey::new("observed-source-test", MemoryClass::Source)?,
                payload.len() as u64,
            )?;
            let lease = self
                .memory
                .try_reserve(&request)?
                .ok_or_else(|| CdfError::data("test memory reservation refused"))?;
            cdf_memory::AccountedBytes::new(payload, lease)
        }
    }

    impl ByteSource for MemorySource {
        fn identity(&self) -> &ContentIdentity {
            &self.identity
        }

        fn capabilities(&self) -> &ByteSourceCapabilities {
            &self.capabilities
        }

        fn open_sequential(
            &self,
            _request: SequentialReadRequest,
        ) -> BoxFuture<'_, Result<AccountedByteStream>> {
            Box::pin(async move {
                let chunk = self.accounted(self.payload.clone())?;
                Ok(Box::pin(futures_util::stream::iter([Ok(chunk)])) as AccountedByteStream)
            })
        }

        fn read_exact_range(
            &self,
            extent: ByteExtent,
            _cancellation: RunCancellation,
        ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
            Box::pin(async move {
                let start = usize::try_from(extent.start)
                    .map_err(|_| CdfError::data("test range start exceeds usize"))?;
                let end = usize::try_from(extent.start + extent.length)
                    .map_err(|_| CdfError::data("test range end exceeds usize"))?;
                self.accounted(self.payload.slice(start..end))
            })
        }
    }

    #[test]
    fn observes_sequential_and_coalesced_range_io_without_changing_payloads() {
        let source = Arc::new(ObservedByteSource::new(Arc::new(
            MemorySource::new(b"abcdefghij").unwrap(),
        )));
        let observer = source.observer();
        let sequential =
            futures_executor::block_on(source.open_sequential(SequentialReadRequest {
                preferred_chunk_bytes: 10,
                cancellation: RunCancellation::default(),
            }))
            .unwrap();
        let chunks = futures_executor::block_on(sequential.try_collect::<Vec<_>>()).unwrap();
        assert_eq!(chunks[0].payload(), b"abcdefghij");

        let ranges = futures_executor::block_on(source.read_exact_ranges(
            vec![
                ByteExtent::new(0, 4).unwrap(),
                ByteExtent::new(2, 4).unwrap(),
            ],
            RunCancellation::default(),
        ))
        .unwrap();
        assert_eq!(ranges.logical()[0].payload(), b"abcd");
        assert_eq!(ranges.logical()[1].payload(), b"cdef");

        let snapshot = observer.snapshot();
        assert_eq!(snapshot.logical_bytes, 18);
        assert_eq!(snapshot.physical_bytes, 16);
        assert_eq!(snapshot.requests, 2);
        assert_eq!(snapshot.prefetch_waste_bytes(), 0);
        assert!(snapshot.duration_ns > 0);
    }
}
