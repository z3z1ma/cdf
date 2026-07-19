use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use cdf_kernel::{BoxFuture, CdfError, PayloadRetention, Result};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    GenerationStrength, RunCancellation, SequentialReadRequest, SpillBudgetCoordinator,
    SpillReservation,
};
use futures_util::{TryStreamExt, stream};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

const DEFAULT_TRANSFER_CHUNK_BYTES: u64 = 4 * 1024 * 1024;
const DEFAULT_TAIL_RANGE_BYTES: u64 = 32 * 1024 * 1024;
const SEQUENTIAL_SPOOL_USEFUL_RANGE_CONCURRENCY: u16 = 1;

pub(crate) struct GrowingSpoolSession {
    pub(crate) source: Arc<dyn ByteSource>,
    pub(crate) retention: PayloadRetention,
    pub(crate) completion: BoxFuture<'static, Result<Option<String>>>,
    pub(crate) spool_path: std::path::PathBuf,
    pub(crate) cache_staged: bool,
}

struct GrowingSpoolStorage {
    file: tempfile::NamedTempFile,
    _reservation: SpillReservation,
}

#[derive(Clone)]
struct GrowingSpoolByteSource {
    upstream: Arc<dyn ByteSource>,
    storage: Arc<GrowingSpoolStorage>,
    progress: Arc<GrowingSpoolProgress>,
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    memory: Arc<dyn MemoryCoordinator>,
    tail_range_bytes: u64,
}

struct GrowingSpoolProgress {
    state: Mutex<GrowingSpoolState>,
    changed: tokio::sync::Notify,
}

struct GrowingSpoolState {
    written_bytes: u64,
    terminal: GrowingSpoolTerminal,
}

enum GrowingSpoolTerminal {
    Running,
    Complete,
    Failed(String),
}

struct GrowingSpoolRequest {
    upstream: Arc<dyn ByteSource>,
    size_bytes: u64,
    maximum_spool_bytes: u64,
    spill: Arc<dyn SpillBudgetCoordinator>,
    memory: Arc<dyn MemoryCoordinator>,
    staging_root: Option<std::path::PathBuf>,
    cancellation: RunCancellation,
}

pub(crate) fn start_growing_spool(
    upstream: Arc<dyn ByteSource>,
    size_bytes: u64,
    maximum_spool_bytes: u64,
    spill: Arc<dyn SpillBudgetCoordinator>,
    memory: Arc<dyn MemoryCoordinator>,
    staging_root: Option<&Path>,
    cancellation: RunCancellation,
) -> Result<Option<GrowingSpoolSession>> {
    start_growing_spool_with_tail_bytes(
        GrowingSpoolRequest {
            upstream,
            size_bytes,
            maximum_spool_bytes,
            spill,
            memory,
            staging_root: staging_root.map(Path::to_path_buf),
            cancellation,
        },
        DEFAULT_TAIL_RANGE_BYTES,
    )
}

fn start_growing_spool_with_tail_bytes(
    request: GrowingSpoolRequest,
    tail_range_bytes: u64,
) -> Result<Option<GrowingSpoolSession>> {
    let GrowingSpoolRequest {
        upstream,
        size_bytes,
        maximum_spool_bytes,
        spill,
        memory,
        staging_root,
        cancellation,
    } = request;
    if size_bytes == 0 {
        return Err(CdfError::contract(
            "growing spool requires a nonempty known-length source",
        ));
    }
    if tail_range_bytes == 0 {
        return Err(CdfError::contract(
            "growing spool requires a nonzero bounded tail-range window",
        ));
    }
    let identity = upstream.identity().clone();
    identity.validate()?;
    let upstream_capabilities = upstream.capabilities();
    upstream_capabilities.validate()?;
    if identity.size_bytes != Some(size_bytes)
        || identity.strength == GenerationStrength::Weak
        || !upstream_capabilities.exact_ranges
    {
        return Err(CdfError::contract(
            "growing spool overlap requires one strong, known-length generation with enforceable exact ranges",
        ));
    }
    if size_bytes > maximum_spool_bytes {
        return Ok(None);
    }
    let Some(reservation) = spill.try_reserve(size_bytes)? else {
        return Ok(None);
    };
    let (file, cache_staged) = if let Some(staging_root) = staging_root {
        match tempfile::NamedTempFile::new_in(staging_root) {
            Ok(file) => (file, true),
            Err(_) => (
                tempfile::NamedTempFile::new().map_err(|error| {
                    CdfError::data(format!("create growing file spool: {error}"))
                })?,
                false,
            ),
        }
    } else {
        (
            tempfile::NamedTempFile::new()
                .map_err(|error| CdfError::data(format!("create growing file spool: {error}")))?,
            false,
        )
    };
    let storage = Arc::new(GrowingSpoolStorage {
        file,
        _reservation: reservation,
    });
    let progress = Arc::new(GrowingSpoolProgress {
        state: Mutex::new(GrowingSpoolState {
            written_bytes: 0,
            terminal: GrowingSpoolTerminal::Running,
        }),
        changed: tokio::sync::Notify::new(),
    });
    let capabilities = ByteSourceCapabilities {
        known_length: true,
        reopenable: true,
        seekable: true,
        exact_ranges: true,
        // The growing spool serves exact logical ranges, but those ranges are fed by one
        // sequential upstream transfer. Advertising the upstream's parallel range budget lets
        // one Parquet file monopolize shared row-group work without adding network bandwidth.
        useful_range_concurrency: SEQUENTIAL_SPOOL_USEFUL_RANGE_CONCURRENCY,
        minimum_chunk_bytes: upstream_capabilities.minimum_chunk_bytes,
        maximum_chunk_bytes: upstream_capabilities.maximum_chunk_bytes,
    };
    capabilities.validate()?;
    let source = Arc::new(GrowingSpoolByteSource {
        upstream: Arc::clone(&upstream),
        storage: Arc::clone(&storage),
        progress: Arc::clone(&progress),
        identity,
        capabilities,
        memory,
        tail_range_bytes: tail_range_bytes.min(size_bytes),
    });
    let owner: Arc<dyn std::any::Any + Send + Sync> = storage.clone();
    let retention = PayloadRetention::new(owner, size_bytes)?;
    let spool_path = storage.path().to_path_buf();
    let completion = Box::pin(async move {
        let result = download_into_growing_spool(
            upstream,
            size_bytes,
            storage.as_ref(),
            progress.as_ref(),
            cache_staged,
            cancellation,
        )
        .await;
        match &result {
            Ok(_) => progress.complete(size_bytes)?,
            Err(error) => progress.fail(error),
        }
        result
    });
    Ok(Some(GrowingSpoolSession {
        source,
        retention,
        completion,
        spool_path,
        cache_staged,
    }))
}

async fn download_into_growing_spool(
    upstream: Arc<dyn ByteSource>,
    size_bytes: u64,
    storage: &GrowingSpoolStorage,
    progress: &GrowingSpoolProgress,
    calculate_sha256: bool,
    cancellation: RunCancellation,
) -> Result<Option<String>> {
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(storage.path())
        .await
        .map_err(|error| CdfError::data(format!("open growing file spool: {error}")))?;
    let chunk_bytes = DEFAULT_TRANSFER_CHUNK_BYTES.clamp(
        upstream.capabilities().minimum_chunk_bytes,
        upstream.capabilities().maximum_chunk_bytes,
    );
    let mut input = upstream
        .open_sequential(SequentialReadRequest {
            preferred_chunk_bytes: chunk_bytes,
            cancellation: cancellation.clone(),
        })
        .await?;
    let mut transferred = 0_u64;
    let expected_checksum = upstream.identity().checksum.as_deref();
    let mut hasher = (expected_checksum.is_some() || calculate_sha256).then(Sha256::new);
    while let Some(chunk) = cancellation.await_or_cancel(input.try_next()).await? {
        cancellation.check()?;
        let length = u64::try_from(chunk.payload().len())
            .map_err(|_| CdfError::data("growing spool chunk exceeds u64"))?;
        transferred = transferred
            .checked_add(length)
            .ok_or_else(|| CdfError::data("growing spool byte count overflowed"))?;
        if transferred > size_bytes {
            return Err(CdfError::data(
                "growing spool exceeded its planned generation length",
            ));
        }
        output
            .write_all(chunk.payload())
            .await
            .map_err(|error| CdfError::data(format!("write growing file spool: {error}")))?;
        if let Some(hasher) = &mut hasher {
            hasher.update(chunk.payload());
        }
        progress.publish(transferred)?;
    }
    output
        .flush()
        .await
        .map_err(|error| CdfError::data(format!("flush growing file spool: {error}")))?;
    if transferred != size_bytes {
        return Err(CdfError::data(format!(
            "growing spool wrote {transferred} bytes for a planned {size_bytes}-byte generation"
        )));
    }
    let observed_sha256 = hasher.map(|hasher| format!("sha256:{}", hex::encode(hasher.finalize())));
    if let (Some(expected), Some(observed)) = (expected_checksum, observed_sha256.as_deref())
        && observed.strip_prefix("sha256:").unwrap_or(observed)
            != expected.strip_prefix("sha256:").unwrap_or(expected)
    {
        return Err(CdfError::data(
            "growing spool checksum does not match planned content identity",
        ));
    }
    cancellation.check()?;
    Ok(observed_sha256)
}

impl GrowingSpoolStorage {
    fn path(&self) -> &Path {
        self.file.path()
    }
}

impl GrowingSpoolProgress {
    fn publish(&self, written_bytes: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CdfError::internal("growing spool progress lock is poisoned"))?;
        if !matches!(state.terminal, GrowingSpoolTerminal::Running)
            || written_bytes < state.written_bytes
        {
            return Err(CdfError::internal(
                "growing spool published non-monotonic or terminal progress",
            ));
        }
        state.written_bytes = written_bytes;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn complete(&self, expected_bytes: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CdfError::internal("growing spool progress lock is poisoned"))?;
        let mismatch = state.written_bytes != expected_bytes;
        if mismatch {
            state.terminal = GrowingSpoolTerminal::Failed(format!(
                "growing spool completed at {} of {expected_bytes} bytes",
                state.written_bytes
            ));
        } else {
            state.terminal = GrowingSpoolTerminal::Complete;
        }
        drop(state);
        self.changed.notify_waiters();
        if mismatch {
            Err(CdfError::internal(
                "growing spool completion violated its planned byte count",
            ))
        } else {
            Ok(())
        }
    }

    fn fail(&self, error: &CdfError) {
        if let Ok(mut state) = self.state.lock() {
            state.terminal = GrowingSpoolTerminal::Failed(error.to_string());
        }
        self.changed.notify_waiters();
    }
}

impl ByteSource for GrowingSpoolByteSource {
    fn identity(&self) -> &ContentIdentity {
        &self.identity
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        &self.capabilities
    }

    fn supports_local_range_replay(&self) -> bool {
        true
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        let source = Arc::new(self.clone());
        Box::pin(async move {
            request.cancellation.check()?;
            if request.preferred_chunk_bytes < source.capabilities.minimum_chunk_bytes
                || request.preferred_chunk_bytes > source.capabilities.maximum_chunk_bytes
            {
                return Err(CdfError::contract(format!(
                    "growing spool sequential chunk target {} is outside {}..={} bytes",
                    request.preferred_chunk_bytes,
                    source.capabilities.minimum_chunk_bytes,
                    source.capabilities.maximum_chunk_bytes
                )));
            }
            let size_bytes = source.identity.size_bytes.unwrap_or_default();
            let state = (
                source,
                request.cancellation,
                0_u64,
                size_bytes,
                request.preferred_chunk_bytes,
            );
            Ok(Box::pin(stream::try_unfold(
                state,
                |(source, cancellation, offset, size_bytes, chunk_bytes)| async move {
                    cancellation.check()?;
                    if offset == size_bytes {
                        return Ok(None);
                    }
                    let length = (size_bytes - offset).min(chunk_bytes);
                    let chunk = source
                        .read_exact_range(ByteExtent::new(offset, length)?, cancellation.clone())
                        .await?;
                    Ok(Some((
                        chunk,
                        (
                            source,
                            cancellation,
                            offset + length,
                            size_bytes,
                            chunk_bytes,
                        ),
                    )))
                },
            )) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<AccountedBytes>> {
        Box::pin(async move {
            cancellation.check()?;
            let size_bytes = self.identity.size_bytes.unwrap_or_default();
            let end = extent
                .start
                .checked_add(extent.length)
                .ok_or_else(|| CdfError::contract("growing spool range overflowed"))?;
            if end > size_bytes {
                return Err(CdfError::data(format!(
                    "growing spool range {}..{end} exceeds generation length {size_bytes}",
                    extent.start
                )));
            }
            loop {
                let changed = self.progress.changed.notified();
                futures_util::pin_mut!(changed);
                changed.as_mut().enable();
                let decision = {
                    let state = self.progress.state.lock().map_err(|_| {
                        CdfError::internal("growing spool progress lock is poisoned")
                    })?;
                    if end <= state.written_bytes {
                        GrowingReadDecision::Local
                    } else {
                        match &state.terminal {
                            GrowingSpoolTerminal::Failed(error) => {
                                GrowingReadDecision::Failed(error.clone())
                            }
                            GrowingSpoolTerminal::Complete => GrowingReadDecision::Failed(
                                "growing spool completed before the requested range was visible"
                                    .to_owned(),
                            ),
                            GrowingSpoolTerminal::Running
                                if end == size_bytes && extent.length <= self.tail_range_bytes =>
                            {
                                GrowingReadDecision::GenerationBoundTail
                            }
                            GrowingSpoolTerminal::Running => GrowingReadDecision::Wait,
                        }
                    }
                };
                match decision {
                    GrowingReadDecision::Local => {
                        return self.read_local_range(extent, cancellation).await;
                    }
                    GrowingReadDecision::GenerationBoundTail => {
                        return self.upstream.read_exact_range(extent, cancellation).await;
                    }
                    GrowingReadDecision::Failed(error) => {
                        return Err(CdfError::data(format!(
                            "growing spool cannot satisfy range: {error}"
                        )));
                    }
                    GrowingReadDecision::Wait => {
                        let cancelled = cancellation.cancelled();
                        futures_util::pin_mut!(cancelled);
                        match futures_util::future::select(changed, cancelled).await {
                            futures_util::future::Either::Left(_) => {}
                            futures_util::future::Either::Right(_) => cancellation.check()?,
                        }
                    }
                }
            }
        })
    }
}

enum GrowingReadDecision {
    Local,
    GenerationBoundTail,
    Wait,
    Failed(String),
}

impl GrowingSpoolByteSource {
    async fn read_local_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> Result<AccountedBytes> {
        cancellation.check()?;
        let lease = reserve(
            Arc::clone(&self.memory),
            ReservationRequest::new(
                ConsumerKey::new("growing-spool-range", MemoryClass::Source)?,
                extent.length,
            )?,
        )
        .await?;
        let mut file = tokio::fs::File::open(self.storage.path())
            .await
            .map_err(|error| CdfError::data(format!("open growing spool range: {error}")))?;
        file.seek(SeekFrom::Start(extent.start))
            .await
            .map_err(|error| CdfError::data(format!("seek growing spool range: {error}")))?;
        let length = usize::try_from(extent.length)
            .map_err(|_| CdfError::data("growing spool range length exceeds usize"))?;
        let mut payload = vec![0_u8; length];
        file.read_exact(&mut payload)
            .await
            .map_err(|error| CdfError::data(format!("read growing spool range: {error}")))?;
        cancellation.check()?;
        AccountedBytes::new(Bytes::from(payload), lease)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
    };

    use cdf_memory::DeterministicMemoryCoordinator;
    use cdf_runtime::FixedSpillBudget;

    use super::*;

    struct GatedStrongSource {
        identity: ContentIdentity,
        capabilities: ByteSourceCapabilities,
        payload: Arc<Vec<u8>>,
        memory: Arc<dyn MemoryCoordinator>,
        continuation: Arc<tokio::sync::Semaphore>,
        range_reads: Arc<AtomicUsize>,
    }

    impl ByteSource for GatedStrongSource {
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
            let payload = Arc::clone(&self.payload);
            let memory = Arc::clone(&self.memory);
            let continuation = Arc::clone(&self.continuation);
            Box::pin(async move {
                request.cancellation.check()?;
                let state = (0_usize, payload, memory, continuation, request.cancellation);
                Ok(Box::pin(stream::try_unfold(
                    state,
                    |(offset, payload, memory, continuation, cancellation)| async move {
                        cancellation.check()?;
                        if offset == payload.len() {
                            return Ok(None);
                        }
                        if offset > 0 {
                            continuation
                                .acquire()
                                .await
                                .map_err(|_| CdfError::internal("test transfer gate closed"))?
                                .forget();
                        }
                        let end = (offset + 32).min(payload.len());
                        let bytes = Bytes::copy_from_slice(&payload[offset..end]);
                        let lease = reserve(
                            Arc::clone(&memory),
                            ReservationRequest::new(
                                ConsumerKey::new(
                                    "growing-spool-test-sequential",
                                    MemoryClass::Source,
                                )?,
                                u64::try_from(bytes.len())
                                    .map_err(|_| CdfError::data("test chunk exceeds u64"))?,
                            )?,
                        )
                        .await?;
                        Ok(Some((
                            AccountedBytes::new(bytes, lease)?,
                            (end, payload, memory, continuation, cancellation),
                        )))
                    },
                )) as AccountedByteStream)
            })
        }

        fn read_exact_range(
            &self,
            extent: ByteExtent,
            cancellation: RunCancellation,
        ) -> BoxFuture<'_, Result<AccountedBytes>> {
            Box::pin(async move {
                cancellation.check()?;
                self.range_reads.fetch_add(1, Ordering::Relaxed);
                let end = usize::try_from(extent.start + extent.length)
                    .map_err(|_| CdfError::data("test range exceeds usize"))?;
                let start = usize::try_from(extent.start)
                    .map_err(|_| CdfError::data("test range exceeds usize"))?;
                let bytes = Bytes::copy_from_slice(&self.payload[start..end]);
                let lease = reserve(
                    Arc::clone(&self.memory),
                    ReservationRequest::new(
                        ConsumerKey::new("growing-spool-test-range", MemoryClass::Source)?,
                        extent.length,
                    )?,
                )
                .await?;
                AccountedBytes::new(bytes, lease)
            })
        }
    }

    #[test]
    fn growing_spool_serves_prefix_and_bounded_tail_while_body_transfer_continues() {
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(1024 * 1024).unwrap());
        let payload = Arc::new((0_u8..96).collect::<Vec<_>>());
        let continuation = Arc::new(tokio::sync::Semaphore::new(0));
        let range_reads = Arc::new(AtomicUsize::new(0));
        let source: Arc<dyn ByteSource> = Arc::new(GatedStrongSource {
            identity: ContentIdentity {
                stable_id: "growing-spool-test".to_owned(),
                size_bytes: Some(payload.len() as u64),
                generation: Some("etag:growing-spool-test".to_owned()),
                checksum: None,
                strength: GenerationStrength::Strong,
            },
            capabilities: ByteSourceCapabilities {
                known_length: true,
                reopenable: true,
                seekable: true,
                exact_ranges: true,
                useful_range_concurrency: 4,
                minimum_chunk_bytes: 1,
                maximum_chunk_bytes: 64,
            },
            payload: Arc::clone(&payload),
            memory: Arc::clone(&memory),
            continuation: Arc::clone(&continuation),
            range_reads: Arc::clone(&range_reads),
        });
        crate::test_execution_services()
            .run_io({
                let memory = Arc::clone(&memory);
                let spill = Arc::clone(&spill);
                async move {
                    let session = start_growing_spool_with_tail_bytes(
                        GrowingSpoolRequest {
                            upstream: source,
                            size_bytes: payload.len() as u64,
                            maximum_spool_bytes: 1024 * 1024,
                            spill,
                            memory,
                            staging_root: None,
                            cancellation: RunCancellation::default(),
                        },
                        16,
                    )?
                    .ok_or_else(|| {
                        CdfError::internal("test growing spool unexpectedly declined admission")
                    })?;
                    assert_eq!(
                        session.source.capabilities().useful_range_concurrency,
                        SEQUENTIAL_SPOOL_USEFUL_RANGE_CONCURRENCY
                    );
                    let completion = tokio::spawn(session.completion);
                    let prefix = session
                        .source
                        .read_exact_range(ByteExtent::new(0, 8)?, RunCancellation::default())
                        .await?;
                    assert_eq!(prefix.payload(), &payload[0..8]);

                    let tail = session
                        .source
                        .read_exact_range(ByteExtent::new(88, 8)?, RunCancellation::default())
                        .await?;
                    assert_eq!(tail.payload(), &payload[88..96]);
                    assert_eq!(range_reads.load(Ordering::Relaxed), 1);

                    let (middle, near_tail) = {
                        let near_tail = session
                            .source
                            .read_exact_range(ByteExtent::new(80, 8)?, RunCancellation::default());
                        futures_util::pin_mut!(near_tail);
                        assert!(matches!(
                            futures_util::poll!(near_tail.as_mut()),
                            Poll::Pending
                        ));
                        let middle = session
                            .source
                            .read_exact_range(ByteExtent::new(40, 8)?, RunCancellation::default());
                        futures_util::pin_mut!(middle);
                        assert!(matches!(
                            futures_util::poll!(middle.as_mut()),
                            Poll::Pending
                        ));
                        continuation.add_permits(2);
                        (middle.await?, near_tail.await?)
                    };
                    assert_eq!(middle.payload(), &payload[40..48]);
                    assert_eq!(near_tail.payload(), &payload[80..88]);
                    assert_eq!(range_reads.load(Ordering::Relaxed), 1);
                    completion.await.map_err(|error| {
                        CdfError::internal(format!("join test spool: {error}"))
                    })??;
                    drop(prefix);
                    drop(tail);
                    drop(middle);
                    drop(near_tail);
                    drop(session.source);
                    drop(session.retention);
                    Ok::<_, CdfError>(())
                }
            })
            .unwrap();
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }

    #[test]
    fn growing_spool_rejects_weak_sources_before_disk_admission() {
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap());
        let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(FixedSpillBudget::new(1024).unwrap());
        let payload = Arc::new(vec![0_u8; 96]);
        let source: Arc<dyn ByteSource> = Arc::new(GatedStrongSource {
            identity: ContentIdentity {
                stable_id: "weak-growing-spool-test".to_owned(),
                size_bytes: Some(payload.len() as u64),
                generation: Some("unversioned-size:96".to_owned()),
                checksum: None,
                strength: GenerationStrength::Weak,
            },
            capabilities: ByteSourceCapabilities {
                known_length: true,
                reopenable: true,
                seekable: false,
                exact_ranges: false,
                useful_range_concurrency: 0,
                minimum_chunk_bytes: 1,
                maximum_chunk_bytes: 64,
            },
            payload,
            memory: Arc::clone(&memory),
            continuation: Arc::new(tokio::sync::Semaphore::new(0)),
            range_reads: Arc::new(AtomicUsize::new(0)),
        });

        let error = start_growing_spool_with_tail_bytes(
            GrowingSpoolRequest {
                upstream: source,
                size_bytes: 96,
                maximum_spool_bytes: 1,
                spill: Arc::clone(&spill),
                memory: Arc::clone(&memory),
                staging_root: None,
                cancellation: RunCancellation::default(),
            },
            16,
        )
        .err()
        .expect("weak source must not enter growing-spool overlap");

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert!(error.message.contains("strong"));
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }

    #[test]
    fn growing_spool_cancellation_releases_blocked_io_memory_disk_and_file() {
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(1024 * 1024).unwrap());
        let payload = Arc::new((0_u8..96).collect::<Vec<_>>());
        let continuation = Arc::new(tokio::sync::Semaphore::new(0));
        let source: Arc<dyn ByteSource> = Arc::new(GatedStrongSource {
            identity: ContentIdentity {
                stable_id: "growing-spool-cancel-test".to_owned(),
                size_bytes: Some(payload.len() as u64),
                generation: Some("etag:growing-spool-cancel-test".to_owned()),
                checksum: None,
                strength: GenerationStrength::Strong,
            },
            capabilities: ByteSourceCapabilities {
                known_length: true,
                reopenable: true,
                seekable: true,
                exact_ranges: true,
                useful_range_concurrency: 4,
                minimum_chunk_bytes: 1,
                maximum_chunk_bytes: 64,
            },
            payload: Arc::clone(&payload),
            memory: Arc::clone(&memory),
            continuation,
            range_reads: Arc::new(AtomicUsize::new(0)),
        });
        let cancellation = RunCancellation::default();
        let spool_path = crate::test_execution_services()
            .run_io({
                let memory = Arc::clone(&memory);
                let spill = Arc::clone(&spill);
                let cancellation = cancellation.clone();
                async move {
                    let session = start_growing_spool_with_tail_bytes(
                        GrowingSpoolRequest {
                            upstream: source,
                            size_bytes: payload.len() as u64,
                            maximum_spool_bytes: 1024 * 1024,
                            spill: Arc::clone(&spill),
                            memory,
                            staging_root: None,
                            cancellation: cancellation.clone(),
                        },
                        16,
                    )?
                    .ok_or_else(|| {
                        CdfError::internal("test growing spool unexpectedly declined admission")
                    })?;
                    let spool_path = session.spool_path.clone();
                    assert!(spool_path.exists());
                    assert_eq!(spill.snapshot().current_bytes, payload.len() as u64);

                    let completion = tokio::spawn(session.completion);
                    let prefix = session
                        .source
                        .read_exact_range(ByteExtent::new(0, 8)?, cancellation.clone())
                        .await?;
                    assert_eq!(prefix.payload(), &payload[0..8]);
                    drop(prefix);

                    let waiting_source = Arc::clone(&session.source);
                    let waiting_cancellation = cancellation.clone();
                    let waiting_reader = tokio::spawn(async move {
                        waiting_source
                            .read_exact_range(ByteExtent::new(40, 8)?, waiting_cancellation)
                            .await
                    });
                    tokio::task::yield_now().await;
                    assert!(!completion.is_finished());
                    assert!(!waiting_reader.is_finished());

                    cancellation.cancel();
                    let completion_error = completion
                        .await
                        .map_err(|error| CdfError::internal(format!("join test spool: {error}")))?
                        .expect_err("cancelled spool producer must fail");
                    assert!(completion_error.message.contains("cancelled"));
                    let reader_error = waiting_reader
                        .await
                        .map_err(|error| {
                            CdfError::internal(format!("join waiting spool reader: {error}"))
                        })?
                        .expect_err("cancelled blocked reader must fail");
                    assert!(reader_error.message.contains("cancelled"));
                    assert_eq!(spill.snapshot().current_bytes, payload.len() as u64);

                    drop(session.source);
                    drop(session.retention);
                    Ok::<_, CdfError>(spool_path)
                }
            })
            .unwrap();

        assert!(!spool_path.exists());
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }
}
