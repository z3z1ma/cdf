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
const DEFAULT_RESIDENCY_BYTES: u64 = 512 * 1024 * 1024;
const MINIMUM_RESIDENCY_BYTES: u64 = 2 * DEFAULT_TRANSFER_CHUNK_BYTES;

pub struct EvictingSpoolSession {
    pub source: Arc<dyn ByteSource>,
    pub retention: PayloadRetention,
    pub completion: BoxFuture<'static, Result<()>>,
    #[cfg(test)]
    spool_path: std::path::PathBuf,
}

struct EvictingSpoolStorage {
    file: tempfile::NamedTempFile,
    io: tokio::sync::RwLock<()>,
    capacity: u64,
    _reservation: SpillReservation,
}

#[derive(Clone)]
struct EvictingSpoolByteSource {
    upstream: Arc<dyn ByteSource>,
    storage: Arc<EvictingSpoolStorage>,
    progress: Arc<EvictingSpoolProgress>,
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    memory: Arc<dyn MemoryCoordinator>,
}

struct EvictingSpoolProgress {
    state: Mutex<EvictingSpoolState>,
    changed: tokio::sync::Notify,
}

struct EvictingSpoolState {
    transferred_bytes: u64,
    release_frontier: u64,
    terminal: EvictingSpoolTerminal,
}

enum EvictingSpoolTerminal {
    Running,
    Complete,
    Failed(String),
}

pub fn start_evicting_spool(
    upstream: Arc<dyn ByteSource>,
    size_bytes: u64,
    maximum_spool_bytes: u64,
    spill: Arc<dyn SpillBudgetCoordinator>,
    memory: Arc<dyn MemoryCoordinator>,
    cancellation: RunCancellation,
) -> Result<Option<EvictingSpoolSession>> {
    validate_evicting_source(upstream.as_ref(), size_bytes)?;
    let snapshot = spill.snapshot();
    let available = snapshot.budget_bytes.saturating_sub(snapshot.current_bytes);
    let capacity = size_bytes
        .saturating_sub(1)
        .min(maximum_spool_bytes)
        .min(DEFAULT_RESIDENCY_BYTES)
        .min(available);
    if capacity < MINIMUM_RESIDENCY_BYTES {
        return Ok(None);
    }
    start_evicting_spool_with_capacity(upstream, size_bytes, capacity, spill, memory, cancellation)
}

fn start_evicting_spool_with_capacity(
    upstream: Arc<dyn ByteSource>,
    size_bytes: u64,
    capacity: u64,
    spill: Arc<dyn SpillBudgetCoordinator>,
    memory: Arc<dyn MemoryCoordinator>,
    cancellation: RunCancellation,
) -> Result<Option<EvictingSpoolSession>> {
    if size_bytes == 0 || capacity == 0 || capacity >= size_bytes {
        return Err(CdfError::contract(
            "evicting spool requires a nonempty finite source and a smaller nonzero residency capacity",
        ));
    }
    validate_evicting_source(upstream.as_ref(), size_bytes)?;
    let identity = upstream.identity().clone();
    let upstream_capabilities = upstream.capabilities();
    let Some(reservation) = spill.try_reserve(capacity)? else {
        return Ok(None);
    };
    let file = tempfile::NamedTempFile::new()
        .map_err(|error| CdfError::data(format!("create evicting file spool: {error}")))?;
    file.as_file()
        .set_len(capacity)
        .map_err(|error| CdfError::data(format!("size evicting file spool: {error}")))?;
    let storage = Arc::new(EvictingSpoolStorage {
        file,
        io: tokio::sync::RwLock::new(()),
        capacity,
        _reservation: reservation,
    });
    let progress = Arc::new(EvictingSpoolProgress {
        state: Mutex::new(EvictingSpoolState {
            transferred_bytes: 0,
            release_frontier: 0,
            terminal: EvictingSpoolTerminal::Running,
        }),
        changed: tokio::sync::Notify::new(),
    });
    let capabilities = ByteSourceCapabilities {
        known_length: true,
        reopenable: false,
        seekable: true,
        exact_ranges: true,
        useful_range_concurrency: upstream_capabilities.useful_range_concurrency.max(1),
        minimum_chunk_bytes: upstream_capabilities.minimum_chunk_bytes,
        maximum_chunk_bytes: upstream_capabilities.maximum_chunk_bytes,
    };
    capabilities.validate()?;
    let source = Arc::new(EvictingSpoolByteSource {
        upstream: Arc::clone(&upstream),
        storage: Arc::clone(&storage),
        progress: Arc::clone(&progress),
        identity,
        capabilities,
        memory,
    });
    let owner: Arc<dyn std::any::Any + Send + Sync> = storage.clone();
    let retention = PayloadRetention::new(owner, capacity)?;
    #[cfg(test)]
    let spool_path = storage.path().to_path_buf();
    let completion = Box::pin(async move {
        let result = download_into_evicting_spool(
            upstream,
            size_bytes,
            storage.as_ref(),
            progress.as_ref(),
            cancellation,
        )
        .await;
        match &result {
            Ok(()) => progress.complete(size_bytes)?,
            Err(error) => progress.fail(error),
        }
        result
    });
    Ok(Some(EvictingSpoolSession {
        source,
        retention,
        completion,
        #[cfg(test)]
        spool_path,
    }))
}

fn validate_evicting_source(source: &dyn ByteSource, size_bytes: u64) -> Result<()> {
    source.identity().validate()?;
    source.capabilities().validate()?;
    if source.identity().size_bytes != Some(size_bytes)
        || source.identity().strength == GenerationStrength::Weak
        || !source.capabilities().exact_ranges
    {
        return Err(CdfError::contract(
            "evicting spool requires one strong, known-length generation with enforceable exact ranges",
        ));
    }
    Ok(())
}

async fn download_into_evicting_spool(
    upstream: Arc<dyn ByteSource>,
    size_bytes: u64,
    storage: &EvictingSpoolStorage,
    progress: &EvictingSpoolProgress,
    cancellation: RunCancellation,
) -> Result<()> {
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .open(storage.path())
        .await
        .map_err(|error| CdfError::data(format!("open evicting file spool: {error}")))?;
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
    let expected_checksum = upstream.identity().checksum.as_deref();
    let mut hasher = expected_checksum.map(|_| Sha256::new());
    let mut transferred = 0_u64;
    while let Some(chunk) = cancellation.await_or_cancel(input.try_next()).await? {
        cancellation.check()?;
        if let Some(hasher) = &mut hasher {
            hasher.update(chunk.payload());
        }
        let mut cursor = 0_usize;
        while cursor < chunk.payload().len() {
            let action = progress
                .next_write_action(
                    transferred,
                    chunk.payload().len() - cursor,
                    storage.capacity,
                    cancellation.clone(),
                )
                .await?;
            let length = usize::try_from(action.length)
                .map_err(|_| CdfError::data("evicting spool write length exceeds usize"))?;
            if action.write {
                let _guard = storage.io.write().await;
                output
                    .seek(SeekFrom::Start(transferred % storage.capacity))
                    .await
                    .map_err(|error| {
                        CdfError::data(format!("seek evicting file spool: {error}"))
                    })?;
                output
                    .write_all(&chunk.payload()[cursor..cursor + length])
                    .await
                    .map_err(|error| {
                        CdfError::data(format!("write evicting file spool: {error}"))
                    })?;
            }
            cursor += length;
            transferred = transferred
                .checked_add(action.length)
                .ok_or_else(|| CdfError::data("evicting spool byte count overflowed"))?;
            if transferred > size_bytes {
                return Err(CdfError::data(
                    "evicting spool exceeded its planned generation length",
                ));
            }
            progress.publish(transferred)?;
        }
    }
    output
        .flush()
        .await
        .map_err(|error| CdfError::data(format!("flush evicting file spool: {error}")))?;
    if transferred != size_bytes {
        return Err(CdfError::data(format!(
            "evicting spool transferred {transferred} bytes for a planned {size_bytes}-byte generation"
        )));
    }
    if let (Some(expected), Some(hasher)) = (expected_checksum, hasher) {
        let observed = hex::encode(hasher.finalize());
        if observed != expected.strip_prefix("sha256:").unwrap_or(expected) {
            return Err(CdfError::data(
                "evicting spool checksum does not match planned content identity",
            ));
        }
    }
    cancellation.check()
}

struct WriteAction {
    length: u64,
    write: bool,
}

impl EvictingSpoolStorage {
    fn path(&self) -> &Path {
        self.file.path()
    }
}

impl EvictingSpoolProgress {
    async fn next_write_action(
        &self,
        transferred: u64,
        remaining: usize,
        capacity: u64,
        cancellation: RunCancellation,
    ) -> Result<WriteAction> {
        let remaining = u64::try_from(remaining)
            .map_err(|_| CdfError::data("evicting spool chunk length exceeds u64"))?;
        loop {
            let changed = self.changed.notified();
            futures_util::pin_mut!(changed);
            changed.as_mut().enable();
            let action =
                {
                    let state = self.state.lock().map_err(|_| {
                        CdfError::internal("evicting spool progress lock is poisoned")
                    })?;
                    if state.transferred_bytes != transferred
                        || !matches!(state.terminal, EvictingSpoolTerminal::Running)
                    {
                        return Err(CdfError::internal(
                            "evicting spool producer observed inconsistent progress",
                        ));
                    }
                    if transferred < state.release_frontier {
                        Some(WriteAction {
                            length: remaining.min(state.release_frontier - transferred),
                            write: false,
                        })
                    } else {
                        let writable_end = state
                            .release_frontier
                            .checked_add(capacity)
                            .ok_or_else(|| {
                                CdfError::data("evicting spool residency frontier overflowed")
                            })?;
                        let available = writable_end.saturating_sub(transferred);
                        (available > 0).then(|| WriteAction {
                            length: remaining
                                .min(available)
                                .min(capacity - (transferred % capacity)),
                            write: true,
                        })
                    }
                };
            if let Some(action) = action {
                return Ok(action);
            }
            let cancelled = cancellation.cancelled();
            futures_util::pin_mut!(cancelled);
            match futures_util::future::select(changed, cancelled).await {
                futures_util::future::Either::Left(_) => {}
                futures_util::future::Either::Right(_) => cancellation.check()?,
            }
        }
    }

    fn publish(&self, transferred_bytes: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CdfError::internal("evicting spool progress lock is poisoned"))?;
        if !matches!(state.terminal, EvictingSpoolTerminal::Running)
            || transferred_bytes < state.transferred_bytes
        {
            return Err(CdfError::internal(
                "evicting spool published non-monotonic or terminal progress",
            ));
        }
        state.transferred_bytes = transferred_bytes;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn release_before(&self, frontier: u64, size_bytes: u64) -> Result<()> {
        if frontier > size_bytes {
            return Err(CdfError::internal(format!(
                "evicting spool release frontier {frontier} exceeds generation length {size_bytes}"
            )));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| CdfError::internal("evicting spool progress lock is poisoned"))?;
        if frontier < state.release_frontier {
            return Err(CdfError::internal(format!(
                "evicting spool release frontier decreased from {} to {frontier}",
                state.release_frontier
            )));
        }
        state.release_frontier = frontier;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn complete(&self, expected_bytes: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CdfError::internal("evicting spool progress lock is poisoned"))?;
        let mismatch = state.transferred_bytes != expected_bytes;
        if mismatch {
            state.terminal = EvictingSpoolTerminal::Failed(format!(
                "evicting spool completed at {} of {expected_bytes} bytes",
                state.transferred_bytes
            ));
        } else {
            state.terminal = EvictingSpoolTerminal::Complete;
        }
        drop(state);
        self.changed.notify_waiters();
        if mismatch {
            Err(CdfError::internal(
                "evicting spool completion violated its planned byte count",
            ))
        } else {
            Ok(())
        }
    }

    fn fail(&self, error: &CdfError) {
        if let Ok(mut state) = self.state.lock() {
            state.terminal = EvictingSpoolTerminal::Failed(error.to_string());
        }
        self.changed.notify_waiters();
    }
}

impl ByteSource for EvictingSpoolByteSource {
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
        let source = Arc::new(self.clone());
        Box::pin(async move {
            request.cancellation.check()?;
            if request.preferred_chunk_bytes < source.capabilities.minimum_chunk_bytes
                || request.preferred_chunk_bytes > source.capabilities.maximum_chunk_bytes
            {
                return Err(CdfError::contract(format!(
                    "evicting spool sequential chunk target {} is outside {}..={} bytes",
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
                .ok_or_else(|| CdfError::contract("evicting spool range overflowed"))?;
            if end > size_bytes {
                return Err(CdfError::data(format!(
                    "evicting spool range {}..{end} exceeds generation length {size_bytes}",
                    extent.start
                )));
            }
            loop {
                let changed = self.progress.changed.notified();
                futures_util::pin_mut!(changed);
                changed.as_mut().enable();
                let decision = {
                    let state = self.progress.state.lock().map_err(|_| {
                        CdfError::internal("evicting spool progress lock is poisoned")
                    })?;
                    if extent.start < state.release_frontier {
                        EvictingReadDecision::Released(state.release_frontier)
                    } else if end <= state.transferred_bytes {
                        EvictingReadDecision::Local
                    } else if extent.length > self.storage.capacity
                        || end > state.release_frontier.saturating_add(self.storage.capacity)
                    {
                        EvictingReadDecision::GenerationBoundRange
                    } else {
                        match &state.terminal {
                            EvictingSpoolTerminal::Failed(error) => {
                                EvictingReadDecision::Failed(error.clone())
                            }
                            EvictingSpoolTerminal::Complete => EvictingReadDecision::Failed(
                                "evicting spool completed before the requested range was visible"
                                    .to_owned(),
                            ),
                            EvictingSpoolTerminal::Running => EvictingReadDecision::Wait,
                        }
                    }
                };
                match decision {
                    EvictingReadDecision::Local => {
                        return self.read_local_range(extent, cancellation).await;
                    }
                    EvictingReadDecision::GenerationBoundRange => {
                        return self.upstream.read_exact_range(extent, cancellation).await;
                    }
                    EvictingReadDecision::Released(frontier) => {
                        return Err(CdfError::internal(format!(
                            "evicting spool requested range {}..{end} below released frontier {frontier}",
                            extent.start
                        )));
                    }
                    EvictingReadDecision::Failed(error) => {
                        return Err(CdfError::data(format!(
                            "evicting spool cannot satisfy range: {error}"
                        )));
                    }
                    EvictingReadDecision::Wait => {
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

    fn release_before(&self, frontier: u64) -> Result<()> {
        self.progress
            .release_before(frontier, self.identity.size_bytes.unwrap_or_default())
    }
}

enum EvictingReadDecision {
    Local,
    GenerationBoundRange,
    Wait,
    Released(u64),
    Failed(String),
}

impl EvictingSpoolByteSource {
    async fn read_local_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> Result<AccountedBytes> {
        cancellation.check()?;
        let lease = reserve(
            Arc::clone(&self.memory),
            ReservationRequest::new(
                ConsumerKey::new("evicting-spool-range", MemoryClass::Source)?,
                extent.length,
            )?,
        )
        .await?;
        let _guard = self.storage.io.read().await;
        {
            let state = self
                .progress
                .state
                .lock()
                .map_err(|_| CdfError::internal("evicting spool progress lock is poisoned"))?;
            let end = extent.start + extent.length;
            if extent.start < state.release_frontier || end > state.transferred_bytes {
                return Err(CdfError::internal(
                    "evicting spool residency changed before a local range read acquired storage",
                ));
            }
        }
        let mut file = tokio::fs::File::open(self.storage.path())
            .await
            .map_err(|error| CdfError::data(format!("open evicting spool range: {error}")))?;
        let length = usize::try_from(extent.length)
            .map_err(|_| CdfError::data("evicting spool range length exceeds usize"))?;
        let first_length = usize::try_from(
            extent
                .length
                .min(self.storage.capacity - (extent.start % self.storage.capacity)),
        )
        .map_err(|_| CdfError::data("evicting spool first range length exceeds usize"))?;
        let mut payload = vec![0_u8; length];
        file.seek(SeekFrom::Start(extent.start % self.storage.capacity))
            .await
            .map_err(|error| CdfError::data(format!("seek evicting spool range: {error}")))?;
        file.read_exact(&mut payload[..first_length])
            .await
            .map_err(|error| CdfError::data(format!("read evicting spool range: {error}")))?;
        if first_length < length {
            file.seek(SeekFrom::Start(0)).await.map_err(|error| {
                CdfError::data(format!("seek wrapped evicting spool range: {error}"))
            })?;
            file.read_exact(&mut payload[first_length..])
                .await
                .map_err(|error| {
                    CdfError::data(format!("read wrapped evicting spool range: {error}"))
                })?;
        }
        cancellation.check()?;
        AccountedBytes::new(Bytes::from(payload), lease)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use cdf_memory::DeterministicMemoryCoordinator;
    use cdf_runtime::FixedSpillBudget;

    use super::*;

    struct MemoryStrongSource {
        identity: ContentIdentity,
        capabilities: ByteSourceCapabilities,
        payload: Arc<Vec<u8>>,
        memory: Arc<dyn MemoryCoordinator>,
    }

    impl ByteSource for MemoryStrongSource {
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
            Box::pin(async move {
                request.cancellation.check()?;
                let state = (0_usize, payload, memory, request.cancellation);
                Ok(Box::pin(stream::try_unfold(
                    state,
                    |(offset, payload, memory, cancellation)| async move {
                        cancellation.check()?;
                        if offset == payload.len() {
                            return Ok(None);
                        }
                        let end = (offset + 32).min(payload.len());
                        let bytes = Bytes::copy_from_slice(&payload[offset..end]);
                        let lease = reserve(
                            Arc::clone(&memory),
                            ReservationRequest::new(
                                ConsumerKey::new(
                                    "evicting-spool-test-sequential",
                                    MemoryClass::Source,
                                )?,
                                u64::try_from(bytes.len())
                                    .map_err(|_| CdfError::data("test chunk exceeds u64"))?,
                            )?,
                        )
                        .await?;
                        Ok(Some((
                            AccountedBytes::new(bytes, lease)?,
                            (end, payload, memory, cancellation),
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
                let start = usize::try_from(extent.start)
                    .map_err(|_| CdfError::data("test range start exceeds usize"))?;
                let end = usize::try_from(extent.start + extent.length)
                    .map_err(|_| CdfError::data("test range end exceeds usize"))?;
                let lease = reserve(
                    Arc::clone(&self.memory),
                    ReservationRequest::new(
                        ConsumerKey::new("evicting-spool-test-range", MemoryClass::Source)?,
                        extent.length,
                    )?,
                )
                .await?;
                AccountedBytes::new(Bytes::copy_from_slice(&self.payload[start..end]), lease)
            })
        }
    }

    #[test]
    fn codec_frontiers_bound_disk_and_release_a_sequential_producer() {
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(1024 * 1024).unwrap());
        let payload = Arc::new((0_u8..192).collect::<Vec<_>>());
        let source: Arc<dyn ByteSource> = Arc::new(MemoryStrongSource {
            identity: ContentIdentity {
                stable_id: "evicting-spool-test".to_owned(),
                size_bytes: Some(payload.len() as u64),
                generation: Some("etag:evicting-spool-test".to_owned()),
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
        });

        let spool_path = crate::test_execution_services()
            .run_io({
                let memory = Arc::clone(&memory);
                let spill = Arc::clone(&spill);
                async move {
                    let session = start_evicting_spool_with_capacity(
                        source,
                        payload.len() as u64,
                        64,
                        Arc::clone(&spill),
                        memory,
                        RunCancellation::default(),
                    )?
                    .ok_or_else(|| {
                        CdfError::internal("test evicting spool unexpectedly declined admission")
                    })?;
                    let spool_path = session.spool_path.clone();
                    assert!(spool_path.exists());
                    assert_eq!(session.retention.bytes(), 64);
                    assert_eq!(spill.snapshot().current_bytes, 64);

                    let completion = tokio::spawn(session.completion);
                    let first = session
                        .source
                        .read_exact_range(ByteExtent::new(0, 8)?, RunCancellation::default())
                        .await?;
                    assert_eq!(first.payload(), &payload[0..8]);
                    drop(first);
                    tokio::task::yield_now().await;
                    assert!(!completion.is_finished());

                    session.source.release_before(64)?;
                    let second = session
                        .source
                        .read_exact_range(ByteExtent::new(64, 8)?, RunCancellation::default())
                        .await?;
                    assert_eq!(second.payload(), &payload[64..72]);
                    drop(second);
                    assert!(session.source.release_before(63).is_err());
                    assert!(session.source.release_before(193).is_err());
                    tokio::task::yield_now().await;
                    assert!(!completion.is_finished());

                    session.source.release_before(128)?;
                    completion.await.map_err(|error| {
                        CdfError::internal(format!("join test evicting spool: {error}"))
                    })??;
                    let third = session
                        .source
                        .read_exact_range(ByteExtent::new(128, 8)?, RunCancellation::default())
                        .await?;
                    assert_eq!(third.payload(), &payload[128..136]);
                    drop(third);
                    assert_eq!(spill.snapshot().peak_bytes, 64);

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
