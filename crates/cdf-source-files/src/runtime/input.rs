use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use arrow_schema::{Schema, SchemaRef};
use cdf_kernel::{
    BoxFuture, CdfError, CompiledScanIntent, PayloadRetention, ResourceId, Result, SchemaHash,
    SourceReadMode,
};
use cdf_memory::{ConsumerKey, MemoryClass};
use cdf_object_access::{
    FileChecksum, FileIdentityMetadata, FilePayloadCache, FilePayloadCacheKey,
    FilePayloadCacheLookup, LocalByteSource, open_identity_preserving_local_source,
    start_evicting_spool, start_growing_spool,
};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ByteTransformId,
    ContentIdentity, ExecutionServices, FormatDriver, GenerationStrength, ObservedByteSource,
    PreparedSourcePayload, PreparedSourcePayloadKey, ReadOptions, SequentialReadRequest,
    SourceContentDigest, SourceDriverId, SourceIoObserver, TransformSourceConfig,
    TransformedByteSource,
};
use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use super::{
    FILE_SOURCE_BLOCKING_LANE_ID, FileRuntimeDependencies,
    model::{ResolvedFileMatch, ResolvedFileOpen},
};

#[derive(Clone, Default)]
pub(super) struct PhysicalSchemaAuthority {
    pub(super) hash: Option<SchemaHash>,
    pub(super) schema: Option<SchemaRef>,
}

pub(super) enum PreparedFileInput {
    Source(Arc<dyn ByteSource>),
    SpoolSource {
        source: Arc<dyn ByteSource>,
        size_bytes: Option<u64>,
    },
}

pub(super) struct PreparedInput {
    pub(super) input: PreparedFileInput,
    pub(super) source_io: SourceIoObserver,
    pub(super) extraction_content_hash: Option<SourceContentDigest>,
    pub(super) hash_sweep_source: Option<Arc<dyn ByteSource>>,
    pub(super) payload_retention: Option<PayloadRetention>,
    pub(super) payload_cache_key: Option<FilePayloadCacheKey>,
}

pub(super) struct ReadyFileInput {
    pub(super) source: Arc<dyn ByteSource>,
    pub(super) payload_retention: Option<PayloadRetention>,
    pub(super) source_completion: Option<BoxFuture<'static, Result<()>>>,
    pub(super) post_decode_completion: Option<BoxFuture<'static, Result<()>>>,
}

pub(super) fn retains_sequential_discovery_payload(
    descriptor: &cdf_runtime::FormatDriverDescriptor,
    discovery_kind: cdf_runtime::FormatDiscoveryKind,
) -> bool {
    descriptor.source_access == cdf_runtime::FormatSourceAccess::Sequential
        && matches!(
            discovery_kind,
            cdf_runtime::FormatDiscoveryKind::BoundedContent
                | cdf_runtime::FormatDiscoveryKind::FullContent
        )
}

pub(super) struct PreparedFilePayload {
    pub(super) source: Arc<dyn ByteSource>,
    pub(super) source_content_digest: Option<SourceContentDigest>,
}

pub(super) struct AccountedSpool {
    file: tempfile::NamedTempFile,
    _reservation: cdf_runtime::SpillReservation,
    bytes: u64,
    sha256: Option<String>,
    cache_staged: bool,
}

pub(super) struct SequentialPayloadCapture {
    source: Arc<CapturingSequentialByteSource>,
    state: Arc<tokio::sync::Mutex<SequentialCaptureState>>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
}

pub(super) struct SequentialCaptureState {
    upstream: Option<Arc<tokio::sync::Mutex<AccountedByteStream>>>,
    output: Option<tokio::fs::File>,
    spool_file: Option<tempfile::NamedTempFile>,
    reservation: Option<cdf_runtime::SpillReservation>,
    captured_bytes: u64,
    opened: bool,
    maximum_spool_bytes: u64,
}

pub(super) struct CapturingSequentialByteSource {
    upstream: Arc<dyn ByteSource>,
    capabilities: ByteSourceCapabilities,
    state: Arc<tokio::sync::Mutex<SequentialCaptureState>>,
}

pub(super) struct ReplayThenContinueByteSource {
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    state: Arc<Mutex<Option<ReplayContinuation>>>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
}

pub(super) struct ReplayContinuation {
    spool_path: PathBuf,
    continuation: Arc<tokio::sync::Mutex<AccountedByteStream>>,
}

#[derive(Clone)]
pub(super) struct HashingByteSource {
    inner: Arc<dyn ByteSource>,
    observation: SourceContentDigest,
}

impl HashingByteSource {
    pub(super) fn new(inner: Arc<dyn ByteSource>, observation: SourceContentDigest) -> Self {
        Self { inner, observation }
    }
}

impl ByteSource for HashingByteSource {
    fn identity(&self) -> &cdf_runtime::ContentIdentity {
        self.inner.identity()
    }

    fn capabilities(&self) -> &cdf_runtime::ByteSourceCapabilities {
        self.inner.capabilities()
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        Box::pin(async move {
            let input = self.inner.open_sequential(request).await?;
            let state = (input, Sha256::new(), self.observation.clone());
            Ok(Box::pin(futures_util::stream::try_unfold(
                state,
                |(mut input, mut hasher, observation)| async move {
                    match input.try_next().await? {
                        Some(chunk) => {
                            hasher.update(chunk.payload());
                            Ok(Some((chunk, (input, hasher, observation))))
                        }
                        None => {
                            observation.record(format!("sha256:{:x}", hasher.finalize()))?;
                            Ok(None)
                        }
                    }
                },
            )) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: cdf_runtime::RunCancellation,
    ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
        self.inner.read_exact_range(extent, cancellation)
    }

    fn release_before(&self, frontier: u64) -> Result<()> {
        self.inner.release_before(frontier)
    }
}

impl AccountedSpool {
    pub(super) fn path(&self) -> &Path {
        self.file.path()
    }

    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(super) fn sha256(&self) -> Option<&str> {
        self.sha256.as_deref()
    }
}

pub(super) fn retain_spool(spool: &Arc<AccountedSpool>, bytes: u64) -> Result<PayloadRetention> {
    let owner: Arc<dyn std::any::Any + Send + Sync> = spool.clone();
    PayloadRetention::new(owner, bytes)
}

pub(super) fn prepared_file_payload(
    source: Arc<dyn ByteSource>,
    retention: PayloadRetention,
    source_content_digest: Option<SourceContentDigest>,
) -> Result<PreparedSourcePayload> {
    source.identity().validate()?;
    source.capabilities().validate()?;
    Ok(PreparedSourcePayload::new(
        PreparedFilePayload {
            source,
            source_content_digest,
        },
        retention,
    ))
}

impl SequentialPayloadCapture {
    pub(super) async fn new(
        upstream: Arc<dyn ByteSource>,
        dependencies: &FileRuntimeDependencies,
    ) -> Result<Self> {
        let mut reservation = dependencies
            .execution()
            .spill()
            .try_reserve(1)?
            .ok_or_else(|| {
                let snapshot = dependencies.execution().spill().snapshot();
                CdfError::data(format!(
                    "retained discovery window requires spill capacity but {} of {} bytes are already reserved; raise the spill budget or reduce discovery concurrency",
                    snapshot.current_bytes, snapshot.budget_bytes
                ))
            })?;
        if reservation.bytes() == 0 && !reservation.try_grow(1)? {
            return Err(CdfError::data(
                "retained discovery window could not reserve its initial spill byte",
            ));
        }
        let spool_file = tempfile::NamedTempFile::new().map_err(|error| {
            CdfError::data(format!("create retained discovery window: {error}"))
        })?;
        let output = tokio::fs::File::create(spool_file.path())
            .await
            .map_err(|error| CdfError::data(format!("open retained discovery window: {error}")))?;
        let state = Arc::new(tokio::sync::Mutex::new(SequentialCaptureState {
            upstream: None,
            output: Some(output),
            spool_file: Some(spool_file),
            reservation: Some(reservation),
            captured_bytes: 0,
            opened: false,
            maximum_spool_bytes: dependencies.max_spool_bytes(),
        }));
        let mut capabilities = upstream.capabilities().clone();
        capabilities.reopenable = false;
        capabilities.validate()?;
        Ok(Self {
            source: Arc::new(CapturingSequentialByteSource {
                upstream,
                capabilities,
                state: Arc::clone(&state),
            }),
            state,
            memory: dependencies.execution().memory(),
        })
    }

    pub(super) fn discovery_source(&self) -> Arc<dyn ByteSource> {
        Arc::clone(&self.source) as Arc<dyn ByteSource>
    }

    pub(super) async fn finish(
        self,
        source_content_digest: Option<SourceContentDigest>,
    ) -> Result<PreparedSourcePayload> {
        let mut state = self.state.lock().await;
        if !state.opened {
            return Err(CdfError::internal(
                "format discovery did not open its retained sequential source",
            ));
        }
        let mut output = state.output.take().ok_or_else(|| {
            CdfError::internal("retained discovery window output was already finalized")
        })?;
        output
            .flush()
            .await
            .map_err(|error| CdfError::data(format!("flush retained discovery window: {error}")))?;
        drop(output);
        let captured_bytes = state.captured_bytes;
        if captured_bytes == 0 {
            return Err(CdfError::data(
                "format discovery retained no source bytes for execution",
            ));
        }
        let continuation = state.upstream.take().ok_or_else(|| {
            CdfError::internal("retained discovery window omitted its live continuation")
        })?;
        let spool_file = state.spool_file.take().ok_or_else(|| {
            CdfError::internal("retained discovery window omitted its spool file")
        })?;
        let reservation = state.reservation.take().ok_or_else(|| {
            CdfError::internal("retained discovery window omitted its spill reservation")
        })?;
        drop(state);

        let spool = Arc::new(AccountedSpool {
            file: spool_file,
            _reservation: reservation,
            bytes: captured_bytes,
            sha256: None,
            cache_staged: false,
        });
        let upstream_capabilities = self.source.capabilities();
        let capabilities = ByteSourceCapabilities {
            known_length: upstream_capabilities.known_length,
            reopenable: false,
            seekable: false,
            exact_ranges: false,
            useful_range_concurrency: 0,
            minimum_chunk_bytes: upstream_capabilities.minimum_chunk_bytes,
            maximum_chunk_bytes: upstream_capabilities.maximum_chunk_bytes,
        };
        capabilities.validate()?;
        let replay: Arc<dyn ByteSource> = Arc::new(ReplayThenContinueByteSource {
            identity: self.source.identity().clone(),
            capabilities,
            state: Arc::new(Mutex::new(Some(ReplayContinuation {
                spool_path: spool.path().to_path_buf(),
                continuation,
            }))),
            memory: self.memory,
        });
        prepared_file_payload(
            replay,
            retain_spool(&spool, captured_bytes)?,
            source_content_digest,
        )
    }
}

impl ByteSource for CapturingSequentialByteSource {
    fn identity(&self) -> &ContentIdentity {
        self.upstream.identity()
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        &self.capabilities
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        Box::pin(async move {
            {
                let mut state = self.state.lock().await;
                if state.opened {
                    return Err(CdfError::contract(
                        "retained discovery source may be opened only once",
                    ));
                }
                // Claim the single invocation before crossing the source boundary. A
                // duplicate open must not contact the transport and then fail locally.
                state.opened = true;
            }
            let input = self.upstream.open_sequential(request).await?;
            {
                let mut state = self.state.lock().await;
                state.upstream = Some(Arc::new(tokio::sync::Mutex::new(input)));
            }
            let state = Arc::clone(&self.state);
            Ok(Box::pin(futures_util::stream::try_unfold(
                state,
                |state| async move {
                    let upstream = {
                        let state_guard = state.lock().await;
                        Arc::clone(state_guard.upstream.as_ref().ok_or_else(|| {
                            CdfError::internal("retained discovery source lost its upstream stream")
                        })?)
                    };
                    let next = upstream.lock().await.try_next().await?;
                    let Some(chunk) = next else {
                        return Ok(None);
                    };
                    let chunk_bytes = u64::try_from(chunk.payload().len())
                        .map_err(|_| CdfError::data("retained discovery chunk exceeds u64"))?;
                    let mut state_guard = state.lock().await;
                    let next_bytes = state_guard
                        .captured_bytes
                        .checked_add(chunk_bytes)
                        .ok_or_else(|| {
                            CdfError::data("retained discovery byte count overflowed")
                        })?;
                    if next_bytes > state_guard.maximum_spool_bytes {
                        return Err(CdfError::data(
                            "retained discovery window exceeded the configured spool budget",
                        ));
                    }
                    let reservation = state_guard.reservation.as_mut().ok_or_else(|| {
                        CdfError::internal("retained discovery spill reservation was finalized")
                    })?;
                    if next_bytes > reservation.bytes()
                        && !reservation.try_grow(next_bytes - reservation.bytes())?
                    {
                        return Err(CdfError::data(
                            "retained discovery window exhausted the shared spill budget",
                        ));
                    }
                    state_guard
                        .output
                        .as_mut()
                        .ok_or_else(|| {
                            CdfError::internal("retained discovery output was finalized")
                        })?
                        .write_all(chunk.payload())
                        .await
                        .map_err(|error| {
                            CdfError::data(format!("write retained discovery window: {error}"))
                        })?;
                    state_guard.captured_bytes = next_bytes;
                    drop(state_guard);
                    Ok(Some((chunk, state)))
                },
            )) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: cdf_runtime::RunCancellation,
    ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
        self.upstream.read_exact_range(extent, cancellation)
    }
}

impl ByteSource for ReplayThenContinueByteSource {
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
            let continuation = self
                .state
                .lock()
                .map_err(|_| CdfError::internal("retained payload state was poisoned"))?
                .take()
                .ok_or_else(|| {
                    CdfError::contract("retained source payload may be consumed only once")
                })?;
            let replay_source =
                LocalByteSource::open(&continuation.spool_path, Arc::clone(&self.memory))?;
            let replay_chunk_bytes = request.preferred_chunk_bytes.clamp(
                replay_source.capabilities().minimum_chunk_bytes,
                replay_source.capabilities().maximum_chunk_bytes,
            );
            let replay = replay_source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: replay_chunk_bytes,
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let state = (
                replay,
                continuation.continuation,
                false,
                request.cancellation,
            );
            Ok(Box::pin(futures_util::stream::try_unfold(
                state,
                |(mut replay, continuation, replay_done, cancellation)| async move {
                    cancellation.check()?;
                    if !replay_done && let Some(chunk) = replay.try_next().await? {
                        return Ok(Some((chunk, (replay, continuation, false, cancellation))));
                    }
                    let next = continuation.lock().await.try_next().await?;
                    Ok(next.map(|chunk| (chunk, (replay, continuation, true, cancellation))))
                },
            )) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        _extent: ByteExtent,
        _cancellation: cdf_runtime::RunCancellation,
    ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
        Box::pin(async {
            Err(CdfError::contract(
                "retained sequential payload does not support independent ranges",
            ))
        })
    }
}

pub(super) struct PreparedFilePartition {
    pub(super) resolved: ResolvedFileMatch,
    pub(super) input: PreparedFileInput,
    pub(super) scan_intent: CompiledScanIntent,
    pub(super) options: ReadOptions,
    pub(super) admission_schema: SchemaRef,
    pub(super) physical_schema_authority: PhysicalSchemaAuthority,
    pub(super) canonical_format_options: serde_json::Value,
    pub(super) driver: Arc<dyn FormatDriver>,
    pub(super) source_io: SourceIoObserver,
    pub(super) extraction_content_hash: Option<SourceContentDigest>,
    pub(super) hash_sweep_source: Option<Arc<dyn ByteSource>>,
    pub(super) payload_retention: Option<PayloadRetention>,
    pub(super) payload_cache_key: Option<FilePayloadCacheKey>,
    pub(super) spool_mode: crate::FileSpoolMode,
}

pub(super) struct PreparedFilePayloadKeyInput<'a> {
    pub(super) resource_id: &'a ResourceId,
    pub(super) location: &'a str,
    pub(super) size_bytes: u64,
    pub(super) source_generation: Option<&'a str>,
    pub(super) etag: Option<&'a str>,
    pub(super) object_version: Option<&'a str>,
    pub(super) sha256: Option<&'a str>,
    pub(super) driver: &'a dyn FormatDriver,
    pub(super) canonical_format_options: &'a serde_json::Value,
    pub(super) transform_name: &'a str,
}

pub(super) fn prepared_file_payload_key(
    input: PreparedFilePayloadKeyInput<'_>,
    dependencies: &FileRuntimeDependencies,
) -> Result<PreparedSourcePayloadKey> {
    let transform = file_transform_identity(input.transform_name, dependencies)?;
    let payload_hash = cdf_runtime::artifact_hash(&serde_json::json!({
        "version": 1,
        "resource_id": input.resource_id.as_str(),
        "location": input.location,
        "size_bytes": input.size_bytes,
        "source_generation": input.source_generation,
        "etag": input.etag,
        "object_version": input.object_version,
        "sha256": input.sha256,
        "format": {
            "id": input.driver.descriptor().format_id.as_str(),
            "version": input.driver.descriptor().semantic_version,
            "options": input.canonical_format_options,
        },
        "transform": transform,
    }))?;
    PreparedSourcePayloadKey::new(
        input.resource_id.clone(),
        SourceDriverId::new("files")?,
        payload_hash,
    )
}

pub(super) fn file_transform_identity(
    transform_name: &str,
    dependencies: &FileRuntimeDependencies,
) -> Result<serde_json::Value> {
    Ok(if transform_name == "none" {
        serde_json::json!({"id": "none", "version": "none"})
    } else {
        let transform = dependencies.transforms().resolve_name(transform_name)?;
        serde_json::json!({
            "id": transform.descriptor().transform_id.as_str(),
            "version": transform.descriptor().semantic_version,
        })
    })
}

pub(super) fn file_payload_cache_key(
    resolved: &ResolvedFileMatch,
    dependencies: &FileRuntimeDependencies,
) -> Result<FilePayloadCacheKey> {
    let transform = file_transform_identity(resolved.compression.mode_name(), dependencies)?;
    FilePayloadCacheKey::new(cdf_runtime::artifact_hash(&serde_json::json!({
        "version": 1,
        "location": &resolved.path_text,
        "size_bytes": resolved.size_bytes,
        "source_generation": &resolved.source_generation,
        "etag": &resolved.etag,
        "object_version": &resolved.version,
        "sha256": &resolved.sha256,
        "transform": transform,
    }))?)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PlannedFileAccessCoverage {
    Full,
    Selective,
}

pub(super) fn planned_file_access_coverage(
    scan_intent: &CompiledScanIntent,
    admission_schema: &Schema,
) -> PlannedFileAccessCoverage {
    // Exact ranges are worthwhile only when the compiled projection proves that
    // at least one physical root can be omitted. Predicate selectivity is unknown
    // without recorded statistics, so predicate-only scans retain sequential spool.
    if scan_intent
        .projection
        .as_ref()
        .is_some_and(|projection| projection.len() < admission_schema.fields().len())
    {
        PlannedFileAccessCoverage::Selective
    } else {
        PlannedFileAccessCoverage::Full
    }
}

pub(super) struct PrepareFileInputRequest<'a> {
    pub(super) resource_id: &'a ResourceId,
    pub(super) resolved: &'a ResolvedFileMatch,
    pub(super) source_access: cdf_runtime::FormatSourceAccess,
    pub(super) access_coverage: PlannedFileAccessCoverage,
    pub(super) driver: &'a dyn FormatDriver,
    pub(super) canonical_format_options: &'a serde_json::Value,
    pub(super) dependencies: &'a FileRuntimeDependencies,
    pub(super) cancellation: &'a cdf_runtime::RunCancellation,
}

pub(super) fn prepare_file_input(request: PrepareFileInputRequest<'_>) -> Result<PreparedInput> {
    let PrepareFileInputRequest {
        resource_id,
        resolved,
        source_access,
        access_coverage,
        driver,
        canonical_format_options,
        dependencies,
        cancellation,
    } = request;
    let prepared_payload_key = prepared_file_payload_key(
        PreparedFilePayloadKeyInput {
            resource_id,
            location: &resolved.path_text,
            size_bytes: resolved.size_bytes,
            source_generation: resolved.source_generation.as_deref(),
            etag: resolved.etag.as_deref(),
            object_version: resolved.version.as_deref(),
            sha256: resolved.sha256.as_deref(),
            driver,
            canonical_format_options,
            transform_name: resolved.compression.mode_name(),
        },
        dependencies,
    )?;
    let payload_cache_key = (dependencies.payload_cache().is_some()
        && matches!(resolved.open, ResolvedFileOpen::Transport(_))
        && resolved.identity_strength != GenerationStrength::Weak
        && resolved.compression.transform_id.is_none()
        && source_access == cdf_runtime::FormatSourceAccess::Adaptive
        && access_coverage == PlannedFileAccessCoverage::Full
        && dependencies
            .payload_cache()
            .is_some_and(|cache| resolved.size_bytes <= cache.policy().maximum_bytes))
    .then(|| file_payload_cache_key(resolved, dependencies))
    .transpose()?;
    if let Some(payload) = dependencies
        .prepared_payloads()
        .take(&prepared_payload_key)?
    {
        let (payload, retention) =
            payload.into_typed::<PreparedFilePayload>("file source execution")?;
        let observed = Arc::new(ObservedByteSource::new(payload.source));
        let source_io = observed.observer();
        return Ok(PreparedInput {
            input: PreparedFileInput::Source(observed),
            source_io,
            extraction_content_hash: payload.source_content_digest,
            hash_sweep_source: None,
            payload_retention: Some(retention),
            payload_cache_key: None,
        });
    }
    if let (Some(cache), Some(cache_key)) = (dependencies.payload_cache(), &payload_cache_key) {
        match cache.lookup(
            cache_key,
            &resolved.path_text,
            resolved.size_bytes,
            cancellation,
            dependencies.execution().memory(),
        ) {
            Ok(FilePayloadCacheLookup::Hit(hit)) => {
                let observed = Arc::new(ObservedByteSource::new(hit.source));
                let source_io = observed.observer();
                source_io.set_mode(SourceReadMode::PayloadCache)?;
                return Ok(PreparedInput {
                    input: PreparedFileInput::Source(observed),
                    source_io,
                    extraction_content_hash: None,
                    hash_sweep_source: None,
                    payload_retention: Some(hit.retention),
                    payload_cache_key: None,
                });
            }
            Ok(FilePayloadCacheLookup::Miss) => {}
            Err(error) if cancellation.is_cancelled() => return Err(error),
            Err(_) => {}
        }
    }
    if resolved.compression.transform_id.is_none() {
        let opened = open_file_byte_source(resolved, dependencies)?;
        let source = opened.source;
        let extraction_content_hash = opened.content_digest;
        let transport_spool = matches!(resolved.open, ResolvedFileOpen::Transport(_))
            && source_access == cdf_runtime::FormatSourceAccess::Adaptive
            && (access_coverage == PlannedFileAccessCoverage::Full
                || source.identity().strength == GenerationStrength::Weak
                || !source.capabilities().exact_ranges);
        let hash_sweep_source = (extraction_content_hash.is_some()
            && source_access != cdf_runtime::FormatSourceAccess::Sequential
            && !transport_spool)
            .then(|| Arc::clone(&source));
        let input = if transport_spool {
            PreparedFileInput::SpoolSource {
                source,
                size_bytes: Some(resolved.size_bytes),
            }
        } else {
            PreparedFileInput::Source(source)
        };
        return Ok(PreparedInput {
            input,
            source_io: opened.observer,
            extraction_content_hash,
            hash_sweep_source,
            payload_retention: None,
            payload_cache_key,
        });
    }
    if let Some(transform_id) = &resolved.compression.transform_id {
        let opened = open_file_byte_source(resolved, dependencies)?;
        let transformed = transformed_byte_source(opened.source, transform_id, dependencies)?;
        let input = if source_access != cdf_runtime::FormatSourceAccess::Sequential {
            PreparedFileInput::SpoolSource {
                source: transformed,
                size_bytes: None,
            }
        } else {
            PreparedFileInput::Source(transformed)
        };
        return Ok(PreparedInput {
            input,
            source_io: opened.observer,
            extraction_content_hash: opened.content_digest,
            hash_sweep_source: None,
            payload_retention: None,
            payload_cache_key,
        });
    }
    Err(CdfError::internal(
        "file preparation reached an unclassified compression state",
    ))
}

pub(super) struct OpenedFileByteSource {
    source: Arc<dyn ByteSource>,
    observer: SourceIoObserver,
    content_digest: Option<SourceContentDigest>,
}

pub(super) fn open_file_byte_source(
    resolved: &ResolvedFileMatch,
    dependencies: &FileRuntimeDependencies,
) -> Result<OpenedFileByteSource> {
    let raw: Arc<dyn ByteSource> = match &resolved.open {
        ResolvedFileOpen::LocalPath(path) => {
            let local: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
                path,
                dependencies.execution().memory(),
            )?);
            verify_opened_local_generation(resolved, local.as_ref())?;
            local
        }
        ResolvedFileOpen::Transport(resource) => {
            let expected = expected_file_identity(resolved);
            dependencies.with_transport(|transport, egress| {
                transport.open_byte_source(
                    egress,
                    resource,
                    &expected,
                    dependencies.execution().memory(),
                )
            })?
        }
    };
    let observed = Arc::new(ObservedByteSource::new(raw));
    let observer = observed.observer();
    let requires_content_digest = matches!(resolved.open, ResolvedFileOpen::LocalPath(_))
        || resolved.identity_strength == GenerationStrength::Weak;
    let (source, content_digest): (Arc<dyn ByteSource>, Option<SourceContentDigest>) =
        if requires_content_digest {
            let digest = SourceContentDigest::default();
            (
                Arc::new(HashingByteSource::new(observed, digest.clone())),
                Some(digest),
            )
        } else {
            (observed, None)
        };
    Ok(OpenedFileByteSource {
        source,
        observer,
        content_digest,
    })
}

pub(super) fn verify_opened_local_generation(
    resolved: &ResolvedFileMatch,
    source: &dyn ByteSource,
) -> Result<()> {
    let observed = source.identity();
    if observed.size_bytes != Some(resolved.size_bytes)
        || observed.generation.as_ref() != resolved.source_generation.as_ref()
    {
        return Err(CdfError::data(format!(
            "local file `{}` changed between planning and open; re-plan before retrying",
            resolved.path_text
        )));
    }
    Ok(())
}

pub(super) fn expected_file_identity(resolved: &ResolvedFileMatch) -> FileIdentityMetadata {
    FileIdentityMetadata {
        location: resolved.path_text.clone(),
        size_bytes: Some(resolved.size_bytes),
        checksum: resolved.sha256.as_ref().map(|sha256| FileChecksum {
            algorithm: "sha256".to_owned(),
            value: sha256.clone(),
        }),
        etag: resolved.etag.clone(),
        version: resolved.version.clone(),
        modified: resolved.source_generation.clone(),
        exact_ranges: resolved.exact_ranges,
    }
}

pub(super) struct SpoolInputRequest<'a> {
    pub(super) source: Arc<dyn ByteSource>,
    pub(super) size_bytes: Option<u64>,
    pub(super) mode: crate::FileSpoolMode,
    pub(super) source_io: SourceIoObserver,
    pub(super) payload_cache_key: Option<FilePayloadCacheKey>,
    pub(super) dependencies: &'a FileRuntimeDependencies,
    pub(super) cancellation: cdf_runtime::RunCancellation,
}

pub(super) async fn ready_spooled_file_input(
    request: SpoolInputRequest<'_>,
) -> Result<ReadyFileInput> {
    let SpoolInputRequest {
        source,
        size_bytes,
        mode,
        source_io,
        payload_cache_key,
        dependencies,
        cancellation,
    } = request;
    let cache_staging_root = payload_cache_key
        .as_ref()
        .and_then(|_| dependencies.payload_cache())
        .map(FilePayloadCache::staging_root);
    let strong_seekable = size_bytes.is_some()
        && source.identity().strength != GenerationStrength::Weak
        && source.capabilities().exact_ranges;
    if strong_seekable && mode == crate::FileSpoolMode::Complete {
        let spool = spool_byte_source_async(
            Arc::clone(&source),
            size_bytes,
            cache_staging_root.as_deref(),
            dependencies,
            cancellation.clone(),
        )
        .await?;
        source_io.set_mode(SourceReadMode::FullSpool)?;
        return ready_materialized_spool(
            spool,
            source.identity().clone(),
            payload_cache_key,
            dependencies,
            cancellation,
        );
    }

    if let Some(size_bytes) = size_bytes
        && strong_seekable
    {
        let growing = start_growing_spool(
            Arc::clone(&source),
            size_bytes,
            dependencies.max_spool_bytes(),
            dependencies.execution().spill(),
            dependencies.execution().memory(),
            cache_staging_root.as_deref(),
            cancellation.clone(),
        )?;
        if let Some(growing) = growing {
            source_io.set_mode(SourceReadMode::GrowingSpool)?;
            let source_identity = growing.source.identity().clone();
            let observed_sha256 = growing
                .cache_staged
                .then(|| Arc::new(std::sync::Mutex::new(None)));
            let completion = growing_spool_completion(growing.completion, observed_sha256.clone());
            let post_decode_completion = match (
                growing.cache_staged,
                dependencies.payload_cache().cloned(),
                payload_cache_key,
                observed_sha256,
            ) {
                (true, Some(cache), Some(cache_key), Some(observed_sha256)) => {
                    Some(payload_cache_post_decode_completion(
                        PayloadCachePromotionRequest {
                            spool_path: growing.spool_path,
                            identity: source_identity,
                            size_bytes,
                            sha256: None,
                            cache,
                            cache_key,
                            execution: dependencies.execution().clone(),
                            cancellation: cancellation.clone(),
                            _retention: growing.retention.clone(),
                        },
                        observed_sha256,
                    ))
                }
                _ => None,
            };
            return Ok(ReadyFileInput {
                source: growing.source,
                payload_retention: Some(growing.retention),
                source_completion: Some(completion),
                post_decode_completion,
            });
        }
        let evicting = start_evicting_spool(
            Arc::clone(&source),
            size_bytes,
            dependencies.max_spool_bytes(),
            dependencies.execution().spill(),
            dependencies.execution().memory(),
            cancellation,
        )?;
        if let Some(evicting) = evicting {
            source_io.set_mode(SourceReadMode::EvictingSpool)?;
            return Ok(ReadyFileInput {
                source: evicting.source,
                payload_retention: Some(evicting.retention),
                source_completion: Some(evicting.completion),
                post_decode_completion: None,
            });
        }
        source_io.set_mode(SourceReadMode::ExactRanges)?;
        return Ok(ReadyFileInput {
            source,
            payload_retention: None,
            source_completion: None,
            post_decode_completion: None,
        });
    }

    source_io.set_mode(SourceReadMode::FullSpool)?;
    let source_identity = source.identity().clone();
    let spool = spool_byte_source_async(
        source,
        size_bytes,
        cache_staging_root.as_deref(),
        dependencies,
        cancellation.clone(),
    )
    .await?;
    ready_materialized_spool(
        spool,
        source_identity,
        payload_cache_key,
        dependencies,
        cancellation,
    )
}

pub(super) fn ready_materialized_spool(
    spool: AccountedSpool,
    source_identity: ContentIdentity,
    payload_cache_key: Option<FilePayloadCacheKey>,
    dependencies: &FileRuntimeDependencies,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<ReadyFileInput> {
    let spool = Arc::new(spool);
    let materialized_identity = materialized_spool_identity(source_identity, spool.bytes())?;
    let retention = retain_spool(&spool, spool.bytes())?;
    let cache_promotion = match (
        spool.cache_staged,
        dependencies.payload_cache().cloned(),
        payload_cache_key,
        spool.sha256(),
    ) {
        (true, Some(cache), Some(cache_key), Some(sha256)) => {
            Some(payload_cache_promotion_completion(
                PayloadCachePromotionRequest {
                    spool_path: spool.path().to_path_buf(),
                    identity: materialized_identity.clone(),
                    size_bytes: spool.bytes(),
                    sha256: None,
                    cache,
                    cache_key,
                    execution: dependencies.execution().clone(),
                    cancellation,
                    _retention: retention.clone(),
                }
                .with_sha256(sha256.to_owned()),
            ))
        }
        _ => None,
    };
    let local = open_identity_preserving_local_source(
        spool.path(),
        materialized_identity,
        spool.bytes(),
        dependencies.execution().memory(),
    )?;
    Ok(ReadyFileInput {
        source: local,
        payload_retention: Some(retention),
        source_completion: None,
        post_decode_completion: cache_promotion,
    })
}

pub(super) fn materialized_spool_identity(
    mut identity: ContentIdentity,
    size_bytes: u64,
) -> Result<ContentIdentity> {
    identity.size_bytes = Some(size_bytes);
    identity.validate()?;
    Ok(identity)
}

pub(super) fn growing_spool_completion(
    completion: BoxFuture<'static, Result<Option<String>>>,
    observed_sha256: Option<Arc<std::sync::Mutex<Option<String>>>>,
) -> BoxFuture<'static, Result<()>> {
    Box::pin(async move {
        let sha256 = completion.await?;
        if let Some(observed_sha256) = observed_sha256 {
            *observed_sha256.lock().map_err(|_| {
                CdfError::internal("growing spool content hash authority was poisoned")
            })? = sha256;
        }
        Ok(())
    })
}

pub(super) struct PayloadCachePromotionRequest {
    spool_path: PathBuf,
    identity: ContentIdentity,
    size_bytes: u64,
    sha256: Option<String>,
    cache: FilePayloadCache,
    cache_key: FilePayloadCacheKey,
    execution: ExecutionServices,
    cancellation: cdf_runtime::RunCancellation,
    _retention: PayloadRetention,
}

impl PayloadCachePromotionRequest {
    fn with_sha256(mut self, sha256: String) -> Self {
        self.sha256 = Some(sha256);
        self
    }
}

pub(super) fn payload_cache_promotion_completion(
    request: PayloadCachePromotionRequest,
) -> BoxFuture<'static, Result<()>> {
    Box::pin(promote_payload_cache(request))
}

pub(super) fn payload_cache_post_decode_completion(
    mut request: PayloadCachePromotionRequest,
    observed_sha256: Arc<std::sync::Mutex<Option<String>>>,
) -> BoxFuture<'static, Result<()>> {
    Box::pin(async move {
        request.sha256 = observed_sha256
            .lock()
            .map_err(|_| CdfError::internal("growing spool content hash authority was poisoned"))?
            .take();
        promote_payload_cache(request).await
    })
}

pub(super) async fn promote_payload_cache(mut request: PayloadCachePromotionRequest) -> Result<()> {
    request.identity = materialized_spool_identity(request.identity, request.size_bytes)?;
    let PayloadCachePromotionRequest {
        spool_path,
        identity,
        size_bytes,
        sha256,
        cache,
        cache_key,
        execution,
        cancellation,
        _retention,
    } = request;
    let Some(sha256) = sha256 else {
        return Ok(());
    };
    cancellation.check()?;
    let operation_cancellation = cancellation.clone();
    let task = match execution.spawn_blocking_value(
        "file-payload-cache-promotion",
        FILE_SOURCE_BLOCKING_LANE_ID,
        move |task_cancellation| {
            operation_cancellation.check()?;
            task_cancellation.check()?;
            cache.promote(
                &cache_key,
                &spool_path,
                identity,
                size_bytes,
                &sha256,
                &task_cancellation,
            )
        },
    ) {
        Ok(task) => task,
        Err(error) if cancellation.is_cancelled() => return Err(error),
        Err(_) => return Ok(()),
    };
    match task.await {
        Ok(_) => Ok(()),
        Err(error) if cancellation.is_cancelled() => Err(error),
        Err(_) => Ok(()),
    }
}

pub(super) async fn spool_byte_source_async(
    source: Arc<dyn ByteSource>,
    size_bytes: Option<u64>,
    cache_staging_root: Option<&Path>,
    dependencies: &FileRuntimeDependencies,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<AccountedSpool> {
    if size_bytes.is_some_and(|bytes| bytes > dependencies.max_spool_bytes()) {
        return Err(CdfError::data(format!(
            "file requires {} spool bytes, exceeding the configured {}-byte disk budget; increase the spool budget or use a streaming format runtime",
            size_bytes.unwrap_or_default(),
            dependencies.max_spool_bytes()
        )));
    }
    let initially_reserved = size_bytes.unwrap_or(1).max(1);
    let mut reservation = dependencies
        .execution()
        .spill()
        .try_reserve(initially_reserved)?
        .ok_or_else(|| {
            let snapshot = dependencies.execution().spill().snapshot();
            CdfError::data(format!(
                "file spool requires {initially_reserved} bytes but the shared spill budget has {} of {} bytes in use; increase the spill budget or reduce concurrent files",
                snapshot.current_bytes, snapshot.budget_bytes
            ))
        })?;
    let (file, cache_staged) = if let Some(staging_root) = cache_staging_root {
        match tempfile::NamedTempFile::new_in(staging_root) {
            Ok(file) => (file, true),
            Err(_) => (
                tempfile::NamedTempFile::new().map_err(|error| {
                    CdfError::data(format!("create accounted file spool: {error}"))
                })?,
                false,
            ),
        }
    } else {
        (
            tempfile::NamedTempFile::new()
                .map_err(|error| CdfError::data(format!("create accounted file spool: {error}")))?,
            false,
        )
    };
    let mut output = tokio::fs::File::create(file.path())
        .await
        .map_err(|error| CdfError::data(format!("open accounted file spool: {error}")))?;
    let capabilities = source.capabilities();
    let chunk_bytes = (4 * 1024 * 1024_u64).clamp(
        capabilities.minimum_chunk_bytes,
        capabilities.maximum_chunk_bytes,
    );
    let mut input = source
        .open_sequential(SequentialReadRequest {
            preferred_chunk_bytes: chunk_bytes,
            cancellation: cancellation.clone(),
        })
        .await?;
    let mut transferred = 0_u64;
    let expected_checksum = source.identity().checksum.clone();
    let mut hasher = (expected_checksum.is_some() || cache_staged).then(Sha256::new);
    while let Some(chunk) = cancellation.await_or_cancel(input.try_next()).await? {
        cancellation.check()?;
        let chunk_bytes = u64::try_from(chunk.payload().len())
            .map_err(|_| CdfError::data("file spool chunk exceeds u64"))?;
        let next_transferred = transferred
            .checked_add(chunk_bytes)
            .ok_or_else(|| CdfError::data("file spool byte count overflowed"))?;
        if next_transferred > dependencies.max_spool_bytes() {
            return Err(CdfError::data(
                "file spool exceeded its configured disk bound",
            ));
        }
        if next_transferred > reservation.bytes()
            && !reservation.try_grow(next_transferred - reservation.bytes())?
        {
            return Err(CdfError::data(
                "file spool exhausted the shared spill budget while streaming transformed output",
            ));
        }
        if size_bytes.is_some_and(|expected| next_transferred > expected) {
            return Err(CdfError::data(
                "file spool exceeded its planned generation length",
            ));
        }
        output
            .write_all(chunk.payload())
            .await
            .map_err(|error| CdfError::data(format!("write accounted file spool: {error}")))?;
        if let Some(hasher) = &mut hasher {
            hasher.update(chunk.payload());
        }
        transferred = next_transferred;
    }
    output
        .flush()
        .await
        .map_err(|error| CdfError::data(format!("flush accounted file spool: {error}")))?;
    if let Some(size_bytes) = size_bytes
        && transferred != size_bytes
    {
        return Err(CdfError::data(format!(
            "file spool wrote {transferred} bytes for a planned {size_bytes}-byte generation"
        )));
    }
    let observed_sha256 = hasher.map(|hasher| format!("sha256:{}", hex::encode(hasher.finalize())));
    if let (Some(expected), Some(observed)) =
        (expected_checksum.as_deref(), observed_sha256.as_deref())
    {
        let expected = expected.strip_prefix("sha256:").unwrap_or(expected);
        let observed = observed.strip_prefix("sha256:").unwrap_or(observed);
        if observed != expected {
            return Err(CdfError::data(
                "file spool checksum does not match planned content identity",
            ));
        }
    }
    cancellation.check()?;
    Ok(AccountedSpool {
        file,
        _reservation: reservation,
        bytes: transferred,
        sha256: observed_sha256,
        cache_staged,
    })
}

pub(super) fn transformed_byte_source(
    upstream: Arc<dyn ByteSource>,
    transform_id: &ByteTransformId,
    dependencies: &FileRuntimeDependencies,
) -> Result<Arc<dyn ByteSource>> {
    const TRANSFORM_CHUNK_BYTES: u64 = 1024 * 1024;

    let transform = dependencies.transforms().resolve(transform_id)?;
    let descriptor = transform.descriptor().clone();
    let preferred_input_chunk_bytes = TRANSFORM_CHUNK_BYTES.clamp(
        upstream.capabilities().minimum_chunk_bytes,
        upstream.capabilities().maximum_chunk_bytes,
    );
    Ok(Arc::new(TransformedByteSource::new(
        upstream,
        transform,
        TransformSourceConfig {
            preferred_input_chunk_bytes,
            maximum_expanded_bytes: descriptor.maximum_expanded_bytes,
            maximum_expansion_ratio: descriptor.maximum_expansion_ratio,
            memory: dependencies.execution().memory(),
            consumer: ConsumerKey::new(
                format!("file-transform-{}", descriptor.transform_id.as_str()),
                MemoryClass::Transform,
            )?,
        },
    )?))
}
