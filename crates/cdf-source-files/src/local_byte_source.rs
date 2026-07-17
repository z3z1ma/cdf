use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::UNIX_EPOCH,
};

use bytes::Bytes;
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    GenerationStrength, RunCancellation, SequentialReadRequest,
};
use futures_util::stream;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
};

const MINIMUM_CHUNK_BYTES: u64 = 8 * 1024;
const MAXIMUM_CHUNK_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Clone)]
pub struct LocalByteSource {
    path: PathBuf,
    identity: ContentIdentity,
    generation: LocalGeneration,
    capabilities: ByteSourceCapabilities,
    memory: Arc<dyn MemoryCoordinator>,
}

impl std::fmt::Debug for LocalByteSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalByteSource")
            .field("identity", &self.identity)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LocalGeneration {
    size_bytes: u64,
    modified_ns: u128,
    change_token: String,
}

impl LocalGeneration {
    fn evidence_token(&self) -> String {
        format!("local-metadata-v1:{}:{}", self.size_bytes, self.modified_ns)
    }
}

impl LocalByteSource {
    pub fn open(path: impl AsRef<Path>, memory: Arc<dyn MemoryCoordinator>) -> Result<Self> {
        let path = std::fs::canonicalize(path.as_ref()).map_err(|error| {
            CdfError::data(format!(
                "canonicalize local byte source {}: {error}",
                path.as_ref().display()
            ))
        })?;
        let generation = local_generation(&path)?;
        let stable_id = path
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| CdfError::data("local byte source path is not valid Unicode"))?;
        let identity = ContentIdentity {
            stable_id,
            size_bytes: Some(generation.size_bytes),
            generation: Some(generation.evidence_token()),
            checksum: None,
            // Metadata generations reattest one planned open, but cannot authorize
            // cross-command observation-cache reuse without a content hash.
            strength: GenerationStrength::Weak,
        };
        identity.validate()?;
        let capabilities = ByteSourceCapabilities {
            known_length: true,
            reopenable: true,
            seekable: true,
            exact_ranges: true,
            useful_range_concurrency: 8,
            minimum_chunk_bytes: MINIMUM_CHUNK_BYTES,
            maximum_chunk_bytes: MAXIMUM_CHUNK_BYTES,
        };
        capabilities.validate()?;
        Ok(Self {
            path,
            identity,
            generation,
            capabilities,
            memory,
        })
    }

    async fn open_attested(&self) -> Result<File> {
        let file = File::open(&self.path).await.map_err(|error| {
            CdfError::data(format!(
                "open local byte source {}: {error}",
                self.path.display()
            ))
        })?;
        attest_file(&file, &self.generation).await?;
        Ok(file)
    }
}

pub(crate) fn open_identity_preserving_local_source(
    path: &Path,
    identity: ContentIdentity,
    size_bytes: u64,
    memory: Arc<dyn MemoryCoordinator>,
) -> Result<Arc<dyn ByteSource>> {
    identity.validate()?;
    let local = LocalByteSource::open(path, memory)?;
    if local.identity().size_bytes != Some(size_bytes) {
        return Err(CdfError::data(
            "materialized source changed before local open",
        ));
    }
    Ok(Arc::new(IdentityPreservingLocalByteSource {
        identity,
        local,
    }))
}

struct IdentityPreservingLocalByteSource {
    identity: ContentIdentity,
    local: LocalByteSource,
}

impl std::fmt::Debug for IdentityPreservingLocalByteSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IdentityPreservingLocalByteSource")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl ByteSource for IdentityPreservingLocalByteSource {
    fn identity(&self) -> &ContentIdentity {
        &self.identity
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        self.local.capabilities()
    }

    fn exact_range_coalescing_policy(&self) -> cdf_runtime::ExactRangeCoalescingPolicy {
        self.local.exact_range_coalescing_policy()
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        self.local.open_sequential(request)
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<AccountedBytes>> {
        self.local.read_exact_range(extent, cancellation)
    }

    fn read_exact_ranges(
        &self,
        extents: Vec<ByteExtent>,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<cdf_runtime::ExactRangeReadBatch>> {
        self.local.read_exact_ranges(extents, cancellation)
    }

    fn release_before(&self, frontier: u64) -> Result<()> {
        self.local.release_before(frontier)
    }
}

impl ByteSource for LocalByteSource {
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
                    "local sequential chunk target {} is outside {}..={} bytes",
                    request.preferred_chunk_bytes,
                    self.capabilities.minimum_chunk_bytes,
                    self.capabilities.maximum_chunk_bytes
                )));
            }
            let state = SequentialState {
                file: self.open_attested().await?,
                generation: self.generation.clone(),
                memory: Arc::clone(&self.memory),
                cancellation: request.cancellation,
                offset: 0,
                chunk_bytes: request.preferred_chunk_bytes,
            };
            Ok(Box::pin(stream::try_unfold(state, |mut state| async move {
                state.cancellation.check()?;
                if state.offset == state.generation.size_bytes {
                    attest_file(&state.file, &state.generation).await?;
                    return Ok(None);
                }
                let bytes = (state.generation.size_bytes - state.offset).min(state.chunk_bytes);
                let reservation = ReservationRequest::new(
                    ConsumerKey::new("local-byte-source-sequential", MemoryClass::Source)?,
                    bytes,
                )?;
                let lease = reserve(Arc::clone(&state.memory), reservation).await?;
                let length = usize::try_from(bytes)
                    .map_err(|_| CdfError::data("local byte chunk length exceeds usize"))?;
                let mut payload = vec![0_u8; length];
                state
                    .file
                    .read_exact(&mut payload)
                    .await
                    .map_err(|error| CdfError::data(format!("read local byte source: {error}")))?;
                state.offset = state
                    .offset
                    .checked_add(bytes)
                    .ok_or_else(|| CdfError::data("local byte source offset overflowed"))?;
                state.cancellation.check()?;
                Ok(Some((
                    AccountedBytes::new(Bytes::from(payload), lease)?,
                    state,
                )))
            })) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<AccountedBytes>> {
        Box::pin(async move {
            cancellation.check()?;
            let end = extent
                .start
                .checked_add(extent.length)
                .ok_or_else(|| CdfError::contract("local byte range overflowed"))?;
            if end > self.generation.size_bytes {
                return Err(CdfError::data(format!(
                    "local byte range {}..{end} exceeds generation length {}",
                    extent.start, self.generation.size_bytes
                )));
            }
            let reservation = ReservationRequest::new(
                ConsumerKey::new("local-byte-source-range", MemoryClass::Source)?,
                extent.length,
            )?;
            let lease = reserve(Arc::clone(&self.memory), reservation).await?;
            let mut file = self.open_attested().await?;
            file.seek(SeekFrom::Start(extent.start))
                .await
                .map_err(|error| CdfError::data(format!("seek local byte source: {error}")))?;
            let length = usize::try_from(extent.length)
                .map_err(|_| CdfError::data("local byte range length exceeds usize"))?;
            let mut payload = vec![0_u8; length];
            file.read_exact(&mut payload)
                .await
                .map_err(|error| CdfError::data(format!("read local byte range: {error}")))?;
            attest_file(&file, &self.generation).await?;
            cancellation.check()?;
            AccountedBytes::new(Bytes::from(payload), lease)
        })
    }
}

struct SequentialState {
    file: File,
    generation: LocalGeneration,
    memory: Arc<dyn MemoryCoordinator>,
    cancellation: cdf_runtime::RunCancellation,
    offset: u64,
    chunk_bytes: u64,
}

fn local_generation(path: &Path) -> Result<LocalGeneration> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        CdfError::data(format!(
            "stat local byte source {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(CdfError::data(format!(
            "local byte source {} is not a regular file",
            path.display()
        )));
    }
    generation_from_metadata(&metadata)
}

fn generation_from_metadata(metadata: &std::fs::Metadata) -> Result<LocalGeneration> {
    let modified_ns = metadata
        .modified()
        .map_err(|error| CdfError::data(format!("read local modification time: {error}")))?
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CdfError::data(format!("local modification time predates epoch: {error}"))
        })?
        .as_nanos();
    Ok(LocalGeneration {
        size_bytes: metadata.len(),
        modified_ns,
        change_token: local_change_token(metadata),
    })
}

#[cfg(unix)]
fn local_change_token(metadata: &std::fs::Metadata) -> String {
    use std::os::unix::fs::MetadataExt;

    format!(
        "dev{}-ino{}-ctime{}-{}",
        metadata.dev(),
        metadata.ino(),
        metadata.ctime(),
        metadata.ctime_nsec()
    )
}

#[cfg(not(unix))]
fn local_change_token(metadata: &std::fs::Metadata) -> String {
    format!("portable-size{}", metadata.len())
}

pub(crate) fn local_source_generation(path: &Path) -> Result<String> {
    Ok(local_generation(path)?.evidence_token())
}

pub(crate) fn local_storage_attestation(path: &Path) -> Result<String> {
    let generation = local_generation(path)?;
    Ok(format!(
        "local-storage-v1:{}:{}:{}",
        generation.size_bytes, generation.modified_ns, generation.change_token
    ))
}

async fn attest_file(file: &File, expected: &LocalGeneration) -> Result<()> {
    let metadata = file
        .metadata()
        .await
        .map_err(|error| CdfError::data(format!("reattest local byte source: {error}")))?;
    let observed = generation_from_metadata(&metadata)?;
    if &observed != expected {
        return Err(CdfError::data(format!(
            "local byte source generation changed: planned {expected:?}, observed {observed:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{io::Write, time::Duration};

    use cdf_runtime::RunCancellation;
    use futures_util::TryStreamExt;

    use super::*;

    #[test]
    fn streams_and_ranges_with_generation_and_memory_authority() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&vec![7_u8; 20_000]).unwrap();
        file.flush().unwrap();
        let services = crate::test_execution_services();
        let memory = services.memory();
        let source = LocalByteSource::open(file.path(), Arc::clone(&memory)).unwrap();
        let stream_source = source.clone();
        let chunks = services
            .run_io(async move {
                stream_source
                    .open_sequential(SequentialReadRequest {
                        preferred_chunk_bytes: 8 * 1024,
                        cancellation: RunCancellation::default(),
                    })
                    .await?
                    .try_collect::<Vec<_>>()
                    .await
            })
            .unwrap();
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.payload().len())
                .sum::<usize>(),
            20_000
        );
        drop(chunks);
        let range_source = source.clone();
        let range = services
            .run_io(async move {
                range_source
                    .read_exact_range(ByteExtent::new(10, 100)?, RunCancellation::default())
                    .await
            })
            .unwrap();
        assert_eq!(range.payload(), &[7_u8; 100]);
        drop(range);
        assert_eq!(memory.snapshot().current_bytes, 0);

        std::fs::write(file.path(), vec![8_u8; 20_001]).unwrap();
        assert!(
            services
                .run_io(async move {
                    source
                        .read_exact_range(ByteExtent::new(0, 1)?, RunCancellation::default())
                        .await
                })
                .is_err()
        );
        assert_eq!(memory.snapshot().current_bytes, 0);
    }

    #[test]
    fn serialized_generation_excludes_host_local_attestation_facts() {
        let first = tempfile::NamedTempFile::new().unwrap();
        let second = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(first.path(), b"same bytes").unwrap();
        std::fs::write(second.path(), b"same bytes").unwrap();
        let modified = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        for path in [first.path(), second.path()] {
            let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            file.set_times(std::fs::FileTimes::new().set_modified(modified))
                .unwrap();
        }

        assert_eq!(
            local_source_generation(first.path()).unwrap(),
            local_source_generation(second.path()).unwrap()
        );
        #[cfg(unix)]
        assert_ne!(
            local_generation(first.path()).unwrap(),
            local_generation(second.path()).unwrap(),
            "host-local inode/ctime evidence must remain available for open re-attestation"
        );
    }
}
