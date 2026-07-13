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

use crate::{ByteRange, FileIdentityMetadata, FileTransport, FileTransportResource};

#[derive(Clone)]
pub(crate) struct RangeDiscoveryByteSource {
    transport: Arc<dyn FileTransport>,
    resource: FileTransportResource,
    expected: FileIdentityMetadata,
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    memory: Arc<dyn MemoryCoordinator>,
}

impl RangeDiscoveryByteSource {
    pub(crate) fn try_new(
        transport: Arc<dyn FileTransport>,
        resource: FileTransportResource,
        expected: &FileIdentityMetadata,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Option<Self>> {
        let size_bytes = expected.size_bytes.ok_or_else(|| {
            CdfError::data("range discovery byte source requires a planned content length")
        })?;
        let checksum = expected.sha256().map(str::to_owned);
        let generation = expected.etag.clone().or_else(|| expected.version.clone());
        let Some(generation) = generation else {
            return Ok(None);
        };
        let identity = ContentIdentity {
            stable_id: expected.location.clone(),
            size_bytes: Some(size_bytes),
            generation: Some(generation),
            checksum: checksum.clone(),
            strength: if checksum.is_some() {
                GenerationStrength::ContentAddressed
            } else {
                GenerationStrength::Strong
            },
        };
        identity.validate()?;
        let capabilities = ByteSourceCapabilities {
            known_length: true,
            reopenable: true,
            seekable: true,
            exact_ranges: true,
            useful_range_concurrency: 1,
            minimum_chunk_bytes: 1,
            maximum_chunk_bytes: 32 * 1024 * 1024,
        };
        capabilities.validate()?;
        Ok(Some(Self {
            transport,
            resource,
            expected: expected.clone(),
            identity,
            capabilities,
            memory,
        }))
    }
}

impl ByteSource for RangeDiscoveryByteSource {
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
        Box::pin(async {
            Err(CdfError::contract(
                "range-only discovery source cannot serve sequential execution",
            ))
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<AccountedBytes>> {
        Box::pin(async move {
            cancellation.check()?;
            let size_bytes = self.identity.size_bytes.expect("validated known length");
            let end = extent
                .start
                .checked_add(extent.length)
                .ok_or_else(|| CdfError::contract("discovery range overflowed"))?;
            if end > size_bytes {
                return Err(CdfError::data("discovery range exceeds planned generation"));
            }
            let lease = reserve(
                Arc::clone(&self.memory),
                ReservationRequest::new(
                    ConsumerKey::new("file-range-discovery", MemoryClass::Discovery)?,
                    extent.length,
                )?,
            )
            .await?;
            let payload = self.transport.read_generation_range(
                &self.resource,
                &self.expected,
                ByteRange::new(extent.start, extent.length)?,
            )?;
            if u64::try_from(payload.len()).ok() != Some(extent.length) {
                return Err(CdfError::data(
                    "discovery transport returned a short exact range",
                ));
            }
            cancellation.check()?;
            AccountedBytes::new(Bytes::from(payload), lease)
        })
    }
}
