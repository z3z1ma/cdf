use std::sync::Arc;

use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_memory::{AccountedBytes, ConsumerKey, MemoryCoordinator};
use sha2::{Digest, Sha256};

use crate::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ByteTransformDriver,
    ByteTransformRequest, ContentIdentity, GenerationStrength, RunCancellation,
    SequentialReadRequest,
};

#[derive(Clone)]
pub struct TransformSourceConfig {
    pub preferred_input_chunk_bytes: u64,
    pub maximum_expanded_bytes: u64,
    pub maximum_expansion_ratio: u32,
    pub memory: Arc<dyn MemoryCoordinator>,
    pub consumer: ConsumerKey,
}

#[derive(Clone)]
pub struct TransformedByteSource {
    upstream: Arc<dyn ByteSource>,
    transform: Arc<dyn ByteTransformDriver>,
    config: TransformSourceConfig,
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
}

impl std::fmt::Debug for TransformedByteSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransformedByteSource")
            .field("identity", &self.identity)
            .field("capabilities", &self.capabilities)
            .field("transform", &self.transform.descriptor().transform_id)
            .finish_non_exhaustive()
    }
}

impl TransformedByteSource {
    pub fn new(
        upstream: Arc<dyn ByteSource>,
        transform: Arc<dyn ByteTransformDriver>,
        config: TransformSourceConfig,
    ) -> Result<Self> {
        upstream.identity().validate()?;
        upstream.capabilities().validate()?;
        transform.descriptor().validate()?;
        if config.preferred_input_chunk_bytes < upstream.capabilities().minimum_chunk_bytes
            || config.preferred_input_chunk_bytes > upstream.capabilities().maximum_chunk_bytes
            || config.maximum_expanded_bytes == 0
            || config.maximum_expanded_bytes > transform.descriptor().maximum_expanded_bytes
            || config.maximum_expansion_ratio == 0
            || config.maximum_expansion_ratio > transform.descriptor().maximum_expansion_ratio
        {
            return Err(CdfError::contract(
                "transformed byte source requires an upstream-supported input chunk and expansion ceilings within driver authority",
            ));
        }
        let identity = transformed_identity(upstream.identity(), transform.as_ref())?;
        let capabilities = ByteSourceCapabilities {
            known_length: false,
            reopenable: upstream.capabilities().reopenable,
            seekable: false,
            exact_ranges: false,
            useful_range_concurrency: 0,
            minimum_chunk_bytes: 1,
            maximum_chunk_bytes: transform.descriptor().maximum_output_chunk_bytes,
        };
        capabilities.validate()?;
        Ok(Self {
            upstream,
            transform,
            config,
            identity,
            capabilities,
        })
    }
}

impl ByteSource for TransformedByteSource {
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
                    "transformed sequential chunk target {} is outside {}..={} bytes",
                    request.preferred_chunk_bytes,
                    self.capabilities.minimum_chunk_bytes,
                    self.capabilities.maximum_chunk_bytes
                )));
            }
            let input = self
                .upstream
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: self.config.preferred_input_chunk_bytes,
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            self.transform.transform(
                input,
                ByteTransformRequest {
                    preferred_output_chunk_bytes: request.preferred_chunk_bytes,
                    maximum_expanded_bytes: self.config.maximum_expanded_bytes,
                    maximum_expansion_ratio: self.config.maximum_expansion_ratio,
                    input_size_bytes: self.upstream.identity().size_bytes,
                    memory: Arc::clone(&self.config.memory),
                    consumer: self.config.consumer.clone(),
                    cancellation: request.cancellation,
                },
            )
        })
    }

    fn read_exact_range(
        &self,
        _extent: ByteExtent,
        _cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<AccountedBytes>> {
        Box::pin(async {
            Err(CdfError::contract(
                "non-random-access byte transforms do not support exact output ranges; use sequential decode or a bounded spool adapter",
            ))
        })
    }
}

fn transformed_identity(
    upstream: &ContentIdentity,
    transform: &dyn ByteTransformDriver,
) -> Result<ContentIdentity> {
    let descriptor = transform.descriptor();
    let mut hasher = Sha256::new();
    hasher.update(b"cdf-transformed-byte-source-v1\0");
    hasher.update(upstream.stable_id.as_bytes());
    hasher.update(b"\0");
    if let Some(generation) = &upstream.generation {
        hasher.update(generation.as_bytes());
    }
    hasher.update(b"\0");
    if let Some(checksum) = &upstream.checksum {
        hasher.update(checksum.as_bytes());
    }
    hasher.update(b"\0");
    hasher.update(descriptor.transform_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(descriptor.semantic_version.as_bytes());
    let identity = ContentIdentity {
        stable_id: format!(
            "{}#transform:{}",
            upstream.stable_id,
            descriptor.transform_id.as_str()
        ),
        size_bytes: None,
        generation: Some(format!("transform-v1:{}", hex::encode(hasher.finalize()))),
        checksum: None,
        strength: match upstream.strength {
            GenerationStrength::Weak => GenerationStrength::Weak,
            GenerationStrength::Strong | GenerationStrength::ContentAddressed => {
                GenerationStrength::Strong
            }
        },
    };
    identity.validate()?;
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use cdf_memory::{DeterministicMemoryCoordinator, MemoryClass, ReservationRequest, reserve};
    use futures_executor::block_on;
    use futures_util::{TryStreamExt, stream};

    use super::*;
    use crate::{
        ByteTransformDescriptor, ByteTransformId, MagicSignature, TransformChecksumBehavior,
    };

    struct MemorySource {
        bytes: Arc<[u8]>,
        identity: ContentIdentity,
        capabilities: ByteSourceCapabilities,
        memory: Arc<dyn MemoryCoordinator>,
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
            request: SequentialReadRequest,
        ) -> BoxFuture<'_, Result<AccountedByteStream>> {
            Box::pin(async move {
                let bytes = Arc::clone(&self.bytes);
                let memory = Arc::clone(&self.memory);
                let chunk_bytes = usize::try_from(request.preferred_chunk_bytes).unwrap();
                Ok(Box::pin(stream::try_unfold(
                    (bytes, 0_usize, memory, request.cancellation),
                    move |(bytes, offset, memory, cancellation)| async move {
                        cancellation.check()?;
                        if offset == bytes.len() {
                            return Ok(None);
                        }
                        let end = offset.saturating_add(chunk_bytes).min(bytes.len());
                        let reservation = ReservationRequest::new(
                            ConsumerKey::new("transform-source-upstream", MemoryClass::Source)?,
                            u64::try_from(end - offset).unwrap(),
                        )?;
                        let lease = reserve(Arc::clone(&memory), reservation).await?;
                        let payload = AccountedBytes::new(
                            Bytes::copy_from_slice(&bytes[offset..end]),
                            lease,
                        )?;
                        Ok(Some((payload, (bytes, end, memory, cancellation))))
                    },
                )) as AccountedByteStream)
            })
        }

        fn read_exact_range(
            &self,
            _extent: ByteExtent,
            _cancellation: RunCancellation,
        ) -> BoxFuture<'_, Result<AccountedBytes>> {
            Box::pin(async { Err(CdfError::internal("not used by transform source test")) })
        }
    }

    struct PassthroughTransform(ByteTransformDescriptor);

    impl ByteTransformDriver for PassthroughTransform {
        fn descriptor(&self) -> &ByteTransformDescriptor {
            &self.0
        }

        fn transform(
            &self,
            input: AccountedByteStream,
            request: ByteTransformRequest,
        ) -> Result<AccountedByteStream> {
            request.validate_for(&self.0)?;
            Ok(input)
        }
    }

    fn transform(version: &str) -> Arc<dyn ByteTransformDriver> {
        Arc::new(PassthroughTransform(ByteTransformDescriptor {
            transform_id: ByteTransformId::new("test-transform").unwrap(),
            semantic_version: version.to_owned(),
            extensions: vec!["test".to_owned()],
            magic: vec![MagicSignature {
                offset: 0,
                bytes: vec![1, 2],
                strong: true,
            }],
            preserves_random_access: false,
            splittable: false,
            supports_concatenated_members: false,
            maximum_output_chunk_bytes: 8,
            maximum_working_set_bytes: 16,
            maximum_expanded_bytes: 1024,
            maximum_expansion_ratio: 10,
            checksum: TransformChecksumBehavior::None,
        }))
    }

    #[test]
    fn composes_sequential_capabilities_identity_and_accounted_stream() {
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024, Default::default()).unwrap());
        let upstream: Arc<dyn ByteSource> = Arc::new(MemorySource {
            bytes: Arc::from(&b"transform-source"[..]),
            identity: ContentIdentity {
                stable_id: "memory://fixture".to_owned(),
                size_bytes: Some(16),
                generation: Some("generation-1".to_owned()),
                checksum: None,
                strength: GenerationStrength::Strong,
            },
            capabilities: ByteSourceCapabilities {
                known_length: true,
                reopenable: true,
                seekable: true,
                exact_ranges: true,
                useful_range_concurrency: 2,
                minimum_chunk_bytes: 1,
                maximum_chunk_bytes: 16,
            },
            memory: Arc::clone(&memory),
        });
        let config = TransformSourceConfig {
            preferred_input_chunk_bytes: 3,
            maximum_expanded_bytes: 1024,
            maximum_expansion_ratio: 10,
            memory: Arc::clone(&memory),
            consumer: ConsumerKey::new("test-transform", MemoryClass::Transform).unwrap(),
        };
        let source =
            TransformedByteSource::new(Arc::clone(&upstream), transform("1.0.0"), config.clone())
                .unwrap();
        assert!(!source.capabilities().known_length);
        assert!(source.capabilities().reopenable);
        assert!(!source.capabilities().seekable);
        assert!(!source.capabilities().exact_ranges);
        assert_eq!(source.capabilities().maximum_chunk_bytes, 8);
        assert_eq!(source.identity().size_bytes, None);
        assert_eq!(source.identity().strength, GenerationStrength::Strong);

        let decoded = block_on(async {
            source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: 4,
                    cancellation: RunCancellation::default(),
                })
                .await?
                .map_ok(|chunk| chunk.payload().to_vec())
                .try_collect::<Vec<_>>()
                .await
        })
        .unwrap()
        .concat();
        assert_eq!(decoded, b"transform-source");
        assert_eq!(memory.snapshot().current_bytes, 0);

        let changed = TransformedByteSource::new(upstream, transform("2.0.0"), config).unwrap();
        assert_ne!(source.identity().generation, changed.identity().generation);
        assert!(
            block_on(
                source
                    .read_exact_range(ByteExtent::new(0, 1).unwrap(), RunCancellation::default(),)
            )
            .unwrap_err()
            .to_string()
            .contains("bounded spool")
        );
    }
}
