use std::sync::Arc;

use arrow_schema::SchemaRef;
use bytes::Bytes;
use cdf_kernel::{Batch, CdfError, Result, SourcePosition};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use futures_util::{TryStreamExt, stream};
use sha2::{Digest, Sha256};

use crate::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    DecodePlanningRequest, DecodeSchemaPlan, FormatDiscoveryRequest, FormatDriver,
    GenerationStrength, PhysicalDecodeRequest, ReadOptions, RunCancellation, SequentialReadRequest,
    decode_unit_no_lookback_frontiers,
};

const BOUNDED_TARGET_BATCH_BYTES: u64 = 16 * 1024 * 1024;

/// A finite, already-materialized payload that retains its shared-ledger lease
/// while codecs consume zero-copy logical slices.
#[derive(Clone, Debug)]
pub struct MemoryByteSource {
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    payload: AccountedBytes,
}

impl MemoryByteSource {
    pub async fn from_bytes(
        stable_id: impl Into<String>,
        payload: Vec<u8>,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Self> {
        if payload.is_empty() {
            return Err(CdfError::data(
                "bounded format payload must contain at least one byte",
            ));
        }
        let size_bytes = u64::try_from(payload.len())
            .map_err(|_| CdfError::data("bounded format payload length exceeds u64"))?;
        let checksum = format!("sha256:{}", hex::encode(Sha256::digest(&payload)));
        let lease = reserve(
            memory,
            ReservationRequest::new(
                ConsumerKey::new("bounded-format-input", MemoryClass::Source)?,
                size_bytes,
            )?,
        )
        .await?;
        let payload = AccountedBytes::new(Bytes::from(payload), lease)?;
        let identity = ContentIdentity {
            stable_id: stable_id.into(),
            size_bytes: Some(size_bytes),
            generation: None,
            checksum: Some(checksum),
            strength: GenerationStrength::ContentAddressed,
        };
        identity.validate()?;
        let capabilities = ByteSourceCapabilities {
            known_length: true,
            reopenable: true,
            seekable: true,
            exact_ranges: true,
            useful_range_concurrency: 1,
            minimum_chunk_bytes: 1,
            maximum_chunk_bytes: size_bytes,
        };
        capabilities.validate()?;
        Ok(Self {
            identity,
            capabilities,
            payload,
        })
    }
}

impl ByteSource for MemoryByteSource {
    fn identity(&self) -> &ContentIdentity {
        &self.identity
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        &self.capabilities
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> cdf_kernel::BoxFuture<'_, Result<AccountedByteStream>> {
        let payload = self.payload.clone();
        Box::pin(async move {
            request.cancellation.check()?;
            if request.preferred_chunk_bytes == 0 {
                return Err(CdfError::contract(
                    "bounded memory source requires a nonzero preferred chunk size",
                ));
            }
            let chunk_bytes = usize::try_from(request.preferred_chunk_bytes)
                .unwrap_or(usize::MAX)
                .max(1);
            let state = (payload, 0_usize, chunk_bytes, request.cancellation);
            Ok(Box::pin(stream::try_unfold(
                state,
                |(payload, offset, chunk_bytes, cancellation)| async move {
                    cancellation.check()?;
                    if offset == payload.payload().len() {
                        return Ok(None);
                    }
                    let end = offset
                        .saturating_add(chunk_bytes)
                        .min(payload.payload().len());
                    let chunk = payload.slice(offset..end)?;
                    Ok(Some((chunk, (payload, end, chunk_bytes, cancellation))))
                },
            )) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> cdf_kernel::BoxFuture<'_, Result<AccountedBytes>> {
        Box::pin(async move {
            cancellation.check()?;
            let start = usize::try_from(extent.start)
                .map_err(|_| CdfError::data("bounded range start exceeds usize"))?;
            let length = usize::try_from(extent.length)
                .map_err(|_| CdfError::data("bounded range length exceeds usize"))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| CdfError::data("bounded range overflows usize"))?;
            self.payload.slice(start..end)
        })
    }
}

#[derive(Clone, Debug)]
pub struct BoundedFormatRead {
    pub schema: SchemaRef,
    pub batches: Vec<Batch>,
}

#[derive(Clone)]
pub struct BoundedFormatRequest {
    pub options: serde_json::Value,
    pub read_options: ReadOptions,
    pub schema: Option<DecodeSchemaPlan>,
    pub source_position: Option<SourcePosition>,
    pub memory: Arc<dyn MemoryCoordinator>,
    pub cancellation: RunCancellation,
}

impl BoundedFormatRequest {
    pub fn new(read_options: ReadOptions, memory: Arc<dyn MemoryCoordinator>) -> Self {
        Self {
            options: serde_json::json!({}),
            read_options,
            schema: None,
            source_position: None,
            memory,
            cancellation: RunCancellation::default(),
        }
    }

    pub fn with_options(mut self, options: serde_json::Value) -> Self {
        self.options = options;
        self
    }

    pub fn with_schema(mut self, schema: DecodeSchemaPlan) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_source_position(mut self, source_position: SourcePosition) -> Self {
        self.source_position = Some(source_position);
        self
    }

    pub fn with_cancellation(mut self, cancellation: RunCancellation) -> Self {
        self.cancellation = cancellation;
        self
    }
}

/// Executes a registered codec over a finite payload through the same neutral
/// discover/prepare/decode contracts used by file sources. This boundary is for
/// bounded pages and completed foreign-process messages, never unbounded input.
pub async fn decode_bounded_format(
    driver: Arc<dyn FormatDriver>,
    source: Arc<dyn ByteSource>,
    request: BoundedFormatRequest,
) -> Result<BoundedFormatRead> {
    let BoundedFormatRequest {
        options,
        read_options,
        schema,
        source_position,
        memory,
        cancellation,
    } = request;
    let canonical_options = driver.canonical_options(options)?;
    let size_bytes = source.identity().size_bytes.ok_or_else(|| {
        CdfError::contract("bounded format execution requires a known payload length")
    })?;
    let decode_schema = match schema {
        Some(schema) => schema,
        None => {
            let observation = driver
                .discover(
                    Arc::clone(&source),
                    FormatDiscoveryRequest {
                        options: canonical_options.clone(),
                        maximum_bytes: size_bytes,
                        maximum_records: u64::MAX,
                        memory: Arc::clone(&memory),
                        cancellation: cancellation.clone(),
                    },
                )
                .await?;
            DecodeSchemaPlan::verified_physical(observation.arrow_schema)
        }
    };
    let target_batch_bytes = BOUNDED_TARGET_BATCH_BYTES.clamp(
        driver.descriptor().minimum_working_set_bytes,
        driver.descriptor().maximum_working_set_bytes,
    );
    let session = driver
        .prepare_decode(
            Arc::clone(&source),
            DecodePlanningRequest {
                options: canonical_options,
                projection: None,
                predicates: Vec::new(),
                target_batch_rows: read_options.batch_size,
                target_batch_bytes,
                cancellation: cancellation.clone(),
            },
        )
        .await?;
    let units = session.units().to_vec();
    if units.is_empty() {
        return Err(CdfError::contract(
            "bounded format session must contain at least one decode unit",
        ));
    }
    let frontiers = decode_unit_no_lookback_frontiers(&units)?;
    let mut batches = Vec::new();
    for (ordinal, unit) in units.into_iter().enumerate() {
        let mut decoded = session
            .decode(PhysicalDecodeRequest {
                unit,
                resource_id: read_options.resource_id.clone(),
                partition_id: read_options.partition_id.clone(),
                batch_id_prefix: read_options.batch_id_prefix.clone(),
                schema: decode_schema.clone(),
                source_position: source_position.clone(),
                projection: None,
                predicates: Vec::new(),
                target_batch_rows: read_options.batch_size,
                target_batch_bytes,
                memory: Arc::clone(&memory),
                cancellation: cancellation.clone(),
            })
            .await?;
        while let Some(batch) = decoded.try_next().await? {
            batches.push(batch.into_batch()?);
        }
        if let Some(frontiers) = &frontiers {
            source.release_before(frontiers[ordinal])?;
        }
    }
    source.release_before(size_bytes)?;
    let schema = batches
        .first()
        .and_then(Batch::record_batch)
        .map(arrow_array::RecordBatch::schema)
        .unwrap_or(decode_schema.decoder_schema);
    Ok(BoundedFormatRead { schema, batches })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use futures_executor::block_on;
    use futures_util::TryStreamExt;

    use super::*;

    #[test]
    fn memory_byte_source_chunks_and_ranges_share_one_lease() {
        block_on(async {
            let memory =
                Arc::new(DeterministicMemoryCoordinator::new(64, BTreeMap::new()).unwrap());
            let source =
                MemoryByteSource::from_bytes("memory:test", b"abcdefgh".to_vec(), memory.clone())
                    .await
                    .unwrap();
            let chunks = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: 3,
                    cancellation: RunCancellation::default(),
                })
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(
                chunks
                    .iter()
                    .flat_map(|chunk| chunk.payload().iter().copied())
                    .collect::<Vec<_>>(),
                b"abcdefgh"
            );
            assert_eq!(
                source
                    .read_exact_range(ByteExtent::new(2, 3).unwrap(), RunCancellation::default(),)
                    .await
                    .unwrap()
                    .payload(),
                b"cde"
            );
            assert_eq!(memory.snapshot().current_bytes, 8);
            drop(chunks);
            drop(source);
            assert_eq!(memory.snapshot().current_bytes, 0);
        });
    }

    #[test]
    fn memory_byte_source_rejects_payload_above_shared_budget() {
        block_on(async {
            let memory = Arc::new(DeterministicMemoryCoordinator::new(4, BTreeMap::new()).unwrap());
            let error = MemoryByteSource::from_bytes(
                "memory:oversized",
                b"abcdefgh".to_vec(),
                memory.clone(),
            )
            .await
            .unwrap_err();

            assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
            assert!(error.message.contains("exceeds managed budget"));
            assert_eq!(memory.snapshot().current_bytes, 0);
        });
    }

    #[test]
    fn memory_byte_source_observes_cancellation_before_and_between_polls() {
        block_on(async {
            let memory =
                Arc::new(DeterministicMemoryCoordinator::new(64, BTreeMap::new()).unwrap());
            let source =
                MemoryByteSource::from_bytes("memory:cancel", b"abcdefgh".to_vec(), memory.clone())
                    .await
                    .unwrap();

            let cancelled_before_open = RunCancellation::default();
            cancelled_before_open.cancel();
            let error = match source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: 3,
                    cancellation: cancelled_before_open,
                })
                .await
            {
                Ok(_) => panic!("cancelled bounded source unexpectedly opened"),
                Err(error) => error,
            };
            assert!(error.message.contains("cancelled"));

            let cancellation = RunCancellation::default();
            let mut stream = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: 3,
                    cancellation: cancellation.clone(),
                })
                .await
                .unwrap();
            assert_eq!(stream.try_next().await.unwrap().unwrap().payload(), b"abc");
            cancellation.cancel();
            let error = stream.try_next().await.unwrap_err();
            assert!(error.message.contains("cancelled"));

            drop(stream);
            drop(source);
            assert_eq!(memory.snapshot().current_bytes, 0);
        });
    }
}
