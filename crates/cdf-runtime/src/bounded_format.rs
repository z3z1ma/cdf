use std::sync::Arc;

use arrow_schema::SchemaRef;
use bytes::Bytes;
use cdf_kernel::{Batch, CdfError, Result, SourcePosition};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use futures_util::{Stream, TryStreamExt, stream};
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
    pub fn from_accounted_bytes(
        stable_id: impl Into<String>,
        payload: AccountedBytes,
    ) -> Result<Self> {
        let size_bytes = u64::try_from(payload.payload().len())
            .map_err(|_| CdfError::data("bounded format payload length exceeds u64"))?;
        if size_bytes == 0 {
            return Err(CdfError::data(
                "bounded format payload must contain at least one byte",
            ));
        }
        let checksum = format!("sha256:{}", hex::encode(Sha256::digest(payload.payload())));
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

    /// Wraps invocation-local accounted bytes without adding a redundant content-hash pass.
    ///
    /// Use this for already-authorized bounded messages such as one REST response page. The
    /// identity is deliberately weak and must never be promoted into persistent source evidence;
    /// the caller's transport/protocol authority owns that evidence.
    pub fn from_ephemeral_accounted_bytes(
        stable_id: impl Into<String>,
        payload: AccountedBytes,
    ) -> Result<Self> {
        let size_bytes = u64::try_from(payload.payload().len())
            .map_err(|_| CdfError::data("bounded format payload length exceeds u64"))?;
        if size_bytes == 0 {
            return Err(CdfError::data(
                "bounded format payload must contain at least one byte",
            ));
        }
        let identity = ContentIdentity {
            stable_id: stable_id.into(),
            size_bytes: Some(size_bytes),
            generation: Some(format!("invocation-local-size:{size_bytes}")),
            checksum: None,
            strength: GenerationStrength::Weak,
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
        let lease = reserve(
            memory,
            ReservationRequest::new(
                ConsumerKey::new("bounded-format-input", MemoryClass::Source)?,
                size_bytes,
            )?,
        )
        .await?;
        let payload = AccountedBytes::new(Bytes::from(payload), lease)?;
        Self::from_accounted_bytes(stable_id, payload)
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

pub type FormatBatchStream = std::pin::Pin<Box<dyn Stream<Item = Result<Batch>> + Send + 'static>>;

pub struct FormatStreamRead {
    pub schema: SchemaRef,
    pub batches: FormatBatchStream,
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
    let stream = decode_format_stream(driver, source, request).await?;
    let schema = Arc::clone(&stream.schema);
    let batches = stream.batches.try_collect::<Vec<_>>().await?;
    Ok(BoundedFormatRead { schema, batches })
}

/// Executes a registered codec against a byte source and streams physical
/// batches as soon as the codec emits them. When `request.schema` is present,
/// the output schema is already compiled and no discovery/current-schema
/// pre-scan is performed. When it is absent, this helper retains bounded-format
/// behavior and therefore requires a known finite payload length before
/// discovery.
pub async fn decode_format_stream(
    driver: Arc<dyn FormatDriver>,
    source: Arc<dyn ByteSource>,
    request: BoundedFormatRequest,
) -> Result<FormatStreamRead> {
    let BoundedFormatRequest {
        options,
        read_options,
        schema,
        source_position,
        memory,
        cancellation,
    } = request;
    let canonical_options = driver.canonical_options(options)?;
    let decode_schema = match schema {
        Some(schema) => schema,
        None => {
            let size_bytes = source.identity().size_bytes.ok_or_else(|| {
                CdfError::contract(
                    "format discovery requires a known finite payload length; provide a compiled schema to stream an unbounded source",
                )
            })?;
            let observation = driver
                .discover(
                    Arc::clone(&source),
                    FormatDiscoveryRequest {
                        options: canonical_options.clone(),
                        discovery_kind: driver.descriptor().discovery.default_kind,
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
    let schema = decode_schema.decoder_schema.clone();
    let state = FormatStreamState {
        source,
        session,
        units,
        frontiers,
        next_unit: 0,
        current: None,
        read_options,
        decode_schema,
        source_position,
        target_batch_bytes,
        memory,
        cancellation,
        final_release_done: false,
    };
    Ok(FormatStreamRead {
        schema,
        batches: Box::pin(stream::try_unfold(state, decode_format_stream_next)),
    })
}

struct FormatStreamState {
    source: Arc<dyn ByteSource>,
    session: Arc<dyn crate::FormatDecodeSession>,
    units: Vec<crate::DecodeUnitPlan>,
    frontiers: Option<Vec<u64>>,
    next_unit: usize,
    current: Option<crate::PhysicalDecodeStream>,
    read_options: ReadOptions,
    decode_schema: DecodeSchemaPlan,
    source_position: Option<SourcePosition>,
    target_batch_bytes: u64,
    memory: Arc<dyn MemoryCoordinator>,
    cancellation: RunCancellation,
    final_release_done: bool,
}

async fn decode_format_stream_next(
    mut state: FormatStreamState,
) -> Result<Option<(Batch, FormatStreamState)>> {
    loop {
        state.cancellation.check()?;
        if let Some(current) = &mut state.current {
            if let Some(batch) = current.try_next().await? {
                return Ok(Some((batch.into_batch()?, state)));
            }
            state.current = None;
            let completed_ordinal = state
                .next_unit
                .checked_sub(1)
                .ok_or_else(|| CdfError::internal("format stream completed before starting"))?;
            if let Some(frontiers) = &state.frontiers {
                state.source.release_before(frontiers[completed_ordinal])?;
            }
            continue;
        }
        let Some(unit) = state.units.get(state.next_unit).cloned() else {
            if !state.final_release_done {
                if let Some(size_bytes) = state.source.identity().size_bytes {
                    state.source.release_before(size_bytes)?;
                }
                state.final_release_done = true;
            }
            return Ok(None);
        };
        state.next_unit = state
            .next_unit
            .checked_add(1)
            .ok_or_else(|| CdfError::data("format stream unit ordinal overflowed"))?;
        let decoded = state
            .session
            .decode(PhysicalDecodeRequest {
                unit,
                resource_id: state.read_options.resource_id.clone(),
                partition_id: state.read_options.partition_id.clone(),
                batch_id_prefix: state.read_options.batch_id_prefix.clone(),
                schema: state.decode_schema.clone(),
                source_position: state.source_position.clone(),
                projection: None,
                predicates: Vec::new(),
                target_batch_rows: state.read_options.batch_size,
                target_batch_bytes: state.target_batch_bytes,
                memory: Arc::clone(&state.memory),
                cancellation: state.cancellation.clone(),
            })
            .await?;
        state.current = Some(decoded);
    }
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
