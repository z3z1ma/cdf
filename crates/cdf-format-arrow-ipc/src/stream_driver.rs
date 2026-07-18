use std::{
    io::Read,
    sync::{Arc, mpsc},
    thread,
};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef;
use cdf_kernel::{Batch, BatchId, BoxFuture, CdfError, PushdownFidelity, Result};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteStream, AccountedChunksReader, AccountedPhysicalBatch, ByteExtent, ByteSource,
    DecodePlanningRequest, DecodeSchemaAuthority, DecodeUnitPlan, FormatDecodeSession,
    FormatDetection, FormatDetectionConfidence, FormatDetectionProbe, FormatDiscoveryRequest,
    FormatDriver, FormatDriverDescriptor, FormatId, FormatProbe, FormatSourceAccess,
    PhysicalDecodeRequest, PhysicalDecodeStream, PhysicalSchemaObservation, SequentialReadRequest,
};
use futures_util::{TryStreamExt, stream};

const CONTINUATION_MARKER: &[u8; 4] = &[0xff, 0xff, 0xff, 0xff];

/// Arrow IPC stream framing used by finite foreign-process payloads. File
/// sources use [`crate::ArrowIpcFileFormatDriver`] and its seekable block plan.
#[derive(Debug)]
pub struct ArrowIpcStreamFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl ArrowIpcStreamFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("arrow_ipc_stream")?,
                semantic_version: "1.0.0".to_owned(),
                aliases: Vec::new(),
                extensions: Vec::new(),
                mime_types: vec!["application/vnd.apache.arrow.stream".to_owned()],
                magic: Vec::new(),
                detection_probe: FormatDetectionProbe {
                    prefix_bytes: 8,
                    suffix_bytes: 0,
                },
                option_schema: serde_json::json!({
                    "type": "object",
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Unsupported,
                predicate_pushdown: PushdownFidelity::Unsupported,
                predicate_operators: Vec::new(),
                source_access: FormatSourceAccess::Sequential,
                discovery: cdf_runtime::FormatDiscoveryCapabilities::only(
                    cdf_runtime::FormatDiscoveryKind::FormatMetadata,
                ),
                decode_unit_policy: "ipc_stream_v1".to_owned(),
                error_isolation: cdf_runtime::FormatErrorIsolation::DecodeUnit,
                decode_cpu: cdf_runtime::CpuTaskSpec {
                    task_kind: "format.arrow_ipc_stream.decode".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                minimum_working_set_bytes: 64 * 1024,
                maximum_working_set_bytes: 4 * 1024 * 1024 * 1024,
            },
        })
    }
}

impl FormatDriver for ArrowIpcStreamFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        if options.as_object().is_some_and(serde_json::Map::is_empty) {
            Ok(options)
        } else {
            Err(CdfError::contract(
                "Arrow IPC stream options must be an empty object",
            ))
        }
    }

    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection> {
        Ok(FormatDetection {
            confidence: if probe.prefix.starts_with(CONTINUATION_MARKER) {
                FormatDetectionConfidence::Weak
            } else {
                FormatDetectionConfidence::None
            },
            reason: "Arrow IPC stream framing is unsupported by the Arrow IPC file framing driver"
                .to_owned(),
        })
    }

    fn discover(
        &self,
        source: Arc<dyn ByteSource>,
        request: FormatDiscoveryRequest,
    ) -> BoxFuture<'_, Result<PhysicalSchemaObservation>> {
        Box::pin(async move {
            self.canonical_options(request.options)?;
            request.cancellation.check()?;
            let (reader, sampled_bytes) = bounded_discovery_stream_reader(
                source.as_ref(),
                request.maximum_bytes,
                request.cancellation,
            )
            .await?;
            Ok(PhysicalSchemaObservation {
                identity: source.identity().clone(),
                arrow_schema: reader.schema(),
                sampled_bytes,
                sampled_records: 0,
                evidence: Default::default(),
            })
        })
    }

    fn prepare_decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Arc<dyn FormatDecodeSession>>> {
        Box::pin(async move {
            self.canonical_options(request.options)?;
            request.cancellation.check()?;
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "Arrow IPC stream projection and predicate pushdown are unsupported",
                ));
            }
            let size = source.identity().size_bytes.ok_or_else(|| {
                CdfError::contract("bounded Arrow IPC stream requires a known payload length")
            })?;
            let units = vec![DecodeUnitPlan {
                unit_id: "ipc-stream".to_owned(),
                ordinal: 0,
                extent: Some(ByteExtent::new(0, size)?),
                estimated_working_set_bytes: request.target_batch_bytes.max(64 * 1024),
                independently_retryable: true,
            }];
            Ok(Arc::new(ArrowIpcStreamDecodeSession { source, units })
                as Arc<dyn FormatDecodeSession>)
        })
    }
}

struct ArrowIpcStreamDecodeSession {
    source: Arc<dyn ByteSource>,
    units: Vec<DecodeUnitPlan>,
}

impl FormatDecodeSession for ArrowIpcStreamDecodeSession {
    fn units(&self) -> &[DecodeUnitPlan] {
        &self.units
    }

    fn decode(
        &self,
        request: PhysicalDecodeRequest,
    ) -> BoxFuture<'_, Result<PhysicalDecodeStream>> {
        Box::pin(async move {
            self.validate_unit(&request.unit)?;
            request.cancellation.check()?;
            let maximum_bytes = self.source.identity().size_bytes.ok_or_else(|| {
                CdfError::contract("bounded Arrow IPC stream requires a known payload length")
            })?;
            let reader = streaming_reader(
                self.source.as_ref(),
                maximum_bytes,
                request.target_batch_bytes.max(1),
                request.cancellation.clone(),
            )
            .await?;
            let physical_schema = reader.schema();
            let actual_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref())?;
            if request.schema.authority == DecodeSchemaAuthority::VerifiedPhysicalObservation {
                let expected = cdf_kernel::canonical_arrow_schema_hash(
                    request.schema.authority_schema.as_ref(),
                )?;
                if expected != actual_hash {
                    return Err(CdfError::data(format!(
                        "Arrow IPC stream schema changed before decode: planned {expected}, observed {actual_hash}"
                    )));
                }
            }
            let state = DecodeState {
                reader,
                physical_schema,
                observed_schema_hash: actual_hash,
                request,
                sequence: 0,
                finished: false,
            };
            Ok(Box::pin(stream::try_unfold(state, decode_next)) as PhysicalDecodeStream)
        })
    }
}

struct DecodeState {
    reader: StreamReader<BlockingAccountedByteStreamReader>,
    physical_schema: SchemaRef,
    observed_schema_hash: cdf_kernel::SchemaHash,
    request: PhysicalDecodeRequest,
    sequence: u64,
    finished: bool,
}

async fn decode_next(
    mut state: DecodeState,
) -> Result<Option<(AccountedPhysicalBatch, DecodeState)>> {
    state.request.cancellation.check()?;
    if state.finished {
        return Ok(None);
    }
    let record_batch = match state.reader.next().transpose().map_err(ipc_error)? {
        Some(batch) => batch,
        None if state.sequence == 0 => {
            state.finished = true;
            RecordBatch::new_empty(Arc::clone(&state.physical_schema))
        }
        None => return Ok(None),
    };
    let batch_id = BatchId::new(format!(
        "{}-u{:08}-b{:08}",
        state.request.batch_id_prefix, state.request.unit.ordinal, state.sequence
    ))?;
    state.sequence = state
        .sequence
        .checked_add(1)
        .ok_or_else(|| CdfError::data("Arrow IPC stream batch sequence overflowed"))?;
    let mut batch = Batch::from_record_batch(
        batch_id,
        state.request.resource_id.clone(),
        state.request.partition_id.clone(),
        state.observed_schema_hash.clone(),
        record_batch,
    )?;
    batch.header.source_position = state.request.source_position.clone();
    let retained = cdf_memory::record_batch_retained_bytes(
        batch
            .record_batch()
            .ok_or_else(|| CdfError::data("Arrow IPC stream driver emitted a non-Arrow batch"))?,
    )?
    .checked_add(batch.header.pre_contract_evidence_retained_bytes()?)
    .ok_or_else(|| CdfError::data("Arrow IPC stream batch retained bytes overflow"))?;
    let lease = reserve_output(&state.request, retained.max(1)).await?;
    let physical = AccountedPhysicalBatch::new(batch, lease)?;
    Ok(Some((physical, state)))
}

async fn reserve_output(request: &PhysicalDecodeRequest, bytes: u64) -> Result<MemoryLease> {
    reserve(
        Arc::clone(&request.memory),
        ReservationRequest::new(
            ConsumerKey::new("arrow-ipc-stream-output", MemoryClass::Decode)?,
            bytes,
        )?
        .as_minimum_working_set(),
    )
    .await
}

async fn bounded_discovery_stream_reader(
    source: &dyn ByteSource,
    maximum_bytes: u64,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<(StreamReader<AccountedChunksReader>, u64)> {
    if maximum_bytes == 0 {
        return Err(CdfError::contract(
            "Arrow IPC stream read requires a nonzero byte bound",
        ));
    }
    let mut input: AccountedByteStream = source
        .open_sequential(SequentialReadRequest {
            preferred_chunk_bytes: maximum_bytes,
            cancellation,
        })
        .await?;
    let mut chunks = Vec::new();
    let mut bytes = 0_u64;
    while let Some(chunk) = input.try_next().await? {
        let length = u64::try_from(chunk.payload().len())
            .map_err(|_| CdfError::data("Arrow IPC stream chunk length exceeds u64"))?;
        bytes = bytes
            .checked_add(length)
            .ok_or_else(|| CdfError::data("Arrow IPC stream byte count overflowed"))?;
        if bytes > maximum_bytes {
            return Err(CdfError::data(format!(
                "Arrow IPC stream exceeds its {maximum_bytes}-byte bound"
            )));
        }
        chunks.push(chunk);
    }
    let reader = AccountedChunksReader::new(chunks);
    let reader = StreamReader::try_new(reader, None).map_err(ipc_error)?;
    Ok((reader, bytes))
}

async fn streaming_reader(
    source: &dyn ByteSource,
    maximum_bytes: u64,
    preferred_chunk_bytes: u64,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<StreamReader<BlockingAccountedByteStreamReader>> {
    if maximum_bytes == 0 {
        return Err(CdfError::contract(
            "Arrow IPC stream read requires a nonzero byte bound",
        ));
    }
    let mut input: AccountedByteStream = source
        .open_sequential(SequentialReadRequest {
            preferred_chunk_bytes,
            cancellation,
        })
        .await?;
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("cdf-arrow-ipc-stream-reader".to_owned())
        .spawn(move || {
            loop {
                let message = futures_executor::block_on(input.try_next());
                let terminal = !matches!(message, Ok(Some(_)));
                if sender.send(message).is_err() || terminal {
                    break;
                }
            }
        })
        .map_err(|error| {
            CdfError::internal(format!("spawn Arrow IPC stream reader thread: {error}"))
        })?;
    let reader = BlockingAccountedByteStreamReader {
        receiver,
        current: None,
        offset: 0,
        consumed_bytes: 0,
        maximum_bytes,
    };
    StreamReader::try_new(reader, None).map_err(ipc_error)
}

struct BlockingAccountedByteStreamReader {
    receiver: mpsc::Receiver<Result<Option<cdf_memory::AccountedBytes>>>,
    current: Option<cdf_memory::AccountedBytes>,
    offset: usize,
    consumed_bytes: u64,
    maximum_bytes: u64,
}

impl BlockingAccountedByteStreamReader {
    fn refill(&mut self) -> std::io::Result<bool> {
        while self
            .current
            .as_ref()
            .is_none_or(|chunk| self.offset == chunk.payload().len())
        {
            self.current = None;
            self.offset = 0;
            let chunk = self
                .receiver
                .recv()
                .map_err(|_| std::io::Error::other("Arrow IPC stream reader stopped"))?
                .map_err(|error| std::io::Error::other(error.message))?;
            let Some(chunk) = chunk else {
                return Ok(false);
            };
            let length = u64::try_from(chunk.payload().len())
                .map_err(|_| std::io::Error::other("Arrow IPC stream chunk length exceeds u64"))?;
            self.consumed_bytes = self
                .consumed_bytes
                .checked_add(length)
                .ok_or_else(|| std::io::Error::other("Arrow IPC stream byte count overflowed"))?;
            if self.consumed_bytes > self.maximum_bytes {
                return Err(std::io::Error::other(format!(
                    "Arrow IPC stream exceeds its {}-byte bound",
                    self.maximum_bytes
                )));
            }
            self.current = Some(chunk);
        }
        Ok(true)
    }
}

impl Read for BlockingAccountedByteStreamReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if !self.refill()? {
            return Ok(0);
        }
        let chunk = self
            .current
            .as_ref()
            .ok_or_else(|| std::io::Error::other("Arrow IPC stream reader lost its chunk"))?;
        let available = &chunk.payload()[self.offset..];
        let copied = available.len().min(output.len());
        output[..copied].copy_from_slice(&available[..copied]);
        self.offset = self.offset.saturating_add(copied);
        Ok(copied)
    }
}

fn ipc_error(error: arrow_schema::ArrowError) -> CdfError {
    CdfError::data(format!("decode Arrow IPC stream: {error}"))
}
