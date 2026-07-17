use std::sync::Arc;

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
                discovery_kind: cdf_runtime::FormatDiscoveryKind::FormatMetadata,
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
            reason: "Arrow IPC stream continuation marker was inspected".to_owned(),
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
            let (reader, sampled_bytes) =
                stream_reader(source.as_ref(), request.maximum_bytes, request.cancellation).await?;
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
                estimated_working_set_bytes: size.max(64 * 1024),
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
            let (reader, _) = stream_reader(
                self.source.as_ref(),
                maximum_bytes,
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
            let reservation_bytes = request
                .target_batch_bytes
                .max(request.unit.estimated_working_set_bytes);
            let output_lease = reserve_output(&request, reservation_bytes).await?;
            let state = DecodeState {
                reader,
                physical_schema,
                observed_schema_hash: actual_hash,
                request,
                reservation_bytes,
                output_lease: Some(output_lease),
                sequence: 0,
                finished: false,
            };
            Ok(Box::pin(stream::try_unfold(state, decode_next)) as PhysicalDecodeStream)
        })
    }
}

struct DecodeState {
    reader: StreamReader<AccountedChunksReader>,
    physical_schema: SchemaRef,
    observed_schema_hash: cdf_kernel::SchemaHash,
    request: PhysicalDecodeRequest,
    reservation_bytes: u64,
    output_lease: Option<MemoryLease>,
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
    let lease = state
        .output_lease
        .take()
        .ok_or_else(|| CdfError::internal("Arrow IPC stream output lease missing"))?;
    let physical = AccountedPhysicalBatch::new(batch, lease)?;
    if !state.finished {
        state.output_lease = Some(reserve_output(&state.request, state.reservation_bytes).await?);
    }
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

async fn stream_reader(
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
            preferred_chunk_bytes: maximum_bytes.min(4 * 1024 * 1024),
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

fn ipc_error(error: arrow_schema::ArrowError) -> CdfError {
    CdfError::data(format!("decode Arrow IPC stream: {error}"))
}
