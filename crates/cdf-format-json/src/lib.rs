#![doc = "Streaming JSON format drivers for cdf."]

use std::{
    io::{BufRead, Read},
    sync::Arc,
};

use arrow_json::reader::{ReaderBuilder, infer_json_schema};
use cdf_kernel::{Batch, BatchId, BoxFuture, CdfError, PushdownFidelity, Result};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteStream, AccountedPhysicalBatch, ByteExtent, ByteSource, DecodePlanningRequest,
    DecodeUnitPlan, FormatDetection, FormatDetectionConfidence, FormatDiscoveryRequest,
    FormatDriver, FormatDriverDescriptor, FormatId, FormatProbe, PhysicalDecodeRequest,
    PhysicalDecodeStream, PhysicalSchemaObservation, SequentialReadRequest,
};
use futures_util::{TryStreamExt, stream};

const DISCOVERY_CHUNK_BYTES: u64 = 1024 * 1024;

#[derive(Debug)]
pub struct NdjsonFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl NdjsonFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("ndjson")?,
                semantic_version: "1.0.0".to_owned(),
                aliases: vec!["jsonl".to_owned()],
                extensions: vec!["ndjson".to_owned(), "jsonl".to_owned()],
                mime_types: vec!["application/x-ndjson".to_owned()],
                magic: Vec::new(),
                option_schema: serde_json::json!({
                    "type": "object",
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Unsupported,
                predicate_pushdown: PushdownFidelity::Unsupported,
                decode_unit_policy: "ndjson_stream_v1".to_owned(),
                minimum_working_set_bytes: 1024 * 1024,
                maximum_working_set_bytes: 64 * 1024 * 1024,
            },
        })
    }
}

impl FormatDriver for NdjsonFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        if options.as_object().is_some_and(serde_json::Map::is_empty) {
            Ok(options)
        } else {
            Err(CdfError::contract("NDJSON format options must be empty"))
        }
    }

    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection> {
        let prefix = trim_ascii_whitespace(&probe.prefix);
        Ok(FormatDetection {
            confidence: if prefix.first() == Some(&b'{') {
                FormatDetectionConfidence::Weak
            } else {
                FormatDetectionConfidence::None
            },
            reason: "NDJSON has no strong magic; first non-whitespace byte was inspected"
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
            if request.maximum_bytes == 0 || request.maximum_records == 0 {
                return Err(CdfError::contract(
                    "NDJSON discovery requires nonzero byte and record bounds",
                ));
            }
            let mut input = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: DISCOVERY_CHUNK_BYTES.min(request.maximum_bytes),
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let mut chunks = Vec::new();
            let mut sampled_bytes = 0_u64;
            while sampled_bytes < request.maximum_bytes {
                let Some(chunk) = input.try_next().await? else {
                    break;
                };
                sampled_bytes =
                    sampled_bytes
                        .checked_add(u64::try_from(chunk.payload().len()).map_err(|_| {
                            CdfError::data("NDJSON discovery chunk length exceeds u64")
                        })?)
                        .ok_or_else(|| CdfError::data("NDJSON discovery byte count overflowed"))?;
                if sampled_bytes > request.maximum_bytes {
                    return Err(CdfError::data(format!(
                        "NDJSON discovery source chunk crossed its {}-byte bound",
                        request.maximum_bytes
                    )));
                }
                chunks.push(chunk);
            }
            let reader = AccountedChunksReader::new(&chunks);
            let maximum_records = usize::try_from(request.maximum_records)
                .map_err(|_| CdfError::contract("NDJSON record bound exceeds usize"))?;
            let (schema, sampled_records) = infer_json_schema(reader, Some(maximum_records))
                .map_err(|error| CdfError::data(format!("infer NDJSON schema: {error}")))?;
            let schema = Arc::new(schema);
            Ok(PhysicalSchemaObservation {
                identity: source.identity().clone(),
                observed_schema: cdf_contract::ObservedSchema::from_arrow(schema.as_ref()),
                arrow_schema: schema,
                sampled_bytes,
                sampled_records: u64::try_from(sampled_records)
                    .map_err(|_| CdfError::data("NDJSON sampled record count exceeds u64"))?,
            })
        })
    }

    fn plan_decode_units(
        &self,
        source: Arc<dyn ByteSource>,
        request: DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Vec<DecodeUnitPlan>>> {
        Box::pin(async move {
            self.canonical_options(request.options)?;
            request.cancellation.check()?;
            if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
                return Err(CdfError::contract(
                    "NDJSON planning requires nonzero row and byte batch targets",
                ));
            }
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "NDJSON projection and predicate pushdown are unsupported",
                ));
            }
            Ok(vec![DecodeUnitPlan {
                unit_id: "ndjson-stream".to_owned(),
                ordinal: 0,
                extent: source
                    .identity()
                    .size_bytes
                    .map(|size| ByteExtent::new(0, size))
                    .transpose()?,
                estimated_working_set_bytes: request
                    .target_batch_bytes
                    .clamp(1024 * 1024, 64 * 1024 * 1024),
                independently_retryable: true,
            }])
        })
    }

    fn decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: PhysicalDecodeRequest,
    ) -> BoxFuture<'_, Result<PhysicalDecodeStream>> {
        Box::pin(async move {
            self.canonical_options(request.options.clone())?;
            request.cancellation.check()?;
            request.unit.validate()?;
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "NDJSON projection and predicate pushdown are unsupported",
                ));
            }
            let input = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: request
                        .target_batch_bytes
                        .clamp(64 * 1024, 4 * 1024 * 1024),
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let decoder = ReaderBuilder::new(Arc::clone(&request.physical_schema))
                .with_batch_size(request.target_batch_rows)
                .build_decoder()
                .map_err(|error| CdfError::data(format!("create NDJSON tape decoder: {error}")))?;
            let output_lease = reserve_output(&request).await?;
            let state = DecodeState {
                input,
                current: None,
                offset: 0,
                decoder,
                request,
                output_lease: Some(output_lease),
                sequence: 0,
                finished: false,
            };
            Ok(Box::pin(stream::try_unfold(state, decode_next)) as PhysicalDecodeStream)
        })
    }
}

struct DecodeState {
    input: AccountedByteStream,
    current: Option<cdf_memory::AccountedBytes>,
    offset: usize,
    decoder: arrow_json::reader::Decoder,
    request: PhysicalDecodeRequest,
    output_lease: Option<MemoryLease>,
    sequence: u64,
    finished: bool,
}

async fn decode_next(
    mut state: DecodeState,
) -> Result<Option<(AccountedPhysicalBatch, DecodeState)>> {
    loop {
        state.request.cancellation.check()?;
        if state.finished {
            return Ok(None);
        }
        if state
            .current
            .as_ref()
            .is_none_or(|chunk| state.offset == chunk.payload().len())
        {
            state.current = state.input.try_next().await?;
            state.offset = 0;
            if state.current.is_none() {
                state.finished = true;
            }
        }
        if let Some(chunk) = &state.current {
            let available = &chunk.payload()[state.offset..];
            let consumed = state
                .decoder
                .decode(available)
                .map_err(|error| CdfError::data(format!("decode NDJSON: {error}")))?;
            state.offset += consumed;
            if consumed == available.len() {
                continue;
            }
        }
        let Some(record_batch) = state
            .decoder
            .flush()
            .map_err(|error| CdfError::data(format!("flush NDJSON batch: {error}")))?
        else {
            if state.finished {
                return Ok(None);
            }
            continue;
        };
        let lease = state
            .output_lease
            .take()
            .ok_or_else(|| CdfError::internal("NDJSON output lease missing"))?;
        let batch_id = BatchId::new(format!(
            "{}-u{:08}-b{:08}",
            state.request.batch_id_prefix, state.request.unit.ordinal, state.sequence
        ))?;
        state.sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(|| CdfError::data("NDJSON batch sequence overflowed"))?;
        let mut batch = Batch::from_record_batch(
            batch_id,
            state.request.resource_id.clone(),
            state.request.partition_id.clone(),
            cdf_contract::canonical_arrow_schema_hash(state.request.physical_schema.as_ref())?,
            record_batch,
        )?;
        batch.header.source_position = state.request.source_position.clone();
        let physical = AccountedPhysicalBatch::new(batch, lease)?;
        if !state.finished {
            state.output_lease = Some(reserve_output(&state.request).await?);
        }
        return Ok(Some((physical, state)));
    }
}

async fn reserve_output(request: &PhysicalDecodeRequest) -> Result<MemoryLease> {
    reserve(
        Arc::clone(&request.memory),
        ReservationRequest::new(
            ConsumerKey::new("ndjson-tape-output", MemoryClass::Decode)?,
            request.target_batch_bytes.max(1024 * 1024),
        )?
        .as_minimum_working_set(),
    )
    .await
}

struct AccountedChunksReader<'a> {
    chunks: &'a [cdf_memory::AccountedBytes],
    chunk: usize,
    offset: usize,
}

impl<'a> AccountedChunksReader<'a> {
    fn new(chunks: &'a [cdf_memory::AccountedBytes]) -> Self {
        Self {
            chunks,
            chunk: 0,
            offset: 0,
        }
    }
}

impl Read for AccountedChunksReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        while self.chunk < self.chunks.len() {
            let input = &self.chunks[self.chunk].payload()[self.offset..];
            if input.is_empty() {
                self.chunk += 1;
                self.offset = 0;
                continue;
            }
            let copied = input.len().min(output.len());
            output[..copied].copy_from_slice(&input[..copied]);
            self.offset += copied;
            return Ok(copied);
        }
        Ok(0)
    }
}

impl BufRead for AccountedChunksReader<'_> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        while self.chunk < self.chunks.len()
            && self.offset == self.chunks[self.chunk].payload().len()
        {
            self.chunk += 1;
            self.offset = 0;
        }
        Ok(self
            .chunks
            .get(self.chunk)
            .map(|chunk| &chunk.payload()[self.offset..])
            .unwrap_or_default())
    }

    fn consume(&mut self, amount: usize) {
        let available = self
            .chunks
            .get(self.chunk)
            .map(|chunk| chunk.payload().len().saturating_sub(self.offset))
            .unwrap_or(0);
        self.offset += amount.min(available);
    }
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    &bytes[start..]
}
