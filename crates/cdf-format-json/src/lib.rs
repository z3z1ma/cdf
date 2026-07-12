#![doc = "Streaming JSON format drivers for cdf."]

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow_json::reader::{ReaderBuilder, infer_json_schema};
use cdf_kernel::{Batch, BatchId, BoxFuture, CdfError, PushdownFidelity, Result};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteStream, AccountedChunksReader, AccountedPhysicalBatch, ByteExtent, ByteSource,
    DecodePlanningRequest, DecodeUnitPlan, FormatDetection, FormatDetectionConfidence,
    FormatDiscoveryRequest, FormatDriver, FormatDriverDescriptor, FormatId, FormatProbe,
    PhysicalDecodeRequest, PhysicalDecodeStream, PhysicalSchemaObservation, SequentialReadRequest,
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
                source_access: cdf_runtime::FormatSourceAccess::Sequential,
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
            let reader = AccountedChunksReader::new(chunks);
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
            decode_ndjson_stream(input, request).await
        })
    }
}

#[derive(Debug)]
pub struct JsonDocumentFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl JsonDocumentFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("json")?,
                semantic_version: "1.0.0".to_owned(),
                aliases: Vec::new(),
                extensions: vec!["json".to_owned()],
                mime_types: vec!["application/json".to_owned()],
                magic: Vec::new(),
                option_schema: serde_json::json!({
                    "type": "object",
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Unsupported,
                predicate_pushdown: PushdownFidelity::Unsupported,
                source_access: cdf_runtime::FormatSourceAccess::Sequential,
                decode_unit_policy: "json_document_stream_v1".to_owned(),
                minimum_working_set_bytes: 1024 * 1024,
                maximum_working_set_bytes: 64 * 1024 * 1024,
            },
        })
    }
}

impl FormatDriver for JsonDocumentFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        if options.as_object().is_some_and(serde_json::Map::is_empty) {
            Ok(options)
        } else {
            Err(CdfError::contract("JSON document options must be empty"))
        }
    }

    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection> {
        let prefix = trim_ascii_whitespace(&probe.prefix);
        Ok(FormatDetection {
            confidence: if matches!(prefix.first(), Some(b'{' | b'[')) {
                FormatDetectionConfidence::Weak
            } else {
                FormatDetectionConfidence::None
            },
            reason: "JSON has no strong magic; the first value delimiter was inspected".to_owned(),
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
                    "JSON discovery requires nonzero byte and record bounds",
                ));
            }
            let input = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: DISCOVERY_CHUNK_BYTES.min(request.maximum_bytes),
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let sampled_bytes = Arc::new(AtomicU64::new(0));
            let mut framed = frame_json_document(
                input,
                JsonFrameRequest {
                    maximum_input_bytes: request.maximum_bytes,
                    maximum_records: Some(request.maximum_records),
                    preferred_output_chunk_bytes: DISCOVERY_CHUNK_BYTES,
                    require_terminal_document: false,
                    input_counter: Arc::clone(&sampled_bytes),
                    memory: Arc::clone(&request.memory),
                    cancellation: request.cancellation,
                },
            )?;
            let mut chunks = Vec::new();
            while let Some(chunk) = framed.try_next().await? {
                chunks.push(chunk);
            }
            let reader = AccountedChunksReader::new(chunks);
            let sampled_bytes = sampled_bytes.load(Ordering::Relaxed);
            let maximum_records = usize::try_from(request.maximum_records)
                .map_err(|_| CdfError::contract("JSON record bound exceeds usize"))?;
            let (schema, sampled_records) = infer_json_schema(reader, Some(maximum_records))
                .map_err(|error| CdfError::data(format!("infer JSON schema: {error}")))?;
            let schema = Arc::new(schema);
            Ok(PhysicalSchemaObservation {
                identity: source.identity().clone(),
                observed_schema: cdf_contract::ObservedSchema::from_arrow(schema.as_ref()),
                arrow_schema: schema,
                sampled_bytes,
                sampled_records: u64::try_from(sampled_records)
                    .map_err(|_| CdfError::data("JSON sampled record count exceeds u64"))?,
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
                    "JSON planning requires nonzero row and byte batch targets",
                ));
            }
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "JSON projection and predicate pushdown are unsupported",
                ));
            }
            Ok(vec![DecodeUnitPlan {
                unit_id: "json-document".to_owned(),
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
                    "JSON projection and predicate pushdown are unsupported",
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
            let framed = frame_json_document(
                input,
                JsonFrameRequest {
                    maximum_input_bytes: source.identity().size_bytes.unwrap_or(u64::MAX),
                    maximum_records: None,
                    preferred_output_chunk_bytes: request
                        .target_batch_bytes
                        .clamp(64 * 1024, 4 * 1024 * 1024),
                    require_terminal_document: true,
                    input_counter: Arc::new(AtomicU64::new(0)),
                    memory: Arc::clone(&request.memory),
                    cancellation: request.cancellation.clone(),
                },
            )?;
            decode_ndjson_stream(framed, request).await
        })
    }
}

#[derive(Clone)]
struct JsonFrameRequest {
    maximum_input_bytes: u64,
    maximum_records: Option<u64>,
    preferred_output_chunk_bytes: u64,
    require_terminal_document: bool,
    input_counter: Arc<AtomicU64>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    cancellation: cdf_runtime::RunCancellation,
}

#[derive(Clone, Copy, Debug)]
enum DocumentPhase {
    Start,
    Single,
    Array { expect_value: bool, seen: bool },
    Done,
}

struct JsonFrameState {
    input: AccountedByteStream,
    current: Option<cdf_memory::AccountedBytes>,
    offset: usize,
    request: JsonFrameRequest,
    phase: DocumentPhase,
    close_stack: [u8; 256],
    depth: usize,
    in_string: bool,
    escaped: bool,
    input_bytes: u64,
    records: u64,
    sample_complete: bool,
    output: Vec<u8>,
    output_lease: Option<MemoryLease>,
    input_finished: bool,
}

fn frame_json_document(
    input: AccountedByteStream,
    request: JsonFrameRequest,
) -> Result<AccountedByteStream> {
    if request.maximum_input_bytes == 0
        || request.preferred_output_chunk_bytes < 2
        || request.maximum_records == Some(0)
    {
        return Err(CdfError::contract(
            "JSON framing requires positive input, record, and output chunk bounds",
        ));
    }
    let state = JsonFrameState {
        input,
        current: None,
        offset: 0,
        request,
        phase: DocumentPhase::Start,
        close_stack: [0; 256],
        depth: 0,
        in_string: false,
        escaped: false,
        input_bytes: 0,
        records: 0,
        sample_complete: false,
        output: Vec::new(),
        output_lease: None,
        input_finished: false,
    };
    Ok(Box::pin(stream::try_unfold(state, frame_next)))
}

async fn frame_next(
    mut state: JsonFrameState,
) -> Result<Option<(cdf_memory::AccountedBytes, JsonFrameState)>> {
    let output_bound = usize::try_from(state.request.preferred_output_chunk_bytes)
        .map_err(|_| CdfError::contract("JSON output chunk bound exceeds usize"))?;
    ensure_frame_output(&mut state).await?;
    loop {
        state.request.cancellation.check()?;
        if state.output.len() + 2 > output_bound {
            return emit_frame_output(state).map(Some);
        }
        if state.sample_complete {
            if state.output.is_empty() {
                return Ok(None);
            }
            return emit_frame_output(state).map(Some);
        }
        if state
            .current
            .as_ref()
            .is_none_or(|chunk| state.offset == chunk.payload().len())
            && !state.input_finished
        {
            state.current = state.input.try_next().await?;
            state.offset = 0;
            state.input_finished = state.current.is_none();
        }
        let Some(chunk) = &state.current else {
            validate_frame_terminal(&state)?;
            state.sample_complete = true;
            continue;
        };
        let byte = chunk.payload()[state.offset];
        state.offset += 1;
        state.input_bytes = state
            .input_bytes
            .checked_add(1)
            .ok_or_else(|| CdfError::data("JSON input byte count overflowed"))?;
        state
            .request
            .input_counter
            .store(state.input_bytes, Ordering::Relaxed);
        if state.input_bytes > state.request.maximum_input_bytes {
            return Err(CdfError::data(format!(
                "JSON discovery exceeded its {}-byte input bound before completing the requested sample",
                state.request.maximum_input_bytes
            )));
        }
        process_frame_byte(&mut state, byte)?;
    }
}

async fn ensure_frame_output(state: &mut JsonFrameState) -> Result<()> {
    if state.output_lease.is_some() {
        return Ok(());
    }
    let lease = reserve(
        Arc::clone(&state.request.memory),
        ReservationRequest::new(
            ConsumerKey::new("json-document-framing", MemoryClass::Transform)?,
            state.request.preferred_output_chunk_bytes,
        )?,
    )
    .await?;
    state.output = Vec::with_capacity(
        usize::try_from(state.request.preferred_output_chunk_bytes)
            .map_err(|_| CdfError::contract("JSON output chunk bound exceeds usize"))?,
    );
    state.output_lease = Some(lease);
    Ok(())
}

fn emit_frame_output(
    mut state: JsonFrameState,
) -> Result<(cdf_memory::AccountedBytes, JsonFrameState)> {
    let lease = state
        .output_lease
        .take()
        .ok_or_else(|| CdfError::internal("JSON framing output lease missing"))?;
    let bytes = cdf_memory::AccountedBytes::new(
        bytes::Bytes::from(std::mem::take(&mut state.output)),
        lease,
    )?;
    Ok((bytes, state))
}

fn process_frame_byte(state: &mut JsonFrameState, byte: u8) -> Result<()> {
    if state.depth != 0 {
        state.output.push(byte);
        if state.in_string {
            if state.escaped {
                state.escaped = false;
            } else if byte == b'\\' {
                state.escaped = true;
            } else if byte == b'"' {
                state.in_string = false;
            }
            return Ok(());
        }
        match byte {
            b'"' => state.in_string = true,
            b'{' => push_close(state, b'}')?,
            b'[' => push_close(state, b']')?,
            b'}' | b']' => {
                if state.close_stack[state.depth - 1] != byte {
                    return Err(CdfError::data("JSON document has mismatched delimiters"));
                }
                state.depth -= 1;
                if state.depth == 0 {
                    state.output.push(b'\n');
                    state.records = state
                        .records
                        .checked_add(1)
                        .ok_or_else(|| CdfError::data("JSON record count overflowed"))?;
                    state.phase = match state.phase {
                        DocumentPhase::Single => DocumentPhase::Done,
                        DocumentPhase::Array { .. } => DocumentPhase::Array {
                            expect_value: false,
                            seen: true,
                        },
                        _ => {
                            return Err(CdfError::internal(
                                "JSON framing closed a record outside a document",
                            ));
                        }
                    };
                    state.sample_complete = state
                        .request
                        .maximum_records
                        .is_some_and(|maximum| state.records >= maximum);
                }
            }
            _ => {}
        }
        return Ok(());
    }

    match state.phase {
        DocumentPhase::Start => {
            if byte.is_ascii_whitespace() {
                return Ok(());
            }
            match byte {
                b'{' => {
                    state.phase = DocumentPhase::Single;
                    start_record(state)?;
                }
                b'[' => {
                    state.phase = DocumentPhase::Array {
                        expect_value: true,
                        seen: false,
                    };
                }
                _ => {
                    return Err(CdfError::data(
                        "JSON file source must be an object or an array of objects",
                    ));
                }
            }
        }
        DocumentPhase::Single | DocumentPhase::Done => {
            if !byte.is_ascii_whitespace() {
                return Err(CdfError::data(
                    "JSON document has trailing non-whitespace data",
                ));
            }
        }
        DocumentPhase::Array { expect_value, seen } => {
            if byte.is_ascii_whitespace() {
                return Ok(());
            }
            if expect_value {
                match byte {
                    b'{' => start_record(state)?,
                    b']' if !seen => state.phase = DocumentPhase::Done,
                    b']' => return Err(CdfError::data("JSON array has a trailing comma")),
                    _ => {
                        return Err(CdfError::data(
                            "JSON file source array entries must be objects",
                        ));
                    }
                }
            } else {
                match byte {
                    b',' => {
                        state.phase = DocumentPhase::Array {
                            expect_value: true,
                            seen,
                        };
                    }
                    b']' => state.phase = DocumentPhase::Done,
                    _ => return Err(CdfError::data("JSON array entries require a comma")),
                }
            }
        }
    }
    Ok(())
}

fn start_record(state: &mut JsonFrameState) -> Result<()> {
    state.output.push(b'{');
    push_close(state, b'}')
}

fn push_close(state: &mut JsonFrameState, close: u8) -> Result<()> {
    if state.depth == state.close_stack.len() {
        return Err(CdfError::data("JSON nesting exceeds the 256-level limit"));
    }
    state.close_stack[state.depth] = close;
    state.depth += 1;
    Ok(())
}

fn validate_frame_terminal(state: &JsonFrameState) -> Result<()> {
    if state.sample_complete && !state.request.require_terminal_document {
        return Ok(());
    }
    if state.depth != 0 || state.in_string || state.escaped {
        return Err(CdfError::data("JSON document ended inside a record"));
    }
    match state.phase {
        DocumentPhase::Done => Ok(()),
        DocumentPhase::Array {
            expect_value: true,
            seen: true,
        } => Err(CdfError::data("JSON array ended after a comma")),
        _ => Err(CdfError::data(
            "JSON document ended before its top-level value completed",
        )),
    }
}

async fn decode_ndjson_stream(
    input: AccountedByteStream,
    request: PhysicalDecodeRequest,
) -> Result<PhysicalDecodeStream> {
    let decoder = ReaderBuilder::new(Arc::clone(&request.physical_schema))
        .with_batch_size(request.target_batch_rows)
        .build_decoder()
        .map_err(|error| CdfError::data(format!("create JSON tape decoder: {error}")))?;
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

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    &bytes[start..]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cdf_memory::{
        AccountedBytes, DeterministicMemoryCoordinator, MemoryCoordinator, reserve_blocking,
    };
    use futures_util::{TryStreamExt, stream};

    use super::*;

    fn frame(input: &[u8], maximum_records: Option<u64>) -> Result<(Vec<u8>, u64, u64)> {
        let coordinator =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let chunks = input
            .iter()
            .enumerate()
            .map(|(index, byte)| {
                let lease = reserve_blocking(
                    Arc::clone(&memory),
                    &ReservationRequest::new(
                        ConsumerKey::new(format!("json-test-input-{index}"), MemoryClass::Source)
                            .unwrap(),
                        1,
                    )
                    .unwrap(),
                )
                .unwrap();
                Ok(AccountedBytes::new(bytes::Bytes::copy_from_slice(&[*byte]), lease).unwrap())
            })
            .collect::<Vec<Result<_>>>();
        let counter = Arc::new(AtomicU64::new(0));
        let mut framed = frame_json_document(
            Box::pin(stream::iter(chunks)),
            JsonFrameRequest {
                maximum_input_bytes: u64::try_from(input.len()).unwrap(),
                maximum_records,
                preferred_output_chunk_bytes: 7,
                require_terminal_document: maximum_records.is_none(),
                input_counter: Arc::clone(&counter),
                memory,
                cancellation: cdf_runtime::RunCancellation::default(),
            },
        )?;
        let output = futures_executor::block_on(async move {
            let mut output = Vec::new();
            while let Some(chunk) = framed.try_next().await? {
                output.extend_from_slice(chunk.payload());
            }
            Result::<Vec<u8>>::Ok(output)
        })?;
        let sampled = counter.load(Ordering::Relaxed);
        let retained = coordinator.snapshot().current_bytes;
        Ok((output, sampled, retained))
    }

    #[test]
    fn json_document_framing_is_invariant_to_one_byte_chunks() {
        let input = br#" [ {"a":"},["}, {"b":{"c":[1,2]}} ] "#;
        let (output, sampled, retained) = frame(input, None).unwrap();

        assert_eq!(
            output,
            br#"{"a":"},["}
{"b":{"c":[1,2]}}
"#
        );
        assert_eq!(sampled, u64::try_from(input.len()).unwrap());
        assert_eq!(retained, 0);
    }

    #[test]
    fn json_document_sampling_stops_after_complete_records() {
        let input = br#"[{"a":1},{"b":2},this-rest-is-not-json"#;
        let (output, sampled, retained) = frame(input, Some(2)).unwrap();

        assert_eq!(output, b"{\"a\":1}\n{\"b\":2}\n");
        assert_eq!(sampled, 16);
        assert_eq!(retained, 0);
    }

    #[test]
    fn json_document_framing_rejects_trailing_commas() {
        let error = frame(br#"[{"a":1},]"#, None).unwrap_err();

        assert!(error.message.contains("trailing comma"), "{error}");
    }
}
