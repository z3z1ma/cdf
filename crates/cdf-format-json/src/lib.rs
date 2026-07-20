#![doc = "Streaming JSON format drivers for cdf."]

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    UInt64Array, new_null_array,
};
use arrow_json::reader::{ReaderBuilder, infer_json_schema};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_kernel::{
    Batch, BatchId, BoxFuture, CdfError, PreContractResidualCandidate, PushdownFidelity, Result,
    source_name, with_physical_type,
};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteStream, AccountedChunksReader, AccountedPhysicalBatch, ByteExtent, ByteSource,
    DecodePlanningRequest, DecodeUnitPlan, FormatDecodeSession, FormatDetection,
    FormatDetectionConfidence, FormatDetectionProbe, FormatDiscoveryKind, FormatDiscoveryRequest,
    FormatDriver, FormatDriverDescriptor, FormatId, FormatProbe, PhysicalDecodeRequest,
    PhysicalDecodeStream, PhysicalSchemaObservation, SequentialReadRequest,
};
use futures_util::{TryStreamExt, stream};
use memchr::{memchr, memchr_iter, memrchr};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor},
};
use serde_json::value::RawValue;

const DISCOVERY_CHUNK_BYTES: u64 = 1024 * 1024;
const FULL_CONTENT_INFERENCE_WINDOW_BYTES: u64 = 8 * 1024 * 1024;
const MAXIMUM_DECODE_WORKING_SET_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_CONFIGURED_RECORD_BYTES: u64 = 32 * 1024 * 1024;
const DEFAULT_MAXIMUM_RECORD_BYTES: u64 = 16 * 1024 * 1024;
const MAXIMUM_JSON_NESTING_DEPTH: usize = 256;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct NdjsonOptions {
    maximum_record_bytes: u64,
}

impl Default for NdjsonOptions {
    fn default() -> Self {
        Self {
            maximum_record_bytes: DEFAULT_MAXIMUM_RECORD_BYTES,
        }
    }
}

impl NdjsonOptions {
    fn parse(value: serde_json::Value) -> Result<Self> {
        let options: Self = serde_json::from_value(value)
            .map_err(|error| CdfError::contract(format!("invalid NDJSON options: {error}")))?;
        validate_maximum_record_bytes(options.maximum_record_bytes)?;
        Ok(options)
    }

    fn canonical(self) -> Result<serde_json::Value> {
        serde_json::to_value(self)
            .map_err(|error| CdfError::internal(format!("encode NDJSON options: {error}")))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct JsonDocumentOptions {
    maximum_record_bytes: u64,
    maximum_nesting_depth: usize,
}

impl Default for JsonDocumentOptions {
    fn default() -> Self {
        Self {
            maximum_record_bytes: DEFAULT_MAXIMUM_RECORD_BYTES,
            maximum_nesting_depth: MAXIMUM_JSON_NESTING_DEPTH,
        }
    }
}

impl JsonDocumentOptions {
    fn parse(value: serde_json::Value) -> Result<Self> {
        let options: Self = serde_json::from_value(value)
            .map_err(|error| CdfError::contract(format!("invalid JSON options: {error}")))?;
        validate_maximum_record_bytes(options.maximum_record_bytes)?;
        if options.maximum_nesting_depth == 0
            || options.maximum_nesting_depth > MAXIMUM_JSON_NESTING_DEPTH
        {
            return Err(CdfError::contract(format!(
                "JSON maximum_nesting_depth must be in 1..={MAXIMUM_JSON_NESTING_DEPTH}"
            )));
        }
        Ok(options)
    }

    fn canonical(self) -> Result<serde_json::Value> {
        serde_json::to_value(self)
            .map_err(|error| CdfError::internal(format!("encode JSON options: {error}")))
    }
}

fn validate_maximum_record_bytes(value: u64) -> Result<()> {
    // Every token and string byte consumes at least one record byte. This limit is therefore also
    // a hard token-count and string-size ceiling without adding counters to the decode hot loop.
    if value == 0 || value > MAXIMUM_CONFIGURED_RECORD_BYTES {
        return Err(CdfError::contract(format!(
            "JSON maximum_record_bytes must be in 1..={MAXIMUM_CONFIGURED_RECORD_BYTES}"
        )));
    }
    Ok(())
}

fn validate_json_discovery_kind(kind: FormatDiscoveryKind) -> Result<()> {
    if matches!(
        kind,
        FormatDiscoveryKind::BoundedContent | FormatDiscoveryKind::FullContent
    ) {
        Ok(())
    } else {
        Err(CdfError::contract(
            "JSON format discovery supports `bounded_content` or `full_content`",
        ))
    }
}

async fn infer_full_content_json_schema(
    mut input: AccountedByteStream,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    cancellation: cdf_runtime::RunCancellation,
    maximum_record_bytes: u64,
    target_window_bytes: u64,
) -> Result<(Schema, u64, u64)> {
    validate_maximum_record_bytes(maximum_record_bytes)?;
    if target_window_bytes == 0 {
        return Err(CdfError::contract(
            "full-content JSON inference requires a nonzero window target",
        ));
    }
    let window_capacity = target_window_bytes
        .checked_add(maximum_record_bytes)
        .ok_or_else(|| CdfError::contract("full-content JSON inference window overflowed"))?;
    let capacity = usize::try_from(window_capacity)
        .map_err(|_| CdfError::contract("full-content JSON inference window exceeds usize"))?;
    let working_set_bytes = (96 * 1024 * 1024_u64)
        .max(MAXIMUM_DECODE_WORKING_SET_BYTES)
        .max(maximum_record_bytes.saturating_mul(3));
    let _working_set = reserve(
        memory,
        ReservationRequest::new(
            ConsumerKey::new("json-full-content-inference", MemoryClass::Discovery)?,
            working_set_bytes,
        )?
        .as_minimum_working_set(),
    )
    .await?;
    let mut window = Vec::with_capacity(capacity);
    let mut effective_schema = Schema::empty();
    let mut sampled_bytes = 0_u64;
    let mut sampled_records = 0_u64;
    let mut current_record_bytes = 0_u64;

    while let Some(chunk) = input.try_next().await? {
        cancellation.check()?;
        sampled_bytes = sampled_bytes
            .checked_add(
                u64::try_from(chunk.payload().len())
                    .map_err(|_| CdfError::data("JSON discovery chunk length exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("JSON discovery byte count overflowed"))?;
        let mut offset = 0_usize;
        for newline in memchr_iter(b'\n', chunk.payload()) {
            let record_fragment = newline.saturating_sub(offset);
            current_record_bytes =
                current_record_bytes
                    .checked_add(u64::try_from(record_fragment).map_err(|_| {
                        CdfError::data("JSON discovery record fragment exceeds u64")
                    })?)
                    .ok_or_else(|| CdfError::data("JSON discovery record byte count overflowed"))?;
            if current_record_bytes > maximum_record_bytes {
                return Err(maximum_record_bytes_error(maximum_record_bytes));
            }
            append_discovery_window(&mut window, &chunk.payload()[offset..=newline], capacity)?;
            current_record_bytes = 0;
            offset = newline + 1;
            if u64::try_from(window.len()).unwrap_or(u64::MAX) >= target_window_bytes {
                infer_and_merge_json_window(&mut effective_schema, &mut sampled_records, &window)?;
                window.clear();
            }
        }
        if offset < chunk.payload().len() {
            let fragment = &chunk.payload()[offset..];
            current_record_bytes =
                current_record_bytes
                    .checked_add(u64::try_from(fragment.len()).map_err(|_| {
                        CdfError::data("JSON discovery record fragment exceeds u64")
                    })?)
                    .ok_or_else(|| CdfError::data("JSON discovery record byte count overflowed"))?;
            if current_record_bytes > maximum_record_bytes {
                return Err(maximum_record_bytes_error(maximum_record_bytes));
            }
            append_discovery_window(&mut window, fragment, capacity)?;
        }
    }
    cancellation.check()?;
    if !window.is_empty() {
        infer_and_merge_json_window(&mut effective_schema, &mut sampled_records, &window)?;
    }
    Ok((effective_schema, sampled_bytes, sampled_records))
}

fn append_discovery_window(window: &mut Vec<u8>, bytes: &[u8], capacity: usize) -> Result<()> {
    let required = window
        .len()
        .checked_add(bytes.len())
        .ok_or_else(|| CdfError::data("JSON discovery window length overflowed"))?;
    if required > capacity {
        return Err(CdfError::internal(
            "JSON discovery window exceeded its record-plus-window authority",
        ));
    }
    window.extend_from_slice(bytes);
    Ok(())
}

fn infer_and_merge_json_window(
    effective_schema: &mut Schema,
    sampled_records: &mut u64,
    window: &[u8],
) -> Result<()> {
    let (observed, records) = infer_json_schema(Cursor::new(window), None)
        .map_err(|error| CdfError::data(format!("infer full-content JSON schema: {error}")))?;
    *sampled_records = sampled_records
        .checked_add(
            u64::try_from(records)
                .map_err(|_| CdfError::data("JSON sampled record count exceeds u64"))?,
        )
        .ok_or_else(|| CdfError::data("JSON sampled record count overflowed"))?;
    *effective_schema = merge_json_inferred_schemas(effective_schema, &observed)?;
    Ok(())
}

fn merge_json_inferred_schemas(left: &Schema, right: &Schema) -> Result<Schema> {
    let fields = merge_json_inferred_fields(left.fields(), right.fields(), "$")?;
    let mut metadata = left.metadata().clone();
    for (key, value) in right.metadata() {
        if metadata
            .insert(key.clone(), value.clone())
            .is_some_and(|prior| prior != *value)
        {
            return Err(CdfError::data(format!(
                "JSON inference metadata key {key:?} changed across windows"
            )));
        }
    }
    Ok(Schema::new_with_metadata(fields, metadata))
}

fn merge_json_inferred_fields(
    left: &arrow_schema::Fields,
    right: &arrow_schema::Fields,
    path: &str,
) -> Result<Vec<Arc<Field>>> {
    let mut merged = left.iter().cloned().collect::<Vec<_>>();
    let mut positions = merged
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name().clone(), index))
        .collect::<BTreeMap<_, _>>();
    for right_field in right {
        if let Some(&index) = positions.get(right_field.name()) {
            let left_field = &merged[index];
            let field_path = format!("{path}.{}", right_field.name());
            let data_type = merge_json_inferred_types(
                left_field.data_type(),
                right_field.data_type(),
                &field_path,
            )?;
            let mut metadata = left_field.metadata().clone();
            for (key, value) in right_field.metadata() {
                if metadata
                    .insert(key.clone(), value.clone())
                    .is_some_and(|prior| prior != *value)
                {
                    return Err(CdfError::data(format!(
                        "JSON inference field metadata changed at {field_path}.{key}"
                    )));
                }
            }
            merged[index] = Arc::new(
                Field::new(
                    left_field.name(),
                    data_type,
                    left_field.is_nullable() || right_field.is_nullable(),
                )
                .with_metadata(metadata),
            );
        } else {
            positions.insert(right_field.name().clone(), merged.len());
            merged.push(Arc::clone(right_field));
        }
    }
    Ok(merged)
}

fn merge_json_inferred_types(left: &DataType, right: &DataType, path: &str) -> Result<DataType> {
    use DataType::{Boolean, Float64, Int64, List, Null, Struct, Utf8};
    Ok(match (left, right) {
        (Null, other) | (other, Null) => other.clone(),
        (Struct(left), Struct(right)) => {
            Struct(merge_json_inferred_fields(left, right, path)?.into())
        }
        (List(left), List(right)) => List(Arc::new(Field::new_list_field(
            merge_json_inferred_types(left.data_type(), right.data_type(), path)?,
            true,
        ))),
        (List(item), scalar) if json_inferred_scalar(scalar) => {
            List(Arc::new(Field::new_list_field(
                merge_json_inferred_types(item.data_type(), scalar, path)?,
                true,
            )))
        }
        (scalar, List(item)) if json_inferred_scalar(scalar) => {
            List(Arc::new(Field::new_list_field(
                merge_json_inferred_types(scalar, item.data_type(), path)?,
                true,
            )))
        }
        (Int64, Float64) | (Float64, Int64) => Float64,
        (Boolean, Boolean) => Boolean,
        (Int64, Int64) => Int64,
        (Float64, Float64) => Float64,
        (Utf8, Utf8) => Utf8,
        (left, right) if json_inferred_scalar(left) && json_inferred_scalar(right) => Utf8,
        (left, right) => {
            return Err(CdfError::data(format!(
                "incompatible JSON types across full-content inference windows at {path}: {left:?} versus {right:?}"
            )));
        }
    })
}

fn json_inferred_scalar(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null | DataType::Boolean | DataType::Int64 | DataType::Float64 | DataType::Utf8
    )
}

fn full_content_discovery_evidence(
    sampled_bytes: u64,
    sampled_records: u64,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("content_coverage".to_owned(), "full_content".to_owned()),
        (
            "source_bytes_observed".to_owned(),
            sampled_bytes.to_string(),
        ),
        (
            "source_records_observed".to_owned(),
            sampled_records.to_string(),
        ),
    ])
}

#[derive(Debug)]
pub struct NdjsonFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl NdjsonFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("ndjson")?,
                semantic_version: "1.1.0".to_owned(),
                aliases: vec!["jsonl".to_owned()],
                extensions: vec!["ndjson".to_owned(), "jsonl".to_owned()],
                mime_types: vec!["application/x-ndjson".to_owned()],
                magic: Vec::new(),
                detection_probe: FormatDetectionProbe {
                    prefix_bytes: 4096,
                    suffix_bytes: 0,
                },
                option_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "maximum_record_bytes": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAXIMUM_CONFIGURED_RECORD_BYTES,
                            "default": DEFAULT_MAXIMUM_RECORD_BYTES
                        }
                    },
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Unsupported,
                predicate_pushdown: PushdownFidelity::Unsupported,
                predicate_operators: Vec::new(),
                source_access: cdf_runtime::FormatSourceAccess::Sequential,
                discovery: cdf_runtime::FormatDiscoveryCapabilities::new(
                    cdf_runtime::FormatDiscoveryKind::BoundedContent,
                    [
                        cdf_runtime::FormatDiscoveryKind::BoundedContent,
                        cdf_runtime::FormatDiscoveryKind::FullContent,
                    ],
                )?,
                decode_unit_policy: "ndjson_stream_v1".to_owned(),
                error_isolation: cdf_runtime::FormatErrorIsolation::Record,
                decode_cpu: cdf_runtime::CpuTaskSpec {
                    task_kind: "format.ndjson.decode".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                minimum_working_set_bytes: 1024 * 1024,
                maximum_working_set_bytes: 96 * 1024 * 1024,
            },
        })
    }
}

impl FormatDriver for NdjsonFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        NdjsonOptions::parse(options)?.canonical()
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
            let options = NdjsonOptions::parse(request.options)?;
            request.cancellation.check()?;
            if request.maximum_bytes == 0 || request.maximum_records == 0 {
                return Err(CdfError::contract(
                    "NDJSON discovery requires nonzero byte and record bounds",
                ));
            }
            validate_json_discovery_kind(request.discovery_kind)?;
            let identity = source.identity().clone();
            let mut input = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: match request.discovery_kind {
                        FormatDiscoveryKind::BoundedContent => {
                            DISCOVERY_CHUNK_BYTES.min(request.maximum_bytes)
                        }
                        FormatDiscoveryKind::FullContent => DISCOVERY_CHUNK_BYTES,
                        FormatDiscoveryKind::FormatMetadata => unreachable!(),
                    },
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            if request.discovery_kind == FormatDiscoveryKind::FullContent {
                let (schema, sampled_bytes, sampled_records) = infer_full_content_json_schema(
                    input,
                    Arc::clone(&request.memory),
                    request.cancellation,
                    options.maximum_record_bytes,
                    FULL_CONTENT_INFERENCE_WINDOW_BYTES,
                )
                .await?;
                return Ok(PhysicalSchemaObservation {
                    identity,
                    arrow_schema: Arc::new(schema),
                    sampled_bytes,
                    sampled_records,
                    evidence: full_content_discovery_evidence(sampled_bytes, sampled_records),
                });
            }
            let mut chunks = Vec::new();
            let mut sampled_bytes = 0_u64;
            while sampled_bytes < request.maximum_bytes {
                let Some(chunk) = input.try_next().await? else {
                    break;
                };
                let chunk_bytes = u64::try_from(chunk.payload().len())
                    .map_err(|_| CdfError::data("NDJSON discovery chunk length exceeds u64"))?;
                sampled_bytes = sampled_bytes
                    .saturating_add(chunk_bytes)
                    .min(request.maximum_bytes);
                chunks.push(chunk);
            }
            let reader = AccountedChunksReader::with_byte_limit(chunks, sampled_bytes)?;
            let maximum_records = usize::try_from(request.maximum_records)
                .map_err(|_| CdfError::contract("NDJSON record bound exceeds usize"))?;
            let (schema, sampled_records) = infer_json_schema(reader, Some(maximum_records))
                .map_err(|error| CdfError::data(format!("infer NDJSON schema: {error}")))?;
            let schema = Arc::new(schema);
            Ok(PhysicalSchemaObservation {
                identity,
                arrow_schema: schema,
                sampled_bytes,
                sampled_records: u64::try_from(sampled_records)
                    .map_err(|_| CdfError::data("NDJSON sampled record count exceeds u64"))?,
                evidence: std::collections::BTreeMap::new(),
            })
        })
    }

    fn prepare_decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Arc<dyn FormatDecodeSession>>> {
        Box::pin(async move {
            let options = NdjsonOptions::parse(request.options)?;
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
            let units = vec![DecodeUnitPlan {
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
            }];
            Ok(Arc::new(NdjsonDecodeSession {
                source,
                units,
                maximum_record_bytes: options.maximum_record_bytes,
            }) as Arc<dyn FormatDecodeSession>)
        })
    }
}

struct NdjsonDecodeSession {
    source: Arc<dyn ByteSource>,
    units: Vec<DecodeUnitPlan>,
    maximum_record_bytes: u64,
}

impl FormatDecodeSession for NdjsonDecodeSession {
    fn units(&self) -> &[DecodeUnitPlan] {
        &self.units
    }

    fn decode(
        &self,
        request: PhysicalDecodeRequest,
    ) -> BoxFuture<'_, Result<PhysicalDecodeStream>> {
        Box::pin(async move {
            request.cancellation.check()?;
            self.validate_unit(&request.unit)?;
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "NDJSON projection and predicate pushdown are unsupported",
                ));
            }
            let input = self
                .source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: request
                        .target_batch_bytes
                        .clamp(64 * 1024, 4 * 1024 * 1024),
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            decode_ndjson_stream(input, request, self.maximum_record_bytes).await
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
                semantic_version: "1.1.0".to_owned(),
                aliases: Vec::new(),
                extensions: vec!["json".to_owned()],
                mime_types: vec!["application/json".to_owned()],
                magic: Vec::new(),
                detection_probe: FormatDetectionProbe {
                    prefix_bytes: 4096,
                    suffix_bytes: 0,
                },
                option_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "maximum_record_bytes": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAXIMUM_CONFIGURED_RECORD_BYTES,
                            "default": DEFAULT_MAXIMUM_RECORD_BYTES
                        },
                        "maximum_nesting_depth": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAXIMUM_JSON_NESTING_DEPTH,
                            "default": MAXIMUM_JSON_NESTING_DEPTH
                        }
                    },
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Unsupported,
                predicate_pushdown: PushdownFidelity::Unsupported,
                predicate_operators: Vec::new(),
                source_access: cdf_runtime::FormatSourceAccess::Sequential,
                discovery: cdf_runtime::FormatDiscoveryCapabilities::new(
                    cdf_runtime::FormatDiscoveryKind::BoundedContent,
                    [
                        cdf_runtime::FormatDiscoveryKind::BoundedContent,
                        cdf_runtime::FormatDiscoveryKind::FullContent,
                    ],
                )?,
                decode_unit_policy: "json_document_stream_v1".to_owned(),
                error_isolation: cdf_runtime::FormatErrorIsolation::Record,
                decode_cpu: cdf_runtime::CpuTaskSpec {
                    task_kind: "format.json.decode".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                minimum_working_set_bytes: 1024 * 1024,
                maximum_working_set_bytes: 96 * 1024 * 1024,
            },
        })
    }
}

impl FormatDriver for JsonDocumentFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        JsonDocumentOptions::parse(options)?.canonical()
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
            let options = JsonDocumentOptions::parse(request.options)?;
            request.cancellation.check()?;
            if request.maximum_bytes == 0 || request.maximum_records == 0 {
                return Err(CdfError::contract(
                    "JSON discovery requires nonzero byte and record bounds",
                ));
            }
            validate_json_discovery_kind(request.discovery_kind)?;
            let identity = source.identity().clone();
            let full_content = request.discovery_kind == FormatDiscoveryKind::FullContent;
            let input = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: if full_content {
                        DISCOVERY_CHUNK_BYTES
                    } else {
                        DISCOVERY_CHUNK_BYTES.min(request.maximum_bytes)
                    },
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let sampled_bytes = Arc::new(AtomicU64::new(0));
            let mut framed = frame_json_document(
                input,
                JsonFrameRequest {
                    maximum_input_bytes: if full_content {
                        u64::MAX
                    } else {
                        request.maximum_bytes
                    },
                    maximum_records: (!full_content).then_some(request.maximum_records),
                    preferred_output_chunk_bytes: DISCOVERY_CHUNK_BYTES,
                    maximum_record_bytes: options.maximum_record_bytes,
                    maximum_nesting_depth: options.maximum_nesting_depth,
                    require_terminal_document: full_content,
                    input_counter: Arc::clone(&sampled_bytes),
                    memory: Arc::clone(&request.memory),
                    cancellation: request.cancellation.clone(),
                },
            )?;
            if full_content {
                let (schema, _, sampled_records) = infer_full_content_json_schema(
                    framed,
                    Arc::clone(&request.memory),
                    request.cancellation,
                    options.maximum_record_bytes,
                    FULL_CONTENT_INFERENCE_WINDOW_BYTES,
                )
                .await?;
                let sampled_bytes = sampled_bytes.load(Ordering::Relaxed);
                return Ok(PhysicalSchemaObservation {
                    identity,
                    arrow_schema: Arc::new(schema),
                    sampled_bytes,
                    sampled_records,
                    evidence: full_content_discovery_evidence(sampled_bytes, sampled_records),
                });
            }
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
                identity,
                arrow_schema: schema,
                sampled_bytes,
                sampled_records: u64::try_from(sampled_records)
                    .map_err(|_| CdfError::data("JSON sampled record count exceeds u64"))?,
                evidence: std::collections::BTreeMap::new(),
            })
        })
    }

    fn prepare_decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Arc<dyn FormatDecodeSession>>> {
        Box::pin(async move {
            let options = JsonDocumentOptions::parse(request.options)?;
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
            let units = vec![DecodeUnitPlan {
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
            }];
            Ok(Arc::new(JsonDocumentDecodeSession {
                source,
                units,
                options,
            }) as Arc<dyn FormatDecodeSession>)
        })
    }
}

struct JsonDocumentDecodeSession {
    source: Arc<dyn ByteSource>,
    units: Vec<DecodeUnitPlan>,
    options: JsonDocumentOptions,
}

impl FormatDecodeSession for JsonDocumentDecodeSession {
    fn units(&self) -> &[DecodeUnitPlan] {
        &self.units
    }

    fn decode(
        &self,
        request: PhysicalDecodeRequest,
    ) -> BoxFuture<'_, Result<PhysicalDecodeStream>> {
        Box::pin(async move {
            request.cancellation.check()?;
            self.validate_unit(&request.unit)?;
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "JSON projection and predicate pushdown are unsupported",
                ));
            }
            let input = self
                .source
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
                    maximum_input_bytes: self.source.identity().size_bytes.unwrap_or(u64::MAX),
                    maximum_records: None,
                    preferred_output_chunk_bytes: request
                        .target_batch_bytes
                        .clamp(64 * 1024, 4 * 1024 * 1024),
                    maximum_record_bytes: self.options.maximum_record_bytes,
                    maximum_nesting_depth: self.options.maximum_nesting_depth,
                    require_terminal_document: true,
                    input_counter: Arc::new(AtomicU64::new(0)),
                    memory: Arc::clone(&request.memory),
                    cancellation: request.cancellation.clone(),
                },
            )?;
            decode_ndjson_stream(framed, request, self.options.maximum_record_bytes).await
        })
    }
}

#[derive(Clone)]
struct JsonFrameRequest {
    maximum_input_bytes: u64,
    maximum_records: Option<u64>,
    preferred_output_chunk_bytes: u64,
    maximum_record_bytes: u64,
    maximum_nesting_depth: usize,
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
    record_bytes: u64,
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
        || request.maximum_record_bytes == 0
        || request.maximum_nesting_depth == 0
        || request.maximum_nesting_depth > MAXIMUM_JSON_NESTING_DEPTH
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
        record_bytes: 0,
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
                state
                    .request
                    .input_counter
                    .store(state.input_bytes, Ordering::Relaxed);
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
    state
        .request
        .input_counter
        .store(state.input_bytes, Ordering::Relaxed);
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
        observe_json_document_record_byte(state)?;
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
                    state.record_bytes = 0;
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
    state.record_bytes = 1;
    if state.record_bytes > state.request.maximum_record_bytes {
        return Err(maximum_record_bytes_error(
            state.request.maximum_record_bytes,
        ));
    }
    state.output.push(b'{');
    push_close(state, b'}')
}

fn observe_json_document_record_byte(state: &mut JsonFrameState) -> Result<()> {
    state.record_bytes = state
        .record_bytes
        .checked_add(1)
        .ok_or_else(|| CdfError::data("JSON record byte count overflowed"))?;
    if state.record_bytes > state.request.maximum_record_bytes {
        return Err(maximum_record_bytes_error(
            state.request.maximum_record_bytes,
        ));
    }
    Ok(())
}

fn push_close(state: &mut JsonFrameState, close: u8) -> Result<()> {
    if state.depth == state.request.maximum_nesting_depth {
        return Err(CdfError::data(format!(
            "JSON nesting exceeds the configured {}-level limit",
            state.request.maximum_nesting_depth
        )));
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
    maximum_record_bytes: u64,
) -> Result<PhysicalDecodeStream> {
    let decoder = strict_decoder(
        Arc::clone(&request.schema.decoder_schema),
        request.target_batch_rows,
    )?;
    let window_target_bytes = request.target_batch_bytes;
    validate_maximum_record_bytes(maximum_record_bytes)?;
    let output_lease = reserve_output(&request, maximum_record_bytes).await?;
    let state = DecodeState {
        input,
        current: None,
        offset: 0,
        decoder,
        request,
        output_lease: Some(output_lease),
        sequence: 0,
        source_row_ordinal: 0,
        retained: Vec::new(),
        retained_bytes: 0,
        record_bytes: 0,
        window_target_bytes,
        maximum_record_bytes,
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
    source_row_ordinal: u64,
    retained: Vec<RetainedDecodeSpan>,
    retained_bytes: u64,
    record_bytes: u64,
    window_target_bytes: u64,
    maximum_record_bytes: u64,
    finished: bool,
}

struct RetainedDecodeSpan {
    chunk: cdf_memory::AccountedBytes,
    range: Range<usize>,
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
            let available = ndjson_decode_window(
                &chunk.payload()[state.offset..],
                state.retained_bytes,
                state.window_target_bytes,
            );
            let prior_record_bytes = state.record_bytes;
            let observed_record_bytes = observe_ndjson_record_bytes(
                available,
                prior_record_bytes,
                state.maximum_record_bytes,
            )?;
            let start = state.offset;
            let consumed = state
                .decoder
                .decode(available)
                .map_err(|error| CdfError::data(format!("decode NDJSON: {error}")))?;
            state.record_bytes = if consumed == available.len() {
                observed_record_bytes
            } else {
                observe_ndjson_record_bytes(
                    &available[..consumed],
                    prior_record_bytes,
                    state.maximum_record_bytes,
                )?
            };
            state.offset += consumed;
            if consumed > 0 {
                state.retained.push(RetainedDecodeSpan {
                    chunk: chunk.clone(),
                    range: start..state.offset,
                });
                state.retained_bytes =
                    state
                        .retained_bytes
                        .checked_add(u64::try_from(consumed).map_err(|_| {
                            CdfError::data("NDJSON retained byte count exceeds u64")
                        })?)
                        .ok_or_else(|| CdfError::data("NDJSON retained byte count overflowed"))?;
            }
            if consumed == available.len() {
                let complete_record_boundary = available
                    .get(consumed.saturating_sub(1))
                    .is_some_and(|byte| *byte == b'\n');
                if !complete_record_boundary
                    || (state.retained_bytes < state.window_target_bytes
                        && state.decoder.len() < state.request.target_batch_rows)
                {
                    continue;
                }
            }
        }
        let flushed = state.decoder.flush();
        let (record_batch, candidates, materialized_residuals_complete) = match flushed {
            Ok(Some(batch)) => (batch, Vec::new(), false),
            Ok(None) => {
                if state.finished {
                    if state.sequence == 0 {
                        (
                            RecordBatch::new_empty(Arc::clone(
                                &state.request.schema.decoder_schema,
                            )),
                            Vec::new(),
                            false,
                        )
                    } else {
                        return Ok(None);
                    }
                } else {
                    continue;
                }
            }
            Err(initial) => {
                let recovered = recover_decode_window(
                    &state.retained,
                    state.retained_bytes,
                    &state.request,
                    state.source_row_ordinal,
                )
                .await
                .map_err(|recovery| {
                    CdfError::data(format!(
                        "decode NDJSON window failed ({initial}); record-local recovery failed: {}",
                        recovery.message
                    ))
                })?;
                if recovered.1.is_empty() {
                    return Err(CdfError::data(format!("flush NDJSON batch: {initial}")));
                }
                state.decoder = strict_decoder(
                    Arc::clone(&state.request.schema.decoder_schema),
                    state.request.target_batch_rows,
                )?;
                (recovered.0, recovered.1, true)
            }
        };
        if record_batch.num_rows() == 0 {
            if state.finished && state.sequence != 0 {
                return Ok(None);
            }
            if !state.finished {
                continue;
            }
        }
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
            cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref())?,
            record_batch,
        )?;
        batch.header.source_position = state.request.source_position.clone();
        batch.header.extend_residual_candidates(candidates);
        if materialized_residuals_complete {
            let physical_schema = batch
                .record_batch()
                .ok_or_else(|| CdfError::internal("decoded NDJSON batch lost its Arrow payload"))?
                .schema();
            batch
                .header
                .mark_materialized_output(physical_schema.as_ref())?;
            batch.header.mark_materialized_residuals_complete();
        }
        state.source_row_ordinal = state
            .source_row_ordinal
            .checked_add(batch.header.row_count)
            .ok_or_else(|| CdfError::data("NDJSON source row ordinal overflowed"))?;
        state.retained.clear();
        state.retained_bytes = 0;
        state.record_bytes = 0;
        let physical = AccountedPhysicalBatch::new(batch, lease)?;
        state.window_target_bytes = next_decode_window_target(
            state.window_target_bytes,
            physical.lease().bytes(),
            state.request.target_batch_bytes,
        );
        if !state.finished {
            state.output_lease =
                Some(reserve_output(&state.request, state.maximum_record_bytes).await?);
        }
        return Ok(Some((physical, state)));
    }
}

fn ndjson_decode_window(available: &[u8], retained_bytes: u64, target_batch_bytes: u64) -> &[u8] {
    let remaining = target_batch_bytes.saturating_sub(retained_bytes);
    let search_from = usize::try_from(remaining)
        .unwrap_or(available.len())
        .min(available.len());
    if search_from == available.len() {
        return available;
    }
    memchr(b'\n', &available[search_from..]).map_or(available, |relative| {
        &available[..search_from + relative + 1]
    })
}

fn observe_ndjson_record_bytes(
    bytes: &[u8],
    current_record_bytes: u64,
    maximum_record_bytes: u64,
) -> Result<u64> {
    let Some(first_newline) = memchr(b'\n', bytes) else {
        let record_bytes = current_record_bytes
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| CdfError::data("NDJSON record fragment length exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("NDJSON record byte count overflowed"))?;
        if record_bytes > maximum_record_bytes {
            return Err(maximum_record_bytes_error(maximum_record_bytes));
        }
        return Ok(record_bytes);
    };
    let prefix_bytes = current_record_bytes
        .checked_add(
            u64::try_from(first_newline)
                .map_err(|_| CdfError::data("NDJSON record prefix length exceeds u64"))?,
        )
        .ok_or_else(|| CdfError::data("NDJSON record byte count overflowed"))?;
    if prefix_bytes > maximum_record_bytes {
        return Err(maximum_record_bytes_error(maximum_record_bytes));
    }
    let last_newline = memrchr(b'\n', bytes)
        .ok_or_else(|| CdfError::internal("NDJSON newline observation diverged"))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum_record_bytes {
        let mut previous = first_newline;
        for newline in memchr_iter(b'\n', &bytes[first_newline + 1..]) {
            let absolute = first_newline + 1 + newline;
            if u64::try_from(absolute - previous - 1).unwrap_or(u64::MAX) > maximum_record_bytes {
                return Err(maximum_record_bytes_error(maximum_record_bytes));
            }
            previous = absolute;
        }
    }
    let trailing = u64::try_from(bytes.len() - last_newline - 1)
        .map_err(|_| CdfError::data("NDJSON trailing record fragment exceeds u64"))?;
    if trailing > maximum_record_bytes {
        return Err(maximum_record_bytes_error(maximum_record_bytes));
    }
    Ok(trailing)
}

fn maximum_record_bytes_error(maximum_record_bytes: u64) -> CdfError {
    CdfError::data(format!(
        "JSON record exceeds the planned {maximum_record_bytes}-byte maximum_record_bytes limit; increase format_options.maximum_record_bytes before planning or split the source record"
    ))
}

fn next_decode_window_target(current: u64, observed_output: u64, ceiling: u64) -> u64 {
    let floor = ceiling.clamp(1, 1024 * 1024);
    if observed_output == 0 {
        return ceiling;
    }
    u64::try_from(
        u128::from(current)
            .saturating_mul(u128::from(ceiling))
            .checked_div(u128::from(observed_output))
            .unwrap_or(u128::from(floor))
            .clamp(u128::from(floor), u128::from(ceiling)),
    )
    .unwrap_or(ceiling)
}

fn strict_decoder(schema: SchemaRef, batch_rows: usize) -> Result<arrow_json::reader::Decoder> {
    ReaderBuilder::new(schema)
        .with_batch_size(batch_rows)
        .with_strict_mode(true)
        .build_decoder()
        .map_err(|error| CdfError::data(format!("create JSON tape decoder: {error}")))
}

async fn recover_decode_window(
    spans: &[RetainedDecodeSpan],
    retained_bytes: u64,
    request: &PhysicalDecodeRequest,
    source_row_ordinal: u64,
) -> Result<(RecordBatch, Vec<PreContractResidualCandidate>)> {
    if retained_bytes == 0 {
        return Err(CdfError::data(
            "NDJSON recovery requires a nonempty retained decode window",
        ));
    }
    let recovery_bytes = retained_bytes
        .checked_mul(3)
        .ok_or_else(|| CdfError::data("NDJSON recovery working set overflowed"))?;
    let _recovery_lease = reserve(
        Arc::clone(&request.memory),
        ReservationRequest::new(
            ConsumerKey::new("ndjson-record-recovery", MemoryClass::Decode)?,
            recovery_bytes,
        )?,
    )
    .await?;
    let retained_len = usize::try_from(retained_bytes)
        .map_err(|_| CdfError::data("NDJSON recovery window exceeds usize"))?;
    let mut raw = Vec::with_capacity(retained_len);
    for span in spans {
        raw.extend_from_slice(&span.chunk.payload()[span.range.clone()]);
    }
    if raw.len() != retained_len {
        return Err(CdfError::internal(
            "NDJSON recovery window byte accounting diverged",
        ));
    }

    let expected = request
        .schema
        .decoder_schema
        .fields()
        .iter()
        .map(|field| {
            (
                source_name(field.as_ref()).unwrap_or_else(|| field.name()),
                field.as_ref(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut sanitized = Vec::with_capacity(raw.len());
    let mut candidates = Vec::new();
    let mut batch_row = 0_usize;
    for line in raw.split(|byte| *byte == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let object: BorrowedJsonObject<'_> = serde_json::from_slice(line)
            .map_err(|error| CdfError::data(format!("decode NDJSON record: {error}")))?;
        let mut seen = BTreeSet::new();
        sanitized.push(b'{');
        let mut wrote = false;
        for (source, value) in object.0 {
            if !seen.insert(source.clone()) {
                return Err(CdfError::data(format!(
                    "NDJSON record {} repeats field {source:?}",
                    source_row_ordinal + batch_row as u64
                )));
            }
            let Some(field) = expected.get(source.as_str()).copied() else {
                candidates.push(raw_residual_candidate(
                    source_row_ordinal + batch_row as u64,
                    batch_row,
                    &source,
                    None,
                    value,
                )?);
                continue;
            };
            let compatible = raw_value_compatible(field, value)?;
            if !compatible && value.get() != "null" {
                candidates.push(raw_residual_candidate(
                    source_row_ordinal + batch_row as u64,
                    batch_row,
                    &source,
                    Some(field.clone()),
                    value,
                )?);
            }
            if wrote {
                sanitized.push(b',');
            }
            serde_json::to_writer(&mut sanitized, field.name()).map_err(|error| {
                CdfError::internal(format!("encode NDJSON recovery field: {error}"))
            })?;
            sanitized.push(b':');
            if compatible {
                sanitized.extend_from_slice(value.get().as_bytes());
            } else {
                sanitized.extend_from_slice(b"null");
            }
            wrote = true;
        }
        for (source, field) in &expected {
            if !seen.contains(*source) && !field.is_nullable() {
                return Err(CdfError::contract(format!(
                    "declared NDJSON field {:?} with source name {source:?} was not observed in record {}",
                    field.name(),
                    source_row_ordinal + batch_row as u64
                )));
            }
        }
        sanitized.extend_from_slice(b"}\n");
        batch_row = batch_row
            .checked_add(1)
            .ok_or_else(|| CdfError::data("NDJSON recovery row count overflowed"))?;
    }
    if batch_row == 0 {
        return Err(CdfError::data(
            "NDJSON recovery window contained no complete records",
        ));
    }

    let nullable = Arc::new(Schema::new_with_metadata(
        request
            .schema
            .decoder_schema
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
            .collect::<Vec<_>>(),
        request.schema.decoder_schema.metadata().clone(),
    ));
    let mut decoder = strict_decoder(nullable, batch_row)?;
    let consumed = decoder
        .decode(&sanitized)
        .map_err(|error| CdfError::data(format!("decode recovered NDJSON window: {error}")))?;
    if consumed != sanitized.len() {
        return Err(CdfError::internal(
            "recovered NDJSON window exceeded its decoder row bound",
        ));
    }
    let recovered = decoder
        .flush()
        .map_err(|error| CdfError::data(format!("flush recovered NDJSON window: {error}")))?
        .ok_or_else(|| CdfError::internal("recovered NDJSON window produced no Arrow batch"))?;
    if recovered.num_rows() != batch_row {
        return Err(CdfError::internal(
            "recovered NDJSON row count diverged from its source window",
        ));
    }
    let nullable_sources = candidates
        .iter()
        .filter(|candidate| candidate.expected_field().is_some())
        .filter_map(|candidate| candidate.source_path().first().map(String::as_str))
        .collect::<BTreeSet<_>>();
    let recovered_schema = Arc::new(Schema::new_with_metadata(
        request
            .schema
            .decoder_schema
            .fields()
            .iter()
            .map(|field| {
                let source = source_name(field.as_ref()).unwrap_or_else(|| field.name());
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_nullable(field.is_nullable() || nullable_sources.contains(source)),
                )
            })
            .collect::<Vec<_>>(),
        request.schema.decoder_schema.metadata().clone(),
    ));
    let recovered = RecordBatch::try_new(recovered_schema, recovered.columns().to_vec())
        .map_err(CdfError::from)?;
    Ok((recovered, candidates))
}

struct BorrowedJsonObject<'a>(Vec<(String, &'a RawValue)>);

impl<'de> Deserialize<'de> for BorrowedJsonObject<'de> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ObjectVisitor;

        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = BorrowedJsonObject<'de>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(source) = map.next_key::<String>()? {
                    fields.push((source, map.next_value::<&'de RawValue>()?));
                }
                Ok(BorrowedJsonObject(fields))
            }
        }

        deserializer.deserialize_map(ObjectVisitor)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedJsonSelection {
    pub byte_range: Range<usize>,
    pub records_present: bool,
    pub top_level_scalar_fields: BTreeMap<String, String>,
}

/// Resolves the bounded streaming-selector grammar without constructing a JSON DOM.
///
/// `$` selects a top-level array. `$.field` selects one top-level object field whose value is an
/// array. The returned range borrows the caller's original accounted body and can therefore be
/// passed to the ordinary JSON document driver as a zero-copy slice.
pub fn select_bounded_json_records(bytes: &[u8], selector: &str) -> Result<BoundedJsonSelection> {
    if selector == "$" {
        let byte_range = trim_ascii_whitespace_range(bytes);
        let records_present =
            json_array_has_records(bytes.get(byte_range.clone()).ok_or_else(|| {
                CdfError::data("JSON record selector `$` requires a top-level array")
            })?)?;
        return Ok(BoundedJsonSelection {
            byte_range,
            records_present,
            top_level_scalar_fields: BTreeMap::new(),
        });
    }
    let Some(field) = selector.strip_prefix("$.") else {
        return Err(CdfError::contract(
            "JSON record selector must be `$` or `$.<field>`",
        ));
    };
    if field.is_empty() || field.contains('.') {
        return Err(CdfError::contract(
            "JSON record selector supports exactly one object field after `$.`",
        ));
    }
    let object: BorrowedJsonObject<'_> = serde_json::from_slice(bytes)
        .map_err(|error| CdfError::data(format!("decode JSON response envelope: {error}")))?;
    let mut selected = None;
    let mut scalars = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for (name, value) in object.0 {
        if !seen.insert(name.clone()) {
            return Err(CdfError::data(format!(
                "JSON response envelope repeats field {name:?}"
            )));
        }
        if name == field {
            if !trim_ascii_whitespace(value.get().as_bytes()).starts_with(b"[") {
                return Err(CdfError::data(format!(
                    "JSON record selector target `{field}` is not an array"
                )));
            }
            selected = Some(raw_value_range(bytes, value)?);
        } else if let Some(marker) = raw_scalar_marker(value)? {
            scalars.insert(name, marker);
        }
    }
    let byte_range = selected.ok_or_else(|| {
        CdfError::data(format!(
            "JSON record selector target `{field}` is missing from response"
        ))
    })?;
    let records_present = json_array_has_records(
        bytes
            .get(byte_range.clone())
            .ok_or_else(|| CdfError::internal("selected JSON range escaped its source body"))?,
    )?;
    Ok(BoundedJsonSelection {
        byte_range,
        records_present,
        top_level_scalar_fields: scalars,
    })
}

fn json_array_has_records(bytes: &[u8]) -> Result<bool> {
    let bytes = trim_ascii_whitespace(bytes);
    if bytes.first() != Some(&b'[') || bytes.last() != Some(&b']') {
        return Err(CdfError::data(
            "JSON record selector target must be a complete array",
        ));
    }
    Ok(!trim_ascii_whitespace(&bytes[1..bytes.len() - 1]).is_empty())
}

fn raw_value_range(bytes: &[u8], value: &RawValue) -> Result<Range<usize>> {
    let start = (value.get().as_ptr() as usize)
        .checked_sub(bytes.as_ptr() as usize)
        .ok_or_else(|| CdfError::internal("borrowed JSON value precedes its source body"))?;
    let end = start
        .checked_add(value.get().len())
        .ok_or_else(|| CdfError::data("borrowed JSON value range overflowed"))?;
    if end > bytes.len() || bytes.get(start..end) != Some(value.get().as_bytes()) {
        return Err(CdfError::internal(
            "borrowed JSON value range escaped its source body",
        ));
    }
    Ok(start..end)
}

fn raw_scalar_marker(value: &RawValue) -> Result<Option<String>> {
    let raw = value.get();
    Ok(match raw.as_bytes().first().copied() {
        Some(b'"') => Some(serde_json::from_str(raw).map_err(|error| {
            CdfError::data(format!("decode JSON response scalar string: {error}"))
        })?),
        Some(b't' | b'f' | b'-' | b'0'..=b'9') => Some(raw.to_owned()),
        Some(b'n' | b'{' | b'[') => None,
        Some(_) => {
            return Err(CdfError::data(
                "JSON response scalar contains an unsupported token",
            ));
        }
        None => return Err(CdfError::data("JSON response scalar is empty")),
    })
}

fn trim_ascii_whitespace_range(bytes: &[u8]) -> Range<usize> {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    start..end
}

fn raw_value_compatible(field: &Field, value: &RawValue) -> Result<bool> {
    let field = field.clone().with_nullable(true);
    let schema = Arc::new(Schema::new([Arc::new(field.clone())]));
    let mut encoded = Vec::with_capacity(field.name().len() + value.get().len() + 8);
    encoded.push(b'{');
    serde_json::to_writer(&mut encoded, field.name())
        .map_err(|error| CdfError::internal(format!("encode JSON field probe: {error}")))?;
    encoded.push(b':');
    encoded.extend_from_slice(value.get().as_bytes());
    encoded.extend_from_slice(b"}\n");
    let mut decoder = strict_decoder(schema, 1)?;
    let consumed = decoder
        .decode(&encoded)
        .map_err(|error| CdfError::data(format!("parse JSON field probe: {error}")))?;
    if consumed != encoded.len() {
        return Err(CdfError::internal(
            "JSON field probe exceeded its one-row decoder bound",
        ));
    }
    match decoder.flush() {
        Ok(Some(batch)) => Ok(!batch.column(0).is_null(0) || value.get() == "null"),
        Ok(None) => Err(CdfError::internal("JSON field probe produced no row")),
        Err(_) => Ok(false),
    }
}

fn raw_residual_candidate(
    source_row_ordinal: u64,
    batch_row_ordinal: usize,
    source: &str,
    expected_field: Option<Field>,
    value: &RawValue,
) -> Result<PreContractResidualCandidate> {
    let (observed_field, values) = raw_residual_array(source, value)?;
    PreContractResidualCandidate::new(
        source_row_ordinal,
        batch_row_ordinal,
        vec![source.to_owned()],
        observed_field,
        expected_field,
        values,
        0,
    )
}

fn raw_residual_array(source: &str, value: &RawValue) -> Result<(Field, ArrayRef)> {
    let raw = value.get();
    let (kind, values): (&str, ArrayRef) = match raw.as_bytes().first().copied() {
        Some(b'n') if raw == "null" => ("null", new_null_array(&DataType::Null, 1)),
        Some(b't') if raw == "true" => (
            "boolean",
            Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef,
        ),
        Some(b'f') if raw == "false" => (
            "boolean",
            Arc::new(BooleanArray::from(vec![Some(false)])) as ArrayRef,
        ),
        Some(b'\"') => (
            "string",
            Arc::new(StringArray::from(vec![Some(
                serde_json::from_str::<String>(raw).map_err(|error| {
                    CdfError::data(format!("decode JSON residual string: {error}"))
                })?,
            )])) as ArrayRef,
        ),
        Some(b'{') => (
            "object",
            Arc::new(BinaryArray::from(vec![Some(raw.as_bytes())])) as ArrayRef,
        ),
        Some(b'[') => (
            "array",
            Arc::new(BinaryArray::from(vec![Some(raw.as_bytes())])) as ArrayRef,
        ),
        Some(_) if !raw.contains(['.', 'e', 'E']) => {
            if let Ok(number) = raw.parse::<i64>() {
                (
                    "number",
                    Arc::new(Int64Array::from(vec![Some(number)])) as ArrayRef,
                )
            } else if let Ok(number) = raw.parse::<u64>() {
                (
                    "number",
                    Arc::new(UInt64Array::from(vec![Some(number)])) as ArrayRef,
                )
            } else {
                (
                    "number-raw",
                    Arc::new(BinaryArray::from(vec![Some(raw.as_bytes())])) as ArrayRef,
                )
            }
        }
        Some(_) => match raw.parse::<f64>() {
            Ok(number) if number.is_finite() => (
                "number",
                Arc::new(Float64Array::from(vec![Some(number)])) as ArrayRef,
            ),
            _ => (
                "number-raw",
                Arc::new(BinaryArray::from(vec![Some(raw.as_bytes())])) as ArrayRef,
            ),
        },
        None => return Err(CdfError::data("JSON residual value is empty")),
    };
    let field = with_physical_type(
        Field::new(source, values.data_type().clone(), true),
        format!("json:{kind}"),
    );
    Ok((field, values))
}

async fn reserve_output(
    request: &PhysicalDecodeRequest,
    maximum_record_bytes: u64,
) -> Result<MemoryLease> {
    let input_window_bytes = request
        .target_batch_bytes
        .max(maximum_record_bytes)
        .clamp(1024 * 1024, MAXIMUM_CONFIGURED_RECORD_BYTES);
    let total_working_set_bytes =
        MAXIMUM_DECODE_WORKING_SET_BYTES.max(maximum_record_bytes.saturating_mul(3));
    let output_authority_bytes = total_working_set_bytes
        .checked_sub(input_window_bytes)
        .ok_or_else(|| CdfError::internal("NDJSON decode working-set split underflowed"))?;
    reserve(
        Arc::clone(&request.memory),
        ReservationRequest::new(
            ConsumerKey::new("ndjson-tape-output", MemoryClass::Decode)?,
            output_authority_bytes,
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
    use std::{collections::BTreeMap, fmt::Write as _, time::Instant};

    use arrow_array::{BinaryArray, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{PartitionId, ResourceId, physical_type};
    use cdf_memory::{
        AccountedBytes, DeterministicMemoryCoordinator, MemoryCoordinator, reserve_blocking,
    };
    use cdf_runtime::{
        BoundedFormatRequest, DecodeSchemaPlan, MemoryByteSource, ReadOptions,
        decode_bounded_format,
    };
    use futures_util::{FutureExt, StreamExt, TryStreamExt, stream};

    use super::*;

    fn frame_with_depth(
        input: &[u8],
        maximum_records: Option<u64>,
        maximum_nesting_depth: usize,
    ) -> Result<(Vec<u8>, u64, u64)> {
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
                maximum_record_bytes: DEFAULT_MAXIMUM_RECORD_BYTES,
                maximum_nesting_depth,
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

    fn frame(input: &[u8], maximum_records: Option<u64>) -> Result<(Vec<u8>, u64, u64)> {
        frame_with_depth(input, maximum_records, MAXIMUM_JSON_NESTING_DEPTH)
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

    #[test]
    fn json_document_framing_enforces_the_compiled_depth_limit() {
        let error = frame_with_depth(br#"[{"a":{"b":{"c":1}}}]"#, None, 2).unwrap_err();

        assert!(error.message.contains("2-level limit"), "{error}");
    }

    #[test]
    fn malformed_json_document_corpus_fails_closed_without_retained_memory() {
        for (input, expected) in [
            (br#"[{"a":[1}}]"#.as_slice(), "mismatched delimiters"),
            (
                br#"[{"a":"unterminated}]"#.as_slice(),
                "ended inside a record",
            ),
            (br#"[1]"#.as_slice(), "array entries must be objects"),
            (br#"{"a":1} trailing"#.as_slice(), "trailing non-whitespace"),
            (br#"[{"a":1},"#.as_slice(), "ended after a comma"),
        ] {
            let error = frame(input, None).unwrap_err();
            assert!(error.message.contains(expected), "{input:?}: {error}");
        }
    }

    #[test]
    fn codec_limits_are_explicit_canonical_plan_evidence() {
        let ndjson = NdjsonFormatDriver::new()
            .unwrap()
            .canonical_options(serde_json::json!({}))
            .unwrap();
        assert_eq!(
            ndjson,
            serde_json::json!({"maximum_record_bytes": DEFAULT_MAXIMUM_RECORD_BYTES})
        );
        let json = JsonDocumentFormatDriver::new()
            .unwrap()
            .canonical_options(serde_json::json!({"maximum_nesting_depth": 32}))
            .unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "maximum_nesting_depth": 32,
                "maximum_record_bytes": DEFAULT_MAXIMUM_RECORD_BYTES
            })
        );
        let error = NdjsonFormatDriver::new()
            .unwrap()
            .canonical_options(serde_json::json!({
                "maximum_record_bytes": MAXIMUM_CONFIGURED_RECORD_BYTES + 1
            }))
            .unwrap_err();
        assert!(error.message.contains("maximum_record_bytes"), "{error}");
    }

    #[test]
    fn full_content_discovery_observes_late_fields_beyond_bounded_limits() {
        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(256 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let ndjson = br#"{"id":1}
{"id":2,"late":"observed"}
"#
        .to_vec();
        let source: Arc<dyn ByteSource> = Arc::new(
            futures_executor::block_on(MemoryByteSource::from_bytes(
                "full-content-ndjson",
                ndjson.clone(),
                Arc::clone(&memory),
            ))
            .unwrap(),
        );
        let observation = futures_executor::block_on(NdjsonFormatDriver::new().unwrap().discover(
            Arc::clone(&source),
            FormatDiscoveryRequest {
                options: serde_json::json!({}),
                discovery_kind: FormatDiscoveryKind::FullContent,
                maximum_bytes: 8,
                maximum_records: 1,
                memory: Arc::clone(&memory),
                cancellation: cdf_runtime::RunCancellation::default(),
            },
        ))
        .unwrap();
        assert_eq!(observation.sampled_bytes, ndjson.len() as u64);
        assert_eq!(observation.sampled_records, 2);
        assert_eq!(observation.arrow_schema.field(1).name(), "late");
        assert_eq!(observation.evidence["content_coverage"], "full_content");
        drop(observation);
        drop(source);

        let document = br#"[{"id":1},{"id":2,"late":"observed"}]"#.to_vec();
        let source: Arc<dyn ByteSource> = Arc::new(
            futures_executor::block_on(MemoryByteSource::from_bytes(
                "full-content-json",
                document.clone(),
                Arc::clone(&memory),
            ))
            .unwrap(),
        );
        let observation =
            futures_executor::block_on(JsonDocumentFormatDriver::new().unwrap().discover(
                Arc::clone(&source),
                FormatDiscoveryRequest {
                    options: serde_json::json!({}),
                    discovery_kind: FormatDiscoveryKind::FullContent,
                    maximum_bytes: 8,
                    maximum_records: 1,
                    memory: Arc::clone(&memory),
                    cancellation: cdf_runtime::RunCancellation::default(),
                },
            ))
            .unwrap();
        assert_eq!(observation.sampled_bytes, document.len() as u64);
        assert_eq!(observation.sampled_records, 2);
        assert_eq!(observation.arrow_schema.field(1).name(), "late");
        assert_eq!(observation.evidence["content_coverage"], "full_content");
        drop(observation);
        drop(source);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn full_content_schema_is_invariant_to_transport_rechunking_and_inference_windows() {
        let input = br#"{"id":1,"metric":1,"values":[1],"nested":{"active":true}}
{"id":2,"metric":1.5,"values":2,"nested":{"label":"x"}}
{"id":3,"metric":null,"values":[3.5],"late":"yes"}
"#;
        let (expected, expected_records) =
            infer_json_schema(Cursor::new(input.as_slice()), None).unwrap();
        assert_eq!(expected_records, 3);

        for chunk_bytes in [1_u64, 2, 7, 31, 1024] {
            let coordinator = Arc::new(
                DeterministicMemoryCoordinator::new(256 * 1024 * 1024, BTreeMap::new()).unwrap(),
            );
            let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
            let source: Arc<dyn ByteSource> = Arc::new(
                futures_executor::block_on(MemoryByteSource::from_bytes(
                    format!("rechunk-{chunk_bytes}"),
                    input.to_vec(),
                    Arc::clone(&memory),
                ))
                .unwrap(),
            );
            let stream =
                futures_executor::block_on(source.open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: chunk_bytes,
                    cancellation: cdf_runtime::RunCancellation::default(),
                }))
                .unwrap();
            let (observed, sampled_bytes, sampled_records) =
                futures_executor::block_on(infer_full_content_json_schema(
                    stream,
                    Arc::clone(&memory),
                    cdf_runtime::RunCancellation::default(),
                    DEFAULT_MAXIMUM_RECORD_BYTES,
                    32,
                ))
                .unwrap();
            assert_eq!(observed, expected, "chunk size {chunk_bytes}");
            assert_eq!(sampled_bytes, input.len() as u64);
            assert_eq!(sampled_records, 3);
            drop(source);
            assert_eq!(coordinator.snapshot().current_bytes, 0);
        }

        for seed in 1_u64..=32 {
            let coordinator = Arc::new(
                DeterministicMemoryCoordinator::new(256 * 1024 * 1024, BTreeMap::new()).unwrap(),
            );
            let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
            let mut state = seed;
            let mut offset = 0_usize;
            let mut chunks = Vec::new();
            while offset < input.len() {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let length = (state as usize % 37 + 1).min(input.len() - offset);
                let lease = reserve_blocking(
                    Arc::clone(&memory),
                    &ReservationRequest::new(
                        ConsumerKey::new(
                            format!("json-random-rechunk-{seed}-{offset}"),
                            MemoryClass::Source,
                        )
                        .unwrap(),
                        length as u64,
                    )
                    .unwrap(),
                )
                .unwrap();
                chunks.push(Ok(AccountedBytes::new(
                    bytes::Bytes::copy_from_slice(&input[offset..offset + length]),
                    lease,
                )
                .unwrap()));
                offset += length;
            }
            let stream: AccountedByteStream = Box::pin(stream::iter(chunks));
            let (observed, sampled_bytes, sampled_records) =
                futures_executor::block_on(infer_full_content_json_schema(
                    stream,
                    Arc::clone(&memory),
                    cdf_runtime::RunCancellation::default(),
                    DEFAULT_MAXIMUM_RECORD_BYTES,
                    32,
                ))
                .unwrap();
            assert_eq!(observed, expected, "random rechunk seed {seed}");
            assert_eq!(sampled_bytes, input.len() as u64);
            assert_eq!(sampled_records, 3);
            assert_eq!(coordinator.snapshot().current_bytes, 0);
        }
    }

    #[test]
    fn bounded_selector_returns_zero_copy_array_range_and_scalar_pagination() {
        let body =
            br#" {"count":2,"next":"page-2","ignored":null,"items" : [ {"id":1}, {"id":2} ]} "#;
        let selected = select_bounded_json_records(body, "$.items").unwrap();

        assert_eq!(&body[selected.byte_range], br#"[ {"id":1}, {"id":2} ]"#);
        assert!(selected.records_present);
        assert_eq!(
            selected.top_level_scalar_fields,
            BTreeMap::from([
                ("count".to_owned(), "2".to_owned()),
                ("next".to_owned(), "page-2".to_owned())
            ])
        );
    }

    #[test]
    fn bounded_selector_rejects_duplicate_and_non_array_targets() {
        let duplicate =
            select_bounded_json_records(br#"{"items":[],"items":[]}"#, "$.items").unwrap_err();
        assert!(duplicate.message.contains("repeats field"), "{duplicate}");
        let scalar = select_bounded_json_records(br#"{"items":1}"#, "$.items").unwrap_err();
        assert!(scalar.message.contains("not an array"), "{scalar}");
        let empty = select_bounded_json_records(br#"{"items": [ ]}"#, "$.items").unwrap();
        assert!(!empty.records_present);
    }

    #[test]
    #[ignore = "release performance envelope"]
    fn rest_selector_tape_decode_release_envelope() {
        const RECORDS: u64 = 262_144;
        const ITERATIONS: usize = 5;
        const PARALLELISM: usize = 2;
        let mut document = String::with_capacity(RECORDS as usize * 52);
        document.push_str(r#"{"next":"done","items":["#);
        for id in 0..RECORDS {
            if id != 0 {
                document.push(',');
            }
            write!(
                document,
                r#"{{"id":{id},"active":true,"category":"benchmark"}}"#
            )
            .unwrap();
        }
        document.push_str("]}");

        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let body_bytes = u64::try_from(document.len()).unwrap();
        let lease = reserve_blocking(
            Arc::clone(&memory),
            &ReservationRequest::new(
                ConsumerKey::new("rest-release-envelope-input", MemoryClass::Source).unwrap(),
                body_bytes,
            )
            .unwrap(),
        )
        .unwrap();
        let body = AccountedBytes::new(bytes::Bytes::from(document), lease).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("active", DataType::Boolean, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let mut observations = Vec::with_capacity(ITERATIONS);
        for iteration in 0..=ITERATIONS {
            let started = Instant::now();
            let decoded_rows = std::thread::scope(|scope| {
                (0..PARALLELISM)
                    .map(|worker| {
                        let body = body.clone();
                        let schema = Arc::clone(&schema);
                        let memory = Arc::clone(&memory);
                        scope.spawn(move || {
                            let selection =
                                select_bounded_json_records(body.payload(), "$.items").unwrap();
                            let selected = body.slice(selection.byte_range).unwrap();
                            let source = Arc::new(
                                MemoryByteSource::from_ephemeral_accounted_bytes(
                                    format!("rest-release-envelope-{iteration}-{worker}"),
                                    selected,
                                )
                                .unwrap(),
                            );
                            let decoded = futures_executor::block_on(decode_bounded_format(
                                Arc::new(JsonDocumentFormatDriver::new().unwrap()),
                                source,
                                BoundedFormatRequest::new(
                                    ReadOptions::new(
                                        ResourceId::new("benchmark.rest").unwrap(),
                                        PartitionId::new(format!("rest-{worker}")).unwrap(),
                                    ),
                                    memory,
                                )
                                .with_schema(DecodeSchemaPlan::fixed_admission(schema)),
                            ))
                            .unwrap();
                            decoded
                                .batches
                                .iter()
                                .map(|batch| batch.header.row_count)
                                .sum::<u64>()
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|worker| worker.join().unwrap())
                    .sum::<u64>()
            });
            assert_eq!(decoded_rows, RECORDS * PARALLELISM as u64);
            let elapsed = started.elapsed();
            if iteration != 0 {
                observations.push((
                    elapsed,
                    body_bytes as f64 * PARALLELISM as f64 / elapsed.as_secs_f64(),
                ));
            }
        }
        observations.sort_by_key(|(elapsed, _)| *elapsed);
        let (median_elapsed, median_bytes_per_second) = observations[ITERATIONS / 2];
        eprintln!(
            "rest selector+tape decode: {} rows, {} bytes in {median_elapsed:?}: {:.1} MiB/s, {:.1} M rows/s",
            RECORDS * PARALLELISM as u64,
            body_bytes * PARALLELISM as u64,
            median_bytes_per_second / (1024.0 * 1024.0),
            RECORDS as f64 * PARALLELISM as f64 / median_elapsed.as_secs_f64() / 1_000_000.0,
        );
        assert!(
            median_bytes_per_second >= 300.0 * 1024.0 * 1024.0,
            "REST aggregate selector+tape decode fell below 300 MiB/s: {median_bytes_per_second} B/s"
        );
        drop(body);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    #[ignore = "release performance envelope"]
    fn rest_selector_tape_decode_exceeds_superseded_dom_shape_by_three_times() {
        const RECORDS: u64 = 262_144;
        const ITERATIONS: usize = 5;
        let mut document = String::with_capacity(RECORDS as usize * 52);
        document.push_str(r#"{"next":"done","items":["#);
        for id in 0..RECORDS {
            if id != 0 {
                document.push(',');
            }
            write!(
                document,
                r#"{{"id":{id},"active":true,"category":"benchmark"}}"#
            )
            .unwrap();
        }
        document.push_str("]}");

        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let body_bytes = u64::try_from(document.len()).unwrap();
        let lease = reserve_blocking(
            Arc::clone(&memory),
            &ReservationRequest::new(
                ConsumerKey::new("rest-dom-comparison-input", MemoryClass::Source).unwrap(),
                body_bytes,
            )
            .unwrap(),
        )
        .unwrap();
        let body = AccountedBytes::new(bytes::Bytes::from(document), lease).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("active", DataType::Boolean, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let mut tape_observations = Vec::with_capacity(ITERATIONS);
        let mut dom_observations = Vec::with_capacity(ITERATIONS);
        for iteration in 0..=ITERATIONS {
            let started = Instant::now();
            let selection = select_bounded_json_records(body.payload(), "$.items").unwrap();
            let selected = body.slice(selection.byte_range).unwrap();
            let source = Arc::new(
                MemoryByteSource::from_ephemeral_accounted_bytes(
                    format!("rest-dom-comparison-{iteration}"),
                    selected,
                )
                .unwrap(),
            );
            let decoded = futures_executor::block_on(decode_bounded_format(
                Arc::new(JsonDocumentFormatDriver::new().unwrap()),
                source,
                BoundedFormatRequest::new(
                    ReadOptions::new(
                        ResourceId::new("benchmark.rest").unwrap(),
                        PartitionId::new("rest-dom-comparison").unwrap(),
                    ),
                    Arc::clone(&memory),
                )
                .with_schema(DecodeSchemaPlan::fixed_admission(Arc::clone(&schema))),
            ))
            .unwrap();
            assert_eq!(
                decoded
                    .batches
                    .iter()
                    .map(|batch| batch.header.row_count)
                    .sum::<u64>(),
                RECORDS
            );
            let tape_elapsed = started.elapsed();
            drop(decoded);

            // This benchmark-only reference intentionally performs less work than the deleted
            // REST implementation: it includes its full DOM, object materialization,
            // reserialization, and Arrow decode, but omits the old per-page schema inference and
            // reconciliation. It is therefore a conservative lower bound for the superseded
            // production shape, not production compatibility code.
            let started = Instant::now();
            let mut root: serde_json::Value = serde_json::from_slice(body.payload()).unwrap();
            let pagination = root
                .as_object()
                .unwrap()
                .iter()
                .filter_map(|(name, value)| {
                    value.as_str().map(|value| (name.clone(), value.to_owned()))
                })
                .collect::<BTreeMap<_, _>>();
            assert_eq!(pagination.get("next").map(String::as_str), Some("done"));
            let records = root
                .get_mut("items")
                .and_then(serde_json::Value::as_array_mut)
                .map(std::mem::take)
                .unwrap();
            let records = records
                .into_iter()
                .map(|record| record.as_object().unwrap().clone())
                .collect::<Vec<_>>();
            let declared = BTreeMap::from([("active", 1_u8), ("category", 4), ("id", 2)]);
            let mut admitted = Vec::with_capacity(records.len());
            for record in &records {
                let mut row = record.clone();
                row.retain(|name, _| declared.contains_key(name.as_str()));
                for (name, kind) in &declared {
                    let value = row.get(*name).unwrap();
                    assert!(match kind {
                        1 => value.is_boolean(),
                        2 => value.is_i64() || value.is_u64(),
                        4 => value.is_string(),
                        _ => false,
                    });
                }
                admitted.push(row);
            }
            let mut inferred = BTreeMap::<String, (u8, bool, bool)>::new();
            for (record_index, record) in admitted.iter().enumerate() {
                for (_, _, seen) in inferred.values_mut() {
                    *seen = false;
                }
                for (name, value) in record {
                    let kind = if value.is_boolean() {
                        1
                    } else if value.is_i64() || value.is_u64() {
                        2
                    } else if value.is_f64() {
                        3
                    } else if value.is_string() {
                        4
                    } else {
                        5
                    };
                    let entry =
                        inferred
                            .entry(name.clone())
                            .or_insert((kind, record_index != 0, true));
                    entry.0 = entry.0.max(kind);
                    entry.2 = true;
                }
                for (_, nullable, seen) in inferred.values_mut() {
                    if !*seen {
                        *nullable = true;
                    }
                }
            }
            assert_eq!(
                inferred
                    .iter()
                    .map(|(name, (kind, nullable, _))| (name.as_str(), *kind, *nullable))
                    .collect::<Vec<_>>(),
                vec![
                    ("active", 1, false),
                    ("category", 4, false),
                    ("id", 2, false)
                ]
            );
            let physical_schema = Schema::new(vec![
                Field::new("active", DataType::Boolean, false),
                Field::new("category", DataType::Utf8, false),
                Field::new("id", DataType::Int64, false),
            ]);
            let physical_schema_hash =
                cdf_kernel::canonical_arrow_schema_hash(&physical_schema).unwrap();
            std::hint::black_box(physical_schema_hash.to_string());
            let mut ndjson = Vec::with_capacity(body.payload().len());
            for record in &records {
                serde_json::to_writer(&mut ndjson, record).unwrap();
                ndjson.push(b'\n');
            }
            let source = Arc::new(
                futures_executor::block_on(MemoryByteSource::from_bytes(
                    format!("rest-dom-reference-{iteration}"),
                    ndjson,
                    Arc::clone(&memory),
                ))
                .unwrap(),
            );
            let decoded = futures_executor::block_on(decode_bounded_format(
                Arc::new(NdjsonFormatDriver::new().unwrap()),
                source,
                BoundedFormatRequest::new(
                    ReadOptions::new(
                        ResourceId::new("benchmark.rest-dom").unwrap(),
                        PartitionId::new("rest-dom-reference").unwrap(),
                    )
                    .with_batch_size(records.len())
                    .unwrap(),
                    Arc::clone(&memory),
                )
                .with_schema(DecodeSchemaPlan::fixed_admission(Arc::clone(&schema))),
            ))
            .unwrap();
            assert_eq!(
                decoded
                    .batches
                    .iter()
                    .map(|batch| batch.header.row_count)
                    .sum::<u64>(),
                RECORDS
            );
            let dom_elapsed = started.elapsed();
            drop(decoded);

            if iteration != 0 {
                tape_observations.push(tape_elapsed);
                dom_observations.push(dom_elapsed);
            }
        }
        tape_observations.sort_unstable();
        dom_observations.sort_unstable();
        let tape = tape_observations[ITERATIONS / 2];
        let dom = dom_observations[ITERATIONS / 2];
        let speedup = dom.as_secs_f64() / tape.as_secs_f64();
        eprintln!(
            "REST selector+tape versus superseded DOM lower bound: {:.1} MiB, tape {tape:?}, DOM {dom:?}, {speedup:.2}x",
            body_bytes as f64 / (1024.0 * 1024.0),
        );
        assert!(
            speedup >= 3.0,
            "REST selector+tape decode did not reach 3x the superseded DOM lower bound: {speedup:.3}x"
        );
        drop(body);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    #[ignore = "release performance envelope"]
    fn full_content_discovery_tracks_arrow_json_roofline() {
        const RECORDS: u64 = 524_288;
        const ITERATIONS: usize = 3;
        let mut input = String::with_capacity(RECORDS as usize * 72);
        for id in 0..RECORDS {
            writeln!(
                input,
                r#"{{"id":{id},"active":true,"metric":12.5,"category":"benchmark"}}"#
            )
            .unwrap();
        }
        let bytes = input.into_bytes();
        let byte_count = bytes.len() as f64;
        let (expected, expected_records) =
            infer_json_schema(Cursor::new(bytes.as_slice()), None).unwrap();
        assert_eq!(expected_records as u64, RECORDS);
        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let source: Arc<dyn ByteSource> = Arc::new(
            futures_executor::block_on(MemoryByteSource::from_bytes(
                "full-content-discovery-envelope",
                bytes.clone(),
                Arc::clone(&memory),
            ))
            .unwrap(),
        );

        let mut reference = Vec::with_capacity(ITERATIONS);
        let mut cdf = Vec::with_capacity(ITERATIONS);
        for _ in 0..ITERATIONS {
            let started = Instant::now();
            let (schema, records) = infer_json_schema(Cursor::new(bytes.as_slice()), None).unwrap();
            reference.push(started.elapsed());
            assert_eq!(schema, expected);
            assert_eq!(records as u64, RECORDS);

            let started = Instant::now();
            let observation =
                futures_executor::block_on(NdjsonFormatDriver::new().unwrap().discover(
                    Arc::clone(&source),
                    FormatDiscoveryRequest {
                        options: serde_json::json!({}),
                        discovery_kind: FormatDiscoveryKind::FullContent,
                        maximum_bytes: 1,
                        maximum_records: 1,
                        memory: Arc::clone(&memory),
                        cancellation: cdf_runtime::RunCancellation::default(),
                    },
                ))
                .unwrap();
            cdf.push(started.elapsed());
            assert_eq!(observation.arrow_schema.as_ref(), &expected);
            assert_eq!(observation.sampled_records, RECORDS);
        }
        reference.sort_unstable();
        cdf.sort_unstable();
        let reference = reference[ITERATIONS / 2];
        let cdf = cdf[ITERATIONS / 2];
        let reference_rate = byte_count / reference.as_secs_f64();
        let cdf_rate = byte_count / cdf.as_secs_f64();
        let roofline_ratio = cdf_rate / reference_rate;
        eprintln!(
            "full-content discovery: {:.1} MiB, Arrow reference {reference:?} ({:.1} MiB/s), CDF {cdf:?} ({:.1} MiB/s), {:.2}x roofline",
            byte_count / (1024.0 * 1024.0),
            reference_rate / (1024.0 * 1024.0),
            cdf_rate / (1024.0 * 1024.0),
            roofline_ratio,
        );
        assert!(
            roofline_ratio >= 0.6,
            "full-content discovery fell below 0.6x raw arrow-json inference: {roofline_ratio:.3}"
        );
        drop(source);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn ndjson_oversized_record_fails_before_publishing_a_batch() {
        let input = br#"{"id":1,"value":"this-record-is-too-large"}
{"id":2,"value":"would-otherwise-be-valid"}
"#;
        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let input_lease = reserve_blocking(
            Arc::clone(&memory),
            &ReservationRequest::new(
                ConsumerKey::new("json-oversized-test-input", MemoryClass::Source).unwrap(),
                u64::try_from(input.len()).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let accounted =
            AccountedBytes::new(bytes::Bytes::copy_from_slice(input), input_lease).unwrap();
        let request = PhysicalDecodeRequest {
            unit: DecodeUnitPlan {
                unit_id: "ndjson-oversized".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: 1024 * 1024,
                independently_retryable: true,
            },
            resource_id: ResourceId::new("events.oversized").unwrap(),
            partition_id: PartitionId::new("file-0001").unwrap(),
            batch_id_prefix: "events-oversized".to_owned(),
            schema: cdf_runtime::DecodeSchemaPlan::verified_physical(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("value", DataType::Utf8, true),
            ]))),
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64,
            target_batch_bytes: 16,
            memory,
            cancellation: cdf_runtime::RunCancellation::default(),
        };
        let error = futures_executor::block_on(async move {
            let input: AccountedByteStream = Box::pin(stream::iter([Ok(accounted)]));
            let mut decoded = decode_ndjson_stream(input, request, 8).await?;
            match decoded.try_next().await {
                Err(error) => Result::<()>::Err(error),
                Ok(_) => Result::<()>::Err(CdfError::internal("oversized NDJSON emitted a batch")),
            }
        })
        .unwrap_err();

        assert!(error.message.contains("planned 8-byte"), "{error}");
        assert!(error.message.contains("maximum_record_bytes"), "{error}");
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn byte_feedback_is_deterministic_and_never_exceeds_the_plan_target() {
        const MIB: u64 = 1024 * 1024;
        assert_eq!(
            next_decode_window_target(16 * MIB, 8 * MIB, 16 * MIB),
            16 * MIB
        );
        assert_eq!(
            next_decode_window_target(16 * MIB, 32 * MIB, 16 * MIB),
            8 * MIB
        );
        assert_eq!(
            next_decode_window_target(8 * MIB, 4 * MIB, 16 * MIB),
            16 * MIB
        );
        assert_eq!(
            next_decode_window_target(16 * MIB, 64 * MIB, 16 * MIB),
            4 * MIB
        );
    }

    #[test]
    fn ndjson_tape_decode_flushes_at_the_byte_target_before_the_row_target() {
        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let chunks = [
            br#"{"id":1,"value":"aaaa"#.as_slice(),
            br#"bbbbbbbb"}
{"id":2,"value":"cccccccc"}
"#
            .as_slice(),
            br#"{"id":3,"value":"dddddddd"}
"#
            .as_slice(),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, input)| {
            let lease = reserve_blocking(
                Arc::clone(&memory),
                &ReservationRequest::new(
                    ConsumerKey::new(
                        format!("json-byte-target-input-{index}"),
                        MemoryClass::Source,
                    )
                    .unwrap(),
                    u64::try_from(input.len()).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
            Ok(AccountedBytes::new(bytes::Bytes::copy_from_slice(input), lease).unwrap())
        })
        .collect::<Vec<Result<_>>>();
        let request = PhysicalDecodeRequest {
            unit: DecodeUnitPlan {
                unit_id: "ndjson-byte-target".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: 1024 * 1024,
                independently_retryable: true,
            },
            resource_id: ResourceId::new("events.byte_target").unwrap(),
            partition_id: PartitionId::new("file-0001").unwrap(),
            batch_id_prefix: "events-byte-target".to_owned(),
            schema: cdf_runtime::DecodeSchemaPlan::verified_physical(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("value", DataType::Utf8, true),
            ]))),
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64,
            target_batch_bytes: 16,
            memory,
            cancellation: cdf_runtime::RunCancellation::default(),
        };
        let batches = futures_executor::block_on(async move {
            let input: AccountedByteStream = Box::pin(stream::iter(chunks));
            let mut decoded =
                decode_ndjson_stream(input, request, DEFAULT_MAXIMUM_RECORD_BYTES).await?;
            let mut batches = Vec::new();
            while let Some(batch) = decoded.try_next().await? {
                batches.push(batch);
            }
            Result::<Vec<AccountedPhysicalBatch>>::Ok(batches)
        })
        .unwrap();

        assert_eq!(batches.len(), 3);
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.batch().header.row_count)
                .collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
        drop(batches);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn ndjson_tape_decode_flushes_at_the_row_target_before_source_eof() {
        let input = br#"{"id":1}
"#;
        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let lease = reserve_blocking(
            Arc::clone(&memory),
            &ReservationRequest::new(
                ConsumerKey::new("json-row-target-input", MemoryClass::Source).unwrap(),
                u64::try_from(input.len()).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let accounted = AccountedBytes::new(bytes::Bytes::copy_from_slice(input), lease).unwrap();
        let request = PhysicalDecodeRequest {
            unit: DecodeUnitPlan {
                unit_id: "ndjson-row-target".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: 1024 * 1024,
                independently_retryable: true,
            },
            resource_id: ResourceId::new("events.row_target").unwrap(),
            partition_id: PartitionId::new("stream-0001").unwrap(),
            batch_id_prefix: "events-row-target".to_owned(),
            schema: cdf_runtime::DecodeSchemaPlan::verified_physical(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
            ]))),
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 1,
            target_batch_bytes: 16 * 1024 * 1024,
            memory,
            cancellation: cdf_runtime::RunCancellation::default(),
        };
        let (batch, decoded) = futures_executor::block_on(async move {
            let input: AccountedByteStream =
                Box::pin(stream::once(async { Ok(accounted) }).chain(stream::pending()));
            let mut decoded = decode_ndjson_stream(input, request, DEFAULT_MAXIMUM_RECORD_BYTES)
                .await
                .unwrap();
            let batch = decoded
                .try_next()
                .now_or_never()
                .expect("row target did not flush before the unbounded source requested more data")
                .unwrap()
                .unwrap();
            (batch, decoded)
        });
        assert_eq!(batch.batch().header.row_count, 1);
        drop(batch);
        drop(decoded);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn ndjson_tape_decode_recovers_drift_with_exact_residual_evidence() {
        let input = br#"{"id":1,"event_type":"order.created","extra":{"source":"mobile"}}
{"id":2,"event_type":"order.updated"}
{"id":3,"event_type":42}
"#;
        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let input_lease = reserve_blocking(
            Arc::clone(&memory),
            &ReservationRequest::new(
                ConsumerKey::new("json-drift-test-input", MemoryClass::Source).unwrap(),
                u64::try_from(input.len()).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let accounted =
            AccountedBytes::new(bytes::Bytes::copy_from_slice(input), input_lease).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("event_type", DataType::Utf8, false),
        ]));
        let request = PhysicalDecodeRequest {
            unit: DecodeUnitPlan {
                unit_id: "ndjson-stream".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: 1024 * 1024,
                independently_retryable: true,
            },
            resource_id: ResourceId::new("events.raw").unwrap(),
            partition_id: PartitionId::new("file-0001").unwrap(),
            batch_id_prefix: "events-raw".to_owned(),
            schema: cdf_runtime::DecodeSchemaPlan::verified_physical(schema),
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64,
            target_batch_bytes: 1024 * 1024,
            memory,
            cancellation: cdf_runtime::RunCancellation::default(),
        };
        let batches = futures_executor::block_on(async move {
            let input: AccountedByteStream = Box::pin(stream::iter([Ok(accounted)]));
            let mut decoded =
                decode_ndjson_stream(input, request, DEFAULT_MAXIMUM_RECORD_BYTES).await?;
            let mut batches = Vec::new();
            while let Some(batch) = decoded.try_next().await? {
                batches.push(batch);
            }
            Result::<Vec<AccountedPhysicalBatch>>::Ok(batches)
        })
        .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = batches[0].batch();
        let record_batch = batch.record_batch().unwrap();
        assert_eq!(record_batch.num_rows(), 3);
        assert_eq!(
            batch.header.observed_schema_hash,
            cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap()
        );
        assert_eq!(
            batch.header.observation_representation,
            cdf_kernel::PhysicalObservationRepresentation::MaterializedOutput
        );
        assert!(
            record_batch
                .schema()
                .field_with_name("event_type")
                .unwrap()
                .is_nullable()
        );
        let event_types = record_batch
            .column_by_name("event_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(event_types.value(0), "order.created");
        assert_eq!(event_types.value(1), "order.updated");
        assert!(event_types.is_null(2));

        let candidates = batch.header.residual_candidates();
        assert_eq!(candidates.len(), 2);
        let extra = candidates
            .iter()
            .find(|candidate| candidate.source_path() == ["extra"])
            .unwrap();
        assert_eq!(physical_type(extra.observed_field()), Some("json:object"));
        assert_eq!(
            extra
                .value()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            br#"{"source":"mobile"}"#
        );
        let drift = candidates
            .iter()
            .find(|candidate| candidate.source_path() == ["event_type"])
            .unwrap();
        assert_eq!(drift.source_row_ordinal(), 2);
        assert_eq!(drift.batch_row_ordinal(), 2);
        assert_eq!(drift.observed_field().data_type(), &DataType::Int64);
        assert_eq!(drift.expected_field().unwrap().data_type(), &DataType::Utf8);
        assert_eq!(
            drift
                .value()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );

        drop(batches);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    #[test]
    fn empty_ndjson_emits_schema_bearing_physical_batch() {
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let request = PhysicalDecodeRequest {
            unit: DecodeUnitPlan {
                unit_id: "empty-ndjson".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: 1024 * 1024,
                independently_retryable: true,
            },
            resource_id: ResourceId::new("events.empty").unwrap(),
            partition_id: PartitionId::new("file-empty").unwrap(),
            batch_id_prefix: "events-empty".to_owned(),
            schema: cdf_runtime::DecodeSchemaPlan::fixed_admission(schema),
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64,
            target_batch_bytes: 1024 * 1024,
            memory,
            cancellation: cdf_runtime::RunCancellation::default(),
        };
        let batches = futures_executor::block_on(async move {
            let input: AccountedByteStream = Box::pin(stream::empty());
            let mut decoded =
                decode_ndjson_stream(input, request, DEFAULT_MAXIMUM_RECORD_BYTES).await?;
            let mut batches = Vec::new();
            while let Some(batch) = decoded.try_next().await? {
                batches.push(batch);
            }
            Result::<Vec<AccountedPhysicalBatch>>::Ok(batches)
        })
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].batch().record_batch().unwrap().num_rows(), 0);
        assert_eq!(
            batches[0].batch().header.observation_representation,
            cdf_kernel::PhysicalObservationRepresentation::ArrowSchema
        );
    }
}
