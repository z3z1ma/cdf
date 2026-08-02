//! Public JSON and NDJSON format-driver composition.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow_json::reader::infer_json_schema;
use cdf_kernel::{BoxFuture, CdfError, PushdownFidelity, Result};
use cdf_runtime::{
    AccountedChunksReader, ByteExtent, ByteSource, DecodePlanningRequest, DecodeUnitPlan,
    FormatDecodeSession, FormatDetection, FormatDetectionConfidence, FormatDetectionProbe,
    FormatDiscoveryKind, FormatDiscoveryRequest, FormatDriver, FormatDriverDescriptor, FormatId,
    FormatProbe, PhysicalDecodeRequest, PhysicalDecodeStream, PhysicalSchemaObservation,
    SequentialReadRequest,
};
use futures_util::TryStreamExt;

use crate::decode::decode_ndjson_stream;
use crate::discovery::{
    full_content_discovery_evidence, infer_full_content_json_schema, validate_json_discovery_kind,
};
use crate::framing::{JsonFrameRequest, frame_json_document};
use crate::options::{
    DEFAULT_MAXIMUM_RECORD_BYTES, DISCOVERY_CHUNK_BYTES, FULL_CONTENT_INFERENCE_WINDOW_BYTES,
    JsonDocumentOptions, MAXIMUM_CONFIGURED_RECORD_BYTES, MAXIMUM_JSON_NESTING_DEPTH,
    NdjsonOptions,
};
use crate::raw::trim_ascii_whitespace;

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
