//! Avro OCF and single-object format-driver configuration and discovery.

use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow_avro::schema::FingerprintAlgorithm;
use cdf_kernel::{BoxFuture, CdfError, PushdownFidelity, Result};
use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest, reserve};
use cdf_runtime::{
    ByteExtent, ByteSource, DecodePlanningRequest, DecodeUnitPlan, FormatDecodeSession,
    FormatDetection, FormatDetectionConfidence, FormatDetectionProbe, FormatDiscoveryCapabilities,
    FormatDiscoveryKind, FormatDiscoveryRequest, FormatDriver, FormatDriverDescriptor,
    FormatErrorIsolation, FormatId, FormatProbe, FormatSourceAccess, MagicSignature,
    PhysicalSchemaObservation,
};

use crate::byte_source::AvroByteSource;
use crate::decode::{OcfDecodeSession, SingleObjectDecodeSession};
use crate::errors::{avro_arrow_error, avro_error};
use crate::options::{
    DEFAULT_MAXIMUM_BLOCK_BYTES, DEFAULT_MAXIMUM_BLOCK_RECORDS, DEFAULT_MAXIMUM_BLOCKS,
    DEFAULT_MAXIMUM_DECODED_BLOCK_BYTES, DEFAULT_MAXIMUM_HEADER_BYTES,
    DEFAULT_MAXIMUM_RECORD_BYTES, MAXIMUM_INDIVIDUAL_VALUE_BYTES, MAXIMUM_WORKING_SET_BYTES,
    OCF_HEADER_READ_BYTES, OCF_MAGIC, OcfOptions, SOE_MAGIC, SingleObjectOptions,
};
use crate::planning::{ocf_units, projection_indices, schema_from_header, single_object_schema};
use crate::validation::{
    validate_decode_request, validate_metadata_discovery, validate_seekable_source,
};

#[derive(Clone, Debug)]
pub struct AvroOcfFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl AvroOcfFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("avro_ocf")?,
                semantic_version: "1.0.0".to_owned(),
                aliases: vec!["avro".to_owned()],
                extensions: vec!["avro".to_owned()],
                mime_types: vec!["application/avro".to_owned(), "avro/binary".to_owned()],
                magic: vec![MagicSignature {
                    offset: 0,
                    bytes: OCF_MAGIC.to_vec(),
                    strong: true,
                }],
                detection_probe: FormatDetectionProbe {
                    prefix_bytes: 4,
                    suffix_bytes: 0,
                },
                option_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "maximum_header_bytes": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAXIMUM_INDIVIDUAL_VALUE_BYTES,
                            "default": DEFAULT_MAXIMUM_HEADER_BYTES
                        },
                        "maximum_block_bytes": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAXIMUM_INDIVIDUAL_VALUE_BYTES,
                            "default": DEFAULT_MAXIMUM_BLOCK_BYTES
                        },
                        "maximum_decoded_block_bytes": {
                            "description": "Maximum retained Arrow output produced atomically by one Avro block",
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAXIMUM_WORKING_SET_BYTES,
                            "default": DEFAULT_MAXIMUM_DECODED_BLOCK_BYTES
                        },
                        "maximum_block_records": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 9223372036854775807_u64,
                            "default": DEFAULT_MAXIMUM_BLOCK_RECORDS
                        },
                        "maximum_blocks": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 4294967295_u64,
                            "default": DEFAULT_MAXIMUM_BLOCKS
                        }
                    },
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Exact,
                predicate_pushdown: PushdownFidelity::Unsupported,
                predicate_operators: Vec::new(),
                source_access: FormatSourceAccess::Adaptive,
                discovery: FormatDiscoveryCapabilities::only(FormatDiscoveryKind::FormatMetadata),
                decode_unit_policy: "ocf_block_v1".to_owned(),
                error_isolation: FormatErrorIsolation::DecodeUnit,
                decode_cpu: cdf_runtime::CpuTaskSpec {
                    task_kind: "format.avro_ocf.decode".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                minimum_working_set_bytes: 1024 * 1024,
                maximum_working_set_bytes: MAXIMUM_WORKING_SET_BYTES,
            },
        })
    }
}

impl FormatDriver for AvroOcfFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        OcfOptions::parse(options)?.canonical()
    }

    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection> {
        let matched = probe.prefix.starts_with(OCF_MAGIC);
        Ok(FormatDetection {
            confidence: if matched {
                FormatDetectionConfidence::Strong
            } else {
                FormatDetectionConfidence::None
            },
            reason: if matched {
                "Avro object-container magic matched"
            } else {
                "Avro object-container magic did not match"
            }
            .to_owned(),
        })
    }

    fn discover(
        &self,
        source: Arc<dyn ByteSource>,
        request: FormatDiscoveryRequest,
    ) -> BoxFuture<'_, Result<PhysicalSchemaObservation>> {
        Box::pin(async move {
            request.cancellation.check()?;
            let options = OcfOptions::parse(request.options)?;
            validate_metadata_discovery(request.discovery_kind, request.maximum_bytes)?;
            let size = validate_seekable_source(source.as_ref(), "Avro OCF")?;
            let _discovery_lease = reserve(
                Arc::clone(&request.memory),
                ReservationRequest::new(
                    ConsumerKey::new("avro-ocf-discovery", MemoryClass::Decode)?,
                    request.maximum_bytes,
                )?
                .as_minimum_working_set(),
            )
            .await?;
            let bytes_read = Arc::new(AtomicU64::new(0));
            let mut reader = AvroByteSource::new(
                Arc::clone(&source),
                request.cancellation.clone(),
                request.maximum_bytes,
                Arc::clone(&bytes_read),
            )
            .with_total_budget(request.maximum_bytes);
            // Match arrow-avro's own bounded header hint. Passing the complete
            // discovery budget as one range would over-fetch every small OCF.
            let hint = request.maximum_bytes.min(size).min(16 * 1024);
            let header =
                arrow_avro::reader::async_reader::read_header_info(&mut reader, size, Some(hint))
                    .await
                    .map_err(avro_error)?;
            let sampled_bytes = bytes_read.load(Ordering::Relaxed);
            if sampled_bytes > request.maximum_bytes {
                return Err(CdfError::data(format!(
                    "Avro OCF discovery read {sampled_bytes} bytes above its {}-byte metadata budget",
                    request.maximum_bytes
                )));
            }
            let identity = source.identity().clone();
            let reader = AvroByteSource::new(
                source,
                request.cancellation,
                options.maximum_request_bytes()?,
                Arc::new(AtomicU64::new(0)),
            );
            let schema = schema_from_header(reader, size, header.clone())?;
            let mut evidence = BTreeMap::new();
            evidence.insert(
                "avro.codec".to_owned(),
                header
                    .compression()
                    .map_err(avro_error)?
                    .map_or_else(|| "null".to_owned(), |codec| format!("{codec:?}")),
            );
            let writer_schema = header.writer_schema().map_err(avro_error)?;
            evidence.insert(
                "avro.writer_fingerprint".to_owned(),
                format!(
                    "{:?}",
                    writer_schema
                        .fingerprint(FingerprintAlgorithm::Rabin)
                        .map_err(avro_arrow_error)?
                ),
            );
            Ok(PhysicalSchemaObservation {
                identity,
                arrow_schema: schema,
                sampled_bytes,
                sampled_records: 0,
                evidence,
            })
        })
    }

    fn prepare_decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Arc<dyn FormatDecodeSession>>> {
        Box::pin(async move {
            request.cancellation.check()?;
            validate_decode_request(&request, "Avro OCF")?;
            let options = OcfOptions::parse(request.options)?;
            let size = validate_seekable_source(source.as_ref(), "Avro OCF")?;
            let mut header_reader = AvroByteSource::new(
                Arc::clone(&source),
                request.cancellation.clone(),
                options.maximum_request_bytes()?,
                Arc::new(AtomicU64::new(0)),
            )
            .with_total_budget(options.maximum_header_bytes);
            let header = arrow_avro::reader::async_reader::read_header_info(
                &mut header_reader,
                size,
                Some(OCF_HEADER_READ_BYTES.min(options.maximum_header_bytes)),
            )
            .await
            .map_err(avro_error)?;
            let schema = schema_from_header(
                AvroByteSource::new(
                    Arc::clone(&source),
                    request.cancellation.clone(),
                    options.maximum_request_bytes()?,
                    Arc::new(AtomicU64::new(0)),
                ),
                size,
                header.clone(),
            )?;
            let projection = projection_indices(schema.as_ref(), request.projection.as_deref())?;
            let (units, ranges) = ocf_units(
                source.as_ref(),
                size,
                header.header_len(),
                header.sync(),
                options,
                request.target_batch_bytes,
                request.cancellation.clone(),
            )
            .await?;
            Ok(Arc::new(OcfDecodeSession {
                source,
                size,
                header,
                physical_schema: schema,
                projection,
                options,
                units,
                ranges,
            }) as Arc<dyn FormatDecodeSession>)
        })
    }
}

#[derive(Clone, Debug)]
pub struct AvroSingleObjectFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl AvroSingleObjectFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("avro_single_object")?,
                semantic_version: "1.0.0".to_owned(),
                aliases: vec!["avro_soe".to_owned()],
                extensions: vec!["avrosoe".to_owned()],
                mime_types: vec!["avro/binary".to_owned()],
                magic: vec![MagicSignature {
                    offset: 0,
                    bytes: SOE_MAGIC.to_vec(),
                    strong: true,
                }],
                detection_probe: FormatDetectionProbe {
                    prefix_bytes: 2,
                    suffix_bytes: 0,
                },
                option_schema: serde_json::json!({
                    "type": "object",
                    "required": ["writer_schema"],
                    "properties": {
                        "writer_schema": {
                            "description": "Explicit Avro writer schema; its Rabin fingerprint must match the single encoded datum",
                            "type": ["object", "array", "string"]
                        },
                        "maximum_record_bytes": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAXIMUM_INDIVIDUAL_VALUE_BYTES,
                            "default": DEFAULT_MAXIMUM_RECORD_BYTES
                        }
                    },
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Exact,
                predicate_pushdown: PushdownFidelity::Unsupported,
                predicate_operators: Vec::new(),
                source_access: FormatSourceAccess::Sequential,
                discovery: FormatDiscoveryCapabilities::only(FormatDiscoveryKind::FormatMetadata),
                decode_unit_policy: "single_object_record_v1".to_owned(),
                error_isolation: FormatErrorIsolation::DecodeUnit,
                decode_cpu: cdf_runtime::CpuTaskSpec {
                    task_kind: "format.avro_single_object.decode".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                minimum_working_set_bytes: 1024 * 1024,
                maximum_working_set_bytes: MAXIMUM_WORKING_SET_BYTES,
            },
        })
    }
}

impl FormatDriver for AvroSingleObjectFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        SingleObjectOptions::parse(options)?.canonical()
    }

    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection> {
        let matched = probe.prefix.starts_with(SOE_MAGIC);
        Ok(FormatDetection {
            confidence: if matched {
                FormatDetectionConfidence::Strong
            } else {
                FormatDetectionConfidence::None
            },
            reason: if matched {
                "Avro single-object magic matched"
            } else {
                "Avro single-object magic did not match"
            }
            .to_owned(),
        })
    }

    fn discover(
        &self,
        source: Arc<dyn ByteSource>,
        request: FormatDiscoveryRequest,
    ) -> BoxFuture<'_, Result<PhysicalSchemaObservation>> {
        Box::pin(async move {
            request.cancellation.check()?;
            validate_metadata_discovery(request.discovery_kind, request.maximum_bytes)?;
            let options = SingleObjectOptions::parse(request.options)?;
            let (schema, fingerprint) = single_object_schema(&options, None, 1)?;
            let mut evidence = BTreeMap::new();
            evidence.insert("avro.writer_fingerprint".to_owned(), fingerprint);
            evidence.insert("avro.schema_authority".to_owned(), "explicit".to_owned());
            Ok(PhysicalSchemaObservation {
                identity: source.identity().clone(),
                arrow_schema: schema,
                sampled_bytes: 0,
                sampled_records: 0,
                evidence,
            })
        })
    }

    fn prepare_decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Arc<dyn FormatDecodeSession>>> {
        Box::pin(async move {
            request.cancellation.check()?;
            validate_decode_request(&request, "Avro single-object")?;
            let options = SingleObjectOptions::parse(request.options)?;
            let (schema, _) = single_object_schema(&options, None, request.target_batch_rows)?;
            projection_indices(schema.as_ref(), request.projection.as_deref())?;
            if let Some(size) = source.identity().size_bytes {
                if size == 0 {
                    return Err(CdfError::data(
                        "Avro single-object source must contain exactly one encoded datum",
                    ));
                }
                if size > options.maximum_record_bytes {
                    return Err(CdfError::data(format!(
                        "Avro single-object source is {size} bytes, above the configured {}-byte maximum; increase format_options.maximum_record_bytes or provide one smaller encoded datum",
                        options.maximum_record_bytes
                    )));
                }
            }
            let unit = DecodeUnitPlan {
                unit_id: "single-object-record".to_owned(),
                ordinal: 0,
                extent: source
                    .identity()
                    .size_bytes
                    .map(|size| ByteExtent::new(0, size))
                    .transpose()?,
                estimated_working_set_bytes: options
                    .maximum_record_bytes
                    .checked_mul(2)
                    .ok_or_else(|| CdfError::contract("Avro record working set overflowed"))?
                    .min(MAXIMUM_WORKING_SET_BYTES),
                independently_retryable: source.capabilities().reopenable,
            };
            unit.validate()?;
            Ok(Arc::new(SingleObjectDecodeSession {
                source,
                physical_schema: schema,
                options,
                projection: request.projection,
                units: vec![unit],
            }) as Arc<dyn FormatDecodeSession>)
        })
    }
}
