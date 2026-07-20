#![doc = "Native Avro object-container and single-object format drivers for cdf."]

use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow_array::RecordBatch;
use arrow_avro::{
    errors::AvroError,
    reader::{AsyncAvroFileReader, AsyncFileReader, HeaderInfo, ReaderBuilder},
    schema::{AvroSchema, FingerprintAlgorithm, SchemaStore},
};
use arrow_schema::SchemaRef;
use bytes::Bytes;
use cdf_kernel::{Batch, BatchId, BoxFuture, CdfError, PushdownFidelity, Result, SchemaHash};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, record_batch_retained_bytes, reserve,
};
use cdf_runtime::{
    AccountedPhysicalBatch, ByteExtent, ByteSource, DecodePlanningRequest, DecodeSchemaAuthority,
    DecodeUnitPlan, FormatDecodeSession, FormatDetection, FormatDetectionConfidence,
    FormatDetectionProbe, FormatDiscoveryCapabilities, FormatDiscoveryKind, FormatDiscoveryRequest,
    FormatDriver, FormatDriverDescriptor, FormatErrorIsolation, FormatId, FormatProbe,
    FormatSourceAccess, MagicSignature, PhysicalDecodeRequest, PhysicalDecodeStream,
    PhysicalSchemaObservation, SequentialReadRequest,
};
use futures_util::{FutureExt, TryStreamExt, future::BoxFuture as FuturesBoxFuture, stream};
use serde::{Deserialize, Serialize};

const OCF_MAGIC: &[u8; 4] = b"Obj\x01";
const SOE_MAGIC: &[u8; 2] = &[0xc3, 0x01];
const DEFAULT_MAXIMUM_BLOCK_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MAXIMUM_DECODED_BLOCK_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_MAXIMUM_BLOCK_RECORDS: u64 = 16 * 1024 * 1024;
const DEFAULT_MAXIMUM_BLOCKS: u32 = 1_000_000;
const DEFAULT_MAXIMUM_HEADER_BYTES: u64 = 16 * 1024 * 1024;
const OCF_HEADER_READ_BYTES: u64 = 16 * 1024;
const DEFAULT_MAXIMUM_RECORD_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_INDIVIDUAL_VALUE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAXIMUM_WORKING_SET_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAXIMUM_VLQ_HEADER_BYTES: u64 = 20;
const OCF_SYNC_MARKER_BYTES: u64 = 16;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct OcfOptions {
    maximum_header_bytes: u64,
    maximum_block_bytes: u64,
    maximum_decoded_block_bytes: u64,
    maximum_block_records: u64,
    maximum_blocks: u32,
}

impl Default for OcfOptions {
    fn default() -> Self {
        Self {
            maximum_header_bytes: DEFAULT_MAXIMUM_HEADER_BYTES,
            maximum_block_bytes: DEFAULT_MAXIMUM_BLOCK_BYTES,
            maximum_decoded_block_bytes: DEFAULT_MAXIMUM_DECODED_BLOCK_BYTES,
            maximum_block_records: DEFAULT_MAXIMUM_BLOCK_RECORDS,
            maximum_blocks: DEFAULT_MAXIMUM_BLOCKS,
        }
    }
}

impl OcfOptions {
    fn parse(value: serde_json::Value) -> Result<Self> {
        let options: Self = serde_json::from_value(value)
            .map_err(|error| CdfError::contract(format!("invalid Avro OCF options: {error}")))?;
        if options.maximum_header_bytes == 0
            || options.maximum_block_bytes == 0
            || options.maximum_decoded_block_bytes == 0
            || options.maximum_block_records == 0
            || options.maximum_blocks == 0
            || options.maximum_header_bytes > MAXIMUM_INDIVIDUAL_VALUE_BYTES
            || options.maximum_block_bytes > MAXIMUM_INDIVIDUAL_VALUE_BYTES
            || options.maximum_decoded_block_bytes > MAXIMUM_WORKING_SET_BYTES
        {
            return Err(CdfError::contract(
                "Avro OCF maximum_header_bytes, maximum_block_bytes, maximum_decoded_block_bytes, maximum_block_records, and maximum_blocks must be nonzero; byte authorities may not exceed their documented physical maximum",
            ));
        }
        Ok(options)
    }

    fn canonical(self) -> Result<serde_json::Value> {
        serde_json::to_value(self)
            .map_err(|error| CdfError::internal(format!("encode Avro OCF options: {error}")))
    }

    fn maximum_request_bytes(self) -> Result<u64> {
        let block_request = self
            .maximum_block_bytes
            .checked_add(MAXIMUM_VLQ_HEADER_BYTES + OCF_SYNC_MARKER_BYTES)
            .ok_or_else(|| CdfError::contract("Avro block request authority overflowed"))?
            .checked_add(MAXIMUM_VLQ_HEADER_BYTES)
            .ok_or_else(|| CdfError::contract("Avro range request authority overflowed"))?;
        Ok(block_request.max(OCF_HEADER_READ_BYTES))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SingleObjectOptions {
    writer_schema: serde_json::Value,
    #[serde(default = "default_maximum_record_bytes")]
    maximum_record_bytes: u64,
}

const fn default_maximum_record_bytes() -> u64 {
    DEFAULT_MAXIMUM_RECORD_BYTES
}

impl SingleObjectOptions {
    fn parse(value: serde_json::Value) -> Result<Self> {
        let options: Self = serde_json::from_value(value).map_err(|error| {
            CdfError::contract(format!("invalid Avro single-object options: {error}"))
        })?;
        if options.maximum_record_bytes == 0
            || options.maximum_record_bytes > MAXIMUM_INDIVIDUAL_VALUE_BYTES
        {
            return Err(CdfError::contract(format!(
                "Avro single-object maximum_record_bytes must be in 1..={MAXIMUM_INDIVIDUAL_VALUE_BYTES}"
            )));
        }
        options.writer_schema()?;
        Ok(options)
    }

    fn canonical(self) -> Result<serde_json::Value> {
        serde_json::to_value(self).map_err(|error| {
            CdfError::internal(format!("encode Avro single-object options: {error}"))
        })
    }

    fn writer_schema(&self) -> Result<AvroSchema> {
        if self.writer_schema.is_null() {
            return Err(CdfError::contract(
                "Avro single-object writer_schema cannot be null",
            ));
        }
        let schema = AvroSchema::new(self.writer_schema.to_string());
        schema
            .fingerprint(FingerprintAlgorithm::Rabin)
            .map_err(avro_arrow_error)?;
        Ok(schema)
    }

    fn schema_store(&self) -> Result<SchemaStore> {
        let mut store = SchemaStore::new();
        store
            .register(self.writer_schema()?)
            .map_err(avro_arrow_error)?;
        Ok(store)
    }
}

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

struct OcfDecodeSession {
    source: Arc<dyn ByteSource>,
    size: u64,
    header: HeaderInfo,
    physical_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    options: OcfOptions,
    units: Vec<DecodeUnitPlan>,
    ranges: Vec<Range<u64>>,
}

impl FormatDecodeSession for OcfDecodeSession {
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
            validate_physical_decode_request(&request, "Avro OCF")?;
            let ordinal = usize::try_from(request.unit.ordinal)
                .map_err(|_| CdfError::contract("Avro OCF unit ordinal exceeds usize"))?;
            let range = self
                .ranges
                .get(ordinal)
                .cloned()
                .ok_or_else(|| CdfError::contract("Avro OCF unit has no planned range"))?;
            let working_set_bytes = self
                .options
                .maximum_request_bytes()?
                .checked_add(request.target_batch_bytes)
                .ok_or_else(|| CdfError::contract("Avro OCF working-set authority overflowed"))?
                .min(MAXIMUM_WORKING_SET_BYTES);
            let working_set = reserve(
                Arc::clone(&request.memory),
                ReservationRequest::new(
                    ConsumerKey::new("avro-ocf-working-set", MemoryClass::Decode)?,
                    working_set_bytes,
                )?
                .as_minimum_working_set(),
            )
            .await?;
            let reader = AvroByteSource::new(
                Arc::clone(&self.source),
                request.cancellation.clone(),
                self.options.maximum_request_bytes()?,
                Arc::new(AtomicU64::new(0)),
            )
            .with_ocf_validation(
                self.header.sync(),
                self.options.maximum_block_bytes,
                self.options.maximum_block_records,
            );
            let mut builder =
                AsyncAvroFileReader::builder(reader, self.size, request.target_batch_rows)
                    .with_range(range)
                    .with_strict_mode(true);
            if let Some(projection) = &self.projection {
                builder = builder.with_projection(projection.clone());
            }
            let mut avro_stream = builder
                .build_with_header(self.header.clone())
                .map_err(avro_error)?;
            validate_schema_authority(&request, self.physical_schema.as_ref(), "Avro OCF")?;
            let physical_schema = avro_stream.schema();
            let observed_schema_hash =
                cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref())?;
            let output_lease = reserve_output(
                &request,
                "avro-ocf-output",
                self.options.maximum_decoded_block_bytes,
            )
            .await?;
            let mut batches = Vec::new();
            let mut retained_bytes = Vec::new();
            let mut total_retained_bytes = 0_u64;
            while let Some(record_batch) = avro_stream.try_next().await.map_err(avro_arrow_error)? {
                let sequence = u64::try_from(batches.len())
                    .map_err(|_| CdfError::data("Avro OCF batch sequence exceeds u64"))?;
                let batch = build_physical_batch(
                    &request,
                    sequence,
                    observed_schema_hash.clone(),
                    record_batch,
                )?;
                let bytes =
                    record_batch_retained_bytes(batch.record_batch().ok_or_else(|| {
                        CdfError::internal("Avro OCF physical batch lost its Arrow payload")
                    })?)?
                    .checked_add(batch.header.pre_contract_evidence_retained_bytes()?)
                    .ok_or_else(|| CdfError::data("Avro OCF output memory overflowed"))?;
                total_retained_bytes = total_retained_bytes
                    .checked_add(bytes)
                    .ok_or_else(|| CdfError::data("Avro OCF unit output memory overflowed"))?;
                if total_retained_bytes > self.options.maximum_decoded_block_bytes {
                    return Err(CdfError::data(format!(
                        "Avro OCF block retains {total_retained_bytes} decoded Arrow bytes above the configured {}-byte maximum; increase format_options.maximum_decoded_block_bytes only for a trusted producer",
                        self.options.maximum_decoded_block_bytes
                    )));
                }
                retained_bytes.push(bytes);
                batches.push(batch);
            }
            if batches.is_empty() && self.size == self.header.header_len() {
                let batch = build_physical_batch(
                    &request,
                    0,
                    observed_schema_hash,
                    RecordBatch::new_empty(physical_schema),
                )?;
                let bytes =
                    record_batch_retained_bytes(batch.record_batch().ok_or_else(|| {
                        CdfError::internal("Avro OCF physical batch lost its Arrow payload")
                    })?)?
                    .checked_add(batch.header.pre_contract_evidence_retained_bytes()?)
                    .ok_or_else(|| CdfError::data("Avro OCF output memory overflowed"))?;
                retained_bytes.push(bytes);
                batches.push(batch);
            }
            let leases = output_lease.into_partitions(retained_bytes)?;
            let output = batches
                .into_iter()
                .zip(leases)
                .map(|(batch, lease)| AccountedPhysicalBatch::new(batch, lease))
                .collect::<Result<Vec<_>>>()?;
            drop(working_set);
            Ok(Box::pin(stream::iter(output.into_iter().map(Ok))) as PhysicalDecodeStream)
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

struct SingleObjectDecodeSession {
    source: Arc<dyn ByteSource>,
    physical_schema: SchemaRef,
    options: SingleObjectOptions,
    projection: Option<Vec<String>>,
    units: Vec<DecodeUnitPlan>,
}

impl FormatDecodeSession for SingleObjectDecodeSession {
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
            validate_physical_decode_request(&request, "Avro single-object")?;
            validate_schema_authority(
                &request,
                self.physical_schema.as_ref(),
                "Avro single-object",
            )?;
            let projection =
                projection_indices(self.physical_schema.as_ref(), self.projection.as_deref())?;
            let mut builder = ReaderBuilder::new()
                .with_writer_schema_store(self.options.schema_store()?)
                .with_batch_size(1)
                .with_strict_mode(true);
            if let Some(projection) = projection {
                builder = builder.with_projection(projection);
            }
            let decoder = builder.build_decoder().map_err(avro_arrow_error)?;
            let decoded_schema = decoder.schema();
            let input_lease = reserve(
                Arc::clone(&request.memory),
                ReservationRequest::new(
                    ConsumerKey::new("avro-single-object-input", MemoryClass::Decode)?,
                    self.options.maximum_record_bytes,
                )?
                .as_minimum_working_set(),
            )
            .await?;
            let input = self
                .source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: request
                        .target_batch_bytes
                        .min(self.options.maximum_record_bytes)
                        .max(1),
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let observed_schema_hash =
                cdf_kernel::canonical_arrow_schema_hash(decoded_schema.as_ref())?;
            let state = SingleObjectDecodeState {
                input,
                decoder,
                request,
                observed_schema_hash,
                buffer: Vec::new(),
                maximum_record_bytes: self.options.maximum_record_bytes,
                decoded: false,
                _input_lease: input_lease,
            };
            Ok(
                Box::pin(stream::try_unfold(state, decode_next_single_object))
                    as PhysicalDecodeStream,
            )
        })
    }
}

struct SingleObjectDecodeState {
    input: cdf_runtime::AccountedByteStream,
    decoder: arrow_avro::reader::Decoder,
    request: PhysicalDecodeRequest,
    observed_schema_hash: SchemaHash,
    buffer: Vec<u8>,
    maximum_record_bytes: u64,
    decoded: bool,
    _input_lease: MemoryLease,
}

async fn decode_next_single_object(
    mut state: SingleObjectDecodeState,
) -> Result<Option<(AccountedPhysicalBatch, SingleObjectDecodeState)>> {
    if state.decoded {
        return Ok(None);
    }
    let output_authority_bytes = state
        .request
        .target_batch_bytes
        .checked_add(state.maximum_record_bytes)
        .ok_or_else(|| CdfError::contract("Avro single-object output authority overflowed"))?
        .min(MAXIMUM_WORKING_SET_BYTES);
    let output_lease = reserve_output(
        &state.request,
        "avro-single-object-output",
        output_authority_bytes,
    )
    .await?;
    while let Some(chunk) = state.input.try_next().await? {
        state.request.cancellation.check()?;
        let retained = u64::try_from(state.buffer.len())
            .map_err(|_| CdfError::data("Avro retained buffer length exceeds u64"))?;
        let incoming = u64::try_from(chunk.payload().len())
            .map_err(|_| CdfError::data("Avro input chunk length exceeds u64"))?;
        if retained
            .checked_add(incoming)
            .ok_or_else(|| CdfError::data("Avro record buffer length overflowed"))?
            > state.maximum_record_bytes
        {
            return Err(CdfError::data(format!(
                "Avro single-object record exceeds the configured {}-byte maximum; increase format_options.maximum_record_bytes or provide one smaller encoded datum",
                state.maximum_record_bytes
            )));
        }
        state.buffer.extend_from_slice(chunk.payload());
    }
    if state.buffer.is_empty() {
        return Err(CdfError::data(
            "Avro single-object source must contain exactly one encoded datum",
        ));
    }
    let consumed = state.decoder.decode(&state.buffer).map_err(avro_error)?;
    if consumed != state.buffer.len() {
        if !state.decoder.batch_is_full() {
            return Err(CdfError::data(format!(
                "Avro single-object source ended inside its encoded datum after {consumed} of {} bytes",
                state.buffer.len()
            )));
        }
        return Err(CdfError::data(format!(
            "Avro single-object source contains trailing bytes or multiple encoded datums: decoded {consumed} of {} bytes; store one datum per file or use a source with message-boundary authority",
            state.buffer.len()
        )));
    }
    let record_batch = state.decoder.flush().map_err(avro_error)?.ok_or_else(|| {
        CdfError::data("Avro single-object source ended inside its encoded datum")
    })?;
    if record_batch.num_rows() != 1 {
        return Err(CdfError::data(format!(
            "Avro single-object source decoded {} rows; exactly one datum is required",
            record_batch.num_rows()
        )));
    }
    let physical = physical_batch(
        &state.request,
        0,
        state.observed_schema_hash.clone(),
        record_batch,
        output_lease,
    )?;
    state.decoded = true;
    Ok(Some((physical, state)))
}

struct AvroByteSource {
    source: Arc<dyn ByteSource>,
    cancellation: cdf_runtime::RunCancellation,
    maximum_request_bytes: u64,
    bytes_read: Arc<AtomicU64>,
    maximum_total_bytes: Option<u64>,
    ocf_validator: Option<OcfBlockValidator>,
}

impl AvroByteSource {
    fn new(
        source: Arc<dyn ByteSource>,
        cancellation: cdf_runtime::RunCancellation,
        maximum_request_bytes: u64,
        bytes_read: Arc<AtomicU64>,
    ) -> Self {
        Self {
            source,
            cancellation,
            maximum_request_bytes,
            bytes_read,
            maximum_total_bytes: None,
            ocf_validator: None,
        }
    }

    fn with_total_budget(mut self, maximum_total_bytes: u64) -> Self {
        self.maximum_total_bytes = Some(maximum_total_bytes);
        self
    }

    fn with_ocf_validation(
        mut self,
        sync: [u8; 16],
        maximum_block_bytes: u64,
        maximum_block_records: u64,
    ) -> Self {
        self.ocf_validator = Some(OcfBlockValidator::new(
            sync,
            maximum_block_bytes,
            maximum_block_records,
        ));
        self
    }
}

impl AsyncFileReader for AvroByteSource {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> FuturesBoxFuture<'_, std::result::Result<Bytes, AvroError>> {
        async move {
            self.cancellation.check().map_err(cdf_to_avro)?;
            let size = self
                .source
                .identity()
                .size_bytes
                .ok_or_else(|| AvroError::General("Avro range source length is unknown".to_owned()))?;
            if range.start >= range.end || range.end > size {
                return Err(AvroError::General(format!(
                    "Avro requested invalid byte range {}..{} for {size}-byte source",
                    range.start, range.end
                )));
            }
            let length = range.end - range.start;
            if length > self.maximum_request_bytes {
                return Err(AvroError::General(format!(
                    "Avro block requires a {length}-byte range above the configured {}-byte authority",
                    self.maximum_request_bytes
                )));
            }
            if let Some(maximum_total_bytes) = self.maximum_total_bytes {
                let already_read = self.bytes_read.load(Ordering::Relaxed);
                let requested_total = already_read.checked_add(length).ok_or_else(|| {
                    AvroError::General("Avro byte observation count overflowed".to_owned())
                })?;
                if requested_total > maximum_total_bytes {
                    return Err(AvroError::General(format!(
                        "Avro discovery requires more than its configured {maximum_total_bytes}-byte observation budget"
                    )));
                }
            }
            let bytes = self
                .source
                .read_exact_range(
                    ByteExtent::new(range.start, length).map_err(cdf_to_avro)?,
                    self.cancellation.clone(),
                )
                .await
                .map_err(cdf_to_avro)?;
            if let Some(validator) = &mut self.ocf_validator {
                validator
                    .validate(range.start, bytes.payload())
                    .map_err(cdf_to_avro)?;
            }
            self.bytes_read.fetch_add(length, Ordering::Relaxed);
            Ok(bytes.into_retained_bytes())
        }
        .boxed()
    }
}

#[derive(Clone, Debug)]
struct OcfBlockValidator {
    sync: [u8; 16],
    maximum_block_bytes: u64,
    maximum_block_records: u64,
    next_offset: Option<u64>,
    state: OcfBlockValidationState,
}

#[derive(Clone, Debug)]
enum OcfBlockValidationState {
    SeekingFirstSync,
    Count(AvroLongDecoder),
    Size(AvroLongDecoder),
    Data(u64),
    Sync(usize),
}

#[derive(Clone, Debug, Default)]
struct AvroLongDecoder {
    raw: u64,
    shift: u32,
    bytes: u8,
}

impl AvroLongDecoder {
    fn push(&mut self, byte: u8) -> Result<Option<i64>> {
        if self.bytes == 10 || (self.bytes == 9 && byte > 1) {
            return Err(CdfError::data("Avro block header contains an invalid long"));
        }
        self.raw |= u64::from(byte & 0x7f) << self.shift;
        self.bytes += 1;
        if byte & 0x80 != 0 {
            self.shift += 7;
            return Ok(None);
        }
        let value = ((self.raw >> 1) as i64) ^ -((self.raw & 1) as i64);
        Ok(Some(value))
    }
}

impl OcfBlockValidator {
    fn new(sync: [u8; 16], maximum_block_bytes: u64, maximum_block_records: u64) -> Self {
        Self {
            sync,
            maximum_block_bytes,
            maximum_block_records,
            next_offset: None,
            state: OcfBlockValidationState::SeekingFirstSync,
        }
    }

    fn validate(&mut self, offset: u64, mut bytes: &[u8]) -> Result<()> {
        let input_length = u64::try_from(bytes.len())
            .map_err(|_| CdfError::data("Avro OCF validation length exceeds u64"))?;
        if let Some(expected) = self.next_offset
            && expected != offset
        {
            return Err(CdfError::data(format!(
                "Avro OCF reader requested non-contiguous validation ranges: expected offset {expected}, observed {offset}"
            )));
        }

        if matches!(self.state, OcfBlockValidationState::SeekingFirstSync) {
            let Some(position) = bytes
                .windows(self.sync.len())
                .position(|candidate| candidate == self.sync)
            else {
                self.next_offset = None;
                return Ok(());
            };
            bytes = &bytes[position + self.sync.len()..];
            self.state = OcfBlockValidationState::Count(AvroLongDecoder::default());
        }

        while !bytes.is_empty() {
            match &mut self.state {
                OcfBlockValidationState::SeekingFirstSync => unreachable!("handled above"),
                OcfBlockValidationState::Count(decoder) => {
                    let byte = bytes[0];
                    bytes = &bytes[1..];
                    if let Some(count) = decoder.push(byte)? {
                        if count < 0 {
                            return Err(CdfError::data(format!(
                                "Avro OCF block count cannot be negative: {count}"
                            )));
                        }
                        let count = u64::try_from(count)
                            .map_err(|_| CdfError::data("Avro OCF block count exceeds u64"))?;
                        if count == 0 || count > self.maximum_block_records {
                            return Err(CdfError::data(format!(
                                "Avro OCF block declares {count} records outside the configured 1..={} record authority; increase format_options.maximum_block_records only for a trusted producer",
                                self.maximum_block_records
                            )));
                        }
                        self.state = OcfBlockValidationState::Size(AvroLongDecoder::default());
                    }
                }
                OcfBlockValidationState::Size(decoder) => {
                    let byte = bytes[0];
                    bytes = &bytes[1..];
                    if let Some(size) = decoder.push(byte)? {
                        let size = u64::try_from(size).map_err(|_| {
                            CdfError::data(format!(
                                "Avro OCF block size cannot be negative: {size}"
                            ))
                        })?;
                        if size > self.maximum_block_bytes {
                            return Err(CdfError::data(format!(
                                "Avro OCF block declares {size} encoded bytes above the configured {}-byte maximum; increase format_options.maximum_block_bytes only for a trusted producer",
                                self.maximum_block_bytes
                            )));
                        }
                        self.state = OcfBlockValidationState::Data(size);
                    }
                }
                OcfBlockValidationState::Data(remaining) => {
                    let available = u64::try_from(bytes.len())
                        .map_err(|_| CdfError::data("Avro OCF range length exceeds u64"))?;
                    let consumed = available.min(*remaining);
                    let consumed_usize = usize::try_from(consumed)
                        .map_err(|_| CdfError::data("Avro OCF block length exceeds usize"))?;
                    bytes = &bytes[consumed_usize..];
                    *remaining -= consumed;
                    if *remaining == 0 {
                        self.state = OcfBlockValidationState::Sync(0);
                    }
                }
                OcfBlockValidationState::Sync(matched) => {
                    let remaining = self.sync.len() - *matched;
                    let compared = remaining.min(bytes.len());
                    if bytes[..compared] != self.sync[*matched..*matched + compared] {
                        return Err(CdfError::data(
                            "Avro OCF block sync marker does not match the file header",
                        ));
                    }
                    *matched += compared;
                    bytes = &bytes[compared..];
                    if *matched == self.sync.len() {
                        self.state = OcfBlockValidationState::Count(AvroLongDecoder::default());
                    }
                }
            }
        }
        self.next_offset = Some(
            offset
                .checked_add(input_length)
                .ok_or_else(|| CdfError::data("Avro OCF validation offset overflowed"))?,
        );
        Ok(())
    }
}

fn schema_from_header(reader: AvroByteSource, size: u64, header: HeaderInfo) -> Result<SchemaRef> {
    AsyncAvroFileReader::builder(reader, size, 1)
        .with_range(header.header_len()..header.header_len())
        .with_strict_mode(true)
        .build_with_header(header)
        .map(|reader| reader.schema())
        .map_err(avro_error)
}

fn single_object_schema(
    options: &SingleObjectOptions,
    projection: Option<&[String]>,
    target_batch_rows: usize,
) -> Result<(SchemaRef, String)> {
    let schema = options.writer_schema()?;
    let fingerprint = schema
        .fingerprint(FingerprintAlgorithm::Rabin)
        .map_err(avro_arrow_error)?;
    let builder = ReaderBuilder::new()
        .with_writer_schema_store(options.schema_store()?)
        .with_batch_size(target_batch_rows.max(1))
        .with_strict_mode(true);
    let decoder = builder.build_decoder().map_err(avro_arrow_error)?;
    let schema_ref = decoder.schema();
    let indices = projection_indices(schema_ref.as_ref(), projection)?;
    if let Some(indices) = indices {
        let projected = ReaderBuilder::new()
            .with_writer_schema_store(options.schema_store()?)
            .with_batch_size(target_batch_rows.max(1))
            .with_strict_mode(true)
            .with_projection(indices)
            .build_decoder()
            .map_err(avro_arrow_error)?;
        return Ok((projected.schema(), format!("{fingerprint:?}")));
    }
    Ok((schema_ref, format!("{fingerprint:?}")))
}

async fn ocf_units(
    source: &dyn ByteSource,
    size: u64,
    header_len: u64,
    sync: [u8; 16],
    options: OcfOptions,
    target_batch_bytes: u64,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<(Vec<DecodeUnitPlan>, Vec<Range<u64>>)> {
    if header_len > size {
        return Err(CdfError::data("Avro OCF header exceeds source length"));
    }
    if header_len == size {
        return Ok((
            vec![DecodeUnitPlan {
                unit_id: "ocf-schema-only".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: target_batch_bytes.max(1),
                independently_retryable: true,
            }],
            std::iter::once(0..size).collect(),
        ));
    }
    let first_sync_start = header_len
        .checked_sub(OCF_SYNC_MARKER_BYTES)
        .ok_or_else(|| CdfError::data("Avro OCF header is too short to contain its sync marker"))?;
    let mut units = Vec::new();
    let mut ranges = Vec::new();
    let mut block_header_offset = header_len;
    while block_header_offset < size {
        cancellation.check()?;
        let ordinal = u32::try_from(units.len())
            .map_err(|_| CdfError::data("Avro OCF block count exceeds u32"))?;
        if ordinal >= options.maximum_blocks {
            return Err(CdfError::data(format!(
                "Avro OCF contains more than the configured {} block maximum; increase format_options.maximum_blocks only when the resulting planning metadata is acceptable",
                options.maximum_blocks
            )));
        }

        let header_end = block_header_offset
            .checked_add(MAXIMUM_VLQ_HEADER_BYTES)
            .ok_or_else(|| CdfError::data("Avro OCF block-header range overflowed"))?
            .min(size);
        let header_extent = ByteExtent::new(block_header_offset, header_end - block_header_offset)?;
        let header_bytes = source
            .read_exact_range(header_extent, cancellation.clone())
            .await?;
        let (record_count, count_bytes) = decode_avro_long(header_bytes.payload())?;
        let (encoded_bytes, size_bytes) = decode_avro_long(&header_bytes.payload()[count_bytes..])?;
        let record_count = u64::try_from(record_count).map_err(|_| {
            CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} declares a negative record count"
            ))
        })?;
        if record_count == 0 || record_count > options.maximum_block_records {
            return Err(CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} declares {record_count} records outside the configured 1..={} record authority; increase format_options.maximum_block_records only for a trusted producer",
                options.maximum_block_records
            )));
        }
        let encoded_bytes = u64::try_from(encoded_bytes).map_err(|_| {
            CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} declares a negative encoded size"
            ))
        })?;
        if encoded_bytes > options.maximum_block_bytes {
            return Err(CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} declares {encoded_bytes} encoded bytes above the configured {}-byte maximum; increase format_options.maximum_block_bytes only for a trusted producer",
                options.maximum_block_bytes
            )));
        }
        let block_header_bytes = u64::try_from(count_bytes + size_bytes)
            .map_err(|_| CdfError::data("Avro OCF block-header length exceeds u64"))?;
        let sync_start = block_header_offset
            .checked_add(block_header_bytes)
            .and_then(|offset| offset.checked_add(encoded_bytes))
            .ok_or_else(|| CdfError::data("Avro OCF block extent overflowed"))?;
        let block_end = sync_start
            .checked_add(OCF_SYNC_MARKER_BYTES)
            .ok_or_else(|| CdfError::data("Avro OCF sync extent overflowed"))?;
        if block_end > size {
            return Err(CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} extends to {block_end} beyond the {size}-byte source"
            )));
        }
        let observed_sync = source
            .read_exact_range(
                ByteExtent::new(sync_start, OCF_SYNC_MARKER_BYTES)?,
                cancellation.clone(),
            )
            .await?;
        if observed_sync.payload() != sync {
            return Err(CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} has a sync marker that does not match the file header"
            )));
        }

        let range_start = if ordinal == 0 {
            first_sync_start
        } else {
            block_header_offset
                .checked_sub(OCF_SYNC_MARKER_BYTES)
                .ok_or_else(|| CdfError::data("Avro OCF block range underflowed"))?
        };
        let range = range_start..block_end;
        let extent = ByteExtent::new(range.start, range.end - range.start)?;
        let unit = DecodeUnitPlan {
            unit_id: format!("block-{ordinal:08}-{}-{}", range.start, range.end),
            ordinal,
            extent: Some(extent),
            estimated_working_set_bytes: extent
                .length
                .checked_add(target_batch_bytes)
                .ok_or_else(|| CdfError::contract("Avro OCF unit estimate overflowed"))?
                .min(MAXIMUM_WORKING_SET_BYTES),
            independently_retryable: true,
        };
        unit.validate()?;
        units.push(unit);
        ranges.push(range);
        block_header_offset = block_end;
    }
    Ok((units, ranges))
}

fn decode_avro_long(bytes: &[u8]) -> Result<(i64, usize)> {
    let mut decoder = AvroLongDecoder::default();
    for (index, byte) in bytes.iter().copied().enumerate() {
        if let Some(value) = decoder.push(byte)? {
            return Ok((value, index + 1));
        }
    }
    Err(CdfError::data(
        "Avro OCF source ended inside a block-header long",
    ))
}

fn projection_indices(
    schema: &arrow_schema::Schema,
    projection: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    projection
        .map(|projection| {
            projection
                .iter()
                .map(|name| {
                    schema.index_of(name).map_err(|_| {
                        CdfError::contract(format!(
                            "Avro projection field {name:?} is absent from the writer schema"
                        ))
                    })
                })
                .collect()
        })
        .transpose()
}

fn validate_seekable_source(source: &dyn ByteSource, label: &str) -> Result<u64> {
    source.identity().validate()?;
    source.capabilities().validate()?;
    if !source.capabilities().known_length || !source.capabilities().exact_ranges {
        return Err(CdfError::contract(format!(
            "{label} requires known-length exact-range byte-source access"
        )));
    }
    source
        .identity()
        .size_bytes
        .filter(|size| *size > 0)
        .ok_or_else(|| CdfError::data(format!("{label} source length is missing or zero")))
}

fn validate_metadata_discovery(kind: FormatDiscoveryKind, maximum_bytes: u64) -> Result<()> {
    if kind != FormatDiscoveryKind::FormatMetadata || maximum_bytes == 0 {
        return Err(CdfError::contract(
            "Avro metadata discovery requires format_metadata coverage and a nonzero byte budget",
        ));
    }
    Ok(())
}

fn validate_decode_request(request: &DecodePlanningRequest, label: &str) -> Result<()> {
    if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
        return Err(CdfError::contract(format!(
            "{label} planning requires nonzero row and byte batch targets"
        )));
    }
    if !request.predicates.is_empty() {
        return Err(CdfError::contract(format!(
            "{label} predicate pushdown is unsupported"
        )));
    }
    Ok(())
}

fn validate_physical_decode_request(request: &PhysicalDecodeRequest, label: &str) -> Result<()> {
    if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
        return Err(CdfError::contract(format!(
            "{label} decode requires nonzero row and byte batch targets"
        )));
    }
    if !request.predicates.is_empty() {
        return Err(CdfError::contract(format!(
            "{label} predicate pushdown is unsupported"
        )));
    }
    Ok(())
}

fn validate_schema_authority(
    request: &PhysicalDecodeRequest,
    physical_schema: &arrow_schema::Schema,
    label: &str,
) -> Result<()> {
    if request.schema.authority == DecodeSchemaAuthority::VerifiedPhysicalObservation {
        let expected =
            cdf_kernel::canonical_arrow_schema_hash(request.schema.authority_schema.as_ref())?;
        let observed = cdf_kernel::canonical_arrow_schema_hash(physical_schema)?;
        if expected != observed {
            return Err(CdfError::data(format!(
                "{label} physical schema changed before decode: planned {expected}, observed {observed}"
            )));
        }
    }
    Ok(())
}

async fn reserve_output(
    request: &PhysicalDecodeRequest,
    consumer: &str,
    authority_bytes: u64,
) -> Result<MemoryLease> {
    reserve(
        Arc::clone(&request.memory),
        ReservationRequest::new(
            ConsumerKey::new(consumer, MemoryClass::Decode)?,
            authority_bytes.max(1),
        )?
        .as_minimum_working_set(),
    )
    .await
}

fn physical_batch(
    request: &PhysicalDecodeRequest,
    sequence: u64,
    observed_schema_hash: SchemaHash,
    record_batch: RecordBatch,
    lease: MemoryLease,
) -> Result<AccountedPhysicalBatch> {
    AccountedPhysicalBatch::new(
        build_physical_batch(request, sequence, observed_schema_hash, record_batch)?,
        lease,
    )
}

fn build_physical_batch(
    request: &PhysicalDecodeRequest,
    sequence: u64,
    observed_schema_hash: SchemaHash,
    record_batch: RecordBatch,
) -> Result<Batch> {
    let batch_id = BatchId::new(format!(
        "{}-u{:08}-b{sequence:08}",
        request.batch_id_prefix, request.unit.ordinal
    ))?;
    let mut batch = Batch::from_record_batch(
        batch_id,
        request.resource_id.clone(),
        request.partition_id.clone(),
        observed_schema_hash,
        record_batch,
    )?;
    batch.header.source_position = request.source_position.clone();
    Ok(batch)
}

fn cdf_to_avro(error: CdfError) -> AvroError {
    AvroError::External(Box::new(error))
}

fn avro_error(error: AvroError) -> CdfError {
    match error {
        AvroError::External(error) => match error.downcast::<CdfError>() {
            Ok(error) => *error,
            Err(error) => CdfError::data(format!("decode Avro: {error}")),
        },
        AvroError::ArrowError(error) => avro_arrow_error(*error),
        error => CdfError::data(format!("decode Avro: {error}")),
    }
}

fn avro_arrow_error(error: arrow_schema::ArrowError) -> CdfError {
    match error {
        arrow_schema::ArrowError::ExternalError(error) => match error.downcast::<CdfError>() {
            Ok(error) => *error,
            Err(error) => CdfError::data(format!("decode Avro: {error}")),
        },
        error => CdfError::data(format!("decode Avro: {error}")),
    }
}

#[cfg(test)]
mod tests;
