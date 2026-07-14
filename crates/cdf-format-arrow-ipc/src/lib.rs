use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::{
    MetadataVersion,
    convert::fb_to_schema,
    reader::{FileDecoder, read_footer_length},
};
use arrow_schema::{Schema, SchemaRef};
use cdf_kernel::{Batch, BatchId, BoxFuture, CdfError, PushdownFidelity, Result};
use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedPhysicalBatch, ByteExtent, ByteSource, DecodePlanningRequest, DecodeSchemaAuthority,
    DecodeUnitPlan, FormatDetection, FormatDetectionConfidence, FormatDetectionProbe,
    FormatDiscoveryRequest, FormatDriver, FormatDriverDescriptor, FormatId, FormatProbe,
    MagicSignature, PhysicalDecodeRequest, PhysicalDecodeStream, PhysicalSchemaObservation,
};
use futures_util::stream;

const MAGIC: &[u8; 6] = b"ARROW1";
const TRAILER_BYTES: u64 = 10;

#[derive(Debug)]
pub struct ArrowIpcFileFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl ArrowIpcFileFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("arrow_ipc")?,
                semantic_version: "1.0.0".to_owned(),
                aliases: vec!["arrow".to_owned(), "feather".to_owned()],
                extensions: vec!["arrow".to_owned(), "feather".to_owned()],
                mime_types: vec!["application/vnd.apache.arrow.file".to_owned()],
                magic: vec![MagicSignature {
                    offset: 0,
                    bytes: MAGIC.to_vec(),
                    strong: true,
                }],
                detection_probe: FormatDetectionProbe {
                    prefix_bytes: 6,
                    suffix_bytes: 6,
                },
                option_schema: serde_json::json!({
                    "type": "object",
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Exact,
                predicate_pushdown: PushdownFidelity::Unsupported,
                source_access: cdf_runtime::FormatSourceAccess::Seekable,
                decode_unit_policy: "ipc_file_blocks_v1".to_owned(),
                error_isolation: cdf_runtime::FormatErrorIsolation::DecodeUnit,
                minimum_working_set_bytes: 64 * 1024,
                maximum_working_set_bytes: 4 * 1024 * 1024 * 1024,
            },
        })
    }
}

impl FormatDriver for ArrowIpcFileFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        match options {
            serde_json::Value::Object(values) if values.is_empty() => {
                Ok(serde_json::Value::Object(values))
            }
            _ => Err(CdfError::contract(
                "Arrow IPC file options must be an empty object",
            )),
        }
    }

    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection> {
        let header = probe.prefix.starts_with(MAGIC);
        let footer = probe.suffix.ends_with(MAGIC);
        Ok(if header && footer {
            FormatDetection {
                confidence: FormatDetectionConfidence::Strong,
                reason: "Arrow IPC file header and footer magic matched".to_owned(),
            }
        } else if header {
            FormatDetection {
                confidence: FormatDetectionConfidence::Weak,
                reason: "Arrow IPC header matched but footer was not observed".to_owned(),
            }
        } else {
            FormatDetection {
                confidence: FormatDetectionConfidence::None,
                reason: "Arrow IPC file magic did not match".to_owned(),
            }
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
            validate_source(source.as_ref())?;
            let footer = read_footer(
                source.as_ref(),
                &request.cancellation,
                request.maximum_bytes,
            )
            .await?;
            let sampled_bytes = footer.sampled_bytes;
            let schema = footer.schema;
            Ok(PhysicalSchemaObservation {
                identity: source.identity().clone(),
                arrow_schema: schema,
                sampled_bytes,
                sampled_records: 0,
                evidence: std::collections::BTreeMap::new(),
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
            validate_source(source.as_ref())?;
            if !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "Arrow IPC file predicate pushdown is unsupported",
                ));
            }
            let footer = read_footer(source.as_ref(), &request.cancellation, u64::MAX).await?;
            validate_projection(footer.schema.as_ref(), request.projection.as_deref())?;
            let size = source
                .identity()
                .size_bytes
                .expect("validated Arrow IPC source length");
            Ok(vec![DecodeUnitPlan {
                unit_id: "ipc-file".to_owned(),
                ordinal: 0,
                extent: Some(ByteExtent::new(0, size)?),
                estimated_working_set_bytes: footer.maximum_block_bytes.max(64 * 1024),
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
            validate_source(source.as_ref())?;
            if !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "Arrow IPC file predicate pushdown is unsupported",
                ));
            }
            let footer = read_footer(source.as_ref(), &request.cancellation, u64::MAX).await?;
            let actual_hash = cdf_kernel::canonical_arrow_schema_hash(footer.schema.as_ref())?;
            if request.schema.authority == DecodeSchemaAuthority::VerifiedPhysicalObservation {
                let expected_hash = cdf_kernel::canonical_arrow_schema_hash(
                    request.schema.authority_schema.as_ref(),
                )?;
                if actual_hash != expected_hash {
                    return Err(CdfError::data(format!(
                        "Arrow IPC physical schema changed before decode: planned {}, observed {actual_hash}",
                        expected_hash
                    )));
                }
            }
            let projection =
                projection_indices(footer.schema.as_ref(), request.projection.as_deref())?;
            let physical_schema = footer.schema.clone();
            let mut decoder = FileDecoder::new(footer.schema, footer.version);
            if let Some(projection) = projection {
                decoder = decoder.with_projection(projection);
            }
            for block in &footer.dictionaries {
                let bytes = read_block(source.as_ref(), block, &request.cancellation).await?;
                let buffer = Buffer::from(bytes::Bytes::from_owner(bytes));
                let footer = parse_footer(footer.footer_bytes.as_ref())?;
                let dictionary = footer
                    .dictionaries()
                    .map(|blocks| blocks.get(block.footer_ordinal))
                    .ok_or_else(|| CdfError::data("Arrow IPC dictionary block disappeared"))?;
                decoder
                    .read_dictionary(dictionary, &buffer)
                    .map_err(ipc_error)?;
            }
            let state = DecodeState {
                source,
                footer_bytes: footer.footer_bytes,
                blocks: footer.record_batches,
                decoder,
                request,
                observed_schema_hash: actual_hash,
                physical_schema,
                emitted_schema: false,
                next_block: 0,
                sequence: 0,
            };
            Ok(Box::pin(stream::try_unfold(state, |mut state| async move {
                state.request.cancellation.check()?;
                let block = match state.blocks.get(state.next_block).cloned() {
                    Some(block) => block,
                    None if !state.emitted_schema => {
                        let reservation = ReservationRequest::new(
                            ConsumerKey::new("arrow-ipc-physical-batch", MemoryClass::Decode)?,
                            state.request.target_batch_bytes.max(1),
                        )?
                        .as_minimum_working_set();
                        let lease = reserve(Arc::clone(&state.request.memory), reservation).await?;
                        state.emitted_schema = true;
                        let batch_id = BatchId::new(format!(
                            "{}-u{:08}-b{:08}",
                            state.request.batch_id_prefix,
                            state.request.unit.ordinal,
                            state.sequence
                        ))?;
                        let mut batch = Batch::from_record_batch(
                            batch_id,
                            state.request.resource_id.clone(),
                            state.request.partition_id.clone(),
                            state.observed_schema_hash.clone(),
                            RecordBatch::new_empty(state.physical_schema.clone()),
                        )?;
                        batch.header.source_position = state.request.source_position.clone();
                        return Ok(Some((AccountedPhysicalBatch::new(batch, lease)?, state)));
                    }
                    None => return Ok(None),
                };
                let reservation = ReservationRequest::new(
                    ConsumerKey::new("arrow-ipc-physical-batch", MemoryClass::Decode)?,
                    block.total_bytes.max(state.request.target_batch_bytes),
                )?
                .as_minimum_working_set();
                let lease = reserve(Arc::clone(&state.request.memory), reservation).await?;
                let bytes =
                    read_block(state.source.as_ref(), &block, &state.request.cancellation).await?;
                let buffer = Buffer::from(bytes::Bytes::from_owner(bytes));
                let footer = parse_footer(state.footer_bytes.as_ref())?;
                let ipc_block = footer
                    .recordBatches()
                    .map(|blocks| blocks.get(block.footer_ordinal))
                    .ok_or_else(|| CdfError::data("Arrow IPC record batch block disappeared"))?;
                let record_batch = state
                    .decoder
                    .read_record_batch(ipc_block, &buffer)
                    .map_err(ipc_error)?;
                state.next_block = state
                    .next_block
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("Arrow IPC block ordinal overflowed"))?;
                let record_batch = record_batch.ok_or_else(|| {
                    CdfError::data("Arrow IPC footer record-batch block contained no record batch")
                })?;
                state.emitted_schema = true;
                let batch_id = BatchId::new(format!(
                    "{}-u{:08}-b{:08}",
                    state.request.batch_id_prefix, state.request.unit.ordinal, state.sequence
                ))?;
                state.sequence = state
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("Arrow IPC batch sequence overflowed"))?;
                let mut batch = Batch::from_record_batch(
                    batch_id,
                    state.request.resource_id.clone(),
                    state.request.partition_id.clone(),
                    state.observed_schema_hash.clone(),
                    record_batch,
                )?;
                batch.header.source_position = state.request.source_position.clone();
                Ok(Some((AccountedPhysicalBatch::new(batch, lease)?, state)))
            })) as PhysicalDecodeStream)
        })
    }
}

struct FooterRead {
    schema: SchemaRef,
    version: MetadataVersion,
    dictionaries: Vec<OwnedBlock>,
    record_batches: Vec<OwnedBlock>,
    footer_bytes: cdf_memory::AccountedBytes,
    sampled_bytes: u64,
    maximum_block_bytes: u64,
}

#[derive(Clone)]
struct OwnedBlock {
    footer_ordinal: usize,
    offset: u64,
    total_bytes: u64,
}

struct DecodeState {
    source: Arc<dyn ByteSource>,
    footer_bytes: cdf_memory::AccountedBytes,
    blocks: Vec<OwnedBlock>,
    decoder: FileDecoder,
    request: PhysicalDecodeRequest,
    observed_schema_hash: cdf_kernel::SchemaHash,
    physical_schema: SchemaRef,
    emitted_schema: bool,
    next_block: usize,
    sequence: u64,
}

async fn read_footer(
    source: &dyn ByteSource,
    cancellation: &cdf_runtime::RunCancellation,
    maximum_bytes: u64,
) -> Result<FooterRead> {
    let size = source
        .identity()
        .size_bytes
        .ok_or_else(|| CdfError::contract("Arrow IPC file requires known length"))?;
    if size < TRAILER_BYTES {
        return Err(CdfError::data("Arrow IPC file is shorter than its trailer"));
    }
    if maximum_bytes < TRAILER_BYTES {
        return Err(CdfError::data(format!(
            "Arrow IPC footer requires at least {TRAILER_BYTES} bytes, exceeding discovery budget {maximum_bytes}"
        )));
    }
    let trailer = source
        .read_exact_range(
            ByteExtent::new(size - TRAILER_BYTES, TRAILER_BYTES)?,
            cancellation.clone(),
        )
        .await?;
    let trailer_array: [u8; 10] = trailer
        .as_ref()
        .try_into()
        .map_err(|_| CdfError::internal("Arrow IPC trailer length changed"))?;
    let footer_len = u64::try_from(read_footer_length(trailer_array).map_err(ipc_error)?)
        .map_err(|_| CdfError::data("Arrow IPC footer length exceeds u64"))?;
    let sampled_bytes = TRAILER_BYTES
        .checked_add(footer_len)
        .ok_or_else(|| CdfError::data("Arrow IPC footer byte count overflowed"))?;
    if sampled_bytes > maximum_bytes {
        return Err(CdfError::data(format!(
            "Arrow IPC footer requires {sampled_bytes} bytes, exceeding discovery budget {maximum_bytes}"
        )));
    }
    let footer_start = size
        .checked_sub(sampled_bytes)
        .ok_or_else(|| CdfError::data("Arrow IPC footer length exceeds file size"))?;
    let footer_bytes = source
        .read_exact_range(
            ByteExtent::new(footer_start, footer_len)?,
            cancellation.clone(),
        )
        .await?;
    let footer = parse_footer(footer_bytes.as_ref())?;
    let ipc_schema = footer
        .schema()
        .ok_or_else(|| CdfError::data("Arrow IPC footer omitted schema"))?;
    if !ipc_schema.endianness().equals_to_target_endianness() {
        return Err(CdfError::data(
            "Arrow IPC source endianness does not match this host",
        ));
    }
    let mut schema = fb_to_schema(ipc_schema);
    let mut metadata = schema.metadata().clone();
    if let Some(custom) = footer.custom_metadata() {
        for entry in custom {
            let key = entry
                .key()
                .ok_or_else(|| CdfError::data("Arrow IPC custom metadata omitted key"))?;
            let value = entry
                .value()
                .ok_or_else(|| CdfError::data("Arrow IPC custom metadata omitted value"))?;
            metadata.insert(key.to_owned(), value.to_owned());
        }
    }
    schema = Schema::new_with_metadata(schema.fields().clone(), metadata);
    let dictionaries = owned_blocks(footer.dictionaries(), footer_start)?;
    let record_batches = owned_blocks(footer.recordBatches(), footer_start)?;
    let maximum_block_bytes = dictionaries
        .iter()
        .chain(&record_batches)
        .map(|block| block.total_bytes)
        .max()
        .unwrap_or(1);
    Ok(FooterRead {
        schema: Arc::new(schema),
        version: footer.version(),
        dictionaries,
        record_batches,
        footer_bytes,
        sampled_bytes,
        maximum_block_bytes,
    })
}

fn owned_blocks(
    blocks: Option<flatbuffers::Vector<'_, arrow_ipc::Block>>,
    data_end: u64,
) -> Result<Vec<OwnedBlock>> {
    blocks
        .map(|blocks| {
            blocks
                .iter()
                .enumerate()
                .map(|(footer_ordinal, block)| {
                    let offset = u64::try_from(block.offset())
                        .map_err(|_| CdfError::data("Arrow IPC block offset is negative"))?;
                    let metadata = u64::try_from(block.metaDataLength()).map_err(|_| {
                        CdfError::data("Arrow IPC block metadata length is negative")
                    })?;
                    let body = u64::try_from(block.bodyLength())
                        .map_err(|_| CdfError::data("Arrow IPC block body length is negative"))?;
                    let total_bytes = metadata
                        .checked_add(body)
                        .ok_or_else(|| CdfError::data("Arrow IPC block length overflowed"))?;
                    let end = offset
                        .checked_add(total_bytes)
                        .ok_or_else(|| CdfError::data("Arrow IPC block extent overflowed"))?;
                    if end > data_end {
                        return Err(CdfError::data(format!(
                            "Arrow IPC block {footer_ordinal} ends at {end}, beyond data region {data_end}"
                        )));
                    }
                    Ok(OwnedBlock {
                        footer_ordinal,
                        offset,
                        total_bytes,
                    })
                })
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

async fn read_block(
    source: &dyn ByteSource,
    block: &OwnedBlock,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<cdf_memory::AccountedBytes> {
    source
        .read_exact_range(
            ByteExtent::new(block.offset, block.total_bytes)?,
            cancellation.clone(),
        )
        .await
}

fn parse_footer(bytes: &[u8]) -> Result<arrow_ipc::Footer<'_>> {
    arrow_ipc::root_as_footer(bytes)
        .map_err(|error| CdfError::data(format!("Arrow IPC footer is invalid: {error:?}")))
}

fn projection_indices(
    schema: &Schema,
    projection: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    projection
        .map(|projection| {
            projection
                .iter()
                .map(|name| {
                    schema.index_of(name).map_err(|_| {
                        CdfError::contract(format!(
                            "Arrow IPC projection field {name:?} is absent from the physical schema"
                        ))
                    })
                })
                .collect()
        })
        .transpose()
}

fn validate_projection(schema: &Schema, projection: Option<&[String]>) -> Result<()> {
    projection_indices(schema, projection).map(|_| ())
}

fn validate_source(source: &dyn ByteSource) -> Result<()> {
    source.identity().validate()?;
    source.capabilities().validate()?;
    if source.identity().size_bytes.is_none() || !source.capabilities().exact_ranges {
        return Err(CdfError::contract(
            "Arrow IPC file decode requires known length and exact ranges; select a verified spool",
        ));
    }
    Ok(())
}

fn ipc_error(error: impl std::fmt::Display) -> CdfError {
    CdfError::data(format!("Arrow IPC file driver: {error}"))
}

#[cfg(test)]
mod tests;
