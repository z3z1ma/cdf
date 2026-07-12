#![doc = "Native Parquet format driver for cdf."]

use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use cdf_contract::ObservedSchema;
use cdf_kernel::{Batch, BatchId, BoxFuture, CdfError, PushdownFidelity, Result};
use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedPhysicalBatch, ByteExtent, ByteSource, DecodePlanningRequest, DecodeUnitPlan,
    FormatDetection, FormatDetectionConfidence, FormatDiscoveryRequest, FormatDriver,
    FormatDriverDescriptor, FormatId, FormatProbe, GenerationStrength, MagicSignature,
    PhysicalDecodeRequest, PhysicalDecodeStream, PhysicalSchemaObservation,
};
use futures_util::{
    FutureExt, StreamExt, TryStreamExt, future::BoxFuture as FuturesBoxFuture, stream,
};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, MetadataSuffixFetch, ParquetRecordBatchStreamBuilder},
    },
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

#[derive(Clone, Debug)]
pub struct ParquetFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl ParquetFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("parquet")?,
                semantic_version: "1.0.0".to_owned(),
                aliases: Vec::new(),
                extensions: vec!["parquet".to_owned()],
                mime_types: vec!["application/vnd.apache.parquet".to_owned()],
                magic: vec![MagicSignature {
                    offset: 0,
                    bytes: b"PAR1".to_vec(),
                    strong: true,
                }],
                option_schema: serde_json::json!({
                    "type": "object",
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Exact,
                predicate_pushdown: PushdownFidelity::Unsupported,
                decode_unit_policy: "row_group".to_owned(),
                minimum_working_set_bytes: 1024 * 1024,
                maximum_working_set_bytes: 256 * 1024 * 1024,
            },
        })
    }
}

impl FormatDriver for ParquetFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        match options {
            serde_json::Value::Object(values) if values.is_empty() => {
                Ok(serde_json::Value::Object(values))
            }
            _ => Err(CdfError::contract(
                "Parquet format options must be an empty object",
            )),
        }
    }

    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection> {
        let header = probe.prefix.starts_with(b"PAR1");
        let footer = probe.suffix.ends_with(b"PAR1");
        Ok(match (header, footer) {
            (true, true) => FormatDetection {
                confidence: FormatDetectionConfidence::Strong,
                reason: "PAR1 header and footer".to_owned(),
            },
            (true, false) | (false, true) => FormatDetection {
                confidence: FormatDetectionConfidence::Weak,
                reason: "only one PAR1 framing marker was observed".to_owned(),
            },
            (false, false) => FormatDetection {
                confidence: FormatDetectionConfidence::None,
                reason: "PAR1 framing was not observed".to_owned(),
            },
        })
    }

    fn discover(
        &self,
        source: Arc<dyn ByteSource>,
        request: FormatDiscoveryRequest,
    ) -> BoxFuture<'_, Result<PhysicalSchemaObservation>> {
        Box::pin(async move {
            request.cancellation.check()?;
            self.canonical_options(request.options)?;
            validate_parquet_source(source.as_ref())?;
            let reader = ParquetByteSource::new(Arc::clone(&source), request.cancellation.clone());
            let bytes_read = Arc::clone(&reader.bytes_read);
            let builder = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(parquet_error)?;
            let schema = Arc::clone(builder.schema());
            let sampled_bytes = bytes_read.load(Ordering::Relaxed);
            if sampled_bytes > request.maximum_bytes {
                return Err(CdfError::data(format!(
                    "Parquet discovery read {sampled_bytes} metadata bytes above its {}-byte budget",
                    request.maximum_bytes
                )));
            }
            Ok(PhysicalSchemaObservation {
                identity: source.identity().clone(),
                observed_schema: ObservedSchema::from_arrow(schema.as_ref()),
                arrow_schema: schema,
                sampled_bytes,
                sampled_records: 0,
            })
        })
    }

    fn plan_decode_units(
        &self,
        source: Arc<dyn ByteSource>,
        request: DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Vec<DecodeUnitPlan>>> {
        Box::pin(async move {
            request.cancellation.check()?;
            self.canonical_options(request.options)?;
            validate_parquet_source(source.as_ref())?;
            if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
                return Err(CdfError::contract(
                    "Parquet unit planning requires nonzero row and byte batch targets",
                ));
            }
            let builder = ParquetRecordBatchStreamBuilder::new(ParquetByteSource::new(
                source,
                request.cancellation.clone(),
            ))
            .await
            .map_err(parquet_error)?;
            builder
                .metadata()
                .row_groups()
                .iter()
                .enumerate()
                .map(|(ordinal, row_group)| {
                    let ordinal = u32::try_from(ordinal)
                        .map_err(|_| CdfError::data("Parquet row-group ordinal exceeds u32"))?;
                    let compressed = u64::try_from(row_group.compressed_size())
                        .map_err(|_| CdfError::data("Parquet row-group size is negative"))?;
                    let estimated_working_set_bytes = compressed
                        .max(request.target_batch_bytes)
                        .min(self.descriptor.maximum_working_set_bytes);
                    let unit = DecodeUnitPlan {
                        unit_id: format!("row-group-{ordinal:08}"),
                        ordinal,
                        extent: None,
                        estimated_working_set_bytes,
                        independently_retryable: true,
                    };
                    unit.validate()?;
                    Ok(unit)
                })
                .collect()
        })
    }

    fn decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: PhysicalDecodeRequest,
    ) -> BoxFuture<'_, Result<PhysicalDecodeStream>> {
        Box::pin(async move {
            request.cancellation.check()?;
            self.canonical_options(request.options.clone())?;
            request.unit.validate()?;
            validate_parquet_source(source.as_ref())?;
            if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
                return Err(CdfError::contract(
                    "Parquet decode requires nonzero row and byte batch targets",
                ));
            }
            if !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "Parquet predicate pushdown is not implemented by this driver version",
                ));
            }
            let mut builder = ParquetRecordBatchStreamBuilder::new(ParquetByteSource::new(
                source,
                request.cancellation.clone(),
            ))
            .await
            .map_err(parquet_error)?
            .with_batch_size(request.target_batch_rows)
            .with_row_groups(vec![usize::try_from(request.unit.ordinal).map_err(
                |_| CdfError::data("Parquet row-group ordinal exceeds usize"),
            )?]);
            let actual_hash = cdf_contract::canonical_arrow_schema_hash(builder.schema())?;
            let expected_hash =
                cdf_contract::canonical_arrow_schema_hash(request.physical_schema.as_ref())?;
            if actual_hash != expected_hash {
                return Err(CdfError::data(format!(
                    "Parquet physical schema changed before decode: planned {}, observed {actual_hash}",
                    expected_hash
                )));
            }
            if let Some(projection) = &request.projection {
                let roots = projection
                    .iter()
                    .map(|name| {
                        builder
                            .schema()
                            .fields()
                            .iter()
                            .position(|field| field.name() == name)
                            .ok_or_else(|| {
                                CdfError::contract(format!(
                                    "Parquet projection field {name:?} is absent from the physical schema"
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
                builder = builder.with_projection(mask);
            }
            let parquet_stream = builder.build().map_err(parquet_error)?;
            let state = DecodeState {
                stream: Box::pin(parquet_stream),
                request,
                sequence: 0,
            };
            Ok(Box::pin(stream::try_unfold(state, |mut state| async move {
                state.request.cancellation.check()?;
                let reservation = ReservationRequest::new(
                    ConsumerKey::new("parquet-physical-batch", MemoryClass::Decode)?,
                    state.request.target_batch_bytes,
                )?
                .as_minimum_working_set();
                let lease = reserve(Arc::clone(&state.request.memory), reservation).await?;
                let Some(record_batch) = state.stream.try_next().await.map_err(parquet_error)?
                else {
                    return Ok(None);
                };
                let batch_id = BatchId::new(format!(
                    "{}-u{:08}-b{:08}",
                    state.request.batch_id_prefix, state.request.unit.ordinal, state.sequence
                ))?;
                state.sequence = state
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("Parquet batch sequence overflowed"))?;
                let mut batch = Batch::from_record_batch(
                    batch_id,
                    state.request.resource_id.clone(),
                    state.request.partition_id.clone(),
                    cdf_contract::canonical_arrow_schema_hash(
                        state.request.physical_schema.as_ref(),
                    )?,
                    record_batch,
                )?;
                batch.header.source_position = state.request.source_position.clone();
                Ok(Some((AccountedPhysicalBatch::new(batch, lease)?, state)))
            })) as PhysicalDecodeStream)
        })
    }
}

struct DecodeState {
    stream: PinParquetStream,
    request: PhysicalDecodeRequest,
    sequence: u64,
}

type PinParquetStream = std::pin::Pin<
    Box<dyn futures_util::Stream<Item = parquet::errors::Result<arrow_array::RecordBatch>> + Send>,
>;

#[derive(Clone)]
struct ParquetByteSource {
    source: Arc<dyn ByteSource>,
    cancellation: cdf_runtime::RunCancellation,
    bytes_read: Arc<AtomicU64>,
}

impl ParquetByteSource {
    fn new(source: Arc<dyn ByteSource>, cancellation: cdf_runtime::RunCancellation) -> Self {
        Self {
            source,
            cancellation,
            bytes_read: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn range(&self, range: Range<u64>) -> parquet::errors::Result<Bytes> {
        let length = range.end.checked_sub(range.start).ok_or_else(|| {
            ParquetError::General("Parquet requested an inverted range".to_owned())
        })?;
        let extent = ByteExtent::new(range.start, length).map_err(to_parquet_error)?;
        let bytes = self
            .source
            .read_exact_range(extent, self.cancellation.clone())
            .await
            .map_err(to_parquet_error)?;
        self.bytes_read.fetch_add(length, Ordering::Relaxed);
        Ok(Bytes::from_owner(bytes))
    }
}

impl AsyncFileReader for ParquetByteSource {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> FuturesBoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.range(range).boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> FuturesBoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let source = self.clone();
        let concurrency = usize::from(source.source.capabilities().useful_range_concurrency.max(1));
        async move {
            let mut completed = stream::iter(ranges.into_iter().enumerate())
                .map(|(ordinal, range)| {
                    let source = source.clone();
                    async move { source.range(range).await.map(|bytes| (ordinal, bytes)) }
                })
                .buffer_unordered(concurrency)
                .try_collect::<Vec<_>>()
                .await?;
            completed.sort_unstable_by_key(|(ordinal, _)| *ordinal);
            Ok(completed.into_iter().map(|(_, bytes)| bytes).collect())
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> FuturesBoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata_options = options.map(|options| options.metadata_options().clone());
            let mut reader = ParquetMetaDataReader::new().with_metadata_options(metadata_options);
            if let Some(options) = options {
                reader = reader
                    .with_column_index_policy(options.column_index_policy())
                    .with_offset_index_policy(options.offset_index_policy());
            }
            Ok(Arc::new(reader.load_via_suffix_and_finish(self).await?))
        }
        .boxed()
    }
}

impl MetadataSuffixFetch for &mut ParquetByteSource {
    fn fetch_suffix(
        &mut self,
        suffix: usize,
    ) -> FuturesBoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let size = self.source.identity().size_bytes.ok_or_else(|| {
                ParquetError::General("Parquet requires known byte length".to_owned())
            })?;
            let suffix = u64::try_from(suffix)
                .map_err(|_| ParquetError::General("Parquet suffix exceeds u64".to_owned()))?;
            if suffix > size {
                return Err(ParquetError::General(format!(
                    "Parquet requested {suffix} suffix bytes from a {size}-byte object"
                )));
            }
            self.range(size - suffix..size).await
        }
        .boxed()
    }
}

fn parquet_error(error: ParquetError) -> CdfError {
    CdfError::data(format!("Parquet driver: {error}"))
}

fn validate_parquet_source(source: &dyn ByteSource) -> Result<()> {
    source.identity().validate()?;
    source.capabilities().validate()?;
    if source.identity().size_bytes.is_none()
        || !source.capabilities().exact_ranges
        || source.identity().strength == GenerationStrength::Weak
    {
        return Err(CdfError::contract(
            "Parquet random-access decode requires known length and enforceable strong/content-addressed exact ranges; select a verified sequential spool for weak sources",
        ));
    }
    Ok(())
}

fn to_parquet_error(error: CdfError) -> ParquetError {
    ParquetError::General(error.to_string())
}

#[cfg(test)]
mod tests;
