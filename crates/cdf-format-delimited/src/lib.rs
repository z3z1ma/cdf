#![doc = "Streaming delimited text format drivers for cdf."]

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_csv::reader::{Decoder, Format, ReaderBuilder};
use cdf_kernel::{Batch, BatchId, BoxFuture, CdfError, PushdownFidelity, Result};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteStream, AccountedChunksReader, AccountedPhysicalBatch, ByteExtent, ByteSource,
    DecodePlanningRequest, DecodeUnitPlan, FormatDetection, FormatDetectionConfidence,
    FormatDetectionProbe, FormatDiscoveryRequest, FormatDriver, FormatDriverDescriptor, FormatId,
    FormatProbe, PhysicalDecodeRequest, PhysicalDecodeStream, PhysicalSchemaObservation,
    SequentialReadRequest,
};
use futures_util::{TryStreamExt, stream};

const DISCOVERY_CHUNK_BYTES: u64 = 1024 * 1024;

#[derive(Debug)]
pub struct CsvFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl CsvFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("csv")?,
                semantic_version: "1.0.0".to_owned(),
                aliases: Vec::new(),
                extensions: vec!["csv".to_owned()],
                mime_types: vec!["text/csv".to_owned()],
                magic: Vec::new(),
                detection_probe: FormatDetectionProbe {
                    prefix_bytes: 4096,
                    suffix_bytes: 0,
                },
                option_schema: serde_json::json!({
                    "type": "object",
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Unsupported,
                predicate_pushdown: PushdownFidelity::Unsupported,
                source_access: cdf_runtime::FormatSourceAccess::Sequential,
                discovery_kind: cdf_runtime::FormatDiscoveryKind::BoundedContent,
                decode_unit_policy: "csv_stream_v1".to_owned(),
                error_isolation: cdf_runtime::FormatErrorIsolation::DecodeUnit,
                minimum_working_set_bytes: 1024 * 1024,
                maximum_working_set_bytes: 64 * 1024 * 1024,
            },
        })
    }
}

impl FormatDriver for CsvFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        if options.as_object().is_some_and(serde_json::Map::is_empty) {
            Ok(options)
        } else {
            Err(CdfError::contract("CSV format options must be empty"))
        }
    }

    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection> {
        Ok(FormatDetection {
            confidence: if probe.prefix.contains(&b',') {
                FormatDetectionConfidence::Weak
            } else {
                FormatDetectionConfidence::None
            },
            reason: "CSV has no strong magic; a comma was inspected in the prefix".to_owned(),
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
                    "CSV discovery requires nonzero byte and record bounds",
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
                let chunk_bytes = u64::try_from(chunk.payload().len())
                    .map_err(|_| CdfError::data("CSV discovery chunk length exceeds u64"))?;
                sampled_bytes = sampled_bytes
                    .checked_add(chunk_bytes)
                    .ok_or_else(|| CdfError::data("CSV discovery byte count overflowed"))?;
                if sampled_bytes > request.maximum_bytes {
                    return Err(CdfError::data(format!(
                        "CSV discovery source chunk crossed its {}-byte bound",
                        request.maximum_bytes
                    )));
                }
                chunks.push(chunk);
            }
            let maximum_records = usize::try_from(request.maximum_records)
                .map_err(|_| CdfError::contract("CSV record bound exceeds usize"))?;
            let (schema, sampled_records) = Format::default()
                .with_header(true)
                .infer_schema(AccountedChunksReader::new(chunks), Some(maximum_records))
                .map_err(|error| CdfError::data(format!("infer CSV schema: {error}")))?;
            let schema = Arc::new(schema);
            Ok(PhysicalSchemaObservation {
                identity: source.identity().clone(),
                arrow_schema: schema,
                sampled_bytes,
                sampled_records: u64::try_from(sampled_records)
                    .map_err(|_| CdfError::data("CSV sampled record count exceeds u64"))?,
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
            if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
                return Err(CdfError::contract(
                    "CSV planning requires nonzero row and byte batch targets",
                ));
            }
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "CSV projection and predicate pushdown are unsupported",
                ));
            }
            Ok(vec![DecodeUnitPlan {
                unit_id: "csv-stream".to_owned(),
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
                    "CSV projection and predicate pushdown are unsupported",
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
            let decoder = ReaderBuilder::new(Arc::clone(&request.schema.decoder_schema))
                .with_header(true)
                .with_batch_size(request.target_batch_rows)
                .build_decoder();
            let output_lease = reserve_output(&request).await?;
            let state = DecodeState {
                input,
                current: None,
                offset: 0,
                decoder,
                request,
                output_lease: Some(output_lease),
                sequence: 0,
                input_finished: false,
                terminal: false,
            };
            Ok(Box::pin(stream::try_unfold(state, decode_next)) as PhysicalDecodeStream)
        })
    }
}

struct DecodeState {
    input: AccountedByteStream,
    current: Option<cdf_memory::AccountedBytes>,
    offset: usize,
    decoder: Decoder,
    request: PhysicalDecodeRequest,
    output_lease: Option<MemoryLease>,
    sequence: u64,
    input_finished: bool,
    terminal: bool,
}

async fn decode_next(
    mut state: DecodeState,
) -> Result<Option<(AccountedPhysicalBatch, DecodeState)>> {
    loop {
        state.request.cancellation.check()?;
        if state.terminal {
            return Ok(None);
        }
        if !state.input_finished
            && state
                .current
                .as_ref()
                .is_none_or(|chunk| state.offset == chunk.payload().len())
        {
            state.current = state.input.try_next().await?;
            state.offset = 0;
            state.input_finished = state.current.is_none();
        }
        let consumed = if let Some(chunk) = &state.current {
            state
                .decoder
                .decode(&chunk.payload()[state.offset..])
                .map_err(|error| CdfError::data(format!("decode CSV: {error}")))?
        } else {
            state
                .decoder
                .decode(&[])
                .map_err(|error| CdfError::data(format!("finish CSV decode: {error}")))?
        };
        state.offset += consumed;
        if consumed != 0 {
            continue;
        }
        let record_batch = state
            .decoder
            .flush()
            .map_err(|error| CdfError::data(format!("flush CSV batch: {error}")))?;
        let record_batch = match record_batch {
            Some(batch) => batch,
            None => {
                state.terminal = state.input_finished;
                if state.terminal && state.sequence == 0 {
                    RecordBatch::new_empty(Arc::clone(&state.request.schema.decoder_schema))
                } else if state.terminal {
                    return Ok(None);
                } else {
                    continue;
                }
            }
        };
        let lease = state
            .output_lease
            .take()
            .ok_or_else(|| CdfError::internal("CSV output lease missing"))?;
        let batch_id = BatchId::new(format!(
            "{}-u{:08}-b{:08}",
            state.request.batch_id_prefix, state.request.unit.ordinal, state.sequence
        ))?;
        state.sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(|| CdfError::data("CSV batch sequence overflowed"))?;
        let mut batch = Batch::from_record_batch(
            batch_id,
            state.request.resource_id.clone(),
            state.request.partition_id.clone(),
            cdf_kernel::canonical_arrow_schema_hash(state.request.schema.decoder_schema.as_ref())?,
            record_batch,
        )?;
        if state.request.schema.authority == cdf_runtime::DecodeSchemaAuthority::FixedAdmission {
            batch
                .header
                .mark_materialized_output(state.request.schema.decoder_schema.as_ref())?;
        }
        batch.header.source_position = state.request.source_position.clone();
        let physical = AccountedPhysicalBatch::new(batch, lease)?;
        if !state.terminal {
            state.output_lease = Some(reserve_output(&state.request).await?);
        }
        return Ok(Some((physical, state)));
    }
}

async fn reserve_output(request: &PhysicalDecodeRequest) -> Result<MemoryLease> {
    reserve(
        Arc::clone(&request.memory),
        ReservationRequest::new(
            ConsumerKey::new("csv-output", MemoryClass::Decode)?,
            request.target_batch_bytes.max(1024 * 1024),
        )?
        .as_minimum_working_set(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{PartitionId, PhysicalObservationRepresentation, ResourceId};
    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use cdf_runtime::{AccountedByteStream, DecodeSchemaPlan, RunCancellation};
    use futures_util::stream;

    use super::*;

    #[test]
    fn empty_csv_emits_schema_bearing_materialized_batch() {
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let request = PhysicalDecodeRequest {
            options: serde_json::json!({}),
            unit: DecodeUnitPlan {
                unit_id: "empty-csv".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: 1024 * 1024,
                independently_retryable: true,
            },
            resource_id: ResourceId::new("events.empty").unwrap(),
            partition_id: PartitionId::new("file-empty").unwrap(),
            batch_id_prefix: "events-empty".to_owned(),
            schema: DecodeSchemaPlan::fixed_admission(schema),
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64,
            target_batch_bytes: 1024 * 1024,
            memory,
            cancellation: RunCancellation::default(),
        };
        let batch = futures_executor::block_on(async move {
            let input: AccountedByteStream = Box::pin(stream::empty());
            let decoder = ReaderBuilder::new(Arc::clone(&request.schema.decoder_schema))
                .with_header(true)
                .with_batch_size(request.target_batch_rows)
                .build_decoder();
            let output_lease = reserve_output(&request).await?;
            let state = DecodeState {
                input,
                current: None,
                offset: 0,
                decoder,
                request,
                output_lease: Some(output_lease),
                sequence: 0,
                input_finished: false,
                terminal: false,
            };
            let (batch, state) = decode_next(state)
                .await?
                .ok_or_else(|| CdfError::internal("empty CSV omitted schema-bearing batch"))?;
            if decode_next(state).await?.is_some() {
                return Err(CdfError::internal("empty CSV emitted multiple batches"));
            }
            Result::<AccountedPhysicalBatch>::Ok(batch)
        })
        .unwrap();

        assert_eq!(batch.batch().record_batch().unwrap().num_rows(), 0);
        assert_eq!(
            batch.batch().header.observation_representation,
            PhysicalObservationRepresentation::MaterializedOutput
        );
    }
}
