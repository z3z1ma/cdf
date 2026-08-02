//! Prepared Avro decode sessions and accounted physical-batch publication.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow_array::RecordBatch;
use arrow_avro::reader::{AsyncAvroFileReader, HeaderInfo, ReaderBuilder};
use arrow_schema::SchemaRef;
use cdf_kernel::{Batch, BatchId, BoxFuture, CdfError, Result, SchemaHash};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, record_batch_retained_bytes, reserve,
};
use cdf_runtime::{
    AccountedPhysicalBatch, ByteSource, DecodeUnitPlan, FormatDecodeSession, PhysicalDecodeRequest,
    PhysicalDecodeStream, SequentialReadRequest,
};
use futures_util::{TryStreamExt, stream};

use crate::byte_source::AvroByteSource;
use crate::errors::{avro_arrow_error, avro_error};
use crate::options::{MAXIMUM_WORKING_SET_BYTES, OcfOptions, SingleObjectOptions};
use crate::planning::projection_indices;
use crate::validation::{
    reserve_output, validate_physical_decode_request, validate_schema_authority,
};

pub(crate) struct OcfDecodeSession {
    pub(crate) source: Arc<dyn ByteSource>,
    pub(crate) size: u64,
    pub(crate) header: HeaderInfo,
    pub(crate) physical_schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) options: OcfOptions,
    pub(crate) units: Vec<DecodeUnitPlan>,
    pub(crate) ranges: Vec<Range<u64>>,
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

pub(crate) struct SingleObjectDecodeSession {
    pub(crate) source: Arc<dyn ByteSource>,
    pub(crate) physical_schema: SchemaRef,
    pub(crate) options: SingleObjectOptions,
    pub(crate) projection: Option<Vec<String>>,
    pub(crate) units: Vec<DecodeUnitPlan>,
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
