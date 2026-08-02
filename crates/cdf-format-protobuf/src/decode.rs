//! Prepared Protobuf decode sessions and accounted batch publication.

use std::{mem::size_of, sync::Arc};

use cdf_kernel::{Batch, BatchId, BoxFuture, CdfError, Result};
use cdf_memory::{
    ConsumerKey, MemoryClass, ReservationRequest, record_batch_retained_bytes, reserve,
};
use cdf_runtime::{
    AccountedByteCursor, AccountedPhysicalBatch, ByteSource, DecodeSchemaAuthority, DecodeUnitPlan,
    FormatDecodeSession, PhysicalDecodeRequest, PhysicalDecodeStream, SequentialReadRequest,
};
use futures_util::stream;

use crate::framing::{BufferedMessage, read_length_prefix};
use crate::materialize::build_record_batch;
use crate::options::{OUTPUT_ESTIMATE_MULTIPLIER, ProtobufOptions};
use crate::schema::MessagePlan;

pub(crate) struct ProtobufDecodeSession {
    pub(crate) source: Arc<dyn ByteSource>,
    pub(crate) options: ProtobufOptions,
    pub(crate) complete_plan: MessagePlan,
    pub(crate) projected_plan: MessagePlan,
    pub(crate) units: Vec<DecodeUnitPlan>,
}

impl FormatDecodeSession for ProtobufDecodeSession {
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
            if !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "Protobuf predicate pushdown is unsupported",
                ));
            }
            let requested_projection = self
                .complete_plan
                .projected(request.projection.as_deref())?;
            if requested_projection.arrow_schema != self.projected_plan.arrow_schema {
                return Err(CdfError::contract(
                    "Protobuf decode projection differs from its prepared session",
                ));
            }
            let observed_hash =
                cdf_kernel::canonical_arrow_schema_hash(self.projected_plan.arrow_schema.as_ref())?;
            if request.schema.authority == DecodeSchemaAuthority::VerifiedPhysicalObservation {
                let expected = cdf_kernel::canonical_arrow_schema_hash(
                    request.schema.authority_schema.as_ref(),
                )?;
                if expected != observed_hash {
                    return Err(CdfError::data(format!(
                        "Protobuf descriptor schema changed before decode: planned {expected}, observed {observed_hash}"
                    )));
                }
            }
            let input = self
                .source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: request
                        .target_batch_bytes
                        .min(self.options.maximum_message_bytes)
                        .max(1),
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let state = DecodeState {
                cursor: AccountedByteCursor::new(input),
                request,
                options: self.options.clone(),
                complete_plan: self.complete_plan.clone(),
                projected_plan: self.projected_plan.clone(),
                observed_hash,
                sequence: 0,
                source_row_ordinal: 0,
                finished: false,
            };
            Ok(Box::pin(stream::try_unfold(state, decode_next)) as PhysicalDecodeStream)
        })
    }
}

struct DecodeState {
    cursor: AccountedByteCursor,
    request: PhysicalDecodeRequest,
    pub(crate) options: ProtobufOptions,
    pub(crate) complete_plan: MessagePlan,
    pub(crate) projected_plan: MessagePlan,
    observed_hash: cdf_kernel::SchemaHash,
    sequence: u64,
    source_row_ordinal: u64,
    finished: bool,
}

async fn decode_next(
    mut state: DecodeState,
) -> Result<Option<(AccountedPhysicalBatch, DecodeState)>> {
    if state.finished {
        return Ok(None);
    }
    let mut messages = Vec::new();
    let mut encoded_bytes = 0_u64;
    while messages.len() < state.request.target_batch_rows
        && (messages.is_empty() || encoded_bytes < state.request.target_batch_bytes)
    {
        state.request.cancellation.check()?;
        let Some(length) = read_length_prefix(&mut state.cursor).await? else {
            state.finished = true;
            break;
        };
        if length > state.options.maximum_message_bytes {
            return Err(CdfError::data(format!(
                "Protobuf message {} declares {length} bytes above the configured {}-byte maximum; increase format_options.maximum_message_bytes only for a trusted producer",
                state.source_row_ordinal, state.options.maximum_message_bytes
            )));
        }
        let length_usize = usize::try_from(length)
            .map_err(|_| CdfError::data("Protobuf message length exceeds usize"))?;
        let accounted = length
            .saturating_add(u64::try_from(size_of::<BufferedMessage>()).unwrap_or(64))
            .max(1);
        let lease = reserve(
            Arc::clone(&state.request.memory),
            ReservationRequest::new(
                ConsumerKey::new("protobuf-framed-message", MemoryClass::Decode)?,
                accounted,
            )?,
        )
        .await?;
        let bytes = if length_usize == 0 {
            Vec::new()
        } else {
            state
                .cursor
                .read_exact(length_usize, "Protobuf framed message")
                .await?
        };
        encoded_bytes = encoded_bytes
            .checked_add(length)
            .ok_or_else(|| CdfError::data("Protobuf batch encoded-byte count overflowed"))?;
        messages.push(BufferedMessage {
            bytes,
            _lease: lease,
        });
        state.source_row_ordinal = state
            .source_row_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Protobuf source row ordinal overflowed"))?;
    }
    if messages.is_empty() {
        return Ok(None);
    }
    let batch_start_ordinal = state
        .source_row_ordinal
        .checked_sub(
            u64::try_from(messages.len())
                .map_err(|_| CdfError::data("Protobuf batch row count exceeds u64"))?,
        )
        .ok_or_else(|| CdfError::internal("Protobuf batch ordinal underflowed"))?;
    let output_authority = output_authority_bytes(
        encoded_bytes,
        messages.len(),
        state.projected_plan.fields.len(),
        state.request.target_batch_bytes,
        state.options.maximum_output_batch_bytes,
    )?;
    let output_lease = reserve(
        Arc::clone(&state.request.memory),
        ReservationRequest::new(
            ConsumerKey::new("protobuf-arrow-output", MemoryClass::Decode)?,
            output_authority,
        )?,
    )
    .await?;
    let (record_batch, unknowns) = build_record_batch(
        &state.complete_plan,
        &state.projected_plan,
        &messages,
        batch_start_ordinal,
        state.options.maximum_nesting_depth,
    )?;
    let batch_id = BatchId::new(format!(
        "{}-u{:08}-b{:08}",
        state.request.batch_id_prefix, state.request.unit.ordinal, state.sequence
    ))?;
    let mut batch = Batch::from_record_batch(
        batch_id,
        state.request.resource_id.clone(),
        state.request.partition_id.clone(),
        state.observed_hash.clone(),
        record_batch,
    )?;
    batch.header.source_position = state.request.source_position.clone();
    unknowns.attach(&mut batch)?;
    let actual_bytes = record_batch_retained_bytes(
        batch
            .record_batch()
            .ok_or_else(|| CdfError::internal("Protobuf batch lost its Arrow payload"))?,
    )?
    .checked_add(batch.header.pre_contract_evidence_retained_bytes()?)
    .ok_or_else(|| CdfError::data("Protobuf Arrow output memory overflowed"))?;
    if actual_bytes > state.options.maximum_output_batch_bytes {
        return Err(CdfError::data(format!(
            "Protobuf batch materialized {actual_bytes} bytes above the configured {}-byte output maximum; lower batch sizing or increase format_options.maximum_output_batch_bytes",
            state.options.maximum_output_batch_bytes
        )));
    }
    if actual_bytes > output_lease.bytes() {
        return Err(CdfError::data(format!(
            "Protobuf output estimate reserved {} bytes but the Arrow batch requires {actual_bytes}; lower batch sizing or increase format_options.maximum_output_batch_bytes",
            output_lease.bytes()
        )));
    }
    let physical = AccountedPhysicalBatch::new(batch, output_lease)?;
    state.sequence = state
        .sequence
        .checked_add(1)
        .ok_or_else(|| CdfError::data("Protobuf batch sequence overflowed"))?;
    Ok(Some((physical, state)))
}

fn output_authority_bytes(
    encoded_bytes: u64,
    rows: usize,
    fields: usize,
    target_batch_bytes: u64,
    maximum_output_batch_bytes: u64,
) -> Result<u64> {
    let row_field_overhead = u64::try_from(rows)
        .ok()
        .and_then(|rows| rows.checked_mul(u64::try_from(fields).ok()?))
        .and_then(|cells| cells.checked_mul(32))
        .ok_or_else(|| CdfError::data("Protobuf output estimate overflowed"))?;
    let estimate = encoded_bytes
        .checked_mul(OUTPUT_ESTIMATE_MULTIPLIER)
        .and_then(|bytes| bytes.checked_add(row_field_overhead))
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or_else(|| CdfError::data("Protobuf output estimate overflowed"))?
        .max(target_batch_bytes)
        .max(1);
    if estimate > maximum_output_batch_bytes {
        return Err(CdfError::data(format!(
            "Protobuf batch requires a conservative {estimate}-byte output authority above the configured {maximum_output_batch_bytes}-byte maximum; lower batch sizing or increase format_options.maximum_output_batch_bytes"
        )));
    }
    Ok(estimate)
}
