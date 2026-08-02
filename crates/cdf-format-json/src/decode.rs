//! Incremental JSON decoding, recovery, residual evidence, and output accounting.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    UInt64Array, new_null_array,
};
use arrow_json::reader::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_kernel::{
    Batch, BatchId, CdfError, PreContractResidualCandidate, Result, source_name, with_physical_type,
};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteStream, AccountedPhysicalBatch, DecodeSchemaAuthority, PhysicalDecodeRequest,
    PhysicalDecodeStream,
};
use futures_util::{TryStreamExt, stream};
use memchr::{memchr, memchr_iter, memrchr};
use serde_json::value::RawValue;

use crate::options::{
    MAXIMUM_CONFIGURED_RECORD_BYTES, MAXIMUM_DECODE_WORKING_SET_BYTES, maximum_record_bytes_error,
    validate_maximum_record_bytes,
};
use crate::raw::BorrowedJsonObject;

pub(crate) async fn decode_ndjson_stream(
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
        let requires_admission_recovery = requires_record_admission_recovery(&state.request);
        let flushed = state.decoder.flush();
        let (record_batch, candidates, materialized_residuals_complete) = match flushed {
            Ok(Some(_)) if requires_admission_recovery => {
                let recovered = recover_decode_window(
                    &state.retained,
                    state.retained_bytes,
                    &state.request,
                    state.source_row_ordinal,
                )
                .await?;
                state.decoder = strict_decoder(
                    Arc::clone(&state.request.schema.decoder_schema),
                    state.request.target_batch_rows,
                )?;
                (recovered.0, recovered.1, true)
            }
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
        let record_batch = materialize_json_authority_schema(record_batch, &state.request)?;
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

pub(crate) fn next_decode_window_target(current: u64, observed_output: u64, ceiling: u64) -> u64 {
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

fn requires_record_admission_recovery(request: &PhysicalDecodeRequest) -> bool {
    if request.schema.authority != DecodeSchemaAuthority::FixedAdmission {
        return false;
    }
    let Some(observed) = request.schema.observed_physical_schema.as_ref() else {
        return false;
    };
    let decoder = &request.schema.decoder_schema;
    observed.fields().len() != decoder.fields().len()
        || observed.fields().iter().any(|observed_field| {
            decoder
                .fields()
                .iter()
                .find(|declared| {
                    source_name(declared.as_ref()).unwrap_or_else(|| declared.name())
                        == observed_field.name()
                })
                .is_none_or(|declared| declared.data_type() != observed_field.data_type())
        })
}

fn materialize_json_authority_schema(
    record_batch: RecordBatch,
    request: &PhysicalDecodeRequest,
) -> Result<RecordBatch> {
    if request.schema.authority != DecodeSchemaAuthority::FixedAdmission {
        return Ok(record_batch);
    }
    if record_batch.num_columns() != request.schema.authority_schema.fields().len() {
        return Err(CdfError::data(
            "fixed-admission JSON output does not match its compiled authority field count",
        ));
    }
    let fields = record_batch
        .schema()
        .fields()
        .iter()
        .zip(request.schema.authority_schema.fields())
        .map(|(decoded, authority)| {
            if decoded.data_type() != authority.data_type() {
                return Err(CdfError::data(format!(
                    "fixed-admission JSON field {:?} decoded as {} instead of compiled type {}",
                    authority.name(),
                    decoded.data_type(),
                    authority.data_type()
                )));
            }
            Ok(Arc::new(
                authority
                    .as_ref()
                    .clone()
                    .with_nullable(decoded.is_nullable()),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        request.schema.authority_schema.metadata().clone(),
    ));
    RecordBatch::try_new(schema, record_batch.columns().to_vec()).map_err(CdfError::from)
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

fn raw_value_compatible(field: &Field, value: &RawValue) -> Result<bool> {
    let raw = value.get();
    if raw == "null" {
        return Ok(field.is_nullable());
    }
    let bytes = raw.as_bytes();
    let lexical_match = match field.data_type() {
        DataType::Boolean => matches!(raw, "true" | "false"),
        DataType::Int8 => raw.parse::<i8>().is_ok(),
        DataType::Int16 => raw.parse::<i16>().is_ok(),
        DataType::Int32 => raw.parse::<i32>().is_ok(),
        DataType::Int64 => raw.parse::<i64>().is_ok(),
        DataType::UInt8 => raw.parse::<u8>().is_ok(),
        DataType::UInt16 => raw.parse::<u16>().is_ok(),
        DataType::UInt32 => raw.parse::<u32>().is_ok(),
        DataType::UInt64 => raw.parse::<u64>().is_ok(),
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => raw.parse::<f64>().is_ok_and(|number| number.is_finite()),
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_) => bytes.first() == Some(&b'"'),
        _ => {
            return raw_value_compatible_via_decoder(field, value);
        }
    };
    Ok(lexical_match)
}

fn raw_value_compatible_via_decoder(field: &Field, value: &RawValue) -> Result<bool> {
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
