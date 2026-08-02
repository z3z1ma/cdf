//! Bounded and full-content JSON schema discovery.

use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use arrow_json::reader::infer_json_schema;
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{CdfError, Result};
use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest, reserve};
use cdf_runtime::{AccountedByteStream, FormatDiscoveryKind};
use futures_util::TryStreamExt;
use memchr::memchr_iter;

use crate::options::{
    MAXIMUM_DECODE_WORKING_SET_BYTES, maximum_record_bytes_error, validate_maximum_record_bytes,
};

pub(crate) fn validate_json_discovery_kind(kind: FormatDiscoveryKind) -> Result<()> {
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

pub(crate) async fn infer_full_content_json_schema(
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

pub(crate) fn full_content_discovery_evidence(
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
