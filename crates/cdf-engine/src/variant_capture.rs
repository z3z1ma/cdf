use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeListArray, LargeStringArray, ListArray, MapArray, RecordBatch, StringArray,
    StringViewArray, StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{ColumnProgram, NestedAction, ValidationProgram};
use cdf_kernel::{CdfError, Result, source_name, with_semantic, with_source_name};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContractEvolutionArtifact {
    variant_capture: Vec<VariantCaptureArtifact>,
    promotion_events: Vec<PromotionEventArtifact>,
    implicit_promotion_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct VariantCaptureArtifact {
    source_field: String,
    variant_column: String,
    semantic: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PromotionEventArtifact {
    source_field: String,
    target_field: String,
    event_id: String,
}

struct CapturedVariantField {
    source_name: String,
    array: ArrayRef,
}

pub(crate) fn normalize_batch(
    batch: RecordBatch,
    program: &ValidationProgram,
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(batch.num_columns() + 1);
    let mut columns = Vec::with_capacity(batch.num_columns() + 1);
    let mut variant_fields = Vec::new();
    let mut variant_column_spec = None;

    for (index, field) in batch.schema().fields().iter().enumerate() {
        let column = column_program_for_field(field.as_ref(), program)?;
        match &column.nested_action {
            NestedAction::CaptureVariant {
                column_name,
                semantic,
            } => {
                if let Some((existing_name, existing_semantic)) = &variant_column_spec {
                    if existing_name != column_name || existing_semantic != semantic {
                        return Err(CdfError::contract(
                            "validation program contains conflicting variant capture targets",
                        ));
                    }
                } else {
                    variant_column_spec = Some((column_name.clone(), semantic.clone()));
                }
                variant_fields.push(CapturedVariantField {
                    source_name: column.source_name.clone(),
                    array: batch.column(index).clone(),
                });
            }
            _ => {
                fields.push(normalize_field(field.as_ref(), program)?);
                columns.push(batch.column(index).clone());
            }
        }
    }

    if let Some((column_name, semantic)) = variant_column_spec {
        if fields.iter().any(|field| field.name() == &column_name) {
            return Err(CdfError::contract(format!(
                "variant capture column {column_name:?} conflicts with normalized output schema"
            )));
        }
        fields.push(with_semantic(
            Field::new(column_name, DataType::Utf8, true),
            semantic,
        ));
        columns.push(materialize_variant_column(
            batch.num_rows(),
            &variant_fields,
        )?);
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(CdfError::from)
}

fn normalize_field(field: &Field, program: &ValidationProgram) -> Result<Field> {
    let column = column_program_for_field(field, program)?;
    Ok(with_source_name(field.clone(), column.source_name.clone()).with_name(&column.output_name))
}

fn column_program_for_field<'a>(
    field: &Field,
    program: &'a ValidationProgram,
) -> Result<&'a ColumnProgram> {
    let field_source_name = source_name(field).unwrap_or_else(|| field.name());
    program
        .column_programs
        .iter()
        .find(|column| {
            column.source_name == field_source_name || column.output_name == *field.name()
        })
        .ok_or_else(|| {
            CdfError::contract(format!(
                "validation program does not cover field {:?}",
                field.name()
            ))
        })
}

fn materialize_variant_column(
    row_count: usize,
    fields: &[CapturedVariantField],
) -> Result<ArrayRef> {
    let mut values = Vec::with_capacity(row_count);
    for row in 0..row_count {
        let mut captured = BTreeMap::new();
        for field in fields {
            captured.insert(
                field.source_name.clone(),
                arrow_value_to_json(field.array.as_ref(), row)?,
            );
        }
        let bytes = cdf_package::canonical_json_bytes(&captured)?;
        let value =
            String::from_utf8(bytes).map_err(|error| CdfError::internal(error.to_string()))?;
        values.push(value);
    }
    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
}

fn arrow_value_to_json(array: &dyn Array, row: usize) -> Result<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }

    match array.data_type() {
        DataType::Boolean => Ok(Value::Bool(
            downcast_array::<BooleanArray>(array, "Boolean")?.value(row),
        )),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => integer_value_to_json(array, row),
        DataType::Float32 | DataType::Float64 => float_value_to_json(array, row),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            string_value_to_json(array, row)
        }
        DataType::Struct(_) => {
            struct_value_to_json(downcast_array::<StructArray>(array, "Struct")?, row)
        }
        DataType::List(_) => list_value_to_json(
            downcast_array::<ListArray>(array, "List")?
                .value(row)
                .as_ref(),
        ),
        DataType::LargeList(_) => list_value_to_json(
            downcast_array::<LargeListArray>(array, "LargeList")?
                .value(row)
                .as_ref(),
        ),
        DataType::Map(_, _) => map_value_to_json(downcast_array::<MapArray>(array, "Map")?, row),
        other => Err(CdfError::contract(format!(
            "variant capture does not support Arrow type {other}"
        ))),
    }
}

fn integer_value_to_json(array: &dyn Array, row: usize) -> Result<Value> {
    match array.data_type() {
        DataType::Int8 => {
            Ok(Number::from(downcast_array::<Int8Array>(array, "Int8")?.value(row)).into())
        }
        DataType::Int16 => {
            Ok(Number::from(downcast_array::<Int16Array>(array, "Int16")?.value(row)).into())
        }
        DataType::Int32 => {
            Ok(Number::from(downcast_array::<Int32Array>(array, "Int32")?.value(row)).into())
        }
        DataType::Int64 => {
            Ok(Number::from(downcast_array::<Int64Array>(array, "Int64")?.value(row)).into())
        }
        DataType::UInt8 => {
            Ok(Number::from(downcast_array::<UInt8Array>(array, "UInt8")?.value(row)).into())
        }
        DataType::UInt16 => {
            Ok(Number::from(downcast_array::<UInt16Array>(array, "UInt16")?.value(row)).into())
        }
        DataType::UInt32 => {
            Ok(Number::from(downcast_array::<UInt32Array>(array, "UInt32")?.value(row)).into())
        }
        DataType::UInt64 => {
            Ok(Number::from(downcast_array::<UInt64Array>(array, "UInt64")?.value(row)).into())
        }
        other => Err(CdfError::internal(format!(
            "Arrow type {other} was routed to integer variant capture"
        ))),
    }
}

fn float_value_to_json(array: &dyn Array, row: usize) -> Result<Value> {
    match array.data_type() {
        DataType::Float32 => finite_json_number(
            downcast_array::<Float32Array>(array, "Float32")?
                .value(row)
                .into(),
        ),
        DataType::Float64 => {
            finite_json_number(downcast_array::<Float64Array>(array, "Float64")?.value(row))
        }
        other => Err(CdfError::internal(format!(
            "Arrow type {other} was routed to float variant capture"
        ))),
    }
}

fn string_value_to_json(array: &dyn Array, row: usize) -> Result<Value> {
    match array.data_type() {
        DataType::Utf8 => Ok(Value::String(
            downcast_array::<StringArray>(array, "Utf8")?
                .value(row)
                .to_owned(),
        )),
        DataType::LargeUtf8 => Ok(Value::String(
            downcast_array::<LargeStringArray>(array, "LargeUtf8")?
                .value(row)
                .to_owned(),
        )),
        DataType::Utf8View => Ok(Value::String(
            downcast_array::<StringViewArray>(array, "Utf8View")?
                .value(row)
                .to_owned(),
        )),
        other => Err(CdfError::internal(format!(
            "Arrow type {other} was routed to string variant capture"
        ))),
    }
}

fn downcast_array<'a, T: 'static>(array: &'a dyn Array, expected: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        CdfError::internal(format!(
            "Arrow array declared as {expected} could not be downcast for variant capture"
        ))
    })
}

fn finite_json_number(value: f64) -> Result<Value> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| CdfError::contract("variant capture cannot encode non-finite float values"))
}

fn struct_value_to_json(array: &StructArray, row: usize) -> Result<Value> {
    let mut fields = BTreeMap::new();
    for (field, column) in array.fields().iter().zip(array.columns()) {
        fields.insert(
            field.name().clone(),
            arrow_value_to_json(column.as_ref(), row)?,
        );
    }
    serde_json::to_value(fields).map_err(|error| CdfError::data(error.to_string()))
}

fn list_value_to_json(array: &dyn Array) -> Result<Value> {
    let mut values = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        values.push(arrow_value_to_json(array, row)?);
    }
    Ok(Value::Array(values))
}

fn map_value_to_json(array: &MapArray, row: usize) -> Result<Value> {
    let entries = array.value(row);
    let mut values = Vec::with_capacity(entries.len());
    let key_column = entries.column(0);
    let value_column = entries.column(1);
    for entry_row in 0..entries.len() {
        let mut entry = BTreeMap::new();
        entry.insert(
            "key".to_owned(),
            arrow_value_to_json(key_column.as_ref(), entry_row)?,
        );
        entry.insert(
            "value".to_owned(),
            arrow_value_to_json(value_column.as_ref(), entry_row)?,
        );
        values
            .push(serde_json::to_value(entry).map_err(|error| CdfError::data(error.to_string()))?);
    }
    Ok(Value::Array(values))
}

pub(crate) fn contract_evolution_artifact(
    program: &ValidationProgram,
) -> Option<ContractEvolutionArtifact> {
    let mut variant_capture = program
        .column_programs
        .iter()
        .filter_map(|column| match &column.nested_action {
            NestedAction::CaptureVariant {
                column_name,
                semantic,
            } => Some(VariantCaptureArtifact {
                source_field: column.source_name.clone(),
                variant_column: column_name.clone(),
                semantic: semantic.clone(),
            }),
            _ => None,
        })
        .collect::<Vec<_>>();

    if variant_capture.is_empty() {
        return None;
    }

    variant_capture.sort_by(|left, right| {
        left.source_field
            .cmp(&right.source_field)
            .then_with(|| left.variant_column.cmp(&right.variant_column))
            .then_with(|| left.semantic.cmp(&right.semantic))
    });
    Some(ContractEvolutionArtifact {
        variant_capture,
        promotion_events: Vec::new(),
        implicit_promotion_count: 0,
    })
}
