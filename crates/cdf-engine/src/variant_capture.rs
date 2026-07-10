use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{
    ColumnProgram, NestedAction, RESIDUAL_ENCODING_METADATA_KEY, RESIDUAL_ENCODING_NAME,
    ResidualFieldRef, ValidationProgram, encode_residual_json_v1,
};
use cdf_kernel::{CdfError, Result, source_name, with_semantic, with_source_name};
use serde::{Deserialize, Serialize};

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
        let variant_field = with_semantic(Field::new(column_name, DataType::Utf8, true), semantic);
        let mut metadata = variant_field.metadata().clone();
        metadata.insert(
            RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
            RESIDUAL_ENCODING_NAME.to_owned(),
        );
        fields.push(variant_field.with_metadata(metadata));
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
        let captured = fields
            .iter()
            .map(|field| {
                ResidualFieldRef::new([field.source_name.as_str()], field.array.as_ref(), row)
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(residual_codec_error)?;
        let bytes = encode_residual_json_v1(captured).map_err(residual_codec_error)?;
        let value =
            String::from_utf8(bytes).map_err(|error| CdfError::internal(error.to_string()))?;
        values.push(value);
    }
    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
}

fn residual_codec_error(error: cdf_contract::ResidualCodecError) -> CdfError {
    CdfError::data(format!("{}: {error}", error.code()))
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
