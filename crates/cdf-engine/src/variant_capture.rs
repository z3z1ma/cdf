use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{
    CanonicalArrowType, ColumnProgram, NestedAction, PiiRedactionPolicy,
    RESIDUAL_ENCODING_METADATA_KEY, RESIDUAL_ENCODING_NAME, RedactionDecision,
    ResidualCandidateVerdict, ResidualFieldRef, ValidationProgram, encode_residual_json_v1,
};
use cdf_kernel::{
    BatchId, CdfError, Result, SchemaHash, source_name, with_semantic, with_source_name,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContractEvolutionArtifact {
    version: u16,
    baseline_schema_hash: SchemaHash,
    effective_schema_hash: SchemaHash,
    variant_capture: Vec<VariantCaptureArtifact>,
    residual_capture: Option<ResidualCaptureArtifact>,
    residual_decisions: Vec<ResidualDecisionArtifact>,
    promotion_events: Vec<PromotionEventArtifact>,
    implicit_promotion_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ResidualCaptureArtifact {
    version: u16,
    variant_column: String,
    semantic: String,
    encoding: String,
    default_verdict: ResidualCandidateVerdict,
    pii_redaction: PiiRedactionPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResidualDecisionArtifact {
    pub version: u16,
    pub observation_id: Option<String>,
    pub batch_id: BatchId,
    pub source_row_ordinal: u64,
    pub source_path: Vec<String>,
    pub observed_physical_type: CanonicalArrowType,
    pub expected_effective_type: Option<CanonicalArrowType>,
    pub verdict: ResidualRuntimeVerdict,
    pub rule_id: String,
    pub residual_encoding: String,
    pub typed_projection: ResidualTypedProjection,
    pub redaction: RedactionDecision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub(crate) enum ResidualRuntimeVerdict {
    Captured,
    Quarantined,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub(crate) enum ResidualTypedProjection {
    Nulled,
    Absent,
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
    let residual_capture = program
        .residual
        .as_ref()
        .and_then(|residual| residual.capture.as_ref());
    let mut existing_variant = None;

    for (index, field) in batch.schema().fields().iter().enumerate() {
        if residual_capture.is_some_and(|capture| field.name() == &capture.variant_column) {
            let values = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CdfError::contract("residual variant column must be utf8"))?
                .clone();
            existing_variant = Some(values);
            continue;
        }
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

    if let Some(capture) = residual_capture {
        if let Some((name, semantic)) = &variant_column_spec {
            if name != &capture.variant_column || semantic != &capture.semantic {
                return Err(CdfError::contract(
                    "nested and residual capture compile to different variant columns",
                ));
            }
        } else {
            variant_column_spec = Some((capture.variant_column.clone(), capture.semantic.clone()));
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
            existing_variant.as_ref(),
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
    existing: Option<&StringArray>,
) -> Result<ArrayRef> {
    if fields.is_empty() {
        return Ok(existing
            .map(|array| Arc::new(array.clone()) as ArrayRef)
            .unwrap_or_else(|| Arc::new(StringArray::new_null(row_count)) as ArrayRef));
    }
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
        let nested =
            String::from_utf8(bytes).map_err(|error| CdfError::internal(error.to_string()))?;
        let value =
            match existing.and_then(|values| (!values.is_null(row)).then(|| values.value(row))) {
                Some(existing) => merge_residual_json(existing, &nested)?,
                None => nested,
            };
        values.push(Some(value));
    }
    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
}

fn merge_residual_json(existing: &str, nested: &str) -> Result<String> {
    #[derive(Deserialize)]
    struct Envelope {
        v: u64,
        fields: std::collections::BTreeMap<String, serde_json::Value>,
    }
    #[derive(Serialize)]
    struct BorrowedEnvelope<'a> {
        v: u64,
        fields: &'a std::collections::BTreeMap<String, serde_json::Value>,
    }
    let mut existing: Envelope = serde_json::from_str(existing)
        .map_err(|error| CdfError::data(format!("decode existing residual envelope: {error}")))?;
    let nested: Envelope = serde_json::from_str(nested)
        .map_err(|error| CdfError::data(format!("decode nested residual envelope: {error}")))?;
    if existing.v != cdf_contract::RESIDUAL_JSON_V1 || nested.v != cdf_contract::RESIDUAL_JSON_V1 {
        return Err(CdfError::data(
            "cannot merge unsupported residual envelope version",
        ));
    }
    for (path, value) in nested.fields {
        if existing.fields.insert(path.clone(), value).is_some() {
            return Err(CdfError::contract(format!(
                "residual capture produced duplicate source path {path:?}"
            )));
        }
    }
    serde_json::to_string(&BorrowedEnvelope {
        v: cdf_contract::RESIDUAL_JSON_V1,
        fields: &existing.fields,
    })
    .map_err(|error| CdfError::internal(format!("encode merged residual envelope: {error}")))
}

fn residual_codec_error(error: cdf_contract::ResidualCodecError) -> CdfError {
    CdfError::data(format!("{}: {error}", error.code()))
}

pub(crate) fn contract_evolution_artifact(
    program: &ValidationProgram,
    baseline_schema_hash: SchemaHash,
    effective_schema_hash: SchemaHash,
    mut residual_decisions: Vec<ResidualDecisionArtifact>,
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

    let residual_capture = program.residual.as_ref().and_then(|residual| {
        residual
            .capture
            .as_ref()
            .map(|capture| ResidualCaptureArtifact {
                version: 1,
                variant_column: capture.variant_column.clone(),
                semantic: capture.semantic.clone(),
                encoding: capture.encoding.clone(),
                default_verdict: residual.default_verdict,
                pii_redaction: residual.pii_redaction.clone(),
            })
    });

    if residual_capture.is_none() && variant_capture.is_empty() && residual_decisions.is_empty() {
        return None;
    }

    variant_capture.sort_by(|left, right| {
        left.source_field
            .cmp(&right.source_field)
            .then_with(|| left.variant_column.cmp(&right.variant_column))
            .then_with(|| left.semantic.cmp(&right.semantic))
    });
    variant_capture.dedup();
    residual_decisions.sort_by(|left, right| {
        left.observation_id
            .cmp(&right.observation_id)
            .then_with(|| left.batch_id.cmp(&right.batch_id))
            .then_with(|| left.source_row_ordinal.cmp(&right.source_row_ordinal))
            .then_with(|| left.source_path.cmp(&right.source_path))
            .then_with(|| left.verdict.cmp(&right.verdict))
    });
    Some(ContractEvolutionArtifact {
        version: 1,
        baseline_schema_hash,
        effective_schema_hash,
        variant_capture,
        residual_capture,
        residual_decisions,
        promotion_events: Vec::new(),
        implicit_promotion_count: 0,
    })
}
