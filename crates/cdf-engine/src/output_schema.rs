use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_contract::{SCHEMA_COERCION_PLAN_METADATA_KEY, TransformDescription, ValidationProgram};
use cdf_kernel::{CdfError, PHYSICAL_TYPE_METADATA_KEY, Result};

use crate::variant_capture::normalize_batch;

pub(crate) fn compile_output_schema(
    resource_schema: &Schema,
    program: &ValidationProgram,
    final_projection: Option<&[String]>,
    canonicalize_observed_schema: bool,
) -> Result<SchemaRef> {
    let empty = apply_transform_output_schema(
        RecordBatch::new_empty(Arc::new(resource_schema.clone())),
        program,
    )?;
    let projected = match final_projection {
        Some(projection) if !projection.is_empty() => {
            let indices = projection
                .iter()
                .map(|name| {
                    empty.schema().index_of(name).map_err(|_| {
                        CdfError::data(format!(
                            "projected field {name:?} is not present in resource schema"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            empty.project(&indices).map_err(CdfError::from)?
        }
        _ => empty,
    };
    let normalized = normalize_batch(projected, program)?;
    let normalized = if canonicalize_observed_schema {
        canonicalize_effective_output_schema(normalized)?
    } else {
        normalized
    };
    Ok(normalized.schema())
}

fn apply_transform_output_schema(
    mut batch: RecordBatch,
    program: &ValidationProgram,
) -> Result<RecordBatch> {
    for transform in &program.transforms {
        if let TransformDescription::Derive { column, .. } = transform {
            let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
            let mut columns = batch.columns().to_vec();
            let field = Arc::new(Field::new(column, DataType::Boolean, true));
            let values = Arc::new(BooleanArray::new_null(batch.num_rows())) as ArrayRef;
            if let Ok(index) = batch.schema().index_of(column) {
                fields[index] = field;
                columns[index] = values;
            } else {
                fields.push(field);
                columns.push(values);
            }
            batch = RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(
                    fields,
                    batch.schema().metadata().clone(),
                )),
                columns,
            )?;
        }
    }
    Ok(batch)
}

pub(crate) fn canonicalize_effective_output_schema(batch: RecordBatch) -> Result<RecordBatch> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let mut metadata = field.metadata().clone();
            metadata.remove(PHYSICAL_TYPE_METADATA_KEY);
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    let mut metadata = batch.schema().metadata().clone();
    metadata.remove(SCHEMA_COERCION_PLAN_METADATA_KEY);
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(CdfError::from)
}
