use std::{path::Path, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use firn_contract::ValidationProgram;
use firn_kernel::{FirnError, ResourceStream, Result, SegmentId, with_source_name};
use firn_package::{PackageBuilder, PackageStatus};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

use crate::{
    EnginePlan, EngineRunOutput, ExecutionProfile, LineageSummary, planning::validate_program,
    predicates::apply_residual_filters,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SchemaArtifact {
    fields: Vec<SchemaFieldArtifact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SchemaFieldArtifact {
    name: String,
    data_type: String,
    nullable: bool,
}

pub async fn execute_to_package<R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
) -> Result<EngineRunOutput>
where
    R: ResourceStream + ?Sized,
{
    validate_program(&plan.validation_program)?;

    let mut builder = PackageBuilder::create(package_dir, plan.package_id.clone())?;
    builder.update_status(PackageStatus::Extracting)?;
    builder.write_json_artifact("plan/scan.json", &plan.scan)?;
    builder.write_json_artifact("plan/explain.json", &plan.explain)?;
    builder.write_json_artifact("plan/validation-program.json", &plan.validation_program)?;

    let mut profile = ExecutionProfile::default();
    let mut lineage = LineageSummary::default();
    let mut segments = Vec::new();
    let mut remaining_limit = plan.scan.request.limit;
    let mut output_schema = None;

    for partition in plan.scan.partitions.clone() {
        if remaining_limit == Some(0) {
            break;
        }

        let mut stream = resource.open(partition).await?;
        while let Some(batch) = stream.next().await {
            if remaining_limit == Some(0) {
                break;
            }

            let batch = batch?;
            lineage.input_batches.push(batch.header.batch_id.clone());
            let Some(record_batch) = batch.record_batch() else {
                return Err(FirnError::data(
                    "package execution requires in-memory Arrow record batches at MVP",
                ));
            };

            let output = execute_batch(record_batch, plan, &mut remaining_limit)?;
            if output.num_rows() == 0 {
                continue;
            }

            let output = apply_contract_exec(output, &plan.validation_program)?;
            let output = apply_normalize_exec(output, &plan.validation_program)?;
            output_schema = Some(schema_artifact(output.schema().as_ref()));
            profile.output_rows += output.num_rows() as u64;
            profile.output_bytes += output.get_array_memory_size() as u64;
            profile.output_batches += 1;

            let segment_id = SegmentId::new(format!("seg-{:06}", segments.len() + 1))?;
            let segment = builder.write_segment(segment_id.clone(), &[output])?;
            lineage.output_segments.push(segment_id);
            segments.push(segment);
        }
    }

    builder.write_json_artifact(
        "schema/output.json",
        &output_schema.unwrap_or(SchemaArtifact { fields: Vec::new() }),
    )?;
    builder.write_stats_artifact(
        "profile.json",
        &firn_package::canonical_json_bytes(&profile)?,
    )?;
    builder.write_lineage_artifact(
        "lineage.json",
        &firn_package::canonical_json_bytes(&lineage)?,
    )?;
    builder.update_status(PackageStatus::Validated)?;
    let manifest = builder.finish()?;

    Ok(EngineRunOutput {
        manifest,
        segments,
        profile,
        lineage,
    })
}

fn execute_batch(
    batch: &RecordBatch,
    plan: &EnginePlan,
    remaining_limit: &mut Option<u64>,
) -> Result<RecordBatch> {
    let filtered = apply_residual_filters(batch, &plan.residual_predicates)?;
    let limited = match remaining_limit {
        Some(remaining) => {
            let take = (*remaining).min(filtered.num_rows() as u64) as usize;
            *remaining -= take as u64;
            filtered.slice(0, take)
        }
        None => filtered,
    };
    apply_projection(&limited, plan.final_projection.as_deref())
}

fn apply_projection(batch: &RecordBatch, projection: Option<&[String]>) -> Result<RecordBatch> {
    let Some(projection) = projection else {
        return Ok(batch.clone());
    };
    if projection.is_empty() {
        return Ok(batch.clone());
    }

    let indices = projection
        .iter()
        .map(|name| {
            batch.schema().index_of(name).map_err(|_| {
                FirnError::data(format!(
                    "projected field {name:?} is not present in resource batch"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    batch.project(&indices).map_err(FirnError::from)
}

fn apply_contract_exec(batch: RecordBatch, program: &ValidationProgram) -> Result<RecordBatch> {
    for field in batch.schema().fields() {
        let field_name = field.name();
        let covered = program
            .column_programs
            .iter()
            .any(|column| column.source_name == *field_name || column.output_name == *field_name);
        if !covered {
            return Err(FirnError::contract(format!(
                "validation program does not cover field {field_name:?}"
            )));
        }
    }
    Ok(batch)
}

fn apply_normalize_exec(batch: RecordBatch, program: &ValidationProgram) -> Result<RecordBatch> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| normalize_field(field.as_ref(), program))
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(FirnError::from)
}

fn normalize_field(field: &Field, program: &ValidationProgram) -> Result<Field> {
    let Some(column) = program
        .column_programs
        .iter()
        .find(|column| column.source_name == *field.name() || column.output_name == *field.name())
    else {
        return Err(FirnError::contract(format!(
            "validation program does not cover field {:?}",
            field.name()
        )));
    };
    Ok(with_source_name(field.clone(), column.source_name.clone()).with_name(&column.output_name))
}

fn schema_artifact(schema: &Schema) -> SchemaArtifact {
    SchemaArtifact {
        fields: schema
            .fields()
            .iter()
            .map(|field| SchemaFieldArtifact {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
            })
            .collect(),
    }
}
