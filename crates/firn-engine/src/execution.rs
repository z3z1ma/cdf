use std::{path::Path, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use firn_contract::ValidationProgram;
use firn_kernel::{FirnError, ResourceStream, Result, RunId, SegmentId, with_source_name};
use firn_package::{PackageBuilder, PackageStatus};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{Instrument, Span, info_span};

use crate::{
    EnginePackageDraft, EnginePlan, EngineRunOutput, EngineRunOutputWithSegmentPositions,
    EngineSegmentPosition, ExecutionProfile, LineageSummary, planning::validate_program,
    predicates::apply_residual_filters,
};

pub type PackagePreFinalizeHook<'a> =
    dyn Fn(&PackageBuilder, EnginePackageDraft<'_>) -> Result<()> + 'a;

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

#[derive(Clone, Debug)]
struct ExecutionTraceContext {
    run_id: String,
    resource_id: String,
    package_id: String,
}

impl ExecutionTraceContext {
    fn new(run_id: &RunId, plan: &EnginePlan) -> Self {
        Self {
            run_id: run_id.as_str().to_owned(),
            resource_id: plan.scan.request.resource_id.as_str().to_owned(),
            package_id: plan.package_id.clone(),
        }
    }
}

pub async fn execute_to_package<R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
) -> Result<EngineRunOutput>
where
    R: ResourceStream + ?Sized,
{
    Ok(
        execute_to_package_inner(None, plan, resource, package_dir, None)
            .await?
            .output,
    )
}

pub async fn execute_to_package_with_run_id<R>(
    run_id: &RunId,
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
) -> Result<EngineRunOutput>
where
    R: ResourceStream + ?Sized,
{
    let trace_context = ExecutionTraceContext::new(run_id, plan);
    Ok(
        execute_to_package_inner(Some(&trace_context), plan, resource, package_dir, None)
            .instrument(package_execution_span(&trace_context))
            .await?
            .output,
    )
}

pub async fn execute_to_package_with_segment_positions<R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
) -> Result<EngineRunOutputWithSegmentPositions>
where
    R: ResourceStream + ?Sized,
{
    execute_to_package_inner(None, plan, resource, package_dir, None).await
}

pub async fn execute_to_package_with_segment_positions_and_pre_finalize<R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
) -> Result<EngineRunOutputWithSegmentPositions>
where
    R: ResourceStream + ?Sized,
{
    execute_to_package_inner(None, plan, resource, package_dir, Some(pre_finalize)).await
}

async fn execute_to_package_inner<R>(
    trace_context: Option<&ExecutionTraceContext>,
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: Option<&PackagePreFinalizeHook<'_>>,
) -> Result<EngineRunOutputWithSegmentPositions>
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
    let mut segment_positions = Vec::new();
    let mut remaining_limit = plan.scan.request.limit;
    let mut output_schema = None;

    for partition in plan.scan.partitions.clone() {
        if remaining_limit == Some(0) {
            break;
        }

        let partition_span = trace_context
            .map(|context| partition_execution_span(context, partition.partition_id.as_str()))
            .unwrap_or_else(Span::none);

        async {
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
                segment_positions.push(EngineSegmentPosition {
                    segment_id: segment.segment_id.clone(),
                    output_position: batch.header.source_position.clone(),
                });
                segments.push(segment);
            }
            Ok(())
        }
        .instrument(partition_span)
        .await?;
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
    if let Some(pre_finalize) = pre_finalize {
        pre_finalize(
            &builder,
            EnginePackageDraft {
                segments: &segments,
                profile: &profile,
                lineage: &lineage,
                segment_positions: &segment_positions,
            },
        )?;
    }
    let manifest = builder.finish()?;

    Ok(EngineRunOutputWithSegmentPositions {
        output: EngineRunOutput {
            manifest,
            segments,
            profile,
            lineage,
        },
        segment_positions,
    })
}

fn package_execution_span(context: &ExecutionTraceContext) -> Span {
    info_span!(
        "firn_engine.package_execution",
        run_id = context.run_id.as_str(),
        resource_id = context.resource_id.as_str(),
        package_id = context.package_id.as_str()
    )
}

fn partition_execution_span(context: &ExecutionTraceContext, partition_id: &str) -> Span {
    info_span!(
        "firn_engine.partition_execution",
        run_id = context.run_id.as_str(),
        resource_id = context.resource_id.as_str(),
        package_id = context.package_id.as_str(),
        partition_id = partition_id
    )
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
