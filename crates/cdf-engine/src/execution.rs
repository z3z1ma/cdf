use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use arrow_select::filter::filter_record_batch;
use cdf_contract::{
    ContractEvaluationContext, QuarantineCandidate, ValidationProgram, VerdictSummary,
    evaluate_package_order_dedup, evaluate_record_batch, reject_untrusted_schema_coercion_metadata,
    schema_coercion_plan_from_trusted_json,
};
use cdf_kernel::{
    CdfError, PHYSICAL_TYPE_METADATA_KEY, PreContractObservedValue, PreContractQuarantineFact,
    ResourceStream, Result, RunId, SOURCE_NAME_METADATA_KEY, ScopeKey, SegmentId, SourcePosition,
    WriteDisposition, semantic,
};
use cdf_package::{PackageBuilder, PackageStatus, QuarantineObservedValue, QuarantineRecord};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{Instrument, Span, info_span};

use crate::{
    EnginePackageDraft, EnginePlan, EngineRunOutput, EngineRunOutputWithSegmentPositions,
    EngineSegmentPosition, ExecutionProfile, LineageSummary,
    planning::validate_program,
    predicates::apply_residual_filters,
    variant_capture::{contract_evolution_artifact, normalize_batch},
};

pub type PackagePreFinalizeHook<'a> =
    dyn Fn(&PackageBuilder, EnginePackageDraft<'_>) -> Result<()> + 'a;

pub fn normalize_record_batch(
    batch: RecordBatch,
    program: &ValidationProgram,
) -> Result<RecordBatch> {
    normalize_batch(batch, program)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SchemaArtifact {
    fields: Vec<SchemaFieldArtifact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SchemaFieldArtifact {
    name: String,
    data_type: String,
    nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    semantic: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
struct ExecutionTraceContext {
    run_id: String,
    resource_id: String,
    package_id: String,
}

struct ContractExecOutput {
    accepted: RecordBatch,
    quarantine_records: Vec<QuarantineRecord>,
    summary: VerdictSummary,
}

struct PendingDedupBatch {
    accepted: RecordBatch,
    output_position: Option<SourcePosition>,
}

struct OutputWriteState<'a> {
    profile: &'a mut ExecutionProfile,
    lineage: &'a mut LineageSummary,
    segments: &'a mut Vec<cdf_package::SegmentEntry>,
    segment_positions: &'a mut Vec<EngineSegmentPosition>,
    output_schema: &'a mut Option<SchemaArtifact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct QuarantineSummaryArtifact {
    quarantined_rows: u64,
    quarantine_candidate_count: u64,
    artifact_count: u64,
    artifacts: Vec<String>,
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
    let mut validation_program = plan.validation_program.clone();
    validate_program(&validation_program)?;

    let mut builder = PackageBuilder::create(package_dir, plan.package_id.clone())?;
    builder.update_status(PackageStatus::Extracting)?;
    builder.write_json_artifact("plan/scan.json", &plan.scan)?;
    builder.write_json_artifact("plan/explain.json", &plan.explain)?;
    builder.write_json_artifact("plan/validation-program.json", &validation_program)?;
    let package_evaluation_context =
        ContractEvaluationContext::observed_at(current_observed_at_ms()?);
    if validation_program.requires_observed_at_ms() {
        builder.write_json_artifact(
            "plan/contract-evaluation-context.json",
            &package_evaluation_context,
        )?;
    }

    let mut profile = ExecutionProfile::default();
    let mut verdict_summary = VerdictSummary::default();
    let mut lineage = LineageSummary::default();
    let mut segments = Vec::new();
    let mut segment_positions = Vec::new();
    let mut quarantine_artifacts = Vec::new();
    let mut quarantine_part_count = 0_usize;
    let mut remaining_limit = plan.scan.request.limit;
    let mut output_schema = None;
    let mut schema_coercion = validation_program.schema_coercion.clone();
    let apply_merge_dedup =
        plan.write_disposition == WriteDisposition::Merge && validation_program.has_dedup_rule();
    let mut pending_dedup_batches = Vec::new();

    for partition in plan.scan.partitions.clone() {
        if remaining_limit == Some(0) {
            break;
        }
        let partition_scope = partition.scope.clone();

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
                if !batch.header.pre_contract_quarantine.is_empty() {
                    let quarantine_records =
                        quarantine_records_from_pre_contract(&batch.header.pre_contract_quarantine);
                    merge_verdict_summary(
                        &mut verdict_summary,
                        pre_contract_quarantine_summary(&batch.header.pre_contract_quarantine),
                    );
                    write_quarantine_part(
                        &mut builder,
                        &quarantine_records,
                        &mut quarantine_part_count,
                        &mut quarantine_artifacts,
                    )?;
                }
                let Some(record_batch) = batch.record_batch() else {
                    return Err(CdfError::data(
                        "package execution requires in-memory Arrow record batches at MVP",
                    ));
                };
                let batch_coercion = match batch.header.schema_coercion_plan.as_deref() {
                    Some(serialized) => Some(schema_coercion_plan_from_trusted_json(
                        record_batch.schema().as_ref(),
                        serialized,
                    )?),
                    None => {
                        reject_untrusted_schema_coercion_metadata(record_batch.schema().as_ref())?;
                        None
                    }
                };
                if let Some(batch_coercion) = batch_coercion {
                    if let Some(existing) = &schema_coercion
                        && existing != &batch_coercion
                    {
                        return Err(CdfError::data(
                            "input batches carry inconsistent schema coercion evidence",
                        ));
                    }
                    schema_coercion = Some(batch_coercion);
                }

                let output = execute_batch(record_batch, plan, &mut remaining_limit)?;
                if output.num_rows() == 0 {
                    continue;
                }
                let batch_source_position = normalize_source_position_for_partition(
                    batch.header.source_position.clone(),
                    &partition_scope,
                );

                let evaluation_context = package_evaluation_context
                    .clone()
                    .with_source_position(batch_source_position.clone());
                let ContractExecOutput {
                    accepted,
                    quarantine_records,
                    summary,
                } = apply_contract_exec(output, &validation_program, &evaluation_context)?;
                merge_verdict_summary(&mut verdict_summary, summary);
                if !quarantine_records.is_empty() {
                    write_quarantine_part(
                        &mut builder,
                        &quarantine_records,
                        &mut quarantine_part_count,
                        &mut quarantine_artifacts,
                    )?;
                }
                let output = accepted;
                if output.num_rows() == 0 {
                    continue;
                }
                if apply_merge_dedup {
                    pending_dedup_batches.push(PendingDedupBatch {
                        accepted: output,
                        output_position: batch_source_position,
                    });
                    continue;
                }
                write_output_batch(
                    &mut builder,
                    &validation_program,
                    output,
                    batch_source_position,
                    &mut OutputWriteState {
                        profile: &mut profile,
                        lineage: &mut lineage,
                        segments: &mut segments,
                        segment_positions: &mut segment_positions,
                        output_schema: &mut output_schema,
                    },
                )?;
            }
            Ok(())
        }
        .instrument(partition_span)
        .await?;
    }

    if apply_merge_dedup {
        apply_dedup_and_write_pending_batches(
            &mut builder,
            &validation_program,
            pending_dedup_batches,
            &mut OutputWriteState {
                profile: &mut profile,
                lineage: &mut lineage,
                segments: &mut segments,
                segment_positions: &mut segment_positions,
                output_schema: &mut output_schema,
            },
        )?;
    }

    if validation_program.schema_coercion.is_none() {
        validation_program.schema_coercion = schema_coercion;
    }
    builder.write_json_artifact("plan/validation-program.json", &validation_program)?;
    if let Some(coercion) = &validation_program.schema_coercion {
        builder.write_json_artifact("schema/coercion-plan.json", coercion)?;
    }
    builder.write_json_artifact(
        "schema/output.json",
        &output_schema.unwrap_or(SchemaArtifact { fields: Vec::new() }),
    )?;
    if let Some(evolution) = contract_evolution_artifact(&validation_program) {
        builder.write_json_artifact("schema/contract-evolution.json", &evolution)?;
    }
    builder.write_stats_artifact(
        "profile.json",
        &cdf_package::canonical_json_bytes(&profile)?,
    )?;
    if verdict_summary.violation_count > 0 || verdict_summary.quarantine_candidate_count > 0 {
        builder.write_stats_artifact(
            "verdict-summary.json",
            &cdf_package::canonical_json_bytes(&verdict_summary)?,
        )?;
    }
    if verdict_summary.quarantine_candidate_count > 0 {
        builder.write_stats_artifact(
            "quarantine-summary.json",
            &cdf_package::canonical_json_bytes(&QuarantineSummaryArtifact {
                quarantined_rows: verdict_summary.quarantined_rows,
                quarantine_candidate_count: verdict_summary.quarantine_candidate_count,
                artifact_count: quarantine_artifacts.len() as u64,
                artifacts: quarantine_artifacts,
            })?,
        )?;
    }
    builder.write_lineage_artifact(
        "lineage.json",
        &cdf_package::canonical_json_bytes(&lineage)?,
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

fn apply_dedup_and_write_pending_batches(
    builder: &mut PackageBuilder,
    program: &ValidationProgram,
    pending: Vec<PendingDedupBatch>,
    state: &mut OutputWriteState<'_>,
) -> Result<()> {
    let accepted = pending
        .iter()
        .map(|batch| batch.accepted.clone())
        .collect::<Vec<_>>();
    let Some(dedup) = evaluate_package_order_dedup(program, &accepted)? else {
        return Ok(());
    };
    builder.write_dedup_summary(&dedup.summary)?;

    for (pending, retained_rows) in pending.into_iter().zip(dedup.retained_rows) {
        let output =
            filter_record_batch(&pending.accepted, &retained_rows).map_err(CdfError::from)?;
        if output.num_rows() == 0 {
            continue;
        }
        write_output_batch(builder, program, output, pending.output_position, state)?;
    }
    Ok(())
}

fn normalize_source_position_for_partition(
    position: Option<SourcePosition>,
    scope: &ScopeKey,
) -> Option<SourcePosition> {
    match (position, scope) {
        (Some(SourcePosition::FileManifest(mut manifest)), ScopeKey::File { path }) => {
            for file in &mut manifest.files {
                file.path = path.clone();
            }
            Some(SourcePosition::FileManifest(manifest))
        }
        (position, _) => position,
    }
}

fn merge_verdict_summary(total: &mut VerdictSummary, batch: VerdictSummary) {
    total.input_rows += batch.input_rows;
    total.accepted_rows += batch.accepted_rows;
    total.quarantined_rows += batch.quarantined_rows;
    total.violation_count += batch.violation_count;
    total.quarantine_candidate_count += batch.quarantine_candidate_count;

    for rule in batch.rule_summaries {
        if let Some(existing) = total.rule_summaries.iter_mut().find(|existing| {
            existing.rule_id == rule.rule_id && existing.error_code == rule.error_code
        }) {
            existing.checked_rows += rule.checked_rows;
            existing.violation_count += rule.violation_count;
        } else {
            total.rule_summaries.push(rule);
        }
    }
}

fn pre_contract_quarantine_summary(facts: &[PreContractQuarantineFact]) -> VerdictSummary {
    let quarantined_rows = facts
        .iter()
        .map(|fact| fact.source_row_ordinal)
        .collect::<BTreeSet<_>>()
        .len() as u64;
    let mut summary = VerdictSummary {
        input_rows: quarantined_rows,
        accepted_rows: 0,
        quarantined_rows,
        violation_count: facts.len() as u64,
        quarantine_candidate_count: facts.len() as u64,
        rule_summaries: Vec::new(),
    };

    for fact in facts {
        if let Some(existing) = summary.rule_summaries.iter_mut().find(|existing| {
            existing.rule_id == fact.rule_id && existing.error_code == fact.error_code
        }) {
            existing.checked_rows += 1;
            existing.violation_count += 1;
        } else {
            summary
                .rule_summaries
                .push(cdf_contract::RuleVerdictSummary {
                    rule_id: fact.rule_id.clone(),
                    error_code: fact.error_code.clone(),
                    checked_rows: 1,
                    violation_count: 1,
                });
        }
    }

    summary
}

fn quarantine_records_from_pre_contract(
    facts: &[PreContractQuarantineFact],
) -> Vec<QuarantineRecord> {
    facts
        .iter()
        .map(|fact| QuarantineRecord {
            source_row_ordinal: fact.source_row_ordinal,
            rule_id: fact.rule_id.clone(),
            error_code: fact.error_code.clone(),
            source_position: fact.source_position.clone(),
            observed_value_redacted: pre_contract_observed_value(&fact.observed_value_redacted),
        })
        .collect()
}

fn pre_contract_observed_value(value: &PreContractObservedValue) -> QuarantineObservedValue {
    match value {
        PreContractObservedValue::Null => QuarantineObservedValue::Null,
        PreContractObservedValue::Preserved { value } => QuarantineObservedValue::Preserved {
            value: value.clone(),
        },
        PreContractObservedValue::Hashed { algorithm, value } => QuarantineObservedValue::Hashed {
            algorithm: algorithm.clone(),
            value: value.clone(),
        },
        PreContractObservedValue::Omitted => QuarantineObservedValue::Omitted,
        PreContractObservedValue::Masked { value } => QuarantineObservedValue::Masked {
            value: value.clone(),
        },
    }
}

fn write_quarantine_part(
    builder: &mut PackageBuilder,
    quarantine_records: &[QuarantineRecord],
    quarantine_part_count: &mut usize,
    quarantine_artifacts: &mut Vec<String>,
) -> Result<()> {
    *quarantine_part_count += 1;
    let file_name = format!("part-{quarantine_part_count:06}.parquet");
    builder.write_quarantine_records(&file_name, quarantine_records)?;
    quarantine_artifacts.push(format!("quarantine/{file_name}"));
    Ok(())
}

fn write_output_batch(
    builder: &mut PackageBuilder,
    program: &ValidationProgram,
    output: RecordBatch,
    output_position: Option<SourcePosition>,
    state: &mut OutputWriteState<'_>,
) -> Result<()> {
    let output = normalize_record_batch(output, program)?;
    *state.output_schema = Some(schema_artifact(output.schema().as_ref()));
    state.profile.output_rows += output.num_rows() as u64;
    state.profile.output_bytes += output.get_array_memory_size() as u64;
    state.profile.output_batches += 1;

    let segment_id = SegmentId::new(format!("seg-{:06}", state.segments.len() + 1))?;
    let segment = builder.write_segment(segment_id.clone(), &[output])?;
    state.lineage.output_segments.push(segment_id);
    state.segment_positions.push(EngineSegmentPosition {
        segment_id: segment.segment_id.clone(),
        output_position,
    });
    state.segments.push(segment);
    Ok(())
}

fn package_execution_span(context: &ExecutionTraceContext) -> Span {
    info_span!(
        "cdf_engine.package_execution",
        run_id = context.run_id.as_str(),
        resource_id = context.resource_id.as_str(),
        package_id = context.package_id.as_str()
    )
}

fn partition_execution_span(context: &ExecutionTraceContext, partition_id: &str) -> Span {
    info_span!(
        "cdf_engine.partition_execution",
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
                CdfError::data(format!(
                    "projected field {name:?} is not present in resource batch"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    batch.project(&indices).map_err(CdfError::from)
}

fn apply_contract_exec(
    batch: RecordBatch,
    program: &ValidationProgram,
    context: &ContractEvaluationContext,
) -> Result<ContractExecOutput> {
    let evaluation = evaluate_record_batch(program, context, &batch)?;
    let summary = evaluation.summary;
    let quarantine_records = quarantine_records_from_candidates(evaluation.quarantine_candidates)?;
    let accepted = if summary.accepted_rows == summary.input_rows {
        batch
    } else {
        filter_record_batch(&batch, &evaluation.accepted_rows).map_err(CdfError::from)?
    };
    Ok(ContractExecOutput {
        accepted,
        quarantine_records,
        summary,
    })
}

fn quarantine_records_from_candidates(
    candidates: Vec<QuarantineCandidate>,
) -> Result<Vec<QuarantineRecord>> {
    candidates
        .into_iter()
        .map(|candidate| {
            Ok(QuarantineRecord {
                source_row_ordinal: u64::try_from(candidate.source_row_ordinal)
                    .map_err(|error| CdfError::internal(error.to_string()))?,
                rule_id: candidate.rule_id,
                error_code: candidate.error_code,
                source_position: candidate.source_position,
                observed_value_redacted: quarantine_observed_value(
                    candidate.observed_value_redacted,
                ),
            })
        })
        .collect()
}

fn quarantine_observed_value(
    value: cdf_contract::RedactedObservedValue,
) -> QuarantineObservedValue {
    match value {
        cdf_contract::RedactedObservedValue::Null => QuarantineObservedValue::Null,
        cdf_contract::RedactedObservedValue::Preserved { value } => {
            QuarantineObservedValue::Preserved { value }
        }
        cdf_contract::RedactedObservedValue::Hashed { algorithm, value } => {
            QuarantineObservedValue::Hashed { algorithm, value }
        }
        cdf_contract::RedactedObservedValue::Omitted => QuarantineObservedValue::Omitted,
        cdf_contract::RedactedObservedValue::Masked { value } => {
            QuarantineObservedValue::Masked { value }
        }
    }
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
                semantic: semantic(field.as_ref()).map(ToOwned::to_owned),
                metadata: schema_field_metadata(field.as_ref()),
            })
            .collect(),
    }
}

fn schema_field_metadata(field: &arrow_schema::Field) -> BTreeMap<String, String> {
    if !field.metadata().contains_key(PHYSICAL_TYPE_METADATA_KEY) {
        return BTreeMap::new();
    }

    [SOURCE_NAME_METADATA_KEY, PHYSICAL_TYPE_METADATA_KEY]
        .into_iter()
        .filter_map(|key| {
            field
                .metadata()
                .get(key)
                .map(|value| (key.to_owned(), value.clone()))
        })
        .collect()
}

fn current_observed_at_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CdfError::internal(format!("system clock before Unix epoch: {error}")))?;
    i64::try_from(duration.as_millis()).map_err(|_| {
        CdfError::internal("system time milliseconds do not fit in i64 evaluation context")
    })
}
