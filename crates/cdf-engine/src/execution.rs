use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::filter::filter_record_batch;
use arrow_select::take::take_record_batch;
use cdf_contract::{
    ContractEvaluationContext, QuarantineCandidate, RESIDUAL_ENCODING_METADATA_KEY,
    RedactionDecision, ResidualCandidateVerdict, ResidualFieldRef, ResidualFieldWithRedaction,
    VARIANT_COLUMN_NAME, ValidationProgram, VectorValidationEvaluator, VerdictSummary,
    encode_package_dedup_keys, encode_residual_json_v1, encode_residual_json_v1_redacted,
    evaluate_package_order_dedup, materialize_schema_coercion, package_dedup_rule,
    reject_untrusted_schema_coercion_metadata, schema_coercion_plan_from_trusted_json,
};
use cdf_expression::{
    BoundBooleanExpression, BoundExpressionTransform, apply_bound_expression_transforms,
    apply_bound_filters, apply_expression_transforms, bind_expression_transforms,
    bind_filter_expressions, expression_transform_output_schema,
};
use cdf_kernel::{
    Batch, CdfError, ExecutionExtent, PHYSICAL_TYPE_METADATA_KEY, PLAN_PHYSICAL_SCHEMA_HASH_KEY,
    PLAN_SCHEMA_OBSERVATION_BINDING_KEY, PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionAttestation,
    PartitionPlan, PhysicalObservationRepresentation, PreContractObservedValue,
    PreContractQuarantineFact, PreContractResidualCandidate, ProcessedObservationOutcome,
    ProcessedObservationPosition, ResourceStream, Result, RunId, RunPhase, RunPhaseContext,
    RunPhaseMetric, RunPhaseStatus, SOURCE_NAME_METADATA_KEY, ScopeKey, SourcePosition,
    StratifiedHashBoundedIdentity, StratifiedHashCandidate, StratifiedHashIdentityStrength,
    TerminalSchemaObservationQuarantine, WatermarkClaim, WriteDisposition,
    aggregate_resource_closed_output_position, aggregate_resource_output_position,
    merge_terminal_position_evidence, semantic, source_name,
};
use cdf_memory::{
    ConsumerKey, DEFAULT_PROCESS_BUDGET_BYTES, DeterministicMemoryCoordinator, MemoryClass,
    MemoryCoordinator, MemoryLease, ReservationRequest, reserve,
};
use cdf_package::PackageBuilder;
use cdf_package_contract::{
    PackageStatus, QuarantineObservedValue, QuarantineRecord, SegmentEntry,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{Instrument, Span, info_span};

use crate::{
    CompiledSchemaAdmissionOutcome, CompiledSchemaAdmissionPlan, CompiledSchemaQuarantineEvidence,
    CompiledStreamAdmissionEvidence, EffectiveSchemaObservationCoercion,
    EffectiveSchemaPlanEvidence, EngineDrainEpoch, EngineExecutionEvidence, EngineExecutionOptions,
    EnginePackageDraft, EnginePlan, EnginePreviewLimits, EnginePreviewOutput, EngineRunOutput,
    EngineRunOutputWithSegmentPositions, EngineSegmentPosition, ExecutionProfile,
    LineageInputObservation, LineageSummary, PhysicalObservationEvidence,
    SchemaQuarantineObservationEvidence, StandaloneExecutionHost,
    StreamAdmissionObservationEvidence,
    output_schema::canonicalize_effective_output_schema,
    planning::{scan_expression_schema, validate_program},
    variant_capture::{
        ContractEvolutionArtifact, ResidualDecisionArtifact, ResidualRuntimeVerdict,
        ResidualTypedProjection, contract_evolution_artifact_metadata, normalize_batch,
    },
};

pub type PackagePreFinalizeHook<'a> =
    dyn Fn(&PackageBuilder, EnginePackageDraft<'_>) -> Result<()> + 'a;
pub type DurableSegmentHook<'a> =
    dyn FnMut(&SegmentEntry, DurableSegmentPayload) -> Result<()> + 'a;
pub type StreamingFinalizeHook<'a> = dyn FnMut() -> Result<()> + 'a;
const SOURCE_ROW_FIELD: &str = "_cdf_internal_source_row";

/// Mutable epoch-scoped authorities that may advance only through one canonical drain closure.
pub struct DrainEpochExecution<'a> {
    durable_segment: Option<&'a mut DurableSegmentHook<'a>>,
    stream_finalize: Option<&'a mut StreamingFinalizeHook<'a>>,
    controller: &'a mut cdf_runtime::DrainEpochController,
}

impl<'a> DrainEpochExecution<'a> {
    pub fn new(controller: &'a mut cdf_runtime::DrainEpochController) -> Self {
        Self {
            durable_segment: None,
            stream_finalize: None,
            controller,
        }
    }

    pub fn with_streaming_hooks(
        mut self,
        durable_segment: &'a mut DurableSegmentHook<'a>,
        stream_finalize: &'a mut StreamingFinalizeHook<'a>,
    ) -> Self {
        self.durable_segment = Some(durable_segment);
        self.stream_finalize = Some(stream_finalize);
        self
    }
}

fn standalone_execution_options() -> Result<EngineExecutionOptions> {
    let (_, services) = StandaloneExecutionHost::default_services(DEFAULT_PROCESS_BUDGET_BYTES)?;
    Ok(EngineExecutionOptions::default().with_execution_services(services))
}

/// An owned, accounted handoff from durable package publication to staged ingress.
///
/// The record batches and their existing memory leases move together so a destination queue does
/// not reserve the same Arrow allocations a second time. Dropping the payload releases ownership.
pub struct DurableSegmentPayload {
    durable_local_file: PathBuf,
    batches: Vec<RecordBatch>,
    memory_leases: Vec<MemoryLease>,
}

impl DurableSegmentPayload {
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_parts(self) -> (PathBuf, Vec<RecordBatch>, Vec<MemoryLease>) {
        (self.durable_local_file, self.batches, self.memory_leases)
    }
}

struct DurableSegmentObserver<'a> {
    hook: Option<&'a mut DurableSegmentHook<'a>>,
}

impl DurableSegmentObserver<'_> {
    fn observe(&mut self, segment: &SegmentEntry, payload: DurableSegmentPayload) -> Result<()> {
        match self.hook.as_deref_mut() {
            Some(hook) => hook(segment, payload),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PhaseAggregate {
    duration_ns: u64,
    input_bytes: u64,
    output_bytes: u64,
    operations: u64,
}

struct PhaseMeasurements {
    enabled: bool,
    values: BTreeMap<(RunPhase, Option<RunPhaseContext>), PhaseAggregate>,
}

impl PhaseMeasurements {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            values: BTreeMap::new(),
        }
    }

    fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }

    fn add(&mut self, phase: RunPhase, duration_ns: u64, input_bytes: u64, output_bytes: u64) {
        self.add_operations(phase, duration_ns, input_bytes, output_bytes, 1);
    }

    fn add_operations(
        &mut self,
        phase: RunPhase,
        duration_ns: u64,
        input_bytes: u64,
        output_bytes: u64,
        operations: u64,
    ) {
        self.add_operations_with_context(
            phase,
            None,
            duration_ns,
            input_bytes,
            output_bytes,
            operations,
        );
    }

    fn add_operations_with_context(
        &mut self,
        phase: RunPhase,
        context: Option<RunPhaseContext>,
        duration_ns: u64,
        input_bytes: u64,
        output_bytes: u64,
        operations: u64,
    ) {
        if !self.enabled {
            return;
        }
        let metric = self.values.entry((phase, context)).or_default();
        metric.duration_ns = metric.duration_ns.saturating_add(duration_ns);
        metric.input_bytes = metric.input_bytes.saturating_add(input_bytes);
        metric.output_bytes = metric.output_bytes.saturating_add(output_bytes);
        metric.operations = metric.operations.saturating_add(operations);
    }

    fn into_metrics(self) -> Vec<RunPhaseMetric> {
        self.values
            .into_iter()
            .map(|((phase, context), metric)| RunPhaseMetric {
                phase,
                context,
                status: RunPhaseStatus::Completed,
                duration_ns: metric.duration_ns,
                input_bytes: metric.input_bytes,
                output_bytes: metric.output_bytes,
                operations: metric.operations,
            })
            .collect()
    }
}

pub fn normalize_record_batch(
    batch: RecordBatch,
    program: &ValidationProgram,
) -> Result<RecordBatch> {
    if !program.transforms.iter().any(|transform| {
        matches!(
            transform,
            cdf_contract::TransformDescription::Derive { .. }
                | cdf_contract::TransformDescription::Filter { .. }
        )
    }) {
        return normalize_batch(batch, program);
    }
    let compiled = program.compiled_expression_plan.as_ref().ok_or_else(|| {
        CdfError::contract("validation program has no recorded compiled expression plan")
    })?;
    compiled.validate_program_binding(program)?;
    normalize_batch(
        apply_expression_transforms(batch, &program.transforms, &compiled.transforms)?,
        program,
    )
}

fn normalize_record_batch_after_expressions(
    batch: RecordBatch,
    program: &ValidationProgram,
) -> Result<RecordBatch> {
    normalize_batch(batch, program)
}

pub async fn preview_resource<R>(
    plan: &EnginePlan,
    resource: &R,
    limits: EnginePreviewLimits,
) -> Result<EnginePreviewOutput>
where
    R: ResourceStream + ?Sized,
{
    plan.validate_execution_extent_for_execution()?;
    plan.validate_compiled_expression_plan()?;
    plan.validate_partition_schedule()?;
    plan.validate_compiled_source_resource(resource)?;
    validate_program(&plan.validation_program)?;
    cdf_kernel::validate_scan_partition_observation_identities(&plan.scan)?;
    cdf_kernel::validate_compiled_scan_intents(&plan.scan)?;
    let schema_authority = plan.schema_authority();
    if schema_authority.version != 1 {
        return Err(CdfError::data(format!(
            "unsupported engine schema-authority version {}",
            schema_authority.version
        )));
    }
    EnginePreviewLimits::new(limits.max_rows, limits.max_bytes, limits.max_batches)?;
    let effective_schema_evidence = validate_effective_schema_plan(plan, resource)?;
    crate::planning::validate_plan_schema_authority(resource, plan)?;
    let resource_schema = resource.schema();
    let runtime_output_schema = plan.output_arrow_schema()?;
    cdf_package_contract::validate_logical_output_schema(runtime_output_schema.as_ref())?;
    let expression_schema = scan_expression_schema(
        resource_schema.as_ref(),
        plan.explain
            .projection_pushed
            .then_some(plan.scan.request.projection.as_deref())
            .flatten(),
    )?;
    let bound_residuals =
        bind_filter_expressions(&plan.compiled_expression_plan.residuals, &expression_schema)?;
    let bound_transforms = bind_expression_transforms(
        &plan.validation_program.transforms,
        &plan.compiled_expression_plan.transforms,
        &expression_schema,
    )?;
    let bound_tracked_transforms = bind_expression_transforms(
        &plan.validation_program.transforms,
        &plan.compiled_expression_plan.transforms,
        &source_row_tracking_schema(&expression_schema)?,
    )?;
    let contract_schema =
        expression_transform_output_schema(&plan.validation_program.transforms, &expression_schema);
    let pre_contract_may_filter = !bound_residuals.is_empty()
        || plan.validation_program.transforms.iter().any(|transform| {
            matches!(transform, cdf_contract::TransformDescription::Filter { .. })
        });
    let evaluation_context = ContractEvaluationContext::observed_at(current_observed_at_ms()?);
    let mut contract_evaluator =
        VectorValidationEvaluator::new_bound(&plan.validation_program, Arc::new(contract_schema))?;
    let mut remaining_rows = limits.max_rows;
    let mut remaining_bytes = limits.max_bytes;
    let mut remaining_batches = limits.max_batches;
    let mut first_partition_id = None;
    let mut first_batch_id = None;
    let mut payload_opened_partition_count = 0_u64;
    let mut attested_partition_count = 0_u64;
    let mut inspected_partition_count = 0_u64;
    let mut inspected_batch_count = 0_u64;
    let mut row_count = 0_u64;
    let mut byte_count = 0_u64;
    let mut output_byte_count = 0_u64;
    let mut quarantined_row_count = 0_u64;
    let mut residual_row_count = 0_u64;
    let mut terminal_quarantines = BTreeSet::new();
    let mut observation_attestations = BTreeMap::<String, PartitionAttestation>::new();
    let mut schema_admission_cache =
        BTreeMap::<cdf_kernel::SchemaHash, cdf_contract::SchemaCoercionPlan>::new();
    let mut fields = runtime_output_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let mut truncated = false;
    let mut terminal = Vec::new();
    let mut payload_candidates = Vec::new();
    let mut location_counts = BTreeMap::<String, usize>::new();
    for partition in &plan.scan.partitions {
        let disposition = effective_schema_evidence
            .map(|evidence| partition_schema_disposition(partition, evidence))
            .transpose()?;
        match disposition {
            Some(PartitionSchemaDisposition::Quarantined(quarantine)) => {
                terminal.push((partition.clone(), quarantine));
            }
            disposition => {
                let expected = disposition.and_then(|item| match item {
                    PartitionSchemaDisposition::Admitted(evidence) => Some(evidence),
                    PartitionSchemaDisposition::Quarantined(_)
                    | PartitionSchemaDisposition::Unobserved => None,
                });
                let (location, bounded_identity) = preview_partition_identity(partition)?;
                *location_counts.entry(location.clone()).or_default() += 1;
                payload_candidates.push(PreviewPayloadCandidate {
                    partition: partition.clone(),
                    expected,
                    location,
                    bounded_identity,
                });
            }
        }
    }
    for candidate in &mut payload_candidates {
        if location_counts
            .get(&candidate.location)
            .copied()
            .unwrap_or(0)
            > 1
        {
            candidate.location = serde_json::to_string(&(
                candidate.location.as_str(),
                candidate.partition.partition_id.as_str(),
            ))
            .map_err(|error| CdfError::internal(error.to_string()))?;
        }
    }

    for (partition, quarantine) in terminal {
        let attestation = required_preview_attestation(
            resource,
            &partition,
            quarantine.observation_id(),
            quarantine.physical_schema_hash(),
            &mut observation_attestations,
        )
        .await?;
        let _ = attestation;
        attested_partition_count += 1;
        terminal_quarantines.insert(quarantine.observation_id().to_owned());
    }

    let selection_plan = if payload_candidates.is_empty() {
        None
    } else {
        Some(cdf_kernel::plan_stratified_hash_v1(
            &plan.scan.request.resource_id,
            limits.max_batches,
            &payload_candidates
                .iter()
                .map(|candidate| {
                    StratifiedHashCandidate::from_bounded_identity(
                        candidate.location.clone(),
                        &candidate.bounded_identity,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        )?)
    };
    let selected_locations = selection_plan
        .as_ref()
        .map(|selection| {
            selection
                .selected
                .iter()
                .map(|selected| selected.canonical_location.clone())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let selected_count = u64::try_from(selected_locations.len())
        .map_err(|error| CdfError::internal(error.to_string()))?;
    let base_quota = limits.max_batches.checked_div(selected_count).unwrap_or(0);
    let quota_remainder = limits.max_batches.checked_rem(selected_count).unwrap_or(0);
    let mut selected_evidence = Vec::new();
    let mut selected_but_uninspected = Vec::new();
    let mut partially_inspected = Vec::new();
    let mut payload_uninspected = Vec::new();

    if let Some(selection) = &selection_plan {
        for (selected_index, selected) in selection.selected.iter().enumerate() {
            let candidate = payload_candidates
                .iter()
                .find(|candidate| candidate.location == selected.canonical_location)
                .ok_or_else(|| CdfError::internal("preview selector lost a partition"))?;
            let quota = base_quota + u64::from((selected_index as u64) < quota_remainder);
            let mut admitted = 0_u64;
            let mut complete = false;
            if remaining_rows > 0 && remaining_bytes > 0 && remaining_batches > 0 {
                let mut opening = resource.open(candidate.partition.clone());
                let mut stream = match (&mut opening).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        return match opening.terminate_and_join().await {
                            Ok(()) => Err(error),
                            Err(cleanup) => Err(with_cleanup_failure(
                                error,
                                "preview source opening termination",
                                cleanup,
                            )),
                        };
                    }
                };
                payload_opened_partition_count += 1;
                let inspection = async {
                    while admitted < quota
                        && remaining_rows > 0
                        && remaining_bytes > 0
                        && remaining_batches > 0
                    {
                        let Some(batch) = stream.next().await else {
                            complete = true;
                            break;
                        };
                        let mut batch = batch?;
                        validate_batch_partition_ownership(
                            &batch,
                            &plan.scan.request.resource_id,
                            &candidate.partition,
                        )?;
                        let record_batch = batch.record_batch().cloned().ok_or_else(|| {
                            CdfError::data(
                                "resource preview requires in-memory Arrow record batches",
                            )
                        })?;
                        let decoded_bytes = u64::try_from(record_batch.get_array_memory_size())
                            .map_err(|error| CdfError::internal(error.to_string()))?;
                        if decoded_bytes > remaining_bytes {
                            truncated = true;
                            break;
                        }
                        let reconciled = materialize_batch_schema_evidence(
                            &batch,
                            &record_batch,
                            BatchSchemaAdmissionContext {
                                planned_observation_id: cdf_kernel::partition_schema_observation_id(
                                    &candidate.partition,
                                ),
                                expected: candidate.expected.as_ref(),
                                expected_physical_observation: preobserved_physical_observation(
                                    plan.effective_schema_evidence.as_ref(),
                                    candidate.expected.as_ref(),
                                )?,
                                effective_schema: &expression_schema,
                            },
                            &plan.compiled_schema_admission,
                            &mut schema_admission_cache,
                        )?;
                        let reconciled = match reconciled {
                            BatchSchemaDisposition::Admitted(reconciled) => reconciled,
                            BatchSchemaDisposition::Quarantined { quarantine, .. } => {
                                terminal_quarantines.insert(quarantine.observation_id().to_owned());
                                admitted = admitted.saturating_add(1);
                                inspected_batch_count = inspected_batch_count.saturating_add(1);
                                remaining_batches = remaining_batches.saturating_sub(1);
                                remaining_bytes = remaining_bytes.saturating_sub(decoded_bytes);
                                byte_count = byte_count.saturating_add(decoded_bytes);
                                quarantined_row_count =
                                    quarantined_row_count.saturating_add(batch.header.row_count);
                                complete = true;
                                break;
                            }
                        };
                        if let Some(coercion_plan) = reconciled.coercion_plan.as_ref()
                            && plan.compiled_schema_admission.captures_unknown_fields()?
                        {
                            let candidates = stream_admission_residual_candidates(
                                &record_batch,
                                coercion_plan,
                                batch.header.residual_candidates(),
                                matches!(
                                    reconciled.extra_field_evidence,
                                    ExtraFieldEvidence::AlreadyCaptured
                                ) && batch.header.materialized_residuals_complete(),
                                0,
                            )?;
                            batch.header.extend_residual_candidates(candidates);
                        }
                        let record_batch = reconciled.record_batch;
                        let pre_contract_quarantined_rows =
                            pre_contract_quarantine_summary(&batch.header.pre_contract_quarantine)
                                .quarantined_rows;
                        let residual_candidates = batch.header.take_residual_candidates();
                        let cdc_operation_field = batch
                            .header
                            .cdc
                            .as_ref()
                            .map(|metadata| metadata.operation_field.clone());
                        let track_source_rows =
                            pre_contract_may_filter || !residual_candidates.is_empty();
                        let mut no_row_limit = None;
                        let executed =
                            execute_batch(&record_batch, &bound_residuals, track_source_rows)?;
                        let ExecutedBatch {
                            batch: output,
                            source_rows,
                            limit_truncated: _,
                        } = apply_pre_contract_expressions(
                            executed.batch,
                            if track_source_rows {
                                &bound_tracked_transforms
                            } else {
                                &bound_transforms
                            },
                            &mut no_row_limit,
                            track_source_rows,
                        )?;
                        let mut discard_quarantine = |_record: QuarantineRecord| Ok(());
                        let contract = apply_contract_exec(
                            output,
                            &mut contract_evaluator,
                            &mut discard_quarantine,
                            residual_candidates,
                            &ResidualBatchContext {
                                evaluation: &evaluation_context,
                                source_rows: source_rows.as_deref(),
                                cdc_operation_field: cdc_operation_field.as_deref(),
                                batch_id: &batch.header.batch_id,
                                observation_id: candidate
                                    .expected
                                    .as_ref()
                                    .map(|evidence| evidence.observation_id.as_str()),
                            },
                            TransformKernelMode::Fused,
                            None,
                        )?;
                        let projected =
                            apply_projection(&contract.accepted, plan.final_projection.as_deref())?;
                        let normalized = append_residual_variant(
                            projected,
                            &plan.validation_program,
                            contract.variant_values,
                        )?;
                        let normalized = normalize_record_batch_after_expressions(
                            normalized,
                            &plan.validation_program,
                        )?;
                        let normalized = if effective_schema_evidence.is_some() {
                            canonicalize_effective_output_schema(normalized)?
                        } else {
                            normalized
                        };
                        let normalized = conform_to_compiled_output_schema(
                            normalized,
                            runtime_output_schema.as_ref(),
                        )?;
                        let render_rows = normalized
                            .num_rows()
                            .min(usize::try_from(remaining_rows).unwrap_or(usize::MAX));
                        let rendered = compact_record_batch_prefix(&normalized, render_rows)?;
                        let rendered_bytes = u64::try_from(rendered.get_array_memory_size())
                            .map_err(|error| CdfError::internal(error.to_string()))?;
                        if first_partition_id.is_none() {
                            first_partition_id = Some(batch.header.partition_id.to_string());
                            first_batch_id = Some(batch.header.batch_id.to_string());
                        }
                        fields = rendered
                            .schema()
                            .fields()
                            .iter()
                            .map(|field| field.name().clone())
                            .collect();
                        admitted += 1;
                        inspected_batch_count += 1;
                        remaining_batches -= 1;
                        remaining_bytes -= decoded_bytes;
                        remaining_rows -= u64::try_from(render_rows)
                            .map_err(|error| CdfError::internal(error.to_string()))?;
                        row_count += u64::try_from(render_rows)
                            .map_err(|error| CdfError::internal(error.to_string()))?;
                        byte_count += decoded_bytes;
                        output_byte_count += rendered_bytes;
                        quarantined_row_count +=
                            contract.summary.quarantined_rows + pre_contract_quarantined_rows;
                        if let Some(variant) = rendered.column_by_name(VARIANT_COLUMN_NAME) {
                            residual_row_count +=
                                u64::try_from(variant.len() - variant.null_count())
                                    .map_err(|error| CdfError::internal(error.to_string()))?;
                        }
                    }
                    Ok::<(), CdfError>(())
                }
                .await;
                let cleanup = stream.terminate_and_join().await;
                match (inspection, cleanup) {
                    (Ok(()), Ok(())) => {}
                    (Err(error), Ok(())) => return Err(error),
                    (Ok(()), Err(cleanup)) => return Err(cleanup),
                    (Err(error), Err(cleanup)) => {
                        return Err(with_cleanup_failure(
                            error,
                            "preview source termination",
                            cleanup,
                        ));
                    }
                }
            }
            if admitted == 0 && !complete {
                selected_but_uninspected.push(candidate.partition.partition_id.to_string());
                payload_uninspected.push(candidate.partition.partition_id.to_string());
            } else {
                inspected_partition_count += 1;
                if !complete {
                    partially_inspected.push(candidate.partition.partition_id.to_string());
                }
            }
            selected_evidence.push(crate::EnginePreviewSelectedPartition {
                partition_id: candidate.partition.partition_id.to_string(),
                canonical_location: selected.canonical_location.clone(),
                score_sha256: selected.score_sha256.clone(),
                bounded_identity_sha256: selected.bounded_identity_sha256.clone(),
                batch_quota: quota,
                inspected_batches: admitted,
            });
        }
    }

    for candidate in &payload_candidates {
        if selected_locations.contains(&candidate.location) {
            continue;
        }
        payload_uninspected.push(candidate.partition.partition_id.to_string());
    }
    payload_uninspected.sort();
    payload_uninspected.dedup();
    selected_but_uninspected.sort();
    partially_inspected.sort();
    let uninspected_ids = payload_uninspected.iter().cloned().collect::<BTreeSet<_>>();
    for candidate in &payload_candidates {
        if !uninspected_ids.contains(candidate.partition.partition_id.as_str()) {
            continue;
        }
        if optional_preview_attestation(
            resource,
            &candidate.partition,
            candidate.expected.as_ref(),
            &mut observation_attestations,
        )
        .await?
        {
            attested_partition_count += 1;
        }
    }

    let planned_partition_count = u64::try_from(plan.scan.partitions.len())
        .map_err(|error| CdfError::internal(error.to_string()))?;
    let payload_eligible_partition_count = u64::try_from(payload_candidates.len())
        .map_err(|error| CdfError::internal(error.to_string()))?;
    let partially_inspected_partition_count = u64::try_from(partially_inspected.len())
        .map_err(|error| CdfError::internal(error.to_string()))?;
    let payload_uninspected_partition_count = u64::try_from(payload_uninspected.len())
        .map_err(|error| CdfError::internal(error.to_string()))?;
    if remaining_rows == 0
        || remaining_bytes == 0
        || remaining_batches == 0
        || partially_inspected_partition_count > 0
        || payload_uninspected_partition_count > 0
    {
        truncated = true;
    }
    Ok(EnginePreviewOutput {
        resource_id: plan.scan.request.resource_id.clone(),
        first_partition_id,
        first_batch_id,
        planned_partition_count,
        payload_eligible_partition_count,
        selected_partition_count: selected_count,
        payload_opened_partition_count,
        attested_partition_count,
        inspected_partition_count,
        partially_inspected_partition_count,
        payload_uninspected_partition_count,
        inspected_batch_count,
        row_count,
        byte_count,
        output_byte_count,
        quarantined_row_count,
        residual_row_count,
        terminal_quarantine_count: u64::try_from(terminal_quarantines.len())
            .map_err(|error| CdfError::internal(error.to_string()))?,
        fields,
        limits,
        selection: crate::EnginePreviewSelectionEvidence {
            policy: crate::PREVIEW_POLICY_BALANCED_STRATIFIED_V1.to_owned(),
            selector: cdf_kernel::STRATIFIED_HASH_SELECTOR_V1.to_owned(),
            candidate_count: payload_eligible_partition_count,
            selected: selected_evidence,
            selected_but_uninspected_partition_ids: selected_but_uninspected,
            partially_inspected_partition_ids: partially_inspected,
            payload_uninspected_partition_ids: payload_uninspected,
        },
        truncated,
    })
}

#[derive(Clone, Debug)]
struct PreviewPayloadCandidate {
    partition: cdf_kernel::PartitionPlan,
    expected: Option<EffectiveSchemaObservationCoercion>,
    location: String,
    bounded_identity: StratifiedHashBoundedIdentity,
}

fn preview_partition_identity(
    partition: &cdf_kernel::PartitionPlan,
) -> Result<(String, StratifiedHashBoundedIdentity)> {
    let planned_file = partition.planned_file()?;
    let location = planned_file.map_or_else(
        || {
            partition
                .metadata
                .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
                .cloned()
                .unwrap_or_else(|| partition.partition_id.to_string())
        },
        |file| file.path.clone(),
    );
    let size_bytes = planned_file.map(|file| file.size_bytes);
    let modified_at_ms = partition
        .metadata
        .get("modified_ms")
        .map(|value| {
            value.parse::<i64>().map_err(|error| {
                CdfError::data(format!(
                    "preview partition {} has invalid modification-time identity {value:?}: {error}",
                    partition.partition_id
                ))
            })
        })
        .transpose()?;
    let (value, strength) = if let Some(file) = planned_file {
        if let Some(sha256) = &file.sha256 {
            (
                Some(sha256.clone()),
                StratifiedHashIdentityStrength::StrongChecksum,
            )
        } else if let Some(etag) = &file.etag {
            let strength = if etag.trim_start().starts_with("W/") {
                StratifiedHashIdentityStrength::WeakEtag
            } else if etag
                .trim_matches('"')
                .rsplit_once('-')
                .is_some_and(|(_, part_count)| {
                    !part_count.is_empty() && part_count.bytes().all(|byte| byte.is_ascii_digit())
                })
            {
                StratifiedHashIdentityStrength::MultipartEtag
            } else {
                StratifiedHashIdentityStrength::StableEtag
            };
            (Some(etag.clone()), strength)
        } else if file.object_version.is_some() || file.source_generation.is_some() {
            (
                Some(cdf_kernel::partition_source_identity_binding(partition)?),
                StratifiedHashIdentityStrength::BoundedObservation,
            )
        } else {
            (None, StratifiedHashIdentityStrength::Unavailable)
        }
    } else {
        (
            Some(cdf_kernel::partition_source_identity_binding(partition)?),
            StratifiedHashIdentityStrength::BoundedObservation,
        )
    };
    let identity = StratifiedHashBoundedIdentity {
        size_bytes,
        modified_at_ms,
        value,
        strength,
    };
    Ok((location, identity))
}

pub fn preview_partition_selector_candidate(
    partition: &cdf_kernel::PartitionPlan,
) -> Result<StratifiedHashCandidate> {
    let (location, identity) = preview_partition_identity(partition)?;
    StratifiedHashCandidate::from_bounded_identity(location, &identity)
}

async fn required_preview_attestation<R>(
    resource: &R,
    partition: &cdf_kernel::PartitionPlan,
    observation_id: &str,
    expected_schema_hash: &cdf_kernel::SchemaHash,
    cache: &mut BTreeMap<String, PartitionAttestation>,
) -> Result<PartitionAttestation>
where
    R: ResourceStream + ?Sized,
{
    let attestation = match cache.get(observation_id) {
        Some(attestation) => attestation.clone(),
        None => {
            let attestation = resource
                .attest_partition(partition.clone())
                .await?
                .ok_or_else(|| {
                CdfError::data(format!(
                    "terminal schema observation {observation_id:?} has no execution-time attestation"
                ))
            })?;
            cache.insert(observation_id.to_owned(), attestation.clone());
            attestation
        }
    };
    if attestation.physical_schema_hash() != Some(expected_schema_hash) {
        return Err(CdfError::data(format!(
            "terminal schema observation {observation_id:?} changed physical schema between planning and preview; expected {expected_schema_hash}, attested {:?}; re-plan before retrying",
            attestation.physical_schema_hash()
        )));
    }
    Ok(attestation)
}

async fn optional_preview_attestation<R>(
    resource: &R,
    partition: &cdf_kernel::PartitionPlan,
    expected: Option<&EffectiveSchemaObservationCoercion>,
    cache: &mut BTreeMap<String, PartitionAttestation>,
) -> Result<bool>
where
    R: ResourceStream + ?Sized,
{
    let observation_id = partition.metadata.get(PLAN_SCHEMA_OBSERVATION_ID_KEY);
    let cached = observation_id.and_then(|id| cache.get(id)).cloned();
    let attestation = match cached {
        Some(attestation) => Some(attestation),
        None => resource.attest_partition(partition.clone()).await?,
    };
    let Some(attestation) = attestation else {
        return Ok(false);
    };
    if let Some(observation_id) = observation_id {
        cache.insert(observation_id.clone(), attestation.clone());
    }
    if let Some(expected) = expected
        && attestation.physical_schema_hash() != Some(&expected.physical_schema_hash)
    {
        return Err(CdfError::data(format!(
            "schema observation {:?} changed physical schema between planning and preview; expected {}, attested {:?}; re-plan before retrying",
            expected.observation_id,
            expected.physical_schema_hash,
            attestation.physical_schema_hash()
        )));
    }
    Ok(true)
}

struct AdmittedBatchSchema {
    record_batch: RecordBatch,
    coercion_plan: Option<cdf_contract::SchemaCoercionPlan>,
    observation_id: Option<String>,
    physical_observation: Option<PhysicalObservationEvidence>,
    extra_field_evidence: ExtraFieldEvidence,
}

#[derive(Clone, Copy)]
enum ExtraFieldEvidence {
    AlreadyCaptured,
    CaptureFromPhysicalBatch,
}

enum BatchSchemaDisposition {
    Admitted(AdmittedBatchSchema),
    Quarantined {
        quarantine: TerminalSchemaObservationQuarantine,
        physical_observation: PhysicalObservationEvidence,
    },
}

struct BatchSchemaAdmissionContext<'a> {
    planned_observation_id: &'a str,
    expected: Option<&'a EffectiveSchemaObservationCoercion>,
    expected_physical_observation: Option<&'a PhysicalObservationEvidence>,
    effective_schema: &'a Schema,
}

fn validate_batch_partition_ownership(
    batch: &cdf_kernel::Batch,
    resource_id: &cdf_kernel::ResourceId,
    partition: &cdf_kernel::PartitionPlan,
) -> Result<()> {
    if &batch.header.resource_id != resource_id {
        return Err(CdfError::data(format!(
            "planned resource `{}` received batch `{}` labeled for resource `{}`",
            resource_id.as_str(),
            batch.header.batch_id.as_str(),
            batch.header.resource_id.as_str()
        )));
    }
    if batch.header.partition_id != partition.partition_id {
        return Err(CdfError::data(format!(
            "planned partition `{}` received batch `{}` labeled for partition `{}`",
            partition.partition_id.as_str(),
            batch.header.batch_id.as_str(),
            batch.header.partition_id.as_str()
        )));
    }
    Ok(())
}

fn preobserved_physical_observation<'a>(
    evidence: Option<&'a EffectiveSchemaPlanEvidence>,
    expected: Option<&EffectiveSchemaObservationCoercion>,
) -> Result<Option<&'a PhysicalObservationEvidence>> {
    let Some(expected) = expected else {
        return Ok(None);
    };
    evidence
        .and_then(|evidence| {
            evidence
                .physical_observation_catalog
                .get(expected.physical_schema_hash.as_str())
        })
        .map(Some)
        .ok_or_else(|| {
            CdfError::internal(format!(
                "preobserved schema evidence {:?} has no physical-observation catalog entry",
                expected.observation_id
            ))
        })
}

fn materialize_batch_schema_evidence(
    batch: &cdf_kernel::Batch,
    record_batch: &RecordBatch,
    context: BatchSchemaAdmissionContext<'_>,
    admission: &CompiledSchemaAdmissionPlan,
    admission_cache: &mut BTreeMap<cdf_kernel::SchemaHash, cdf_contract::SchemaCoercionPlan>,
) -> Result<BatchSchemaDisposition> {
    let BatchSchemaAdmissionContext {
        planned_observation_id,
        expected,
        expected_physical_observation,
        effective_schema,
    } = context;
    if expected.is_some() != expected_physical_observation.is_some() {
        return Err(CdfError::internal(
            "preobserved coercion and physical-observation catalog entry must be supplied together",
        ));
    }
    if planned_observation_id.is_empty() {
        return Err(CdfError::internal(
            "planned schema observation identity cannot be empty",
        ));
    }
    let stream_observation_id = planned_observation_id.to_owned();
    if let Some(expected) = expected
        && batch.header.observed_schema_hash != expected.physical_schema_hash
    {
        return Err(CdfError::data(format!(
            "schema observation {:?} produced physical schema hash {} but verified discovery evidence requires {}",
            expected.observation_id,
            batch.header.observed_schema_hash,
            expected.physical_schema_hash
        )));
    }
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
    match (expected, &batch_coercion) {
        (Some(expected), Some(batch_coercion)) => {
            if batch.header.observation_representation
                != PhysicalObservationRepresentation::MaterializedOutput
            {
                return Err(CdfError::data(
                    "a batch carrying source-materialized coercion evidence must identify its payload as materialized output",
                ));
            }
            let physical_schema = batch.header.materialized_physical_schema()?;
            admission.validate_materialized(&physical_schema, &expected.coercion_plan)?;
            validate_effective_batch_schema(record_batch.schema().as_ref(), effective_schema)?;
            if batch_coercion != &expected.coercion_plan {
                return Err(CdfError::data(format!(
                    "schema observation {:?} produced coercion evidence that does not match the typed engine plan",
                    expected.observation_id
                )));
            }
            Ok(BatchSchemaDisposition::Admitted(AdmittedBatchSchema {
                record_batch: record_batch.clone(),
                coercion_plan: Some(batch_coercion.clone()),
                observation_id: Some(expected.observation_id.clone()),
                physical_observation: expected_physical_observation.cloned(),
                extra_field_evidence: ExtraFieldEvidence::AlreadyCaptured,
            }))
        }
        (Some(expected), None) => {
            if batch.header.observation_representation
                == PhysicalObservationRepresentation::MaterializedOutput
            {
                let physical_schema = batch.header.materialized_physical_schema()?;
                admission.validate_materialized(&physical_schema, &expected.coercion_plan)?;
                validate_materialized_effective_batch_schema(
                    record_batch.schema().as_ref(),
                    effective_schema,
                    batch.header.residual_candidates(),
                )?;
                return Ok(BatchSchemaDisposition::Admitted(AdmittedBatchSchema {
                    record_batch: record_batch.clone(),
                    coercion_plan: Some(expected.coercion_plan.clone()),
                    observation_id: Some(expected.observation_id.clone()),
                    physical_observation: expected_physical_observation.cloned(),
                    extra_field_evidence: ExtraFieldEvidence::AlreadyCaptured,
                }));
            }
            let observed_schema = record_batch.schema();
            admission.validate_materialized(observed_schema.as_ref(), &expected.coercion_plan)?;
            let materialized = materialize_schema_coercion(
                record_batch,
                effective_schema,
                &expected.coercion_plan,
            )?;
            validate_effective_batch_schema(materialized.schema().as_ref(), effective_schema)?;
            Ok(BatchSchemaDisposition::Admitted(AdmittedBatchSchema {
                record_batch: materialized,
                coercion_plan: Some(expected.coercion_plan.clone()),
                observation_id: Some(expected.observation_id.clone()),
                physical_observation: expected_physical_observation.cloned(),
                extra_field_evidence: ExtraFieldEvidence::CaptureFromPhysicalBatch,
            }))
        }
        (None, supplied) => {
            if let Some(supplied) = supplied {
                if batch.header.observation_representation
                    != PhysicalObservationRepresentation::MaterializedOutput
                {
                    return Err(CdfError::data(
                        "a batch carrying source-materialized coercion evidence must identify its payload as materialized output",
                    ));
                }
                let physical_schema = batch.header.materialized_physical_schema()?;
                admission.validate_materialized(&physical_schema, supplied)?;
                validate_materialized_effective_batch_schema(
                    record_batch.schema().as_ref(),
                    effective_schema,
                    batch.header.residual_candidates(),
                )?;
                return Ok(BatchSchemaDisposition::Admitted(AdmittedBatchSchema {
                    record_batch: record_batch.clone(),
                    coercion_plan: Some(supplied.clone()),
                    observation_id: Some(stream_observation_id),
                    physical_observation: Some(materialized_output_evidence(
                        record_batch,
                        &physical_schema,
                        batch.header.observed_schema_hash.clone(),
                        effective_schema,
                    )?),
                    extra_field_evidence: ExtraFieldEvidence::AlreadyCaptured,
                }));
            }
            if batch.header.observation_representation
                == PhysicalObservationRepresentation::MaterializedOutput
            {
                let physical_schema = batch.header.materialized_physical_schema()?;
                validate_materialized_effective_batch_schema(
                    record_batch.schema().as_ref(),
                    effective_schema,
                    batch.header.residual_candidates(),
                )?;
                let physical_schema_hash =
                    cdf_kernel::canonical_arrow_schema_hash(&physical_schema)?;
                let compiled = admission.instantiate(&physical_schema, &physical_schema_hash)?;
                return Ok(BatchSchemaDisposition::Admitted(AdmittedBatchSchema {
                    record_batch: record_batch.clone(),
                    coercion_plan: Some(compiled),
                    observation_id: Some(stream_observation_id),
                    physical_observation: Some(materialized_output_evidence(
                        record_batch,
                        &physical_schema,
                        batch.header.observed_schema_hash.clone(),
                        effective_schema,
                    )?),
                    extra_field_evidence: ExtraFieldEvidence::AlreadyCaptured,
                }));
            }
            if cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref())?
                == batch.header.observed_schema_hash
                && validate_effective_batch_schema(record_batch.schema().as_ref(), effective_schema)
                    .is_ok()
            {
                let compiled = admission.instantiate(
                    record_batch.schema().as_ref(),
                    &batch.header.observed_schema_hash,
                )?;
                return Ok(BatchSchemaDisposition::Admitted(AdmittedBatchSchema {
                    record_batch: record_batch.clone(),
                    coercion_plan: Some(compiled),
                    observation_id: Some(stream_observation_id),
                    physical_observation: Some(PhysicalObservationEvidence::arrow_schema(
                        record_batch.schema().as_ref(),
                    )?),
                    extra_field_evidence: ExtraFieldEvidence::CaptureFromPhysicalBatch,
                }));
            }
            let compiled = match admission_cache.get(&batch.header.observed_schema_hash) {
                Some(plan) => plan.clone(),
                None => {
                    let outcome = admission.instantiate_or_quarantine(
                        &stream_observation_id,
                        record_batch.schema().as_ref(),
                        &batch.header.observed_schema_hash,
                    )?;
                    let plan = match outcome {
                        CompiledSchemaAdmissionOutcome::Admitted(plan) => plan,
                        CompiledSchemaAdmissionOutcome::Quarantined(quarantine) => {
                            return Ok(BatchSchemaDisposition::Quarantined {
                                quarantine: *quarantine,
                                physical_observation: PhysicalObservationEvidence::arrow_schema(
                                    record_batch.schema().as_ref(),
                                )?,
                            });
                        }
                    };
                    admission_cache.insert(batch.header.observed_schema_hash.clone(), plan.clone());
                    plan
                }
            };
            let materialized =
                materialize_schema_coercion(record_batch, effective_schema, &compiled)?;
            validate_effective_batch_schema(materialized.schema().as_ref(), effective_schema)?;
            Ok(BatchSchemaDisposition::Admitted(AdmittedBatchSchema {
                record_batch: materialized,
                coercion_plan: Some(compiled),
                observation_id: Some(stream_observation_id),
                physical_observation: Some(PhysicalObservationEvidence::arrow_schema(
                    record_batch.schema().as_ref(),
                )?),
                extra_field_evidence: ExtraFieldEvidence::CaptureFromPhysicalBatch,
            }))
        }
    }
}

fn stream_admission_residual_candidates(
    physical_batch: &RecordBatch,
    coercion_plan: &cdf_contract::SchemaCoercionPlan,
    existing: &[PreContractResidualCandidate],
    materialized_residuals_complete: bool,
    source_row_ordinal: u64,
) -> Result<Vec<PreContractResidualCandidate>> {
    let mut candidates = Vec::new();
    for decision in &coercion_plan.fields {
        if decision.decision != cdf_contract::FieldCoercionDecision::Extra {
            continue;
        }
        let observed_name = decision.observed_name.as_deref().ok_or_else(|| {
            CdfError::data(format!(
                "extra field {:?} has no observed field name",
                decision.source_name
            ))
        })?;
        let mut covered_rows = BTreeSet::new();
        for candidate in existing.iter().filter(|candidate| {
            candidate.expected_field().is_none()
                && candidate.source_path().first().map(String::as_str)
                    == Some(decision.source_name.as_str())
        }) {
            if candidate.batch_row_ordinal() >= physical_batch.num_rows() {
                return Err(CdfError::data(format!(
                    "extra field {:?} has residual evidence outside the materialized batch",
                    decision.source_name
                )));
            }
            if !covered_rows.insert(candidate.batch_row_ordinal()) {
                return Err(CdfError::data(format!(
                    "extra field {:?} has duplicate residual evidence for batch row {}",
                    decision.source_name,
                    candidate.batch_row_ordinal()
                )));
            }
        }
        if materialized_residuals_complete {
            continue;
        }
        if covered_rows.len() == physical_batch.num_rows() {
            continue;
        }
        let field_index = physical_batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == observed_name)
            .ok_or_else(|| {
                CdfError::data(format!(
                    "extra field {:?} is absent from its physical batch",
                    decision.source_name
                ))
            })?;
        let field = physical_batch.schema().field(field_index).clone();
        let values = Arc::clone(physical_batch.column(field_index));
        for row in 0..physical_batch.num_rows() {
            if covered_rows.contains(&row) {
                continue;
            }
            candidates.push(PreContractResidualCandidate::new(
                source_row_ordinal.saturating_add(row as u64),
                row,
                vec![decision.source_name.clone()],
                field.clone(),
                None,
                Arc::clone(&values),
                row,
            )?);
        }
    }
    Ok(candidates)
}

fn materialized_nullable_residual_fields(output: &Schema, effective: &Schema) -> Vec<String> {
    output
        .fields()
        .iter()
        .zip(effective.fields())
        .filter(|(output, effective)| output.is_nullable() && !effective.is_nullable())
        .map(|(_, effective)| {
            source_name(effective.as_ref())
                .unwrap_or_else(|| effective.name())
                .to_owned()
        })
        .collect()
}

fn materialized_output_evidence(
    batch: &RecordBatch,
    physical_schema: &Schema,
    decoder_observation_hash: cdf_kernel::SchemaHash,
    effective: &Schema,
) -> Result<PhysicalObservationEvidence> {
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema)?;
    if physical_hash != decoder_observation_hash {
        return Err(CdfError::data(format!(
            "materialized physical schema hash {physical_hash} does not match batch observation hash {decoder_observation_hash}"
        )));
    }
    let nullable_residual_fields =
        materialized_nullable_residual_fields(batch.schema().as_ref(), effective);
    let nullable_residual_sources = nullable_residual_fields
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let fields = effective
        .fields()
        .iter()
        .map(|field| {
            let source = source_name(field.as_ref()).unwrap_or_else(|| field.name());
            if nullable_residual_sources.contains(source) {
                Arc::new(field.as_ref().clone().with_nullable(true))
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    let output_schema = Schema::new_with_metadata(fields, effective.metadata().clone());
    PhysicalObservationEvidence::materialized_output(
        physical_schema,
        &output_schema,
        nullable_residual_fields,
    )
}

fn compact_record_batch_prefix(batch: &RecordBatch, rows: usize) -> Result<RecordBatch> {
    let rows = rows.min(batch.num_rows()).min(u32::MAX as usize);
    if rows == batch.num_rows() {
        return Ok(batch.clone());
    }
    let rows = u32::try_from(rows).map_err(|error| CdfError::internal(error.to_string()))?;
    let indices = UInt32Array::from_iter_values(0..rows);
    take_record_batch(batch, &indices).map_err(CdfError::from)
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
    variant_values: Vec<Option<String>>,
    summary: VerdictSummary,
    residual_decisions: Vec<ResidualDecisionArtifact>,
    memory_lease: Option<MemoryLease>,
}

struct QuarantinePartAccumulator<'a> {
    builder: &'a PackageBuilder,
    part_count: &'a mut usize,
    records: Vec<QuarantineRecord>,
    memory_lease: Option<MemoryLease>,
    retained_bytes: u64,
}

impl<'a> QuarantinePartAccumulator<'a> {
    const ROWS: usize = 8 * 1024;

    fn new(
        builder: &'a PackageBuilder,
        part_count: &'a mut usize,
        memory_lease: Option<MemoryLease>,
    ) -> Self {
        Self {
            builder,
            part_count,
            records: Vec::new(),
            memory_lease,
            retained_bytes: 0,
        }
    }

    fn push(&mut self, record: QuarantineRecord) -> Result<()> {
        let record_bytes = quarantine_record_working_set_bytes(&record)?;
        if self.records.len() == Self::ROWS {
            self.flush()?;
        }
        if let Some(lease) = self.memory_lease.clone() {
            let projected = self
                .retained_bytes
                .checked_add(record_bytes)
                .and_then(|bytes| bytes.checked_mul(3))
                .ok_or_else(|| CdfError::data("quarantine evidence working set overflowed"))?;
            if let Err(error) = lease.reconcile(projected.max(1)) {
                if self.records.is_empty() {
                    return Err(error);
                }
                self.flush()?;
                lease.reconcile(record_bytes.checked_mul(3).ok_or_else(|| {
                    CdfError::data("quarantine evidence working set overflowed")
                })?)?;
            }
        }
        self.retained_bytes = self
            .retained_bytes
            .checked_add(record_bytes)
            .ok_or_else(|| CdfError::data("quarantine evidence byte count overflowed"))?;
        self.records.push(record);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        *self.part_count = self
            .part_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("quarantine part count overflowed"))?;
        let file_name = format!("part-{:06}.parquet", self.part_count);
        let mut writer = self.builder.begin_quarantine_records(file_name)?;
        writer.write_records(&self.records)?;
        writer.finish()?;
        self.records = Vec::new();
        self.retained_bytes = 0;
        if let Some(lease) = &self.memory_lease {
            lease.reconcile(1)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.flush()
    }
}

fn quarantine_record_working_set_bytes(record: &QuarantineRecord) -> Result<u64> {
    let source_position_bytes = record
        .source_position
        .as_ref()
        .map(serde_json::to_vec)
        .transpose()
        .map_err(|error| CdfError::internal(error.to_string()))?
        .map_or(0_usize, |bytes| bytes.len());
    let observed_bytes = match &record.observed_value_redacted {
        QuarantineObservedValue::Null | QuarantineObservedValue::Omitted => 0,
        QuarantineObservedValue::Preserved { value }
        | QuarantineObservedValue::Masked { value } => value.len(),
        QuarantineObservedValue::Hashed { algorithm, value } => {
            algorithm.len().saturating_add(value.len())
        }
    };
    let bytes = std::mem::size_of::<QuarantineRecord>()
        .saturating_add(record.rule_id.len())
        .saturating_add(record.error_code.len())
        .saturating_add(source_position_bytes.saturating_mul(2))
        .saturating_add(observed_bytes)
        .saturating_add(256);
    u64::try_from(bytes).map_err(|_| CdfError::data("quarantine evidence bytes exceed u64"))
}

fn reserve_quarantine_evidence(
    memory: Option<&Arc<dyn MemoryCoordinator>>,
) -> Result<Option<MemoryLease>> {
    let Some(memory) = memory else {
        return Ok(None);
    };
    let request = ReservationRequest::new(
        ConsumerKey::new("quarantine-evidence", MemoryClass::Transform)?,
        1,
    )?;
    memory.try_reserve(&request)?.map(Some).ok_or_else(|| {
        CdfError::data(
            "quarantine evidence could not reserve one byte of managed headroom; reduce jobs or raise the memory budget",
        )
    })
}

fn program_may_quarantine(program: &ValidationProgram) -> bool {
    matches!(
        program.disposition_for(cdf_contract::RuleOutcome::Violation, "quarantine-admission"),
        cdf_contract::RuleDisposition::Quarantine { .. }
    ) && program
        .row_rules
        .iter()
        .any(|rule| !rule.is_dedup_expression())
}

enum ResidualDecisionAccumulator {
    Memory(Vec<ResidualDecisionArtifact>),
    Spill(crate::residual_spill::ResidualDecisionRuns),
}

enum ResidualDecisionOutput {
    Memory(std::vec::IntoIter<ResidualDecisionArtifact>),
    Spill(crate::residual_spill::ResidualDecisionReader),
}

impl ResidualDecisionAccumulator {
    fn push(&mut self, decisions: Vec<ResidualDecisionArtifact>) -> Result<()> {
        match self {
            Self::Memory(all) => all.extend(decisions),
            Self::Spill(runs) => runs.push(decisions)?,
        }
        Ok(())
    }

    fn finish(self) -> Result<Option<ResidualDecisionOutput>> {
        match self {
            Self::Memory(mut decisions) => {
                decisions.sort_by(crate::variant_capture::residual_decision_cmp);
                Ok((!decisions.is_empty())
                    .then(|| ResidualDecisionOutput::Memory(decisions.into_iter())))
            }
            Self::Spill(runs) => Ok(runs.finish()?.map(ResidualDecisionOutput::Spill)),
        }
    }
}

impl ResidualDecisionOutput {
    fn next(&mut self) -> Result<Option<ResidualDecisionArtifact>> {
        match self {
            Self::Memory(decisions) => Ok(decisions.next()),
            Self::Spill(decisions) => decisions.next(),
        }
    }
}

struct ResidualBatchContext<'a> {
    evaluation: &'a ContractEvaluationContext,
    source_rows: Option<&'a [usize]>,
    cdc_operation_field: Option<&'a str>,
    batch_id: &'a cdf_kernel::BatchId,
    observation_id: Option<&'a str>,
}

struct ExecutedBatch {
    batch: RecordBatch,
    source_rows: Option<Vec<usize>>,
    limit_truncated: bool,
}

struct PendingDedupBatch {
    partition_ordinal: u32,
    output: RecordBatch,
    output_position: Option<SourcePosition>,
    _memory_lease: Option<MemoryLease>,
}

struct PreparedOutputBatch {
    output: RecordBatch,
    variant_values: Vec<Option<String>>,
    output_position: Option<SourcePosition>,
    memory_lease: Option<MemoryLease>,
}

struct PreparedKernelOutput {
    output: RecordBatch,
    memory_lease: Option<MemoryLease>,
}

struct OutputWriteState<'a> {
    profile: &'a mut ExecutionProfile,
    lineage: &'a mut LineageSummary,
    segments: &'a mut Vec<SegmentEntry>,
    segment_positions: &'a mut Vec<EngineSegmentPosition>,
    output_schema: &'a mut Option<SchemaArtifact>,
    expected_schema: &'a Schema,
    phase_measurements: &'a mut PhaseMeasurements,
    memory: Option<&'a Arc<dyn MemoryCoordinator>>,
    statistics: Option<StatisticsProfileState<'a>>,
}

struct StatisticsProfileState<'a> {
    statistics_memory: &'a Arc<dyn MemoryCoordinator>,
    statistics_memory_lease: &'a mut Option<MemoryLease>,
    statistics_profile: &'a mut cdf_package::StatisticsProfileWriter,
    statistics_profile_schema_hash: &'a str,
    statistics_segment_ordinal: &'a mut u64,
}

struct SegmentOutputSink<'a, 'b> {
    builder: &'a PackageBuilder,
    queue: &'a mut SegmentEncodeQueue,
    durable: &'a mut DurableSegmentObserver<'b>,
}

struct SegmentEncodeWork {
    ordinal: u64,
    segment_id: cdf_kernel::SegmentId,
    package_row_ord_start: u64,
    partition_ordinal: u32,
    output_position: Option<SourcePosition>,
    batches: Vec<RecordBatch>,
    normalization_output_bytes: u64,
    _transform_memory_leases: Vec<MemoryLease>,
    _scratch_memory_lease: Option<MemoryLease>,
}

struct SegmentEncodeCompletion {
    work: SegmentEncodeWork,
    encoded: Result<cdf_package::EncodedPackageSegment>,
}

fn statistics_profile_state<'a>(
    statistics_memory: &'a Arc<dyn MemoryCoordinator>,
    statistics_memory_lease: &'a mut Option<MemoryLease>,
    statistics_profile: &'a mut Option<cdf_package::StatisticsProfileWriter>,
    statistics_profile_schema_hash: &'a str,
    statistics_segment_ordinal: &'a mut u64,
) -> Option<StatisticsProfileState<'a>> {
    statistics_profile
        .as_mut()
        .map(|statistics_profile| StatisticsProfileState {
            statistics_memory,
            statistics_memory_lease,
            statistics_profile,
            statistics_profile_schema_hash,
            statistics_segment_ordinal,
        })
}

enum SegmentEncodeMode {
    Inline,
    Parallel {
        services: cdf_runtime::ExecutionServices,
        scope: Option<Box<dyn cdf_runtime::ExecutionTaskScope>>,
        sender: mpsc::Sender<SegmentEncodeCompletion>,
        receiver: mpsc::Receiver<SegmentEncodeCompletion>,
        maximum_in_flight: usize,
        in_flight: usize,
    },
}

struct SegmentEncodeQueue {
    encoder: cdf_package::PackageSegmentEncoder,
    measure: bool,
    next_submission: u64,
    next_registration: u64,
    next_package_row_ord: u64,
    pending: BTreeMap<u64, SegmentEncodeCompletion>,
    mode: SegmentEncodeMode,
}

impl SegmentEncodeQueue {
    fn abort_and_cleanup(&mut self) -> Result<()> {
        let mut join_error = None;
        let mut cleanup_error = None;
        if let SegmentEncodeMode::Parallel {
            services,
            scope,
            receiver,
            in_flight,
            ..
        } = &mut self.mode
        {
            if let Some(scope) = scope.take() {
                scope.cancel();
                if let Err(error) = services.run_io(scope.join()) {
                    join_error = Some(error);
                }
            }
            while let Ok(completion) = receiver.try_recv() {
                *in_flight = in_flight.saturating_sub(1);
                self.pending.insert(completion.work.ordinal, completion);
            }
        }
        for completion in std::mem::take(&mut self.pending).into_values() {
            if let Ok(encoded) = completion.encoded
                && let Err(error) = encoded.rollback_unpublished()
                && cleanup_error.is_none()
            {
                cleanup_error = Some(error);
            }
        }
        match (join_error, cleanup_error) {
            (Some(join), Some(cleanup)) => Err(CdfError::internal(format!(
                "{join}; unpublished segment cleanup also failed: {cleanup}"
            ))),
            (Some(error), None) | (None, Some(error)) => Err(error),
            (None, None) => Ok(()),
        }
    }

    fn new(
        builder: &PackageBuilder,
        services: Option<&cdf_runtime::ExecutionServices>,
        measure: bool,
        scope_id: &str,
        maximum_segment_bytes: u64,
    ) -> Result<Self> {
        let mode = match services {
            Some(services) if services.capabilities().logical_cpu_slots > 1 => {
                let cpu_parallelism =
                    u64::from(services.capabilities().logical_cpu_slots.saturating_sub(1));
                let conservative_segment_working_set = maximum_segment_bytes
                    .max(1)
                    .checked_mul(3)
                    .ok_or_else(|| CdfError::data("segment encode working set overflow"))?;
                let memory_parallelism = services
                    .memory()
                    .snapshot()
                    .budget_bytes
                    .checked_div(conservative_segment_working_set)
                    .unwrap_or(0)
                    .max(1);
                let maximum_in_flight = usize::try_from(cpu_parallelism.min(memory_parallelism))
                    .map_err(|_| CdfError::data("segment encode parallelism exceeds usize"))?;
                let (sender, receiver) = mpsc::channel();
                SegmentEncodeMode::Parallel {
                    services: services.clone(),
                    scope: Some(services.open_scope(scope_id)?),
                    sender,
                    receiver,
                    maximum_in_flight,
                    in_flight: 0,
                }
            }
            _ => SegmentEncodeMode::Inline,
        };
        Ok(Self {
            encoder: builder.segment_encoder(),
            measure,
            next_submission: 0,
            next_registration: 0,
            next_package_row_ord: 0,
            pending: BTreeMap::new(),
            mode,
        })
    }

    fn submit(
        &mut self,
        mut work: SegmentEncodeWork,
        builder: &PackageBuilder,
        state: &mut OutputWriteState<'_>,
        durable_segment: &mut DurableSegmentObserver<'_>,
    ) -> Result<()> {
        work.ordinal = self.next_submission;
        self.next_submission = self
            .next_submission
            .checked_add(1)
            .ok_or_else(|| CdfError::data("segment encode ordinal overflow"))?;
        loop {
            let full = match &self.mode {
                SegmentEncodeMode::Parallel {
                    maximum_in_flight,
                    in_flight,
                    ..
                } => *in_flight >= *maximum_in_flight,
                SegmentEncodeMode::Inline => false,
            };
            if !full {
                break;
            }
            self.receive_one(true)?;
            self.register_ready(builder, state, durable_segment)?;
        }
        match &mut self.mode {
            SegmentEncodeMode::Inline => {
                let encoded = self.encoder.encode(
                    work.segment_id.clone(),
                    work.package_row_ord_start,
                    &work.batches,
                    self.measure,
                );
                self.pending
                    .insert(work.ordinal, SegmentEncodeCompletion { work, encoded });
            }
            SegmentEncodeMode::Parallel {
                scope,
                sender,
                in_flight,
                ..
            } => {
                let encoder = self.encoder.clone();
                let measure = self.measure;
                let sender = sender.clone();
                scope
                    .as_mut()
                    .ok_or_else(|| CdfError::internal("segment encode scope is absent"))?
                    .spawn_cpu(
                        cdf_runtime::CpuTaskSpec {
                            task_kind: "package.segment_encode".to_owned(),
                            cpu_slot_cost: 1,
                            native_internal_parallelism: 1,
                        },
                        Box::new(move || {
                            let encoded =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    encoder.encode(
                                        work.segment_id.clone(),
                                        work.package_row_ord_start,
                                        &work.batches,
                                        measure,
                                    )
                                }))
                                .unwrap_or_else(|_| {
                                    Err(CdfError::internal("segment encode worker panicked"))
                                });
                            if let Err(send_error) =
                                sender.send(SegmentEncodeCompletion { work, encoded })
                            {
                                let completion = send_error.0;
                                if let Ok(encoded) = completion.encoded {
                                    encoded.rollback_unpublished()?;
                                }
                                return Err(CdfError::internal("segment encode frontier stopped"));
                            }
                            Ok(())
                        }),
                    )?;
                *in_flight = in_flight.saturating_add(1);
            }
        }
        self.receive_one(false)?;
        self.register_ready(builder, state, durable_segment)
    }

    fn receive_one(&mut self, block: bool) -> Result<()> {
        let SegmentEncodeMode::Parallel {
            receiver,
            in_flight,
            ..
        } = &mut self.mode
        else {
            return Ok(());
        };
        let completion = if block {
            Some(
                receiver
                    .recv()
                    .map_err(|_| CdfError::internal("segment encode workers stopped"))?,
            )
        } else {
            match receiver.try_recv() {
                Ok(completion) => Some(completion),
                Err(mpsc::TryRecvError::Empty) => None,
                Err(mpsc::TryRecvError::Disconnected) if *in_flight == 0 => None,
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(CdfError::internal("segment encode workers stopped"));
                }
            }
        };
        if let Some(completion) = completion {
            *in_flight = in_flight.saturating_sub(1);
            if self
                .pending
                .insert(completion.work.ordinal, completion)
                .is_some()
            {
                return Err(CdfError::internal(
                    "segment encode completion ordinal repeated",
                ));
            }
        }
        Ok(())
    }

    fn relieve_memory_pressure(
        &mut self,
        builder: &PackageBuilder,
        state: &mut OutputWriteState<'_>,
        durable_segment: &mut DurableSegmentObserver<'_>,
    ) -> Result<bool> {
        self.register_ready(builder, state, durable_segment)?;
        let in_flight = match &self.mode {
            SegmentEncodeMode::Inline => 0,
            SegmentEncodeMode::Parallel { in_flight, .. } => *in_flight,
        };
        if in_flight == 0 {
            return Ok(false);
        }
        self.receive_one(true)?;
        self.register_ready(builder, state, durable_segment)?;
        Ok(true)
    }

    fn register_ready(
        &mut self,
        builder: &PackageBuilder,
        state: &mut OutputWriteState<'_>,
        durable_segment: &mut DurableSegmentObserver<'_>,
    ) -> Result<()> {
        while let Some(completion) = self.pending.remove(&self.next_registration) {
            let write = completion.encoded?;
            let write = builder.register_encoded_segment(write)?;
            let SegmentEncodeWork {
                ordinal: _,
                segment_id,
                package_row_ord_start: _,
                partition_ordinal,
                output_position,
                batches,
                normalization_output_bytes,
                mut _transform_memory_leases,
                _scratch_memory_lease,
            } = completion.work;
            state.phase_measurements.add(
                RunPhase::SegmentEncode,
                write.encode_duration_ns,
                normalization_output_bytes,
                write.segment.byte_count,
            );
            state.phase_measurements.add(
                RunPhase::PersistHash,
                write.persist_hash_duration_ns,
                write.segment.byte_count,
                write.segment.byte_count,
            );
            let segment = write.segment;
            let durable_local_file = builder.package_dir().join(&segment.path);
            if let Some(lease) = _scratch_memory_lease {
                _transform_memory_leases.push(lease);
            }
            durable_segment.observe(
                &segment,
                DurableSegmentPayload {
                    durable_local_file,
                    batches,
                    memory_leases: _transform_memory_leases,
                },
            )?;
            state.profile.output_rows = state.profile.output_rows.saturating_add(segment.row_count);
            state.profile.output_bytes = state
                .profile
                .output_bytes
                .saturating_add(segment.byte_count);
            state.profile.output_batches = state.profile.output_batches.saturating_add(1);
            state.lineage.output_segments.push(segment_id);
            state.segment_positions.push(EngineSegmentPosition {
                segment_id: segment.segment_id.clone(),
                partition_ordinal,
                output_position,
            });
            state.segments.push(segment);
            self.next_registration = self
                .next_registration
                .checked_add(1)
                .ok_or_else(|| CdfError::data("segment registration ordinal overflow"))?;
        }
        Ok(())
    }

    fn finish(
        &mut self,
        builder: &PackageBuilder,
        state: &mut OutputWriteState<'_>,
        durable_segment: &mut DurableSegmentObserver<'_>,
    ) -> Result<()> {
        let mut first_error = None;
        loop {
            let in_flight = match &self.mode {
                SegmentEncodeMode::Inline => 0,
                SegmentEncodeMode::Parallel { in_flight, .. } => *in_flight,
            };
            if in_flight == 0 {
                break;
            }
            if let Err(error) = self
                .receive_one(true)
                .and_then(|()| self.register_ready(builder, state, durable_segment))
            {
                first_error = Some(error);
                if let SegmentEncodeMode::Parallel { scope, .. } = &self.mode
                    && let Some(scope) = scope
                {
                    scope.cancel();
                }
                break;
            }
        }
        if let SegmentEncodeMode::Parallel {
            services, scope, ..
        } = &mut self.mode
        {
            let report = services.run_io(
                scope
                    .take()
                    .ok_or_else(|| CdfError::internal("segment encode scope already joined"))?
                    .join(),
            )?;
            if first_error.is_none() && (report.failed > 0 || report.cancelled > 0) {
                first_error = Some(CdfError::internal(
                    "segment encode scope did not complete cleanly",
                ));
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        self.register_ready(builder, state, durable_segment)?;
        if self.next_registration != self.next_submission || !self.pending.is_empty() {
            return Err(CdfError::internal(
                "segment encode frontier ended before every canonical segment registered",
            ));
        }
        Ok(())
    }
}

impl Drop for SegmentEncodeQueue {
    fn drop(&mut self) {
        let _ = self.abort_and_cleanup();
    }
}

const DEDUP_PROVENANCE_SHARD_ROWS: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DedupSummaryV2 {
    version: u16,
    rule_id: String,
    keys: Vec<String>,
    keep: cdf_contract::DedupKeepProgram,
    input_rows: u64,
    output_rows: u64,
    duplicate_key_count: u64,
    dropped_row_count: u64,
    provenance_format: String,
    provenance_version: u16,
    provenance_shard_row_target: u64,
    shard_count: u64,
    shards: Vec<DedupProvenanceShard>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DedupProvenanceShard {
    path: String,
    row_count: u64,
    byte_count: u64,
    sha256: String,
}

struct DedupProvenanceSink {
    rows: Vec<(u64, u64)>,
    shards: Vec<DedupProvenanceShard>,
}

impl DedupProvenanceSink {
    fn new() -> Self {
        Self {
            rows: Vec::with_capacity(DEDUP_PROVENANCE_SHARD_ROWS),
            shards: Vec::new(),
        }
    }

    fn push(&mut self, builder: &PackageBuilder, dropped: u64, kept: u64) -> Result<()> {
        if self
            .rows
            .last()
            .is_some_and(|previous| previous.0 >= dropped)
        {
            return Err(CdfError::internal(
                "dedup provenance is not in strict dropped-row order",
            ));
        }
        self.rows.push((dropped, kept));
        if self.rows.len() == DEDUP_PROVENANCE_SHARD_ROWS {
            self.flush(builder)?;
        }
        Ok(())
    }

    fn finish(mut self, builder: &PackageBuilder) -> Result<Vec<DedupProvenanceShard>> {
        self.flush(builder)?;
        Ok(self.shards)
    }

    fn flush(&mut self, builder: &PackageBuilder) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let file_name = format!("part-{:06}.parquet", self.shards.len() + 1);
        let row_count = self.rows.len() as u64;
        let entry = builder.write_dedup_provenance_shard(&file_name, &self.rows)?;
        self.shards.push(DedupProvenanceShard {
            path: entry.path,
            row_count,
            byte_count: entry.byte_count,
            sha256: entry.sha256,
        });
        self.rows.clear();
        Ok(())
    }
}

fn record_observation_schema_coercion(
    evidence: &mut BTreeMap<String, StreamAdmissionObservationEvidence>,
    physical_observation_catalog: &mut BTreeMap<String, PhysicalObservationEvidence>,
    observation_id: &str,
    physical_observation: PhysicalObservationEvidence,
    coercion_plan: cdf_contract::SchemaCoercionPlan,
) -> Result<()> {
    let physical_observation_hash = physical_observation.identity_hash()?;
    match physical_observation_catalog.entry(physical_observation_hash.to_string()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(physical_observation);
        }
        std::collections::btree_map::Entry::Occupied(entry)
            if entry.get() != &physical_observation =>
        {
            return Err(CdfError::data(
                "physical-observation identity collision carries conflicting evidence",
            ));
        }
        std::collections::btree_map::Entry::Occupied(_) => {}
    }
    let artifact = StreamAdmissionObservationEvidence::new(
        observation_id,
        physical_observation_hash,
        coercion_plan,
        crate::StreamAdmissionCompletion::Partial {
            attempted_position: None,
            observed_rows: 0,
            partition_binding: String::new(),
        },
    )?;
    if let Some(existing) = evidence.get(observation_id) {
        if existing.observation_id != artifact.observation_id
            || existing.physical_observation_hash != artifact.physical_observation_hash
            || existing.coercion_plan != artifact.coercion_plan
        {
            return Err(CdfError::data(format!(
                "schema observation {:?} produced inconsistent coercion/physical evidence: first={existing:?}, next={artifact:?}",
                observation_id,
            )));
        }
    } else {
        evidence.insert(observation_id.to_owned(), artifact);
    }
    Ok(())
}

fn record_schema_quarantine(
    quarantines: &mut Vec<TerminalSchemaObservationQuarantine>,
    physical_observations: &mut BTreeMap<String, PhysicalObservationEvidence>,
    quarantine: TerminalSchemaObservationQuarantine,
    physical_observation: PhysicalObservationEvidence,
) -> Result<()> {
    let observation_id = quarantine.observation_id().to_owned();
    if let Some(existing) = quarantines
        .iter()
        .find(|existing| existing.observation_id() == observation_id)
    {
        if existing != &quarantine
            || physical_observations.get(&observation_id) != Some(&physical_observation)
        {
            return Err(CdfError::data(format!(
                "repeated schema quarantine {observation_id:?} produced conflicting verdict, position, or physical evidence"
            )));
        }
        return Ok(());
    }
    if physical_observations
        .insert(observation_id.clone(), physical_observation)
        .is_some()
    {
        return Err(CdfError::internal(format!(
            "physical evidence for schema quarantine {observation_id:?} existed before its verdict"
        )));
    }
    quarantines.push(quarantine);
    Ok(())
}

enum PartitionSchemaDisposition {
    Admitted(EffectiveSchemaObservationCoercion),
    Quarantined(TerminalSchemaObservationQuarantine),
    Unobserved,
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
    Ok(execute_to_package_inner(
        None,
        plan,
        resource,
        package_dir,
        None,
        None,
        None,
        None,
        standalone_execution_options()?,
    )
    .await?
    .output)
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
    Ok(execute_to_package_inner(
        Some(&trace_context),
        plan,
        resource,
        package_dir,
        None,
        None,
        None,
        None,
        standalone_execution_options()?,
    )
    .instrument(package_execution_span(&trace_context))
    .await?
    .output)
}

pub async fn execute_to_package_with_segment_positions<R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
) -> Result<EngineRunOutputWithSegmentPositions>
where
    R: ResourceStream + ?Sized,
{
    execute_to_package_inner(
        None,
        plan,
        resource,
        package_dir,
        None,
        None,
        None,
        None,
        standalone_execution_options()?,
    )
    .await
}

pub async fn execute_to_package_with_segment_positions_and_pre_finalize<R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    options: EngineExecutionOptions,
) -> Result<EngineRunOutputWithSegmentPositions>
where
    R: ResourceStream + ?Sized,
{
    execute_to_package_inner(
        None,
        plan,
        resource,
        package_dir,
        Some(pre_finalize),
        None,
        None,
        None,
        options,
    )
    .await
}

pub async fn execute_to_package_with_streaming_hooks<'a, R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    durable_segment: &'a mut DurableSegmentHook<'a>,
    stream_finalize: &'a mut StreamingFinalizeHook<'a>,
    options: EngineExecutionOptions,
) -> Result<EngineRunOutputWithSegmentPositions>
where
    R: ResourceStream + ?Sized,
{
    execute_to_package_inner(
        None,
        plan,
        resource,
        package_dir,
        Some(pre_finalize),
        Some(durable_segment),
        Some(stream_finalize),
        None,
        options,
    )
    .await
}

pub async fn execute_drain_epoch_with_hooks<'a, R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    epoch: DrainEpochExecution<'a>,
    options: EngineExecutionOptions,
) -> Result<EngineRunOutputWithSegmentPositions>
where
    R: ResourceStream + ?Sized,
{
    Box::pin(execute_to_package_inner(
        None,
        plan,
        resource,
        package_dir,
        Some(pre_finalize),
        epoch.durable_segment,
        epoch.stream_finalize,
        Some(epoch.controller),
        options,
    ))
    .await
}

#[derive(Clone)]
struct PartitionOpenEvidence {
    duration_ns: u64,
    retry_pre_attestation: Option<PartitionAttestation>,
}

#[derive(Clone)]
struct PartitionOpenMetadata {
    ordinal: usize,
    partition: cdf_kernel::PartitionPlan,
    evidence: PartitionOpenEvidence,
}

type OpenedPartition = (
    PartitionOpenMetadata,
    Option<cdf_kernel::OpenedPartitionStream>,
);

#[derive(Clone)]
struct PartitionOpenRuntime {
    services: Option<cdf_runtime::ExecutionServices>,
    cancellation: cdf_runtime::RunCancellation,
    retry_journal: cdf_runtime::SourceRetryJournal,
}

fn open_partition<'a, R>(
    resource: &'a R,
    ordinal: usize,
    partition: cdf_kernel::PartitionPlan,
    terminal_quarantine: bool,
    plan_id: String,
    scheduled: Option<cdf_runtime::ScheduledPartition>,
    runtime: PartitionOpenRuntime,
) -> cdf_kernel::BoxFuture<'a, Result<OpenedPartition>>
where
    R: ResourceStream + ?Sized,
{
    if terminal_quarantine {
        return Box::pin(async move {
            Ok((
                PartitionOpenMetadata {
                    ordinal,
                    partition,
                    evidence: PartitionOpenEvidence {
                        duration_ns: 0,
                        retry_pre_attestation: None,
                    },
                },
                None,
            ))
        });
    }
    Box::pin(async move {
        let PartitionOpenRuntime {
            services,
            cancellation,
            retry_journal,
        } = runtime;
        // Construct the source-owned stream only when this open future is polled. In particular,
        // remote sources may resolve short-lived access capabilities while opening; creating them
        // while merely filling the scheduler frontier can let them expire before transfer starts.
        let started = Instant::now();
        let retry = scheduled
            .as_ref()
            .and_then(|partition| partition.retry.clone());
        let mut retry_state = match (retry, services) {
            (Some(retry), Some(services)) => {
                Some(cdf_runtime::SourceRetryState::new(&retry, None, services)?)
            }
            (Some(_), None) => {
                return Err(CdfError::contract(
                    "retryable partition execution requires injected execution services",
                ));
            }
            (None, _) => None,
        };
        loop {
            let retry_pre_attestation = if retry_state
                .as_ref()
                .is_some_and(|state| state.current_attempt() > 1)
            {
                match attest_partition_with_terminal_join(resource, &partition, &cancellation).await
                {
                    Ok(Some(attestation)) => Some(attestation),
                    Ok(None) => {
                        let error = CdfError::data(format!(
                            "retry of partition `{}` requires source reattestation before reopen",
                            partition.partition_id
                        ));
                        schedule_partition_retry(
                            retry_state.as_mut().expect("retry state exists"),
                            &error,
                            cancellation.clone(),
                            &plan_id,
                            scheduled.as_ref().expect("retry schedule exists"),
                            &retry_journal,
                        )
                        .await?;
                        continue;
                    }
                    Err(error) => {
                        schedule_partition_retry(
                            retry_state.as_mut().expect("retry state exists"),
                            &error,
                            cancellation.clone(),
                            &plan_id,
                            scheduled.as_ref().expect("retry schedule exists"),
                            &retry_journal,
                        )
                        .await?;
                        continue;
                    }
                }
            } else {
                None
            };
            let mut opening = resource.open(partition.clone());
            match cancellation.await_or_cancel(&mut opening).await {
                Ok(mut stream) => {
                    let first_batch = if let Some(retry_state) = retry_state.as_mut() {
                        match next_source_batch(&mut stream, &cancellation).await {
                            Ok(batch) => batch,
                            Err(error) => {
                                let cancelled = cancellation.is_cancelled();
                                let decision = if cancelled {
                                    Ok(None)
                                } else {
                                    decide_partition_retry(
                                        retry_state,
                                        &error,
                                        &plan_id,
                                        scheduled.as_ref().expect("retry schedule exists"),
                                        &retry_journal,
                                    )
                                    .map(Some)
                                    .map_err(
                                        |decision_error| {
                                            with_cleanup_failure(
                                                error.clone(),
                                                "source retry decision recording",
                                                decision_error,
                                            )
                                        },
                                    )
                                };
                                let cleanup = if cancelled {
                                    stream.terminate_and_join().await
                                } else {
                                    stream.join_failed_attempt().await
                                };
                                let decision = match (decision, cleanup) {
                                    (Ok(decision), Ok(())) => decision,
                                    (Err(error), Ok(())) => return Err(error),
                                    (Ok(_), Err(cleanup)) => {
                                        return Err(with_cleanup_failure(
                                            error,
                                            "failed source attempt termination",
                                            cleanup,
                                        ));
                                    }
                                    (Err(error), Err(cleanup)) => {
                                        return Err(with_cleanup_failure(
                                            error,
                                            "failed source attempt termination",
                                            cleanup,
                                        ));
                                    }
                                };
                                let Some(decision) = decision else {
                                    return Err(error);
                                };
                                await_partition_retry(
                                    retry_state,
                                    decision,
                                    &error,
                                    cancellation.clone(),
                                    &plan_id,
                                    scheduled.as_ref().expect("retry schedule exists"),
                                    &retry_journal,
                                )
                                .await?;
                                continue;
                            }
                        }
                    } else {
                        None
                    };
                    if let Some(first_batch) = first_batch {
                        stream.prepend_batch(first_batch)?;
                    }
                    return Ok((
                        PartitionOpenMetadata {
                            ordinal,
                            partition,
                            evidence: PartitionOpenEvidence {
                                duration_ns: elapsed_ns(Some(started), "resource open")?,
                                retry_pre_attestation,
                            },
                        },
                        Some(stream),
                    ));
                }
                Err(error) => {
                    if cancellation.is_cancelled() {
                        return match opening.terminate_and_join().await {
                            Ok(()) => Err(error),
                            Err(cleanup) => Err(with_cleanup_failure(
                                error,
                                "cancelled source opening termination",
                                cleanup,
                            )),
                        };
                    }
                    let decision = retry_state
                        .as_mut()
                        .map(|state| {
                            decide_partition_retry(
                                state,
                                &error,
                                &plan_id,
                                scheduled.as_ref().expect("retry schedule exists"),
                                &retry_journal,
                            )
                        })
                        .transpose()
                        .map_err(|decision_error| {
                            with_cleanup_failure(
                                error.clone(),
                                "source retry decision recording",
                                decision_error,
                            )
                        });
                    let cleanup = opening.terminate_and_join().await;
                    let decision = match (decision, cleanup) {
                        (Ok(decision), Ok(())) => decision,
                        (Err(error), Ok(())) => return Err(error),
                        (Ok(_), Err(cleanup)) => {
                            return Err(with_cleanup_failure(
                                error,
                                "opening source invocation termination",
                                cleanup,
                            ));
                        }
                        (Err(error), Err(cleanup)) => {
                            return Err(with_cleanup_failure(
                                error,
                                "opening source invocation termination",
                                cleanup,
                            ));
                        }
                    };
                    let Some(state) = retry_state.as_mut() else {
                        return Err(error);
                    };
                    await_partition_retry(
                        state,
                        decision.expect("retry state produced a decision"),
                        &error,
                        cancellation.clone(),
                        &plan_id,
                        scheduled.as_ref().expect("retry schedule exists"),
                        &retry_journal,
                    )
                    .await?;
                }
            }
        }
    })
}

async fn attest_partition_with_terminal_join<R>(
    resource: &R,
    partition: &cdf_kernel::PartitionPlan,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<Option<cdf_kernel::PartitionAttestation>>
where
    R: ResourceStream + ?Sized,
{
    let mut attempt = resource.attest_partition(partition.clone());
    match cancellation.await_or_cancel(&mut attempt).await {
        Ok(attestation) => Ok(attestation),
        Err(error) if cancellation.is_cancelled() => match attempt.terminate_and_join().await {
            Ok(()) => Err(error),
            Err(cleanup) => Err(with_cleanup_failure(
                error,
                "cancelled source attestation termination",
                cleanup,
            )),
        },
        Err(error) => Err(error),
    }
}

async fn next_source_batch(
    stream: &mut cdf_kernel::OpenedPartitionStream,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<Option<Batch>> {
    cancellation
        .await_or_cancel(async {
            match stream.next().await {
                Some(batch) => batch.map(Some),
                None => Ok(None),
            }
        })
        .await
}

fn decide_partition_retry(
    state: &mut cdf_runtime::SourceRetryState,
    error: &CdfError,
    plan_id: &str,
    partition: &cdf_runtime::ScheduledPartition,
    journal: &cdf_runtime::SourceRetryJournal,
) -> Result<cdf_runtime::SourceRetryDecision> {
    let decision = state.decide_after_failure(error)?;
    journal.record(plan_id, partition, state.history())?;
    Ok(decision)
}

async fn await_partition_retry(
    state: &mut cdf_runtime::SourceRetryState,
    decision: cdf_runtime::SourceRetryDecision,
    error: &CdfError,
    cancellation: cdf_runtime::RunCancellation,
    plan_id: &str,
    partition: &cdf_runtime::ScheduledPartition,
    journal: &cdf_runtime::SourceRetryJournal,
) -> Result<()> {
    if matches!(
        decision,
        cdf_runtime::SourceRetryDecision::GiveUp {
            reason: cdf_runtime::SourceRetryExhaustion::Ineligible
        }
    ) {
        return Err(error.clone());
    }
    let retry = state.wait_for_retry(decision, cancellation).await;
    journal.record(plan_id, partition, state.history())?;
    let retry = retry?;
    if !retry {
        return Err(retry_exhausted_error(error, state.history()));
    }
    Ok(())
}

async fn schedule_partition_retry(
    state: &mut cdf_runtime::SourceRetryState,
    error: &CdfError,
    cancellation: cdf_runtime::RunCancellation,
    plan_id: &str,
    partition: &cdf_runtime::ScheduledPartition,
    journal: &cdf_runtime::SourceRetryJournal,
) -> Result<()> {
    let decision = decide_partition_retry(state, error, plan_id, partition, journal)?;
    await_partition_retry(
        state,
        decision,
        error,
        cancellation,
        plan_id,
        partition,
        journal,
    )
    .await
}

fn with_cleanup_failure(mut primary: CdfError, context: &str, cleanup: CdfError) -> CdfError {
    primary.message = format!(
        "{}; {context} also failed: {}",
        primary.message, cleanup.message
    );
    primary
}

fn retry_exhausted_error(
    error: &CdfError,
    history: &[cdf_runtime::SourceRetryHistoryEntry],
) -> CdfError {
    let attempts = history
        .last()
        .map_or(1, |entry| entry.failed_attempt.max(1));
    CdfError::new(
        error.kind.clone(),
        format!(
            "{}; source retry stopped after {attempts} attempt(s) ({})",
            error.message,
            history.last().and_then(|entry| entry.exhaustion).map_or(
                "retry unavailable",
                |reason| match reason {
                    cdf_runtime::SourceRetryExhaustion::Ineligible => "error is not retryable",
                    cdf_runtime::SourceRetryExhaustion::AttemptLimit => {
                        "attempt limit exhausted"
                    }
                    cdf_runtime::SourceRetryExhaustion::ElapsedDeadline => {
                        "elapsed deadline exhausted"
                    }
                }
            )
        ),
    )
}

fn scheduled_partition(
    schedule: Option<&cdf_runtime::CanonicalPartitionSchedule>,
    ordinal: usize,
    partition: &PartitionPlan,
) -> Result<Option<cdf_runtime::ScheduledPartition>> {
    let Some(schedule) = schedule else {
        return Ok(None);
    };
    let scheduled = schedule.partitions.get(ordinal).ok_or_else(|| {
        CdfError::contract("partition schedule does not cover every scan partition")
    })?;
    if usize::try_from(scheduled.ordinal.get()).ok() != Some(ordinal)
        || &scheduled.partition != partition
    {
        return Err(CdfError::contract(
            "partition schedule ordinal or plan differs from the executable scan partition",
        ));
    }
    Ok(Some(scheduled.clone()))
}

fn source_partition_opener<'a, R>(
    resource: &'a R,
    partitions: Vec<cdf_kernel::PartitionPlan>,
    effective_schema_evidence: Option<&'a EffectiveSchemaPlanEvidence>,
    schedule: Option<&'a cdf_runtime::CanonicalPartitionSchedule>,
    open_runtime: PartitionOpenRuntime,
) -> cdf_runtime::SourcePartitionOpener<'a, PartitionOpenMetadata>
where
    R: ResourceStream + ?Sized,
{
    Box::new(move |ordinal, cancellation| {
        let partition = partitions.get(ordinal).cloned().ok_or_else(|| {
            CdfError::internal("source frontier requested an absent partition ordinal")
        })?;
        let terminal = effective_schema_evidence
            .map(|evidence| partition_schema_disposition(&partition, evidence))
            .transpose()?
            .is_some_and(|disposition| {
                matches!(disposition, PartitionSchemaDisposition::Quarantined(_))
            });
        let scheduled = scheduled_partition(schedule, ordinal, &partition)?;
        let mut partition_runtime = open_runtime.clone();
        partition_runtime.cancellation = cancellation;
        Ok(open_partition(
            resource,
            ordinal,
            partition,
            terminal,
            schedule.map_or_else(String::new, |schedule| schedule.plan_id.clone()),
            scheduled,
            partition_runtime,
        ))
    })
}

fn source_frontier_batch_bounds(plan: &EnginePlan, partition_count: usize) -> Result<Vec<u64>> {
    if partition_count == 0 {
        return Ok(Vec::new());
    }
    let schedule = plan.partition_schedule.as_ref().ok_or_else(|| {
        CdfError::contract("package execution requires a compiled partition schedule")
    })?;
    if schedule.partitions.len() != partition_count {
        return Err(CdfError::contract(
            "source frontier schedule does not cover every executable partition",
        ));
    }
    if schedule
        .partitions
        .iter()
        .any(|partition| partition.maximum_working_set_bytes == 0)
    {
        return Err(CdfError::contract(
            "source frontier partition requires a nonzero working-set bound",
        ));
    }
    let maximum_batch_bytes = plan
        .compiled_source_execution
        .as_ref()
        .ok_or_else(|| {
            CdfError::contract("package execution requires a compiled source execution plan")
        })?
        .execution_capabilities()
        .maximum_decode_bytes;
    // The partition schedule owns admission for the source's complete concurrent working set
    // (transport poll plus decode). The frontier retains only the decoded batch crossing the
    // source edge, so reserving the schedule total here double-counts transport memory and can
    // make a valid single-partition schedule impossible to execute under the same ledger.
    Ok(vec![maximum_batch_bytes; partition_count])
}

pub(crate) fn partition_open_jobs(plan: &EnginePlan, options: &EngineExecutionOptions) -> usize {
    let partition_count = plan.scan.partitions.len();
    if partition_count <= 1 {
        return partition_count.max(1);
    }
    let (Some(_graph), Some(_schedule), Some(scheduler)) = (
        plan.operator_graph.as_ref(),
        plan.partition_schedule.as_ref(),
        options.scheduler.as_ref(),
    ) else {
        return 1;
    };
    // A canonical limit owns exact input-attempt authority. Opening a later payload can perform
    // transport I/O, mutate source-owned accounting, or attest an input the limit will discard.
    // Keep the frontier serial until the planner can assign an explicit speculative byte budget
    // and the source can prove that opening is side-effect free.
    if plan.scan.request.limit.is_some() {
        return 1;
    }
    let speculative_safe = plan
        .compiled_source_execution
        .as_ref()
        .is_some_and(|source| source.execution_capabilities().speculative_safe);
    if !speculative_safe {
        return 1;
    }
    usize::from(scheduler.effective_jobs.jobs.max(1)).min(partition_count)
}

#[allow(clippy::too_many_arguments)]
async fn execute_to_package_inner<'a, R>(
    trace_context: Option<&ExecutionTraceContext>,
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: Option<&PackagePreFinalizeHook<'_>>,
    durable_segment: Option<&'a mut DurableSegmentHook<'a>>,
    stream_finalize: Option<&'a mut StreamingFinalizeHook<'a>>,
    mut drain_controller: Option<&mut cdf_runtime::DrainEpochController>,
    options: EngineExecutionOptions,
) -> Result<EngineRunOutputWithSegmentPositions>
where
    R: ResourceStream + ?Sized,
{
    plan.validate_execution_extent_for_execution()?;
    match (&plan.execution_extent, drain_controller.is_some()) {
        (ExecutionExtent::Bounded { .. }, false) | (ExecutionExtent::Drain { .. }, true) => {}
        (ExecutionExtent::Bounded { .. }, true) => {
            return Err(CdfError::contract(
                "bounded execution cannot use the drain epoch controller",
            ));
        }
        (ExecutionExtent::Drain { .. }, false) => {
            return Err(CdfError::contract(
                "drain execution requires the finite epoch controller and settlement gate",
            ));
        }
        (ExecutionExtent::Resident { .. }, _) => {
            return Err(CdfError::contract(
                "resident execution is not enabled; use a finite drain termination",
            ));
        }
    }
    if let Some(controller) = drain_controller.as_deref() {
        controller.validate_ready_for_epoch()?;
    }
    plan.validate_compiled_expression_plan()?;
    plan.validate_partition_schedule()?;
    plan.validate_compiled_source_resource(resource)?;
    if let Some(scheduler) = &options.scheduler {
        let source = plan.compiled_source_execution.as_ref().ok_or_else(|| {
            CdfError::contract("package execution requires a compiled source execution plan")
        })?;
        scheduler
            .validate_for_source(plan.scan.partitions.len(), source.execution_capabilities())?;
    }
    let validation_program = plan.validation_program.clone();
    validate_program(&validation_program)?;
    cdf_kernel::validate_scan_partition_observation_identities(&plan.scan)?;
    cdf_kernel::validate_compiled_scan_intents(&plan.scan)?;
    let schema_authority = plan.schema_authority();
    if schema_authority.version != 1 {
        return Err(CdfError::data(format!(
            "unsupported engine schema-authority version {}",
            schema_authority.version
        )));
    }
    let effective_schema_evidence = validate_effective_schema_plan(plan, resource)?;
    crate::planning::validate_plan_schema_authority(resource, plan)?;
    let resource_schema = resource.schema();
    let runtime_output_schema = plan.output_arrow_schema()?;
    let expression_schema = scan_expression_schema(
        resource_schema.as_ref(),
        plan.explain
            .projection_pushed
            .then_some(plan.scan.request.projection.as_deref())
            .flatten(),
    )?;
    let bound_residuals =
        bind_filter_expressions(&plan.compiled_expression_plan.residuals, &expression_schema)?;
    let bound_transforms = bind_expression_transforms(
        &plan.validation_program.transforms,
        &plan.compiled_expression_plan.transforms,
        &expression_schema,
    )?;
    let bound_tracked_transforms = bind_expression_transforms(
        &plan.validation_program.transforms,
        &plan.compiled_expression_plan.transforms,
        &source_row_tracking_schema(&expression_schema)?,
    )?;
    let contract_schema =
        expression_transform_output_schema(&plan.validation_program.transforms, &expression_schema);
    let pre_contract_may_filter = !bound_residuals.is_empty()
        || plan.validation_program.transforms.iter().any(|transform| {
            matches!(transform, cdf_contract::TransformDescription::Filter { .. })
        });

    let builder = PackageBuilder::create(package_dir, plan.package_id.clone())?;
    builder.update_status(PackageStatus::Extracting)?;
    builder.write_json_artifact(cdf_package_contract::SCAN_PLAN_FILE, &plan.scan)?;
    builder.write_json_artifact("plan/explain.json", &plan.explain)?;
    if let Some(graph) = &plan.operator_graph {
        graph.validate_plan_join(&plan.execution_extent, plan.compiled_stream_policy.as_ref())?;
        builder.write_json_artifact("plan/operator-graph.json", graph)?;
    }
    builder.write_json_artifact("plan/validation-program.json", &validation_program)?;
    builder.write_json_artifact(
        "plan/schema-admission.json",
        &plan.compiled_schema_admission,
    )?;
    if let Some(evidence) = effective_schema_evidence {
        builder.write_json_artifact("schema/effective-schema-evidence.json", evidence)?;
    }
    let package_evaluation_context =
        ContractEvaluationContext::observed_at(current_observed_at_ms()?);
    let mut contract_evaluator =
        VectorValidationEvaluator::new_bound(&validation_program, Arc::new(contract_schema))?;
    if validation_program.requires_observed_at_ms() {
        builder.write_json_artifact(
            "plan/contract-evaluation-context.json",
            &package_evaluation_context,
        )?;
    }

    let statistics_memory: Arc<dyn MemoryCoordinator> = match options.services.as_ref() {
        Some(services) => services.memory(),
        None => Arc::new(DeterministicMemoryCoordinator::new(
            DEFAULT_PROCESS_BUDGET_BYTES,
            BTreeMap::new(),
        )?),
    };
    let mut profile = ExecutionProfile::default();
    let mut statistics_memory_lease = None;
    let mut statistics_profile = options
        .statistics_profile
        .then(|| builder.begin_statistics_profile())
        .transpose()?;
    let statistics_profile_schema_hash = schema_authority.effective_schema_hash.as_str().to_owned();
    let mut statistics_segment_ordinal = 0_u64;
    let mut verdict_summary = VerdictSummary::default();
    let mut lineage = LineageSummary::default();
    let mut segments = Vec::new();
    let mut segment_positions = Vec::new();
    let mut quarantine_part_count = 0_usize;
    let mut remaining_limit = plan.scan.request.limit;
    let mut output_schema = Some(schema_artifact(runtime_output_schema.as_ref()));
    let mut stream_admission_evidence =
        BTreeMap::<String, StreamAdmissionObservationEvidence>::new();
    let mut stream_physical_observation_catalog =
        BTreeMap::<String, PhysicalObservationEvidence>::new();
    let mut schema_admission_cache =
        BTreeMap::<cdf_kernel::SchemaHash, cdf_contract::SchemaCoercionPlan>::new();
    let mut processed_observations = Vec::new();
    let mut checkpoint_eligible = true;
    let mut drain_partition_resume = None;
    let mut completion_positions = Vec::<(u32, PartitionPlan, SourcePosition)>::new();
    let mut terminal_quarantines = Vec::new();
    let mut quarantine_physical_observations =
        BTreeMap::<String, PhysicalObservationEvidence>::new();
    let mut observation_attestations = BTreeMap::<String, PartitionAttestation>::new();
    let mut residual_decisions = match (&options.services, validation_program.residual.as_ref()) {
        (Some(services), Some(_)) => {
            ResidualDecisionAccumulator::Spill(crate::residual_spill::ResidualDecisionRuns::create(
                builder.package_dir().join(".residual-decisions-spill"),
                services.spill(),
                Some(services.memory()),
            )?)
        }
        _ => ResidualDecisionAccumulator::Memory(Vec::new()),
    };
    let apply_package_dedup = validation_program.has_exact_row_dedup_rule()
        || (plan.write_disposition == WriteDisposition::Merge
            && validation_program.has_keyed_dedup_rule());
    let mut pending_dedup_batches = Vec::new();
    let mut phase_measurements = PhaseMeasurements::new(options.phase_metrics);
    let memory = options
        .services
        .as_ref()
        .map(cdf_runtime::ExecutionServices::memory);
    let mut external_dedup = if apply_package_dedup {
        options
            .services
            .as_ref()
            .map(|services| -> Result<_> {
                let rule = package_dedup_rule(&validation_program)?.ok_or_else(|| {
                    CdfError::internal("package dedup was selected without a compiled rule")
                })?;
                let index = crate::dedup_spill::ExternalDedupIndex::create(
                    builder.package_dir().join(".dedup-spill"),
                    services.spill(),
                    Some(services.memory()),
                )?;
                let payload = crate::dedup_spill::DedupPayloadSpool::create(
                    builder.package_dir().join(".dedup-payload"),
                    services.spill(),
                )?;
                Ok((rule, index, payload))
            })
            .transpose()?
    } else {
        None
    };
    let segmentation_policy = plan.segmentation_policy()?.clone();
    let mut durable_segment_observer = DurableSegmentObserver {
        hook: durable_segment,
    };
    let mut segment_queue = SegmentEncodeQueue::new(
        &builder,
        options.services.as_ref(),
        phase_measurements.enabled,
        &plan.package_id,
        segmentation_policy.maximum_bytes,
    )?;

    let partition_jobs = if remaining_limit == Some(0) {
        0
    } else {
        partition_open_jobs(plan, &options)
    };
    let run_cancellation = options.cancellation.clone();
    let partition_open_runtime = PartitionOpenRuntime {
        services: options.services.clone(),
        cancellation: run_cancellation.clone(),
        retry_journal: options.retry_journal.clone(),
    };
    let frontier_partition_count = if partition_jobs == 0 {
        0
    } else {
        plan.scan.partitions.len()
    };
    let source_opener = source_partition_opener(
        resource,
        plan.scan.partitions.clone(),
        effective_schema_evidence,
        plan.partition_schedule.as_ref(),
        partition_open_runtime,
    );
    let source_batch_bounds = source_frontier_batch_bounds(plan, frontier_partition_count)?;
    let source_batch_memory = plan
        .compiled_source_execution
        .as_ref()
        .ok_or_else(|| {
            CdfError::contract("package execution requires a compiled source execution plan")
        })?
        .batch_memory_contract();
    let mut source_frontier = cdf_runtime::CanonicalSourceFrontier::new(
        frontier_partition_count,
        partition_jobs.max(1),
        source_opener,
        source_batch_bounds,
        memory.clone(),
        source_batch_memory,
        run_cancellation.clone(),
    )?
    .with_measurement(options.phase_metrics);
    let drain_epoch_started = Instant::now();
    let drain_clock_base = drain_controller
        .as_deref()
        .map_or(0, cdf_runtime::DrainEpochController::monotonic_milliseconds);
    let drain_batch_frontiers_enabled = drain_controller.is_some()
        && !plan
            .compiled_source_execution
            .as_ref()
            .ok_or_else(|| {
                CdfError::contract("drain execution requires compiled source authority")
            })?
            .execution_capabilities()
            .bounded;
    let mut drain_epoch_closure = None;
    let mut consumed_partition_count = 0_usize;

    let segment_result: Result<()> = async {
    while let Some(mut opened_partition) = source_frontier.next_partition().await? {
        let open_metadata = opened_partition.metadata().clone();
        let partition_ordinal_usize = open_metadata.ordinal;
        let partition = open_metadata.partition;
        let open_evidence = open_metadata.evidence;
        let partition_ordinal = u32::try_from(partition_ordinal_usize)
            .map_err(|_| CdfError::data("partition ordinal exceeds u32"))?;
        let partition_scope = partition.scope.clone();
        let partition_drain_batch_frontiers_enabled =
            drain_batch_frontiers_enabled && partition.planned_file()?.is_none();
        let current_schema_disposition = effective_schema_evidence
            .map(|evidence| partition_schema_disposition(&partition, evidence))
            .transpose()?;
        if let Some(PartitionSchemaDisposition::Quarantined(quarantine)) =
            &current_schema_disposition
        {
            let attestation = match observation_attestations.get(quarantine.observation_id()) {
                Some(attestation) => attestation.clone(),
                None => {
                    let attestation = attest_partition_with_terminal_join(
                        resource,
                        &partition,
                        &run_cancellation,
                    )
                    .await?
                        .ok_or_else(|| {
                            CdfError::data(format!(
                                "terminal schema observation {:?} has no execution-time attestation",
                                quarantine.observation_id()
                            ))
                        })?;
                    observation_attestations
                        .insert(quarantine.observation_id().to_owned(), attestation.clone());
                    attestation
                }
            };
            if attestation.physical_schema_hash() != Some(quarantine.physical_schema_hash()) {
                return Err(CdfError::data(format!(
                    "terminal schema observation {:?} changed physical schema between planning and execution; expected {}, attested {:?}; re-plan before retrying",
                    quarantine.observation_id(),
                    quarantine.physical_schema_hash(),
                    attestation.physical_schema_hash()
                )));
            }
            let source_position = attestation.into_processed_position();
            processed_observations.push(ProcessedObservationPosition::new(
                quarantine.observation_id().to_owned(),
                ProcessedObservationOutcome::Quarantined,
                source_position.clone(),
            )?);
            let mut quarantine = quarantine.clone();
            quarantine.bind_source_position(source_position)?;
            let physical_observation = effective_schema_evidence
                .and_then(|evidence| {
                    evidence
                        .physical_observation_catalog
                        .get(quarantine.physical_schema_hash().as_str())
                })
                .cloned()
                .ok_or_else(|| {
                    CdfError::internal(format!(
                        "preobserved quarantine {:?} omitted its physical observation",
                        quarantine.observation_id()
                    ))
                })?;
            record_schema_quarantine(
                &mut terminal_quarantines,
                &mut quarantine_physical_observations,
                quarantine,
                physical_observation,
            )?;
            opened_partition.finish_metadata_only()?;
            consumed_partition_count = consumed_partition_count.saturating_add(1);
            if let Some(controller) = drain_controller.as_deref_mut() {
                let frontier = drain_resource_frontier(
                    resource.descriptor(),
                    resource_schema.as_ref(),
                    controller.committed_frontier(),
                    &processed_observations,
                )?;
                let decision = controller.observe_safe_frontier(
                    cdf_runtime::DrainSafeFrontierObservation {
                        frontier,
                        carryover: None,
                        admitted_batches: 0,
                        admitted_rows: 0,
                        admitted_bytes: 0,
                        admitted_positions: 1,
                        global_watermark: None,
                        source_exhausted: consumed_partition_count == frontier_partition_count,
                        monotonic_milliseconds: drain_clock_base.saturating_add(
                            u64::try_from(drain_epoch_started.elapsed().as_millis())
                                .unwrap_or(u64::MAX),
                        ),
                        observed_at_unix_milliseconds: current_observed_at_u64_ms()?,
                    },
                )?;
                match decision {
                    cdf_runtime::DrainEpochDecision::Continue => continue,
                    cdf_runtime::DrainEpochDecision::Close(closure) => {
                        drain_epoch_closure = Some(*closure);
                        break;
                    }
                    cdf_runtime::DrainEpochDecision::FinishedNoOp => {
                        return Err(CdfError::internal(
                            "drain controller classified a processed source position as an empty epoch",
                        ));
                    }
                }
            }
            continue;
        }
        let partition_schema_evidence =
            current_schema_disposition
                .as_ref()
                .and_then(|item| match item {
                    PartitionSchemaDisposition::Admitted(evidence) => Some(evidence),
                    PartitionSchemaDisposition::Quarantined(_)
                    | PartitionSchemaDisposition::Unobserved => None,
                });
        let partition_span = trace_context
            .map(|context| partition_execution_span(context, partition.partition_id.as_str()))
            .unwrap_or_else(Span::none);

        let mut segment_assembler =
            crate::CanonicalSegmentAssembler::new(segmentation_policy.clone(), partition_ordinal)?;
        if !opened_partition.has_stream() {
            return Err(CdfError::internal(
                "admitted partition reached execution without an open stream",
            ));
        }
        let partition_result = async {
            phase_measurements.add(
                RunPhase::Decode,
                open_evidence.duration_ns,
                0,
                0,
            );
            let mut fully_processed = true;
            let mut observed_partition_position = None;
            let mut dynamic_quarantine = None;
            let mut partition_observation_id = None::<String>;
            let mut admitted_batch_count = 0_u64;
            let mut partition_input_batch_count = 0_u64;
            let mut partition_input_bytes = 0_u64;
            let mut partition_watermark = None;
            let mut partition_source_row_ordinal = 0_u64;
            let mut partition_epoch_closed = false;
            let mut partition_batch_frontiers_observed = false;
            loop {
                if remaining_limit == Some(0) {
                    fully_processed = false;
                    break;
                }
                let decode_started = phase_measurements.start();
                let next_batch = opened_partition.next_batch().await?;
                let decode_duration_ns = elapsed_ns(decode_started, "resource decode")?;
                let Some(batch) = next_batch else {
                    phase_measurements.add(RunPhase::Decode, decode_duration_ns, 0, 0);
                    break;
                };
                let mut batch = batch;
                partition_input_batch_count = partition_input_batch_count.saturating_add(1);
                partition_input_bytes = partition_input_bytes
                    .checked_add(batch.header.byte_count)
                    .ok_or_else(|| CdfError::data("drain partition input byte count overflow"))?;
                if let Some(watermark) = batch.header.watermarks.last() {
                    partition_watermark = Some(watermark.clone());
                }
                validate_batch_partition_ownership(
                    &batch,
                    &plan.scan.request.resource_id,
                    &partition,
                )?;
                let decoded_input_bytes = batch.header.byte_count;
                phase_measurements.add(
                    RunPhase::Decode,
                    decode_duration_ns,
                    decoded_input_bytes,
                    decoded_input_bytes,
                );
                let validation_started = phase_measurements.start();
                if lineage.input_partitions.last() != Some(&batch.header.partition_id) {
                    lineage.input_partitions.push(batch.header.partition_id.clone());
                }
                lineage.input_rows = lineage.input_rows.saturating_add(batch.header.row_count);
                if !batch.header.pre_contract_quarantine.is_empty() {
                    merge_verdict_summary(
                        &mut verdict_summary,
                        pre_contract_quarantine_summary(&batch.header.pre_contract_quarantine),
                    );
                    let quarantine_lease = reserve_quarantine_evidence(memory.as_ref())?;
                    let mut quarantine_sink =
                        QuarantinePartAccumulator::new(
                            &builder,
                            &mut quarantine_part_count,
                            quarantine_lease,
                        );
                    for fact in &batch.header.pre_contract_quarantine {
                        quarantine_sink.push(quarantine_record_from_pre_contract(fact))?;
                    }
                    quarantine_sink.finish()?;
                }
                let cdc_operation_field = batch
                    .header
                    .cdc
                    .as_ref()
                    .map(|metadata| metadata.operation_field.clone());
                let Some(record_batch) = batch.record_batch() else {
                    return Err(CdfError::data(
                        "package execution requires in-memory Arrow record batches at MVP",
                    ));
                };
                let validation_input_bytes = u64::try_from(record_batch.get_array_memory_size())
                    .map_err(|error| CdfError::internal(error.to_string()))?;
                let reconciled = materialize_batch_schema_evidence(
                    &batch,
                    record_batch,
                    BatchSchemaAdmissionContext {
                        planned_observation_id: cdf_kernel::partition_schema_observation_id(
                            &partition,
                        ),
                        expected: partition_schema_evidence,
                        expected_physical_observation: preobserved_physical_observation(
                            effective_schema_evidence,
                            partition_schema_evidence,
                        )?,
                        effective_schema: &expression_schema,
                    },
                    &plan.compiled_schema_admission,
                    &mut schema_admission_cache,
                )?;
                let reconciled = match reconciled {
                    BatchSchemaDisposition::Admitted(reconciled) => reconciled,
                    BatchSchemaDisposition::Quarantined {
                        quarantine,
                        physical_observation,
                    } => {
                        if admitted_batch_count != 0 {
                            return Err(CdfError::data(format!(
                                "partition {:?} changed to an incompatible physical schema after {admitted_batch_count} admitted batches; the codec must isolate schema epochs before partition admission",
                                partition.partition_id
                            )));
                        }
                        if let Some(source_position) = normalize_source_position_for_partition(
                            batch.header.source_position.clone(),
                            &partition_scope,
                        ) {
                            accumulate_processed_partition_position(
                                cdf_kernel::partition_schema_observation_id(&partition),
                                resource.descriptor(),
                                resource_schema.as_ref(),
                                &mut observed_partition_position,
                                source_position,
                            )?;
                        }
                        partition_source_row_ordinal = partition_source_row_ordinal
                            .saturating_add(batch.header.row_count);
                        dynamic_quarantine = Some((quarantine, physical_observation));

                        // Schema quarantine is a whole-partition verdict. Drain the invocation to
                        // EOF so weak sources can finish their terminal content hash and the
                        // checkpoint records only fully consumed input. No drained batch enters
                        // validation or segment production after this verdict is fixed.
                        loop {
                            let decode_started = phase_measurements.start();
                            let next_batch = opened_partition.next_batch().await?;
                            let decode_duration_ns =
                                elapsed_ns(decode_started, "quarantined resource drain")?;
                            let Some(drained) = next_batch else {
                                phase_measurements.add(
                                    RunPhase::Decode,
                                    decode_duration_ns,
                                    0,
                                    0,
                                );
                                break;
                            };
                            let drained = drained;
                            partition_input_batch_count =
                                partition_input_batch_count.saturating_add(1);
                            partition_input_bytes = partition_input_bytes
                                .checked_add(drained.header.byte_count)
                                .ok_or_else(|| {
                                    CdfError::data("drain partition input byte count overflow")
                                })?;
                            if let Some(watermark) = drained.header.watermarks.last() {
                                partition_watermark = Some(watermark.clone());
                            }
                            validate_batch_partition_ownership(
                                &drained,
                                &plan.scan.request.resource_id,
                                &partition,
                            )?;
                            let decoded_input_bytes = drained.header.byte_count;
                            phase_measurements.add(
                                RunPhase::Decode,
                                decode_duration_ns,
                                decoded_input_bytes,
                                decoded_input_bytes,
                            );
                            lineage.input_rows =
                                lineage.input_rows.saturating_add(drained.header.row_count);
                            partition_source_row_ordinal = partition_source_row_ordinal
                                .saturating_add(drained.header.row_count);
                            if let Some(source_position) =
                                normalize_source_position_for_partition(
                                    drained.header.source_position.clone(),
                                    &partition_scope,
                                )
                            {
                                accumulate_processed_partition_position(
                                    cdf_kernel::partition_schema_observation_id(&partition),
                                    resource.descriptor(),
                                    resource_schema.as_ref(),
                                    &mut observed_partition_position,
                                    source_position,
                                )?;
                            }
                        }
                        break;
                    }
                };
                admitted_batch_count = admitted_batch_count.saturating_add(1);
                if let Some(coercion_plan) = reconciled.coercion_plan.as_ref()
                    && plan.compiled_schema_admission.captures_unknown_fields()?
                {
                    let candidates = stream_admission_residual_candidates(
                        record_batch,
                        coercion_plan,
                        batch.header.residual_candidates(),
                        matches!(
                            reconciled.extra_field_evidence,
                            ExtraFieldEvidence::AlreadyCaptured
                        ) && batch.header.materialized_residuals_complete(),
                        partition_source_row_ordinal,
                    )?;
                    batch.header.extend_residual_candidates(candidates);
                }
                partition_source_row_ordinal = partition_source_row_ordinal
                    .saturating_add(batch.header.row_count);
                let residual_candidates = batch.header.take_residual_candidates();
                let record_batch = reconciled.record_batch;
                let batch_coercion = reconciled.coercion_plan;
                if let Some(batch_coercion) = batch_coercion {
                    let observation_id = reconciled.observation_id.as_deref().ok_or_else(|| {
                        CdfError::internal("schema coercion omitted its observation identity")
                    })?;
                    let physical_observation = reconciled.physical_observation.ok_or_else(|| {
                        CdfError::internal("schema coercion omitted its physical observation")
                    })?;
                    if partition_observation_id
                        .as_deref()
                        .is_some_and(|existing| existing != observation_id)
                    {
                        return Err(CdfError::data(format!(
                            "partition {:?} emitted multiple schema observation identities",
                            partition.partition_id
                        )));
                    }
                    partition_observation_id = Some(observation_id.to_owned());
                    record_observation_schema_coercion(
                        &mut stream_admission_evidence,
                        &mut stream_physical_observation_catalog,
                        observation_id,
                        physical_observation,
                        batch_coercion,
                    )?;
                } else if partition_schema_evidence.is_some() {
                    return Err(CdfError::data(
                        "effective-schema execution requires trusted per-observation coercion evidence on every batch",
                    ));
                }

                let track_source_rows =
                    pre_contract_may_filter || !residual_candidates.is_empty();
                let executed = execute_batch(&record_batch, &bound_residuals, track_source_rows)?;
                let ExecutedBatch {
                    batch: output,
                    source_rows,
                    limit_truncated,
                } = apply_pre_contract_expressions(
                    executed.batch,
                    if track_source_rows {
                        &bound_tracked_transforms
                    } else {
                        &bound_transforms
                    },
                    &mut remaining_limit,
                    track_source_rows,
                )?;
                let batch_source_position = normalize_source_position_for_partition(
                    batch.header.source_position.clone(),
                    &partition_scope,
                );
                if let Some(position) = &batch_source_position {
                    accumulate_processed_partition_position(
                        cdf_kernel::partition_schema_observation_id(&partition),
                        resource.descriptor(),
                        resource_schema.as_ref(),
                        &mut observed_partition_position,
                        position.clone(),
                    )?;
                }
                let batch_output_position = batch_source_position
                    .as_ref()
                    .filter(|position| {
                        !limit_truncated || position.is_batch_slice_invariant()
                    })
                    .cloned();
                macro_rules! close_drain_epoch_at_batch_frontier {
                    () => {
                        if partition_drain_batch_frontiers_enabled
                            && !matches!(
                                batch.header.source_position.as_ref(),
                                Some(SourcePosition::FileManifest(_))
                            )
                        {
                            partition_batch_frontiers_observed = true;
                            if let Some(closed) = observe_drain_batch_frontier(
                                drain_controller.as_deref_mut(),
                                resource.descriptor(),
                                resource_schema.as_ref(),
                                &processed_observations,
                                &partition,
                                observed_partition_position.as_ref(),
                                batch.header.row_count,
                                batch.header.byte_count,
                                partition_watermark.clone(),
                                drain_clock_base.saturating_add(
                                    u64::try_from(drain_epoch_started.elapsed().as_millis())
                                        .unwrap_or(u64::MAX),
                                ),
                            )? {
                                drain_partition_resume = Some(Box::new(crate::DrainPartitionResume {
                                    partition_id: partition.partition_id.clone(),
                                    start_position: closed.partition_position,
                                }));
                                drain_epoch_closure = Some(closed.closure);
                                fully_processed = false;
                                partition_epoch_closed = true;
                                break;
                            }
                        }
                    };
                }
                if output.num_rows() == 0 {
                    phase_measurements.add(
                        RunPhase::ValidationNormalization,
                        elapsed_ns(validation_started, "validation/normalization")?,
                        validation_input_bytes,
                        0,
                    );
                    close_drain_epoch_at_batch_frontier!();
                    continue;
                }

                let evaluation_context = package_evaluation_context
                    .clone()
                    .with_source_position(batch_source_position.clone());
                let transform_memory_lease = reserve_transform_working_set(
                    memory.as_ref(),
                    &output,
                    &residual_candidates,
                )
                .await?;
                let quarantine_lease = if residual_candidates.is_empty()
                    && !program_may_quarantine(&validation_program)
                {
                    None
                } else {
                    reserve_quarantine_evidence(memory.as_ref())?
                };
                let mut quarantine_sink = QuarantinePartAccumulator::new(
                    &builder,
                    &mut quarantine_part_count,
                    quarantine_lease,
                );
                let ContractExecOutput {
                    accepted,
                    variant_values,
                    summary,
                    residual_decisions: batch_residual_decisions,
                    memory_lease,
                } = apply_contract_exec(
                    output,
                    &mut contract_evaluator,
                    &mut |record| quarantine_sink.push(record),
                    residual_candidates,
                    &ResidualBatchContext {
                        evaluation: &evaluation_context,
                        source_rows: source_rows.as_deref(),
                        cdc_operation_field: cdc_operation_field.as_deref(),
                        batch_id: &batch.header.batch_id,
                        observation_id: partition_observation_id.as_deref(),
                    },
                    if options.unfused_transform {
                        TransformKernelMode::Unfused
                    } else {
                        TransformKernelMode::Fused
                    },
                    transform_memory_lease,
                )?;
                quarantine_sink.finish()?;
                residual_decisions.push(batch_residual_decisions)?;
                merge_verdict_summary(&mut verdict_summary, summary);
                let output = apply_projection(&accepted, plan.final_projection.as_deref())?;
                if output.num_rows() == 0 {
                    phase_measurements.add(
                        RunPhase::ValidationNormalization,
                        elapsed_ns(validation_started, "validation/normalization")?,
                        validation_input_bytes,
                        0,
                    );
                    close_drain_epoch_at_batch_frontier!();
                    continue;
                }
                let validation_output_bytes =
                    u64::try_from(output.get_array_memory_size())
                        .map_err(|error| CdfError::internal(error.to_string()))?;
                if apply_package_dedup {
                    let prepared_output = prepare_output_batch(
                        &validation_program,
                        effective_schema_evidence.is_some(),
                        PreparedOutputBatch {
                            output,
                            variant_values,
                            output_position: batch_output_position.clone(),
                            memory_lease,
                        },
                        &mut output_schema,
                        runtime_output_schema.as_ref(),
                        &mut phase_measurements,
                    )?;
                    let PreparedKernelOutput {
                        output,
                        memory_lease,
                    } = prepared_output;
                    phase_measurements.add(
                        RunPhase::ValidationNormalization,
                        elapsed_ns(validation_started, "validation/normalization")?,
                        validation_input_bytes,
                        validation_output_bytes,
                    );
                    if let Some((rule, index, payload)) = &mut external_dedup {
                        index.push_owned_keys(encode_package_dedup_keys(
                            &validation_program,
                            rule,
                            &output,
                        )?)?;
                        payload.push(partition_ordinal, batch_output_position, &output)?;
                    } else {
                        pending_dedup_batches.push(PendingDedupBatch {
                            partition_ordinal,
                            output,
                            output_position: batch_output_position,
                            _memory_lease: memory_lease,
                        });
                    }
                    close_drain_epoch_at_batch_frontier!();
                    continue;
                }
                phase_measurements.add(
                    RunPhase::ValidationNormalization,
                    elapsed_ns(validation_started, "validation/normalization")?,
                    validation_input_bytes,
                    validation_output_bytes,
                );
                write_output_batch(
                    &validation_program,
                    effective_schema_evidence.is_some(),
                    PreparedOutputBatch {
                        output,
                        variant_values,
                        output_position: batch_output_position,
                        memory_lease,
                    },
                    &mut segment_assembler,
                    &mut OutputWriteState {
                        profile: &mut profile,
                        lineage: &mut lineage,
                        segments: &mut segments,
                        segment_positions: &mut segment_positions,
                        output_schema: &mut output_schema,
                        expected_schema: runtime_output_schema.as_ref(),
                        phase_measurements: &mut phase_measurements,
                        memory: memory.as_ref(),
                        statistics: statistics_profile_state(
                            &statistics_memory,
                            &mut statistics_memory_lease,
                            &mut statistics_profile,
                            &statistics_profile_schema_hash,
                            &mut statistics_segment_ordinal,
                        ),
                    },
                    &mut SegmentOutputSink {
                        builder: &builder,
                        queue: &mut segment_queue,
                        durable: &mut durable_segment_observer,
                    },
                )?;
                close_drain_epoch_at_batch_frontier!();
            }
            persist_canonical_segments(
                segment_assembler.finish()?,
                &mut OutputWriteState {
                    profile: &mut profile,
                    lineage: &mut lineage,
                    segments: &mut segments,
                    segment_positions: &mut segment_positions,
                    output_schema: &mut output_schema,
                    expected_schema: runtime_output_schema.as_ref(),
                    phase_measurements: &mut phase_measurements,
                    memory: memory.as_ref(),
                    statistics: statistics_profile_state(
                        &statistics_memory,
                        &mut statistics_memory_lease,
                        &mut statistics_profile,
                        &statistics_profile_schema_hash,
                        &mut statistics_segment_ordinal,
                    ),
                },
                &mut SegmentOutputSink {
                    builder: &builder,
                    queue: &mut segment_queue,
                    durable: &mut durable_segment_observer,
                },
            )?;
            let completion = if fully_processed {
                let (_, completion) = opened_partition.finish()?;
                completion
            } else {
                opened_partition.terminate_partial().await?;
                None
            };
            if let Some(source_io) = completion
                .as_ref()
                .and_then(cdf_kernel::PartitionCompletion::source_io)
            {
                phase_measurements.add_operations_with_context(
                    RunPhase::SourceRead,
                    source_io
                        .mode
                        .map(|mode| RunPhaseContext::SourceRead { mode }),
                    source_io.duration_ns,
                    source_io.physical_bytes,
                    source_io.useful_bytes,
                    source_io.requests,
                );
            }
            let completion_attestation = completion
                .and_then(cdf_kernel::PartitionCompletion::into_attestation);
            Ok::<_, CdfError>((
                fully_processed,
                observed_partition_position,
                dynamic_quarantine,
                partition_observation_id,
                partition_source_row_ordinal,
                completion_attestation,
                partition_input_batch_count,
                partition_input_bytes,
                partition_watermark,
                partition_epoch_closed,
                partition_batch_frontiers_observed,
            ))
        }
        .instrument(partition_span)
        .await;
        let (
            fully_processed,
            observed_partition_position,
            dynamic_quarantine,
            partition_observation_id,
            partition_observed_rows,
            completion_attestation,
            partition_input_batch_count,
            partition_input_bytes,
            partition_watermark,
            partition_epoch_closed,
            partition_batch_frontiers_observed,
        ) = partition_result?;
        checkpoint_eligible &= fully_processed || partition_epoch_closed;
        let partial_retry_attestation = if open_evidence.retry_pre_attestation.is_some()
            && completion_attestation.is_none()
        {
            Some(
                attest_partition_with_terminal_join(resource, &partition, &run_cancellation)
                    .await?
                    .ok_or_else(|| {
                        CdfError::data(format!(
                            "retried partial partition `{}` has no post-consumption reattestation; re-plan before retrying",
                            partition.partition_id
                        ))
                    })?,
            )
        } else {
            None
        };
        if let Some(expected) = &open_evidence.retry_pre_attestation {
            let observed = completion_attestation
                .as_ref()
                .or(partial_retry_attestation.as_ref())
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "retried partition `{}` has no terminal reattestation; re-plan before retrying",
                        partition.partition_id
                    ))
                })?;
            if !observed.is_monotonic_refinement_of(expected) {
                return Err(CdfError::data(format!(
                    "retried partition `{}` changed source generation or schema between reopen and EOF; re-plan before retrying",
                    partition.partition_id
                )));
            }
        }
        if let Some(attestation) = &completion_attestation {
            completion_positions.push((
                partition_ordinal,
                partition.clone(),
                attestation.processed_position().clone(),
            ));
        }
        if let Some((mut quarantine, physical_observation)) = dynamic_quarantine {
            let observation_id = quarantine.observation_id().to_owned();
            let fallback_attestation = if observed_partition_position.is_none()
                && completion_attestation.is_none()
            {
                attest_partition_with_terminal_join(resource, &partition, &run_cancellation)
                    .await?
            } else {
                None
            };
            let terminal_position = completion_attestation
                .as_ref()
                .map(|attestation| attestation.processed_position().clone())
                .or_else(|| {
                    fallback_attestation.map(PartitionAttestation::into_processed_position)
                });
            let source_position = aggregate_processed_partition_positions(
                &observation_id,
                observed_partition_position.as_ref(),
                terminal_position,
            )?;
            processed_observations.push(ProcessedObservationPosition::new(
                observation_id,
                ProcessedObservationOutcome::Quarantined,
                source_position.clone(),
            )?);
            quarantine.bind_source_position(source_position)?;
            record_schema_quarantine(
                &mut terminal_quarantines,
                &mut quarantine_physical_observations,
                quarantine,
                physical_observation,
            )?;
        } else if let Some(observation_id) = partition
                .metadata
                .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
                .cloned()
                .or(partition_observation_id)
        {
            let fallback_attestation = if observed_partition_position.is_none()
                && completion_attestation.is_none()
            {
                match observation_attestations.get(&observation_id) {
                    Some(attestation) => Some(attestation.clone()),
                    None => {
                        let attestation = attest_partition_with_terminal_join(
                            resource,
                            &partition,
                            &run_cancellation,
                        )
                        .await?;
                        if let Some(attestation) = &attestation {
                            observation_attestations
                                .insert(observation_id.clone(), attestation.clone());
                        }
                        attestation
                    }
                }
            } else {
                None
            };
            if let Some(expected) = partition_schema_evidence
                && !stream_admission_evidence.contains_key(&expected.observation_id)
            {
                let attestation = fallback_attestation.as_ref().ok_or_else(|| {
                    CdfError::data(format!(
                        "schema observation {:?} produced no batches and has no execution-time attestation",
                        expected.observation_id
                    ))
                })?;
                if attestation.physical_schema_hash() != Some(&expected.physical_schema_hash) {
                    return Err(CdfError::data(format!(
                        "schema observation {:?} produced no batches and changed physical schema between planning and execution; expected {}, attested {:?}; re-plan before retrying",
                        expected.observation_id,
                        expected.physical_schema_hash,
                        attestation.physical_schema_hash()
                    )));
                }
                record_observation_schema_coercion(
                    &mut stream_admission_evidence,
                    &mut stream_physical_observation_catalog,
                    &expected.observation_id,
                    preobserved_physical_observation(
                        effective_schema_evidence,
                        Some(expected),
                    )?
                    .cloned()
                    .ok_or_else(|| {
                        CdfError::internal(
                            "preobserved empty partition omitted its physical observation",
                        )
                    })?,
                    expected.coercion_plan.clone(),
                )?;
            }
            let fallback_position = completion_attestation
                .map(PartitionAttestation::into_processed_position)
                .or_else(|| {
                    fallback_attestation.map(PartitionAttestation::into_processed_position)
                });
            let source_position = if observed_partition_position.is_none() && fallback_position.is_none() {
                None
            } else {
                Some(aggregate_processed_partition_positions(
                    &observation_id,
                    observed_partition_position.as_ref(),
                    fallback_position,
                )?)
            };
            lineage.input_observations.push(LineageInputObservation {
                observation_id: observation_id.clone(),
                partition_id: partition.partition_id.clone(),
                observed_rows: partition_observed_rows,
                output_position: source_position.clone(),
            });
            if let Some(source_position) = source_position {
                let evidence = stream_admission_evidence
                    .get_mut(&observation_id)
                    .ok_or_else(|| {
                        CdfError::internal(format!(
                            "admitted observation {observation_id:?} omitted stream-admission evidence"
                        ))
                    })?;
                if fully_processed || partition_epoch_closed {
                    evidence.bind_source_position(source_position.clone())?;
                    processed_observations.push(ProcessedObservationPosition::new(
                        observation_id,
                        ProcessedObservationOutcome::Admitted,
                        source_position,
                    )?);
                } else {
                    evidence.bind_partial_attempt(
                        source_position,
                        partition_observed_rows,
                        cdf_kernel::partition_source_identity_binding(&partition)?,
                    )?;
                }
            } else {
                let evidence = stream_admission_evidence
                    .get_mut(&observation_id)
                    .ok_or_else(|| {
                        CdfError::internal(format!(
                            "admitted observation {observation_id:?} omitted stream-admission evidence"
                        ))
                    })?;
                if fully_processed {
                    evidence.bind_unpositioned_completion(
                        cdf_kernel::partition_source_identity_binding(&partition)?,
                    )?;
                } else {
                    return Err(CdfError::data(format!(
                        "partial schema observation {observation_id:?} requires exact generation and slice-position authority"
                    )));
                }
            }
        }
        if fully_processed {
            consumed_partition_count = consumed_partition_count.saturating_add(1);
        }
        if let Some(controller) = drain_controller.as_deref_mut() {
            if partition_epoch_closed {
                break;
            }
            let frontier = drain_resource_frontier(
                resource.descriptor(),
                resource_schema.as_ref(),
                controller.committed_frontier(),
                &processed_observations,
            )?;
            let decision = controller.observe_safe_frontier(
                cdf_runtime::DrainSafeFrontierObservation {
                    frontier,
                    carryover: None,
                    admitted_batches: if partition_batch_frontiers_observed {
                        0
                    } else {
                        partition_input_batch_count
                    },
                    admitted_rows: if partition_batch_frontiers_observed {
                        0
                    } else {
                        partition_observed_rows
                    },
                    admitted_bytes: if partition_batch_frontiers_observed {
                        0
                    } else {
                        partition_input_bytes
                    },
                    admitted_positions: u64::from(!partition_batch_frontiers_observed),
                    global_watermark: partition_watermark,
                    source_exhausted: consumed_partition_count == frontier_partition_count,
                    monotonic_milliseconds: drain_clock_base.saturating_add(
                        u64::try_from(drain_epoch_started.elapsed().as_millis())
                            .unwrap_or(u64::MAX),
                    ),
                    observed_at_unix_milliseconds: current_observed_at_u64_ms()?,
                },
            )?;
            match decision {
                cdf_runtime::DrainEpochDecision::Continue => {}
                cdf_runtime::DrainEpochDecision::Close(closure) => {
                    drain_epoch_closure = Some(*closure);
                    break;
                }
                cdf_runtime::DrainEpochDecision::FinishedNoOp => {
                    return Err(CdfError::internal(
                        "drain controller classified a processed source position as an empty epoch",
                    ));
                }
            }
        }
    }

    if apply_package_dedup {
        apply_dedup_and_write_pending_batches(
            &builder,
            &validation_program,
            pending_dedup_batches,
            external_dedup,
            &segmentation_policy,
            &mut OutputWriteState {
                profile: &mut profile,
                lineage: &mut lineage,
                segments: &mut segments,
                segment_positions: &mut segment_positions,
                output_schema: &mut output_schema,
                expected_schema: runtime_output_schema.as_ref(),
                phase_measurements: &mut phase_measurements,
                memory: memory.as_ref(),
                statistics: statistics_profile_state(
                    &statistics_memory,
                    &mut statistics_memory_lease,
                    &mut statistics_profile,
                    &statistics_profile_schema_hash,
                    &mut statistics_segment_ordinal,
                ),
            },
            &mut SegmentOutputSink {
                builder: &builder,
                queue: &mut segment_queue,
                durable: &mut durable_segment_observer,
            },
        )?;
    }

    segment_queue.finish(
        &builder,
        &mut OutputWriteState {
            profile: &mut profile,
            lineage: &mut lineage,
            segments: &mut segments,
            segment_positions: &mut segment_positions,
            output_schema: &mut output_schema,
            expected_schema: runtime_output_schema.as_ref(),
            phase_measurements: &mut phase_measurements,
            memory: memory.as_ref(),
            statistics: statistics_profile_state(
                &statistics_memory,
                &mut statistics_memory_lease,
                &mut statistics_profile,
                &statistics_profile_schema_hash,
                &mut statistics_segment_ordinal,
            ),
        },
        &mut durable_segment_observer,
    )?;
    for (partition_ordinal, partition, completion) in &completion_positions {
        enrich_segment_positions_with_completion(
            &mut segment_positions,
            *partition_ordinal,
            partition,
            completion,
        )?;
    }
    Ok(())
    }
    .await;
    if let Err(mut error) = segment_result {
        run_cancellation.cancel();
        if let Err(cleanup_error) = source_frontier.terminate_and_join().await {
            error.message = format!(
                "{}; source frontier termination also failed: {}",
                error.message, cleanup_error.message
            );
        }
        return match segment_queue.abort_and_cleanup() {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(with_cleanup_failure(
                error,
                "segment encode cleanup",
                cleanup_error,
            )),
        };
    }
    if drain_epoch_closure.is_some() && consumed_partition_count < frontier_partition_count {
        source_frontier.terminate_and_join().await?;
    }
    let source_frontier_report = source_frontier.report();

    drop(contract_evaluator);
    builder.write_json_artifact("plan/validation-program.json", &validation_program)?;
    if let Some(coercion) = &validation_program.schema_coercion {
        builder.write_json_artifact("schema/coercion-plan.json", coercion)?;
    }
    let lineage_observation_ids = lineage
        .input_observations
        .iter()
        .map(|observation| observation.observation_id.as_str())
        .collect::<BTreeSet<_>>();
    if lineage_observation_ids.len() != lineage.input_observations.len() {
        return Err(CdfError::data(
            "execution lineage contains a schema observation identity assigned to more than one partition",
        ));
    }
    let stream_observation_ids = stream_admission_evidence
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if lineage_observation_ids != stream_observation_ids {
        return Err(CdfError::data(
            "execution lineage does not exactly bind every admitted stream observation to one partition",
        ));
    }
    let admitted_observations = processed_observations
        .iter()
        .filter(|observation| observation.outcome == ProcessedObservationOutcome::Admitted)
        .map(|observation| observation.observation_id.as_str())
        .collect::<BTreeSet<_>>();
    let quarantined_observations = processed_observations
        .iter()
        .filter(|observation| observation.outcome == ProcessedObservationOutcome::Quarantined)
        .map(|observation| observation.observation_id.as_str())
        .collect::<BTreeSet<_>>();
    if admitted_observations
        != stream_admission_evidence
            .values()
            .filter(|observation| {
                matches!(
                    observation.completion,
                    crate::StreamAdmissionCompletion::Complete { .. }
                )
            })
            .map(|observation| observation.observation_id.as_str())
            .collect::<BTreeSet<_>>()
    {
        return Err(CdfError::data(
            "processed admitted observations do not exactly match stream-admission evidence",
        ));
    }
    if quarantined_observations
        != terminal_quarantines
            .iter()
            .map(TerminalSchemaObservationQuarantine::observation_id)
            .collect::<BTreeSet<_>>()
    {
        return Err(CdfError::data(
            "processed quarantined observations do not exactly match schema-quarantine evidence",
        ));
    }
    builder.write_json_artifact(
        "schema/stream-admission-evidence.json",
        &CompiledStreamAdmissionEvidence::new(
            &plan.compiled_schema_admission,
            stream_physical_observation_catalog,
            stream_admission_evidence.into_values().collect(),
        )?,
    )?;
    if !terminal_quarantines.is_empty() {
        let mut quarantine_physical_observation_catalog = BTreeMap::new();
        let quarantine_evidence = terminal_quarantines
            .iter()
            .map(|quarantine| {
                let physical = quarantine_physical_observations
                    .remove(quarantine.observation_id())
                    .ok_or_else(|| {
                        CdfError::internal(format!(
                            "schema quarantine {:?} omitted physical-observation evidence",
                            quarantine.observation_id()
                        ))
                    })?;
                let physical_hash = physical.identity_hash()?;
                if physical_hash != *quarantine.physical_schema_hash() {
                    return Err(CdfError::internal(format!(
                        "schema quarantine {:?} physical evidence does not match its recorded hash",
                        quarantine.observation_id()
                    )));
                }
                quarantine_physical_observation_catalog.insert(physical_hash.to_string(), physical);
                SchemaQuarantineObservationEvidence::new(quarantine, physical_hash)
            })
            .collect::<Result<Vec<_>>>()?;
        if !quarantine_physical_observations.is_empty() {
            return Err(CdfError::internal(
                "physical-observation evidence exists without a schema quarantine",
            ));
        }
        builder
            .write_json_artifact("quarantine/schema-observations.json", &terminal_quarantines)?;
        builder.write_json_artifact(
            "quarantine/schema-admission-evidence.json",
            &CompiledSchemaQuarantineEvidence::new(
                &plan.compiled_schema_admission,
                quarantine_physical_observation_catalog,
                quarantine_evidence,
            )?,
        )?;
    }
    builder.write_json_artifact(
        "schema/output.json",
        &output_schema.expect("compiled output schema is always present"),
    )?;
    builder.write_runtime_arrow_schema(runtime_output_schema.as_ref())?;
    let mut residual_decisions = residual_decisions.finish()?;
    if let Some(evolution) = contract_evolution_artifact_metadata(
        &validation_program,
        schema_authority.baseline_schema_hash.clone(),
        schema_authority.effective_schema_hash.clone(),
        residual_decisions.is_some(),
    ) {
        write_contract_evolution_stream(&builder, &evolution, residual_decisions.as_mut())?;
    }
    if let Some(mut statistics_profile) = statistics_profile {
        statistics_profile.write_stats(
            cdf_package::StatisticsProfileGrain::Package,
            0,
            &plan.package_id,
            &statistics_profile_schema_hash,
            &profile.statistics,
        )?;
        statistics_profile.finish()?;
    }
    if verdict_summary.violation_count > 0 || verdict_summary.quarantine_candidate_count > 0 {
        builder.write_stats_artifact(
            "verdict-summary.json",
            &cdf_package::canonical_json_bytes(&verdict_summary)?,
        )?;
    }
    if verdict_summary.quarantine_candidate_count > 0 {
        write_quarantine_summary(&builder, &verdict_summary, quarantine_part_count)?;
    }
    builder.write_lineage_artifact(
        "lineage.json",
        &cdf_package::canonical_json_bytes(&lineage)?,
    )?;
    let execution_evidence = EngineExecutionEvidence::new(
        processed_observations,
        options.retry_journal.snapshot()?,
        plan.partition_schedule.as_ref(),
        checkpoint_eligible,
    )?;
    if let Some(closure) = &drain_epoch_closure {
        builder.write_json_artifact("plan/epoch-frontier.json", &closure.frontier)?;
        builder.write_json_artifact("plan/epoch-closure.json", &closure.evidence)?;
    }
    if let Some(stream_finalize) = stream_finalize {
        stream_finalize()?;
    }
    builder.update_status(PackageStatus::Validated)?;
    if let Some(pre_finalize) = pre_finalize {
        pre_finalize(
            &builder,
            EnginePackageDraft {
                segments: &segments,
                profile: &profile,
                lineage: &lineage,
                segment_positions: &segment_positions,
                execution_evidence: &execution_evidence,
            },
        )?;
    }
    let finalize_started = phase_measurements.start();
    let (manifest, verification) = builder.finish_verified()?;
    phase_measurements.add(
        RunPhase::PackageFinalize,
        elapsed_ns(finalize_started, "package finalize")?,
        profile.output_bytes,
        manifest
            .identity
            .files
            .iter()
            .map(|file| file.byte_count)
            .sum(),
    );

    Ok(EngineRunOutputWithSegmentPositions {
        output: EngineRunOutput {
            manifest,
            verification,
            segments,
            profile,
            lineage,
            terminal_schema_quarantines: terminal_quarantines.clone(),
        },
        segment_positions,
        phase_metrics: phase_measurements.into_metrics(),
        source_frontier: source_frontier_report,
        drain_epoch: drain_epoch_closure.map(|closure| EngineDrainEpoch {
            closure,
            consumed_partition_count,
            resume_partition: drain_partition_resume,
        }),
        execution_evidence,
    })
}

fn apply_dedup_and_write_pending_batches(
    builder: &PackageBuilder,
    program: &ValidationProgram,
    pending: Vec<PendingDedupBatch>,
    external: Option<(
        cdf_contract::PackageDedupRuleSpec,
        crate::dedup_spill::ExternalDedupIndex,
        crate::dedup_spill::DedupPayloadSpool,
    )>,
    segmentation_policy: &crate::CanonicalSegmentationPolicy,
    state: &mut OutputWriteState<'_>,
    sink: &mut SegmentOutputSink<'_, '_>,
) -> Result<()> {
    let validation_started = state.phase_measurements.start();
    let pending_input_bytes = pending
        .iter()
        .map(|batch| batch.output.get_array_memory_size() as u64)
        .sum();
    if let Some((rule, index, payload)) = external {
        let validation_input_bytes = payload.input_bytes;
        let mut decisions = index.finish(rule.keep.clone())?;
        if let Some(memory) = state.memory {
            memory.record_event(cdf_memory::MemoryEvent::Spill {
                bytes: decisions.summary.spill_bytes,
            });
        }
        let mut payload = payload.finish()?;
        let mut assembler = None::<(u32, crate::CanonicalSegmentAssembler)>;
        let mut provenance = DedupProvenanceSink::new();
        let mut expected_ordinal = 0_u64;
        while let Some(payload_batch) = match payload.as_mut() {
            Some(payload) => payload.next()?,
            None => None,
        } {
            let mut retained = Vec::with_capacity(payload_batch.batch.num_rows());
            for _ in 0..payload_batch.batch.num_rows() {
                let decision = decisions.next()?.ok_or_else(|| {
                    CdfError::internal("external dedup decision stream ended early")
                })?;
                if decision.ordinal != expected_ordinal {
                    return Err(CdfError::internal(
                        "external dedup decision stream is not canonically ordered",
                    ));
                }
                let keep = decision.ordinal == decision.kept_ordinal;
                retained.push(keep);
                if !keep {
                    provenance.push(builder, decision.ordinal, decision.kept_ordinal)?;
                }
                expected_ordinal += 1;
            }
            let output = filter_record_batch(&payload_batch.batch, &BooleanArray::from(retained))
                .map_err(CdfError::from)?;
            if output.num_rows() == 0 {
                continue;
            }
            if assembler.as_ref().map(|(ordinal, _)| *ordinal)
                != Some(payload_batch.partition_ordinal)
            {
                if let Some((_, mut previous)) = assembler.take() {
                    persist_canonical_segments(previous.finish()?, state, sink)?;
                }
                assembler = Some((
                    payload_batch.partition_ordinal,
                    crate::CanonicalSegmentAssembler::new(
                        segmentation_policy.clone(),
                        payload_batch.partition_ordinal,
                    )?,
                ));
            }
            write_normalized_output_batch(
                PreparedKernelOutput {
                    output,
                    memory_lease: None,
                },
                payload_batch.output_position,
                &mut assembler.as_mut().expect("assembler initialized").1,
                state,
                sink,
            )?;
        }
        if decisions.next()?.is_some() {
            return Err(CdfError::internal(
                "external dedup decision stream contains excess rows",
            ));
        }
        if let Some((_, mut assembler)) = assembler {
            persist_canonical_segments(assembler.finish()?, state, sink)?;
        }
        let shards = provenance.finish(builder)?;
        write_dedup_summary_v2(
            builder,
            cdf_contract::DedupSummary {
                rule_id: rule.rule_id,
                keys: rule.keys,
                keep: rule.keep,
                input_rows: decisions.summary.input_rows,
                output_rows: decisions.summary.output_rows,
                duplicate_key_count: decisions.summary.duplicate_key_count,
                dropped_row_count: decisions.summary.dropped_row_count,
                dropped_rows: Vec::new(),
            },
            shards,
        )?;
        state.phase_measurements.add(
            RunPhase::ValidationNormalization,
            elapsed_ns(validation_started, "package dedup")?,
            validation_input_bytes,
            validation_input_bytes,
        );
        return Ok(());
    }
    let validation_input_bytes = pending_input_bytes;
    let accepted = pending
        .iter()
        .map(|batch| batch.output.clone())
        .collect::<Vec<_>>();
    let dedup = evaluate_package_order_dedup(program, &accepted)?
        .ok_or_else(|| CdfError::internal("package dedup was selected without an evaluation"))?;
    let mut provenance = DedupProvenanceSink::new();
    for dropped in &dedup.summary.dropped_rows {
        provenance.push(
            builder,
            dropped.package_row_ordinal,
            dropped.kept_package_row_ordinal,
        )?;
    }
    let shards = provenance.finish(builder)?;
    write_dedup_summary_v2(builder, dedup.summary.clone(), shards)?;
    state.phase_measurements.add(
        RunPhase::ValidationNormalization,
        elapsed_ns(validation_started, "package dedup")?,
        validation_input_bytes,
        validation_input_bytes,
    );

    let mut assembler = None::<(u32, crate::CanonicalSegmentAssembler)>;
    for (pending, retained_rows) in pending.into_iter().zip(dedup.retained_rows) {
        let output =
            filter_record_batch(&pending.output, &retained_rows).map_err(CdfError::from)?;
        if output.num_rows() == 0 {
            continue;
        }
        if assembler.as_ref().map(|(ordinal, _)| *ordinal) != Some(pending.partition_ordinal) {
            if let Some((_, mut previous)) = assembler.take() {
                persist_canonical_segments(previous.finish()?, state, sink)?;
            }
            assembler = Some((
                pending.partition_ordinal,
                crate::CanonicalSegmentAssembler::new(
                    segmentation_policy.clone(),
                    pending.partition_ordinal,
                )?,
            ));
        }
        write_normalized_output_batch(
            PreparedKernelOutput {
                output,
                memory_lease: None,
            },
            pending.output_position,
            &mut assembler.as_mut().expect("assembler initialized").1,
            state,
            sink,
        )?;
    }
    if let Some((_, mut assembler)) = assembler {
        persist_canonical_segments(assembler.finish()?, state, sink)?;
    }
    Ok(())
}

fn write_dedup_summary_v2(
    builder: &PackageBuilder,
    summary: cdf_contract::DedupSummary,
    shards: Vec<DedupProvenanceShard>,
) -> Result<()> {
    builder.write_dedup_summary(&DedupSummaryV2 {
        version: 2,
        rule_id: summary.rule_id,
        keys: summary.keys,
        keep: summary.keep,
        input_rows: summary.input_rows,
        output_rows: summary.output_rows,
        duplicate_key_count: summary.duplicate_key_count,
        dropped_row_count: summary.dropped_row_count,
        provenance_format: "parquet".to_owned(),
        provenance_version: 1,
        provenance_shard_row_target: DEDUP_PROVENANCE_SHARD_ROWS as u64,
        shard_count: shards.len() as u64,
        shards,
    })?;
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

fn enrich_segment_positions_with_completion(
    positions: &mut [EngineSegmentPosition],
    partition_ordinal: u32,
    partition: &PartitionPlan,
    completion: &SourcePosition,
) -> Result<()> {
    for position in positions
        .iter_mut()
        .filter(|position| position.partition_ordinal == partition_ordinal)
    {
        let Some(existing) = &mut position.output_position else {
            return Err(CdfError::data(format!(
                "segment {} for partition `{}` omitted source-position evidence required by terminal attestation",
                position.segment_id.as_str(),
                partition.partition_id.as_str()
            )));
        };
        *existing = merge_terminal_position_evidence(existing, completion)?;
    }
    // A fully consumed partition may legitimately produce no output segment after filtering,
    // quarantine, or package-wide dedup. Its processed/checkpoint evidence still retains the
    // terminal content identity; there is simply no segment position to enrich.
    Ok(())
}

fn aggregate_processed_partition_positions(
    observation_id: &str,
    observed: Option<&SourcePosition>,
    attested: Option<SourcePosition>,
) -> Result<SourcePosition> {
    let observed = observed.cloned();
    match (observed, attested) {
        (Some(observed), Some(attested)) => {
            merge_terminal_position_evidence(&observed, &attested).map_err(|error| {
                CdfError::data(format!(
                    "processed observation {observation_id:?} has invalid terminal source-position evidence: {error}"
                ))
            })
        }
        (Some(observed), None) => Ok(observed),
        (None, Some(attested)) => Ok(attested),
        (None, None) => Err(CdfError::data(format!(
            "processed observation {observation_id:?} completed without source-position evidence"
        ))),
    }
}

fn accumulate_processed_partition_position(
    observation_id: &str,
    descriptor: &cdf_kernel::ResourceDescriptor,
    schema: &Schema,
    accumulated: &mut Option<SourcePosition>,
    observed: SourcePosition,
) -> Result<()> {
    observed.validate()?;
    let Some(previous) = accumulated.as_ref() else {
        *accumulated = Some(observed);
        return Ok(());
    };
    *accumulated = Some(
        aggregate_resource_output_position(
            descriptor,
            schema,
            None,
            &[previous.clone(), observed],
        )
        .map_err(|error| {
            CdfError::data(format!(
                "processed observation {observation_id:?} has invalid incremental source-position evidence: {error}"
            ))
        })?,
    );
    Ok(())
}

fn drain_resource_frontier(
    descriptor: &cdf_kernel::ResourceDescriptor,
    schema: &Schema,
    committed_frontier: Option<&SourcePosition>,
    processed: &[ProcessedObservationPosition],
) -> Result<SourcePosition> {
    let positions = processed
        .iter()
        .map(|observation| observation.source_position.clone())
        .collect::<Vec<_>>();
    aggregate_resource_closed_output_position(descriptor, schema, committed_frontier, &positions)
        .map_err(|error| {
            CdfError::data(format!(
                "drain epoch cannot form a canonical safe source frontier: {error}"
            ))
        })
}

#[allow(clippy::too_many_arguments)]
fn observe_drain_batch_frontier(
    controller: Option<&mut cdf_runtime::DrainEpochController>,
    descriptor: &cdf_kernel::ResourceDescriptor,
    schema: &Schema,
    processed: &[ProcessedObservationPosition],
    partition: &PartitionPlan,
    observed_partition_position: Option<&SourcePosition>,
    admitted_rows: u64,
    admitted_bytes: u64,
    global_watermark: Option<WatermarkClaim>,
    monotonic_milliseconds: u64,
) -> Result<Option<DrainBatchFrontierClosure>> {
    let Some(controller) = controller else {
        return Ok(None);
    };
    let observation_id = cdf_kernel::partition_schema_observation_id(partition);
    let partition_position =
        aggregate_processed_partition_positions(observation_id, observed_partition_position, None)?;
    let mut positions = processed
        .iter()
        .map(|observation| observation.source_position.clone())
        .collect::<Vec<_>>();
    positions.push(partition_position.clone());
    let frontier = aggregate_resource_closed_output_position(
        descriptor,
        schema,
        controller.committed_frontier(),
        &positions,
    )
    .map_err(|error| {
        CdfError::data(format!(
            "drain batch cannot form a canonical safe source frontier: {error}"
        ))
    })?;
    match controller.observe_safe_frontier(cdf_runtime::DrainSafeFrontierObservation {
        frontier,
        carryover: None,
        admitted_batches: 1,
        admitted_rows,
        admitted_bytes,
        admitted_positions: 1,
        global_watermark,
        source_exhausted: false,
        monotonic_milliseconds,
        observed_at_unix_milliseconds: current_observed_at_u64_ms()?,
    })? {
        cdf_runtime::DrainEpochDecision::Continue => Ok(None),
        cdf_runtime::DrainEpochDecision::Close(closure) => Ok(Some(DrainBatchFrontierClosure {
            closure: *closure,
            partition_position,
        })),
        cdf_runtime::DrainEpochDecision::FinishedNoOp => Err(CdfError::internal(
            "drain controller classified a processed batch position as an empty epoch",
        )),
    }
}

struct DrainBatchFrontierClosure {
    closure: cdf_runtime::DrainEpochClosure,
    partition_position: SourcePosition,
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

fn quarantine_record_from_pre_contract(fact: &PreContractQuarantineFact) -> QuarantineRecord {
    QuarantineRecord {
        source_row_ordinal: fact.source_row_ordinal,
        rule_id: fact.rule_id.clone(),
        error_code: fact.error_code.clone(),
        source_position: fact.source_position.clone(),
        observed_value_redacted: pre_contract_observed_value(&fact.observed_value_redacted),
    }
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

fn write_quarantine_summary(
    builder: &PackageBuilder,
    summary: &VerdictSummary,
    artifact_count: usize,
) -> Result<()> {
    let artifact_count = u64::try_from(artifact_count)
        .map_err(|_| CdfError::data("quarantine artifact count exceeds u64"))?;
    let mut artifact =
        builder.begin_streaming_identity_artifact("stats/quarantine-summary.json")?;
    artifact.write_all(b"{\"artifact_count\":")?;
    artifact.write_json(&artifact_count)?;
    artifact.write_all(b",\"artifacts\":[")?;
    for part in 1..=artifact_count {
        if part > 1 {
            artifact.write_all(b",")?;
        }
        artifact.write_json(&format!("quarantine/part-{part:06}.parquet"))?;
    }
    artifact.write_all(b"],\"quarantine_candidate_count\":")?;
    artifact.write_json(&summary.quarantine_candidate_count)?;
    artifact.write_all(b",\"quarantined_rows\":")?;
    artifact.write_json(&summary.quarantined_rows)?;
    artifact.write_all(b"}")?;
    artifact.finish()?;
    Ok(())
}

fn write_contract_evolution_stream(
    builder: &PackageBuilder,
    evolution: &ContractEvolutionArtifact,
    mut decisions: Option<&mut ResidualDecisionOutput>,
) -> Result<()> {
    let mut artifact =
        builder.begin_streaming_identity_artifact("schema/contract-evolution.json")?;
    artifact.write_all(b"{\"baseline_schema_hash\":")?;
    artifact.write_json(&evolution.baseline_schema_hash)?;
    artifact.write_all(b",\"effective_schema_hash\":")?;
    artifact.write_json(&evolution.effective_schema_hash)?;
    artifact.write_all(b",\"implicit_promotion_count\":")?;
    artifact.write_json(&evolution.implicit_promotion_count)?;
    artifact.write_all(b",\"promotion_events\":")?;
    artifact.write_json(&evolution.promotion_events)?;
    artifact.write_all(b",\"residual_capture\":")?;
    artifact.write_json(&evolution.residual_capture)?;
    artifact.write_all(b",\"residual_decisions\":[")?;
    let mut first = true;
    if let Some(decisions) = decisions.as_mut() {
        while let Some(decision) = decisions.next()? {
            if !first {
                artifact.write_all(b",")?;
            }
            artifact.write_json(&decision)?;
            first = false;
        }
    }
    artifact.write_all(b"],\"variant_capture\":")?;
    artifact.write_json(&evolution.variant_capture)?;
    artifact.write_all(b",\"version\":")?;
    artifact.write_json(&evolution.version)?;
    artifact.write_all(b"}")?;
    artifact.finish()?;
    Ok(())
}

fn write_output_batch(
    program: &ValidationProgram,
    canonicalize_observed_schema: bool,
    prepared: PreparedOutputBatch,
    assembler: &mut crate::CanonicalSegmentAssembler,
    state: &mut OutputWriteState<'_>,
    sink: &mut SegmentOutputSink<'_, '_>,
) -> Result<()> {
    let output_position = prepared.output_position.clone();
    let prepared = prepare_output_batch(
        program,
        canonicalize_observed_schema,
        prepared,
        state.output_schema,
        state.expected_schema,
        state.phase_measurements,
    )?;
    write_normalized_output_batch(prepared, output_position, assembler, state, sink)
}

fn prepare_output_batch(
    program: &ValidationProgram,
    canonicalize_observed_schema: bool,
    prepared: PreparedOutputBatch,
    output_schema: &mut Option<SchemaArtifact>,
    expected_schema: &Schema,
    phase_measurements: &mut PhaseMeasurements,
) -> Result<PreparedKernelOutput> {
    let PreparedOutputBatch {
        output,
        variant_values,
        output_position: _,
        memory_lease,
    } = prepared;
    let normalization_started = phase_measurements.start();
    let normalization_input_bytes = output.get_array_memory_size() as u64;
    let output = append_residual_variant(output, program, variant_values)?;
    let output = normalize_record_batch_after_expressions(output, program)?;
    let output = if canonicalize_observed_schema {
        canonicalize_effective_output_schema(output)?
    } else {
        output
    };
    let output = conform_to_compiled_output_schema(output, expected_schema)?;
    let normalization_output_bytes = output.get_array_memory_size() as u64;
    phase_measurements.add(
        RunPhase::ValidationNormalization,
        elapsed_ns(normalization_started, "output normalization")?,
        normalization_input_bytes,
        normalization_output_bytes,
    );
    let actual_schema = schema_artifact(output.schema().as_ref());
    if let Some(expected_schema) = output_schema.as_ref()
        && expected_schema != &actual_schema
    {
        return Err(CdfError::data(format!(
            "emitted batch schema does not match the compiled output schema authority: expected {expected_schema:?}, observed {actual_schema:?}"
        )));
    }
    *output_schema = Some(actual_schema);
    if let Some(lease) = &memory_lease {
        lease.reconcile(normalization_output_bytes.max(1))?;
    }
    Ok(PreparedKernelOutput {
        output,
        memory_lease,
    })
}

fn write_normalized_output_batch(
    prepared: PreparedKernelOutput,
    output_position: Option<SourcePosition>,
    assembler: &mut crate::CanonicalSegmentAssembler,
    state: &mut OutputWriteState<'_>,
    sink: &mut SegmentOutputSink<'_, '_>,
) -> Result<()> {
    let canonical_segments =
        assembler.push_accounted(prepared.output, output_position, prepared.memory_lease)?;
    persist_canonical_segments(canonical_segments, state, sink)
}

fn persist_canonical_segments(
    canonical_segments: Vec<crate::CanonicalSegment>,
    state: &mut OutputWriteState<'_>,
    sink: &mut SegmentOutputSink<'_, '_>,
) -> Result<()> {
    for canonical in canonical_segments {
        let crate::CanonicalSegment {
            segment_id,
            partition_ordinal,
            batches,
            output_position,
            row_count,
            retained_bytes,
            canonical_batch_rows,
            canonical_batch_bytes,
            memory_leases: _transform_memory_leases,
            ..
        } = canonical;
        let mut _memory_lease = match state.memory.map(Arc::clone) {
            Some(memory) => {
                let ordinal_bytes = row_count
                    .checked_mul(8)
                    .ok_or_else(|| CdfError::data("canonical ordinal buffer size overflow"))?;
                let bytes = retained_bytes
                    .max(1)
                    .checked_mul(2)
                    .and_then(|bytes| bytes.checked_add(ordinal_bytes))
                    .ok_or_else(|| {
                        CdfError::data("canonical concat and ordinal working set overflow")
                    })?;
                let request = ReservationRequest::new(
                    ConsumerKey::new("canonical-segment-concat", MemoryClass::Package)?,
                    bytes,
                )?
                .as_minimum_working_set();
                Some(reserve_with_encode_backpressure(
                    memory,
                    &request,
                    state,
                    sink,
                    &format!(
                        "canonical segment requires {bytes} bytes for retained input, concat output, and package ordinal"
                    ),
                )?)
            }
            None => None,
        };
        let output = crate::segmentation::canonicalize_batches(
            batches,
            canonical_batch_rows,
            canonical_batch_bytes,
        )?;
        let observed_rows = output.iter().try_fold(0_u64, |total, batch| {
            total
                .checked_add(
                    u64::try_from(batch.num_rows())
                        .map_err(|_| CdfError::data("canonical output rows exceed u64"))?,
                )
                .ok_or_else(|| CdfError::data("canonical output rows overflow"))
        })?;
        if observed_rows != row_count {
            return Err(CdfError::internal(format!(
                "canonical segment {segment_id} retained {row_count} rows but canonicalized {observed_rows}"
            )));
        }
        if state.statistics.is_some() {
            let statistics_reservation_bytes = output.iter().try_fold(0_u64, |total, batch| {
                total
                    .checked_add(cdf_kernel::BatchStats::computation_reservation_bytes(
                        batch,
                    )?)
                    .ok_or_else(|| CdfError::data("segment statistics working set overflow"))
            })?;
            let request = ReservationRequest::new(
                ConsumerKey::new("profile-statistics", MemoryClass::Package)?,
                statistics_reservation_bytes.max(1),
            )?
            .as_minimum_working_set();
            let statistics_memory = Arc::clone(
                state
                    .statistics
                    .as_ref()
                    .ok_or_else(|| CdfError::internal("statistics profile state is absent"))?
                    .statistics_memory,
            );
            let _statistics_memory_lease = Some(reserve_with_encode_backpressure(
                statistics_memory,
                &request,
                state,
                sink,
                &format!(
                    "segment statistics require {} bytes",
                    statistics_reservation_bytes.max(1)
                ),
            )?);
            let mut statistics = cdf_kernel::BatchStats::default();
            for batch in &output {
                statistics.merge_owned(cdf_kernel::BatchStats::compute(batch)?)?;
            }
            _statistics_memory_lease
                .as_ref()
                .ok_or_else(|| CdfError::internal("segment statistics lease is absent"))?
                .reconcile(statistics.retained_bytes()?)?;
            let statistics_state = state
                .statistics
                .as_mut()
                .ok_or_else(|| CdfError::internal("statistics profile state is absent"))?;
            statistics_state.statistics_profile.write_stats(
                cdf_package::StatisticsProfileGrain::Segment,
                *statistics_state.statistics_segment_ordinal,
                segment_id.as_str(),
                statistics_state.statistics_profile_schema_hash,
                &statistics,
            )?;
            *statistics_state.statistics_segment_ordinal = statistics_state
                .statistics_segment_ordinal
                .checked_add(1)
                .ok_or_else(|| CdfError::data("statistics profile segment ordinal overflow"))?;
            retain_package_statistics(state, statistics, _statistics_memory_lease)?;
        }
        let package_row_ord_start = sink.queue.next_package_row_ord;
        let next_package_row_ord = package_row_ord_start
            .checked_add(row_count)
            .ok_or_else(|| CdfError::data("package row ordinal overflow"))?;
        let output = cdf_package_contract::append_package_row_ord(output, package_row_ord_start)?;
        sink.queue.next_package_row_ord = next_package_row_ord;
        let normalization_output_bytes = output.iter().try_fold(0_u64, |total, batch| {
            total
                .checked_add(
                    u64::try_from(batch.get_array_memory_size())
                        .map_err(|_| CdfError::data("canonical output bytes exceed u64"))?,
                )
                .ok_or_else(|| CdfError::data("canonical output bytes overflow"))
        })?;
        // Construction needs the retained input, canonical concat output, and ordinal buffer at
        // once. Once construction finishes, only the canonical output follows the encode/staged
        // path; retaining the peak scratch reservation there can starve source-frontier progress
        // and form a memory/backpressure cycle. Transform leases continue to own any shared input
        // buffers, while this reconciled lease owns the complete canonical output working set.
        if let Some(lease) = &_memory_lease {
            lease.reconcile(normalization_output_bytes.max(1))?;
        }
        sink.queue.submit(
            SegmentEncodeWork {
                ordinal: 0,
                segment_id,
                package_row_ord_start,
                partition_ordinal,
                output_position,
                batches: output,
                normalization_output_bytes,
                _transform_memory_leases,
                _scratch_memory_lease: _memory_lease,
            },
            sink.builder,
            state,
            sink.durable,
        )?;
    }
    Ok(())
}

fn reserve_with_encode_backpressure(
    memory: Arc<dyn MemoryCoordinator>,
    request: &ReservationRequest,
    state: &mut OutputWriteState<'_>,
    sink: &mut SegmentOutputSink<'_, '_>,
    operation: &str,
) -> Result<MemoryLease> {
    loop {
        if let Some(lease) = memory.try_reserve(request)? {
            return Ok(lease);
        }
        let SegmentOutputSink {
            builder,
            queue,
            durable,
        } = sink;
        if !queue.relieve_memory_pressure(builder, state, durable)? {
            return Err(CdfError::data(format!(
                "{operation} but the shared memory budget is exhausted with no completed encode work available to release; reduce jobs or raise the memory budget"
            )));
        }
    }
}

fn retain_package_statistics(
    state: &mut OutputWriteState<'_>,
    statistics: cdf_kernel::BatchStats,
    mut segment_lease: Option<MemoryLease>,
) -> Result<()> {
    let Some(statistics_state) = state.statistics.as_mut() else {
        return Ok(());
    };
    let segment_statistics_bytes = statistics.retained_bytes()?;
    let current_statistics_bytes = if state.profile.statistics.columns.is_empty() {
        0
    } else {
        state.profile.statistics.retained_bytes()?
    };
    let required_statistics_bytes = current_statistics_bytes
        .checked_add(segment_statistics_bytes)
        .ok_or_else(|| CdfError::data("package statistics retained bytes overflow"))?
        .max(1);
    if let Some(package_lease) = statistics_state.statistics_memory_lease.as_ref() {
        // Reserve cumulative ownership while the segment lease is still alive. This boundary is
        // deliberately before encode(), whose success publishes the durable IPC segment.
        package_lease.reconcile(required_statistics_bytes)?;
    } else {
        let lease = segment_lease
            .take()
            .ok_or_else(|| CdfError::internal("segment statistics lease is absent"))?;
        lease.reconcile(required_statistics_bytes)?;
        *statistics_state.statistics_memory_lease = Some(lease);
    }
    state.profile.statistics.merge_owned(statistics)?;
    statistics_state
        .statistics_memory_lease
        .as_ref()
        .ok_or_else(|| CdfError::internal("package statistics lease is absent"))?
        .reconcile(state.profile.statistics.retained_bytes()?)
}

fn conform_to_compiled_output_schema(
    batch: RecordBatch,
    expected_schema: &Schema,
) -> Result<RecordBatch> {
    if batch.num_columns() != expected_schema.fields().len() {
        return Err(CdfError::data(format!(
            "emitted batch has {} columns but compiled output schema requires {}",
            batch.num_columns(),
            expected_schema.fields().len()
        )));
    }
    for (index, (actual, expected)) in batch
        .schema()
        .fields()
        .iter()
        .zip(expected_schema.fields())
        .enumerate()
    {
        let actual_encoding = actual.metadata().get(RESIDUAL_ENCODING_METADATA_KEY);
        let expected_encoding = expected.metadata().get(RESIDUAL_ENCODING_METADATA_KEY);
        let expected_source_name = expected.metadata().get(SOURCE_NAME_METADATA_KEY);
        let actual_source_name = actual.metadata().get(SOURCE_NAME_METADATA_KEY);
        if actual.name() != expected.name()
            || actual.data_type() != expected.data_type()
            || actual.is_nullable() != expected.is_nullable()
            || semantic(actual.as_ref()) != semantic(expected.as_ref())
            || actual_encoding != expected_encoding
            || expected_source_name.is_some() && actual_source_name != expected_source_name
        {
            return Err(CdfError::data(format!(
                "emitted field {index} does not match compiled output schema authority: expected {expected:?}, observed {actual:?}"
            )));
        }
    }
    RecordBatch::try_new(Arc::new(expected_schema.clone()), batch.columns().to_vec())
        .map_err(CdfError::from)
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
    residuals: &[BoundBooleanExpression],
    track_source_rows: bool,
) -> Result<ExecutedBatch> {
    let tracked = if track_source_rows {
        if batch.schema().index_of(SOURCE_ROW_FIELD).is_ok() {
            return Err(CdfError::contract(format!(
                "input field {SOURCE_ROW_FIELD:?} conflicts with reserved execution metadata"
            )));
        }
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            SOURCE_ROW_FIELD,
            DataType::UInt64,
            false,
        )));
        let mut columns = batch.columns().to_vec();
        columns
            .push(Arc::new(UInt64Array::from_iter_values(0..batch.num_rows() as u64)) as ArrayRef);
        RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                fields,
                batch.schema().metadata().clone(),
            )),
            columns,
        )
        .map_err(CdfError::from)?
    } else {
        batch.clone()
    };
    let filtered = apply_bound_filters(&tracked, residuals)?;
    Ok(ExecutedBatch {
        batch: filtered,
        source_rows: None,
        limit_truncated: false,
    })
}

fn apply_pre_contract_expressions(
    batch: RecordBatch,
    transforms: &[BoundExpressionTransform],
    remaining_limit: &mut Option<u64>,
    track_source_rows: bool,
) -> Result<ExecutedBatch> {
    let transformed = apply_bound_expression_transforms(batch, transforms)?;
    let transformed_rows = transformed.num_rows();
    let (transformed, limit_truncated) = match remaining_limit {
        Some(remaining) => {
            let take = (*remaining).min(transformed.num_rows() as u64) as usize;
            *remaining -= take as u64;
            (transformed.slice(0, take), take < transformed_rows)
        }
        None => (transformed, false),
    };
    if !track_source_rows {
        return Ok(ExecutedBatch {
            batch: transformed,
            source_rows: None,
            limit_truncated,
        });
    }
    let ordinal_index = transformed.schema().index_of(SOURCE_ROW_FIELD)?;
    let ordinals = transformed
        .column(ordinal_index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| CdfError::internal("source-row tracking column is not uint64"))?;
    let source_rows = ordinals
        .values()
        .iter()
        .map(|value| usize::try_from(*value).map_err(|error| CdfError::internal(error.to_string())))
        .collect::<Result<Vec<_>>>()?;
    let keep = (0..transformed.num_columns())
        .filter(|index| *index != ordinal_index)
        .collect::<Vec<_>>();
    let batch = transformed.project(&keep).map_err(CdfError::from)?;
    Ok(ExecutedBatch {
        batch,
        source_rows: Some(source_rows),
        limit_truncated,
    })
}

fn source_row_tracking_schema(schema: &Schema) -> Result<Schema> {
    if schema.index_of(SOURCE_ROW_FIELD).is_ok() {
        return Err(CdfError::contract(format!(
            "input field {SOURCE_ROW_FIELD:?} conflicts with reserved execution metadata"
        )));
    }
    let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        SOURCE_ROW_FIELD,
        DataType::UInt64,
        false,
    )));
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransformKernelMode {
    Fused,
    Unfused,
}

async fn reserve_transform_working_set(
    memory: Option<&Arc<dyn MemoryCoordinator>>,
    batch: &RecordBatch,
    residual_candidates: &[PreContractResidualCandidate],
) -> Result<Option<MemoryLease>> {
    let Some(memory) = memory else {
        return Ok(None);
    };
    let input_bytes = u64::try_from(batch.get_array_memory_size())
        .map_err(|_| CdfError::data("transform input memory exceeds u64"))?;
    let residual_bytes = residual_candidates
        .iter()
        .try_fold(0u64, |total, candidate| {
            let value_bytes = u64::try_from(candidate.value().get_array_memory_size())
                .map_err(|_| CdfError::data("residual candidate memory exceeds u64"))?;
            let path_bytes = candidate
                .source_path()
                .iter()
                .try_fold(0u64, |total, part| {
                    total
                        .checked_add(u64::try_from(part.len()).unwrap_or(u64::MAX))
                        .ok_or_else(|| CdfError::data("residual path memory overflow"))
                })?;
            let candidate_bytes = value_bytes
                .checked_mul(8)
                .and_then(|bytes| bytes.checked_add(path_bytes))
                .and_then(|bytes| bytes.checked_add(256))
                .ok_or_else(|| CdfError::data("residual transform working set overflow"))?;
            total
                .checked_add(candidate_bytes)
                .ok_or_else(|| CdfError::data("residual transform working set overflow"))
        })?;
    let bytes = input_bytes
        .max(1)
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(residual_bytes))
        .ok_or_else(|| CdfError::data("transform working set overflow"))?;
    let request = ReservationRequest::new(
        ConsumerKey::new("fused-transform", MemoryClass::Transform)?,
        bytes,
    )?
    .as_minimum_working_set();
    Ok(Some(reserve(Arc::clone(memory), request).await?))
}

fn apply_contract_exec(
    batch: RecordBatch,
    evaluator: &mut VectorValidationEvaluator<'_>,
    quarantine_sink: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
    residual_candidates: Vec<PreContractResidualCandidate>,
    context: &ResidualBatchContext<'_>,
    mode: TransformKernelMode,
    memory_lease: Option<MemoryLease>,
) -> Result<ContractExecOutput> {
    if mode == TransformKernelMode::Fused && residual_candidates.is_empty() {
        return apply_contract_exec_without_residual_candidates(
            batch,
            evaluator,
            quarantine_sink,
            context,
            memory_lease,
        );
    }
    let residual = apply_residual_verdicts(
        batch,
        evaluator.program(),
        residual_candidates,
        context,
        quarantine_sink,
    )?;
    let evaluation = evaluator.evaluate_with_quarantine_sink(
        context.evaluation,
        &residual.typed_batch,
        |candidate| {
            quarantine_sink(quarantine_record_from_candidate(
                candidate,
                residual.typed_source_rows.as_deref(),
            )?)
        },
    )?;
    let summary = evaluation.summary;
    let accepted = if summary.accepted_rows == summary.input_rows {
        residual.typed_batch
    } else {
        filter_record_batch(&residual.typed_batch, &evaluation.accepted_rows)
            .map_err(CdfError::from)?
    };
    let variants = filter_optional_strings(&residual.variant_values, &evaluation.accepted_rows);
    let mut combined = summary;
    combined.input_rows = residual.input_rows;
    combined.quarantined_rows += residual.quarantined_rows;
    combined.violation_count += residual.violation_count;
    combined.quarantine_candidate_count += residual.quarantine_candidate_count;
    combined.rule_summaries.extend(residual.rule_summaries);
    Ok(ContractExecOutput {
        accepted,
        variant_values: variants,
        summary: combined,
        residual_decisions: residual.residual_decisions,
        memory_lease,
    })
}

fn apply_contract_exec_without_residual_candidates(
    batch: RecordBatch,
    evaluator: &mut VectorValidationEvaluator<'_>,
    quarantine_sink: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
    context: &ResidualBatchContext<'_>,
    memory_lease: Option<MemoryLease>,
) -> Result<ContractExecOutput> {
    let batch = restore_contract_nullability(batch, evaluator.program())?;
    let evaluation =
        evaluator.evaluate_with_quarantine_sink(context.evaluation, &batch, |candidate| {
            quarantine_sink(quarantine_record_from_candidate(
                candidate,
                context.source_rows,
            )?)
        })?;
    let summary = evaluation.summary;
    let accepted = if summary.accepted_rows == summary.input_rows {
        batch
    } else {
        filter_record_batch(&batch, &evaluation.accepted_rows).map_err(CdfError::from)?
    };
    let variant_values = if evaluator
        .program()
        .residual
        .as_ref()
        .and_then(|residual| residual.capture.as_ref())
        .is_some()
    {
        vec![None; accepted.num_rows()]
    } else {
        Vec::new()
    };
    Ok(ContractExecOutput {
        accepted,
        variant_values,
        summary,
        residual_decisions: Vec::new(),
        memory_lease,
    })
}

struct ResidualExecOutput {
    typed_batch: RecordBatch,
    typed_source_rows: Option<Vec<usize>>,
    variant_values: Vec<Option<String>>,
    input_rows: u64,
    quarantined_rows: u64,
    violation_count: u64,
    quarantine_candidate_count: u64,
    rule_summaries: Vec<cdf_contract::RuleVerdictSummary>,
    residual_decisions: Vec<ResidualDecisionArtifact>,
}

fn apply_residual_verdicts(
    batch: RecordBatch,
    program: &ValidationProgram,
    candidates: Vec<PreContractResidualCandidate>,
    context: &ResidualBatchContext<'_>,
    quarantine_sink: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
) -> Result<ResidualExecOutput> {
    let input_rows = batch.num_rows() as u64;
    let mut variants = vec![None; batch.num_rows()];
    let mut accepted = vec![true; batch.num_rows()];
    let mut quarantine_candidate_count = 0_u64;
    let mut rule_summaries = BTreeMap::<(String, String), cdf_contract::RuleVerdictSummary>::new();
    let mut residual_decisions = Vec::new();
    let source_to_output = context
        .source_rows
        .map(|rows| {
            rows.iter()
                .copied()
                .enumerate()
                .map(|(output, source)| (source, output))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_else(|| (0..batch.num_rows()).map(|row| (row, row)).collect());
    let mut grouped = BTreeMap::<usize, Vec<PreContractResidualCandidate>>::new();
    for candidate in candidates {
        if let Some(output_row) = source_to_output.get(&candidate.batch_row_ordinal()) {
            grouped.entry(*output_row).or_default().push(candidate);
        }
    }

    let mut dynamic_controls = BTreeSet::new();
    if let Some(field) = context.cdc_operation_field {
        dynamic_controls.insert(field.to_owned());
    }
    collect_source_position_control_fields(
        context.evaluation.source_position.as_ref(),
        &mut dynamic_controls,
    );

    for (row, row_candidates) in grouped {
        let mut quarantine_reason = None;
        for candidate in &row_candidates {
            let field = candidate
                .source_path()
                .first()
                .map(String::as_str)
                .unwrap_or_default();
            let field_program = program.residual.as_ref().and_then(|residual| {
                residual
                    .fields
                    .iter()
                    .find(|item| item.source_name == field || item.output_name == field)
            });
            let control_critical = dynamic_controls.contains(field)
                || field_program.is_some_and(|field| field.control_critical || field.required);
            let residual = program.residual.as_ref();
            let captures = residual.is_some_and(|residual| {
                residual.default_verdict == ResidualCandidateVerdict::Capture
                    && residual.capture.is_some()
            });
            if control_critical {
                quarantine_reason = Some((
                    format!("residual:{}:control-critical", residual_path(candidate)),
                    "cdf.residual_control_critical".to_owned(),
                ));
                break;
            }
            if !captures {
                quarantine_reason = Some((
                    format!("residual:{}:contract", residual_path(candidate)),
                    "cdf.residual_contract_quarantine".to_owned(),
                ));
                break;
            }
        }

        let encoded = if quarantine_reason.is_none() {
            let redactions = row_candidates
                .iter()
                .map(|candidate| residual_redaction(program, candidate))
                .collect::<Vec<_>>();
            let fields = row_candidates
                .iter()
                .zip(&redactions)
                .map(|(candidate, redaction)| {
                    let field = ResidualFieldRef::new(
                        candidate.source_path().iter().map(String::as_str),
                        candidate.value(),
                        candidate.value_index(),
                    )?;
                    Ok(ResidualFieldWithRedaction::new(field, redaction))
                })
                .collect::<std::result::Result<Vec<_>, cdf_contract::ResidualCodecError>>();
            match fields.and_then(encode_residual_json_v1_redacted) {
                Ok(bytes) => Some(String::from_utf8(bytes).map_err(|error| {
                    CdfError::internal(format!("residual codec produced non-UTF-8 JSON: {error}"))
                })?),
                Err(error) => {
                    quarantine_reason = Some((
                        format!("residual:{}:encode", residual_path(&row_candidates[0])),
                        error.code().to_owned(),
                    ));
                    None
                }
            }
        } else {
            None
        };

        if let Some((rule_id, error_code)) = quarantine_reason {
            accepted[row] = false;
            for candidate in &row_candidates {
                quarantine_sink(QuarantineRecord {
                    source_row_ordinal: candidate.source_row_ordinal(),
                    rule_id: rule_id.clone(),
                    error_code: error_code.clone(),
                    source_position: context.evaluation.source_position.clone(),
                    observed_value_redacted: residual_observed_value(program, candidate),
                })?;
                quarantine_candidate_count = quarantine_candidate_count
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("residual quarantine count overflowed"))?;
                residual_decisions.push(residual_decision_artifact(
                    program,
                    candidate,
                    context.batch_id,
                    context.observation_id,
                    ResidualRuntimeVerdict::Quarantined,
                    &rule_id,
                )?);
            }
            let summary = rule_summaries
                .entry((rule_id.clone(), error_code.clone()))
                .or_insert(cdf_contract::RuleVerdictSummary {
                    rule_id,
                    error_code,
                    checked_rows: 0,
                    violation_count: 0,
                });
            summary.checked_rows += 1;
            summary.violation_count += 1;
        } else {
            variants[row] = encoded;
            for candidate in &row_candidates {
                residual_decisions.push(residual_decision_artifact(
                    program,
                    candidate,
                    context.batch_id,
                    context.observation_id,
                    ResidualRuntimeVerdict::Captured,
                    "cdf.residual_capture",
                )?);
            }
        }
    }

    let accepted_mask = BooleanArray::from(accepted.clone());
    let typed_batch = if accepted.iter().all(|accepted| *accepted) {
        batch
    } else {
        filter_record_batch(&batch, &accepted_mask).map_err(CdfError::from)?
    };
    let typed_batch = restore_contract_nullability(typed_batch, program)?;
    let typed_source_rows = context.source_rows.map(|source_rows| {
        accepted
            .iter()
            .zip(source_rows)
            .filter_map(|(accepted, source_row)| accepted.then_some(*source_row))
            .collect::<Vec<_>>()
    });
    let variant_values = accepted
        .into_iter()
        .zip(variants)
        .filter_map(|(accepted, value)| accepted.then_some(value))
        .collect::<Vec<_>>();
    let quarantined_rows = input_rows - typed_batch.num_rows() as u64;
    Ok(ResidualExecOutput {
        typed_batch,
        typed_source_rows,
        variant_values,
        input_rows,
        quarantined_rows,
        violation_count: quarantine_candidate_count,
        quarantine_candidate_count,
        rule_summaries: rule_summaries.into_values().collect(),
        residual_decisions,
    })
}

fn restore_contract_nullability(
    batch: RecordBatch,
    program: &ValidationProgram,
) -> Result<RecordBatch> {
    let Some(residual) = &program.residual else {
        return Ok(batch);
    };
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let source = source_name(field.as_ref()).unwrap_or_else(|| field.name());
            residual
                .fields
                .iter()
                .find(|item| item.source_name == source || item.output_name == *field.name())
                .map_or_else(
                    || field.as_ref().clone(),
                    |program| field.as_ref().clone().with_nullable(!program.required),
                )
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        batch.columns().to_vec(),
    )
    .map_err(CdfError::from)
}

fn residual_decision_artifact(
    program: &ValidationProgram,
    candidate: &PreContractResidualCandidate,
    batch_id: &cdf_kernel::BatchId,
    observation_id: Option<&str>,
    verdict: ResidualRuntimeVerdict,
    rule_id: &str,
) -> Result<ResidualDecisionArtifact> {
    Ok(ResidualDecisionArtifact {
        version: 1,
        observation_id: observation_id.map(str::to_owned),
        batch_id: batch_id.clone(),
        source_row_ordinal: candidate.source_row_ordinal(),
        source_path: candidate.source_path().to_vec(),
        observed_physical_type: cdf_contract::CanonicalArrowType::from_arrow(
            candidate.observed_field().data_type(),
        )?,
        expected_effective_type: candidate
            .expected_field()
            .map(|field| cdf_contract::CanonicalArrowType::from_arrow(field.data_type()))
            .transpose()?,
        verdict,
        rule_id: rule_id.to_owned(),
        residual_encoding: program
            .residual
            .as_ref()
            .and_then(|residual| residual.capture.as_ref())
            .map(|capture| capture.encoding.clone())
            .unwrap_or_else(|| cdf_contract::RESIDUAL_ENCODING_NAME.to_owned()),
        typed_projection: if candidate.expected_field().is_some() {
            ResidualTypedProjection::Nulled
        } else {
            ResidualTypedProjection::Absent
        },
        redaction: residual_redaction(program, candidate),
    })
}

fn append_residual_variant(
    batch: RecordBatch,
    program: &ValidationProgram,
    values: Vec<Option<String>>,
) -> Result<RecordBatch> {
    let Some(capture) = program
        .residual
        .as_ref()
        .and_then(|residual| residual.capture.as_ref())
    else {
        return Ok(batch);
    };
    if values.len() != batch.num_rows() {
        return Err(CdfError::internal(
            "residual variant values do not align with accepted rows",
        ));
    }
    if batch.schema().index_of(&capture.variant_column).is_ok() {
        return Err(CdfError::contract(format!(
            "residual variant column {:?} conflicts with typed output",
            capture.variant_column
        )));
    }
    let field = cdf_kernel::with_semantic(
        Field::new(&capture.variant_column, DataType::Utf8, true),
        capture.semantic.clone(),
    );
    let mut metadata = field.metadata().clone();
    metadata.insert(
        RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
        capture.encoding.clone(),
    );
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(field.with_metadata(metadata)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )
    .map_err(CdfError::from)
}

fn filter_optional_strings(values: &[Option<String>], mask: &BooleanArray) -> Vec<Option<String>> {
    values
        .iter()
        .zip(mask.iter())
        .filter_map(|(value, keep)| keep.unwrap_or(false).then_some(value.clone()))
        .collect()
}

fn residual_path(candidate: &PreContractResidualCandidate) -> String {
    candidate.source_path().join(".")
}

fn residual_observed_value(
    program: &ValidationProgram,
    candidate: &PreContractResidualCandidate,
) -> QuarantineObservedValue {
    if candidate.value().is_null(candidate.value_index()) {
        return QuarantineObservedValue::Null;
    }
    let decision = residual_redaction(program, candidate);
    let encoded = ResidualFieldRef::new(
        candidate.source_path().iter().map(String::as_str),
        candidate.value(),
        candidate.value_index(),
    )
    .and_then(|field| encode_residual_json_v1([field]))
    .ok();
    match &decision {
        RedactionDecision::Preserve => encoded
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .map(|value| QuarantineObservedValue::Preserved { value })
            .unwrap_or(QuarantineObservedValue::Omitted),
        RedactionDecision::Hash { algorithm } if algorithm == "sha256" => {
            encoded.map_or(QuarantineObservedValue::Omitted, |bytes| {
                QuarantineObservedValue::Hashed {
                    algorithm: algorithm.clone(),
                    value: format!("sha256:{:x}", Sha256::digest(bytes)),
                }
            })
        }
        RedactionDecision::Mask { replacement } => QuarantineObservedValue::Masked {
            value: replacement.clone(),
        },
        RedactionDecision::Hash { .. } | RedactionDecision::Omit => {
            QuarantineObservedValue::Omitted
        }
    }
}

fn residual_redaction(
    program: &ValidationProgram,
    candidate: &PreContractResidualCandidate,
) -> RedactionDecision {
    let field = candidate
        .source_path()
        .first()
        .map(String::as_str)
        .unwrap_or_default();
    if let Some(decision) = program
        .residual
        .as_ref()
        .and_then(|residual| {
            residual
                .fields
                .iter()
                .find(|item| item.source_name == field || item.output_name == field)
        })
        .map(|field| field.redaction.clone())
    {
        return decision;
    }
    program
        .residual
        .as_ref()
        .map_or(RedactionDecision::Omit, |residual| {
            cdf_contract::redaction_decision_for_field(
                candidate.observed_field(),
                &residual.pii_redaction,
            )
        })
}

fn collect_source_position_control_fields(
    position: Option<&SourcePosition>,
    controls: &mut BTreeSet<String>,
) {
    match position {
        Some(SourcePosition::Cursor(cursor)) => {
            controls.insert(cursor.field.clone());
        }
        Some(SourcePosition::Composite(composite)) => {
            for position in composite.positions.values() {
                collect_source_position_control_fields(Some(position), controls);
            }
        }
        _ => {}
    }
}

fn quarantine_record_from_candidate(
    candidate: QuarantineCandidate,
    source_rows: Option<&[usize]>,
) -> Result<QuarantineRecord> {
    let source_row_ordinal = match source_rows {
        Some(rows) => rows
            .get(candidate.source_row_ordinal)
            .copied()
            .ok_or_else(|| {
                CdfError::internal("contract quarantine ordinal is absent from the source-row map")
            })?,
        None => candidate.source_row_ordinal,
    };
    Ok(QuarantineRecord {
        source_row_ordinal: u64::try_from(source_row_ordinal)
            .map_err(|error| CdfError::internal(error.to_string()))?,
        rule_id: candidate.rule_id,
        error_code: candidate.error_code,
        source_position: candidate.source_position,
        observed_value_redacted: quarantine_observed_value(candidate.observed_value_redacted),
    })
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
    [
        SOURCE_NAME_METADATA_KEY,
        PHYSICAL_TYPE_METADATA_KEY,
        RESIDUAL_ENCODING_METADATA_KEY,
    ]
    .into_iter()
    .filter_map(|key| {
        field
            .metadata()
            .get(key)
            .map(|value| (key.to_owned(), value.clone()))
    })
    .collect()
}

fn validate_effective_schema_plan<'a, R>(
    plan: &'a EnginePlan,
    resource: &R,
) -> Result<Option<&'a EffectiveSchemaPlanEvidence>>
where
    R: ResourceStream + ?Sized,
{
    let Some(evidence) = plan.effective_schema_evidence.as_ref() else {
        if resource.effective_schema_runtime().is_some() {
            return Err(CdfError::data(
                "resource carries effective-schema evidence but the serialized engine plan omitted it",
            ));
        }
        return Ok(None);
    };
    let schema_authority = plan.schema_authority();
    if schema_authority.baseline_schema_hash != *evidence.authority.baseline.schema_hash()
        || schema_authority.effective_schema_hash != evidence.authority.effective_schema_hash
    {
        return Err(CdfError::data(
            "engine plan schema authority does not match effective-schema evidence",
        ));
    }
    evidence
        .authority
        .validate_for_resource(resource.descriptor())?;
    let effective_arrow_schema_hash =
        cdf_kernel::canonical_arrow_schema_hash(resource.schema().as_ref())?;
    if evidence.effective_arrow_schema_hash != effective_arrow_schema_hash {
        return Err(CdfError::data(format!(
            "serialized effective Arrow schema hash {} does not match execution resource schema {}",
            evidence.effective_arrow_schema_hash, effective_arrow_schema_hash
        )));
    }
    if resource
        .effective_schema_runtime()
        .map(|runtime| &runtime.evidence)
        != Some(&evidence.authority)
    {
        return Err(CdfError::data(
            "serialized engine plan effective-schema evidence does not match the execution resource",
        ));
    }
    if resource
        .effective_schema_runtime()
        .map(|runtime| runtime.terminal_quarantines.as_slice())
        != Some(evidence.terminal_quarantines.as_slice())
    {
        return Err(CdfError::data(
            "serialized terminal schema-observation evidence does not match the execution resource",
        ));
    }
    if resource
        .effective_schema_runtime()
        .and_then(|runtime| runtime.discovery_executor_budget.as_ref())
        != evidence.discovery_executor_budget.as_ref()
    {
        return Err(CdfError::data(
            "serialized discovery executor budget does not match the execution resource",
        ));
    }
    for partition in &plan.scan.partitions {
        partition_schema_disposition(partition, evidence)?;
    }
    Ok(Some(evidence))
}

fn validate_plan_metadata(
    partition: &cdf_kernel::PartitionPlan,
    key: &str,
    expected: &str,
) -> Result<()> {
    if partition.metadata.get(key).map(String::as_str) != Some(expected) {
        return Err(CdfError::data(format!(
            "planned partition {} has missing or spoofed {key} effective-schema evidence",
            partition.partition_id
        )));
    }
    Ok(())
}

fn partition_schema_disposition(
    partition: &cdf_kernel::PartitionPlan,
    evidence: &EffectiveSchemaPlanEvidence,
) -> Result<PartitionSchemaDisposition> {
    let observation_id = partition
        .metadata
        .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
        .ok_or_else(|| {
            CdfError::data("effective-schema partition omitted its observation identity")
        })?;
    let expected_binding = evidence
        .observation_bindings
        .get(observation_id)
        .ok_or_else(|| {
            CdfError::data(format!(
                "effective-schema evidence omitted source identity binding for observation {observation_id:?}"
            ))
        })?;
    validate_plan_metadata(
        partition,
        PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
        expected_binding,
    )?;
    if let Some(quarantine) = evidence
        .terminal_quarantines
        .binary_search_by(|item| item.observation_id().cmp(observation_id))
        .ok()
        .map(|index| &evidence.terminal_quarantines[index])
    {
        validate_plan_metadata(
            partition,
            PLAN_PHYSICAL_SCHEMA_HASH_KEY,
            quarantine.physical_schema_hash().as_str(),
        )?;
        return Ok(PartitionSchemaDisposition::Quarantined(quarantine.clone()));
    }
    let observation = evidence
        .observations
        .binary_search_by(|observation| observation.observation_id.as_str().cmp(observation_id))
        .ok()
        .map(|index| &evidence.observations[index]);
    let Some(observation) = observation else {
        if partition
            .metadata
            .contains_key(PLAN_PHYSICAL_SCHEMA_HASH_KEY)
        {
            return Err(CdfError::data(format!(
                "unobserved schema candidate {observation_id:?} carries spoofed physical-schema evidence"
            )));
        }
        return Ok(PartitionSchemaDisposition::Unobserved);
    };
    validate_plan_metadata(
        partition,
        PLAN_PHYSICAL_SCHEMA_HASH_KEY,
        observation.physical_schema_hash.as_str(),
    )?;
    Ok(PartitionSchemaDisposition::Admitted(observation.clone()))
}

fn validate_effective_batch_schema(observed: &Schema, effective: &Schema) -> Result<()> {
    validate_effective_batch_schema_with_nullable_sources(observed, effective, &BTreeSet::new())
}

fn validate_materialized_effective_batch_schema(
    observed: &Schema,
    effective: &Schema,
    residual_candidates: &[PreContractResidualCandidate],
) -> Result<()> {
    let nullable_sources = residual_candidates
        .iter()
        .filter_map(|candidate| candidate.source_path().first().cloned())
        .collect::<BTreeSet<_>>();
    validate_effective_batch_schema_with_nullable_sources(observed, effective, &nullable_sources)
}

fn validate_effective_batch_schema_with_nullable_sources(
    observed: &Schema,
    effective: &Schema,
    nullable_sources: &BTreeSet<String>,
) -> Result<()> {
    if observed.fields().len() != effective.fields().len() {
        return Err(CdfError::data(format!(
            "per-observation coercion produced {} fields but the effective schema requires {}",
            observed.fields().len(),
            effective.fields().len()
        )));
    }
    for (observed, effective) in observed.fields().iter().zip(effective.fields()) {
        let observed_source = source_name(observed.as_ref()).unwrap_or_else(|| observed.name());
        let effective_source = source_name(effective.as_ref()).unwrap_or_else(|| effective.name());
        let nullable_matches = observed.is_nullable() == effective.is_nullable()
            || (observed.is_nullable()
                && !effective.is_nullable()
                && nullable_sources.contains(effective_source));
        if observed.name() != effective.name()
            || observed_source != effective_source
            || observed.data_type() != effective.data_type()
            || !nullable_matches
        {
            return Err(CdfError::data(format!(
                "per-observation coercion output field {:?} does not target effective field {:?}",
                observed.name(),
                effective.name()
            )));
        }
    }
    Ok(())
}

fn current_observed_at_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CdfError::internal(format!("system clock before Unix epoch: {error}")))?;
    i64::try_from(duration.as_millis()).map_err(|_| {
        CdfError::internal("system time milliseconds do not fit in i64 evaluation context")
    })
}

fn current_observed_at_u64_ms() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CdfError::internal(format!("system clock before Unix epoch: {error}")))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| CdfError::internal("system time milliseconds do not fit in u64"))
}

fn elapsed_ns(started: Option<Instant>, label: &str) -> Result<u64> {
    let Some(started) = started else {
        return Ok(0);
    };
    u64::try_from(started.elapsed().as_nanos())
        .map_err(|error| CdfError::internal(format!("{label} duration overflow: {error}")))
}

#[cfg(test)]
mod transform_kernel_tests {
    use std::{collections::BTreeMap, hint::black_box, sync::Arc, time::Instant};

    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_contract::{
        ContractEvaluationContext, ContractPolicy, Expression, ExpressionUse, ObservedSchema,
        SchemaEvolutionMode, TransformDescription, VectorValidationEvaluator,
        compile_validation_program,
    };
    use cdf_kernel::{BatchId, TrustLevel};
    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use cdf_package::PackageBuilder;
    use cdf_package_contract::{QuarantineObservedValue, QuarantineRecord};

    use super::{
        QuarantinePartAccumulator, ResidualBatchContext, TransformKernelMode, apply_contract_exec,
        apply_pre_contract_expressions, execute_batch, reserve_quarantine_evidence,
        source_row_tracking_schema,
    };

    #[test]
    fn tracked_source_rows_do_not_shift_sequential_derive_filter_bindings() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let transforms = vec![
            TransformDescription::Derive {
                column: "selected".to_owned(),
                expression: Expression::parse_comparison("id >= 2").unwrap(),
            },
            TransformDescription::Filter {
                expression: Expression::parse_comparison("selected = true").unwrap(),
            },
        ];
        let derive = crate::expression::plan_expression(
            match &transforms[0] {
                TransformDescription::Derive { expression, .. } => expression.clone(),
                _ => unreachable!(),
            },
            ExpressionUse::Derive,
            schema.as_ref(),
        )
        .unwrap();
        let derived_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("selected", DataType::Boolean, true),
        ]);
        let filter = crate::expression::plan_expression(
            match &transforms[1] {
                TransformDescription::Filter { expression } => expression.clone(),
                _ => unreachable!(),
            },
            ExpressionUse::Filter,
            &derived_schema,
        )
        .unwrap();
        let tracked_schema = source_row_tracking_schema(schema.as_ref()).unwrap();
        let bound = cdf_expression::bind_expression_transforms(
            &transforms,
            &[derive, filter],
            &tracked_schema,
        )
        .unwrap();
        let input =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).unwrap();
        let tracked = execute_batch(&input, &[], true).unwrap();
        let output =
            apply_pre_contract_expressions(tracked.batch, &bound, &mut None, true).unwrap();

        assert_eq!(output.batch.num_rows(), 1);
        assert_eq!(
            output
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(output.source_rows, Some(vec![1]));
        assert!(
            output
                .batch
                .schema()
                .index_of(super::SOURCE_ROW_FIELD)
                .is_err()
        );
    }

    #[test]
    fn production_execution_cannot_call_the_scalar_contract_oracle() {
        let scalar_oracle = ["evaluate", "record", "batch"].join("_") + "(";
        assert!(!include_str!("execution.rs").contains(&scalar_oracle));
    }

    #[test]
    fn quarantine_evidence_fails_cleanly_before_exceeding_managed_budget() {
        let memory = Arc::new(DeterministicMemoryCoordinator::new(1_024, BTreeMap::new()).unwrap());
        let managed: Arc<dyn MemoryCoordinator> = memory.clone();
        let lease = reserve_quarantine_evidence(Some(&managed)).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let builder = PackageBuilder::create(temp.path(), "quarantine-budget").unwrap();
        let mut part_count = 0;
        let mut sink = QuarantinePartAccumulator::new(&builder, &mut part_count, lease);
        let error = sink
            .push(QuarantineRecord {
                source_row_ordinal: 0,
                rule_id: "oversized".to_owned(),
                error_code: "domain_violation".to_owned(),
                source_position: None,
                observed_value_redacted: QuarantineObservedValue::Preserved {
                    value: "x".repeat(4_096),
                },
            })
            .unwrap_err();
        assert!(error.message.contains("exceeds available managed capacity"));
        drop(sink);
        assert_eq!(part_count, 0);
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert!(
            std::fs::read_dir(temp.path().join("quarantine"))
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    fn dense_quarantine_evidence_stays_bounded_without_losing_rows() {
        const BUDGET: u64 = 512 * 1024;
        const ROWS: usize = 25_000;
        let memory =
            Arc::new(DeterministicMemoryCoordinator::new(BUDGET, BTreeMap::new()).unwrap());
        let managed: Arc<dyn MemoryCoordinator> = memory.clone();
        let lease = reserve_quarantine_evidence(Some(&managed)).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let builder = PackageBuilder::create(temp.path(), "dense-quarantine-budget").unwrap();
        let mut part_count = 0;
        let mut sink = QuarantinePartAccumulator::new(&builder, &mut part_count, lease);
        for row in 0..ROWS {
            sink.push(QuarantineRecord {
                source_row_ordinal: u64::try_from(row).unwrap(),
                rule_id: "dense-domain".to_owned(),
                error_code: "domain_violation".to_owned(),
                source_position: None,
                observed_value_redacted: QuarantineObservedValue::Preserved {
                    value: format!("{row:08}-{}", "x".repeat(512)),
                },
            })
            .unwrap();
        }
        sink.finish().unwrap();
        assert!(part_count > 1);
        let snapshot = memory.snapshot();
        assert!(snapshot.peak_bytes <= BUDGET);
        assert_eq!(snapshot.current_bytes, 0);

        let mut paths = std::fs::read_dir(temp.path().join("quarantine"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        paths.sort();
        let mut records = Vec::new();
        for path in paths {
            records.extend(cdf_package::quarantine_records_from_parquet_file(path).unwrap());
        }
        assert_eq!(records.len(), ROWS);
        assert_eq!(records.first().unwrap().source_row_ordinal, 0);
        assert_eq!(
            records.last().unwrap().source_row_ordinal,
            u64::try_from(ROWS - 1).unwrap()
        );
    }

    #[test]
    #[ignore = "V2 quarantine RSS calibration; run outside fast checks"]
    fn dense_quarantine_evidence_rss_probe() {
        const BUDGET: u64 = 512 * 1024;
        let rows = std::env::var("CDF_QUARANTINE_RSS_ROWS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(25_000);
        let memory =
            Arc::new(DeterministicMemoryCoordinator::new(BUDGET, BTreeMap::new()).unwrap());
        let managed: Arc<dyn MemoryCoordinator> = memory.clone();
        let lease = reserve_quarantine_evidence(Some(&managed)).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let builder = PackageBuilder::create(temp.path(), "dense-quarantine-rss").unwrap();
        let mut part_count = 0;
        let mut sink = QuarantinePartAccumulator::new(&builder, &mut part_count, lease);
        for row in 0..rows {
            sink.push(QuarantineRecord {
                source_row_ordinal: u64::try_from(row).unwrap(),
                rule_id: "dense-domain".to_owned(),
                error_code: "domain_violation".to_owned(),
                source_position: None,
                observed_value_redacted: QuarantineObservedValue::Preserved {
                    value: format!("{row:08}-{}", "x".repeat(512)),
                },
            })
            .unwrap();
        }
        sink.finish().unwrap();
        assert!(part_count > 1);
        let snapshot = memory.snapshot();
        assert!(snapshot.peak_bytes <= BUDGET);
        assert_eq!(snapshot.current_bytes, 0);
    }

    #[test]
    #[ignore = "release-mode A5b fused/unfused kernel benchmark"]
    fn fused_transform_hot_path_benchmark() {
        let rows = 64 * 1024;
        let iterations = std::env::var("CDF_A5_FUSION_BENCH_ITERATIONS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(200);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|_| "yellow-taxi"),
                )),
                Arc::new(BooleanArray::from(vec![true; rows])),
            ],
        )
        .unwrap();
        let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
        policy.schema.mode = SchemaEvolutionMode::Evolve;
        let program =
            compile_validation_program(&policy, &ObservedSchema::from_arrow(schema.as_ref()))
                .unwrap();
        let evaluation = ContractEvaluationContext::observed_at(0);
        let batch_id = BatchId::new("fusion-benchmark").unwrap();
        let context = ResidualBatchContext {
            evaluation: &evaluation,
            source_rows: None,
            cdc_operation_field: None,
            batch_id: &batch_id,
            observation_id: None,
        };

        let measure = |mode| {
            let mut evaluator = VectorValidationEvaluator::new(&program);
            let mut discard_quarantine = |_record: QuarantineRecord| Ok(());
            let started = Instant::now();
            for _ in 0..iterations {
                let output = apply_contract_exec(
                    black_box(batch.clone()),
                    &mut evaluator,
                    &mut discard_quarantine,
                    Vec::new(),
                    black_box(&context),
                    mode,
                    None,
                )
                .unwrap();
                black_box(output);
            }
            started.elapsed()
        };
        let unfused = measure(TransformKernelMode::Unfused);
        let fused = measure(TransformKernelMode::Fused);
        let bytes = batch.get_array_memory_size() as f64 * iterations as f64;
        let unfused_gib_s = bytes / unfused.as_secs_f64() / 1024_f64.powi(3);
        let fused_gib_s = bytes / fused.as_secs_f64() / 1024_f64.powi(3);
        eprintln!(
            "fused-transform rows={rows} iterations={iterations} unfused_gib_s={unfused_gib_s:.3} fused_gib_s={fused_gib_s:.3} speedup={:.3}",
            fused_gib_s / unfused_gib_s
        );
        assert!(
            fused <= unfused,
            "fused hot path regressed: fused={fused:?}, unfused={unfused:?}"
        );
    }
}
