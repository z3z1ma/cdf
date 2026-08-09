use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::Path,
    sync::{Arc, mpsc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::expression_execution::{
    BoundExpressionTransform, BoundScalarExpression, SOURCE_ROW_TRACKING_FIELD,
    apply_bound_expression_transforms, apply_bound_filters, apply_expression_transforms,
    bind_expression_transforms, bind_filter_expressions, bind_relational_expression_plan,
    execute_bound_relational_expression_plan_tracked, expression_transform_output_schema,
};
use crate::expression_memory::expression_working_set_bytes;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::filter::filter_record_batch;
use arrow_select::take::take_record_batch;
use cdf_contract::{
    ContractEvaluationContext, FieldDisposition, FieldRole, QuarantineCandidate,
    RESIDUAL_ENCODING_METADATA_KEY, RecordViolationDisposition, RedactionDecision,
    ResidualFieldRef, ResidualFieldWithRedaction, VARIANT_COLUMN_NAME, ValidationProgram,
    VectorValidationEvaluator, VerdictSummary, encode_package_dedup_keys, encode_residual_json_v1,
    encode_residual_json_v1_redacted, evaluate_package_order_dedup, materialize_schema_coercion,
    package_dedup_rule, reject_untrusted_schema_coercion_metadata,
    schema_coercion_plan_from_trusted_json,
};
use cdf_kernel::{
    Batch, CdfError, CompositePosition, ExecutablePartition, ExecutionExtent,
    OrderedStratifiedHashV1, PHYSICAL_TYPE_METADATA_KEY, PLAN_SCHEMA_OBSERVATION_ID_KEY,
    PartitionAttestation, PartitionPlan, PhysicalObservationRepresentation,
    PreContractObservedValue, PreContractPhysicalReconciliation, PreContractQuarantineFact,
    PreContractResidualCandidate, ProcessedObservationOutcome, ProcessedObservationPosition,
    ResourceStream, Result, RunId, RunPhase, RunPhaseContext, SOURCE_NAME_METADATA_KEY,
    SOURCE_POSITION_VERSION, ScopeKey, SourcePosition, StratifiedHashBoundedIdentity,
    StratifiedHashCandidate, StratifiedHashIdentityStrength, TerminalSchemaObservationQuarantine,
    WatermarkClaim, WatermarkPolicy, WriteDisposition, aggregate_resource_closed_output_position,
    aggregate_resource_output_position, merge_terminal_position_evidence, semantic, source_name,
};
use cdf_memory::{
    ConsumerKey, DEFAULT_PROCESS_BUDGET_BYTES, DeterministicMemoryCoordinator, MemoryClass,
    MemoryCoordinator, MemoryLease, ReservationRequest, reserve,
};
use cdf_package::PackageBuilder;
use cdf_package_contract::{
    LATE_DATA_EVIDENCE_FILE, LATE_DATA_EVIDENCE_VERSION, LATE_DATA_PAYLOAD_CATALOG_FILE,
    LATE_DATA_PAYLOAD_CATALOG_VERSION, LateDataBatchEvidence, LateDataPayloadArtifact,
    LateDataPayloadLocation, LateDataRowEvidence, PackageStatus, QuarantineObservedValue,
    QuarantineRecord,
};
use futures_util::{StreamExt, future::Either};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{Instrument, Span, info_span};

use super::finalization::{
    PackageExecutionOutcome, PackageFinalization, PackagePreFinalizeHook, StreamingFinalizeHook,
};
use super::measurements::{PhaseMeasurements, elapsed_ns};
use super::partition_lifecycle::{
    ExecutablePartitionPlans, OpenedPartition, PartitionOpenEvidence, PartitionOpenMetadata,
    PartitionOpenRuntime, validate_execution_invocation,
};
use super::retry_frontier::{
    await_partition_retry, decide_partition_retry, partition_open_jobs, schedule_partition_retry,
    source_frontier_batch_bound, with_cleanup_failure,
};
use super::schema_admission::{
    AdmittedBatchSchema, BatchSchemaAdmissionContext, BatchSchemaDisposition, ExtraFieldEvidence,
    PartitionSchemaDisposition, partition_schema_disposition, validate_effective_schema_plan,
};
use super::segment_sink::{
    DurableSegmentHook, DurableSegmentObserver, DurableSegmentPayload, PackageSegmentProgressHook,
};

use crate::{
    AdmittedEnginePartitionEvidence, CompiledSchemaAdmissionOutcome, CompiledSchemaAdmissionPlan,
    CompiledSchemaQuarantineEvidence, CompiledStreamAdmissionEvidence,
    EffectiveSchemaObservationCoercion, EffectiveSchemaPlanEvidence, EngineDrainEpoch,
    EngineDrainEpochOutcome, EngineExecutionConfig, EngineExecutionEvidence,
    EngineExecutionInvocation, EnginePlan, EnginePreviewLimits, EnginePreviewOutput,
    EngineRunOutput, EngineRunOutputWithSegmentPositions, EngineSegmentPosition,
    EngineWorkerArtifactAuthority, ExecutionProfile, LineageInputObservation, LineageSummary,
    PhysicalObservationEvidence, SchemaQuarantineObservationEvidence, StandaloneExecutionHost,
    StreamAdmissionObservationEvidence,
    output_schema::canonicalize_effective_output_schema,
    planning::{scan_expression_schema, validate_program},
    variant_capture::{
        FieldTypeEvidenceArtifact, ResidualDecisionArtifact, ResidualRuntimeVerdict,
        ResidualTypedProjection, SchemaAdmissionArtifact, normalize_batch,
        schema_admission_artifact_metadata,
    },
};

/// Mutable epoch-scoped authorities that may advance only through one canonical drain closure.
pub struct DrainEpochExecution<'a> {
    durable_segment: Option<&'a mut DurableSegmentHook<'a>>,
    package_progress: Option<&'a mut PackageSegmentProgressHook<'a>>,
    stream_finalize: Option<&'a mut StreamingFinalizeHook<'a>>,
    late_data_carryover: Vec<LateDataCarryoverInput>,
    controller: &'a mut cdf_runtime::DrainEpochController,
}

pub struct LateDataCarryoverInput {
    reference: cdf_kernel::LateDataCarryoverRef,
    object: cdf_package::VerifiedIdentityObject,
}

impl LateDataCarryoverInput {
    pub fn new(
        reference: cdf_kernel::LateDataCarryoverRef,
        object: cdf_package::VerifiedIdentityObject,
    ) -> Result<Self> {
        reference.validate()?;
        if reference.relative_path != object.relative_path() {
            return Err(CdfError::data(
                "late-data carryover reference does not match its verified package object",
            ));
        }
        if reference.byte_count != object.byte_count() || reference.sha256 != object.sha256() {
            return Err(CdfError::data(
                "late-data carryover content identity does not match its verified package object",
            ));
        }
        Ok(Self { reference, object })
    }
}

impl<'a> DrainEpochExecution<'a> {
    pub fn new(controller: &'a mut cdf_runtime::DrainEpochController) -> Self {
        Self {
            durable_segment: None,
            package_progress: None,
            stream_finalize: None,
            late_data_carryover: Vec::new(),
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

    pub fn with_package_progress(
        mut self,
        progress: &'a mut PackageSegmentProgressHook<'a>,
    ) -> Self {
        self.package_progress = Some(progress);
        self
    }

    pub fn with_late_data_carryover(
        mut self,
        late_data_carryover: Vec<LateDataCarryoverInput>,
    ) -> Self {
        self.late_data_carryover = late_data_carryover;
        self
    }
}

fn standalone_execution_options() -> Result<EngineExecutionInvocation> {
    let (_, services) = StandaloneExecutionHost::default_services(DEFAULT_PROCESS_BUDGET_BYTES)?;
    Ok(EngineExecutionConfig::default()
        .with_execution_services(services)
        .new_invocation())
}

fn package_builder_resources(
    services: Option<&cdf_runtime::ExecutionServices>,
) -> Result<cdf_package::PackageBuilderResources> {
    let services = services.ok_or_else(|| {
        CdfError::contract(
            "package execution requires shared execution services for manifest memory and spill accounting",
        )
    })?;
    cdf_package::PackageBuilderResources::shared(services.memory(), services.spill())
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
    let invocation = standalone_execution_options()?;
    let memory = invocation
        .services
        .as_ref()
        .ok_or_else(|| CdfError::internal("standalone expression execution omitted memory"))?
        .memory();
    let expression_bytes = expression_working_set_bytes(
        compiled
            .transforms
            .iter()
            .map(|planned| &planned.expression),
        batch.num_rows(),
    )?;
    let request = transform_working_set_request(&batch, &[], expression_bytes)?;
    let _expression_memory_lease = memory.try_reserve(&request)?.ok_or_else(|| {
        CdfError::environment(
            "standalone expression execution could not reserve its bounded working set",
        )
    })?;
    normalize_batch(
        apply_expression_transforms(
            batch,
            &program.transforms,
            &compiled.transforms,
            Some(&_expression_memory_lease),
            &cdf_runtime::RunCancellation::default(),
        )?,
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
    let preview_invocation = standalone_execution_options()?;
    let preview_memory = preview_invocation
        .services
        .as_ref()
        .ok_or_else(|| CdfError::internal("preview expression execution omitted memory"))?
        .memory();
    let effective_schema_evidence = validate_effective_schema_plan(plan, resource)?;
    crate::planning::validate_plan_schema_authority(resource, plan)?;
    let resource_schema = resource.schema();
    let runtime_output_schema = plan.output_arrow_schema()?;
    cdf_package_contract::validate_logical_output_schema(runtime_output_schema.as_ref())?;
    let admission_schema = scan_expression_schema(
        resource_schema.as_ref(),
        plan.explain
            .projection_pushed
            .then_some(plan.scan.request.projection.as_deref())
            .flatten(),
    )?;
    let bound_relational = plan
        .relational_expression_plan
        .as_ref()
        .map(bind_relational_expression_plan)
        .transpose()?;
    let expression_schema = plan
        .relational_expression_plan
        .as_ref()
        .map(|relational| relational.output_schema.to_arrow())
        .transpose()?
        .unwrap_or_else(|| admission_schema.clone());
    let bound_residuals =
        bind_filter_expressions(&plan.compiled_expression_plan.residuals, &expression_schema)?;
    let tracking_expression_schema = source_row_tracking_schema(&expression_schema)?;
    let bound_tracked_residuals = bind_filter_expressions(
        &plan.compiled_expression_plan.residuals,
        &tracking_expression_schema,
    )?;
    let bound_transforms = bind_expression_transforms(
        &plan.validation_program.transforms,
        &plan.compiled_expression_plan.transforms,
        &expression_schema,
    )?;
    let bound_tracked_transforms = bind_expression_transforms(
        &plan.validation_program.transforms,
        &plan.compiled_expression_plan.transforms,
        &tracking_expression_schema,
    )?;
    let contract_schema = expression_transform_output_schema(
        &plan.validation_program.transforms,
        &plan.compiled_expression_plan.transforms,
        &expression_schema,
    )?;
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
    let mut remaining_query_limit = plan.final_limit.or(plan.scan.request.limit);
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
    let planned_partition_count = plan
        .partition_schedule
        .as_ref()
        .ok_or_else(|| CdfError::contract("preview requires a compiled partition schedule"))?
        .partition_count();
    let external_tasks = plan.scan.external_task_set().is_some();
    let payload_eligible_partition_count = if external_tasks {
        let mut payload_count = 0_u64;
        let mut partitions = executable_partition_plans(plan, resource)?;
        for ordinal in 0..planned_partition_count {
            let executable = partitions.next(ordinal)?;
            let disposition = effective_schema_evidence
                .map(|evidence| partition_schema_disposition(executable.plan(), evidence, true))
                .transpose()?;
            if let Some(PartitionSchemaDisposition::Quarantined(quarantine)) = disposition {
                required_preview_attestation(
                    resource,
                    &executable,
                    quarantine.observation_id(),
                    quarantine.physical_schema_hash(),
                    &mut observation_attestations,
                )
                .await?;
                attested_partition_count += 1;
                terminal_quarantines.insert(quarantine.observation_id().to_owned());
            } else {
                payload_count = payload_count
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("preview payload count exceeds u64"))?;
            }
        }
        let selection = if payload_count > 0 {
            let mut selector = OrderedStratifiedHashV1::new(
                plan.scan.request.resource_id.clone(),
                limits.max_batches,
                payload_count,
            )?;
            let mut retained = BTreeMap::<String, PreviewPayloadCandidate>::new();
            let mut partitions = executable_partition_plans(plan, resource)?;
            for ordinal in 0..planned_partition_count {
                let executable = partitions.next(ordinal)?;
                let disposition = effective_schema_evidence
                    .map(|evidence| partition_schema_disposition(executable.plan(), evidence, true))
                    .transpose()?;
                if matches!(
                    disposition,
                    Some(PartitionSchemaDisposition::Quarantined(_))
                ) {
                    continue;
                }
                let expected = disposition.and_then(|item| match item {
                    PartitionSchemaDisposition::Admitted(evidence) => Some(evidence),
                    PartitionSchemaDisposition::Quarantined(_)
                    | PartitionSchemaDisposition::Unobserved => None,
                });
                let (display_location, bounded_identity) =
                    preview_partition_identity(executable.plan())?;
                // External task ordinals are already canonical. Prefixing their display location
                // gives the ordered accumulator a unique key without retaining a cardinality-sized
                // duplicate-location map.
                let location = format!("{ordinal:020}:{display_location}");
                let change = selector.push(StratifiedHashCandidate::from_bounded_identity(
                    location.clone(),
                    &bounded_identity,
                )?)?;
                if let Some(evicted) = change.evicted_location {
                    retained.remove(&evicted);
                }
                if change.retained {
                    retained.insert(
                        location.clone(),
                        PreviewPayloadCandidate {
                            executable,
                            expected,
                            location,
                            bounded_identity,
                        },
                    );
                }
            }
            let plan = selector.finish()?;
            payload_candidates.extend(retained.into_values());
            Some(plan)
        } else {
            None
        };
        Some((payload_count, selection))
    } else {
        let mut location_counts = BTreeMap::<String, usize>::new();
        for partition in plan
            .scan
            .inline_partitions()
            .ok_or_else(|| CdfError::contract("inline preview authority is unavailable"))?
        {
            let disposition = effective_schema_evidence
                .map(|evidence| partition_schema_disposition(partition, evidence, false))
                .transpose()?;
            match disposition {
                Some(PartitionSchemaDisposition::Quarantined(quarantine)) => {
                    terminal.push((ExecutablePartition::inline(partition.clone()), quarantine));
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
                        executable: ExecutablePartition::inline(partition.clone()),
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
                    candidate.partition().partition_id.as_str(),
                ))
                .map_err(|error| CdfError::internal(error.to_string()))?;
            }
        }
        None
    };

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

    let (payload_eligible_partition_count, selection_plan) = if external_tasks {
        payload_eligible_partition_count.ok_or_else(|| {
            CdfError::internal("external preview payload accounting was not initialized")
        })?
    } else {
        let count = u64::try_from(payload_candidates.len())
            .map_err(|_| CdfError::data("preview payload count exceeds u64"))?;
        let selection = if payload_candidates.is_empty() {
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
        (count, selection)
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
            if remaining_rows > 0
                && remaining_bytes > 0
                && remaining_batches > 0
                && remaining_query_limit != Some(0)
            {
                let mut opening = resource.open_executable(candidate.executable.clone());
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
                        && remaining_query_limit != Some(0)
                    {
                        let Some(batch) = stream.next().await else {
                            complete = true;
                            break;
                        };
                        let mut batch = batch?;
                        validate_batch_partition_ownership(
                            &batch,
                            &plan.scan.request.resource_id,
                            candidate.partition(),
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
                                    candidate.partition(),
                                ),
                                expected: candidate.expected.as_ref(),
                                expected_physical_observation: preobserved_physical_observation(
                                    plan.effective_schema_evidence.as_ref(),
                                    candidate.expected.as_ref(),
                                )?,
                                effective_schema: &admission_schema,
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
                        let physical_reconciliations = batch.header.take_physical_reconciliations();
                        validate_physical_reconciliations(
                            &record_batch,
                            physical_reconciliations,
                            &batch.header.batch_id,
                            reconciled.observation_id.as_deref(),
                        )?;
                        let pre_contract_quarantined_rows =
                            pre_contract_quarantine_summary(&batch.header.pre_contract_quarantine)
                                .quarantined_rows;
                        let residual_candidates = batch.header.take_residual_candidates();
                        validate_cdc_batch_authority(&plan.write_disposition, &batch.header)?;
                        let residual_preflight = preflight_residual_quarantines(
                            &plan.validation_program,
                            residual_candidates,
                            &ResidualBatchContext {
                                evaluation: &evaluation_context,
                                source_rows: None,
                                cdc_operation_field: None,
                                batch_id: &batch.header.batch_id,
                                observation_id: candidate
                                    .expected
                                    .as_ref()
                                    .map(|evidence| evidence.observation_id.as_str()),
                            },
                        )?;
                        let residual_candidates = residual_preflight.remaining_candidates;
                        let track_source_rows = pre_contract_may_filter
                            || !residual_candidates.is_empty()
                            || !residual_preflight.quarantined_batch_rows.is_empty()
                            || plan
                                .relational_expression_plan
                                .as_ref()
                                .is_some_and(|relational| relational.filter.is_some());
                        let expression_bytes = expression_working_set_bytes(
                            plan.compiled_expression_plan
                                .residuals
                                .iter()
                                .chain(plan.compiled_expression_plan.transforms.iter())
                                .map(|planned| &planned.expression)
                                .chain(plan.relational_expression_plan.iter().flat_map(
                                    |relational| {
                                        relational.filter.iter().chain(
                                            relational
                                                .projection
                                                .iter()
                                                .map(|projection| &projection.expression),
                                        )
                                    },
                                )),
                            record_batch.num_rows(),
                        )?;
                        let transform_memory_lease = reserve_transform_working_set(
                            Some(&preview_memory),
                            &record_batch,
                            &residual_candidates,
                            expression_bytes,
                        )
                        .await?;
                        let (record_batch, relational_source_rows) = match &bound_relational {
                            Some(relational) => {
                                let memory = transform_memory_lease.as_ref().ok_or_else(|| {
                                    CdfError::internal(
                                        "relational execution omitted its reserved memory lease",
                                    )
                                })?;
                                let (output, rows) =
                                    execute_bound_relational_expression_plan_tracked(
                                        relational,
                                        &record_batch,
                                        memory,
                                        &cdf_runtime::RunCancellation::default(),
                                    )?;
                                (output, Some(rows))
                            }
                            None => (record_batch, None),
                        };
                        let executed = execute_batch(
                            &record_batch,
                            if track_source_rows {
                                &bound_tracked_residuals
                            } else {
                                &bound_residuals
                            },
                            track_source_rows,
                            transform_memory_lease.as_ref(),
                            &cdf_runtime::RunCancellation::default(),
                        )?;
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
                            &mut remaining_query_limit,
                            track_source_rows,
                            transform_memory_lease.as_ref(),
                            &cdf_runtime::RunCancellation::default(),
                        )?;
                        let source_rows = remap_relational_source_rows(
                            source_rows,
                            relational_source_rows.as_deref(),
                        )?;
                        let (output, source_rows) = remove_preflight_quarantined_rows(
                            output,
                            source_rows,
                            &residual_preflight.quarantined_batch_rows,
                        )?;
                        let mut discard_quarantine = |_record: QuarantineRecord| Ok(());
                        for record in residual_preflight.quarantine_records {
                            discard_quarantine(record)?;
                        }
                        let mut contract = apply_contract_exec(
                            output,
                            &mut contract_evaluator,
                            &mut discard_quarantine,
                            residual_candidates,
                            &ResidualBatchContext {
                                evaluation: &evaluation_context,
                                source_rows: source_rows.as_deref(),
                                cdc_operation_field: None,
                                batch_id: &batch.header.batch_id,
                                observation_id: candidate
                                    .expected
                                    .as_ref()
                                    .map(|evidence| evidence.observation_id.as_str()),
                            },
                            TransformKernelMode::Fused,
                            transform_memory_lease,
                        )?;
                        merge_verdict_summary(&mut contract.summary, residual_preflight.summary);
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
                selected_but_uninspected.push(candidate.partition().partition_id.to_string());
                payload_uninspected.push(candidate.partition().partition_id.to_string());
            } else {
                inspected_partition_count += 1;
                if !complete {
                    partially_inspected.push(candidate.partition().partition_id.to_string());
                }
            }
            selected_evidence.push(crate::EnginePreviewSelectedPartition {
                partition_id: candidate.partition().partition_id.to_string(),
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
        payload_uninspected.push(candidate.partition().partition_id.to_string());
    }
    payload_uninspected.sort();
    payload_uninspected.dedup();
    selected_but_uninspected.sort();
    partially_inspected.sort();
    let uninspected_ids = payload_uninspected.iter().cloned().collect::<BTreeSet<_>>();
    for candidate in &payload_candidates {
        if !uninspected_ids.contains(candidate.partition().partition_id.as_str()) {
            continue;
        }
        if optional_preview_attestation(
            resource,
            &candidate.executable,
            candidate.expected.as_ref(),
            &mut observation_attestations,
        )
        .await?
        {
            attested_partition_count += 1;
        }
    }

    let partially_inspected_partition_count = u64::try_from(partially_inspected.len())
        .map_err(|error| CdfError::internal(error.to_string()))?;
    let payload_uninspected_partition_count = payload_eligible_partition_count
        .checked_sub(inspected_partition_count)
        .ok_or_else(|| {
            CdfError::internal("preview inspected more partitions than were eligible")
        })?;
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
    executable: ExecutablePartition,
    expected: Option<EffectiveSchemaObservationCoercion>,
    location: String,
    bounded_identity: StratifiedHashBoundedIdentity,
}

impl PreviewPayloadCandidate {
    fn partition(&self) -> &PartitionPlan {
        self.executable.plan()
    }
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
                Some(cdf_kernel::partition_schema_observation_binding(partition)?.to_string()),
                StratifiedHashIdentityStrength::BoundedObservation,
            )
        } else {
            (None, StratifiedHashIdentityStrength::Unavailable)
        }
    } else {
        (
            Some(cdf_kernel::partition_schema_observation_binding(partition)?.to_string()),
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
    partition: &ExecutablePartition,
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
                .attest_executable(partition.clone())
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
    partition: &ExecutablePartition,
    expected: Option<&EffectiveSchemaObservationCoercion>,
    cache: &mut BTreeMap<String, PartitionAttestation>,
) -> Result<bool>
where
    R: ResourceStream + ?Sized,
{
    let observation_id = partition
        .plan()
        .metadata
        .get(PLAN_SCHEMA_OBSERVATION_ID_KEY);
    let cached = observation_id.and_then(|id| cache.get(id)).cloned();
    let attestation = match cached {
        Some(attestation) => Some(attestation),
        None => resource.attest_executable(partition.clone()).await?,
    };
    let Some(attestation) = attestation else {
        return Ok(false);
    };
    if let Some(observation_id) = observation_id {
        cache.insert(observation_id.clone(), attestation.clone());
    }
    if let (Some(expected), Some(attested_schema_hash)) =
        (expected, attestation.physical_schema_hash())
        && attested_schema_hash != &expected.physical_schema_hash
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

fn validate_cdc_batch_authority(
    disposition: &WriteDisposition,
    header: &cdf_kernel::BatchHeader,
) -> Result<()> {
    if let Some(marker) = &header.cdc_settlement {
        if !matches!(disposition, WriteDisposition::CdcApply) {
            return Err(CdfError::contract(
                "CDC settlement boundaries require cdc_apply disposition",
            ));
        }
        marker.validate()?;
        if header.row_count != 0
            || header.byte_count != 0
            || header.cdc.is_some()
            || header.partition_idleness.is_some()
            || !header.watermarks.is_empty()
            || header.source_position.as_ref() != Some(&marker.position)
        {
            return Err(CdfError::data(
                "CDC settlement boundary must be a zero-row, zero-byte control batch with matching source position and no operation, idleness, or watermark metadata",
            ));
        }
        return Ok(());
    }
    match (&header.cdc, disposition) {
        (Some(metadata), WriteDisposition::Merge | WriteDisposition::CdcApply) => {
            metadata.validate(header.row_count, header.source_position.as_ref())
        }
        (Some(_), WriteDisposition::Append | WriteDisposition::Replace) => Err(CdfError::contract(
            "append and replace resources cannot admit CDC operation batches",
        )),
        (None, WriteDisposition::CdcApply) => Err(CdfError::data(
            "cdc_apply requires operation metadata on every admitted batch",
        )),
        (None, WriteDisposition::Append | WriteDisposition::Replace | WriteDisposition::Merge) => {
            Ok(())
        }
    }
}

fn settlement_unit_kind(
    kind: cdf_kernel::CdcSettlementUnitKind,
) -> cdf_runtime::SettlementUnitKind {
    match kind {
        cdf_kernel::CdcSettlementUnitKind::CommittedTransaction => {
            cdf_runtime::SettlementUnitKind::CommittedTransaction
        }
        cdf_kernel::CdcSettlementUnitKind::EventPrefix => {
            cdf_runtime::SettlementUnitKind::EventPrefix
        }
    }
}

fn observe_cdc_order_identity(
    observed: &mut Option<(String, String)>,
    header: &cdf_kernel::BatchHeader,
) -> Result<()> {
    let Some(metadata) = &header.cdc else {
        return Ok(());
    };
    let identity = metadata.position.cdc_protocol_order_identity()?;
    if observed
        .as_ref()
        .is_some_and(|current| current != &identity)
    {
        return Err(CdfError::data(
            "one package cannot combine CDC batches from different source-protocol scopes",
        ));
    }
    *observed = Some(identity);
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
                record_batch: canonicalize_admitted_batch_schema(record_batch, effective_schema)?,
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
                    record_batch,
                    effective_schema,
                    batch.header.residual_candidates(),
                )?;
                return Ok(BatchSchemaDisposition::Admitted(AdmittedBatchSchema {
                    record_batch: canonicalize_admitted_batch_schema(
                        record_batch,
                        effective_schema,
                    )?,
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
                    record_batch,
                    effective_schema,
                    batch.header.residual_candidates(),
                )?;
                return Ok(BatchSchemaDisposition::Admitted(AdmittedBatchSchema {
                    record_batch: canonicalize_admitted_batch_schema(
                        record_batch,
                        effective_schema,
                    )?,
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
                    record_batch,
                    effective_schema,
                    batch.header.residual_candidates(),
                )?;
                let physical_schema_hash =
                    cdf_kernel::canonical_arrow_schema_hash(&physical_schema)?;
                let compiled = admission.instantiate(&physical_schema, &physical_schema_hash)?;
                return Ok(BatchSchemaDisposition::Admitted(AdmittedBatchSchema {
                    record_batch: canonicalize_admitted_batch_schema(
                        record_batch,
                        effective_schema,
                    )?,
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
                    record_batch: canonicalize_admitted_batch_schema(
                        record_batch,
                        effective_schema,
                    )?,
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
                                quarantine,
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

fn canonicalize_admitted_batch_schema(
    batch: &RecordBatch,
    effective: &Schema,
) -> Result<RecordBatch> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .zip(effective.fields())
        .map(|(observed, effective)| {
            Arc::new(
                effective
                    .as_ref()
                    .clone()
                    .with_nullable(observed.is_nullable()),
            )
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        effective.metadata().clone(),
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(CdfError::from)
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
    accepted_source_rows: Option<Vec<usize>>,
    variant_values: Vec<Option<String>>,
    summary: VerdictSummary,
    residual_decisions: Vec<ResidualDecisionArtifact>,
    memory_lease: Option<MemoryLease>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PhysicalReconciliationArtifact {
    version: u16,
    observation_id: Option<String>,
    batch_id: cdf_kernel::BatchId,
    source_path: Vec<String>,
    observed_field: FieldTypeEvidenceArtifact,
    expected_field: FieldTypeEvidenceArtifact,
    row_count: u64,
    row_ranges: Vec<PhysicalReconciliationRowRange>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PhysicalReconciliationRowRange {
    start: u64,
    end_exclusive: u64,
}

#[derive(Default)]
struct PhysicalReconciliationEvidenceAccumulator {
    artifact: Option<cdf_package::StreamingIdentityArtifact>,
    count: u64,
}

impl PhysicalReconciliationEvidenceAccumulator {
    fn push(
        &mut self,
        builder: &PackageBuilder,
        reconciliations: Vec<PhysicalReconciliationArtifact>,
    ) -> Result<()> {
        if reconciliations.is_empty() {
            return Ok(());
        }
        if self.artifact.is_none() {
            let mut artifact = builder
                .begin_streaming_identity_artifact("schema/physical-reconciliations.json")?;
            artifact.write_all(b"{\"version\":1,\"reconciliations\":[")?;
            self.artifact = Some(artifact);
        }
        let artifact = self.artifact.as_mut().ok_or_else(|| {
            CdfError::internal("physical reconciliation evidence artifact is unavailable")
        })?;
        for reconciliation in reconciliations {
            if self.count != 0 {
                artifact.write_all(b",")?;
            }
            artifact.write_all(&cdf_package::canonical_json_bytes(&reconciliation)?)?;
            self.count = self
                .count
                .checked_add(1)
                .ok_or_else(|| CdfError::data("physical reconciliation count overflow"))?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        let Some(mut artifact) = self.artifact.take() else {
            return Ok(());
        };
        artifact.write_all(b"]}")?;
        artifact.finish().map(|_| ())
    }
}

#[derive(Default)]
struct LateDataEvidenceAccumulator {
    artifact: Option<cdf_package::StreamingIdentityArtifact>,
    batch_count: u64,
}

impl LateDataEvidenceAccumulator {
    fn push(&mut self, builder: &PackageBuilder, batch: &LateDataBatchEvidence) -> Result<()> {
        batch.validate()?;
        if self.artifact.is_none() {
            let mut artifact =
                builder.begin_streaming_identity_artifact(LATE_DATA_EVIDENCE_FILE)?;
            artifact.write_all(b"{\"batches\":[")?;
            self.artifact = Some(artifact);
        }
        let artifact = self
            .artifact
            .as_mut()
            .ok_or_else(|| CdfError::internal("late-data evidence artifact is unavailable"))?;
        if self.batch_count != 0 {
            artifact.write_all(b",")?;
        }
        artifact.write_all(&cdf_package::canonical_json_bytes(batch)?)?;
        self.batch_count = self
            .batch_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("late-data evidence batch count overflow"))?;
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        let Some(mut artifact) = self.artifact.take() else {
            return Ok(());
        };
        artifact.write_all(format!("],\"version\":{LATE_DATA_EVIDENCE_VERSION}}}").as_bytes())?;
        artifact.finish()?;
        Ok(())
    }
}

#[derive(Default)]
struct LateDataPayloadCatalogAccumulator {
    artifact: Option<cdf_package::StreamingIdentityArtifact>,
    artifact_count: u64,
}

impl LateDataPayloadCatalogAccumulator {
    const fn next_ordinal(&self) -> u64 {
        self.artifact_count
    }

    fn push(&mut self, builder: &PackageBuilder, payload: &LateDataPayloadArtifact) -> Result<u64> {
        payload.validate()?;
        if payload.artifact_ordinal != self.artifact_count {
            return Err(CdfError::internal(
                "late-data payload artifacts must enter canonical ordinal order",
            ));
        }
        if self.artifact.is_none() {
            let mut artifact =
                builder.begin_streaming_identity_artifact(LATE_DATA_PAYLOAD_CATALOG_FILE)?;
            artifact.write_all(b"{\"artifacts\":[")?;
            self.artifact = Some(artifact);
        }
        let artifact = self
            .artifact
            .as_mut()
            .ok_or_else(|| CdfError::internal("late-data payload catalog is unavailable"))?;
        if self.artifact_count != 0 {
            artifact.write_all(b",")?;
        }
        artifact.write_all(&cdf_package::canonical_json_bytes(payload)?)?;
        let ordinal = self.artifact_count;
        self.artifact_count = self
            .artifact_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("late-data payload artifact count overflow"))?;
        Ok(ordinal)
    }

    fn finish(mut self) -> Result<()> {
        let Some(mut artifact) = self.artifact.take() else {
            return Ok(());
        };
        artifact
            .write_all(format!("],\"version\":{LATE_DATA_PAYLOAD_CATALOG_VERSION}}}").as_bytes())?;
        artifact.finish()?;
        Ok(())
    }
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
    let row_rules_quarantine = matches!(
        program.disposition_for(cdf_contract::RuleOutcome::Violation, "quarantine-admission"),
        cdf_contract::RuleDisposition::Quarantine { .. }
    ) && program
        .row_rules
        .iter()
        .any(|rule| !rule.is_dedup_expression());
    row_rules_quarantine
        || program.admission.field == FieldDisposition::QuarantineRow
        || program.admission.record == RecordViolationDisposition::QuarantineRecord
        || program.residual.as_ref().is_some_and(|residual| {
            residual
                .fields
                .iter()
                .any(|field| field.disposition == FieldDisposition::QuarantineRow)
        })
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
    kind: cdf_kernel::PackageSegmentKind,
    partition_ordinal: u64,
    output: RecordBatch,
    output_position: Option<SourcePosition>,
    _memory_lease: Option<MemoryLease>,
}

struct AppliedDedup {
    summary: cdf_contract::DedupSummary,
    input_effects: cdf_kernel::KeyedEffectCounts,
    surviving_effects: cdf_kernel::KeyedEffectCounts,
}

struct ExternalDedupState {
    index: crate::dedup_spill::ExternalDedupIndex,
    payload: crate::dedup_spill::DedupPayloadSpool,
    effect_sort: Option<crate::dedup_spill::EffectSortSpool>,
}

struct PreparedOutputBatch {
    output: RecordBatch,
    variant_values: Vec<Option<String>>,
    memory_lease: Option<MemoryLease>,
}

struct PreparedKernelOutput {
    output: RecordBatch,
    memory_lease: Option<MemoryLease>,
}

struct OutputWriteState<'a> {
    kind: cdf_kernel::PackageSegmentKind,
    profile: &'a mut ExecutionProfile,
    segment_positions: &'a mut Vec<EngineSegmentPosition>,
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
    routing: Option<&'a mut RoutedWriteState>,
}

struct RoutedWriteState {
    family: cdf_kernel::RouteTargetFamily,
    segmentation: crate::CanonicalSegmentationPolicy,
    outputs: Vec<RoutedOutputWriteState>,
}

struct RoutedOutputWriteState {
    active: Option<(
        u64,
        cdf_kernel::PackageSegmentKind,
        crate::CanonicalSegmentAssembler,
    )>,
    segment_ids: Vec<cdf_kernel::SegmentId>,
    input_effects: cdf_kernel::KeyedEffectCounts,
    surviving_effects: cdf_kernel::KeyedEffectCounts,
}

struct SegmentEncodeWork {
    ordinal: u64,
    kind: cdf_kernel::PackageSegmentKind,
    segment_id: cdf_kernel::SegmentId,
    package_row_ord_start: u64,
    partition_ordinal: u64,
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
        maximum_in_flight: usize,
    ) -> Result<Self> {
        let mode = match services {
            Some(services) if maximum_in_flight > 0 => {
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
                    work.kind,
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
                                        work.kind,
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
                kind: _,
                segment_id: _,
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
                write.metrics.encode_duration_ns,
                normalization_output_bytes,
                write.metrics.segment.byte_count,
            );
            state.phase_measurements.add(
                RunPhase::PersistHash,
                write.metrics.persist_hash_duration_ns,
                write.metrics.segment.byte_count,
                write.metrics.segment.byte_count,
            );
            let segment = write.metrics.segment;
            let durable_file = write.durable_file;
            if let Some(lease) = _scratch_memory_lease {
                _transform_memory_leases.push(lease);
            }
            durable_segment.observe(
                &segment,
                DurableSegmentPayload {
                    durable_file,
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
            state.segment_positions.push(EngineSegmentPosition {
                segment_id: segment.segment_id.clone(),
                partition_ordinal,
                output_position,
            });
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PipelineConcurrency {
    pub(crate) source_jobs: usize,
    pub(crate) segment_encode_jobs: usize,
}

fn resolve_pipeline_concurrency(
    plan: &EnginePlan,
    options: &EngineExecutionInvocation,
    requested_source_jobs: usize,
    maximum_segment_bytes: u64,
    staged_handoff: bool,
) -> Result<PipelineConcurrency> {
    let Some(services) = options.services.as_ref() else {
        return Ok(PipelineConcurrency {
            source_jobs: requested_source_jobs,
            segment_encode_jobs: 0,
        });
    };
    if requested_source_jobs == 0 {
        return Ok(PipelineConcurrency {
            source_jobs: 0,
            segment_encode_jobs: 0,
        });
    }
    let admission = &plan
        .partition_schedule
        .as_ref()
        .ok_or_else(|| {
            CdfError::contract("pipeline admission requires a compiled partition schedule")
        })?
        .admission;
    let source_minimum_working_set_bytes = admission.minimum_working_set_bytes;
    let source_working_set_bytes = admission.maximum_working_set_bytes;
    let snapshot = services.memory().snapshot();
    let available_bytes = snapshot.budget_bytes.saturating_sub(snapshot.current_bytes);
    require_source_minimum_headroom(available_bytes, source_minimum_working_set_bytes)?;
    let requested_encode_jobs =
        usize::from(services.capabilities().logical_cpu_slots.saturating_sub(1));
    if requested_encode_jobs == 0 {
        return Ok(PipelineConcurrency {
            source_jobs: requested_source_jobs,
            segment_encode_jobs: 0,
        });
    }
    let compiled_source = plan.compiled_source_execution.as_ref().ok_or_else(|| {
        CdfError::contract("pipeline admission requires a compiled source execution plan")
    })?;
    let staged_handoff_bytes = if staged_handoff {
        plan.operator_graph
            .as_ref()
            .and_then(|graph| {
                graph
                    .nodes
                    .iter()
                    .find(|node| node.kind == cdf_runtime::GraphNodeKind::StagedIngress)
            })
            .map(|node| {
                maximum_segment_bytes
                    .checked_mul(u64::from(node.maximum_concurrency))
                    .map(|retained_bytes| node.maximum_working_set_bytes.max(retained_bytes))
                    .ok_or_else(|| CdfError::data("staged ingress working set overflow"))
            })
            .transpose()?
            .unwrap_or(maximum_segment_bytes)
    } else {
        0
    };
    if compiled_source.batch_memory_contract()
        == cdf_runtime::SourceBatchMemoryContract::FrontierReserved
    {
        let encode_working_set_bytes = maximum_segment_bytes
            .checked_mul(3)
            .ok_or_else(|| CdfError::data("segment encode working set overflow"))?;
        let parallel_floor = maximum_segment_bytes
            .checked_add(encode_working_set_bytes)
            .and_then(|bytes| bytes.checked_add(staged_handoff_bytes))
            .and_then(|bytes| bytes.checked_add(source_working_set_bytes))
            .ok_or_else(|| CdfError::data("parallel pipeline working set overflow"))?;
        if available_bytes < parallel_floor {
            // Frontier-reserved producers cannot overrun their one-batch queue authority. When
            // the full parallel encode topology cannot fit, retain every source job that can
            // coexist with a maximum canonical segment and encode inline. Tiny runs remain
            // parallel at the source edge; a real oversized encode still fails through the exact
            // allocation ledger instead of waiting on another operator.
            let source_capacity = available_bytes
                .saturating_sub(staged_handoff_bytes)
                .saturating_sub(maximum_segment_bytes)
                / source_working_set_bytes;
            return Ok(PipelineConcurrency {
                source_jobs: requested_source_jobs.min(
                    usize::try_from(source_capacity)
                        .unwrap_or(usize::MAX)
                        .max(1),
                ),
                segment_encode_jobs: 0,
            });
        }
    }
    resolve_pipeline_concurrency_from_bounds(
        requested_source_jobs,
        requested_encode_jobs,
        available_bytes,
        source_minimum_working_set_bytes,
        source_working_set_bytes,
        maximum_segment_bytes,
        staged_handoff_bytes,
    )
}

fn require_source_minimum_headroom(
    available_bytes: u64,
    source_minimum_working_set_bytes: u64,
) -> Result<()> {
    if source_minimum_working_set_bytes == 0 {
        return Err(CdfError::contract(
            "pipeline admission requires a nonzero source minimum working-set bound",
        ));
    }
    if available_bytes < source_minimum_working_set_bytes {
        return Err(CdfError::data(format!(
            "source execution requires at least {source_minimum_working_set_bytes} managed bytes after resident destination working sets, but only {available_bytes} bytes are free",
        )));
    }
    Ok(())
}

pub(crate) fn resolve_pipeline_concurrency_from_bounds(
    requested_source_jobs: usize,
    requested_encode_jobs: usize,
    available_bytes: u64,
    source_minimum_working_set_bytes: u64,
    source_working_set_bytes: u64,
    segment_admission_bytes: u64,
    staged_handoff_bytes: u64,
) -> Result<PipelineConcurrency> {
    if requested_source_jobs == 0 {
        return Ok(PipelineConcurrency {
            source_jobs: 0,
            segment_encode_jobs: 0,
        });
    }
    require_source_minimum_headroom(available_bytes, source_minimum_working_set_bytes)?;
    if source_working_set_bytes == 0 || segment_admission_bytes == 0 {
        return Err(CdfError::contract(
            "pipeline admission requires nonzero source and segment working-set bounds",
        ));
    }
    let encode_working_set_bytes = segment_admission_bytes
        .checked_mul(3)
        .ok_or_else(|| CdfError::data("segment encode working set overflow"))?;
    let parallel_floor = segment_admission_bytes
        .checked_add(encode_working_set_bytes)
        .and_then(|bytes| bytes.checked_add(staged_handoff_bytes))
        .and_then(|bytes| bytes.checked_add(source_working_set_bytes))
        .ok_or_else(|| CdfError::data("parallel pipeline working set overflow"))?;

    if requested_encode_jobs > 0 && available_bytes >= parallel_floor {
        // Preserve source fan-out first, because it owns transport/decode overlap. One encoder is
        // the irreducible parallel sink; remaining capacity expands encoder fan-out without
        // compromising the canonical head's next source poll.
        let source_capacity = available_bytes
            .saturating_sub(staged_handoff_bytes)
            .saturating_sub(segment_admission_bytes)
            .saturating_sub(encode_working_set_bytes)
            / source_working_set_bytes;
        let source_jobs = requested_source_jobs.min(
            usize::try_from(source_capacity)
                .unwrap_or(usize::MAX)
                .max(1),
        );
        let used_by_source = u64::try_from(source_jobs)
            .ok()
            .and_then(|jobs| jobs.checked_mul(source_working_set_bytes))
            .ok_or_else(|| CdfError::data("source frontier working set overflow"))?;
        let remaining = available_bytes
            .saturating_sub(staged_handoff_bytes)
            .saturating_sub(segment_admission_bytes)
            .saturating_sub(used_by_source);
        let encode_capacity = remaining / encode_working_set_bytes;
        let segment_encode_jobs = requested_encode_jobs.min(
            usize::try_from(encode_capacity)
                .unwrap_or(usize::MAX)
                .max(1),
        );
        return Ok(PipelineConcurrency {
            source_jobs,
            segment_encode_jobs,
        });
    }

    // A single-threaded encoder does not coexist with a growing canonical segment, but later
    // prefetched source partitions remain live while the canonical head encodes. Resolve that
    // topology independently rather than pretending the parallel floor is mandatory.
    let execution_bytes = available_bytes.saturating_sub(staged_handoff_bytes);
    let assembly_capacity =
        execution_bytes.saturating_sub(segment_admission_bytes) / source_working_set_bytes;
    let encode_prefetch_capacity = if execution_bytes >= encode_working_set_bytes {
        1_u64.saturating_add(
            execution_bytes.saturating_sub(encode_working_set_bytes) / source_working_set_bytes,
        )
    } else {
        0
    };
    let inline_capacity = assembly_capacity.min(encode_prefetch_capacity);
    if inline_capacity == 0 {
        // The admission estimate is not a mandatory allocation. A small input can remain far below
        // it, so do not reject the run from a hypothetical peak. Serial execution has no
        // speculative holder and the ordinary per-allocation ledger checks remain the exact
        // authority if a real batch or segment cannot fit.
        return Ok(PipelineConcurrency {
            source_jobs: 1,
            segment_encode_jobs: 0,
        });
    }
    Ok(PipelineConcurrency {
        source_jobs: requested_source_jobs.min(
            usize::try_from(inline_capacity)
                .unwrap_or(usize::MAX)
                .max(1),
        ),
        segment_encode_jobs: 0,
    })
}

const DEDUP_PROVENANCE_SHARD_ROWS: usize = 64 * 1024;

struct DedupProvenanceSink {
    rows: Vec<(u64, u64)>,
    shard_count: u64,
}

impl DedupProvenanceSink {
    fn new() -> Self {
        Self {
            rows: Vec::with_capacity(DEDUP_PROVENANCE_SHARD_ROWS),
            shard_count: 0,
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

    fn finish(mut self, builder: &PackageBuilder) -> Result<u64> {
        self.flush(builder)?;
        Ok(self.shard_count)
    }

    fn flush(&mut self, builder: &PackageBuilder) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.shard_count = self
            .shard_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("dedup provenance shard count overflow"))?;
        builder.write_dedup_provenance_shard(self.shard_count, &self.rows)?;
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
        crate::StreamAdmissionCompletion::Pending,
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
        Vec::new(),
        None,
        standalone_execution_options()?,
    )
    .await?
    .into_package()?
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
        Vec::new(),
        None,
        standalone_execution_options()?,
    )
    .instrument(package_execution_span(&trace_context))
    .await?
    .into_package()?
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
        Vec::new(),
        None,
        standalone_execution_options()?,
    )
    .await?
    .into_package()
}

pub async fn execute_to_package_with_segment_positions_and_pre_finalize<R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    options: EngineExecutionInvocation,
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
        Vec::new(),
        None,
        options,
    )
    .await?
    .into_package()
}

pub async fn execute_to_package_with_streaming_hooks<'a, R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    durable_segment: &'a mut DurableSegmentHook<'a>,
    stream_finalize: &'a mut StreamingFinalizeHook<'a>,
    options: EngineExecutionInvocation,
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
        Some(durable_segment),
        Some(stream_finalize),
        Vec::new(),
        None,
        options,
    )
    .await?
    .into_package()
}

pub async fn execute_to_package_with_progress_hook<'a, R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    package_progress: &'a mut PackageSegmentProgressHook<'a>,
    options: EngineExecutionInvocation,
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
        Some(package_progress),
        None,
        None,
        Vec::new(),
        None,
        options,
    )
    .await?
    .into_package()
}

pub async fn execute_drain_epoch_with_hooks<'a, R>(
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    epoch: DrainEpochExecution<'a>,
    options: EngineExecutionInvocation,
) -> Result<EngineDrainEpochOutcome>
where
    R: ResourceStream + ?Sized,
{
    match Box::pin(execute_to_package_inner(
        None,
        plan,
        resource,
        package_dir,
        Some(pre_finalize),
        epoch.package_progress,
        epoch.durable_segment,
        epoch.stream_finalize,
        epoch.late_data_carryover,
        Some(epoch.controller),
        options,
    ))
    .await?
    {
        PackageExecutionOutcome::Package(output) => Ok(EngineDrainEpochOutcome::Package(output)),
        PackageExecutionOutcome::DrainFinishedNoOp { source_frontier } => {
            Ok(EngineDrainEpochOutcome::FinishedNoOp { source_frontier })
        }
    }
}

enum DrainAwarePoll<T> {
    Ready(T),
    Timer(cdf_runtime::DrainEpochDecision),
}

struct DrainExecutionClock {
    controller_base_milliseconds: u64,
    host_started: Option<Duration>,
    local_started: Instant,
}

impl DrainExecutionClock {
    fn new(
        controller: Option<&cdf_runtime::DrainEpochController>,
        services: Option<&cdf_runtime::ExecutionServices>,
    ) -> Self {
        Self {
            controller_base_milliseconds: controller
                .map_or(0, cdf_runtime::DrainEpochController::monotonic_milliseconds),
            host_started: services.map(cdf_runtime::ExecutionServices::monotonic_now),
            local_started: Instant::now(),
        }
    }

    fn monotonic_milliseconds(&self, services: Option<&cdf_runtime::ExecutionServices>) -> u64 {
        let elapsed = match (services, self.host_started) {
            (Some(services), Some(started)) => services
                .monotonic_now()
                .checked_sub(started)
                .unwrap_or_default(),
            _ => self.local_started.elapsed(),
        };
        self.controller_base_milliseconds
            .saturating_add(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
    }

    fn observed_at_unix_milliseconds(
        &self,
        services: Option<&cdf_runtime::ExecutionServices>,
    ) -> Result<u64> {
        match services {
            Some(services) => u64::try_from(services.unix_now().as_millis())
                .map_err(|error| CdfError::internal(error.to_string())),
            None => current_observed_at_u64_ms(),
        }
    }
}

async fn poll_with_drain_timer<F, T>(
    operation: F,
    controller: Option<&mut cdf_runtime::DrainEpochController>,
    services: Option<&cdf_runtime::ExecutionServices>,
    cancellation: &cdf_runtime::RunCancellation,
    clock: &DrainExecutionClock,
) -> Result<DrainAwarePoll<T>>
where
    F: Future<Output = Result<T>>,
{
    let Some(controller) = controller else {
        return operation.await.map(DrainAwarePoll::Ready);
    };
    let mut operation = Box::pin(operation);
    loop {
        let Some(delay_milliseconds) = controller.next_timer_delay_milliseconds()? else {
            return operation.await.map(DrainAwarePoll::Ready);
        };
        let services = services.ok_or_else(|| {
            CdfError::contract("time-bounded drain execution requires injected host timers")
        })?;
        let armed_at = controller.monotonic_milliseconds();
        let timer = services.delay(
            Duration::from_millis(delay_milliseconds),
            cancellation.clone(),
        );
        match futures_util::future::select(operation.as_mut(), timer).await {
            Either::Left((result, _)) => return result.map(DrainAwarePoll::Ready),
            Either::Right((timer_result, _)) => {
                timer_result?;
                let observed_at = clock
                    .monotonic_milliseconds(Some(services))
                    .max(armed_at.saturating_add(delay_milliseconds));
                let decision = controller.observe_timer(
                    observed_at,
                    clock.observed_at_unix_milliseconds(Some(services))?,
                )?;
                if !matches!(decision, cdf_runtime::DrainEpochDecision::Continue) {
                    return Ok(DrainAwarePoll::Timer(decision));
                }
            }
        }
    }
}

fn open_partition<'a, R>(
    resource: &'a R,
    ordinal: u64,
    partition: ExecutablePartition,
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
            retry_progress,
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
        let retry_schedule = if retry_state.is_some() {
            Some(scheduled.as_ref().ok_or_else(|| {
                CdfError::internal("retry state was initialized without a retry schedule")
            })?)
        } else {
            None
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
                            partition.plan().partition_id
                        ));
                        let state = retry_state.as_mut().ok_or_else(|| {
                            CdfError::internal("retry state disappeared before reattestation")
                        })?;
                        let schedule = retry_schedule.ok_or_else(|| {
                            CdfError::internal("retry schedule disappeared before reattestation")
                        })?;
                        schedule_partition_retry(
                            state,
                            &error,
                            cancellation.clone(),
                            &plan_id,
                            schedule,
                            &retry_journal,
                            retry_progress.as_deref(),
                        )
                        .await?;
                        continue;
                    }
                    Err(error) => {
                        let state = retry_state.as_mut().ok_or_else(|| {
                            CdfError::internal("retry state disappeared after attestation failure")
                        })?;
                        let schedule = retry_schedule.ok_or_else(|| {
                            CdfError::internal(
                                "retry schedule disappeared after attestation failure",
                            )
                        })?;
                        schedule_partition_retry(
                            state,
                            &error,
                            cancellation.clone(),
                            &plan_id,
                            schedule,
                            &retry_journal,
                            retry_progress.as_deref(),
                        )
                        .await?;
                        continue;
                    }
                }
            } else {
                None
            };
            let mut opening = resource.open_executable(partition.clone());
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
                                        retry_schedule.ok_or_else(|| {
                                            CdfError::internal(
                                                "retry schedule disappeared after stream failure",
                                            )
                                        })?,
                                        &retry_journal,
                                        retry_progress.as_deref(),
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
                                    retry_schedule.ok_or_else(|| {
                                        CdfError::internal(
                                            "retry schedule disappeared before stream retry",
                                        )
                                    })?,
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
                                retry_schedule.ok_or_else(|| {
                                    CdfError::internal(
                                        "retry schedule disappeared after open failure",
                                    )
                                })?,
                                &retry_journal,
                                retry_progress.as_deref(),
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
                        decision.ok_or_else(|| {
                            CdfError::internal("retry state did not produce a retry decision")
                        })?,
                        &error,
                        cancellation.clone(),
                        &plan_id,
                        retry_schedule.ok_or_else(|| {
                            CdfError::internal("retry schedule disappeared before open retry")
                        })?,
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
    partition: &ExecutablePartition,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<Option<cdf_kernel::PartitionAttestation>>
where
    R: ResourceStream + ?Sized,
{
    let mut attempt = resource.attest_executable(partition.clone());
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

fn scheduled_partition(
    schedule: Option<&cdf_runtime::CanonicalPartitionSchedule>,
    source: Option<&cdf_runtime::CompiledSourceExecutionPlan>,
    ordinal: u64,
    partition: &PartitionPlan,
) -> Result<Option<cdf_runtime::ScheduledPartition>> {
    let Some(schedule) = schedule else {
        return Ok(None);
    };
    let source = source.ok_or_else(|| {
        CdfError::contract("partition schedule requires compiled source execution authority")
    })?;
    schedule
        .scheduled_partition(source, ordinal, partition)
        .map(Some)
}

fn executable_partition_plans<R>(
    plan: &EnginePlan,
    resource: &R,
) -> Result<ExecutablePartitionPlans>
where
    R: ResourceStream + ?Sized,
{
    match plan.scan.external_task_set() {
        Some(reference) => Ok(ExecutablePartitionPlans::External(
            resource.planned_partition_reader(reference)?,
        )),
        None => Ok(ExecutablePartitionPlans::Inline(
            plan.scan
                .inline_partitions()
                .ok_or_else(|| CdfError::contract("inline partition authority is unavailable"))?
                .to_vec(),
        )),
    }
}

fn source_partition_opener<'a, R>(
    resource: &'a R,
    mut partitions: ExecutablePartitionPlans,
    effective_schema_evidence: Option<&'a EffectiveSchemaPlanEvidence>,
    schedule: Option<&'a cdf_runtime::CanonicalPartitionSchedule>,
    compiled_source: Option<&'a cdf_runtime::CompiledSourceExecutionPlan>,
    open_runtime: PartitionOpenRuntime,
) -> cdf_runtime::SourcePartitionOpener<'a, PartitionOpenMetadata>
where
    R: ResourceStream + ?Sized,
{
    let external_task_identity_authority =
        matches!(partitions, ExecutablePartitionPlans::External(_));
    Box::new(move |ordinal, cancellation| {
        let partition = partitions.next(ordinal)?;
        let terminal = effective_schema_evidence
            .map(|evidence| {
                partition_schema_disposition(
                    partition.plan(),
                    evidence,
                    external_task_identity_authority,
                )
            })
            .transpose()?
            .is_some_and(|disposition| {
                matches!(disposition, PartitionSchemaDisposition::Quarantined(_))
            });
        let scheduled = scheduled_partition(schedule, compiled_source, ordinal, partition.plan())?;
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

#[allow(clippy::too_many_arguments)]
async fn execute_to_package_inner<'a, R>(
    trace_context: Option<&ExecutionTraceContext>,
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: Option<&PackagePreFinalizeHook<'_>>,
    package_progress: Option<&'a mut PackageSegmentProgressHook<'a>>,
    durable_segment: Option<&'a mut DurableSegmentHook<'a>>,
    stream_finalize: Option<&'a mut StreamingFinalizeHook<'a>>,
    late_data_carryover_input: Vec<LateDataCarryoverInput>,
    mut drain_controller: Option<&mut cdf_runtime::DrainEpochController>,
    options: EngineExecutionInvocation,
) -> Result<PackageExecutionOutcome>
where
    R: ResourceStream + ?Sized,
{
    let planned_partition_count =
        validate_execution_invocation(plan, resource, drain_controller.as_deref(), &options)?;
    let validation_program = plan.validation_program.clone();
    let effective_schema_evidence = validate_effective_schema_plan(plan, resource)?;
    crate::planning::validate_plan_schema_authority(resource, plan)?;
    let resource_schema = resource.schema();
    let runtime_output_schema = plan.output_arrow_schema()?;
    let admission_schema = scan_expression_schema(
        resource_schema.as_ref(),
        plan.explain
            .projection_pushed
            .then_some(plan.scan.request.projection.as_deref())
            .flatten(),
    )?;
    let bound_relational = plan
        .relational_expression_plan
        .as_ref()
        .map(bind_relational_expression_plan)
        .transpose()?;
    let expression_schema = plan
        .relational_expression_plan
        .as_ref()
        .map(|relational| relational.output_schema.to_arrow())
        .transpose()?
        .unwrap_or_else(|| admission_schema.clone());
    let bound_residuals =
        bind_filter_expressions(&plan.compiled_expression_plan.residuals, &expression_schema)?;
    let tracking_expression_schema = source_row_tracking_schema(&expression_schema)?;
    let bound_tracked_residuals = bind_filter_expressions(
        &plan.compiled_expression_plan.residuals,
        &tracking_expression_schema,
    )?;
    let bound_transforms = bind_expression_transforms(
        &plan.validation_program.transforms,
        &plan.compiled_expression_plan.transforms,
        &expression_schema,
    )?;
    let bound_tracked_transforms = bind_expression_transforms(
        &plan.validation_program.transforms,
        &plan.compiled_expression_plan.transforms,
        &tracking_expression_schema,
    )?;
    let contract_schema = expression_transform_output_schema(
        &plan.validation_program.transforms,
        &plan.compiled_expression_plan.transforms,
        &expression_schema,
    )?;
    let pre_contract_may_filter = !bound_residuals.is_empty()
        || plan.validation_program.transforms.iter().any(|transform| {
            matches!(transform, cdf_contract::TransformDescription::Filter { .. })
        });
    let late_data_policy = match &plan.execution_extent {
        ExecutionExtent::Drain { policy, .. } => match &policy.watermark {
            WatermarkPolicy::Enabled {
                event_time_field, ..
            } => Some((event_time_field.clone(), policy.late_data)),
            WatermarkPolicy::Disabled => None,
        },
        ExecutionExtent::Bounded { .. } | ExecutionExtent::Resident { .. } => None,
    };

    let initial_content = planned_empty_package_content(plan)?;
    let package_segment_kind = match plan.write_disposition {
        WriteDisposition::Append | WriteDisposition::Replace => cdf_kernel::PackageSegmentKind::Row,
        WriteDisposition::Merge | WriteDisposition::CdcApply => {
            cdf_kernel::PackageSegmentKind::Upsert
        }
    };
    let builder = PackageBuilder::create(
        package_dir,
        plan.package_id.clone(),
        initial_content,
        package_builder_resources(options.services.as_ref())?,
    )?;
    builder.update_status(PackageStatus::Extracting)?;
    builder.write_json_artifact(cdf_package_contract::SCAN_PLAN_FILE, &plan.scan)?;
    builder.write_json_artifact("plan/explain.json", &plan.explain)?;
    if let Some(graph) = &plan.operator_graph {
        graph.validate_plan_join(&plan.execution_extent, plan.compiled_stream_policy.as_ref())?;
        builder.write_json_artifact("plan/operator-graph.json", graph)?;
    }
    builder.write_json_artifact("plan/validation-program.json", &validation_program)?;
    builder.write_json_artifact(
        cdf_package_contract::SCHEMA_ADMISSION_PROGRAM_FILE,
        &plan.schema_admission_program,
    )?;
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
    let mut source_transfer = cdf_kernel::SourceTransferReport::default();
    let mut statistics_memory_lease = None;
    let mut statistics_profile = options
        .statistics_profile
        .then(|| builder.begin_statistics_profile())
        .transpose()?;
    let statistics_profile_schema_hash = plan.output_schema.arrow_schema_hash.as_str().to_owned();
    let mut statistics_segment_ordinal = 0_u64;
    let mut verdict_summary = VerdictSummary::default();
    let mut lineage = LineageSummary::default();
    let mut segment_positions = Vec::new();
    let mut quarantine_part_count = 0_usize;
    let mut late_data_evidence = LateDataEvidenceAccumulator::default();
    let mut physical_reconciliation_evidence = PhysicalReconciliationEvidenceAccumulator::default();
    let mut late_data_payloads = LateDataPayloadCatalogAccumulator::default();
    let mut late_data_carryover = Vec::<cdf_kernel::LateDataCarryoverRef>::new();
    let mut remaining_limit = plan.final_limit.or(plan.scan.request.limit);
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
    let mut completion_positions = Vec::<(u64, PartitionPlan, SourcePosition)>::new();
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
    let package_dedup_rule = if matches!(
        plan.write_disposition,
        WriteDisposition::Merge | WriteDisposition::CdcApply
    ) {
        Some(effective_keyed_effect_rule(plan)?)
    } else if validation_program.has_exact_row_dedup_rule() {
        package_dedup_rule(&validation_program)?
    } else {
        None
    };
    let apply_package_dedup = package_dedup_rule.is_some();
    if apply_package_dedup
        && late_data_policy
            .as_ref()
            .is_some_and(|(_, action)| *action == cdf_kernel::LateDataAction::AdmitWithAnnotation)
    {
        return Err(CdfError::contract(
            "admit_with_annotation requires stable package-row ordinals and cannot be combined with package dedup; use quarantine or recapture_next_epoch, or disable package dedup",
        ));
    }
    let mut pending_dedup_batches = Vec::new();
    let mut next_package_output_row_ordinal = 0_u64;
    let mut cdc_order_identity = None::<(String, String)>;
    let mut cdc_settlement_runtime = None::<cdf_runtime::CdcLogSourceRuntime>;
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
                let index = crate::dedup_spill::ExternalDedupIndex::create(
                    builder.package_dir().join(".dedup-spill"),
                    services.spill(),
                    Some(services.memory()),
                )?;
                let payload = crate::dedup_spill::DedupPayloadSpool::create(
                    builder.package_dir().join(".dedup-payload"),
                    services.spill(),
                )?;
                let effect_sort = matches!(
                    plan.write_disposition,
                    WriteDisposition::Merge | WriteDisposition::CdcApply
                )
                .then(|| {
                    crate::dedup_spill::EffectSortSpool::create(
                        builder.package_dir().join(".effect-sort-spill"),
                        services.spill(),
                        services.memory(),
                    )
                })
                .transpose()?;
                Ok(ExternalDedupState {
                    index,
                    payload,
                    effect_sort,
                })
            })
            .transpose()?
    } else {
        None
    };
    let segmentation_policy = plan.segmentation_policy()?.clone();
    let mut routed_write = plan
        .route_family
        .clone()
        .map(|family| RoutedWriteState::new(family, segmentation_policy.clone()));
    let staged_handoff = durable_segment.is_some();
    let mut durable_segment_observer = DurableSegmentObserver {
        hook: durable_segment,
        progress: package_progress,
    };
    let requested_partition_jobs = if remaining_limit == Some(0) {
        0
    } else {
        partition_open_jobs(plan, &options)
    };
    let pipeline_concurrency = resolve_pipeline_concurrency(
        plan,
        &options,
        requested_partition_jobs,
        segmentation_policy.maximum_bytes,
        staged_handoff,
    )?;
    let mut segment_queue = SegmentEncodeQueue::new(
        &builder,
        options.services.as_ref(),
        phase_measurements.enabled,
        &plan.package_id,
        pipeline_concurrency.segment_encode_jobs,
    )?;

    let partition_jobs = pipeline_concurrency.source_jobs;
    let run_cancellation = options.cancellation.clone();
    let partition_open_runtime = PartitionOpenRuntime {
        services: options.services.clone(),
        cancellation: run_cancellation.clone(),
        retry_journal: options.retry_journal.clone(),
        retry_progress: options.source_retry_progress.clone(),
    };
    let frontier_partition_count = if partition_jobs == 0 {
        0
    } else {
        planned_partition_count
    };
    let executable_partitions = executable_partition_plans(plan, resource)?;
    let source_opener = source_partition_opener(
        resource,
        executable_partitions,
        effective_schema_evidence,
        plan.partition_schedule.as_ref(),
        plan.compiled_source_execution.as_ref(),
        partition_open_runtime,
    );
    let source_batch_bound = source_frontier_batch_bound(plan, frontier_partition_count)?;
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
        source_batch_bound,
        memory.clone(),
        source_batch_memory,
        run_cancellation.clone(),
    )?
    .with_measurement(options.phase_metrics);
    let drain_clock =
        DrainExecutionClock::new(drain_controller.as_deref(), options.services.as_ref());
    let mut partition_watermarks = match (&plan.execution_extent, drain_controller.as_ref()) {
        (ExecutionExtent::Drain { policy, .. }, Some(controller)) => {
            Some(cdf_runtime::PartitionWatermarkTracker::new_with_state(
                &policy.watermark,
                plan.scan
                    .inline_partitions()
                    .unwrap_or_default()
                    .iter()
                    .map(|partition| &partition.partition_id),
                drain_clock.monotonic_milliseconds(options.services.as_ref()),
                controller.committed_watermark().cloned(),
                controller.committed_partition_watermarks(),
            )?)
        }
        _ => None,
    };
    let drain_batch_frontiers_enabled = drain_controller.is_some()
        && !matches!(plan.write_disposition, WriteDisposition::CdcApply)
        && !plan
            .compiled_source_execution
            .as_ref()
            .ok_or_else(|| {
                CdfError::contract("drain execution requires compiled source authority")
            })?
            .execution_capabilities()
            .bounded;
    let mut drain_epoch_closure = None;
    let mut consumed_partition_count = 0_u64;
    let mut drain_finished_noop = false;
    let mut source_progress_observed = false;
    let mut source_progress_rows = 0_u64;
    let mut source_progress_bytes = 0_u64;
    let mut source_progress_batches = 0_u64;
    let mut last_drain_partition_resume = None::<Box<crate::DrainPartitionResume>>;
    let mut drain_partition_positions = drain_source_continuation_positions(
        drain_controller
            .as_deref()
            .and_then(cdf_runtime::DrainEpochController::committed_source_continuation),
        plan.scan.inline_partitions().unwrap_or_default(),
    )?;
    let mut carryover_progress_observed = false;

    let consumed_late_data_carryover = late_data_carryover_input
        .iter()
        .map(|input| input.reference.clone())
        .collect::<Vec<_>>();
    if !consumed_late_data_carryover.is_empty() {
        builder.write_json_artifact(
            "plan/late-data-carryover-input.json",
            &consumed_late_data_carryover,
        )?;
    }

    let segment_result: Result<()> = async {
    if !late_data_carryover_input.is_empty() {
        let controller = drain_controller.as_deref_mut().ok_or_else(|| {
            CdfError::contract("late-data carryover requires drain execution authority")
        })?;
        let carryover_frontier = controller.committed_frontier().cloned().ok_or_else(|| {
            CdfError::data(
                "late-data carryover requires the receipt-gated frontier that produced it",
            )
        })?;
        let carryover_partition_ordinal = planned_partition_count;
        let mut carryover_assembler = crate::CanonicalSegmentAssembler::new(
            segmentation_policy.clone(),
            carryover_partition_ordinal,
        )?;
        for input in late_data_carryover_input {
            let decode_window = input
                .reference
                .memory_bound_bytes
                .checked_add(input.reference.byte_count)
                .ok_or_else(|| CdfError::data("late-data carryover decode window overflow"))?;
            let lease = match memory.as_ref() {
                Some(memory) => {
                    let request = ReservationRequest::new(
                        ConsumerKey::new("late-data-carryover", MemoryClass::Decode)?,
                        decode_window,
                    )?
                    .as_minimum_working_set();
                    Some(reserve(Arc::clone(memory), request).await?)
                }
                None => None,
            };
            let mut reader = arrow_ipc::reader::FileReader::try_new_buffered(
                input.object.open_verified_file()?,
                None,
            )
            .map_err(CdfError::from)?;
            if reader.schema().as_ref() != runtime_output_schema.as_ref() {
                return Err(CdfError::data(format!(
                    "late-data carryover {} schema does not match the compiled output schema",
                    input.reference.relative_path
                )));
            }
            let mut artifact_rows = 0_u64;
            let mut artifact_retained_bytes = 0_u64;
            for batch in &mut reader {
                let batch = batch.map_err(CdfError::from)?;
                let batch_rows = u64::try_from(batch.num_rows())
                    .map_err(|_| CdfError::data("late-data carryover rows exceed u64"))?;
                let batch_retained_bytes = cdf_memory::record_batch_retained_bytes(&batch)?;
                artifact_rows = artifact_rows
                    .checked_add(batch_rows)
                    .ok_or_else(|| CdfError::data("late-data carryover row count overflow"))?;
                artifact_retained_bytes = artifact_retained_bytes
                    .checked_add(batch_retained_bytes)
                    .ok_or_else(|| CdfError::data("late-data carryover memory count overflow"))?;

                if apply_package_dedup {
                    if let Some(routing) = routed_write.as_mut() {
                        routing.observe_input(package_segment_kind, &batch)?;
                    }
                    if let Some(external) = &mut external_dedup {
                        let rule = package_dedup_rule.as_ref().ok_or_else(|| {
                            CdfError::internal("package dedup rule is absent")
                        })?;
                        let keys = encode_effect_keys(
                            plan,
                            &validation_program,
                            rule,
                            &batch,
                        )?;
                        external.index.push_owned_keys(keys.iter().cloned())?;
                        external.payload.push(
                            package_segment_kind,
                            carryover_partition_ordinal,
                            Some(carryover_frontier.clone()),
                            &keys,
                            &batch,
                        )?;
                    } else {
                        pending_dedup_batches.push(PendingDedupBatch {
                            kind: package_segment_kind,
                            partition_ordinal: carryover_partition_ordinal,
                            output: batch,
                            output_position: Some(carryover_frontier.clone()),
                            _memory_lease: lease.clone(),
                        });
                    }
                } else {
                    next_package_output_row_ordinal = next_package_output_row_ordinal
                        .checked_add(batch_rows)
                        .ok_or_else(|| CdfError::data("package output row ordinal overflow"))?;
                    write_normalized_output_batch(
                        PreparedKernelOutput {
                            output: batch,
                            memory_lease: lease.clone(),
                        },
                        Some(carryover_frontier.clone()),
                        carryover_partition_ordinal,
                        &mut carryover_assembler,
                        &mut OutputWriteState {
                            kind: package_segment_kind,
                            profile: &mut profile,
                            segment_positions: &mut segment_positions,
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
                            routing: routed_write.as_mut(),
                        },
                    )?;
                }
            }
            if artifact_rows != input.reference.row_count
                || artifact_retained_bytes > input.reference.memory_bound_bytes
            {
                return Err(CdfError::data(format!(
                    "late-data carryover {} decoded as {artifact_rows} rows/{artifact_retained_bytes} retained bytes, expected {} rows within its {}-byte memory bound",
                    input.reference.relative_path,
                    input.reference.row_count,
                    input.reference.memory_bound_bytes
                )));
            }
            if let Some(lease) = &lease {
                lease.reconcile(artifact_retained_bytes.max(1))?;
            }
        }
        if !apply_package_dedup {
            persist_canonical_segments(
                carryover_assembler.finish()?,
                &mut OutputWriteState {
                    kind: package_segment_kind,
                    profile: &mut profile,
                    segment_positions: &mut segment_positions,
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
                    routing: routed_write.as_mut(),
                },
                None,
            )?;
        }
        carryover_progress_observed = true;
        let carryover = controller.committed_source_continuation().cloned();
        let global_watermark = controller.committed_watermark().cloned();
        let decision = controller.observe_safe_frontier(
            cdf_runtime::DrainSafeFrontierObservation {
                frontier: carryover_frontier,
                carryover,
                // These rows already contributed to source-admission counters in the epoch that
                // recaptured them. One synthetic position marks this package as nonempty without
                // double-counting termination or cadence thresholds.
                admitted_batches: 0,
                admitted_rows: 0,
                admitted_bytes: 0,
                admitted_positions: 1,
                global_watermark,
                source_exhausted: false,
                monotonic_milliseconds: drain_clock
                    .monotonic_milliseconds(options.services.as_ref()),
                observed_at_unix_milliseconds: drain_clock
                    .observed_at_unix_milliseconds(options.services.as_ref())?,
            },
        )?;
        match decision {
            cdf_runtime::DrainEpochDecision::Continue => {}
            cdf_runtime::DrainEpochDecision::Close(closure) => {
                drain_epoch_closure = Some(*closure);
            }
            cdf_runtime::DrainEpochDecision::FinishedNoOp => {
                return Err(CdfError::internal(
                    "drain controller classified persisted late-data carryover as an empty epoch",
                ));
            }
        }
    }

    if drain_epoch_closure.is_some() {
        return Ok(());
    }
    loop {
        let next_partition = match poll_with_drain_timer(
            source_frontier.next_partition(),
            drain_controller.as_deref_mut(),
            options.services.as_ref(),
            &run_cancellation,
            &drain_clock,
        )
        .await?
        {
            DrainAwarePoll::Ready(next_partition) => next_partition,
            DrainAwarePoll::Timer(cdf_runtime::DrainEpochDecision::Close(closure)) => {
                drain_epoch_closure = Some(*closure);
                break;
            }
            DrainAwarePoll::Timer(cdf_runtime::DrainEpochDecision::FinishedNoOp) => {
                drain_finished_noop = true;
                break;
            }
            DrainAwarePoll::Timer(cdf_runtime::DrainEpochDecision::Continue) => {
                return Err(CdfError::internal(
                    "drain timer returned a continue decision to its source poll",
                ));
            }
        };
        let Some(mut opened_partition) = next_partition else {
            if let Some(controller) = drain_controller.as_deref_mut()
                && processed_observations.is_empty()
                && !source_progress_observed
            {
                if carryover_progress_observed {
                    let frontier = controller.committed_frontier().cloned().ok_or_else(|| {
                        CdfError::internal(
                            "late-data carryover lost its committed source frontier",
                        )
                    })?;
                    let carryover = controller.committed_source_continuation().cloned();
                    let global_watermark = controller.committed_watermark().cloned();
                    match controller.observe_safe_frontier(
                        cdf_runtime::DrainSafeFrontierObservation {
                            frontier,
                            carryover,
                            admitted_batches: 0,
                            admitted_rows: 0,
                            admitted_bytes: 0,
                            admitted_positions: 0,
                            global_watermark,
                            source_exhausted: true,
                            monotonic_milliseconds: drain_clock
                                .monotonic_milliseconds(options.services.as_ref()),
                            observed_at_unix_milliseconds: drain_clock
                                .observed_at_unix_milliseconds(options.services.as_ref())?,
                        },
                    )? {
                        cdf_runtime::DrainEpochDecision::Close(closure) => {
                            drain_epoch_closure = Some(*closure);
                        }
                        cdf_runtime::DrainEpochDecision::Continue
                        | cdf_runtime::DrainEpochDecision::FinishedNoOp => {
                            return Err(CdfError::internal(
                                "source exhaustion did not close a nonempty carryover epoch",
                            ));
                        }
                    }
                } else {
                    controller.finish_empty_source(
                        drain_clock.monotonic_milliseconds(options.services.as_ref()),
                    )?;
                    drain_finished_noop = true;
                }
            }
            break;
        };
        let open_metadata = opened_partition.metadata()?.clone();
        let partition_ordinal = open_metadata.ordinal;
        let executable_partition = open_metadata.partition;
        let partition = executable_partition.plan().clone();
        if plan.scan.external_task_set().is_some()
            && let Some(watermarks) = partition_watermarks.as_mut()
        {
            watermarks.register_partition(&partition.partition_id)?;
        }
        let open_evidence = open_metadata.evidence;
        let partition_scope = partition.scope.clone();
        let partition_drain_batch_frontiers_enabled =
            drain_batch_frontiers_enabled && partition.planned_file()?.is_none();
        let current_schema_disposition = effective_schema_evidence
            .map(|evidence| {
                partition_schema_disposition(
                    &partition,
                    evidence,
                    plan.scan.external_task_set().is_some(),
                )
            })
            .transpose()?;
        if let Some(PartitionSchemaDisposition::Quarantined(quarantine)) =
            &current_schema_disposition
        {
            let attestation = match observation_attestations.get(quarantine.observation_id()) {
                Some(attestation) => attestation.clone(),
                None => {
                    let attestation = attest_partition_with_terminal_join(
                        resource,
                        &executable_partition,
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
            if drain_controller.is_some() {
                record_drain_partition_position(
                    &mut drain_partition_positions,
                    &partition,
                    source_position.clone(),
                )?;
            }
            processed_observations.push(ProcessedObservationPosition::new(
                quarantine.observation_id().to_owned(),
                ProcessedObservationOutcome::Quarantined,
                source_position.clone(),
            )?);
            let mut quarantine = quarantine.as_ref().clone();
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
                last_drain_partition_resume = None;
                let frontier = drain_resource_frontier(
                    resource.descriptor(),
                    resource_schema.as_ref(),
                    controller.committed_frontier(),
                    &processed_observations,
                )?;
                let decision = controller.observe_safe_frontier(
                    cdf_runtime::DrainSafeFrontierObservation {
                        frontier,
                        carryover: drain_source_continuation(
                            &drain_partition_positions,
                        )?,
                        admitted_batches: 0,
                        admitted_rows: 0,
                        admitted_bytes: 0,
                        admitted_positions: 1,
                        global_watermark: None,
                        source_exhausted: consumed_partition_count == frontier_partition_count,
                        monotonic_milliseconds: drain_clock
                            .monotonic_milliseconds(options.services.as_ref()),
                        observed_at_unix_milliseconds: drain_clock
                            .observed_at_unix_milliseconds(options.services.as_ref())?,
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
            let mut source_poll_interrupted = false;
            loop {
                if remaining_limit == Some(0) {
                    fully_processed = false;
                    break;
                }
                let decode_started = phase_measurements.start();
                let timer_controller = if cdc_settlement_runtime
                    .as_ref()
                    .is_some_and(cdf_runtime::CdcLogSourceRuntime::unit_open)
                {
                    None
                } else {
                    drain_controller.as_deref_mut()
                };
                let next_batch = match poll_with_drain_timer(
                    opened_partition.next_batch(),
                    timer_controller,
                    options.services.as_ref(),
                    &run_cancellation,
                    &drain_clock,
                )
                .await?
                {
                    DrainAwarePoll::Ready(next_batch) => next_batch,
                    DrainAwarePoll::Timer(cdf_runtime::DrainEpochDecision::Close(closure)) => {
                        drain_epoch_closure = Some(*closure);
                        drain_partition_resume = last_drain_partition_resume.clone();
                        fully_processed = false;
                        partition_epoch_closed = true;
                        source_poll_interrupted = true;
                        break;
                    }
                    DrainAwarePoll::Timer(cdf_runtime::DrainEpochDecision::FinishedNoOp) => {
                        if source_progress_observed {
                            return Err(CdfError::data(
                                "drain duration elapsed after source progress but before the source exposed a checkpointable safe frontier",
                            ));
                        }
                        drain_finished_noop = true;
                        fully_processed = false;
                        partition_epoch_closed = true;
                        source_poll_interrupted = true;
                        break;
                    }
                    DrainAwarePoll::Timer(cdf_runtime::DrainEpochDecision::Continue) => {
                        return Err(CdfError::internal(
                            "drain timer returned a continue decision to its batch poll",
                        ));
                    }
                };
                let decode_duration_ns = elapsed_ns(decode_started, "resource decode")?;
                let Some(batch) = next_batch else {
                    phase_measurements.add(RunPhase::Decode, decode_duration_ns, 0, 0);
                    if cdc_settlement_runtime
                        .as_ref()
                        .is_some_and(cdf_runtime::CdcLogSourceRuntime::unit_open)
                    {
                        return Err(CdfError::data(
                            "CDC source ended before the open settlement unit exposed its terminal boundary; no state advances",
                        ));
                    }
                    break;
                };
                let mut batch = batch;
                source_progress_observed = true;
                validate_batch_partition_ownership(
                    &batch,
                    &plan.scan.request.resource_id,
                    &partition,
                )?;
                validate_cdc_batch_authority(&plan.write_disposition, &batch.header)?;
                if let Some(marker) = batch.header.cdc_settlement.clone() {
                    phase_measurements.add(RunPhase::Decode, decode_duration_ns, 0, 0);
                    let kind = settlement_unit_kind(marker.unit_kind);
                    match marker.boundary {
                        cdf_kernel::CdcSettlementBoundary::Begin => {
                            if cdc_settlement_runtime.is_none() {
                                let limit = plan.resolved_transaction_limit_bytes.ok_or_else(|| {
                                    CdfError::data(
                                        "CDC settlement boundary requires a plan-frozen transaction byte limit",
                                    )
                                })?;
                                let mut runtime = cdf_runtime::CdcLogSourceRuntime::new(
                                    &plan.execution_extent,
                                    kind,
                                    cdf_runtime::TransactionByteCeiling::from_resolved_plan(limit)?,
                                )?;
                                runtime.bind_initial_committed_position(
                                    drain_controller
                                        .as_deref()
                                        .and_then(cdf_runtime::DrainEpochController::committed_frontier),
                                )?;
                                cdc_settlement_runtime = Some(runtime);
                            }
                            let runtime = cdc_settlement_runtime.as_mut().ok_or_else(|| {
                                CdfError::internal("CDC settlement runtime disappeared")
                            })?;
                            if runtime.kind() != kind {
                                return Err(CdfError::data(
                                    "one CDC stream cannot mix committed-transaction and event-prefix settlement",
                                ));
                            }
                            runtime.begin_unit(&marker.position)?;
                        }
                        cdf_kernel::CdcSettlementBoundary::Terminal => {
                            let runtime = cdc_settlement_runtime.as_mut().ok_or_else(|| {
                                CdfError::data(
                                    "CDC terminal boundary arrived before a begin boundary",
                                )
                            })?;
                            if runtime.kind() != kind {
                                return Err(CdfError::data(
                                    "CDC terminal boundary kind does not match the open settlement unit",
                                ));
                            }
                            let completed = runtime.complete_unit(&marker.position)?;
                            let identity = completed.cdc_order_identity()?;
                            if cdc_order_identity
                                .as_ref()
                                .is_some_and(|current| current != &identity)
                            {
                                return Err(CdfError::data(
                                    "one package cannot combine CDC settlement units from different source-protocol scopes",
                                ));
                            }
                            cdc_order_identity = Some(identity);
                            record_drain_partition_position(
                                &mut drain_partition_positions,
                                &partition,
                                marker.position.clone(),
                            )?;
                            observed_partition_position = Some(marker.position.clone());
                            partition_batch_frontiers_observed = true;
                            let monotonic_milliseconds =
                                drain_clock.monotonic_milliseconds(options.services.as_ref());
                            let controller = drain_controller.as_deref_mut().ok_or_else(|| {
                                CdfError::contract(
                                    "CDC settlement boundary requires the finite drain controller",
                                )
                            })?;
                            let decision = controller.observe_safe_frontier(
                                completed.into_observation(
                                    None,
                                    partition_watermark.clone(),
                                    false,
                                    monotonic_milliseconds,
                                    drain_clock.observed_at_unix_milliseconds(
                                        options.services.as_ref(),
                                    )?,
                                ),
                            )?;
                            let resume = Box::new(crate::DrainPartitionResume {
                                partition_id: partition.partition_id.clone(),
                                start_position: marker.position,
                            });
                            last_drain_partition_resume = Some(resume.clone());
                            if let cdf_runtime::DrainEpochDecision::Close(closure) = decision {
                                drain_partition_resume = Some(resume);
                                drain_epoch_closure = Some(*closure);
                                fully_processed = false;
                                partition_epoch_closed = true;
                                break;
                            }
                        }
                    }
                    continue;
                }
                partition_input_batch_count = partition_input_batch_count
                    .checked_add(1)
                    .ok_or_else(|| CdfError::internal("drain partition input batch count overflow"))?;
                partition_input_bytes = partition_input_bytes
                    .checked_add(batch.header.byte_count)
                    .ok_or_else(|| CdfError::internal("drain partition input byte count overflow"))?;
                source_progress_rows = source_progress_rows.saturating_add(batch.header.row_count);
                source_progress_bytes =
                    source_progress_bytes.saturating_add(batch.header.byte_count);
                source_progress_batches = source_progress_batches.saturating_add(1);
                if let Some(observer) = options.source_batch_progress.as_deref() {
                    observer(crate::SourceBatchProgress {
                        row_count: source_progress_rows,
                        byte_count: source_progress_bytes,
                        batch_count: source_progress_batches,
                    });
                }
                observe_cdc_order_identity(&mut cdc_order_identity, &batch.header)?;
                if let Some(watermarks) = partition_watermarks.as_ref() {
                    for watermark in &batch.header.watermarks {
                        watermarks.validate_partition_claim(
                            &partition.partition_id,
                            partition_watermark.as_ref(),
                            watermark,
                        )?;
                        partition_watermark = Some(watermark.clone());
                    }
                } else if let Some(watermark) = batch.header.watermarks.last() {
                    partition_watermark = Some(watermark.clone());
                }
                if let Some(idleness) = &batch.header.partition_idleness {
                    idleness.validate()?;
                    if idleness.partition_id != partition.partition_id
                        || batch.header.row_count != 0
                        || !batch.header.watermarks.is_empty()
                        || batch.header.source_position.as_ref() != Some(&idleness.source_position)
                    {
                        return Err(CdfError::data(
                            "partition idleness must be a zero-row control batch with matching partition/source-position authority and no watermark claim",
                        ));
                    }
                }
                if matches!(plan.write_disposition, WriteDisposition::CdcApply) {
                    let metadata = batch.header.cdc.as_ref().ok_or_else(|| {
                        CdfError::data("cdc_apply data batch omitted operation metadata")
                    })?;
                    let position = batch.header.source_position.as_ref().ok_or_else(|| {
                        CdfError::data("cdc_apply data batch omitted source position")
                    })?;
                    let controller = drain_controller.as_deref().ok_or_else(|| {
                        CdfError::contract(
                            "cdc_apply batch admission requires the finite drain controller",
                        )
                    })?;
                    cdc_settlement_runtime
                        .as_mut()
                        .ok_or_else(|| {
                            CdfError::data(
                                "cdc_apply data batch arrived before a settlement begin boundary",
                            )
                        })?
                        .admit_batch_with_controller(
                            controller,
                            metadata,
                            position,
                            cdf_runtime::CdcBatchObservation {
                                rows: batch.header.row_count,
                                bytes: batch.header.byte_count,
                                monotonic_milliseconds: drain_clock
                                    .monotonic_milliseconds(options.services.as_ref()),
                                global_watermark: partition_watermark.clone(),
                            },
                        )?;
                }
                let decoded_input_bytes = batch.header.byte_count;
                phase_measurements.add(
                    RunPhase::Decode,
                    decode_duration_ns,
                    decoded_input_bytes,
                    decoded_input_bytes,
                );
                let validation_started = phase_measurements.start();
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
                let Some(record_batch) = batch.record_batch() else {
                    return Err(CdfError::data(
                        "package execution requires in-memory Arrow record batches at MVP",
                    ));
                };
                let validation_input_bytes = u64::try_from(record_batch.get_array_memory_size())
                    .map_err(|error| CdfError::internal(error.to_string()))?;
                if batch
                    .header
                    .cdc
                    .as_ref()
                    .is_some_and(|metadata| metadata.operation == cdf_kernel::CdcOperation::Delete)
                {
                    if remaining_limit.is_some() {
                        return Err(CdfError::contract(
                            "cdc_apply delete effects cannot be combined with a row limit",
                        ));
                    }
                    if !batch.header.pre_contract_quarantine.is_empty()
                        || !batch.header.residual_candidates().is_empty()
                        || !batch.header.physical_reconciliations().is_empty()
                    {
                        return Err(CdfError::data(
                            "CDC delete keys cannot be quarantined, residualized, or physically reconciled",
                        ));
                    }
                    let output = prepare_delete_effect_batch(plan, &batch, record_batch)?;
                    let batch_source_position = normalize_source_position_for_partition(
                        batch.header.source_position.clone(),
                        &partition_scope,
                    );
                    if cdc_settlement_runtime.is_none()
                        && let Some(position) = &batch_source_position
                    {
                        accumulate_processed_partition_position(
                            cdf_kernel::partition_schema_observation_id(&partition),
                            resource.descriptor(),
                            resource_schema.as_ref(),
                            &mut observed_partition_position,
                            position.clone(),
                        )?;
                    }
                    admitted_batch_count = admitted_batch_count.saturating_add(1);
                    partition_source_row_ordinal = partition_source_row_ordinal
                        .checked_add(batch.header.row_count)
                        .ok_or_else(|| CdfError::data("CDC source row count overflowed u64"))?;
                    verdict_summary.input_rows = verdict_summary
                        .input_rows
                        .saturating_add(batch.header.row_count);
                    verdict_summary.accepted_rows = verdict_summary
                        .accepted_rows
                        .saturating_add(batch.header.row_count);
                    let rule = package_dedup_rule.as_ref().ok_or_else(|| {
                        CdfError::internal("CDC delete package omitted its exact-key rule")
                    })?;
                    let external = external_dedup.as_mut().ok_or_else(|| {
                        CdfError::contract(
                            "CDC delete reduction requires bounded execution services and spill authority",
                        )
                    })?;
                    let keys = encode_effect_keys(
                        plan,
                        &validation_program,
                        rule,
                        &output,
                    )?;
                    external.index.push_owned_keys(keys.iter().cloned())?;
                    external.payload.push(
                        cdf_kernel::PackageSegmentKind::Delete,
                        partition_ordinal,
                        batch_source_position,
                        &keys,
                        &output,
                    )?;
                    phase_measurements.add(
                        RunPhase::ValidationNormalization,
                        elapsed_ns(validation_started, "CDC delete-key validation")?,
                        validation_input_bytes,
                        u64::try_from(output.get_array_memory_size())
                            .map_err(|_| CdfError::data("CDC delete batch bytes exceed u64"))?,
                    );
                    if partition_drain_batch_frontiers_enabled {
                        partition_batch_frontiers_observed = true;
                        let watermark_observation_milliseconds =
                            drain_clock.monotonic_milliseconds(options.services.as_ref());
                        if let Some((decision, partition_position)) = observe_drain_batch_frontier(
                            drain_controller.as_deref_mut(),
                            resource.descriptor(),
                            resource_schema.as_ref(),
                            &processed_observations,
                            &partition,
                            observed_partition_position.as_ref(),
                            &mut drain_partition_positions,
                            batch.header.row_count,
                            batch.header.byte_count,
                            None,
                            watermark_observation_milliseconds,
                            drain_clock.observed_at_unix_milliseconds(options.services.as_ref())?,
                        )? {
                            let resume = Box::new(crate::DrainPartitionResume {
                                partition_id: partition.partition_id.clone(),
                                start_position: partition_position,
                            });
                            last_drain_partition_resume = Some(resume.clone());
                            if let cdf_runtime::DrainEpochDecision::Close(closure) = decision {
                                drain_partition_resume = Some(resume);
                                drain_epoch_closure = Some(*closure);
                                fully_processed = false;
                                partition_epoch_closed = true;
                                break;
                            }
                        }
                    }
                    continue;
                }
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
                        effective_schema: &admission_schema,
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
                        dynamic_quarantine = Some((*quarantine, physical_observation));

                        // Schema quarantine is a whole-partition verdict. Drain the invocation to
                        // EOF so weak sources can finish their terminal content hash and the
                        // checkpoint records only fully consumed input. No drained batch enters
                        // validation or segment production after this verdict is fixed.
                        loop {
                            let decode_started = phase_measurements.start();
                            let next_batch = match poll_with_drain_timer(
                                opened_partition.next_batch(),
                                drain_controller.as_deref_mut(),
                                options.services.as_ref(),
                                &run_cancellation,
                                &drain_clock,
                            )
                            .await?
                            {
                                DrainAwarePoll::Ready(next_batch) => next_batch,
                                DrainAwarePoll::Timer(
                                    cdf_runtime::DrainEpochDecision::Close(_),
                                ) => {
                                    return Err(CdfError::internal(
                                        "drain timer closed without a safe frontier while quarantining a partition",
                                    ));
                                }
                                DrainAwarePoll::Timer(
                                    cdf_runtime::DrainEpochDecision::FinishedNoOp,
                                ) => {
                                    return Err(CdfError::data(
                                        "drain duration elapsed while quarantining a partition before its checkpointable terminal frontier",
                                    ));
                                }
                                DrainAwarePoll::Timer(
                                    cdf_runtime::DrainEpochDecision::Continue,
                                ) => {
                                    return Err(CdfError::internal(
                                        "drain timer returned a continue decision to its quarantine poll",
                                    ));
                                }
                            };
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
                            validate_batch_partition_ownership(
                                &drained,
                                &plan.scan.request.resource_id,
                                &partition,
                            )?;
                            source_progress_rows =
                                source_progress_rows.saturating_add(drained.header.row_count);
                            source_progress_bytes =
                                source_progress_bytes.saturating_add(drained.header.byte_count);
                            source_progress_batches = source_progress_batches.saturating_add(1);
                            if let Some(observer) = options.source_batch_progress.as_deref() {
                                observer(crate::SourceBatchProgress {
                                    row_count: source_progress_rows,
                                    byte_count: source_progress_bytes,
                                    batch_count: source_progress_batches,
                                });
                            }
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
                let batch_source_row_base = partition_source_row_ordinal;
                partition_source_row_ordinal = partition_source_row_ordinal
                    .saturating_add(batch.header.row_count);
                let residual_candidates = batch.header.take_residual_candidates();
                let physical_reconciliations = batch.header.take_physical_reconciliations();
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
                let physical_reconciliation_artifacts = validate_physical_reconciliations(
                    &record_batch,
                    physical_reconciliations,
                    &batch.header.batch_id,
                    partition_observation_id.as_deref(),
                )?;
                physical_reconciliation_evidence.push(
                    &builder,
                    physical_reconciliation_artifacts,
                )?;

                let batch_source_position = normalize_source_position_for_partition(
                    batch.header.source_position.clone(),
                    &partition_scope,
                );
                let evaluation_context = package_evaluation_context
                    .clone()
                    .with_source_position(batch_source_position.clone());
                let residual_preflight = preflight_residual_quarantines(
                    &validation_program,
                    residual_candidates,
                    &ResidualBatchContext {
                        evaluation: &evaluation_context,
                        source_rows: None,
                        cdc_operation_field: None,
                        batch_id: &batch.header.batch_id,
                        observation_id: partition_observation_id.as_deref(),
                    },
                )?;
                let residual_candidates = residual_preflight.remaining_candidates;

                let track_source_rows = pre_contract_may_filter
                    || !residual_candidates.is_empty()
                    || !residual_preflight.quarantined_batch_rows.is_empty()
                    || late_data_policy.is_some()
                    || plan
                        .relational_expression_plan
                        .as_ref()
                        .is_some_and(|relational| relational.filter.is_some());
                let expression_bytes = expression_working_set_bytes(
                    plan.compiled_expression_plan
                        .residuals
                        .iter()
                        .chain(plan.compiled_expression_plan.transforms.iter())
                        .map(|planned| &planned.expression)
                        .chain(
                            plan.relational_expression_plan
                                .iter()
                                .flat_map(|relational| {
                                    relational.filter.iter().chain(
                                        relational
                                            .projection
                                            .iter()
                                            .map(|projection| &projection.expression),
                                    )
                                }),
                        ),
                    record_batch.num_rows(),
                )?;
                let transform_memory_lease = reserve_transform_working_set(
                    memory.as_ref(),
                    &record_batch,
                    &residual_candidates,
                    expression_bytes,
                )
                .await?;
                let (record_batch, relational_source_rows) = match &bound_relational {
                    Some(relational) => {
                        let memory = transform_memory_lease.as_ref().ok_or_else(|| {
                            CdfError::internal(
                                "relational execution omitted its reserved memory lease",
                            )
                        })?;
                        let (output, rows) = execute_bound_relational_expression_plan_tracked(
                            relational,
                            &record_batch,
                            memory,
                            &run_cancellation,
                        )?;
                        (output, Some(rows))
                    }
                    None => (record_batch, None),
                };
                let executed = execute_batch(
                    &record_batch,
                    if track_source_rows {
                        &bound_tracked_residuals
                    } else {
                        &bound_residuals
                    },
                    track_source_rows,
                    transform_memory_lease.as_ref(),
                    &run_cancellation,
                )?;
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
                    transform_memory_lease.as_ref(),
                    &run_cancellation,
                )?;
                let source_rows = remap_relational_source_rows(
                    source_rows,
                    relational_source_rows.as_deref(),
                )?;
                let (output, source_rows) = remove_preflight_quarantined_rows(
                    output,
                    source_rows,
                    &residual_preflight.quarantined_batch_rows,
                )?;
                // CDC data positions order keyed effects inside the open settlement unit, but
                // only its explicit terminal marker is checkpoint-safe. Feeding every opaque
                // resume token through generic position aggregation would either invent an order
                // or reject a valid sequence before the source-attested terminal arrives.
                if cdc_settlement_runtime.is_none()
                    && let Some(position) = &batch_source_position
                {
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
                let watermark_observation_milliseconds =
                    drain_clock.monotonic_milliseconds(options.services.as_ref());
                let effective_batch_watermark = partition_watermarks
                    .as_mut()
                    .map(|watermarks| match &batch.header.partition_idleness {
                        Some(idleness) => watermarks.observe_partition_idle(
                            &partition.partition_id,
                            idleness,
                            watermark_observation_milliseconds,
                        ),
                        None => watermarks.observe_partition_progress(
                            &partition.partition_id,
                            partition_watermark.as_ref(),
                            watermark_observation_milliseconds,
                        ),
                    })
                    .transpose()?
                    .flatten();
                macro_rules! close_drain_epoch_at_batch_frontier {
                    () => {
                        if partition_drain_batch_frontiers_enabled
                            && !matches!(
                                batch.header.source_position.as_ref(),
                                Some(SourcePosition::FileManifest(_))
                            )
                        {
                            partition_batch_frontiers_observed = true;
                            if let Some((decision, partition_position)) = observe_drain_batch_frontier(
                                drain_controller.as_deref_mut(),
                                resource.descriptor(),
                                resource_schema.as_ref(),
                                &processed_observations,
                                &partition,
                                observed_partition_position.as_ref(),
                                &mut drain_partition_positions,
                                batch.header.row_count,
                                batch.header.byte_count,
                                effective_batch_watermark.clone(),
                                watermark_observation_milliseconds,
                                drain_clock.observed_at_unix_milliseconds(options.services.as_ref())?,
                            )? {
                                let resume = Box::new(crate::DrainPartitionResume {
                                    partition_id: partition.partition_id.clone(),
                                    start_position: partition_position,
                                });
                                last_drain_partition_resume = Some(resume.clone());
                                if let cdf_runtime::DrainEpochDecision::Close(closure) = decision {
                                    drain_partition_resume = Some(resume);
                                    drain_epoch_closure = Some(*closure);
                                    fully_processed = false;
                                    partition_epoch_closed = true;
                                    break;
                                }
                            }
                        }
                    };
                }
                if output.num_rows() == 0 && residual_preflight.quarantine_records.is_empty() {
                    phase_measurements.add(
                        RunPhase::ValidationNormalization,
                        elapsed_ns(validation_started, "validation/normalization")?,
                        validation_input_bytes,
                        0,
                    );
                    close_drain_epoch_at_batch_frontier!();
                    continue;
                }

                let quarantine_lease = if residual_candidates.is_empty()
                    && residual_preflight.quarantine_records.is_empty()
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
                for record in residual_preflight.quarantine_records {
                    quarantine_sink.push(record)?;
                }
                let ContractExecOutput {
                    accepted,
                    accepted_source_rows,
                    variant_values,
                    mut summary,
                    residual_decisions: mut batch_residual_decisions,
                    memory_lease,
                } = apply_contract_exec(
                    output,
                    &mut contract_evaluator,
                    &mut |record| quarantine_sink.push(record),
                    residual_candidates,
                    &ResidualBatchContext {
                        evaluation: &evaluation_context,
                        source_rows: source_rows.as_deref(),
                        cdc_operation_field: None,
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
                merge_verdict_summary(&mut summary, residual_preflight.summary);
                batch_residual_decisions.splice(
                    0..0,
                    residual_preflight.residual_decisions,
                );
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
                let prepared_output = prepare_output_batch(
                    &validation_program,
                    effective_schema_evidence.is_some(),
                        PreparedOutputBatch {
                            output,
                            variant_values,
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
                let (output, memory_lease) = if let (Some((event_time_field, action)), Some(watermark)) = (
                    late_data_policy.as_ref(),
                    drain_controller
                        .as_deref()
                        .and_then(cdf_runtime::DrainEpochController::late_data_watermark)
                        .cloned(),
                ) {
                    let mut classification = crate::late_data::classify_late_data(
                        output,
                        accepted_source_rows.as_deref().ok_or_else(|| {
                            CdfError::internal(
                                "watermark-enabled contract execution omitted accepted source-row tracking",
                            )
                        })?,
                        event_time_field,
                        &watermark,
                        *action,
                        &partition.partition_id,
                        batch_source_position.as_ref(),
                        batch_source_row_base,
                        (*action == cdf_kernel::LateDataAction::AdmitWithAnnotation)
                            .then_some(next_package_output_row_ordinal),
                    )?;
                    if let Some(lease) = &memory_lease {
                        let classification_bytes = [
                            Some(&classification.admitted),
                            classification.recaptured.as_ref(),
                            classification.quarantined.as_ref(),
                        ]
                        .into_iter()
                        .flatten()
                        .try_fold(0_u64, |total, batch| {
                            total
                                .checked_add(cdf_memory::record_batch_retained_bytes(batch)?)
                                .ok_or_else(|| {
                                    CdfError::data("late-data classification memory overflow")
                                })
                        })?
                        .checked_add(
                            classification
                                .evidence
                                .as_ref()
                                .map(late_data_evidence_retained_bytes)
                                .transpose()?
                                .unwrap_or(0),
                        )
                        .ok_or_else(|| {
                            CdfError::data("late-data classification memory overflow")
                        })?;
                        lease.reconcile(classification_bytes.max(1))?;
                    }
                    if let Some(recaptured) = classification.recaptured {
                        let row_count = u64::try_from(recaptured.num_rows()).map_err(|_| {
                            CdfError::data("late-data carryover row count exceeds u64")
                        })?;
                        remove_late_data_from_accepted_summary(
                            &mut verdict_summary,
                            row_count,
                            residual_row_count(&recaptured)?,
                        )?;
                        let memory_bound_bytes =
                            cdf_memory::record_batch_retained_bytes(&recaptured)?;
                        let relative_path = format!(
                            "carryover/late-data-{:020}.arrow",
                            late_data_carryover.len()
                        );
                        let file = builder
                            .write_ipc_identity_batches(&relative_path, &[recaptured])?;
                        let artifact_ordinal = late_data_payloads.next_ordinal();
                        late_data_payloads.push(&builder, &LateDataPayloadArtifact {
                            artifact_ordinal,
                            action: *action,
                            path: file.path.clone(),
                            byte_count: file.byte_count,
                            sha256: file.sha256.clone(),
                            row_count,
                        })?;
                        let evidence = classification.evidence.as_mut().ok_or_else(|| {
                            CdfError::internal("recaptured late data omitted row evidence")
                        })?;
                        for (row_ordinal, row) in evidence.rows.iter_mut().enumerate() {
                            row.payload = LateDataPayloadLocation::ArtifactRow {
                                artifact_ordinal,
                                row_ordinal: u64::try_from(row_ordinal).map_err(|_| {
                                    CdfError::data("late-data payload row ordinal exceeds u64")
                                })?,
                            };
                        }
                        let output_position = batch_output_position.clone().ok_or_else(|| {
                            CdfError::data(
                                "recapture_next_epoch requires exact source position authority for every withheld batch",
                            )
                        })?;
                        let carryover = cdf_kernel::LateDataCarryoverRef {
                            version: cdf_kernel::LATE_DATA_CARRYOVER_VERSION,
                            package_id: plan.package_id.clone(),
                            relative_path: file.path,
                            byte_count: file.byte_count,
                            sha256: file.sha256,
                            row_count,
                            memory_bound_bytes,
                            output_position,
                        };
                        carryover.validate()?;
                        late_data_carryover.push(carryover);
                    }
                    if *action == cdf_kernel::LateDataAction::Quarantine
                        && classification.evidence.is_some()
                    {
                        let quarantined = classification.quarantined.take().ok_or_else(|| {
                            CdfError::internal(
                                "late-data quarantine omitted its exact normalized row payload",
                            )
                        })?;
                        let row_count = u64::try_from(quarantined.num_rows()).map_err(|_| {
                            CdfError::data("late-data quarantine row count exceeds u64")
                        })?;
                        let residual_rows = residual_row_count(&quarantined)?;
                        let relative_path = format!(
                            "quarantine/late-data-{:020}.arrow",
                            late_data_payloads.next_ordinal()
                        );
                        let file = builder
                            .write_ipc_identity_batches(&relative_path, &[quarantined])?;
                        let artifact_ordinal = late_data_payloads.next_ordinal();
                        late_data_payloads.push(&builder, &LateDataPayloadArtifact {
                            artifact_ordinal,
                            action: *action,
                            path: file.path,
                            byte_count: file.byte_count,
                            sha256: file.sha256,
                            row_count,
                        })?;
                        let evidence = classification.evidence.as_mut().ok_or_else(|| {
                            CdfError::internal("quarantined late data omitted row evidence")
                        })?;
                        for (row_ordinal, row) in evidence.rows.iter_mut().enumerate() {
                            row.payload = LateDataPayloadLocation::ArtifactRow {
                                artifact_ordinal,
                                row_ordinal: u64::try_from(row_ordinal).map_err(|_| {
                                    CdfError::data("late-data payload row ordinal exceeds u64")
                                })?,
                            };
                        }
                        let quarantine_lease = reserve_quarantine_evidence(memory.as_ref())?;
                        let mut quarantine_sink = QuarantinePartAccumulator::new(
                            &builder,
                            &mut quarantine_part_count,
                            quarantine_lease,
                        );
                        for row in &evidence.rows {
                            quarantine_sink
                                .push(quarantine_record_from_late_data(evidence, row)?)?;
                        }
                        quarantine_sink.finish()?;
                        apply_late_data_quarantine_summary(
                            &mut verdict_summary,
                            evidence,
                            residual_rows,
                        )?;
                    }
                    if let Some(evidence) = &classification.evidence {
                        late_data_evidence.push(&builder, evidence)?;
                    }
                    if let Some(lease) = &memory_lease {
                        lease.reconcile(
                            cdf_memory::record_batch_retained_bytes(&classification.admitted)?
                                .max(1),
                        )?;
                    }
                    (classification.admitted, memory_lease)
                } else {
                    (output, memory_lease)
                };
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
                if apply_package_dedup {
                    phase_measurements.add(
                        RunPhase::ValidationNormalization,
                        elapsed_ns(validation_started, "validation/normalization")?,
                        validation_input_bytes,
                        validation_output_bytes,
                    );
                    if let Some(routing) = routed_write.as_mut() {
                        routing.observe_input(package_segment_kind, &output)?;
                    }
                    if let Some(external) = &mut external_dedup {
                        let rule = package_dedup_rule.as_ref().ok_or_else(|| {
                            CdfError::internal("package dedup rule is absent")
                        })?;
                        let keys = encode_effect_keys(
                            plan,
                            &validation_program,
                            rule,
                            &output,
                        )?;
                        external.index.push_owned_keys(keys.iter().cloned())?;
                        external.payload.push(
                            package_segment_kind,
                            partition_ordinal,
                            batch_output_position,
                            &keys,
                            &output,
                        )?;
                    } else {
                        pending_dedup_batches.push(PendingDedupBatch {
                            kind: package_segment_kind,
                            partition_ordinal,
                            output,
                            output_position: batch_output_position,
                            _memory_lease: memory_lease,
                        });
                    }
                    close_drain_epoch_at_batch_frontier!();
                    continue;
                }
                next_package_output_row_ordinal = next_package_output_row_ordinal
                    .checked_add(u64::try_from(output.num_rows()).map_err(|_| {
                        CdfError::data("package output row count exceeds u64")
                    })?)
                    .ok_or_else(|| CdfError::data("package output row ordinal overflow"))?;
                phase_measurements.add(
                    RunPhase::ValidationNormalization,
                    elapsed_ns(validation_started, "validation/normalization")?,
                    validation_input_bytes,
                    validation_output_bytes,
                );
                write_normalized_output_batch(
                    PreparedKernelOutput {
                        output,
                        memory_lease,
                    },
                    batch_output_position,
                    partition_ordinal,
                    &mut segment_assembler,
                    &mut OutputWriteState {
                        kind: package_segment_kind,
                        profile: &mut profile,
                        segment_positions: &mut segment_positions,
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
                        routing: routed_write.as_mut(),
                    },
                )?;
                close_drain_epoch_at_batch_frontier!();
            }
            persist_canonical_segments(
                segment_assembler.finish()?,
                &mut OutputWriteState {
                    kind: package_segment_kind,
                    profile: &mut profile,
                    segment_positions: &mut segment_positions,
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
                    routing: routed_write.as_mut(),
                },
                None,
            )?;
            let completion = if fully_processed {
                let (_, completion) = opened_partition.finish()?;
                completion
            } else if source_poll_interrupted {
                None
            } else {
                let (_, completion) = opened_partition.terminate_partial().await?;
                completion
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
            let completion_source_transfer = completion
                .as_ref()
                .and_then(cdf_kernel::PartitionCompletion::source_transfer)
                .cloned();
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
                partition_epoch_closed,
                partition_batch_frontiers_observed,
                completion_source_transfer,
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
            partition_epoch_closed,
            partition_batch_frontiers_observed,
            completion_source_transfer,
        ) = partition_result?;
        if let Some(completion_source_transfer) = completion_source_transfer {
            source_transfer.merge(&completion_source_transfer)?;
        }
        if drain_finished_noop {
            break;
        }
        checkpoint_eligible &= fully_processed || partition_epoch_closed;
        reconcile_partition_completion(
            resource,
            resource_schema.as_ref(),
            &executable_partition,
            &run_cancellation,
            &partition,
            partition_ordinal,
            &open_evidence,
            fully_processed,
            partition_epoch_closed,
            observed_partition_position,
            dynamic_quarantine,
            partition_observation_id,
            partition_observed_rows,
            completion_attestation,
            partition_schema_evidence,
            effective_schema_evidence,
            &mut completion_positions,
            &mut processed_observations,
            &mut terminal_quarantines,
            &mut quarantine_physical_observations,
            &mut observation_attestations,
            &mut stream_admission_evidence,
            &mut stream_physical_observation_catalog,
            &mut lineage,
            drain_controller.is_some(),
            &mut drain_partition_positions,
        )
        .await?;
        if fully_processed {
            consumed_partition_count = consumed_partition_count.saturating_add(1);
        }
        if settle_partition_frontier(
            resource,
            resource_schema.as_ref(),
            drain_controller.as_deref_mut(),
            partition_epoch_closed,
            &processed_observations,
            consumed_partition_count,
            frontier_partition_count,
            carryover_progress_observed,
            &drain_clock,
            options.services.as_ref(),
            &mut last_drain_partition_resume,
            &mut partition_watermarks,
            &drain_partition_positions,
            partition_batch_frontiers_observed,
            partition_input_batch_count,
            partition_observed_rows,
            partition_input_bytes,
            &mut drain_epoch_closure,
            &mut drain_finished_noop,
        )? {
            break;
        }
    }

    if drain_finished_noop {
        return Ok(());
    }

    let mut keyed_reduction = None;
    if apply_package_dedup {
        let rule = package_dedup_rule.as_ref().ok_or_else(|| {
            CdfError::internal("package dedup was selected without a compiled rule")
        })?;
        let applied = apply_dedup_and_write_pending_batches(
            &builder,
            &validation_program,
            rule,
            pending_dedup_batches,
            external_dedup.take(),
            &segmentation_policy,
            &mut OutputWriteState {
                kind: package_segment_kind,
                profile: &mut profile,
                segment_positions: &mut segment_positions,
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
                routing: routed_write.as_mut(),
            },
        )?;
        if matches!(
            plan.write_disposition,
            WriteDisposition::Merge | WriteDisposition::CdcApply
        ) {
            keyed_reduction = Some(keyed_reduction_authority(
                rule,
                &applied.summary,
                plan,
                cdc_order_identity.as_ref(),
                Some((applied.input_effects, applied.surviving_effects)),
            )?);
        }
    }

    if let Some(routing) = routed_write.as_mut() {
        routing.finish(
            &mut OutputWriteState {
                kind: package_segment_kind,
                profile: &mut profile,
                segment_positions: &mut segment_positions,
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
                routing: None,
            },
        )?;
    }
    segment_queue.finish(
        &builder,
        &mut OutputWriteState {
            kind: package_segment_kind,
            profile: &mut profile,
            segment_positions: &mut segment_positions,
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
    if let Some(routing) = routed_write.as_ref() {
        builder.set_content_authority(routed_package_content(
            plan,
            routing,
            keyed_reduction.as_ref(),
        )?)?;
    } else if let Some(reduction) = keyed_reduction {
        builder.set_content_authority(keyed_package_content(plan, reduction)?)?;
    }
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
    if drain_finished_noop {
        run_cancellation.cancel();
        source_frontier.terminate_and_join().await?;
        let source_frontier_report = source_frontier.report();
        segment_queue.abort_and_cleanup()?;
        drop(contract_evaluator);
        drop(residual_decisions);
        drop(external_dedup);
        drop(statistics_profile);
        drop(statistics_memory_lease);
        builder.abort_unpublished()?;
        return Ok(PackageExecutionOutcome::DrainFinishedNoOp {
            source_frontier: source_frontier_report,
        });
    }
    if drain_epoch_closure.is_some() && consumed_partition_count < frontier_partition_count {
        source_frontier.terminate_and_join().await?;
    }
    let source_frontier_report = source_frontier.report();

    drop(contract_evaluator);
    let PreparedPackageArtifacts {
        lineage,
        terminal_schema_quarantines,
        partition_watermarks,
        execution_evidence,
        residual_decision_output: _residual_decision_output,
    } = prepare_package_artifacts(
        &builder,
        plan,
        &validation_program,
        lineage,
        stream_admission_evidence,
        stream_physical_observation_catalog,
        processed_observations,
        terminal_quarantines,
        quarantine_physical_observations,
        output_schema,
        runtime_output_schema.as_ref(),
        residual_decisions,
        physical_reconciliation_evidence,
        statistics_profile,
        &statistics_profile_schema_hash,
        &profile,
        &verdict_summary,
        quarantine_part_count,
        partition_watermarks,
        drain_epoch_closure.as_ref(),
        drain_controller,
        late_data_evidence,
        late_data_payloads,
        &options.retry_journal,
        checkpoint_eligible,
    )?;
    PackageFinalization {
        builder,
        pre_finalize,
        stream_finalize,
        profile,
        lineage,
        admission: validation_program.admission.clone(),
        verdict_summary,
        terminal_schema_quarantines,
        segment_positions,
        phase_measurements,
        source_frontier: source_frontier_report,
        source_transfer,
        drain_epoch_closure,
        consumed_partition_count,
        drain_partition_resume,
        consumed_late_data_carryover,
        late_data_carryover,
        partition_watermarks,
        execution_evidence,
    }
    .finish()
}

struct PreparedPackageArtifacts {
    lineage: LineageSummary,
    terminal_schema_quarantines: Vec<TerminalSchemaObservationQuarantine>,
    partition_watermarks: Vec<cdf_kernel::PartitionWatermarkState>,
    execution_evidence: EngineExecutionEvidence,
    // Spill-backed output owns the managed lease and scratch directory through finalization hooks.
    residual_decision_output: Option<ResidualDecisionOutput>,
}

#[allow(clippy::too_many_arguments)]
fn settle_partition_frontier<R>(
    resource: &R,
    resource_schema: &Schema,
    controller: Option<&mut cdf_runtime::DrainEpochController>,
    partition_epoch_closed: bool,
    processed_observations: &[ProcessedObservationPosition],
    consumed_partition_count: u64,
    frontier_partition_count: u64,
    carryover_progress_observed: bool,
    drain_clock: &DrainExecutionClock,
    services: Option<&cdf_runtime::ExecutionServices>,
    last_drain_partition_resume: &mut Option<Box<crate::DrainPartitionResume>>,
    partition_watermarks: &mut Option<cdf_runtime::PartitionWatermarkTracker>,
    drain_partition_positions: &BTreeMap<String, SourcePosition>,
    partition_batch_frontiers_observed: bool,
    partition_input_batch_count: u64,
    partition_observed_rows: u64,
    partition_input_bytes: u64,
    drain_epoch_closure: &mut Option<cdf_runtime::DrainEpochClosure>,
    drain_finished_noop: &mut bool,
) -> Result<bool>
where
    R: ResourceStream + ?Sized,
{
    let Some(controller) = controller else {
        return Ok(false);
    };
    if partition_epoch_closed {
        return Ok(true);
    }
    if processed_observations.is_empty() {
        if consumed_partition_count != frontier_partition_count {
            return Ok(false);
        }
        if carryover_progress_observed {
            let frontier = controller.committed_frontier().cloned().ok_or_else(|| {
                CdfError::internal("late-data carryover lost its committed source frontier")
            })?;
            let carryover = controller.committed_source_continuation().cloned();
            let global_watermark = controller.committed_watermark().cloned();
            match controller.observe_safe_frontier(cdf_runtime::DrainSafeFrontierObservation {
                frontier,
                carryover,
                admitted_batches: 0,
                admitted_rows: 0,
                admitted_bytes: 0,
                admitted_positions: 0,
                global_watermark,
                source_exhausted: true,
                monotonic_milliseconds: drain_clock.monotonic_milliseconds(services),
                observed_at_unix_milliseconds: drain_clock
                    .observed_at_unix_milliseconds(services)?,
            })? {
                cdf_runtime::DrainEpochDecision::Close(closure) => {
                    *drain_epoch_closure = Some(*closure);
                }
                cdf_runtime::DrainEpochDecision::Continue
                | cdf_runtime::DrainEpochDecision::FinishedNoOp => {
                    return Err(CdfError::internal(
                        "source exhaustion did not close a nonempty carryover epoch",
                    ));
                }
            }
        } else {
            controller.finish_empty_source(drain_clock.monotonic_milliseconds(services))?;
            *drain_finished_noop = true;
        }
        return Ok(true);
    }
    *last_drain_partition_resume = None;
    let frontier = drain_resource_frontier(
        resource.descriptor(),
        resource_schema,
        controller.committed_frontier(),
        processed_observations,
    )?;
    let monotonic_milliseconds = drain_clock.monotonic_milliseconds(services);
    let effective_watermark = partition_watermarks
        .as_mut()
        .map(|watermarks| watermarks.effective_watermark(monotonic_milliseconds))
        .transpose()?
        .flatten();
    let decision = controller.observe_safe_frontier(cdf_runtime::DrainSafeFrontierObservation {
        frontier,
        carryover: drain_source_continuation(drain_partition_positions)?,
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
        global_watermark: effective_watermark,
        source_exhausted: consumed_partition_count == frontier_partition_count,
        monotonic_milliseconds,
        observed_at_unix_milliseconds: drain_clock.observed_at_unix_milliseconds(services)?,
    })?;
    match decision {
        cdf_runtime::DrainEpochDecision::Continue => Ok(false),
        cdf_runtime::DrainEpochDecision::Close(closure) => {
            *drain_epoch_closure = Some(*closure);
            Ok(true)
        }
        cdf_runtime::DrainEpochDecision::FinishedNoOp => Err(CdfError::internal(
            "drain controller classified a processed source position as an empty epoch",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_partition_completion<R>(
    resource: &R,
    resource_schema: &Schema,
    executable_partition: &ExecutablePartition,
    run_cancellation: &cdf_runtime::RunCancellation,
    partition: &PartitionPlan,
    partition_ordinal: u64,
    open_evidence: &PartitionOpenEvidence,
    fully_processed: bool,
    partition_epoch_closed: bool,
    observed_partition_position: Option<SourcePosition>,
    dynamic_quarantine: Option<(
        TerminalSchemaObservationQuarantine,
        PhysicalObservationEvidence,
    )>,
    partition_observation_id: Option<String>,
    partition_observed_rows: u64,
    completion_attestation: Option<PartitionAttestation>,
    partition_schema_evidence: Option<&EffectiveSchemaObservationCoercion>,
    effective_schema_evidence: Option<&EffectiveSchemaPlanEvidence>,
    completion_positions: &mut Vec<(u64, PartitionPlan, SourcePosition)>,
    processed_observations: &mut Vec<ProcessedObservationPosition>,
    terminal_quarantines: &mut Vec<TerminalSchemaObservationQuarantine>,
    quarantine_physical_observations: &mut BTreeMap<String, PhysicalObservationEvidence>,
    observation_attestations: &mut BTreeMap<String, PartitionAttestation>,
    stream_admission_evidence: &mut BTreeMap<String, StreamAdmissionObservationEvidence>,
    stream_physical_observation_catalog: &mut BTreeMap<String, PhysicalObservationEvidence>,
    lineage: &mut LineageSummary,
    drain_execution: bool,
    drain_partition_positions: &mut BTreeMap<String, SourcePosition>,
) -> Result<()>
where
    R: ResourceStream + ?Sized,
{
    let partial_retry_attestation = if open_evidence.retry_pre_attestation.is_some()
        && completion_attestation.is_none()
    {
        Some(
            attest_partition_with_terminal_join(
                resource,
                executable_partition,
                run_cancellation,
            )
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
            attest_partition_with_terminal_join(resource, executable_partition, run_cancellation)
                .await?
        } else {
            None
        };
        let terminal_position = completion_attestation
            .as_ref()
            .map(|attestation| attestation.processed_position().clone())
            .or_else(|| fallback_attestation.map(PartitionAttestation::into_processed_position));
        let source_position = aggregate_processed_partition_positions(
            &observation_id,
            resource.descriptor(),
            resource_schema,
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
            terminal_quarantines,
            quarantine_physical_observations,
            quarantine,
            physical_observation,
        )?;
    } else if let Some(observation_id) = partition
        .metadata
        .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
        .cloned()
        .or(partition_observation_id)
    {
        let fallback_attestation =
            if observed_partition_position.is_none() && completion_attestation.is_none() {
                match observation_attestations.get(&observation_id) {
                    Some(attestation) => Some(attestation.clone()),
                    None => {
                        let attestation = attest_partition_with_terminal_join(
                            resource,
                            executable_partition,
                            run_cancellation,
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
                stream_admission_evidence,
                stream_physical_observation_catalog,
                &expected.observation_id,
                preobserved_physical_observation(effective_schema_evidence, Some(expected))?
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
            .or_else(|| fallback_attestation.map(PartitionAttestation::into_processed_position));
        let source_position =
            if observed_partition_position.is_none() && fallback_position.is_none() {
                None
            } else {
                Some(aggregate_processed_partition_positions(
                    &observation_id,
                    resource.descriptor(),
                    resource_schema,
                    observed_partition_position.as_ref(),
                    fallback_position,
                )?)
            };
        let partition_binding = cdf_kernel::partition_schema_observation_binding(partition)?;
        lineage.input_observations.push(LineageInputObservation {
            observation_id: observation_id.clone(),
            partition_id: partition.partition_id.clone(),
            partition_binding: partition_binding.clone(),
            observed_rows: partition_observed_rows,
            output_position: source_position.clone(),
        });
        if let Some(source_position) = source_position {
            if drain_execution {
                record_drain_partition_position(
                    drain_partition_positions,
                    partition,
                    source_position.clone(),
                )?;
            }
            let evidence = stream_admission_evidence
                .get_mut(&observation_id)
                .ok_or_else(|| {
                    CdfError::internal(format!(
                        "admitted observation {observation_id:?} omitted stream-admission evidence"
                    ))
                })?;
            if fully_processed || partition_epoch_closed {
                evidence
                    .bind_source_position(source_position.clone(), partition_binding.clone())?;
                processed_observations.push(ProcessedObservationPosition::new(
                    observation_id,
                    ProcessedObservationOutcome::Admitted,
                    source_position,
                )?);
            } else {
                evidence.bind_partial_attempt(
                    source_position,
                    partition_observed_rows,
                    partition_binding.clone(),
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
                evidence.bind_unpositioned_completion(partition_binding)?;
            } else {
                return Err(CdfError::data(format!(
                    "partial schema observation {observation_id:?} requires exact generation and slice-position authority"
                )));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn prepare_package_artifacts(
    builder: &PackageBuilder,
    plan: &EnginePlan,
    validation_program: &ValidationProgram,
    mut lineage: LineageSummary,
    stream_admission_evidence: BTreeMap<String, StreamAdmissionObservationEvidence>,
    stream_physical_observation_catalog: BTreeMap<String, PhysicalObservationEvidence>,
    mut processed_observations: Vec<ProcessedObservationPosition>,
    mut terminal_quarantines: Vec<TerminalSchemaObservationQuarantine>,
    mut quarantine_physical_observations: BTreeMap<String, PhysicalObservationEvidence>,
    output_schema: Option<SchemaArtifact>,
    runtime_output_schema: &Schema,
    residual_decisions: ResidualDecisionAccumulator,
    physical_reconciliation_evidence: PhysicalReconciliationEvidenceAccumulator,
    statistics_profile: Option<cdf_package::StatisticsProfileWriter>,
    statistics_profile_schema_hash: &str,
    profile: &ExecutionProfile,
    verdict_summary: &VerdictSummary,
    quarantine_part_count: usize,
    partition_watermarks: Option<cdf_runtime::PartitionWatermarkTracker>,
    drain_epoch_closure: Option<&cdf_runtime::DrainEpochClosure>,
    drain_controller: Option<&mut cdf_runtime::DrainEpochController>,
    late_data_evidence: LateDataEvidenceAccumulator,
    late_data_payloads: LateDataPayloadCatalogAccumulator,
    retry_journal: &cdf_runtime::SourceRetryJournal,
    checkpoint_eligible: bool,
) -> Result<PreparedPackageArtifacts> {
    if let Some(coercion) = &validation_program.schema_coercion {
        builder.write_json_artifact("schema/coercion-plan.json", coercion)?;
    }
    lineage
        .input_observations
        .sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
    if lineage
        .input_observations
        .windows(2)
        .any(|pair| pair[0].observation_id == pair[1].observation_id)
    {
        return Err(CdfError::data(
            "execution lineage contains a schema observation identity assigned to more than one partition",
        ));
    }
    if !lineage
        .input_observations
        .iter()
        .map(|observation| observation.observation_id.as_str())
        .eq(stream_admission_evidence.keys().map(String::as_str))
    {
        return Err(CdfError::data(
            "execution lineage does not exactly bind every admitted stream observation to one partition",
        ));
    }
    processed_observations.sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
    if processed_observations
        .windows(2)
        .any(|pair| pair[0].observation_id == pair[1].observation_id)
    {
        return Err(CdfError::data(
            "processed schema observation identity is assigned to more than one partition",
        ));
    }
    terminal_quarantines.sort_by(|left, right| left.observation_id().cmp(right.observation_id()));
    if terminal_quarantines
        .windows(2)
        .any(|pair| pair[0].observation_id() == pair[1].observation_id())
    {
        return Err(CdfError::data(
            "schema-quarantine evidence contains a duplicate observation identity",
        ));
    }
    if !processed_observations
        .iter()
        .filter(|observation| observation.outcome == ProcessedObservationOutcome::Admitted)
        .map(|observation| observation.observation_id.as_str())
        .eq(stream_admission_evidence
            .iter()
            .filter(|(_, observation)| {
                matches!(
                    observation.completion,
                    crate::StreamAdmissionCompletion::Complete { .. }
                )
            })
            .map(|(observation_id, _)| observation_id.as_str()))
    {
        return Err(CdfError::data(
            "processed admitted observations do not exactly match stream-admission evidence",
        ));
    }
    if !processed_observations
        .iter()
        .filter(|observation| observation.outcome == ProcessedObservationOutcome::Quarantined)
        .map(|observation| observation.observation_id.as_str())
        .eq(terminal_quarantines
            .iter()
            .map(TerminalSchemaObservationQuarantine::observation_id))
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
    let output_schema =
        output_schema.ok_or_else(|| CdfError::internal("compiled output schema is missing"))?;
    builder.write_json_artifact("schema/output.json", &output_schema)?;
    builder.write_runtime_arrow_schema(runtime_output_schema)?;
    let mut residual_decision_output = residual_decisions.finish()?;
    physical_reconciliation_evidence.finish()?;
    let schema_authority = plan.schema_authority();
    if let Some(admission) = schema_admission_artifact_metadata(
        validation_program,
        schema_authority.baseline_schema_hash.clone(),
        schema_authority.effective_schema_hash.clone(),
        residual_decision_output.is_some(),
    ) {
        write_schema_admission_stream(builder, &admission, residual_decision_output.as_mut())?;
    }
    if let Some(mut statistics_profile) = statistics_profile {
        statistics_profile.write_stats(
            cdf_package::StatisticsProfileGrain::Package,
            0,
            &plan.package_id,
            statistics_profile_schema_hash,
            &profile.statistics,
        )?;
        statistics_profile.finish()?;
    }
    if verdict_summary.accepted_with_residual_rows > 0
        || verdict_summary.violation_count > 0
        || verdict_summary.quarantine_candidate_count > 0
    {
        builder.write_stats_artifact(
            "verdict-summary.json",
            &cdf_package::canonical_json_bytes(verdict_summary)?,
        )?;
    }
    if verdict_summary.quarantine_candidate_count > 0 {
        write_quarantine_summary(builder, verdict_summary, quarantine_part_count)?;
    }
    let partition_watermark_state = partition_watermarks
        .as_ref()
        .map(cdf_runtime::PartitionWatermarkTracker::snapshot)
        .transpose()?
        .unwrap_or_default();
    if drain_epoch_closure.is_some() {
        drain_controller
            .ok_or_else(|| CdfError::internal("drain closure omitted its controller authority"))?
            .stage_partition_watermarks(partition_watermark_state.clone())?;
        builder.write_json_artifact(
            cdf_package_contract::PARTITION_WATERMARK_STATE_FILE,
            &cdf_package_contract::PartitionWatermarkStateArtifact::new(
                partition_watermark_state.clone(),
            )?,
        )?;
    }
    late_data_evidence.finish()?;
    late_data_payloads.finish()?;
    builder.write_lineage_artifact(
        "lineage.json",
        &cdf_package::canonical_json_bytes(&lineage)?,
    )?;
    let execution_evidence = EngineExecutionEvidence::new(
        processed_observations,
        retry_journal.snapshot()?,
        plan.partition_schedule.as_ref(),
        checkpoint_eligible,
    )?;
    if let Some(closure) = drain_epoch_closure {
        builder.write_json_artifact("plan/epoch-frontier.json", &closure.frontier)?;
        builder.write_json_artifact("plan/epoch-closure.json", &closure.evidence)?;
    }
    Ok(PreparedPackageArtifacts {
        lineage,
        terminal_schema_quarantines: terminal_quarantines,
        partition_watermarks: partition_watermark_state,
        execution_evidence,
        residual_decision_output,
    })
}

// Reduction, canonical assembly, and the two publication sinks must share one snapshot and write
// transaction, so keeping their authorities explicit is safer than hiding them in ambient state.
#[allow(clippy::too_many_arguments)]
fn apply_dedup_and_write_pending_batches(
    builder: &PackageBuilder,
    program: &ValidationProgram,
    rule: &cdf_contract::PackageDedupRuleSpec,
    pending: Vec<PendingDedupBatch>,
    external: Option<ExternalDedupState>,
    segmentation_policy: &crate::CanonicalSegmentationPolicy,
    state: &mut OutputWriteState<'_>,
    sink: &mut SegmentOutputSink<'_, '_>,
) -> Result<AppliedDedup> {
    let validation_started = state.phase_measurements.start();
    let pending_input_bytes = pending
        .iter()
        .map(|batch| batch.output.get_array_memory_size() as u64)
        .sum();
    if let Some(ExternalDedupState {
        index,
        payload,
        mut effect_sort,
    }) = external
    {
        let validation_input_bytes = payload.input_bytes;
        let mut decisions = index.finish(rule.keep.clone())?;
        if let Some(memory) = state.memory {
            memory.record_event(cdf_memory::MemoryEvent::Spill {
                bytes: decisions.summary.spill_bytes,
            });
        }
        let mut payload = payload.finish()?;
        let mut assembler = None::<(
            u64,
            cdf_kernel::PackageSegmentKind,
            crate::CanonicalSegmentAssembler,
        )>;
        let mut provenance = DedupProvenanceSink::new();
        let mut expected_ordinal = 0_u64;
        let mut input_effects = cdf_kernel::KeyedEffectCounts::default();
        let mut surviving_effects = cdf_kernel::KeyedEffectCounts::default();
        while let Some(payload_batch) = match payload.as_mut() {
            Some(payload) => payload.next()?,
            None => None,
        } {
            add_effect_count(
                &mut input_effects,
                payload_batch.kind,
                u64::try_from(payload_batch.batch.num_rows())
                    .map_err(|_| CdfError::data("dedup payload row count exceeds u64"))?,
            )?;
            let mut retained = Vec::with_capacity(payload_batch.batch.num_rows());
            let mut retained_count = 0_u64;
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
                if keep {
                    retained_count = retained_count
                        .checked_add(1)
                        .ok_or_else(|| CdfError::data("surviving effect count overflowed u64"))?;
                }
                if !keep {
                    provenance.push(builder, decision.ordinal, decision.kept_ordinal)?;
                }
                expected_ordinal += 1;
            }
            let retained_keys = effect_sort.as_ref().map(|_| {
                payload_batch
                    .keys
                    .iter()
                    .zip(&retained)
                    .filter(|(_, keep)| **keep)
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>()
            });
            let output = filter_record_batch(&payload_batch.batch, &BooleanArray::from(retained))
                .map_err(CdfError::from)?;
            add_effect_count(&mut surviving_effects, payload_batch.kind, retained_count)?;
            if output.num_rows() == 0 {
                continue;
            }
            if let Some(sorter) = effect_sort.as_mut() {
                sorter.push(
                    payload_batch.kind,
                    payload_batch.output_position,
                    output,
                    retained_keys.ok_or_else(|| {
                        CdfError::internal("keyed-effect sort omitted retained exact keys")
                    })?,
                )?;
                continue;
            }
            if assembler
                .as_ref()
                .map(|(ordinal, kind, _)| (*ordinal, *kind))
                != Some((payload_batch.partition_ordinal, payload_batch.kind))
            {
                if let Some((_, kind, mut previous)) = assembler.take() {
                    state.kind = kind;
                    persist_canonical_segments(previous.finish()?, state, sink, None)?;
                }
                assembler = Some((
                    payload_batch.partition_ordinal,
                    payload_batch.kind,
                    crate::CanonicalSegmentAssembler::new(
                        segmentation_policy.clone(),
                        payload_batch.partition_ordinal,
                    )?,
                ));
            }
            let assembler = assembler
                .as_mut()
                .ok_or_else(|| CdfError::internal("dedup segment assembler was not initialized"))?;
            state.kind = payload_batch.kind;
            write_normalized_output_batch(
                PreparedKernelOutput {
                    output,
                    memory_lease: None,
                },
                payload_batch.output_position,
                payload_batch.partition_ordinal,
                &mut assembler.2,
                state,
                sink,
            )?;
        }
        if decisions.next()?.is_some() {
            return Err(CdfError::internal(
                "external dedup decision stream contains excess rows",
            ));
        }
        if let Some((_, kind, mut assembler)) = assembler {
            state.kind = kind;
            persist_canonical_segments(assembler.finish()?, state, sink, None)?;
        }
        if let Some(sorter) = effect_sort
            && let Some(mut sorted) = sorter.finish()?
        {
            let mut assembler = None::<(
                cdf_kernel::PackageSegmentKind,
                crate::CanonicalSegmentAssembler,
            )>;
            while let Some(effect) = sorted.next()? {
                if assembler.as_ref().map(|(kind, _)| *kind) != Some(effect.kind) {
                    if let Some((kind, mut previous)) = assembler.take() {
                        state.kind = kind;
                        persist_canonical_segments(previous.finish()?, state, sink, None)?;
                    }
                    assembler = Some((
                        effect.kind,
                        crate::CanonicalSegmentAssembler::new(segmentation_policy.clone(), 0)?,
                    ));
                }
                let (kind, assembler) = assembler.as_mut().ok_or_else(|| {
                    CdfError::internal("keyed-effect sort assembler was not initialized")
                })?;
                state.kind = *kind;
                write_normalized_output_batch(
                    PreparedKernelOutput {
                        output: effect.batch,
                        memory_lease: None,
                    },
                    effect.output_position,
                    0,
                    assembler,
                    state,
                    sink,
                )?;
            }
            if let Some((kind, mut assembler)) = assembler {
                state.kind = kind;
                persist_canonical_segments(assembler.finish()?, state, sink, None)?;
            }
        }
        let shard_count = provenance.finish(builder)?;
        let summary = cdf_contract::DedupSummary {
            rule_id: rule.rule_id.clone(),
            keys: rule.keys.clone(),
            keep: rule.keep.clone(),
            input_rows: decisions.summary.input_rows,
            output_rows: decisions.summary.output_rows,
            duplicate_key_count: decisions.summary.duplicate_key_count,
            dropped_row_count: decisions.summary.dropped_row_count,
            dropped_rows: Vec::new(),
        };
        write_dedup_summary_v3(builder, summary.clone(), shard_count)?;
        state.phase_measurements.add(
            RunPhase::ValidationNormalization,
            elapsed_ns(validation_started, "package dedup")?,
            validation_input_bytes,
            validation_input_bytes,
        );
        return Ok(AppliedDedup {
            summary,
            input_effects,
            surviving_effects,
        });
    }
    let validation_input_bytes = pending_input_bytes;
    if pending.iter().any(|batch| {
        matches!(
            batch.kind,
            cdf_kernel::PackageSegmentKind::Upsert | cdf_kernel::PackageSegmentKind::Delete
        )
    }) {
        return Err(CdfError::contract(
            "keyed-effect reduction and canonical ordering require bounded execution services",
        ));
    }
    let accepted = pending
        .iter()
        .map(|batch| batch.output.clone())
        .collect::<Vec<_>>();
    let dedup = evaluate_package_order_dedup(program, &accepted)?.ok_or_else(|| {
        CdfError::contract(
            "keyed effect reduction requires execution services when no authored dedup rule is present",
        )
    })?;
    let mut provenance = DedupProvenanceSink::new();
    for dropped in &dedup.summary.dropped_rows {
        provenance.push(
            builder,
            dropped.package_row_ordinal,
            dropped.kept_package_row_ordinal,
        )?;
    }
    let shard_count = provenance.finish(builder)?;
    write_dedup_summary_v3(builder, dedup.summary.clone(), shard_count)?;
    state.phase_measurements.add(
        RunPhase::ValidationNormalization,
        elapsed_ns(validation_started, "package dedup")?,
        validation_input_bytes,
        validation_input_bytes,
    );

    let mut input_effects = cdf_kernel::KeyedEffectCounts::default();
    let mut surviving_effects = cdf_kernel::KeyedEffectCounts::default();
    let mut assembler = None::<(
        u64,
        cdf_kernel::PackageSegmentKind,
        crate::CanonicalSegmentAssembler,
    )>;
    for (pending, retained_rows) in pending.into_iter().zip(dedup.retained_rows) {
        add_effect_count(
            &mut input_effects,
            pending.kind,
            u64::try_from(pending.output.num_rows())
                .map_err(|_| CdfError::data("dedup payload row count exceeds u64"))?,
        )?;
        let output =
            filter_record_batch(&pending.output, &retained_rows).map_err(CdfError::from)?;
        add_effect_count(
            &mut surviving_effects,
            pending.kind,
            u64::try_from(output.num_rows())
                .map_err(|_| CdfError::data("surviving effect count exceeds u64"))?,
        )?;
        if output.num_rows() == 0 {
            continue;
        }
        if assembler
            .as_ref()
            .map(|(ordinal, kind, _)| (*ordinal, *kind))
            != Some((pending.partition_ordinal, pending.kind))
        {
            if let Some((_, kind, mut previous)) = assembler.take() {
                state.kind = kind;
                persist_canonical_segments(previous.finish()?, state, sink, None)?;
            }
            assembler = Some((
                pending.partition_ordinal,
                pending.kind,
                crate::CanonicalSegmentAssembler::new(
                    segmentation_policy.clone(),
                    pending.partition_ordinal,
                )?,
            ));
        }
        let assembler = assembler
            .as_mut()
            .ok_or_else(|| CdfError::internal("dedup segment assembler was not initialized"))?;
        state.kind = pending.kind;
        write_normalized_output_batch(
            PreparedKernelOutput {
                output,
                memory_lease: None,
            },
            pending.output_position,
            pending.partition_ordinal,
            &mut assembler.2,
            state,
            sink,
        )?;
    }
    if let Some((_, kind, mut assembler)) = assembler {
        state.kind = kind;
        persist_canonical_segments(assembler.finish()?, state, sink, None)?;
    }
    Ok(AppliedDedup {
        summary: dedup.summary,
        input_effects,
        surviving_effects,
    })
}

fn add_effect_count(
    counts: &mut cdf_kernel::KeyedEffectCounts,
    kind: cdf_kernel::PackageSegmentKind,
    rows: u64,
) -> Result<()> {
    let target = match kind {
        cdf_kernel::PackageSegmentKind::Upsert => &mut counts.upserts,
        cdf_kernel::PackageSegmentKind::Delete => &mut counts.deletes,
        cdf_kernel::PackageSegmentKind::Row => return Ok(()),
    };
    *target = target
        .checked_add(rows)
        .ok_or_else(|| CdfError::data("keyed effect count overflowed u64"))?;
    Ok(())
}

pub fn planned_empty_package_content(
    plan: &EnginePlan,
) -> Result<cdf_kernel::PackageContentAuthority> {
    let content = match plan.write_disposition {
        WriteDisposition::Append | WriteDisposition::Replace => Ok(
            cdf_kernel::PackageContentAuthority::rows(plan.output_schema.arrow_schema_hash.clone()),
        ),
        WriteDisposition::Merge | WriteDisposition::CdcApply => {
            let rule = effective_keyed_effect_rule(plan)?;
            keyed_package_content(
                plan,
                keyed_reduction_authority(&rule, &empty_dedup_summary(&rule), plan, None, None)?,
            )
        }
    }?;
    let Some(family) = &plan.route_family else {
        return Ok(content);
    };
    let schema =
        cdf_kernel::CanonicalArrowSchema::from_arrow(plan.output_schema.to_arrow()?.as_ref())?;
    let routed = cdf_kernel::PackageContentAuthority::Routed {
        family: family.clone(),
        outputs: family
            .bindings
            .iter()
            .map(|binding| cdf_kernel::RoutedOutputContentAuthority {
                output_binding: binding.output_binding.clone(),
                schema: schema.clone(),
                content: Box::new(content.clone()),
                segment_ids: Vec::new(),
            })
            .collect(),
    };
    routed.validate()?;
    Ok(routed)
}

fn encode_effect_keys(
    plan: &EnginePlan,
    program: &ValidationProgram,
    rule: &cdf_contract::PackageDedupRuleSpec,
    batch: &RecordBatch,
) -> Result<Vec<Vec<u8>>> {
    if matches!(
        plan.write_disposition,
        WriteDisposition::Merge | WriteDisposition::CdcApply
    ) {
        for key in &plan.effect_key {
            let matches = batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| field.name() == key)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let [index] = matches.as_slice() else {
                return Err(CdfError::data(format!(
                    "effect key field `{key}` must resolve to exactly one package output field"
                )));
            };
            if batch.column(*index).null_count() != 0 {
                return Err(CdfError::data(format!(
                    "effect key field `{key}` contains null values"
                )));
            }
        }
    }
    let mut keys = encode_package_dedup_keys(program, rule, batch)?;
    if let Some(family) = &plan.route_family {
        let assignments = route_output_indices(family, batch)?;
        let mut row_outputs = vec![0_u32; batch.num_rows()];
        for (output, rows) in assignments.into_iter().enumerate() {
            let output = u32::try_from(output)
                .map_err(|_| CdfError::data("routed output ordinal exceeds u32"))?;
            for row in rows {
                row_outputs[row as usize] = output;
            }
        }
        for (key, output) in keys.iter_mut().zip(row_outputs) {
            let mut namespaced = Vec::with_capacity(key.len() + 5);
            namespaced.push(b'R');
            namespaced.extend_from_slice(&output.to_be_bytes());
            namespaced.append(key);
            *key = namespaced;
        }
    }
    Ok(keys)
}

fn prepare_delete_effect_batch(
    plan: &EnginePlan,
    batch: &cdf_kernel::Batch,
    input: &RecordBatch,
) -> Result<RecordBatch> {
    if input.num_rows()
        != usize::try_from(batch.header.row_count)
            .map_err(|_| CdfError::data("CDC delete row count exceeds usize"))?
    {
        return Err(CdfError::data(
            "CDC delete batch header row count does not match its Arrow payload",
        ));
    }
    let observed_hash = cdf_kernel::canonical_arrow_schema_hash(input.schema().as_ref())?;
    if observed_hash != batch.header.observed_schema_hash {
        return Err(CdfError::data(
            "CDC delete batch schema does not match its observed schema authority",
        ));
    }
    let output_schema = plan.output_schema.to_arrow()?;
    let input_schema = input.schema();
    let route_field = plan
        .route_family
        .as_ref()
        .map(|family| family.route.field.as_str())
        .filter(|field| !plan.effect_key.iter().any(|key| key == field));
    let expected_columns = plan.effect_key.len() + usize::from(route_field.is_some());
    if input.num_columns() != expected_columns {
        return Err(CdfError::data(
            "CDC delete batches must contain exactly the complete ordered effect key and, when routed, its protected route field",
        ));
    }
    let mut output_indices = Vec::with_capacity(expected_columns);
    let mut columns = Vec::with_capacity(expected_columns);
    for key in &plan.effect_key {
        let output_matches = output_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() == key)
            .collect::<Vec<_>>();
        let [(output_index, output_field)] = output_matches.as_slice() else {
            return Err(CdfError::contract(format!(
                "CDC delete key `{key}` does not resolve exactly once in the compiled output schema"
            )));
        };
        let input_matches = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() == key)
            .collect::<Vec<_>>();
        let [(input_index, input_field)] = input_matches.as_slice() else {
            return Err(CdfError::data(format!(
                "CDC delete key `{key}` does not resolve exactly once in the delete payload"
            )));
        };
        if input_field.as_ref() != output_field.as_ref() {
            return Err(CdfError::data(format!(
                "CDC delete key `{key}` does not preserve its compiled Arrow field authority"
            )));
        }
        if input.column(*input_index).null_count() != 0 {
            return Err(CdfError::data(format!(
                "CDC delete key `{key}` contains null values"
            )));
        }
        output_indices.push(*output_index);
        columns.push(Arc::clone(input.column(*input_index)));
    }
    if let Some(route_field) = route_field {
        let output_matches = output_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() == route_field)
            .collect::<Vec<_>>();
        let [(output_index, output_field)] = output_matches.as_slice() else {
            return Err(CdfError::contract(format!(
                "CDC route field `{route_field}` does not resolve exactly once in the compiled output schema"
            )));
        };
        let input_matches = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() == route_field)
            .collect::<Vec<_>>();
        let [(input_index, input_field)] = input_matches.as_slice() else {
            return Err(CdfError::data(format!(
                "CDC route field `{route_field}` does not resolve exactly once in the delete payload"
            )));
        };
        if input_field.as_ref() != output_field.as_ref()
            || input.column(*input_index).null_count() != 0
        {
            return Err(CdfError::data(format!(
                "CDC route field `{route_field}` must preserve its non-null compiled Arrow field authority"
            )));
        }
        output_indices.push(*output_index);
        columns.push(Arc::clone(input.column(*input_index)));
    }
    let delete_schema = Arc::new(
        output_schema
            .project(&output_indices)
            .map_err(CdfError::from)?,
    );
    RecordBatch::try_new(delete_schema, columns).map_err(CdfError::from)
}

fn effective_keyed_effect_rule(plan: &EnginePlan) -> Result<cdf_contract::PackageDedupRuleSpec> {
    if let Some(rule) = package_dedup_rule(&plan.validation_program)? {
        return Ok(rule);
    }
    Ok(cdf_contract::PackageDedupRuleSpec {
        rule_id: "cdf-keyed-effect-unique".to_owned(),
        keys: plan.effect_key.clone(),
        keep: match plan.write_disposition {
            WriteDisposition::Merge => cdf_contract::DedupKeepProgram::Fail,
            WriteDisposition::CdcApply => cdf_contract::DedupKeepProgram::Last,
            WriteDisposition::Append | WriteDisposition::Replace => {
                return Err(CdfError::internal(
                    "ordinary-row disposition requested a keyed-effect rule",
                ));
            }
        },
    })
}

fn empty_dedup_summary(rule: &cdf_contract::PackageDedupRuleSpec) -> cdf_contract::DedupSummary {
    cdf_contract::DedupSummary {
        rule_id: rule.rule_id.clone(),
        keys: rule.keys.clone(),
        keep: rule.keep.clone(),
        input_rows: 0,
        output_rows: 0,
        duplicate_key_count: 0,
        dropped_row_count: 0,
        dropped_rows: Vec::new(),
    }
}

fn keyed_package_content(
    plan: &EnginePlan,
    reduction: cdf_kernel::KeyedEffectReductionAuthority,
) -> Result<cdf_kernel::PackageContentAuthority> {
    let (key, delete_schema_hash) = keyed_effect_key_authority(plan)?;
    let content = cdf_kernel::PackageContentAuthority::KeyedChanges {
        logical_schema_hash: plan.output_schema.arrow_schema_hash.clone(),
        upsert_schema_hash: plan.output_schema.arrow_schema_hash.clone(),
        delete_schema_hash,
        key,
        reduction: Box::new(reduction),
        deletion_capture: plan.keyed_effects.deletion_capture.clone(),
        delete_application: plan.keyed_effects.delete_application.clone(),
    };
    content.validate()?;
    Ok(content)
}

fn routed_package_content(
    plan: &EnginePlan,
    routing: &RoutedWriteState,
    reduction: Option<&cdf_kernel::KeyedEffectReductionAuthority>,
) -> Result<cdf_kernel::PackageContentAuthority> {
    if routing.family
        != *plan.route_family.as_ref().ok_or_else(|| {
            CdfError::internal("routed package writer has no matching engine plan family")
        })?
    {
        return Err(CdfError::internal(
            "routed package writer family changed during execution",
        ));
    }
    if reduction.is_some_and(|reduction| {
        reduction.duplicate_key_count != 0 || reduction.input != reduction.surviving
    }) {
        return Err(CdfError::data(
            "one routed settlement unit contains repeated destination keys; reduce the CDC/source epoch so each routed key has one effect",
        ));
    }
    let schema =
        cdf_kernel::CanonicalArrowSchema::from_arrow(plan.output_schema.to_arrow()?.as_ref())?;
    let outputs = routing
        .family
        .bindings
        .iter()
        .zip(&routing.outputs)
        .map(|(binding, output)| {
            let inner = match plan.write_disposition {
                WriteDisposition::Append | WriteDisposition::Replace => {
                    cdf_kernel::PackageContentAuthority::rows(binding.schema_hash.clone())
                }
                WriteDisposition::Merge | WriteDisposition::CdcApply => {
                    let mut route_reduction = reduction.cloned().ok_or_else(|| {
                        CdfError::internal("routed keyed package omitted its reduction authority")
                    })?;
                    route_reduction.input = output.input_effects;
                    route_reduction.surviving = output.surviving_effects;
                    route_reduction.duplicate_key_count = 0;
                    keyed_package_content(plan, route_reduction)?
                }
            };
            Ok(cdf_kernel::RoutedOutputContentAuthority {
                output_binding: binding.output_binding.clone(),
                schema: schema.clone(),
                content: Box::new(inner),
                segment_ids: output.segment_ids.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let content = cdf_kernel::PackageContentAuthority::Routed {
        family: routing.family.clone(),
        outputs,
    };
    content.validate()?;
    Ok(content)
}

fn keyed_effect_key_authority(
    plan: &EnginePlan,
) -> Result<(cdf_kernel::KeyAuthority, cdf_kernel::SchemaHash)> {
    if plan.effect_key.is_empty() {
        return Err(CdfError::contract(
            "merge and cdc_apply require a nonempty ordered effect key",
        ));
    }
    let output = plan.output_schema.to_arrow()?;
    let mut indices = Vec::with_capacity(plan.effect_key.len());
    for key in &plan.effect_key {
        let matches = output
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() == key)
            .collect::<Vec<_>>();
        let [(index, field)] = matches.as_slice() else {
            return Err(CdfError::contract(format!(
                "effect key field `{key}` must resolve to exactly one normalized output field"
            )));
        };
        if field.is_nullable() {
            return Err(CdfError::contract(format!(
                "effect key field `{key}` must be non-nullable"
            )));
        }
        indices.push(*index);
    }
    let key_schema = output.project(&indices).map_err(CdfError::from)?;
    let key_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&key_schema)?;
    let mut delete_indices = indices;
    if let Some(family) = &plan.route_family
        && !plan.effect_key.contains(&family.route.field)
    {
        let route_index = output
            .fields()
            .iter()
            .position(|field| field.name() == &family.route.field)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "route field `{}` is absent from the normalized output schema",
                    family.route.field
                ))
            })?;
        delete_indices.push(route_index);
    }
    let delete_schema = output.project(&delete_indices).map_err(CdfError::from)?;
    let delete_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&delete_schema)?;
    Ok((
        cdf_kernel::KeyAuthority {
            version: cdf_kernel::KEYED_EFFECT_AUTHORITY_VERSION,
            fields: plan.effect_key.clone(),
            encoding: cdf_kernel::DEDUP_KEY_ENCODING_VERSION.to_owned(),
            schema_hash: key_schema_hash,
        },
        delete_schema_hash,
    ))
}

fn keyed_reduction_authority(
    rule: &cdf_contract::PackageDedupRuleSpec,
    summary: &cdf_contract::DedupSummary,
    plan: &EnginePlan,
    cdc_order_identity: Option<&(String, String)>,
    effect_counts: Option<(cdf_kernel::KeyedEffectCounts, cdf_kernel::KeyedEffectCounts)>,
) -> Result<cdf_kernel::KeyedEffectReductionAuthority> {
    if summary.keys != plan.effect_key {
        return Err(CdfError::contract(
            "keyed-effect reduction key does not match the compiled resource effect key",
        ));
    }
    let (winner, input_order) = match rule.keep {
        cdf_contract::DedupKeepProgram::Fail => (
            cdf_kernel::KeyedEffectWinnerPolicy::Fail,
            cdf_kernel::KeyedEffectInputOrder::Unordered,
        ),
        cdf_contract::DedupKeepProgram::First => (
            cdf_kernel::KeyedEffectWinnerPolicy::First,
            cdf_kernel::KeyedEffectInputOrder::CanonicalPackageRows { version: 1 },
        ),
        cdf_contract::DedupKeepProgram::Last => {
            let input_order = if plan.write_disposition == WriteDisposition::CdcApply {
                match cdc_order_identity {
                    Some((protocol, scope_sha256)) => {
                        cdf_kernel::KeyedEffectInputOrder::SourceProtocol {
                            protocol: protocol.clone(),
                            version: 1,
                            scope_sha256: scope_sha256.clone(),
                        }
                    }
                    None if summary.input_rows == 0 => {
                        cdf_kernel::KeyedEffectInputOrder::CanonicalPackageRows { version: 1 }
                    }
                    None => {
                        return Err(CdfError::data(
                            "cdc_apply reduction lacks source-protocol order authority",
                        ));
                    }
                }
            } else {
                cdf_kernel::KeyedEffectInputOrder::CanonicalPackageRows { version: 1 }
            };
            (cdf_kernel::KeyedEffectWinnerPolicy::Last, input_order)
        }
    };
    let (input, surviving) = effect_counts.unwrap_or((
        cdf_kernel::KeyedEffectCounts {
            upserts: summary.input_rows,
            deletes: 0,
        },
        cdf_kernel::KeyedEffectCounts {
            upserts: summary.output_rows,
            deletes: 0,
        },
    ));
    if input.total()? != summary.input_rows || surviving.total()? != summary.output_rows {
        return Err(CdfError::data(
            "typed keyed-effect counts do not match the exact-key reduction summary",
        ));
    }
    let reduction = cdf_kernel::KeyedEffectReductionAuthority {
        version: cdf_kernel::KEYED_EFFECT_AUTHORITY_VERSION,
        winner,
        input_order,
        input,
        duplicate_key_count: summary.duplicate_key_count,
        surviving,
        provenance_format: "parquet".to_owned(),
        provenance_version: cdf_package_contract::DEDUP_PROVENANCE_VERSION,
    };
    let (key, _) = keyed_effect_key_authority(plan)?;
    reduction.validate(&key)?;
    Ok(reduction)
}

fn write_dedup_summary_v3(
    builder: &PackageBuilder,
    summary: cdf_contract::DedupSummary,
    shard_count: u64,
) -> Result<()> {
    let keep = match summary.keep {
        cdf_contract::DedupKeepProgram::First => cdf_package_contract::PackageDedupKeep::First,
        cdf_contract::DedupKeepProgram::Last => cdf_package_contract::PackageDedupKeep::Last,
        cdf_contract::DedupKeepProgram::Fail => cdf_package_contract::PackageDedupKeep::Fail,
    };
    builder.write_dedup_summary(&cdf_package_contract::PackageDedupSummary {
        version: cdf_package_contract::DEDUP_SUMMARY_VERSION,
        rule_id: summary.rule_id,
        keys: summary.keys,
        keep,
        input_rows: summary.input_rows,
        output_rows: summary.output_rows,
        duplicate_key_count: summary.duplicate_key_count,
        dropped_row_count: summary.dropped_row_count,
        provenance_format: "parquet".to_owned(),
        provenance_version: cdf_package_contract::DEDUP_PROVENANCE_VERSION,
        provenance_path: cdf_package_contract::DEDUP_PROVENANCE_DIRECTORY.to_owned(),
        provenance_shard_row_target: DEDUP_PROVENANCE_SHARD_ROWS as u64,
        shard_count,
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
    partition_ordinal: u64,
    partition: &PartitionPlan,
    completion: &SourcePosition,
) -> Result<()> {
    for position in positions
        .iter_mut()
        .filter(|position| position.partition_ordinal == partition_ordinal)
    {
        match &mut position.output_position {
            Some(existing) => {
                *existing = merge_terminal_position_evidence(existing, completion)?;
            }
            None if completion.is_batch_slice_invariant() => {
                // Some indivisible units cannot prove checkpoint-safe identity until EOF (for
                // example weakly versioned HTTP files gaining a content hash). Their terminal
                // position applies exactly to every segment produced by that unit.
                position.output_position = Some(completion.clone());
            }
            None => {
                return Err(CdfError::data(format!(
                    "segment {} for partition `{}` omitted slice-position evidence required by its non-invariant terminal attestation",
                    position.segment_id.as_str(),
                    partition.partition_id.as_str()
                )));
            }
        }
    }
    // A fully consumed partition may legitimately produce no output segment after filtering,
    // quarantine, or package-wide dedup. Its processed/checkpoint evidence still retains the
    // terminal content identity; there is simply no segment position to enrich.
    Ok(())
}

fn aggregate_processed_partition_positions(
    observation_id: &str,
    descriptor: &cdf_kernel::ResourceDescriptor,
    schema: &Schema,
    observed: Option<&SourcePosition>,
    attested: Option<SourcePosition>,
) -> Result<SourcePosition> {
    let observed = observed.cloned();
    let raw = match (observed, attested) {
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
    }?;
    aggregate_resource_output_position(descriptor, schema, None, &[raw]).map_err(|error| {
        CdfError::data(format!(
            "processed observation {observation_id:?} cannot close its source-position evidence: {error}"
        ))
    })
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
        aggregate_resource_closed_output_position(
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

fn drain_source_continuation_positions(
    committed: Option<&SourcePosition>,
    partitions: &[PartitionPlan],
) -> Result<BTreeMap<String, SourcePosition>> {
    let Some(committed) = committed else {
        return Ok(BTreeMap::new());
    };
    committed.validate()?;
    let positions = match committed {
        SourcePosition::Composite(composite) => composite.positions.clone(),
        position if partitions.len() == 1 => BTreeMap::from([(
            partitions[0].partition_id.as_str().to_owned(),
            position.clone(),
        )]),
        _ => {
            return Err(CdfError::data(
                "multi-partition drain checkpoint requires partition-keyed source continuation",
            ));
        }
    };
    Ok(positions)
}

fn record_drain_partition_position(
    positions: &mut BTreeMap<String, SourcePosition>,
    partition: &PartitionPlan,
    position: SourcePosition,
) -> Result<()> {
    position.validate()?;
    positions.insert(partition.partition_id.as_str().to_owned(), position);
    Ok(())
}

fn drain_source_continuation(
    positions: &BTreeMap<String, SourcePosition>,
) -> Result<Option<SourcePosition>> {
    if positions.is_empty() {
        return Ok(None);
    }
    if positions
        .values()
        .all(SourcePosition::is_batch_slice_invariant)
    {
        return Ok(None);
    }
    if positions.len() == 1 {
        return Ok(positions.values().next().cloned());
    }
    let continuation = SourcePosition::Composite(CompositePosition {
        version: SOURCE_POSITION_VERSION,
        positions: positions.clone(),
    });
    continuation.validate()?;
    Ok(Some(continuation))
}

#[allow(clippy::too_many_arguments)]
fn observe_drain_batch_frontier(
    controller: Option<&mut cdf_runtime::DrainEpochController>,
    descriptor: &cdf_kernel::ResourceDescriptor,
    schema: &Schema,
    processed: &[ProcessedObservationPosition],
    partition: &PartitionPlan,
    observed_partition_position: Option<&SourcePosition>,
    partition_positions: &mut BTreeMap<String, SourcePosition>,
    admitted_rows: u64,
    admitted_bytes: u64,
    global_watermark: Option<WatermarkClaim>,
    monotonic_milliseconds: u64,
    observed_at_unix_milliseconds: u64,
) -> Result<Option<(cdf_runtime::DrainEpochDecision, SourcePosition)>> {
    let Some(controller) = controller else {
        return Ok(None);
    };
    let observation_id = cdf_kernel::partition_schema_observation_id(partition);
    let partition_position = aggregate_processed_partition_positions(
        observation_id,
        descriptor,
        schema,
        observed_partition_position,
        None,
    )?;
    record_drain_partition_position(partition_positions, partition, partition_position.clone())?;
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
    let decision = controller.observe_safe_frontier(cdf_runtime::DrainSafeFrontierObservation {
        frontier,
        carryover: drain_source_continuation(partition_positions)?,
        admitted_batches: 1,
        admitted_rows,
        admitted_bytes,
        admitted_positions: 1,
        global_watermark,
        source_exhausted: false,
        monotonic_milliseconds,
        observed_at_unix_milliseconds,
    })?;
    if matches!(decision, cdf_runtime::DrainEpochDecision::FinishedNoOp) {
        return Err(CdfError::internal(
            "drain controller classified a processed batch position as an empty epoch",
        ));
    }
    Ok(Some((decision, partition_position)))
}

fn merge_verdict_summary(total: &mut VerdictSummary, batch: VerdictSummary) {
    total.input_rows += batch.input_rows;
    total.accepted_rows += batch.accepted_rows;
    total.accepted_with_residual_rows += batch.accepted_with_residual_rows;
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
        accepted_with_residual_rows: 0,
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

fn late_data_evidence_retained_bytes(evidence: &LateDataBatchEvidence) -> Result<u64> {
    let row_bytes = std::mem::size_of::<LateDataRowEvidence>()
        .checked_mul(evidence.rows.capacity())
        .ok_or_else(|| CdfError::data("late-data row evidence memory overflow"))?;
    let structural_bytes = std::mem::size_of::<LateDataBatchEvidence>()
        .checked_add(row_bytes)
        .ok_or_else(|| CdfError::data("late-data batch evidence memory overflow"))?;
    let serialized_bytes = cdf_package::canonical_json_bytes(evidence)?.len();
    u64::try_from(structural_bytes.saturating_add(serialized_bytes))
        .map_err(|_| CdfError::data("late-data evidence memory exceeds u64"))
}

fn quarantine_record_from_late_data(
    evidence: &LateDataBatchEvidence,
    row: &LateDataRowEvidence,
) -> Result<QuarantineRecord> {
    Ok(QuarantineRecord {
        source_row_ordinal: row.source_row_ordinal,
        rule_id: "cdf.late_data".to_owned(),
        error_code: "cdf.event_time_behind_watermark".to_owned(),
        source_position: evidence.source_position.clone(),
        observed_value_redacted: QuarantineObservedValue::Preserved {
            value: serde_json::to_string(&row.event_time).map_err(|error| {
                CdfError::internal(format!("serialize late-data event time: {error}"))
            })?,
        },
    })
}

fn apply_late_data_quarantine_summary(
    summary: &mut VerdictSummary,
    evidence: &LateDataBatchEvidence,
    residual_rows: u64,
) -> Result<()> {
    let row_count = u64::try_from(evidence.rows.len())
        .map_err(|_| CdfError::data("late-data quarantine count exceeds u64"))?;
    remove_late_data_from_accepted_summary(summary, row_count, residual_rows)?;
    summary.quarantined_rows = summary
        .quarantined_rows
        .checked_add(row_count)
        .ok_or_else(|| CdfError::data("quarantined row count overflow"))?;
    summary.violation_count = summary
        .violation_count
        .checked_add(row_count)
        .ok_or_else(|| CdfError::data("contract violation count overflow"))?;
    summary.quarantine_candidate_count = summary
        .quarantine_candidate_count
        .checked_add(row_count)
        .ok_or_else(|| CdfError::data("quarantine candidate count overflow"))?;
    match summary
        .rule_summaries
        .iter_mut()
        .find(|rule| rule.rule_id == "cdf.late_data")
    {
        Some(rule) => {
            rule.checked_rows = rule
                .checked_rows
                .checked_add(row_count)
                .ok_or_else(|| CdfError::data("late-data checked row count overflow"))?;
            rule.violation_count = rule
                .violation_count
                .checked_add(row_count)
                .ok_or_else(|| CdfError::data("late-data violation count overflow"))?;
        }
        None => summary
            .rule_summaries
            .push(cdf_contract::RuleVerdictSummary {
                rule_id: "cdf.late_data".to_owned(),
                error_code: "cdf.event_time_behind_watermark".to_owned(),
                checked_rows: row_count,
                violation_count: row_count,
            }),
    }
    Ok(())
}

fn remove_late_data_from_accepted_summary(
    summary: &mut VerdictSummary,
    row_count: u64,
    residual_rows: u64,
) -> Result<()> {
    if residual_rows > row_count {
        return Err(CdfError::internal(
            "late-data residual row count exceeds withheld row count",
        ));
    }
    summary.accepted_rows = summary
        .accepted_rows
        .checked_sub(row_count)
        .ok_or_else(|| {
            CdfError::internal("late-data removal exceeds contract-accepted row count")
        })?;
    summary.accepted_with_residual_rows = summary
        .accepted_with_residual_rows
        .checked_sub(residual_rows)
        .ok_or_else(|| {
            CdfError::internal(
                "late-data residual removal exceeds accepted-with-residual row count",
            )
        })?;
    Ok(())
}

fn residual_row_count(batch: &RecordBatch) -> Result<u64> {
    batch
        .column_by_name(VARIANT_COLUMN_NAME)
        .map_or(Ok(0), |variant| {
            u64::try_from(variant.len() - variant.null_count())
                .map_err(|error| CdfError::internal(error.to_string()))
        })
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

fn write_schema_admission_stream(
    builder: &PackageBuilder,
    admission: &SchemaAdmissionArtifact,
    mut decisions: Option<&mut ResidualDecisionOutput>,
) -> Result<()> {
    let mut artifact =
        builder.begin_streaming_identity_artifact("schema/admission-evidence.json")?;
    artifact.write_all(b"{\"baseline_schema_hash\":")?;
    artifact.write_json(&admission.baseline_schema_hash)?;
    artifact.write_all(b",\"effective_schema_hash\":")?;
    artifact.write_json(&admission.effective_schema_hash)?;
    artifact.write_all(b",\"residual_capture\":")?;
    artifact.write_json(&admission.residual_capture)?;
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
    artifact.write_json(&admission.variant_capture)?;
    artifact.write_all(b",\"version\":")?;
    artifact.write_json(&admission.version)?;
    artifact.write_all(b"}")?;
    artifact.finish()?;
    Ok(())
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
    partition_ordinal: u64,
    assembler: &mut crate::CanonicalSegmentAssembler,
    state: &mut OutputWriteState<'_>,
    sink: &mut SegmentOutputSink<'_, '_>,
) -> Result<()> {
    if sink.routing.is_some() {
        return route_normalized_output_batch(
            prepared,
            output_position,
            partition_ordinal,
            state,
            sink,
        );
    }
    let canonical_segments =
        assembler.push_accounted(prepared.output, output_position, prepared.memory_lease)?;
    persist_canonical_segments(canonical_segments, state, sink, None)
}

fn route_normalized_output_batch(
    prepared: PreparedKernelOutput,
    output_position: Option<SourcePosition>,
    partition_ordinal: u64,
    state: &mut OutputWriteState<'_>,
    sink: &mut SegmentOutputSink<'_, '_>,
) -> Result<()> {
    let routing = sink
        .routing
        .take()
        .ok_or_else(|| CdfError::internal("routed output state is absent"))?;
    let result = routing.write_batch(prepared, output_position, partition_ordinal, state, sink);
    sink.routing = Some(routing);
    result
}

impl RoutedWriteState {
    fn new(
        family: cdf_kernel::RouteTargetFamily,
        segmentation: crate::CanonicalSegmentationPolicy,
    ) -> Self {
        let outputs = family
            .bindings
            .iter()
            .map(|_| RoutedOutputWriteState {
                active: None,
                segment_ids: Vec::new(),
                input_effects: cdf_kernel::KeyedEffectCounts::default(),
                surviving_effects: cdf_kernel::KeyedEffectCounts::default(),
            })
            .collect();
        Self {
            family,
            segmentation,
            outputs,
        }
    }

    fn observe_input(
        &mut self,
        kind: cdf_kernel::PackageSegmentKind,
        batch: &RecordBatch,
    ) -> Result<()> {
        let assignments = route_output_indices(&self.family, batch)?;
        for (output, indices) in self.outputs.iter_mut().zip(assignments) {
            add_effect_count(
                &mut output.input_effects,
                kind,
                u64::try_from(indices.len())
                    .map_err(|_| CdfError::data("routed input row count exceeds u64"))?,
            )?;
        }
        Ok(())
    }

    fn write_batch(
        &mut self,
        prepared: PreparedKernelOutput,
        output_position: Option<SourcePosition>,
        partition_ordinal: u64,
        state: &mut OutputWriteState<'_>,
        sink: &mut SegmentOutputSink<'_, '_>,
    ) -> Result<()> {
        let assignments = route_output_indices(&self.family, &prepared.output)?;
        let _input_lease = prepared.memory_lease;
        for (output_index, indices) in assignments.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let indices = UInt32Array::from(indices);
            let batch = take_record_batch(&prepared.output, &indices).map_err(CdfError::from)?;
            let retained = cdf_memory::record_batch_retained_bytes(&batch)?.max(1);
            let memory_lease = match state.memory.map(Arc::clone) {
                Some(memory) => {
                    let request = ReservationRequest::new(
                        ConsumerKey::new("routed-output-partition", MemoryClass::Package)?,
                        retained,
                    )?
                    .as_minimum_working_set();
                    let lease = reserve_with_encode_backpressure(
                        memory,
                        &request,
                        state,
                        sink,
                        "routed output partition requires accounted Arrow buffers",
                    )?;
                    lease.reconcile(retained)?;
                    Some(lease)
                }
                None => None,
            };
            let binding = self.family.bindings[output_index].output_binding.clone();
            let output = &mut self.outputs[output_index];
            if output.active.as_ref().is_some_and(|(partition, kind, _)| {
                *partition != partition_ordinal || *kind != state.kind
            }) && let Some((_, kind, mut assembler)) = output.active.take()
            {
                let prior_kind = state.kind;
                state.kind = kind;
                persist_routed_segments(assembler.finish()?, &binding, output, state, sink)?;
                state.kind = prior_kind;
            }
            if output.active.is_none() {
                output.active = Some((
                    partition_ordinal,
                    state.kind,
                    crate::CanonicalSegmentAssembler::new(
                        self.segmentation.clone(),
                        partition_ordinal,
                    )?,
                ));
            }
            let canonical = output
                .active
                .as_mut()
                .ok_or_else(|| CdfError::internal("routed segment assembler is absent"))?
                .2
                .push_accounted(batch, output_position.clone(), memory_lease)?;
            persist_routed_segments(canonical, &binding, output, state, sink)?;
        }
        Ok(())
    }

    fn finish(
        &mut self,
        state: &mut OutputWriteState<'_>,
        sink: &mut SegmentOutputSink<'_, '_>,
    ) -> Result<()> {
        for (binding, output) in self.family.bindings.iter().zip(&mut self.outputs) {
            if let Some((_, kind, mut assembler)) = output.active.take() {
                state.kind = kind;
                persist_routed_segments(
                    assembler.finish()?,
                    &binding.output_binding,
                    output,
                    state,
                    sink,
                )?;
            }
        }
        Ok(())
    }
}

fn persist_routed_segments(
    segments: Vec<crate::CanonicalSegment>,
    binding: &cdf_kernel::OutputBindingId,
    output: &mut RoutedOutputWriteState,
    state: &mut OutputWriteState<'_>,
    sink: &mut SegmentOutputSink<'_, '_>,
) -> Result<()> {
    for segment in segments {
        let segment_id = output_segment_id(
            state.kind,
            segment.segment_id.clone(),
            Some(binding),
            Some(sink.queue.next_submission),
        )?;
        output.segment_ids.push(segment_id);
        add_effect_count(&mut output.surviving_effects, state.kind, segment.row_count)?;
        persist_canonical_segments(vec![segment], state, sink, Some(binding))?;
    }
    Ok(())
}

fn route_output_indices(
    family: &cdf_kernel::RouteTargetFamily,
    batch: &RecordBatch,
) -> Result<Vec<Vec<u32>>> {
    let matches = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name() == &family.route.field)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let [route_index] = matches.as_slice() else {
        return Err(CdfError::data(format!(
            "route field `{}` must resolve exactly once in every normalized output batch",
            family.route.field
        )));
    };
    let route = batch.column(*route_index);
    let mut assignments = vec![Vec::new(); family.bindings.len()];
    for row in 0..batch.num_rows() {
        let value = cdf_kernel::RouteScalar::from_array(route.as_ref(), row)?;
        let index = family
            .bindings
            .binary_search_by(|binding| binding.route_value.cmp(&value))
            .map_err(|_| {
                CdfError::data(
                    "normalized output contains a route value absent from compiled output authority; discover and compile the output before retrying",
                )
            })?;
        assignments[index]
            .push(u32::try_from(row).map_err(|_| CdfError::data("routed batch row exceeds u32"))?);
    }
    Ok(assignments)
}

fn persist_canonical_segments(
    canonical_segments: Vec<crate::CanonicalSegment>,
    state: &mut OutputWriteState<'_>,
    sink: &mut SegmentOutputSink<'_, '_>,
    output_binding: Option<&cdf_kernel::OutputBindingId>,
) -> Result<()> {
    for canonical in canonical_segments {
        let crate::CanonicalSegment {
            segment_id: canonical_segment_id,
            partition_ordinal,
            batches,
            output_position,
            row_count,
            retained_bytes,
            canonical_batch_rows,
            canonical_batch_bytes,
            unaccounted_retained_bytes,
            memory_leases: _transform_memory_leases,
            ..
        } = canonical;
        let segment_id = output_segment_id(
            state.kind,
            canonical_segment_id,
            output_binding,
            output_binding.map(|_| sink.queue.next_submission),
        )?;
        let mut _memory_lease = match state.memory.map(Arc::clone) {
            Some(memory) => {
                let canonical_output_allocation_bytes =
                    if crate::segmentation::canonicalization_is_zero_copy(
                        &batches,
                        canonical_batch_rows,
                        canonical_batch_bytes,
                    )? {
                        0
                    } else {
                        retained_bytes
                    };
                let bytes = canonical_construction_reservation_bytes(
                    canonical_output_allocation_bytes,
                    row_count,
                    unaccounted_retained_bytes,
                )?;
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
                        "canonical segment requires {bytes} bytes for concat output, package ordinal, and any unaccounted input"
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
        if state.statistics.is_some() && state.kind != cdf_kernel::PackageSegmentKind::Delete {
            let statistics_reservation_bytes = statistics_computation_reservation_bytes(&output)?;
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
        // Traveling transform leases continue to own every reused input buffer through
        // encode/staged ingress. The construction lease owns only allocations created here:
        // concatenated replacement buffers, package ordinals, and any input that arrived without
        // a traveling lease. Reconciling it to the complete output would charge reused arrays
        // twice and can exhaust the pool despite exact upstream accounting.
        sink.queue.submit(
            SegmentEncodeWork {
                ordinal: 0,
                kind: state.kind,
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

fn effect_segment_id(
    kind: cdf_kernel::PackageSegmentKind,
    canonical: cdf_kernel::SegmentId,
) -> Result<cdf_kernel::SegmentId> {
    let prefix = match kind {
        cdf_kernel::PackageSegmentKind::Row => return Ok(canonical),
        cdf_kernel::PackageSegmentKind::Upsert => "effect-0-upsert",
        cdf_kernel::PackageSegmentKind::Delete => "effect-1-delete",
    };
    cdf_kernel::SegmentId::new(format!("{prefix}-{}", canonical.as_str()))
}

fn output_segment_id(
    kind: cdf_kernel::PackageSegmentKind,
    canonical: cdf_kernel::SegmentId,
    output_binding: Option<&cdf_kernel::OutputBindingId>,
    routed_ordinal: Option<u64>,
) -> Result<cdf_kernel::SegmentId> {
    let effect = effect_segment_id(kind, canonical)?;
    match (output_binding, routed_ordinal) {
        (Some(binding), Some(ordinal)) => cdf_kernel::SegmentId::new(format!(
            "route-{ordinal:020}-{}-{}",
            binding.as_str(),
            effect.as_str()
        )),
        (Some(_), None) => Err(CdfError::internal(
            "routed segment identity omitted its package ordinal",
        )),
        (None, Some(_)) => Err(CdfError::internal(
            "ordinary segment identity carried a routed package ordinal",
        )),
        (None, None) => Ok(effect),
    }
}

pub(crate) fn canonical_construction_reservation_bytes(
    canonical_output_allocation_bytes: u64,
    row_count: u64,
    unaccounted_input_bytes: u64,
) -> Result<u64> {
    let ordinal_bytes = row_count
        .checked_mul(8)
        .ok_or_else(|| CdfError::data("canonical ordinal buffer size overflow"))?;
    canonical_output_allocation_bytes
        .max(1)
        .checked_add(unaccounted_input_bytes)
        .and_then(|bytes| bytes.checked_add(ordinal_bytes))
        .ok_or_else(|| CdfError::data("canonical concat and ordinal working set overflow"))
}

pub(crate) fn statistics_computation_reservation_bytes(batches: &[RecordBatch]) -> Result<u64> {
    batches.iter().try_fold(1_u64, |maximum, batch| {
        Ok(
            maximum.max(cdf_kernel::BatchStats::computation_reservation_bytes(
                batch,
            )?),
        )
    })
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
            routing: _,
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
    residuals: &[BoundScalarExpression],
    track_source_rows: bool,
    memory: Option<&MemoryLease>,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<ExecutedBatch> {
    let tracked = if track_source_rows {
        if batch.schema().index_of(SOURCE_ROW_TRACKING_FIELD).is_ok() {
            return Err(CdfError::contract(format!(
                "input field {SOURCE_ROW_TRACKING_FIELD:?} conflicts with reserved execution metadata"
            )));
        }
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            SOURCE_ROW_TRACKING_FIELD,
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
    let filtered = apply_bound_filters(&tracked, residuals, memory, cancellation)?;
    Ok(ExecutedBatch {
        batch: filtered,
        source_rows: None,
        limit_truncated: false,
    })
}

fn remap_relational_source_rows(
    downstream: Option<Vec<usize>>,
    relational: Option<&[usize]>,
) -> Result<Option<Vec<usize>>> {
    let Some(downstream) = downstream else {
        return Ok(None);
    };
    let Some(relational) = relational else {
        return Ok(Some(downstream));
    };
    downstream
        .into_iter()
        .map(|index| {
            relational.get(index).copied().ok_or_else(|| {
                CdfError::internal(
                    "downstream source-row tracking exceeded the relational filter mapping",
                )
            })
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn apply_pre_contract_expressions(
    batch: RecordBatch,
    transforms: &[BoundExpressionTransform],
    remaining_limit: &mut Option<u64>,
    track_source_rows: bool,
    memory: Option<&MemoryLease>,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<ExecutedBatch> {
    let transformed = apply_bound_expression_transforms(batch, transforms, memory, cancellation)?;
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
    let ordinal_index = transformed.schema().index_of(SOURCE_ROW_TRACKING_FIELD)?;
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
    if schema.index_of(SOURCE_ROW_TRACKING_FIELD).is_ok() {
        return Err(CdfError::contract(format!(
            "input field {SOURCE_ROW_TRACKING_FIELD:?} conflicts with reserved execution metadata"
        )));
    }
    let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        SOURCE_ROW_TRACKING_FIELD,
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
    expression_bytes: u64,
) -> Result<Option<MemoryLease>> {
    let Some(memory) = memory else {
        return Ok(None);
    };
    let request = transform_working_set_request(batch, residual_candidates, expression_bytes)?;
    Ok(Some(reserve(Arc::clone(memory), request).await?))
}

fn transform_working_set_request(
    batch: &RecordBatch,
    residual_candidates: &[PreContractResidualCandidate],
    expression_bytes: u64,
) -> Result<ReservationRequest> {
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
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(residual_bytes))
        .and_then(|bytes| bytes.checked_add(expression_bytes))
        .ok_or_else(|| CdfError::data("transform working set overflow"))?;
    Ok(ReservationRequest::new(
        ConsumerKey::new("fused-transform", MemoryClass::Transform)?,
        bytes,
    )?
    .as_minimum_working_set())
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
    let accepted_source_rows = residual.typed_source_rows.as_ref().map(|source_rows| {
        evaluation
            .accepted_rows
            .iter()
            .zip(source_rows)
            .filter_map(|(accepted, source_row)| accepted.unwrap_or(false).then_some(*source_row))
            .collect::<Vec<_>>()
    });
    let accepted = if summary.accepted_rows == summary.input_rows {
        residual.typed_batch
    } else {
        filter_record_batch(&residual.typed_batch, &evaluation.accepted_rows)
            .map_err(CdfError::from)?
    };
    let variants = filter_optional_strings(&residual.variant_values, &evaluation.accepted_rows);
    let accepted_with_residual_rows =
        variants.iter().filter(|value| value.is_some()).count() as u64;
    let mut combined = summary;
    combined.input_rows = residual.input_rows;
    combined.accepted_with_residual_rows = accepted_with_residual_rows;
    combined.quarantined_rows += residual.quarantined_rows;
    combined.violation_count += residual.violation_count;
    combined.quarantine_candidate_count += residual.quarantine_candidate_count;
    combined.rule_summaries.extend(residual.rule_summaries);
    Ok(ContractExecOutput {
        accepted,
        accepted_source_rows,
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
    let batch = evaluator.restore_compiled_nullability(batch)?;
    let evaluation =
        evaluator.evaluate_with_quarantine_sink(context.evaluation, &batch, |candidate| {
            quarantine_sink(quarantine_record_from_candidate(
                candidate,
                context.source_rows,
            )?)
        })?;
    let summary = evaluation.summary;
    let accepted_source_rows = context.source_rows.map(|source_rows| {
        evaluation
            .accepted_rows
            .iter()
            .zip(source_rows)
            .filter_map(|(accepted, source_row)| accepted.unwrap_or(false).then_some(*source_row))
            .collect::<Vec<_>>()
    });
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
        accepted_source_rows,
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

struct ResidualQuarantinePreflight {
    quarantined_batch_rows: BTreeSet<usize>,
    remaining_candidates: Vec<PreContractResidualCandidate>,
    quarantine_records: Vec<QuarantineRecord>,
    summary: VerdictSummary,
    residual_decisions: Vec<ResidualDecisionArtifact>,
}

fn preflight_residual_quarantines(
    program: &ValidationProgram,
    candidates: Vec<PreContractResidualCandidate>,
    context: &ResidualBatchContext<'_>,
) -> Result<ResidualQuarantinePreflight> {
    let mut grouped = BTreeMap::<usize, Vec<PreContractResidualCandidate>>::new();
    for candidate in candidates {
        grouped
            .entry(candidate.batch_row_ordinal())
            .or_default()
            .push(candidate);
    }
    let dynamic_controls = residual_dynamic_controls(context);
    let mut output = ResidualQuarantinePreflight {
        quarantined_batch_rows: BTreeSet::new(),
        remaining_candidates: Vec::new(),
        quarantine_records: Vec::new(),
        summary: VerdictSummary::default(),
        residual_decisions: Vec::new(),
    };
    for (batch_row, row_candidates) in grouped {
        let Some((rule_id, error_code)) =
            residual_quarantine_reason(program, &row_candidates, &dynamic_controls)?
        else {
            output.remaining_candidates.extend(row_candidates);
            continue;
        };
        output.quarantined_batch_rows.insert(batch_row);
        output.summary.input_rows += 1;
        output.summary.quarantined_rows += 1;
        let summary = output
            .summary
            .rule_summaries
            .iter_mut()
            .find(|summary| summary.rule_id == rule_id && summary.error_code == error_code);
        if let Some(summary) = summary {
            summary.checked_rows += 1;
            summary.violation_count += 1;
        } else {
            output
                .summary
                .rule_summaries
                .push(cdf_contract::RuleVerdictSummary {
                    rule_id: rule_id.clone(),
                    error_code: error_code.clone(),
                    checked_rows: 1,
                    violation_count: 1,
                });
        }
        for candidate in row_candidates {
            let redaction = residual_redaction(program, &candidate)?;
            output.quarantine_records.push(QuarantineRecord {
                source_row_ordinal: candidate.source_row_ordinal(),
                rule_id: rule_id.clone(),
                error_code: error_code.clone(),
                source_position: context.evaluation.source_position.clone(),
                observed_value_redacted: residual_observed_value(&candidate, &redaction),
            });
            output.summary.violation_count += 1;
            output.summary.quarantine_candidate_count += 1;
            output.residual_decisions.push(residual_decision_artifact(
                program,
                &candidate,
                context.batch_id,
                context.observation_id,
                ResidualRuntimeVerdict::Quarantined,
                &rule_id,
                redaction,
            )?);
        }
    }
    Ok(output)
}

fn remove_preflight_quarantined_rows(
    batch: RecordBatch,
    source_rows: Option<Vec<usize>>,
    quarantined: &BTreeSet<usize>,
) -> Result<(RecordBatch, Option<Vec<usize>>)> {
    if quarantined.is_empty() {
        return Ok((batch, source_rows));
    }
    let source_rows = source_rows.ok_or_else(|| {
        CdfError::internal("preflight residual quarantine omitted source-row tracking")
    })?;
    let accepted = source_rows
        .iter()
        .map(|source| !quarantined.contains(source))
        .collect::<Vec<_>>();
    let accepted_mask = BooleanArray::from(accepted.clone());
    let batch = filter_record_batch(&batch, &accepted_mask).map_err(CdfError::from)?;
    let source_rows = accepted
        .into_iter()
        .zip(source_rows)
        .filter_map(|(accepted, source)| accepted.then_some(source))
        .collect();
    Ok((batch, Some(source_rows)))
}

fn validate_physical_reconciliations(
    batch: &RecordBatch,
    reconciliations: Vec<PreContractPhysicalReconciliation>,
    batch_id: &cdf_kernel::BatchId,
    observation_id: Option<&str>,
) -> Result<Vec<PhysicalReconciliationArtifact>> {
    reconciliations
        .into_iter()
        .map(|reconciliation| {
            let source = reconciliation.source_path().join(".");
            let expected_field = reconciliation.expected_field();
            if !lossless_physical_projection(
                reconciliation.observed_field().data_type(),
                expected_field.data_type(),
            ) {
                return Err(CdfError::data(format!(
                    "physical reconciliation field {source:?} claims unsupported projection {} to {}",
                    reconciliation.observed_field().data_type(),
                    expected_field.data_type()
                )));
            }
            for (value_index, row) in reconciliation
                .batch_row_ordinals()
                .iter()
                .copied()
                .enumerate()
            {
                if row >= batch.num_rows() {
                    return Err(CdfError::data(format!(
                        "physical reconciliation field {source:?} names batch row {row} outside {} rows",
                        batch.num_rows()
                    )));
                }
                let (materialized_field, materialized) = materialized_leaf_value(
                    batch,
                    reconciliation.source_path(),
                    row,
                )?;
                if materialized_field.data_type() != expected_field.data_type()
                    || materialized_field.name() != expected_field.name()
                    || materialized_field.metadata() != expected_field.metadata()
                {
                    return Err(CdfError::data(format!(
                        "physical reconciliation field {source:?} does not match its materialized typed field"
                    )));
                }
                if !lossless_reconciliation_value_equals(
                    reconciliation.observed_values().as_ref(),
                    value_index,
                    materialized.as_ref(),
                )? {
                    return Err(CdfError::data(format!(
                        "physical reconciliation field {source:?} does not equal its materialized typed value at batch row {row}"
                    )));
                }
            }
            Ok(PhysicalReconciliationArtifact {
                version: 1,
                observation_id: observation_id.map(str::to_owned),
                batch_id: batch_id.clone(),
                source_path: reconciliation.source_path().to_vec(),
                observed_field: field_type_evidence(reconciliation.observed_field())?,
                expected_field: field_type_evidence(expected_field)?,
                row_count: u64::try_from(reconciliation.batch_row_ordinals().len()).map_err(
                    |_| CdfError::data("physical reconciliation row count exceeds u64"),
                )?,
                row_ranges: physical_reconciliation_row_ranges(
                    reconciliation.batch_row_ordinals(),
                )?,
            })
        })
        .collect()
}

fn lossless_physical_projection(observed: &DataType, expected: &DataType) -> bool {
    if observed == expected {
        return true;
    }
    match (observed, expected) {
        (DataType::Int32, DataType::Int64) => true,
        (DataType::List(observed), DataType::List(expected)) => {
            lossless_physical_projection(observed.data_type(), expected.data_type())
        }
        (DataType::Struct(observed), DataType::Struct(expected))
            if observed.len() == expected.len() =>
        {
            observed.iter().zip(expected).all(|(observed, expected)| {
                observed.name() == expected.name()
                    && lossless_physical_projection(observed.data_type(), expected.data_type())
            })
        }
        _ => false,
    }
}

fn materialized_leaf_value(
    batch: &RecordBatch,
    source_path: &[String],
    batch_row: usize,
) -> Result<(Field, ArrayRef)> {
    let schema = batch.schema();
    let (field_index, consumed) = (1..=source_path.len())
        .rev()
        .find_map(|consumed| {
            let source = source_path[..consumed].join(".");
            schema
                .fields()
                .iter()
                .position(|field| {
                    source_name(field.as_ref()).unwrap_or_else(|| field.name()) == source
                })
                .map(|index| (index, consumed))
        })
        .ok_or_else(|| {
            CdfError::data(format!(
                "physical reconciliation field {:?} is absent from its materialized batch",
                source_path.join(".")
            ))
        })?;
    let mut field = schema.field(field_index).clone();
    let mut values = Arc::clone(batch.column(field_index));
    let mut value_index = batch_row;
    for segment in &source_path[consumed..] {
        match (field.data_type(), values.as_ref()) {
            (DataType::Struct(fields), array) => {
                let struct_values =
                    array
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            CdfError::data(
                                "materialized reconciliation struct has incompatible storage",
                            )
                        })?;
                let child_index = fields
                    .iter()
                    .position(|child| source_name(child).unwrap_or_else(|| child.name()) == segment)
                    .ok_or_else(|| {
                        CdfError::data(format!(
                            "physical reconciliation nested field {segment:?} is absent"
                        ))
                    })?;
                if struct_values.is_null(value_index) {
                    return Err(CdfError::data(
                        "physical reconciliation names a null materialized struct",
                    ));
                }
                field = fields[child_index].as_ref().clone();
                values = Arc::clone(struct_values.column(child_index));
            }
            (DataType::List(child), array) => {
                let list_values = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    CdfError::data("materialized reconciliation list has incompatible storage")
                })?;
                if list_values.is_null(value_index) {
                    return Err(CdfError::data(
                        "physical reconciliation names a null materialized list",
                    ));
                }
                let element = segment.parse::<usize>().map_err(|_| {
                    CdfError::data(format!(
                        "physical reconciliation list path segment {segment:?} is not an index"
                    ))
                })?;
                let offsets = list_values.value_offsets();
                let start = usize::try_from(offsets[value_index])
                    .map_err(|_| CdfError::data("materialized list offset is negative"))?;
                let end = usize::try_from(offsets[value_index + 1])
                    .map_err(|_| CdfError::data("materialized list offset is negative"))?;
                value_index = start
                    .checked_add(element)
                    .ok_or_else(|| CdfError::data("physical reconciliation list index overflow"))?;
                if value_index >= end {
                    return Err(CdfError::data(
                        "physical reconciliation list index is outside the materialized value",
                    ));
                }
                field = child.as_ref().clone();
                values = Arc::clone(list_values.values());
            }
            _ => {
                return Err(CdfError::data(format!(
                    "physical reconciliation path {:?} traverses a scalar materialized field",
                    source_path.join(".")
                )));
            }
        }
    }
    Ok((field, values.slice(value_index, 1)))
}

fn lossless_reconciliation_value_equals(
    observed: &dyn Array,
    observed_index: usize,
    materialized: &dyn Array,
) -> Result<bool> {
    match (observed.data_type(), materialized.data_type()) {
        (DataType::Int32, DataType::Int64) => {
            let observed = observed
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    CdfError::data("physical Int32 reconciliation has incompatible storage")
                })?;
            let materialized = materialized
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    CdfError::data("materialized Int64 reconciliation has incompatible storage")
                })?;
            Ok(!observed.is_null(observed_index)
                && !materialized.is_null(0)
                && i64::from(observed.value(observed_index)) == materialized.value(0))
        }
        (observed_type, materialized_type) if observed_type == materialized_type => {
            Ok(observed.slice(observed_index, 1).to_data() == materialized.slice(0, 1).to_data())
        }
        _ => Ok(false),
    }
}

fn physical_reconciliation_row_ranges(
    rows: &[usize],
) -> Result<Vec<PhysicalReconciliationRowRange>> {
    let mut ranges = Vec::<PhysicalReconciliationRowRange>::new();
    for row in rows {
        let row = u64::try_from(*row)
            .map_err(|_| CdfError::data("physical reconciliation row ordinal exceeds u64"))?;
        if let Some(last) = ranges.last_mut()
            && last.end_exclusive == row
        {
            last.end_exclusive = row
                .checked_add(1)
                .ok_or_else(|| CdfError::data("physical reconciliation row range overflow"))?;
        } else {
            ranges.push(PhysicalReconciliationRowRange {
                start: row,
                end_exclusive: row
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("physical reconciliation row range overflow"))?,
            });
        }
    }
    Ok(ranges)
}

fn field_type_evidence(field: &Field) -> Result<FieldTypeEvidenceArtifact> {
    Ok(FieldTypeEvidenceArtifact {
        arrow_type: cdf_contract::CanonicalArrowType::from_arrow(field.data_type())?,
        nullable: field.is_nullable(),
        semantic: semantic(field).map(str::to_owned),
        metadata: field.metadata().clone().into_iter().collect(),
    })
}

fn residual_dynamic_controls(context: &ResidualBatchContext<'_>) -> BTreeSet<String> {
    let mut controls = BTreeSet::new();
    if let Some(field) = context.cdc_operation_field {
        controls.insert(field.to_owned());
    }
    collect_source_position_control_fields(
        context.evaluation.source_position.as_ref(),
        &mut controls,
    );
    controls
}

fn residual_quarantine_reason(
    program: &ValidationProgram,
    candidates: &[PreContractResidualCandidate],
    dynamic_controls: &BTreeSet<String>,
) -> Result<Option<(String, String)>> {
    for candidate in candidates {
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
        let disposition = if dynamic_controls.contains(field)
            || field_program.is_some_and(|field| {
                field.roles.iter().any(|role| {
                    matches!(
                        role,
                        FieldRole::DestinationIdentity
                            | FieldRole::SourceProgress
                            | FieldRole::CdcOperation
                            | FieldRole::TransactionBoundary
                    )
                })
            }) {
            FieldDisposition::FailRun
        } else {
            field_program.map_or(program.admission.field, |field| field.disposition)
        };
        match disposition {
            FieldDisposition::CaptureVariant => {}
            FieldDisposition::QuarantineRow => {
                return Ok(Some((
                    format!("residual:{}:contract", residual_path(candidate)),
                    "cdf.residual_quarantine_row".to_owned(),
                )));
            }
            FieldDisposition::FailRun => {
                return Err(CdfError::data(format!(
                    "field {:?} violates the active schema and its compiled disposition is fail_run",
                    residual_path(candidate)
                )));
            }
        }
    }
    Ok(None)
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

    let dynamic_controls = residual_dynamic_controls(context);

    for (row, row_candidates) in grouped {
        let redactions = row_candidates
            .iter()
            .map(|candidate| residual_redaction(program, candidate))
            .collect::<Result<Vec<_>>>()?;
        let mut quarantine_reason =
            residual_quarantine_reason(program, &row_candidates, &dynamic_controls)?;

        let encoded = if quarantine_reason.is_none() {
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
                    if program.admission.record == RecordViolationDisposition::FailRun {
                        return Err(CdfError::data(format!(
                            "residual value at {:?} cannot be encoded and the compiled malformed-record disposition is fail_run: {error}",
                            residual_path(&row_candidates[0])
                        )));
                    }
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
            for (candidate, redaction) in row_candidates.iter().zip(&redactions) {
                quarantine_sink(QuarantineRecord {
                    source_row_ordinal: candidate.source_row_ordinal(),
                    rule_id: rule_id.clone(),
                    error_code: error_code.clone(),
                    source_position: context.evaluation.source_position.clone(),
                    observed_value_redacted: residual_observed_value(candidate, redaction),
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
                    redaction.clone(),
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
            for (candidate, redaction) in row_candidates.iter().zip(redactions) {
                residual_decisions.push(residual_decision_artifact(
                    program,
                    candidate,
                    context.batch_id,
                    context.observation_id,
                    ResidualRuntimeVerdict::Captured,
                    "cdf.residual_capture",
                    redaction,
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
    let non_null_fields = program
        .row_rules
        .iter()
        .filter(|rule| rule.expression_function() == Some("is_not_null"))
        .flat_map(|rule| rule.expression.column_dependencies())
        .collect::<BTreeSet<_>>();
    let dispositions = residual
        .fields
        .iter()
        .flat_map(|field| {
            [
                (field.source_name.as_str(), field.disposition),
                (field.output_name.as_str(), field.disposition),
            ]
        })
        .collect::<std::collections::HashMap<_, _>>();
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let source = source_name(field.as_ref()).unwrap_or_else(|| field.name());
            dispositions
                .get(source)
                .or_else(|| dispositions.get(field.name().as_str()))
                .map_or_else(
                    || field.as_ref().clone(),
                    |disposition| {
                        let nullable = if non_null_fields.contains(source)
                            || non_null_fields.contains(field.name())
                        {
                            false
                        } else if *disposition == FieldDisposition::CaptureVariant {
                            true
                        } else {
                            field.is_nullable()
                        };
                        field.as_ref().clone().with_nullable(nullable)
                    },
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
    redaction: RedactionDecision,
) -> Result<ResidualDecisionArtifact> {
    Ok(ResidualDecisionArtifact {
        version: 1,
        observation_id: observation_id.map(str::to_owned),
        batch_id: batch_id.clone(),
        source_row_ordinal: candidate.source_row_ordinal(),
        source_path: candidate.source_path().to_vec(),
        observed_field: field_type_evidence(candidate.observed_field())?,
        expected_field: candidate
            .expected_field()
            .map(field_type_evidence)
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
        redaction,
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
    if let Ok(index) = batch.schema().index_of(&capture.variant_column) {
        let field = batch.schema().field(index).clone();
        if !cdf_contract::is_framework_variant_field(&field) {
            return Err(CdfError::contract(format!(
                "residual variant column {:?} conflicts with typed output",
                capture.variant_column
            )));
        }
        let mut columns = batch.columns().to_vec();
        columns[index] = Arc::new(StringArray::from(values)) as ArrayRef;
        return RecordBatch::try_new(batch.schema(), columns).map_err(CdfError::from);
    }
    let field = cdf_semantic::builtin_catalog()?.apply_reference(
        Field::new(&capture.variant_column, DataType::Utf8, true),
        &capture.semantic,
        cdf_semantic::SemanticAuthority::Compiled,
    )?;
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
    candidate: &PreContractResidualCandidate,
    decision: &RedactionDecision,
) -> QuarantineObservedValue {
    if candidate.value().is_null(candidate.value_index()) {
        return QuarantineObservedValue::Null;
    }
    let encoded = ResidualFieldRef::new(
        candidate.source_path().iter().map(String::as_str),
        candidate.value(),
        candidate.value_index(),
    )
    .and_then(|field| encode_residual_json_v1([field]))
    .ok();
    match decision {
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
) -> Result<RedactionDecision> {
    let default_policy = cdf_contract::PiiRedactionPolicy::default();
    let policy = program
        .residual
        .as_ref()
        .map_or(&default_policy, |residual| &residual.pii_redaction);
    let observed = cdf_contract::redaction_decision_for_field(
        candidate.observed_field(),
        policy,
        cdf_semantic::SemanticAuthority::Observed,
    )?;
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
        return Ok(decision);
    }
    Ok(observed)
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

/// Assembles one coordinator-owned package exclusively from admitted isolated-worker artifacts.
///
/// Partition evidence and canonical segment results have already crossed their independent
/// admission boundaries. This function performs the canonical merge and package finalization;
/// worker-local package directories and in-memory execution outputs are deliberately absent.
pub fn assemble_isolated_worker_package(
    plan: &EnginePlan,
    package_dir: impl AsRef<Path>,
    partition_evidence: Vec<AdmittedEnginePartitionEvidence>,
    canonical_segments: &[cdf_runtime::AdmittedSegmentWorkerResult],
    artifacts: &dyn EngineWorkerArtifactAuthority,
    resources: &cdf_runtime::WorkerResourceBudget,
    services: &cdf_runtime::ExecutionServices,
) -> Result<EngineRunOutputWithSegmentPositions> {
    if matches!(
        plan.write_disposition,
        WriteDisposition::Merge | WriteDisposition::CdcApply
    ) {
        return Err(CdfError::contract(
            "isolated worker assembly does not yet admit keyed-change packages because winner reduction must be coordinator-global",
        ));
    }
    let mut preparation_results = BTreeMap::new();
    for admitted in &partition_evidence {
        admitted.validate_plan(plan)?;
        let evidence = admitted.evidence();
        if preparation_results
            .insert(
                (
                    evidence.canonical_partition_ordinal,
                    evidence.partition_id.clone(),
                ),
                admitted.preparation_result_sha256().to_owned(),
            )
            .is_some()
        {
            return Err(CdfError::contract(
                "isolated package assembly contains duplicate admitted partition authority",
            ));
        }
    }
    let segmentation_policy_hash = cdf_runtime::artifact_hash(plan.segmentation_policy()?)?;
    let mut partition_evidence = partition_evidence
        .into_iter()
        .map(AdmittedEnginePartitionEvidence::into_evidence)
        .collect::<Vec<_>>();
    if partition_evidence.is_empty() {
        return Err(CdfError::contract(
            "isolated package assembly requires partition evidence",
        ));
    }
    resources.validate()?;
    partition_evidence.sort_by_key(|evidence| evidence.canonical_partition_ordinal);
    if partition_evidence
        .iter()
        .enumerate()
        .any(|(ordinal, evidence)| {
            evidence.canonical_partition_ordinal != u64::try_from(ordinal).unwrap_or(u64::MAX)
                || plan
                    .scan
                    .inline_partitions()
                    .and_then(|partitions| partitions.get(ordinal))
                    .is_none_or(|partition| partition.partition_id != evidence.partition_id)
        })
    {
        return Err(CdfError::contract(
            "isolated partition evidence is not the plan's complete canonical partition prefix",
        ));
    }

    let mut profile = ExecutionProfile::default();
    let mut lineage = LineageSummary::default();
    let mut verdict_summary = VerdictSummary::default();
    let mut segment_positions = Vec::new();
    let mut processed_observations = Vec::new();
    let mut source_retries = Vec::new();
    let mut stream_catalog = BTreeMap::new();
    let mut stream_observations = Vec::new();
    let mut phase_metrics = Vec::new();
    let mut source_frontier = cdf_runtime::SourceFrontierReport::default();
    let checkpoint_eligible = partition_evidence
        .iter()
        .all(|evidence| evidence.checkpoint_eligible);

    for evidence in &partition_evidence {
        profile.output_rows = profile
            .output_rows
            .checked_add(evidence.profile.output_rows)
            .ok_or_else(|| CdfError::data("isolated profile output rows overflowed u64"))?;
        profile.output_bytes = profile
            .output_bytes
            .checked_add(evidence.profile.output_bytes)
            .ok_or_else(|| CdfError::data("isolated profile output bytes overflowed u64"))?;
        profile.output_batches = profile
            .output_batches
            .checked_add(evidence.profile.output_batches)
            .ok_or_else(|| CdfError::data("isolated profile output batches overflowed u64"))?;
        profile.statistics.merge(&evidence.profile.statistics)?;

        lineage.input_rows = lineage
            .input_rows
            .checked_add(evidence.lineage.input_rows)
            .ok_or_else(|| CdfError::data("isolated lineage input rows overflowed u64"))?;
        lineage
            .input_observations
            .extend(evidence.lineage.input_observations.iter().cloned());
        merge_verdict_summary(&mut verdict_summary, evidence.verdict_summary.clone());
        segment_positions.extend(evidence.segment_positions.iter().cloned());
        processed_observations.extend(evidence.processed_observations.iter().cloned());
        source_retries.extend(evidence.source_retries.iter().cloned());
        for (hash, observation) in &evidence.stream_admission.physical_observation_catalog {
            if let Some(existing) = stream_catalog.insert(hash.clone(), observation.clone())
                && existing != *observation
            {
                return Err(CdfError::contract(
                    "isolated stream-admission catalog contains conflicting physical evidence",
                ));
            }
        }
        stream_observations.extend(evidence.stream_admission.observations.iter().cloned());
        phase_metrics.extend(evidence.phase_metrics.iter().cloned());
        source_frontier.partition_count = source_frontier
            .partition_count
            .checked_add(evidence.source_frontier.partition_count)
            .ok_or_else(|| CdfError::data("isolated source frontier partitions overflowed u64"))?;
        source_frontier.maximum_active = source_frontier
            .maximum_active
            .max(evidence.source_frontier.maximum_active);
        source_frontier.wait_ns = source_frontier
            .wait_ns
            .checked_add(evidence.source_frontier.wait_ns)
            .ok_or_else(|| CdfError::data("isolated source frontier wait overflowed u64"))?;
        source_frontier.prefetched_batches = source_frontier
            .prefetched_batches
            .checked_add(evidence.source_frontier.prefetched_batches)
            .ok_or_else(|| CdfError::data("isolated source frontier prefetch overflowed u64"))?;
        source_frontier.discarded_prefetched_batches = source_frontier
            .discarded_prefetched_batches
            .checked_add(evidence.source_frontier.discarded_prefetched_batches)
            .ok_or_else(|| CdfError::data("isolated discarded prefetch overflowed u64"))?;
        source_frontier.peak_ready_partitions = source_frontier
            .peak_ready_partitions
            .max(evidence.source_frontier.peak_ready_partitions);
    }

    lineage
        .input_observations
        .sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
    if lineage
        .input_observations
        .windows(2)
        .any(|pair| pair[0].observation_id == pair[1].observation_id)
    {
        return Err(CdfError::contract(
            "isolated partition evidence assigns one observation to multiple partitions",
        ));
    }
    stream_observations.sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
    let stream_admission = CompiledStreamAdmissionEvidence::new(
        &plan.compiled_schema_admission,
        stream_catalog,
        stream_observations,
    )?;

    let terminal_schema_quarantines = partition_evidence
        .iter()
        .flat_map(|evidence| evidence.terminal_schema_quarantines.iter().cloned())
        .collect::<Vec<_>>();
    let mut schema_quarantine_catalog = BTreeMap::new();
    let mut schema_quarantine_observations = BTreeMap::new();
    for evidence in partition_evidence
        .iter()
        .filter_map(|evidence| evidence.schema_quarantine_evidence.as_ref())
    {
        evidence.validate_admission(&plan.compiled_schema_admission)?;
        for (hash, physical) in &evidence.physical_observation_catalog {
            if let Some(existing) = schema_quarantine_catalog.insert(hash.clone(), physical.clone())
                && existing != *physical
            {
                return Err(CdfError::contract(
                    "isolated schema-quarantine catalog contains conflicting physical evidence",
                ));
            }
        }
        for observation in &evidence.observations {
            if schema_quarantine_observations
                .insert(observation.observation_id.clone(), observation.clone())
                .is_some()
            {
                return Err(CdfError::contract(
                    "isolated schema-quarantine evidence contains a duplicate observation",
                ));
            }
        }
    }
    if terminal_schema_quarantines.len() != schema_quarantine_observations.len() {
        return Err(CdfError::contract(
            "isolated schema quarantines do not exactly match their compiled evidence",
        ));
    }
    for quarantine in &terminal_schema_quarantines {
        schema_quarantine_observations
            .get(quarantine.observation_id())
            .ok_or_else(|| {
                CdfError::contract("isolated schema quarantine omitted its observation evidence")
            })?
            .validate(quarantine)?;
    }
    let schema_quarantine_evidence = (!terminal_schema_quarantines.is_empty())
        .then(|| {
            CompiledSchemaQuarantineEvidence::new(
                &plan.compiled_schema_admission,
                schema_quarantine_catalog,
                schema_quarantine_observations.into_values().collect(),
            )
        })
        .transpose()?;
    if !profile.statistics.columns.is_empty() {
        return Err(CdfError::contract(
            "isolated package assembly requires referenced statistics-profile evidence",
        ));
    }

    let mut canonical = canonical_segments
        .iter()
        .map(|admitted| {
            let preparation_result_sha256 = preparation_results
                .get(&(
                    admitted.canonical_partition_ordinal(),
                    admitted.partition_id().clone(),
                ))
                .ok_or_else(|| {
                    CdfError::contract(
                        "admitted canonical segment has no matching partition preparation",
                    )
                })?;
            admitted.validate_chain(
                &plan.scan.request.resource_id,
                &plan.scan.plan_id,
                preparation_result_sha256,
                &plan.output_schema.arrow_schema_hash,
                &segmentation_policy_hash,
            )?;
            let result = admitted.result();
            let receipt = result.artifact.as_ref().ok_or_else(|| {
                CdfError::contract("admitted canonical segment lacks its artifact receipt")
            })?;
            let cdf_runtime::WorkerArtifactRole::CanonicalSegment {
                segment_id,
                partition_ordinal,
                segment_ordinal,
                row_count,
            } = &receipt.role
            else {
                return Err(CdfError::contract(
                    "admitted segment result does not carry a canonical segment role",
                ));
            };
            Ok((
                *partition_ordinal,
                *segment_ordinal,
                segment_id.clone(),
                *row_count,
                receipt.artifact.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    canonical.sort_by_key(|(partition, segment, ..)| (*partition, *segment));
    let actual_segment_ids = canonical
        .iter()
        .map(|(_, _, segment_id, ..)| segment_id.clone())
        .collect::<Vec<_>>();
    if segment_positions
        .iter()
        .map(|position| position.segment_id.clone())
        .collect::<Vec<_>>()
        != actual_segment_ids
    {
        return Err(CdfError::contract(
            "isolated canonical segment results do not match partition evidence",
        ));
    }

    let builder = PackageBuilder::create(
        package_dir,
        plan.package_id.clone(),
        cdf_kernel::PackageContentAuthority::rows(plan.output_schema.arrow_schema_hash.clone()),
        cdf_package::PackageBuilderResources::shared(services.memory(), services.spill())?,
    )?;
    let mut package_row_ord_start = 0_u64;
    for (_, _, segment_id, row_count, reference) in canonical {
        let bytes = artifacts
            .read_canonical_segment(&reference, resources.disk_bytes, resources.memory_bytes)?
            .into_bytes();
        builder.import_canonical_segment(
            cdf_kernel::PackageSegmentKind::Row,
            segment_id,
            package_row_ord_start,
            row_count,
            bytes.payload(),
        )?;
        package_row_ord_start = package_row_ord_start
            .checked_add(row_count)
            .ok_or_else(|| CdfError::data("isolated package row ordinal overflowed u64"))?;
    }

    builder.update_status(PackageStatus::Extracting)?;
    builder.write_json_artifact(cdf_package_contract::SCAN_PLAN_FILE, &plan.scan)?;
    builder.write_json_artifact("plan/explain.json", &plan.explain)?;
    if let Some(graph) = &plan.operator_graph {
        graph.validate_plan_join(&plan.execution_extent, plan.compiled_stream_policy.as_ref())?;
        builder.write_json_artifact("plan/operator-graph.json", graph)?;
    }
    builder.write_json_artifact("plan/validation-program.json", &plan.validation_program)?;
    builder.write_json_artifact(
        cdf_package_contract::SCHEMA_ADMISSION_PROGRAM_FILE,
        &plan.schema_admission_program,
    )?;
    builder.write_json_artifact(
        "plan/schema-admission.json",
        &plan.compiled_schema_admission,
    )?;
    if let Some(evidence) = &plan.effective_schema_evidence {
        builder.write_json_artifact("schema/effective-schema-evidence.json", evidence)?;
    }
    if plan.validation_program.requires_observed_at_ms() {
        return Err(CdfError::contract(
            "isolated package assembly requires recorded contract-evaluation context evidence",
        ));
    }
    if let Some(coercion) = &plan.validation_program.schema_coercion {
        builder.write_json_artifact("schema/coercion-plan.json", coercion)?;
    }
    builder.write_json_artifact("schema/stream-admission-evidence.json", &stream_admission)?;
    if let Some(evidence) = &schema_quarantine_evidence {
        builder.write_json_artifact(
            "quarantine/schema-observations.json",
            &terminal_schema_quarantines,
        )?;
        builder.write_json_artifact("quarantine/schema-admission-evidence.json", evidence)?;
    }
    let runtime_output_schema = plan.output_arrow_schema()?;
    builder.write_json_artifact(
        "schema/output.json",
        &schema_artifact(&runtime_output_schema),
    )?;
    builder.write_runtime_arrow_schema(&runtime_output_schema)?;
    if let Some(admission) = schema_admission_artifact_metadata(
        &plan.validation_program,
        plan.schema_authority.baseline_schema_hash.clone(),
        plan.schema_authority.effective_schema_hash.clone(),
        false,
    ) {
        write_schema_admission_stream(&builder, &admission, None)?;
    }
    builder.write_lineage_artifact(
        "lineage.json",
        &cdf_package::canonical_json_bytes(&lineage)?,
    )?;

    let drain_epoch = match &plan.execution_extent {
        ExecutionExtent::Bounded { .. } => {
            if partition_evidence
                .iter()
                .any(|evidence| evidence.drain.is_some())
            {
                return Err(CdfError::contract(
                    "bounded isolated package evidence cannot contain a drain epoch",
                ));
            }
            None
        }
        ExecutionExtent::Drain { .. } => {
            if partition_evidence.len() != 1 {
                return Err(CdfError::contract(
                    "multi-partition drain requires the canonical epoch-task topology",
                ));
            }
            let drain = partition_evidence[0].drain.clone().ok_or_else(|| {
                CdfError::contract("drain isolated package evidence lacks epoch closure")
            })?;
            builder.write_json_artifact("plan/epoch-frontier.json", &drain.frontier)?;
            builder.write_json_artifact("plan/epoch-closure.json", &drain.closure)?;
            builder.write_json_artifact(
                cdf_package_contract::PARTITION_WATERMARK_STATE_FILE,
                &cdf_package_contract::PartitionWatermarkStateArtifact::new(
                    drain.partition_watermarks.clone(),
                )?,
            )?;
            Some(EngineDrainEpoch {
                closure: cdf_runtime::DrainEpochClosure {
                    frontier: drain.frontier,
                    evidence: drain.closure,
                    observed_at_unix_milliseconds: drain.observed_at_unix_milliseconds,
                    terminate_after_settlement: drain.terminate_after_settlement,
                },
                consumed_partition_count: drain.consumed_partition_count,
                resume_partition: drain.resume_partition.map(Box::new),
                consumed_late_data_carryover: drain.consumed_late_data_carryover,
                late_data_carryover: drain.late_data_carryover,
                partition_watermarks: drain.partition_watermarks,
            })
        }
        ExecutionExtent::Resident { .. } => {
            return Err(CdfError::contract(
                "isolated resident package assembly is not enabled",
            ));
        }
    };

    let execution_evidence = EngineExecutionEvidence::new(
        processed_observations,
        source_retries,
        plan.partition_schedule.as_ref(),
        checkpoint_eligible,
    )?;
    builder.update_status(PackageStatus::Validated)?;
    let (manifest, verification) = builder.finish_verified()?;
    Ok(EngineRunOutputWithSegmentPositions {
        output: EngineRunOutput {
            manifest,
            verification,
            profile,
            lineage,
            admission: plan.validation_program.admission.clone(),
            verdict_summary,
            terminal_schema_quarantines,
        },
        segment_positions,
        phase_metrics,
        source_frontier,
        source_transfer: cdf_kernel::SourceTransferReport::default(),
        drain_epoch,
        execution_evidence,
    })
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

fn validate_effective_batch_schema(observed: &Schema, effective: &Schema) -> Result<()> {
    validate_effective_batch_schema_with_nullable_sources(observed, effective, &BTreeSet::new())
}

fn validate_materialized_effective_batch_schema(
    observed: &RecordBatch,
    effective: &Schema,
    residual_candidates: &[PreContractResidualCandidate],
) -> Result<()> {
    let nullable_sources = effective
        .fields()
        .iter()
        .map(|field| {
            source_name(field.as_ref())
                .unwrap_or_else(|| field.name())
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    validate_effective_batch_schema_with_nullable_sources(
        observed.schema().as_ref(),
        effective,
        &nullable_sources,
    )?;

    for (index, field) in effective.fields().iter().enumerate() {
        if field.is_nullable() || observed.column(index).null_count() == 0 {
            continue;
        }
        let source = source_name(field.as_ref()).unwrap_or_else(|| field.name());
        let covered_rows = residual_candidates
            .iter()
            .filter(|candidate| candidate.source_path().first().map(String::as_str) == Some(source))
            .map(PreContractResidualCandidate::batch_row_ordinal)
            .collect::<BTreeSet<_>>();
        for row in 0..observed.num_rows() {
            if observed.column(index).is_null(row) && !covered_rows.contains(&row) {
                return Err(CdfError::data(format!(
                    "materialized non-null field {source:?} contains an unaccounted null at batch row {row}"
                )));
            }
        }
    }
    Ok(())
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
        .map_err(|error| {
            CdfError::environment(format!(
                "system clock before Unix epoch: {error}; correct the host clock before retrying"
            ))
        })?;
    i64::try_from(duration.as_millis()).map_err(|_| {
        CdfError::internal("system time milliseconds do not fit in i64 evaluation context")
    })
}

fn current_observed_at_u64_ms() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CdfError::environment(format!(
                "system clock before Unix epoch: {error}; correct the host clock before retrying"
            ))
        })?;
    u64::try_from(duration.as_millis())
        .map_err(|_| CdfError::internal("system time milliseconds do not fit in u64"))
}

#[cfg(test)]
mod transform_kernel_tests {
    use std::{collections::BTreeMap, hint::black_box, sync::Arc, time::Instant};

    use arrow_array::{BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_contract::{
        ContractEvaluationContext, ContractPolicy, DeclarativeExpression, ExpressionUse,
        ObservedSchema, TransformDescription, VectorValidationEvaluator,
        compile_validation_program,
    };
    use cdf_kernel::{
        BatchId, CursorPosition, CursorValue, ErrorKind, PreContractPhysicalReconciliation,
        PreContractResidualCandidate, SOURCE_NAME_METADATA_KEY, SourcePosition, TrustLevel,
        with_physical_type, with_semantic,
    };
    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use cdf_package::PackageBuilder;
    use cdf_package_contract::{QuarantineObservedValue, QuarantineRecord};

    use super::{
        QuarantinePartAccumulator, ResidualBatchContext, TransformKernelMode, apply_contract_exec,
        apply_pre_contract_expressions, bind_filter_expressions,
        canonicalize_admitted_batch_schema, execute_batch, preflight_residual_quarantines,
        reserve_quarantine_evidence, residual_redaction, source_row_tracking_schema,
        validate_materialized_effective_batch_schema, validate_physical_reconciliations,
    };

    #[test]
    fn admitted_physical_batch_is_rebound_to_effective_logical_metadata() {
        let observed = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["hello"]))],
        )
        .unwrap();
        let effective = Schema::new(vec![
            Field::new("text", DataType::Utf8, true).with_metadata(
                std::collections::HashMap::from([(
                    SOURCE_NAME_METADATA_KEY.to_owned(),
                    "text".to_owned(),
                )]),
            ),
        ]);

        let rebound = canonicalize_admitted_batch_schema(&observed, &effective).unwrap();

        assert_eq!(rebound.columns(), observed.columns());
        assert_eq!(rebound.schema().as_ref(), &effective);
    }

    #[test]
    fn materialized_nullable_domain_requires_exact_evidence_for_actual_nulls() {
        let effective = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let observed = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();

        let error =
            validate_materialized_effective_batch_schema(&observed, &effective, &[]).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.message.contains("unaccounted null"));

        let candidate = PreContractResidualCandidate::new(
            1,
            1,
            vec!["id".to_owned()],
            Field::new("id", DataType::Utf8, true),
            Some(Field::new("id", DataType::Int64, false)),
            Arc::new(StringArray::from(vec!["wrong"])),
            0,
        )
        .unwrap();
        validate_materialized_effective_batch_schema(&observed, &effective, &[candidate]).unwrap();

        let populated = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)]))],
        )
        .unwrap();
        validate_materialized_effective_batch_schema(&populated, &effective, &[]).unwrap();
    }

    #[test]
    fn physical_reconciliation_requires_lossless_equality_with_materialized_cell() {
        let expected = with_physical_type(Field::new("id", DataType::Int64, false), "bson:int64");
        let materialized = RecordBatch::try_new(
            Arc::new(Schema::new(vec![expected.clone().with_nullable(true)])),
            vec![Arc::new(Int64Array::from(vec![7_i64]))],
        )
        .unwrap();
        let exact = PreContractPhysicalReconciliation::new(
            vec!["id".to_owned()],
            with_physical_type(Field::new("id", DataType::Int32, true), "bson:int32"),
            expected.clone(),
            Arc::new(Int32Array::from(vec![7_i32])),
            vec![0],
        )
        .unwrap();
        let artifacts = validate_physical_reconciliations(
            &materialized,
            vec![exact],
            &BatchId::new("physical-exact").unwrap(),
            Some("observation"),
        )
        .unwrap();
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].row_count, 1);
        assert_eq!(
            artifacts[0]
                .observed_field
                .metadata
                .get(cdf_kernel::PHYSICAL_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some("bson:int32")
        );
        assert_eq!(artifacts[0].row_ranges[0].start, 0);
        assert_eq!(artifacts[0].row_ranges[0].end_exclusive, 1);

        let incompatible = PreContractPhysicalReconciliation::new(
            vec!["id".to_owned()],
            Field::new("id", DataType::Utf8, true),
            expected.clone(),
            Arc::new(StringArray::from(vec!["wrong"])),
            vec![0],
        )
        .unwrap();
        let error = validate_physical_reconciliations(
            &materialized,
            vec![incompatible],
            &BatchId::new("physical-incompatible").unwrap(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.message.contains("unsupported projection"));

        let unequal = PreContractPhysicalReconciliation::new(
            vec!["id".to_owned()],
            Field::new("id", DataType::Int32, true),
            expected,
            Arc::new(Int32Array::from(vec![8_i32])),
            vec![0],
        )
        .unwrap();
        let error = validate_physical_reconciliations(
            &materialized,
            vec![unequal],
            &BatchId::new("physical-unequal").unwrap(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.message.contains("does not equal"));
    }

    #[test]
    fn progress_residual_fails_before_filtering_can_hide_it() {
        let expected_schema = Schema::new(vec![Field::new("cursor", DataType::Int64, false)]);
        let program = compile_validation_program(
            &ContractPolicy::for_trust(TrustLevel::Governed),
            &ObservedSchema::from_arrow(&expected_schema),
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "cursor",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![None, Some(2)]))],
        )
        .unwrap();
        let filter = crate::expression::plan_expression(
            DeclarativeExpression::parse_comparison("cursor > 0").unwrap(),
            ExpressionUse::Filter,
            &expected_schema,
        )
        .unwrap();
        let tracked_schema = source_row_tracking_schema(&expected_schema).unwrap();
        let bound = bind_filter_expressions(&[filter], &tracked_schema).unwrap();
        let candidate = PreContractResidualCandidate::new(
            41,
            0,
            vec!["cursor".to_owned()],
            Field::new("cursor", DataType::Utf8, true),
            Some(Field::new("cursor", DataType::Int64, false)),
            Arc::new(StringArray::from(vec!["wrong"])),
            0,
        )
        .unwrap();
        let evaluation = ContractEvaluationContext::default().with_source_position(Some(
            SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "cursor".to_owned(),
                value: CursorValue::I64(2),
            }),
        ));
        let batch_id = BatchId::new("filtered-control-residual").unwrap();
        let error = match preflight_residual_quarantines(
            &program,
            vec![candidate],
            &ResidualBatchContext {
                evaluation: &evaluation,
                source_rows: None,
                cdc_operation_field: None,
                batch_id: &batch_id,
                observation_id: Some("observation"),
            },
        ) {
            Ok(_) => panic!("source progress residual unexpectedly remained admissible"),
            Err(error) => error,
        };

        assert!(error.message.contains("compiled disposition is fail_run"));
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(bound.len(), 1);
    }

    #[test]
    fn residual_semantic_unknowns_retain_observed_data_ownership() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let program = compile_validation_program(
            &ContractPolicy::for_trust(TrustLevel::Governed),
            &ObservedSchema::from_arrow(&schema),
        )
        .unwrap();
        let unknown = "project.unknown@1".parse().unwrap();
        let candidate = PreContractResidualCandidate::new(
            0,
            0,
            vec!["new_value".to_owned()],
            with_semantic(Field::new("new_value", DataType::Utf8, true), &unknown),
            None,
            Arc::new(StringArray::from(vec!["secret"])),
            0,
        )
        .unwrap();

        let error = residual_redaction(&program, &candidate).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Data);
    }

    #[test]
    fn tracked_source_rows_do_not_shift_sequential_derive_filter_bindings() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let transforms = vec![
            TransformDescription::Derive {
                column: "selected".to_owned(),
                expression: DeclarativeExpression::parse_comparison("id >= 2").unwrap(),
            },
            TransformDescription::Filter {
                expression: DeclarativeExpression::parse_comparison("selected = true").unwrap(),
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
            Field::new("selected", DataType::Boolean, false),
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
        let bound = crate::expression_execution::bind_expression_transforms(
            &transforms,
            &[derive, filter],
            &tracked_schema,
        )
        .unwrap();
        let input =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).unwrap();
        let cancellation = cdf_runtime::RunCancellation::default();
        let tracked = execute_batch(&input, &[], true, None, &cancellation).unwrap();
        let output = apply_pre_contract_expressions(
            tracked.batch,
            &bound,
            &mut None,
            true,
            None,
            &cancellation,
        )
        .unwrap();

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
                .index_of(crate::expression_execution::SOURCE_ROW_TRACKING_FIELD)
                .is_err()
        );
    }

    #[test]
    fn quarantine_evidence_fails_cleanly_before_exceeding_managed_budget() {
        let memory = Arc::new(DeterministicMemoryCoordinator::new(1_024, BTreeMap::new()).unwrap());
        let managed: Arc<dyn MemoryCoordinator> = memory.clone();
        let lease = reserve_quarantine_evidence(Some(&managed)).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let builder = PackageBuilder::create(
            temp.path(),
            "quarantine-budget",
            cdf_kernel::PackageContentAuthority::rows(
                cdf_kernel::SchemaHash::new("quarantine-budget-schema").unwrap(),
            ),
            cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
                .unwrap(),
        )
        .unwrap();
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
        let builder = PackageBuilder::create(
            temp.path(),
            "dense-quarantine-budget",
            cdf_kernel::PackageContentAuthority::rows(
                cdf_kernel::SchemaHash::new("dense-quarantine-budget-schema").unwrap(),
            ),
            cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
                .unwrap(),
        )
        .unwrap();
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
            cdf_package::for_each_quarantine_record_in_parquet_file(path, &mut |record| {
                records.push(record);
                Ok(())
            })
            .unwrap();
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
        let builder = PackageBuilder::create(
            temp.path(),
            "dense-quarantine-rss",
            cdf_kernel::PackageContentAuthority::rows(
                cdf_kernel::SchemaHash::new("dense-quarantine-rss-schema").unwrap(),
            ),
            cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
                .unwrap(),
        )
        .unwrap();
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
        let policy = ContractPolicy::for_trust(TrustLevel::Governed);
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
            let mut evaluator =
                VectorValidationEvaluator::new_bound(&program, batch.schema()).unwrap();
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
