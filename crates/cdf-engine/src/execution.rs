use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
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
    VARIANT_COLUMN_NAME, ValidationProgram, VerdictSummary, encode_package_dedup_keys,
    encode_residual_json_v1, encode_residual_json_v1_redacted, evaluate_package_order_dedup,
    evaluate_record_batch, package_dedup_rule, reject_untrusted_schema_coercion_metadata,
    schema_coercion_plan_from_trusted_json,
};
use cdf_kernel::{
    CdfError, PHYSICAL_TYPE_METADATA_KEY, PLAN_PHYSICAL_SCHEMA_HASH_KEY,
    PLAN_SCHEMA_OBSERVATION_BINDING_KEY, PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionAttestation,
    PreContractObservedValue, PreContractQuarantineFact, PreContractResidualCandidate,
    ProcessedObservationOutcome, ProcessedObservationPosition, ResourceStream, Result, RunId,
    RunPhase, RunPhaseMetric, RunPhaseStatus, SOURCE_NAME_METADATA_KEY, ScopeKey, SourcePosition,
    StratifiedHashBoundedIdentity, StratifiedHashCandidate, StratifiedHashIdentityStrength,
    TerminalSchemaObservationQuarantine, WriteDisposition, semantic, source_name,
};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest};
use cdf_package::{PackageBuilder, PackageStatus, QuarantineObservedValue, QuarantineRecord};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{Instrument, Span, info_span};

use crate::{
    EffectiveSchemaObservationCoercion, EffectiveSchemaPlanEvidence, EngineExecutionEvidence,
    EngineExecutionOptions, EnginePackageDraft, EnginePlan, EnginePreviewLimits,
    EnginePreviewOutput, EngineRunOutput, EngineRunOutputWithSegmentPositions,
    EngineSegmentPosition, ExecutionProfile, LineageSummary,
    output_schema::canonicalize_effective_output_schema,
    planning::validate_program,
    predicates::apply_residual_filters,
    variant_capture::{
        ResidualDecisionArtifact, ResidualRuntimeVerdict, ResidualTypedProjection,
        contract_evolution_artifact, normalize_batch,
    },
};

pub type PackagePreFinalizeHook<'a> =
    dyn Fn(&PackageBuilder, EnginePackageDraft<'_>) -> Result<()> + 'a;

#[derive(Clone, Copy, Debug, Default)]
struct PhaseAggregate {
    duration_ns: u64,
    input_bytes: u64,
    output_bytes: u64,
    operations: u64,
}

struct PhaseMeasurements {
    enabled: bool,
    values: BTreeMap<RunPhase, PhaseAggregate>,
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
        if !self.enabled {
            return;
        }
        let metric = self.values.entry(phase).or_default();
        metric.duration_ns = metric.duration_ns.saturating_add(duration_ns);
        metric.input_bytes = metric.input_bytes.saturating_add(input_bytes);
        metric.output_bytes = metric.output_bytes.saturating_add(output_bytes);
        metric.operations = metric.operations.saturating_add(1);
    }

    fn into_metrics(self) -> Vec<RunPhaseMetric> {
        self.values
            .into_iter()
            .map(|(phase, metric)| RunPhaseMetric {
                phase,
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
    validate_program(&plan.validation_program)?;
    let schema_authority = plan.schema_authority()?;
    if schema_authority.version != 1 {
        return Err(CdfError::data(format!(
            "unsupported engine schema-authority version {}",
            schema_authority.version
        )));
    }
    EnginePreviewLimits::new(limits.max_rows, limits.max_bytes, limits.max_batches)?;
    let effective_schema_evidence = validate_effective_schema_plan(plan, resource)?;
    crate::planning::validate_plan_schema_authority(resource, plan)?;
    let runtime_output_schema = plan.output_arrow_schema()?;
    let evaluation_context = ContractEvaluationContext::observed_at(current_observed_at_ms()?);
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
                    PartitionSchemaDisposition::Quarantined(_) => None,
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
                let mut stream = resource.open(candidate.partition.clone()).await?;
                payload_opened_partition_count += 1;
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
                    let record_batch = batch.record_batch().cloned().ok_or_else(|| {
                        CdfError::data("resource preview requires in-memory Arrow record batches")
                    })?;
                    let decoded_bytes = u64::try_from(record_batch.get_array_memory_size())
                        .map_err(|error| CdfError::internal(error.to_string()))?;
                    if decoded_bytes > remaining_bytes {
                        truncated = true;
                        break;
                    }
                    validate_batch_schema_evidence(
                        &batch,
                        &record_batch,
                        candidate.expected.as_ref(),
                        resource.schema().as_ref(),
                    )?;
                    let pre_contract_quarantined_rows =
                        pre_contract_quarantine_summary(&batch.header.pre_contract_quarantine)
                            .quarantined_rows;
                    let residual_candidates = batch.header.take_residual_candidates();
                    let cdc_operation_field = batch
                        .header
                        .cdc
                        .as_ref()
                        .map(|metadata| metadata.operation_field.clone());
                    let mut no_row_limit = None;
                    let ExecutedBatch {
                        batch: output,
                        source_rows,
                    } = execute_batch(
                        &record_batch,
                        plan,
                        &mut no_row_limit,
                        !residual_candidates.is_empty(),
                    )?;
                    let contract = apply_contract_exec(
                        output,
                        &plan.validation_program,
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
                    )?;
                    let normalized = append_residual_variant(
                        contract.accepted,
                        &plan.validation_program,
                        contract.variant_values,
                    )?;
                    let normalized = normalize_record_batch(normalized, &plan.validation_program)?;
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
    let location = partition
        .metadata
        .get("path")
        .or_else(|| partition.metadata.get(PLAN_SCHEMA_OBSERVATION_ID_KEY))
        .cloned()
        .unwrap_or_else(|| partition.partition_id.to_string());
    let is_file = partition.metadata.get("kind").map(String::as_str) == Some("files");
    let size_bytes = partition
        .metadata
        .get("bytes")
        .map(|value| {
            value.parse::<u64>().map_err(|error| {
                CdfError::data(format!(
                    "preview partition {} has invalid byte-size identity {value:?}: {error}",
                    partition.partition_id
                ))
            })
        })
        .transpose()?;
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
    let (value, strength) = if is_file {
        if let Some(sha256) = partition.metadata.get("sha256") {
            (
                Some(sha256.clone()),
                StratifiedHashIdentityStrength::StrongChecksum,
            )
        } else if let Some(etag) = partition.metadata.get("etag") {
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
        } else {
            (None, StratifiedHashIdentityStrength::Unavailable)
        }
    } else if let Some(binding) = partition.metadata.get(PLAN_SCHEMA_OBSERVATION_BINDING_KEY) {
        (
            Some(binding.clone()),
            StratifiedHashIdentityStrength::BoundedObservation,
        )
    } else {
        let bytes = serde_json::to_vec(&(
            &partition.partition_id,
            &partition.scope,
            &partition.start_position,
            &partition.metadata,
        ))
        .map_err(|error| CdfError::internal(error.to_string()))?;
        (
            Some(format!("sha256:{:x}", Sha256::digest(bytes))),
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
            let attestation = resource.attest_partition(partition).await?.ok_or_else(|| {
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
        None => resource.attest_partition(partition).await?,
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

fn validate_batch_schema_evidence(
    batch: &cdf_kernel::Batch,
    record_batch: &RecordBatch,
    expected: Option<&EffectiveSchemaObservationCoercion>,
    effective_schema: &Schema,
) -> Result<Option<cdf_contract::SchemaCoercionPlan>> {
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
            if batch.header.observed_schema_hash != expected.physical_schema_hash {
                return Err(CdfError::data(format!(
                    "schema observation {:?} produced physical schema hash {} but verified discovery evidence requires {}",
                    expected.observation_id,
                    batch.header.observed_schema_hash,
                    expected.physical_schema_hash
                )));
            }
            validate_effective_batch_schema(record_batch.schema().as_ref(), effective_schema)?;
            if batch_coercion != &expected.coercion_plan {
                return Err(CdfError::data(format!(
                    "schema observation {:?} produced coercion evidence that does not match the typed engine plan",
                    expected.observation_id
                )));
            }
        }
        (Some(_), None) => {
            return Err(CdfError::data(
                "effective-schema execution requires trusted per-observation coercion evidence on every observed batch",
            ));
        }
        (None, _) => {}
    }
    Ok(batch_coercion)
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
    quarantine_records: Vec<QuarantineRecord>,
    summary: VerdictSummary,
    residual_decisions: Vec<ResidualDecisionArtifact>,
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
}

struct PendingDedupBatch {
    partition_ordinal: u32,
    output: RecordBatch,
    output_position: Option<SourcePosition>,
}

struct PreparedOutputBatch {
    output: RecordBatch,
    variant_values: Vec<Option<String>>,
    output_position: Option<SourcePosition>,
}

struct OutputWriteState<'a> {
    profile: &'a mut ExecutionProfile,
    lineage: &'a mut LineageSummary,
    segments: &'a mut Vec<cdf_package::SegmentEntry>,
    segment_positions: &'a mut Vec<EngineSegmentPosition>,
    output_schema: &'a mut Option<SchemaArtifact>,
    expected_schema: &'a Schema,
    phase_measurements: &'a mut PhaseMeasurements,
    memory: Option<&'a Arc<dyn MemoryCoordinator>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct QuarantineSummaryArtifact {
    quarantined_rows: u64,
    quarantine_candidate_count: u64,
    artifact_count: u64,
    artifacts: Vec<String>,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PerObservationSchemaEvidenceArtifact {
    baseline_snapshot_schema_hash: String,
    effective_snapshot_schema_hash: String,
    effective_arrow_schema_hash: String,
    discovery_manifest_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    discovery_coverage: Option<cdf_kernel::DiscoveryCoverageEvidence>,
    observations: Vec<PerObservationSchemaCoercionArtifact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PerObservationSchemaCoercionArtifact {
    observation_id: String,
    physical_schema_hash: String,
    coercion_plan: cdf_contract::SchemaCoercionPlan,
}

enum PartitionSchemaDisposition {
    Admitted(EffectiveSchemaObservationCoercion),
    Quarantined(TerminalSchemaObservationQuarantine),
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
        EngineExecutionOptions::default(),
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
        EngineExecutionOptions::default(),
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
        EngineExecutionOptions::default(),
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
        options,
    )
    .await
}

async fn execute_to_package_inner<R>(
    trace_context: Option<&ExecutionTraceContext>,
    plan: &EnginePlan,
    resource: &R,
    package_dir: impl AsRef<Path>,
    pre_finalize: Option<&PackagePreFinalizeHook<'_>>,
    options: EngineExecutionOptions,
) -> Result<EngineRunOutputWithSegmentPositions>
where
    R: ResourceStream + ?Sized,
{
    let mut validation_program = plan.validation_program.clone();
    validate_program(&validation_program)?;
    let schema_authority = plan.schema_authority()?;
    if schema_authority.version != 1 {
        return Err(CdfError::data(format!(
            "unsupported engine schema-authority version {}",
            schema_authority.version
        )));
    }
    let effective_schema_evidence = validate_effective_schema_plan(plan, resource)?;
    crate::planning::validate_plan_schema_authority(resource, plan)?;
    let runtime_output_schema = plan.output_arrow_schema()?;

    let mut builder = PackageBuilder::create(package_dir, plan.package_id.clone())?;
    builder.update_status(PackageStatus::Extracting)?;
    builder.write_json_artifact("plan/scan.json", &plan.scan)?;
    builder.write_json_artifact("plan/explain.json", &plan.explain)?;
    if let Some(graph) = &plan.operator_graph {
        graph.validate()?;
        builder.write_json_artifact("plan/operator-graph.json", graph)?;
    }
    builder.write_json_artifact("plan/validation-program.json", &validation_program)?;
    if let Some(evidence) = effective_schema_evidence {
        builder.write_json_artifact("schema/effective-schema-evidence.json", evidence)?;
    }
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
    let mut output_schema = Some(schema_artifact(runtime_output_schema.as_ref()));
    let mut schema_coercion = validation_program.schema_coercion.clone();
    let mut per_observation_schema_evidence =
        BTreeMap::<String, PerObservationSchemaCoercionArtifact>::new();
    let mut attempted_observations = BTreeSet::new();
    let mut processed_observations = Vec::new();
    let mut terminal_quarantines = Vec::new();
    let mut observation_attestations = BTreeMap::<String, PartitionAttestation>::new();
    let mut residual_decisions = Vec::new();
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

    for (partition_ordinal, partition) in plan.scan.partitions.clone().into_iter().enumerate() {
        let partition_ordinal = u32::try_from(partition_ordinal)
            .map_err(|_| CdfError::data("partition ordinal exceeds u32"))?;
        if remaining_limit == Some(0) {
            break;
        }
        let partition_scope = partition.scope.clone();
        let partition_schema_disposition = effective_schema_evidence
            .map(|evidence| partition_schema_disposition(&partition, evidence))
            .transpose()?;
        if let Some(PartitionSchemaDisposition::Quarantined(quarantine)) =
            &partition_schema_disposition
        {
            let attestation = match observation_attestations.get(quarantine.observation_id()) {
                Some(attestation) => attestation.clone(),
                None => {
                    let attestation = resource
                        .attest_partition(&partition)
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
                source_position,
            )?);
            terminal_quarantines.push(quarantine.clone());
            continue;
        }
        let partition_schema_evidence =
            partition_schema_disposition
                .as_ref()
                .and_then(|item| match item {
                    PartitionSchemaDisposition::Admitted(evidence) => Some(evidence),
                    PartitionSchemaDisposition::Quarantined(_) => None,
                });
        if let Some(expected) = partition_schema_evidence {
            attempted_observations.insert(expected.observation_id.clone());
        }

        let partition_span = trace_context
            .map(|context| partition_execution_span(context, partition.partition_id.as_str()))
            .unwrap_or_else(Span::none);

        let partition_for_open = partition.clone();
        let mut segment_assembler =
            crate::CanonicalSegmentAssembler::new(segmentation_policy.clone(), partition_ordinal)?;
        let (fully_processed, observed_positions) = async {
            let decode_started = phase_measurements.start();
            let mut stream = resource.open(partition_for_open).await?;
            phase_measurements.add(
                RunPhase::Decode,
                elapsed_ns(decode_started, "resource open")?,
                0,
                0,
            );
            let mut fully_processed = true;
            let mut observed_positions = Vec::new();
            loop {
                let decode_started = phase_measurements.start();
                let next_batch = stream.next().await;
                let decode_duration_ns = elapsed_ns(decode_started, "resource decode")?;
                let Some(batch) = next_batch else {
                    phase_measurements.add(RunPhase::Decode, decode_duration_ns, 0, 0);
                    break;
                };
                if remaining_limit == Some(0) {
                    fully_processed = false;
                    break;
                }

                let mut batch = batch?;
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
                let residual_candidates = batch.header.take_residual_candidates();
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
                let batch_coercion = validate_batch_schema_evidence(
                    &batch,
                    record_batch,
                    partition_schema_evidence,
                    resource.schema().as_ref(),
                )?;
                if let Some(batch_coercion) = batch_coercion {
                    if let Some(expected) = &partition_schema_evidence {
                        let artifact = PerObservationSchemaCoercionArtifact {
                            observation_id: expected.observation_id.clone(),
                            physical_schema_hash: expected.physical_schema_hash.to_string(),
                            coercion_plan: batch_coercion,
                        };
                        match per_observation_schema_evidence
                            .insert(expected.observation_id.clone(), artifact.clone())
                        {
                            Some(existing) if existing != artifact => {
                                return Err(CdfError::data(format!(
                                    "schema observation {:?} produced inconsistent coercion evidence across batches",
                                    expected.observation_id
                                )));
                            }
                            _ => {}
                        }
                    } else {
                        if let Some(existing) = &schema_coercion
                            && existing != &batch_coercion
                        {
                            return Err(CdfError::data(
                                "input batches carry inconsistent schema coercion evidence",
                            ));
                        }
                        schema_coercion = Some(batch_coercion);
                    }
                } else if partition_schema_evidence.is_some() {
                    return Err(CdfError::data(
                        "effective-schema execution requires trusted per-observation coercion evidence on every batch",
                    ));
                }

                let ExecutedBatch {
                    batch: output,
                    source_rows,
                } = execute_batch(
                    record_batch,
                    plan,
                    &mut remaining_limit,
                    !residual_candidates.is_empty(),
                )?;
                let batch_source_position = normalize_source_position_for_partition(
                    batch.header.source_position.clone(),
                    &partition_scope,
                );
                if let Some(position) = &batch_source_position {
                    observed_positions.push(position.clone());
                }
                if output.num_rows() == 0 {
                    phase_measurements.add(
                        RunPhase::ValidationNormalization,
                        elapsed_ns(validation_started, "validation/normalization")?,
                        validation_input_bytes,
                        0,
                    );
                    continue;
                }

                let evaluation_context = package_evaluation_context
                    .clone()
                    .with_source_position(batch_source_position.clone());
                let ContractExecOutput {
                    accepted,
                    variant_values,
                    quarantine_records,
                    summary,
                    residual_decisions: batch_residual_decisions,
                } = apply_contract_exec(
                    output,
                    &validation_program,
                    residual_candidates,
                    &ResidualBatchContext {
                        evaluation: &evaluation_context,
                        source_rows: source_rows.as_deref(),
                        cdc_operation_field: cdc_operation_field.as_deref(),
                        batch_id: &batch.header.batch_id,
                        observation_id: partition_schema_evidence
                            .map(|evidence| evidence.observation_id.as_str()),
                    },
                    if options.unfused_transform {
                        TransformKernelMode::Unfused
                    } else {
                        TransformKernelMode::Fused
                    },
                )?;
                residual_decisions.extend(batch_residual_decisions);
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
                    phase_measurements.add(
                        RunPhase::ValidationNormalization,
                        elapsed_ns(validation_started, "validation/normalization")?,
                        validation_input_bytes,
                        0,
                    );
                    continue;
                }
                let validation_output_bytes =
                    u64::try_from(output.get_array_memory_size())
                        .map_err(|error| CdfError::internal(error.to_string()))?;
                if apply_package_dedup {
                    let output = prepare_output_batch(
                        &validation_program,
                        effective_schema_evidence.is_some(),
                        PreparedOutputBatch {
                            output,
                            variant_values,
                            output_position: batch_source_position.clone(),
                        },
                        &mut output_schema,
                        runtime_output_schema.as_ref(),
                        &mut phase_measurements,
                    )?;
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
                        payload.push(partition_ordinal, batch_source_position, &output)?;
                    } else {
                        pending_dedup_batches.push(PendingDedupBatch {
                            partition_ordinal,
                            output,
                            output_position: batch_source_position,
                        });
                    }
                    continue;
                }
                phase_measurements.add(
                    RunPhase::ValidationNormalization,
                    elapsed_ns(validation_started, "validation/normalization")?,
                    validation_input_bytes,
                    validation_output_bytes,
                );
                write_output_batch(
                    &mut builder,
                    &validation_program,
                    effective_schema_evidence.is_some(),
                    PreparedOutputBatch {
                        output,
                        variant_values,
                        output_position: batch_source_position,
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
                    },
                )?;
            }
            persist_canonical_segments(
                &mut builder,
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
                },
            )?;
            Ok::<_, CdfError>((fully_processed, observed_positions))
        }
        .instrument(partition_span)
        .await?;
        if fully_processed
            && let Some(observation_id) = partition
                .metadata
                .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
                .cloned()
        {
            let fallback_attestation = if observed_positions.is_empty() {
                match observation_attestations.get(&observation_id) {
                    Some(attestation) => Some(attestation.clone()),
                    None => {
                        let attestation = resource.attest_partition(&partition).await?;
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
            let source_position = aggregate_processed_partition_positions(
                &observation_id,
                &observed_positions,
                fallback_attestation.map(PartitionAttestation::into_processed_position),
            )?;
            processed_observations.push(ProcessedObservationPosition::new(
                observation_id,
                ProcessedObservationOutcome::Admitted,
                source_position,
            )?);
        }
    }

    if apply_package_dedup {
        apply_dedup_and_write_pending_batches(
            &mut builder,
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
            },
        )?;
    }

    if effective_schema_evidence.is_none() && validation_program.schema_coercion.is_none() {
        validation_program.schema_coercion = schema_coercion;
    }
    builder.write_json_artifact("plan/validation-program.json", &validation_program)?;
    if let Some(coercion) = &validation_program.schema_coercion {
        builder.write_json_artifact("schema/coercion-plan.json", coercion)?;
    }
    if let Some(evidence) = effective_schema_evidence {
        if per_observation_schema_evidence
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != attempted_observations
        {
            return Err(CdfError::data(format!(
                "effective-schema execution recorded coercion evidence for {} observations but extraction attempted {} unique observations",
                per_observation_schema_evidence.len(),
                attempted_observations.len()
            )));
        }
        builder.write_json_artifact(
            "schema/per-observation-coercion.json",
            &PerObservationSchemaEvidenceArtifact {
                baseline_snapshot_schema_hash: evidence
                    .authority
                    .baseline_snapshot
                    .schema_hash
                    .to_string(),
                effective_snapshot_schema_hash: evidence
                    .authority
                    .effective_snapshot_schema_hash
                    .to_string(),
                effective_arrow_schema_hash: evidence.effective_arrow_schema_hash.to_string(),
                discovery_manifest_hash: evidence
                    .authority
                    .discovery_manifest
                    .manifest_hash
                    .to_string(),
                discovery_coverage: evidence.authority.discovery_coverage.clone(),
                observations: per_observation_schema_evidence.into_values().collect(),
            },
        )?;
    }
    if !terminal_quarantines.is_empty() {
        builder
            .write_json_artifact("quarantine/schema-observations.json", &terminal_quarantines)?;
    }
    builder.write_json_artifact(
        "schema/output.json",
        &output_schema.expect("compiled output schema is always present"),
    )?;
    builder.write_runtime_arrow_schema(runtime_output_schema.as_ref())?;
    if let Some(evolution) = contract_evolution_artifact(
        &validation_program,
        schema_authority.baseline_schema_hash.clone(),
        schema_authority.effective_schema_hash.clone(),
        residual_decisions,
    ) {
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
    let execution_evidence = EngineExecutionEvidence::new(processed_observations)?;
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
    let manifest = builder.finish()?;
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
            segments,
            profile,
            lineage,
        },
        segment_positions,
        phase_metrics: phase_measurements.into_metrics(),
        execution_evidence,
    })
}

fn apply_dedup_and_write_pending_batches(
    builder: &mut PackageBuilder,
    program: &ValidationProgram,
    pending: Vec<PendingDedupBatch>,
    external: Option<(
        cdf_contract::PackageDedupRuleSpec,
        crate::dedup_spill::ExternalDedupIndex,
        crate::dedup_spill::DedupPayloadSpool,
    )>,
    segmentation_policy: &crate::CanonicalSegmentationPolicy,
    state: &mut OutputWriteState<'_>,
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
                    persist_canonical_segments(builder, previous.finish()?, state)?;
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
                builder,
                output,
                payload_batch.output_position,
                &mut assembler.as_mut().expect("assembler initialized").1,
                state,
            )?;
        }
        if decisions.next()?.is_some() {
            return Err(CdfError::internal(
                "external dedup decision stream contains excess rows",
            ));
        }
        if let Some((_, mut assembler)) = assembler {
            persist_canonical_segments(builder, assembler.finish()?, state)?;
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
                persist_canonical_segments(builder, previous.finish()?, state)?;
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
            builder,
            output,
            pending.output_position,
            &mut assembler.as_mut().expect("assembler initialized").1,
            state,
        )?;
    }
    if let Some((_, mut assembler)) = assembler {
        persist_canonical_segments(builder, assembler.finish()?, state)?;
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

fn aggregate_processed_partition_positions(
    observation_id: &str,
    observed: &[SourcePosition],
    attested: Option<SourcePosition>,
) -> Result<SourcePosition> {
    let mut positions = observed.to_vec();
    if let Some(attested) = attested {
        positions.push(attested);
    }
    let first = positions.first().cloned().ok_or_else(|| {
        CdfError::data(format!(
            "processed observation {observation_id:?} completed without source-position evidence"
        ))
    })?;
    if positions.iter().any(|position| position != &first) {
        return Err(CdfError::data(format!(
            "processed observation {observation_id:?} produced source-position evidence that differs from execution-time attestation"
        )));
    }
    Ok(first)
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
    canonicalize_observed_schema: bool,
    prepared: PreparedOutputBatch,
    assembler: &mut crate::CanonicalSegmentAssembler,
    state: &mut OutputWriteState<'_>,
) -> Result<()> {
    let output_position = prepared.output_position.clone();
    let output = prepare_output_batch(
        program,
        canonicalize_observed_schema,
        prepared,
        state.output_schema,
        state.expected_schema,
        state.phase_measurements,
    )?;
    write_normalized_output_batch(builder, output, output_position, assembler, state)
}

fn prepare_output_batch(
    program: &ValidationProgram,
    canonicalize_observed_schema: bool,
    prepared: PreparedOutputBatch,
    output_schema: &mut Option<SchemaArtifact>,
    expected_schema: &Schema,
    phase_measurements: &mut PhaseMeasurements,
) -> Result<RecordBatch> {
    let PreparedOutputBatch {
        output,
        variant_values,
        output_position: _,
    } = prepared;
    let normalization_started = phase_measurements.start();
    let normalization_input_bytes = output.get_array_memory_size() as u64;
    let output = append_residual_variant(output, program, variant_values)?;
    let output = normalize_record_batch(output, program)?;
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
    Ok(output)
}

fn write_normalized_output_batch(
    builder: &mut PackageBuilder,
    output: RecordBatch,
    output_position: Option<SourcePosition>,
    assembler: &mut crate::CanonicalSegmentAssembler,
    state: &mut OutputWriteState<'_>,
) -> Result<()> {
    let canonical_segments = assembler.push(output, output_position)?;
    persist_canonical_segments(builder, canonical_segments, state)
}

fn persist_canonical_segments(
    builder: &mut PackageBuilder,
    canonical_segments: Vec<crate::CanonicalSegment>,
    state: &mut OutputWriteState<'_>,
) -> Result<()> {
    for canonical in canonical_segments {
        let _memory_lease = match state.memory {
            Some(memory) => {
                let bytes = canonical
                    .retained_bytes
                    .max(1)
                    .checked_mul(2)
                    .ok_or_else(|| CdfError::data("canonical concat working set overflow"))?;
                let request = ReservationRequest::new(
                    ConsumerKey::new("canonical-segment-concat", MemoryClass::Package)?,
                    bytes,
                )?
                .as_minimum_working_set();
                Some(memory.try_reserve(&request)?.ok_or_else(|| {
                    CdfError::data(format!(
                        "canonical segment requires {bytes} bytes for retained input and concat output but the shared memory budget is exhausted; reduce jobs or raise the memory budget"
                    ))
                })?)
            }
            None => None,
        };
        let schema = canonical
            .batches
            .first()
            .ok_or_else(|| CdfError::internal("canonical segment has no batches"))?
            .schema();
        let output = arrow_select::concat::concat_batches(&schema, &canonical.batches)
            .map_err(CdfError::from)?;
        let normalization_output_bytes = u64::try_from(output.get_array_memory_size())
            .map_err(|_| CdfError::data("canonical output bytes exceed u64"))?;
        let segment_id = canonical.segment_id;
        let write = if state.phase_measurements.enabled {
            builder.write_segment_with_metrics(segment_id.clone(), &[output])?
        } else {
            cdf_package::SegmentWriteMetrics {
                segment: builder.write_segment(segment_id.clone(), &[output])?,
                encode_duration_ns: 0,
                persist_hash_duration_ns: 0,
            }
        };
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
        state.profile.output_rows = state.profile.output_rows.saturating_add(segment.row_count);
        state.profile.output_bytes = state
            .profile
            .output_bytes
            .saturating_add(segment.byte_count);
        state.profile.output_batches = state.profile.output_batches.saturating_add(1);
        state.lineage.output_segments.push(segment_id);
        state.segment_positions.push(EngineSegmentPosition {
            segment_id: segment.segment_id.clone(),
            output_position: canonical.output_position,
        });
        state.segments.push(segment);
    }
    Ok(())
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
    plan: &EnginePlan,
    remaining_limit: &mut Option<u64>,
    track_source_rows: bool,
) -> Result<ExecutedBatch> {
    const SOURCE_ROW_FIELD: &str = "_cdf_internal_source_row";
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
    let filtered = apply_residual_filters(&tracked, &plan.residual_predicates)?;
    let limited = match remaining_limit {
        Some(remaining) => {
            let take = (*remaining).min(filtered.num_rows() as u64) as usize;
            *remaining -= take as u64;
            filtered.slice(0, take)
        }
        None => filtered,
    };
    let projected = if track_source_rows {
        let mut projection = plan.final_projection.clone().unwrap_or_else(|| {
            limited
                .schema()
                .fields()
                .iter()
                .filter(|field| field.name() != SOURCE_ROW_FIELD)
                .map(|field| field.name().clone())
                .collect()
        });
        if projection.is_empty() {
            projection = limited
                .schema()
                .fields()
                .iter()
                .filter(|field| field.name() != SOURCE_ROW_FIELD)
                .map(|field| field.name().clone())
                .collect();
        }
        projection.push(SOURCE_ROW_FIELD.to_owned());
        apply_projection(&limited, Some(&projection))?
    } else {
        apply_projection(&limited, plan.final_projection.as_deref())?
    };
    if !track_source_rows {
        return Ok(ExecutedBatch {
            batch: projected,
            source_rows: None,
        });
    }
    let ordinal_index = projected.schema().index_of(SOURCE_ROW_FIELD)?;
    let ordinals = projected
        .column(ordinal_index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| CdfError::internal("source-row tracking column is not uint64"))?;
    let source_rows = ordinals
        .values()
        .iter()
        .map(|value| usize::try_from(*value).map_err(|error| CdfError::internal(error.to_string())))
        .collect::<Result<Vec<_>>>()?;
    let keep = (0..projected.num_columns())
        .filter(|index| *index != ordinal_index)
        .collect::<Vec<_>>();
    let batch = projected.project(&keep).map_err(CdfError::from)?;
    Ok(ExecutedBatch {
        batch,
        source_rows: Some(source_rows),
    })
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

fn apply_contract_exec(
    batch: RecordBatch,
    program: &ValidationProgram,
    residual_candidates: Vec<PreContractResidualCandidate>,
    context: &ResidualBatchContext<'_>,
    mode: TransformKernelMode,
) -> Result<ContractExecOutput> {
    if mode == TransformKernelMode::Fused && residual_candidates.is_empty() {
        return apply_contract_exec_without_residual_candidates(batch, program, context);
    }
    let residual = apply_residual_verdicts(batch, program, residual_candidates, context)?;
    let evaluation = evaluate_record_batch(program, context.evaluation, &residual.typed_batch)?;
    let summary = evaluation.summary;
    let mut quarantine_records = residual.quarantine_records;
    quarantine_records.extend(quarantine_records_from_candidates(
        evaluation.quarantine_candidates,
    )?);
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
        quarantine_records,
        summary: combined,
        residual_decisions: residual.residual_decisions,
    })
}

fn apply_contract_exec_without_residual_candidates(
    batch: RecordBatch,
    program: &ValidationProgram,
    context: &ResidualBatchContext<'_>,
) -> Result<ContractExecOutput> {
    let batch = restore_contract_nullability(batch, program)?;
    let evaluation = evaluate_record_batch(program, context.evaluation, &batch)?;
    let summary = evaluation.summary;
    let quarantine_records = quarantine_records_from_candidates(evaluation.quarantine_candidates)?;
    let accepted = if summary.accepted_rows == summary.input_rows {
        batch
    } else {
        filter_record_batch(&batch, &evaluation.accepted_rows).map_err(CdfError::from)?
    };
    let variant_values = if program
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
        quarantine_records,
        summary,
        residual_decisions: Vec::new(),
    })
}

struct ResidualExecOutput {
    typed_batch: RecordBatch,
    variant_values: Vec<Option<String>>,
    quarantine_records: Vec<QuarantineRecord>,
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
) -> Result<ResidualExecOutput> {
    let input_rows = batch.num_rows() as u64;
    let mut variants = vec![None; batch.num_rows()];
    let mut accepted = vec![true; batch.num_rows()];
    let mut quarantine_records = Vec::new();
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
                quarantine_records.push(QuarantineRecord {
                    source_row_ordinal: candidate.source_row_ordinal(),
                    rule_id: rule_id.clone(),
                    error_code: error_code.clone(),
                    source_position: context.evaluation.source_position.clone(),
                    observed_value_redacted: residual_observed_value(program, candidate),
                });
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
    let variant_values = accepted
        .into_iter()
        .zip(variants)
        .filter_map(|(accepted, value)| accepted.then_some(value))
        .collect::<Vec<_>>();
    let quarantined_rows = input_rows - typed_batch.num_rows() as u64;
    let quarantine_candidate_count = quarantine_records.len() as u64;
    Ok(ResidualExecOutput {
        typed_batch,
        variant_values,
        quarantine_records,
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
    let schema_authority = plan.schema_authority()?;
    if schema_authority.baseline_schema_hash != evidence.authority.baseline_snapshot.schema_hash
        || schema_authority.effective_schema_hash
            != evidence.authority.effective_snapshot_schema_hash
    {
        return Err(CdfError::data(
            "engine plan schema authority does not match effective-schema evidence",
        ));
    }
    evidence
        .authority
        .validate_for_resource(resource.descriptor())?;
    let effective_arrow_schema_hash =
        cdf_contract::canonical_arrow_schema_hash(resource.schema().as_ref())?;
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
        .map(|index| &evidence.observations[index])
        .ok_or_else(|| {
            CdfError::data(format!(
                "effective-schema evidence has no candidate for observation {observation_id:?}"
            ))
        })?;
    validate_plan_metadata(
        partition,
        PLAN_PHYSICAL_SCHEMA_HASH_KEY,
        observation.physical_schema_hash.as_str(),
    )?;
    Ok(PartitionSchemaDisposition::Admitted(observation.clone()))
}

fn validate_effective_batch_schema(observed: &Schema, effective: &Schema) -> Result<()> {
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
        if observed.name() != effective.name()
            || observed_source != effective_source
            || observed.data_type() != effective.data_type()
            || observed.is_nullable() != effective.is_nullable()
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

fn elapsed_ns(started: Option<Instant>, label: &str) -> Result<u64> {
    let Some(started) = started else {
        return Ok(0);
    };
    u64::try_from(started.elapsed().as_nanos())
        .map_err(|error| CdfError::internal(format!("{label} duration overflow: {error}")))
}

#[cfg(test)]
mod transform_kernel_tests {
    use std::{hint::black_box, sync::Arc, time::Instant};

    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_contract::{
        ContractEvaluationContext, ContractPolicy, ObservedSchema, SchemaEvolutionMode,
        compile_validation_program,
    };
    use cdf_kernel::{BatchId, TrustLevel};

    use super::{ResidualBatchContext, TransformKernelMode, apply_contract_exec};

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
            let started = Instant::now();
            for _ in 0..iterations {
                let output = apply_contract_exec(
                    black_box(batch.clone()),
                    black_box(&program),
                    Vec::new(),
                    black_box(&context),
                    mode,
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
