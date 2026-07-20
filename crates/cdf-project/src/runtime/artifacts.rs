use super::prelude::*;
use cdf_kernel::CapabilitySupport;
use cdf_package_contract::{PROCESSED_OBSERVATIONS_FILE, ProcessedObservationEvidenceArtifact};

const QUARANTINE_MIRROR_OUTCOME_FILE: &str = "destination/quarantine-mirror.json";
const QUARANTINE_MIRROR_OUTCOME_VERSION: u16 = 1;
const QUARANTINE_DIRECTORY: &str = "quarantine/";

pub(super) fn write_run_state_commit_artifacts(
    builder: &cdf_package::PackageBuilder,
    draft: EnginePackageDraft<'_>,
    context: &StateCommitArtifactContext<'_>,
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: &Option<Checkpoint>,
) -> Result<()> {
    let state_delta = state_delta_preimage_from_run_draft(
        context,
        StateDeltaRunDraft {
            segment_positions: draft.segment_positions,
            execution_evidence: draft.execution_evidence(),
            source_continuation: draft
                .drain_frontier
                .and_then(|frontier| frontier.carryover.clone()),
            output_watermark: draft
                .drain_frontier
                .and_then(|frontier| frontier.watermark.clone()),
            consumed_late_data_carryover: draft.consumed_late_data_carryover.to_vec(),
            late_data_carryover: draft.late_data_carryover.to_vec(),
            partition_watermarks: draft.partition_watermarks.to_vec(),
        },
        schema_hash,
        scope,
        head.as_ref(),
        |visitor| builder.visit_segment_entries(visitor),
    )?;
    if !draft
        .execution_evidence()
        .processed_observations()
        .is_empty()
    {
        builder.write_json_artifact(
            PROCESSED_OBSERVATIONS_FILE,
            &ProcessedObservationEvidenceArtifact::new(
                head.as_ref()
                    .map(|checkpoint| checkpoint.delta.output_position.clone()),
                context.descriptor.write_disposition.clone(),
                draft.execution_evidence().processed_observations().to_vec(),
                state_delta.output_position.clone(),
            )?,
        )?;
    }
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        context.target.clone(),
        context.descriptor.write_disposition.clone(),
        context.descriptor.merge_key.clone(),
        schema_hash.clone(),
        state_delta.segments.clone(),
    );
    builder.write_input_checkpoint_artifact(head)?;
    builder.write_state_delta_preimage_artifact(&state_delta)?;
    builder.write_commit_plan_preimage_artifact(&commit_plan)?;
    Ok(())
}

pub(super) fn write_quarantine_mirror_outcome_artifact(
    builder: &cdf_package::PackageBuilder,
    context: &QuarantineMirrorArtifactContext,
) -> Result<()> {
    let Some(artifacts) = quarantine_artifact_summary(builder.package_dir())? else {
        return Ok(());
    };

    let (outcome, reason) = match context.quarantine_table_support {
        CapabilitySupport::Supported => ("mirror_supported", None),
        CapabilitySupport::Unsupported => (
            "not_mirrored",
            Some("destination sheet declares quarantine_tables unsupported"),
        ),
    };
    builder.write_json_artifact(
        QUARANTINE_MIRROR_OUTCOME_FILE,
        &QuarantineMirrorOutcomeArtifact {
            version: QUARANTINE_MIRROR_OUTCOME_VERSION,
            destination_id: context.destination_id.as_str().to_owned(),
            quarantine_table_support: capability_support_name(&context.quarantine_table_support),
            quarantine_directory: QUARANTINE_DIRECTORY,
            quarantine_part_count: artifacts.part_count,
            schema_observations_present: artifacts.schema_observations_present,
            outcome,
            reason,
        },
    )?;
    Ok(())
}

pub(super) struct StateCommitArtifactContext<'a> {
    pub(super) descriptor: &'a ResourceDescriptor,
    pub(super) schema: &'a Schema,
    pub(super) pipeline_id: &'a PipelineId,
    pub(super) checkpoint_id: &'a CheckpointId,
    pub(super) target: &'a TargetName,
}

pub(super) struct QuarantineMirrorArtifactContext {
    pub(super) destination_id: DestinationId,
    pub(super) quarantine_table_support: CapabilitySupport,
}

#[derive(serde::Serialize)]
struct QuarantineMirrorOutcomeArtifact<'a> {
    version: u16,
    destination_id: String,
    quarantine_table_support: &'static str,
    quarantine_directory: &'static str,
    quarantine_part_count: u64,
    schema_observations_present: bool,
    outcome: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
}

struct QuarantineArtifactSummary {
    part_count: u64,
    schema_observations_present: bool,
}

fn quarantine_artifact_summary(package_dir: &Path) -> Result<Option<QuarantineArtifactSummary>> {
    let directory = package_dir.join("quarantine");
    if !directory.exists() {
        return Ok(None);
    }
    let mut part_count = 0_u64;
    let mut schema_observations_present = false;
    for entry in fs::read_dir(&directory)
        .map_err(|error| CdfError::data(format!("read {}: {error}", directory.display())))?
    {
        let entry = entry
            .map_err(|error| CdfError::data(format!("read {}: {error}", directory.display())))?;
        let path = entry.path();
        if !entry
            .file_type()
            .map_err(|error| CdfError::data(format!("stat {}: {error}", path.display())))?
            .is_file()
        {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            return Err(CdfError::data(format!(
                "quarantine artifact path is not UTF-8: {}",
                path.display()
            )));
        };
        if file_name.starts_with("part-") && file_name.ends_with(".parquet") {
            part_count = part_count
                .checked_add(1)
                .ok_or_else(|| CdfError::data("quarantine artifact count overflowed u64"))?;
        } else if file_name == "schema-observations.json" {
            schema_observations_present = true;
        }
    }
    if part_count == 0 && !schema_observations_present {
        return Ok(None);
    }
    Ok(Some(QuarantineArtifactSummary {
        part_count,
        schema_observations_present,
    }))
}

fn capability_support_name(support: &CapabilitySupport) -> &'static str {
    match support {
        CapabilitySupport::Supported => "supported",
        CapabilitySupport::Unsupported => "unsupported",
    }
}

#[cfg(test)]
pub(crate) struct StateDeltaTestRequest<'a> {
    pub resource: &'a dyn QueryableResource,
    pub pipeline_id: PipelineId,
    pub checkpoint_id: CheckpointId,
    pub target: TargetName,
}

#[cfg(test)]
pub(crate) fn state_delta_from_run(
    request: &StateDeltaTestRequest<'_>,
    output: &EngineRunOutputWithSegmentPositions,
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: Option<&Checkpoint>,
) -> Result<StateDelta> {
    let schema = request.resource.schema();
    let context = StateCommitArtifactContext {
        descriptor: request.resource.descriptor(),
        schema: schema.as_ref(),
        pipeline_id: &request.pipeline_id,
        checkpoint_id: &request.checkpoint_id,
        target: &request.target,
    };
    let preimage = state_delta_preimage_from_run_draft(
        &context,
        StateDeltaRunDraft {
            segment_positions: &output.segment_positions,
            execution_evidence: output.execution_evidence(),
            source_continuation: output
                .drain_epoch
                .as_ref()
                .and_then(|epoch| epoch.closure.frontier.carryover.clone()),
            output_watermark: output
                .drain_epoch
                .as_ref()
                .and_then(|epoch| epoch.closure.frontier.watermark.clone()),
            consumed_late_data_carryover: output
                .drain_epoch
                .as_ref()
                .map(|epoch| epoch.consumed_late_data_carryover.clone())
                .unwrap_or_default(),
            late_data_carryover: output
                .drain_epoch
                .as_ref()
                .map(|epoch| epoch.late_data_carryover.clone())
                .unwrap_or_default(),
            partition_watermarks: output
                .drain_epoch
                .as_ref()
                .map(|epoch| epoch.partition_watermarks.clone())
                .unwrap_or_default(),
        },
        schema_hash,
        scope,
        head,
        |visitor| {
            for segment in output.output.identity_segments() {
                visitor(segment.clone())?;
            }
            Ok(())
        },
    )?;
    Ok(preimage.into_state_delta(PackageHash::new(
        output.output.manifest.package_hash.clone(),
    )?))
}

struct StateDeltaRunDraft<'a> {
    segment_positions: &'a [cdf_engine::EngineSegmentPosition],
    execution_evidence: &'a cdf_engine::EngineExecutionEvidence,
    source_continuation: Option<SourcePosition>,
    output_watermark: Option<cdf_kernel::WatermarkClaim>,
    consumed_late_data_carryover: Vec<cdf_kernel::LateDataCarryoverRef>,
    late_data_carryover: Vec<cdf_kernel::LateDataCarryoverRef>,
    partition_watermarks: Vec<cdf_kernel::PartitionWatermarkState>,
}

fn state_delta_preimage_from_run_draft(
    context: &StateCommitArtifactContext<'_>,
    draft: StateDeltaRunDraft<'_>,
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: Option<&Checkpoint>,
    visit_segments: impl FnOnce(&mut dyn FnMut(SegmentEntry) -> Result<()>) -> Result<()>,
) -> Result<StateDeltaPreimage> {
    if !draft.execution_evidence.checkpoint_eligible() {
        return Err(CdfError::data(
            "checkpoint state requires complete source execution; a partial or limited source execution cannot advance state",
        ));
    }
    let mut positions = draft.segment_positions.iter();
    let mut state_segments = Vec::with_capacity(draft.segment_positions.len());
    visit_segments(&mut |segment| {
        let segment_position = positions.next().ok_or_else(|| {
            CdfError::internal(format!(
                "engine output omitted canonical source-position evidence for segment {}",
                segment.segment_id
            ))
        })?;
        if segment_position.segment_id != segment.segment_id {
            return Err(CdfError::internal(format!(
                "engine source-position segment {} does not match canonical package segment {}",
                segment_position.segment_id, segment.segment_id
            )));
        }
        let output_position = segment_position.output_position.clone().ok_or_else(|| {
                CdfError::data(format!(
                    "package segment {} has no source position evidence; cdf run cannot checkpoint without source position evidence",
                    segment.segment_id
                ))
            })?;
        state_segments.push(StateSegment {
            segment_id: segment.segment_id,
            scope: scope.clone(),
            output_position,
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        });
        Ok(())
    })?;
    if positions.next().is_some() || state_segments.len() != draft.segment_positions.len() {
        return Err(CdfError::internal(format!(
            "engine output has {} segment source-position record(s) but the package builder exposed {} durable segment(s)",
            draft.segment_positions.len(),
            state_segments.len()
        )));
    }

    let observed_positions = draft
        .execution_evidence
        .processed_observations()
        .iter()
        .map(|observation| observation.source_position.clone())
        .collect::<Vec<_>>();
    let output_position = if observed_positions.is_empty() {
        if draft.consumed_late_data_carryover.is_empty() {
            return Err(CdfError::data(
                "checkpoint state requires processed source observations or persisted late-data carryover",
            ));
        }
        let head = head.ok_or_else(|| {
            CdfError::data("late-data carryover checkpoint requires a committed input head")
        })?;
        let carryover_positions = draft
            .consumed_late_data_carryover
            .iter()
            .map(|carryover| carryover.output_position.clone())
            .collect::<Vec<_>>();
        let observed = cdf_kernel::aggregate_resource_closed_output_position(
            context.descriptor,
            context.schema,
            Some(&head.delta.output_position),
            &carryover_positions,
        )?;
        if observed != head.delta.output_position
            || state_segments
                .iter()
                .any(|segment| segment.output_position != head.delta.output_position)
        {
            return Err(CdfError::data(
                "late-data carryover cannot advance or disagree with its committed source frontier",
            ));
        }
        head.delta.output_position.clone()
    } else {
        cdf_kernel::aggregate_resource_closed_output_position(
            context.descriptor,
            context.schema,
            head.map(|checkpoint| &checkpoint.delta.output_position),
            &observed_positions,
        )?
    };
    Ok(StateDeltaPreimage {
        checkpoint_id: context.checkpoint_id.clone(),
        pipeline_id: context.pipeline_id.clone(),
        resource_id: context.descriptor.resource_id.clone(),
        scope: scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: head.map(|checkpoint| checkpoint.delta.checkpoint_id.clone()),
        input_position: head.map(|checkpoint| checkpoint.delta.output_position.clone()),
        output_position,
        output_watermark: draft.output_watermark,
        partition_watermarks: draft.partition_watermarks,
        late_data_carryover: draft.late_data_carryover,
        source_continuation: draft.source_continuation,
        schema_hash: schema_hash.clone(),
        segments: state_segments,
    })
}
