use super::prelude::*;
#[cfg(test)]
use super::types::LocalFileDuckDbRunRequest;
use cdf_kernel::CapabilitySupport;
use cdf_package_contract::{PROCESSED_OBSERVATIONS_FILE, ProcessedObservationEvidenceArtifact};

const QUARANTINE_MIRROR_OUTCOME_FILE: &str = "destination/quarantine-mirror.json";

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
        draft.segments,
        draft.segment_positions,
        draft.execution_evidence(),
        schema_hash,
        scope,
        head.as_ref(),
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
    let artifacts = quarantine_artifacts(builder.package_dir())?;
    if artifacts.is_empty() {
        return Ok(());
    }

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
            destination_id: context.destination_id.as_str().to_owned(),
            quarantine_table_support: capability_support_name(&context.quarantine_table_support),
            quarantine_artifacts: artifacts,
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
    destination_id: String,
    quarantine_table_support: &'static str,
    quarantine_artifacts: Vec<String>,
    outcome: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
}

fn quarantine_artifacts(package_dir: &Path) -> Result<Vec<String>> {
    let directory = package_dir.join("quarantine");
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut artifacts = Vec::new();
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
        if (file_name.starts_with("part-") && file_name.ends_with(".parquet"))
            || file_name == "schema-observations.json"
        {
            artifacts.push(format!("quarantine/{file_name}"));
        }
    }
    artifacts.sort();
    Ok(artifacts)
}

fn capability_support_name(support: &CapabilitySupport) -> &'static str {
    match support {
        CapabilitySupport::Supported => "supported",
        CapabilitySupport::Unsupported => "unsupported",
    }
}

#[cfg(test)]
pub(crate) fn state_delta_from_run(
    request: &LocalFileDuckDbRunRequest<'_>,
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
        &output.output.segments,
        &output.segment_positions,
        output.execution_evidence(),
        schema_hash,
        scope,
        head,
    )?;
    Ok(preimage.into_state_delta(PackageHash::new(
        output.output.manifest.package_hash.clone(),
    )?))
}

fn state_delta_preimage_from_run_draft(
    context: &StateCommitArtifactContext<'_>,
    segments: &[SegmentEntry],
    segment_positions: &[cdf_engine::EngineSegmentPosition],
    execution_evidence: &cdf_engine::EngineExecutionEvidence,
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: Option<&Checkpoint>,
) -> Result<StateDeltaPreimage> {
    if !execution_evidence.checkpoint_eligible() {
        return Err(CdfError::data(
            "checkpoint state requires complete source execution; a partial or limited source execution cannot advance state",
        ));
    }
    let positions = segment_positions_by_id(segments, segment_positions)?;
    let mut segment_evidence = Vec::with_capacity(segments.len());

    for segment in segments {
        let segment_position = positions
            .get(&segment.segment_id)
            .ok_or_else(|| {
                CdfError::internal(format!(
                    "engine output omitted source position evidence for segment {}",
                    segment.segment_id
                ))
            })?
            .clone()
            .ok_or_else(|| {
                CdfError::data(format!(
                    "package segment {} has no source position evidence; cdf run cannot checkpoint without source position evidence",
                    segment.segment_id
                ))
            })?;
        let segment_position = normalize_source_position_for_scope(segment_position, scope);
        segment_evidence.push((segment, segment_position));
    }

    if execution_evidence.processed_observations().is_empty() {
        return Err(CdfError::data(
            "checkpoint state requires complete processed-observation evidence; a partial or limited source execution cannot advance state",
        ));
    }
    let observed_positions = execution_evidence
        .processed_observations()
        .iter()
        .map(|observation| {
            normalize_source_position_for_scope(observation.source_position.clone(), scope)
        })
        .collect::<Vec<_>>();
    let output_position = cdf_kernel::aggregate_resource_closed_output_position(
        context.descriptor,
        context.schema,
        head.map(|checkpoint| &checkpoint.delta.output_position),
        &observed_positions,
    )?;
    let state_segments = segment_evidence
        .into_iter()
        .map(|(segment, segment_position)| StateSegment {
            segment_id: segment.segment_id.clone(),
            scope: scope.clone(),
            output_position: segment_position,
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        })
        .collect();
    Ok(StateDeltaPreimage {
        checkpoint_id: context.checkpoint_id.clone(),
        pipeline_id: context.pipeline_id.clone(),
        resource_id: context.descriptor.resource_id.clone(),
        scope: scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: head.map(|checkpoint| checkpoint.delta.checkpoint_id.clone()),
        input_position: head.map(|checkpoint| checkpoint.delta.output_position.clone()),
        output_position,
        schema_hash: schema_hash.clone(),
        segments: state_segments,
    })
}

fn normalize_source_position_for_scope(
    position: SourcePosition,
    scope: &ScopeKey,
) -> SourcePosition {
    match (scope, position) {
        (ScopeKey::File { path }, SourcePosition::FileManifest(mut manifest)) => {
            for file in &mut manifest.files {
                file.path = path.clone();
            }
            SourcePosition::FileManifest(manifest)
        }
        (_, position) => position,
    }
}

fn segment_positions_by_id(
    segments: &[SegmentEntry],
    segment_positions: &[cdf_engine::EngineSegmentPosition],
) -> Result<BTreeMap<SegmentId, Option<SourcePosition>>> {
    if segment_positions.len() != segments.len() {
        return Err(CdfError::internal(format!(
            "engine output has {} segment(s) but {} segment source position record(s)",
            segments.len(),
            segment_positions.len()
        )));
    }

    let positions = segment_positions
        .iter()
        .map(|position| {
            (
                position.segment_id.clone(),
                position.output_position.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if positions.len() != segment_positions.len() {
        return Err(CdfError::internal(
            "engine output contains duplicate segment source position records",
        ));
    }
    Ok(positions)
}
