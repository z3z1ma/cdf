use super::{
    destinations::{
        DestinationCommitPlanningInputs, ProjectDestinationDescription, ResolvedProjectDestination,
    },
    prelude::*,
};
use cdf_kernel::{CommitPlan, DestinationSheet, ForeignState};

const PLAN_PREVIEW_PACKAGE_HASH: &str = "sha256:plan-preview";
const PLAN_PREVIEW_IDEMPOTENCY_TOKEN: &str = "sha256:plan-preview";
const PLAN_PREVIEW_SEGMENT_ID: &str = "seg-plan-preview";
const EMPTY_SHA256: &str =
    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectDestinationCommitPlan {
    pub description: ProjectDestinationDescription,
    pub target: TargetName,
    pub schema_hash: SchemaHash,
    pub synthetic: ProjectDestinationSyntheticInput,
    pub request: DestinationCommitRequest,
    pub sheet: DestinationSheet,
    pub commit_plan: CommitPlan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectDestinationSyntheticInput {
    pub package_hash: PackageHash,
    pub idempotency_token: IdempotencyToken,
    pub segment_ids: Vec<SegmentId>,
}

impl ResolvedProjectDestination {
    pub fn plan_resource_commit(
        &mut self,
        resource: &dyn ResourceStream,
        plan: &EnginePlan,
    ) -> Result<ProjectDestinationCommitPlan> {
        let description = self.describe();
        let target = self.target().clone();
        let output = self.output_schema(plan)?;
        let schema_hash = output.schema_hash;
        let inputs = destination_planning_inputs(resource, &target, &schema_hash)?;
        let outcome =
            self.runtime_mut()
                .plan_resource_commit(resource, output.schema.as_ref(), &inputs)?;
        let synthetic = ProjectDestinationSyntheticInput {
            package_hash: inputs.destination_commit.package_hash.clone(),
            idempotency_token: inputs.destination_commit.idempotency_token.clone(),
            segment_ids: inputs
                .destination_commit
                .segments
                .iter()
                .map(|segment| segment.segment_id.clone())
                .collect(),
        };
        Ok(ProjectDestinationCommitPlan {
            description,
            target,
            schema_hash,
            synthetic,
            request: inputs.destination_commit,
            sheet: outcome.sheet,
            commit_plan: outcome.plan,
        })
    }
}

fn destination_planning_inputs(
    resource: &dyn ResourceStream,
    target: &TargetName,
    schema_hash: &SchemaHash,
) -> Result<DestinationCommitPlanningInputs> {
    let package_hash = PackageHash::new(PLAN_PREVIEW_PACKAGE_HASH)?;
    let segment = synthetic_segment(resource)?;
    let state_delta = StateDelta {
        checkpoint_id: CheckpointId::new("checkpoint-plan-preview")?,
        pipeline_id: PipelineId::new("pipeline-plan-preview")?,
        resource_id: resource.descriptor().resource_id.clone(),
        scope: resource.descriptor().state_scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: segment.output_position.clone(),
        package_hash: package_hash.clone(),
        schema_hash: schema_hash.clone(),
        segments: vec![segment],
    };
    let destination_commit = DestinationCommitRequest {
        package_hash,
        target: target.clone(),
        disposition: resource.descriptor().write_disposition.clone(),
        segments: state_delta.segments.clone(),
        idempotency_token: IdempotencyToken::new(PLAN_PREVIEW_IDEMPOTENCY_TOKEN)?,
    };
    Ok(DestinationCommitPlanningInputs {
        state_delta,
        destination_commit,
        schema_hash: schema_hash.clone(),
    })
}

fn synthetic_segment(resource: &dyn ResourceStream) -> Result<StateSegment> {
    let position = SourcePosition::ForeignState(ForeignState {
        version: CHECKPOINT_STATE_VERSION,
        protocol: "cdf-plan-preview".to_owned(),
        opaque_blob: Vec::new(),
        blob_sha256: EMPTY_SHA256.to_owned(),
    });
    Ok(StateSegment {
        segment_id: SegmentId::new(PLAN_PREVIEW_SEGMENT_ID)?,
        scope: resource.descriptor().state_scope.clone(),
        output_position: position,
        row_count: 0,
        byte_count: 0,
    })
}
