use super::destinations::{
    DestinationCommitPlanningInputs, ProjectDestinationDescription, ResolvedProjectDestination,
};
use cdf_engine::EnginePlan;
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CapabilitySupport, CdfError, CheckpointId, CommitPlan,
    DestinationCommitRequest, DestinationSheet, ForeignState, IdempotencyToken, PackageHash,
    PipelineId, ResourceStream, Result, SchemaHash, SegmentId, SourcePosition, StateDelta,
    StateSegment, TargetName,
};

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
        let schema_hash = plan.route_family.as_ref().map_or_else(
            || output.schema_hash.clone(),
            |family| family.schema_family_hash.clone(),
        );
        let sheet = self.validate_output_schema_mappings(resource, output.schema.as_ref())?;
        let inputs = if plan.route_family.is_some() {
            routed_destination_planning_inputs(resource, &target, plan)?
        } else {
            destination_planning_inputs(resource, &target, plan, &schema_hash)?
        };
        let outcome = if plan.route_family.is_some() {
            if self
                .runtime_mut()
                .protocol()
                .protocol_capabilities()
                .routed_target_families
                != CapabilitySupport::Supported
            {
                return Err(CdfError::destination(format!(
                    "destination {} does not support atomic routed target families",
                    description.destination_id
                )));
            }
            let commit_plan = self
                .runtime_mut()
                .plan_routed_package(&inputs.destination_commit)?;
            cdf_runtime::DestinationCommitPlanningOutcome::new(sheet.clone(), commit_plan)
        } else {
            self.runtime_mut()
                .plan_resource_commit(resource, output.schema.as_ref(), &inputs)?
        };
        if outcome.sheet != sheet {
            return Err(CdfError::contract(format!(
                "destination {} changed its capability sheet between schema mapping and commit planning",
                description.destination_id
            )));
        }
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
            sheet,
            commit_plan: outcome.plan,
        })
    }
}

fn routed_destination_planning_inputs(
    resource: &dyn ResourceStream,
    target: &TargetName,
    plan: &EnginePlan,
) -> Result<DestinationCommitPlanningInputs> {
    let content = cdf_engine::planned_empty_package_content(plan)?;
    let schema_hash = content.logical_schema_hash().clone();
    let package_hash = PackageHash::new(PLAN_PREVIEW_PACKAGE_HASH)?;
    let state_delta = StateDelta {
        checkpoint_id: CheckpointId::new("checkpoint-plan-preview")?,
        pipeline_id: PipelineId::new("pipeline-plan-preview")?,
        resource_id: resource.descriptor().resource_id.clone(),
        scope: resource.descriptor().state_scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: synthetic_position(),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        package_hash: package_hash.clone(),
        content: content.clone(),
        schema_hash: schema_hash.clone(),
        segments: Vec::new(),
    };
    let destination_commit = DestinationCommitRequest {
        package_hash,
        content,
        target: target.clone(),
        disposition: resource.descriptor().write_disposition.clone(),
        segments: Vec::new(),
        idempotency_token: IdempotencyToken::new(PLAN_PREVIEW_IDEMPOTENCY_TOKEN)?,
    };
    Ok(DestinationCommitPlanningInputs {
        state_delta,
        destination_commit,
        schema_hash,
    })
}

fn destination_planning_inputs(
    resource: &dyn ResourceStream,
    target: &TargetName,
    plan: &EnginePlan,
    schema_hash: &SchemaHash,
) -> Result<DestinationCommitPlanningInputs> {
    let package_hash = PackageHash::new(PLAN_PREVIEW_PACKAGE_HASH)?;
    let content = cdf_engine::planned_empty_package_content(plan)?;
    let segments = if matches!(content, cdf_kernel::PackageContentAuthority::Rows { .. }) {
        vec![synthetic_segment(resource)?]
    } else {
        Vec::new()
    };
    let state_delta = StateDelta {
        checkpoint_id: CheckpointId::new("checkpoint-plan-preview")?,
        pipeline_id: PipelineId::new("pipeline-plan-preview")?,
        resource_id: resource.descriptor().resource_id.clone(),
        scope: resource.descriptor().state_scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: segments.first().map_or_else(synthetic_position, |segment| {
            segment.output_position.clone()
        }),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        package_hash: package_hash.clone(),
        content: content.clone(),
        schema_hash: schema_hash.clone(),
        segments: segments.clone(),
    };
    let destination_commit = DestinationCommitRequest {
        package_hash,
        content,
        target: target.clone(),
        disposition: resource.descriptor().write_disposition.clone(),
        segments,
        idempotency_token: IdempotencyToken::new(PLAN_PREVIEW_IDEMPOTENCY_TOKEN)?,
    };
    Ok(DestinationCommitPlanningInputs {
        state_delta,
        destination_commit,
        schema_hash: schema_hash.clone(),
    })
}

fn synthetic_segment(resource: &dyn ResourceStream) -> Result<StateSegment> {
    let position = synthetic_position();
    Ok(StateSegment {
        kind: cdf_kernel::PackageSegmentKind::Row,
        segment_id: SegmentId::new(PLAN_PREVIEW_SEGMENT_ID)?,
        scope: resource.descriptor().state_scope.clone(),
        output_position: position,
        row_count: 0,
        byte_count: 0,
    })
}

fn synthetic_position() -> SourcePosition {
    SourcePosition::ForeignState(ForeignState {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        protocol: "cdf-plan-preview".to_owned(),
        opaque_blob: Vec::new(),
        blob_sha256: EMPTY_SHA256.to_owned(),
    })
}
