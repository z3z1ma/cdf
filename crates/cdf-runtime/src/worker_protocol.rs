use std::collections::BTreeSet;

use cdf_kernel::{
    CdfError, CheckpointId, ContentObjectKey, ContentProviderGeneration, ContentStoreNamespace,
    CursorPosition, CursorValue, FencingToken, LeaseAuthorityDomainId, PartitionId, PipelineId,
    PlanId, ProcessedObservationOutcome, ResourceId, Result, SchemaHash, ScopeKey, SecretReference,
    SegmentId, SourcePosition, partition_source_identity_binding,
};
use serde::{Deserialize, Serialize};

use crate::{
    BlockingLaneBinding, BlockingLaneSpec, ExecutionHostCapabilities, SourceDriverId, artifact_hash,
};

pub const PORTABLE_PARTITION_TASK_VERSION: u16 = 1;
pub const PARTITION_ATTEMPT_VERSION: u16 = 1;
pub const PARTITION_WORKER_RESULT_VERSION: u16 = 1;
pub const PORTABLE_SOURCE_POSITION_VERSION: u16 = 1;
pub const PORTABLE_CHECKPOINT_STATE_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCompatibility {
    pub cdf_version: String,
    pub artifact_version: String,
    pub arrow_version: String,
    pub relational_engine: WorkerComponentVersion,
    pub normalizer_version: String,
}

impl WorkerCompatibility {
    pub fn validate(&self) -> Result<()> {
        validate_token("CDF version", &self.cdf_version)?;
        validate_token("artifact version", &self.artifact_version)?;
        validate_token("Arrow version", &self.arrow_version)?;
        self.relational_engine.validate()?;
        validate_token("normalizer version", &self.normalizer_version)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerComponentVersion {
    pub component: String,
    pub version: String,
}

impl WorkerComponentVersion {
    pub fn validate(&self) -> Result<()> {
        validate_token("worker component", &self.component)?;
        validate_token("worker component version", &self.version)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerArtifactKind {
    ProjectPlan,
    CompiledSourcePlan,
    PartitionPlan,
    OutputSchema,
    ValidationProgram,
    NormalizationPolicy,
    CompiledExpressionPlan,
    OperatorGraph,
    SegmentationPolicy,
    ExecutionExtent,
    DecodeUnitPlan,
    SegmentPlan,
    InputPayload,
    ForeignState,
    CanonicalSegment,
    Quarantine,
    Residual,
    Verdict,
    Lineage,
}

impl WorkerArtifactKind {
    fn is_worker_output(self) -> bool {
        matches!(
            self,
            Self::CanonicalSegment
                | Self::Quarantine
                | Self::Residual
                | Self::Verdict
                | Self::Lineage
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerArtifactReference {
    pub kind: WorkerArtifactKind,
    pub store_namespace: ContentStoreNamespace,
    pub object_key: ContentObjectKey,
    pub byte_count: u64,
    pub content_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_generation: Option<ContentProviderGeneration>,
}

impl WorkerArtifactReference {
    pub fn validate(&self) -> Result<()> {
        ContentStoreNamespace::new(self.store_namespace.as_str())?;
        ContentObjectKey::new(self.object_key.as_str())?;
        validate_object_key(self.object_key.as_str())?;
        validate_sha256("portable worker artifact", &self.content_sha256)?;
        if let Some(generation) = &self.provider_generation {
            ContentProviderGeneration::new(generation.as_str())?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableSourceBinding {
    pub driver_id: SourceDriverId,
    pub driver_version: String,
    pub option_schema_hash: String,
    pub compiled_source_plan: WorkerArtifactReference,
    pub redacted_options_hash: String,
    pub physical_plan_hash: String,
    pub source_semantics_hash: String,
    pub execution_capabilities_hash: String,
}

impl PortableSourceBinding {
    pub fn validate(&self) -> Result<()> {
        SourceDriverId::new(self.driver_id.as_str())?;
        validate_token("source driver version", &self.driver_version)?;
        validate_sha256("source option schema", &self.option_schema_hash)?;
        validate_artifact_kind(
            &self.compiled_source_plan,
            WorkerArtifactKind::CompiledSourcePlan,
            "portable source binding",
        )?;
        validate_sha256("compiled source options", &self.redacted_options_hash)?;
        validate_sha256("compiled source physical plan", &self.physical_plan_hash)?;
        validate_sha256("compiled source semantics", &self.source_semantics_hash)?;
        validate_sha256(
            "compiled source execution capabilities",
            &self.execution_capabilities_hash,
        )
    }

    pub fn validate_reconstructed(&self, plan: &crate::CompiledSourcePlan) -> Result<()> {
        self.validate()?;
        plan.validate()?;
        if plan.driver.driver_id != self.driver_id
            || plan.driver.driver_version != self.driver_version
            || plan.driver.option_schema_hash != self.option_schema_hash
            || artifact_hash(plan)? != self.compiled_source_plan.content_sha256
            || plan.redacted_options_hash != self.redacted_options_hash
            || plan.physical_plan_hash != self.physical_plan_hash
            || plan.schema_binding_stable_hash()? != self.source_semantics_hash
            || artifact_hash(&plan.execution_capabilities)? != self.execution_capabilities_hash
        {
            return Err(CdfError::contract(
                "reconstructed compiled source plan does not match portable task authority",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortablePartitionBinding {
    pub partition_id: PartitionId,
    pub scope: ScopeKey,
    pub canonical_partition_ordinal: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch_ordinal: Option<u64>,
    pub partition_plan: WorkerArtifactReference,
    pub source_identity_hash: String,
    pub unit_authority_hash: String,
    pub segment_authority_hash: String,
}

impl PortablePartitionBinding {
    pub fn validate(&self) -> Result<()> {
        PartitionId::new(self.partition_id.as_str())?;
        validate_portable_scope(&self.scope)?;
        validate_artifact_kind(
            &self.partition_plan,
            WorkerArtifactKind::PartitionPlan,
            "portable partition binding",
        )?;
        validate_sha256("partition source identity", &self.source_identity_hash)?;
        validate_sha256("partition unit authority", &self.unit_authority_hash)?;
        validate_sha256("partition segment authority", &self.segment_authority_hash)
    }

    pub fn validate_reconstructed(&self, plan: &cdf_kernel::PartitionPlan) -> Result<()> {
        self.validate()?;
        plan.scan_intent.validate()?;
        plan.planned_file()?;
        if plan.partition_id != self.partition_id
            || plan.scope != self.scope
            || artifact_hash(plan)? != self.partition_plan.content_sha256
            || partition_source_identity_binding(plan)? != self.source_identity_hash
        {
            return Err(CdfError::contract(
                "reconstructed partition plan does not match portable task authority",
            ));
        }
        validate_portable_scope(&plan.scope)?;
        for value in plan.metadata.values() {
            validate_no_absolute_coordinator_path(value)?;
        }
        validate_inline_source_position(plan.planned_position.as_ref())?;
        validate_inline_source_position(plan.start_position.as_ref())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerExecutionArtifacts {
    pub project_plan: WorkerArtifactReference,
    pub output_schema: WorkerArtifactReference,
    pub validation_program: WorkerArtifactReference,
    pub normalization_policy: WorkerArtifactReference,
    pub compiled_expression_plan: WorkerArtifactReference,
    pub operator_graph: WorkerArtifactReference,
    pub segmentation_policy: WorkerArtifactReference,
    pub execution_extent: WorkerArtifactReference,
    pub decode_unit_plan: WorkerArtifactReference,
    pub segment_plan: WorkerArtifactReference,
}

impl WorkerExecutionArtifacts {
    pub fn validate(&self) -> Result<()> {
        for (reference, kind, label) in [
            (
                &self.project_plan,
                WorkerArtifactKind::ProjectPlan,
                "project plan",
            ),
            (
                &self.output_schema,
                WorkerArtifactKind::OutputSchema,
                "output schema",
            ),
            (
                &self.validation_program,
                WorkerArtifactKind::ValidationProgram,
                "validation program",
            ),
            (
                &self.normalization_policy,
                WorkerArtifactKind::NormalizationPolicy,
                "normalization policy",
            ),
            (
                &self.compiled_expression_plan,
                WorkerArtifactKind::CompiledExpressionPlan,
                "compiled expression plan",
            ),
            (
                &self.operator_graph,
                WorkerArtifactKind::OperatorGraph,
                "operator graph",
            ),
            (
                &self.segmentation_policy,
                WorkerArtifactKind::SegmentationPolicy,
                "segmentation policy",
            ),
            (
                &self.execution_extent,
                WorkerArtifactKind::ExecutionExtent,
                "execution extent",
            ),
            (
                &self.decode_unit_plan,
                WorkerArtifactKind::DecodeUnitPlan,
                "decode unit plan",
            ),
            (
                &self.segment_plan,
                WorkerArtifactKind::SegmentPlan,
                "segment plan",
            ),
        ] {
            validate_artifact_kind(reference, kind, label)?;
        }
        let references = self.references();
        if references.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(CdfError::contract(
                "portable execution artifact references must be in canonical typed-reference order",
            ));
        }
        Ok(())
    }

    pub fn references(&self) -> [&WorkerArtifactReference; 10] {
        [
            &self.project_plan,
            &self.output_schema,
            &self.validation_program,
            &self.normalization_policy,
            &self.compiled_expression_plan,
            &self.operator_graph,
            &self.segmentation_policy,
            &self.execution_extent,
            &self.decode_unit_plan,
            &self.segment_plan,
        ]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableExecutionBinding {
    pub project_identity_hash: String,
    pub artifacts: WorkerExecutionArtifacts,
    pub output_schema_hash: SchemaHash,
    pub validation_program_hash: String,
    pub normalization_policy_hash: String,
    pub compiled_expression_plan_hash: String,
    pub operator_graph_hash: String,
    pub segmentation_policy_hash: String,
    pub execution_extent_hash: String,
}

impl PortableExecutionBinding {
    pub fn validate(&self) -> Result<()> {
        validate_sha256("project identity", &self.project_identity_hash)?;
        self.artifacts.validate()?;
        validate_sha256("output schema", self.output_schema_hash.as_str())?;
        validate_sha256("validation program", &self.validation_program_hash)?;
        validate_sha256("normalization policy", &self.normalization_policy_hash)?;
        validate_sha256(
            "compiled expression plan",
            &self.compiled_expression_plan_hash,
        )?;
        validate_sha256("operator graph", &self.operator_graph_hash)?;
        validate_sha256("segmentation policy", &self.segmentation_policy_hash)?;
        validate_sha256("execution extent", &self.execution_extent_hash)
    }

    fn validate_reconstructed(&self, authority: &ReconstructedExecutionAuthority) -> Result<()> {
        self.validate()?;
        authority.validate()?;
        if self.project_identity_hash != authority.project_identity_hash
            || self.output_schema_hash != authority.output_schema_hash
            || self.validation_program_hash != authority.validation_program_hash
            || self.normalization_policy_hash != authority.normalization_policy_hash
            || self.compiled_expression_plan_hash != authority.compiled_expression_plan_hash
            || self.operator_graph_hash != authority.operator_graph_hash
            || self.segmentation_policy_hash != authority.segmentation_policy_hash
            || self.execution_extent_hash != authority.execution_extent_hash
        {
            return Err(CdfError::contract(
                "reconstructed execution program does not match portable task authority",
            ));
        }
        Ok(())
    }
}

/// Semantic authority reconstructed from the typed compiler artifacts named by a task.
///
/// A worker host creates this value only after reading and validating each referenced artifact.
/// It is deliberately engine-neutral: the compiler/engine owns decoding its artifacts, while this
/// protocol owns proving that execution and coordinator admission used the same frozen semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconstructedExecutionAuthority {
    project_identity_hash: String,
    output_schema_hash: SchemaHash,
    validation_program_hash: String,
    normalization_policy_hash: String,
    compiled_expression_plan_hash: String,
    operator_graph_hash: String,
    segmentation_policy_hash: String,
    execution_extent_hash: String,
    unit_authority_hash: String,
    segment_authority_hash: String,
}

impl ReconstructedExecutionAuthority {
    /// Constructs execution authority from identities decoded from the verified compiler
    /// artifacts. This constructor is intentionally used only by a trusted artifact resolver;
    /// coordinator admission obtains the value through `WorkerAdmissionVerifier` and never from
    /// a worker-authored result.
    #[allow(clippy::too_many_arguments)]
    pub fn from_verified_compiler_artifacts(
        project_identity_hash: String,
        output_schema_hash: SchemaHash,
        validation_program_hash: String,
        normalization_policy_hash: String,
        compiled_expression_plan_hash: String,
        operator_graph_hash: String,
        segmentation_policy_hash: String,
        execution_extent_hash: String,
        unit_authority_hash: String,
        segment_authority_hash: String,
    ) -> Result<Self> {
        let authority = Self {
            project_identity_hash,
            output_schema_hash,
            validation_program_hash,
            normalization_policy_hash,
            compiled_expression_plan_hash,
            operator_graph_hash,
            segmentation_policy_hash,
            execution_extent_hash,
            unit_authority_hash,
            segment_authority_hash,
        };
        authority.validate()?;
        Ok(authority)
    }

    pub fn validate(&self) -> Result<()> {
        validate_sha256(
            "reconstructed project identity",
            &self.project_identity_hash,
        )?;
        validate_sha256(
            "reconstructed output schema",
            self.output_schema_hash.as_str(),
        )?;
        validate_sha256(
            "reconstructed validation program",
            &self.validation_program_hash,
        )?;
        validate_sha256(
            "reconstructed normalization policy",
            &self.normalization_policy_hash,
        )?;
        validate_sha256(
            "reconstructed compiled expression plan",
            &self.compiled_expression_plan_hash,
        )?;
        validate_sha256("reconstructed operator graph", &self.operator_graph_hash)?;
        validate_sha256(
            "reconstructed segmentation policy",
            &self.segmentation_policy_hash,
        )?;
        validate_sha256(
            "reconstructed execution extent",
            &self.execution_extent_hash,
        )?;
        validate_sha256("reconstructed unit authority", &self.unit_authority_hash)?;
        validate_sha256(
            "reconstructed segment authority",
            &self.segment_authority_hash,
        )
    }

    pub fn output_schema_hash(&self) -> &SchemaHash {
        &self.output_schema_hash
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerPosition {
    Inline {
        position: SourcePosition,
    },
    ExternalForeign {
        version: u16,
        protocol: String,
        artifact: WorkerArtifactReference,
    },
}

impl WorkerPosition {
    pub fn inline(position: SourcePosition) -> Result<Self> {
        let position = Self::Inline { position };
        position.validate()?;
        Ok(position)
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Inline { position } => validate_inline_source_position(Some(position)),
            Self::ExternalForeign {
                version,
                protocol,
                artifact,
            } => {
                validate_exact_version("external foreign position", *version)?;
                validate_token("external foreign position protocol", protocol)?;
                validate_artifact_kind(
                    artifact,
                    WorkerArtifactKind::ForeignState,
                    "external foreign position",
                )
            }
        }
    }

    fn artifact(&self) -> Option<&WorkerArtifactReference> {
        match self {
            Self::Inline { .. } => None,
            Self::ExternalForeign { artifact, .. } => Some(artifact),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerInputCheckpointBinding {
    pub checkpoint_id: CheckpointId,
    pub scope: ScopeKey,
    pub state_version: u16,
    pub position: WorkerPosition,
    pub content_sha256: String,
}

impl WorkerInputCheckpointBinding {
    pub fn validate(&self) -> Result<()> {
        CheckpointId::new(self.checkpoint_id.as_str())?;
        validate_portable_scope(&self.scope)?;
        if self.state_version != PORTABLE_CHECKPOINT_STATE_VERSION {
            return Err(CdfError::contract(format!(
                "portable input checkpoint state version {} is unsupported; expected {}",
                self.state_version, PORTABLE_CHECKPOINT_STATE_VERSION
            )));
        }
        self.position.validate()?;
        validate_sha256("input checkpoint", &self.content_sha256)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerControlBudget {
    pub maximum_task_bytes: u64,
    pub maximum_attempt_bytes: u64,
    pub maximum_result_bytes: u64,
    pub maximum_input_artifacts: u32,
    pub maximum_output_artifacts: u32,
    pub maximum_secret_references: u32,
}

impl WorkerControlBudget {
    pub fn validate(&self) -> Result<()> {
        if self.maximum_task_bytes == 0
            || self.maximum_attempt_bytes == 0
            || self.maximum_result_bytes == 0
            || self.maximum_input_artifacts == 0
            || self.maximum_output_artifacts == 0
        {
            return Err(CdfError::contract(
                "portable worker control-message budgets must be nonzero except for secret references",
            ));
        }
        Ok(())
    }

    fn validate_within(&self, ceiling: &Self) -> Result<()> {
        self.validate()?;
        ceiling.validate()?;
        if self.maximum_task_bytes > ceiling.maximum_task_bytes
            || self.maximum_attempt_bytes > ceiling.maximum_attempt_bytes
            || self.maximum_result_bytes > ceiling.maximum_result_bytes
            || self.maximum_input_artifacts > ceiling.maximum_input_artifacts
            || self.maximum_output_artifacts > ceiling.maximum_output_artifacts
            || self.maximum_secret_references > ceiling.maximum_secret_references
        {
            return Err(CdfError::contract(
                "portable task control budget exceeds this worker's externally configured ceiling",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResourceBudget {
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub cpu_slots: u16,
    pub io_slots: u16,
    pub control: WorkerControlBudget,
}

impl WorkerResourceBudget {
    pub fn validate(&self) -> Result<()> {
        if self.memory_bytes == 0 || self.cpu_slots == 0 || self.io_slots == 0 {
            return Err(CdfError::contract(
                "portable worker resource budget requires nonzero memory, CPU, and I/O slots",
            ));
        }
        self.control.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerAttemptPolicy {
    pub maximum_attempts: u16,
    pub maximum_attempt_duration_ms: u64,
}

impl WorkerAttemptPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.maximum_attempts == 0 || self.maximum_attempt_duration_ms == 0 {
            return Err(CdfError::contract(
                "portable worker attempt policy requires nonzero attempts and duration",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCapabilityRequirements {
    pub required_blocking_lanes: Vec<BlockingLaneSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<String>,
}

impl WorkerCapabilityRequirements {
    pub fn validate(&self) -> Result<()> {
        let mut lanes = BTreeSet::new();
        for lane in &self.required_blocking_lanes {
            lane.validate()?;
            if lane.binding == BlockingLaneBinding::RuntimeResolved {
                return Err(CdfError::contract(format!(
                    "portable worker requirement `{}` contains a runtime-resolved lane",
                    lane.lane_id
                )));
            }
            if !lanes.insert(lane.lane_id.as_str()) {
                return Err(CdfError::contract(
                    "portable worker blocking-lane requirements must be unique",
                ));
            }
        }
        if self
            .required_blocking_lanes
            .windows(2)
            .any(|pair| pair[0].lane_id >= pair[1].lane_id)
        {
            return Err(CdfError::contract(
                "portable worker blocking-lane requirements must be sorted by id",
            ));
        }
        validate_sorted_unique_tokens("portable worker service", &self.services)
    }

    pub fn validate_worker(
        &self,
        budget: &WorkerResourceBudget,
        worker: &WorkerRuntimeCapabilities,
    ) -> Result<()> {
        self.validate()?;
        budget.validate()?;
        worker.validate()?;
        budget.control.validate_within(&worker.control)?;
        if worker.host.logical_cpu_slots < budget.cpu_slots
            || worker.host.io_workers < budget.io_slots
            || worker.memory_bytes < budget.memory_bytes
            || worker.disk_bytes < budget.disk_bytes
        {
            return Err(CdfError::contract(
                "execution host does not satisfy portable worker CPU/I/O/memory/disk requirements",
            ));
        }
        for required in &self.required_blocking_lanes {
            let available = worker
                .host
                .blocking_lanes
                .iter()
                .find(|candidate| candidate.lane_id == required.lane_id)
                .ok_or_else(|| {
                    CdfError::contract(format!(
                        "execution host is missing required blocking lane `{}`",
                        required.lane_id
                    ))
                })?;
            if available.validate_tightening_of(required).is_err() {
                return Err(CdfError::contract(format!(
                    "execution host blocking lane `{}` does not satisfy the portable task requirement",
                    required.lane_id
                )));
            }
        }
        for service in &self.services {
            if worker.services.binary_search(service).is_err() {
                return Err(CdfError::contract(format!(
                    "execution worker is missing required service `{service}`"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRuntimeCapabilities {
    pub host: ExecutionHostCapabilities,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub control: WorkerControlBudget,
    pub services: Vec<String>,
}

impl WorkerRuntimeCapabilities {
    pub fn validate(&self) -> Result<()> {
        self.host.validate()?;
        if self.memory_bytes == 0 {
            return Err(CdfError::contract(
                "execution worker must declare nonzero admitted memory",
            ));
        }
        self.control.validate()?;
        validate_sorted_unique_tokens("execution worker service", &self.services)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerOutputPolicy {
    pub allowed_kinds: Vec<WorkerArtifactKind>,
    pub maximum_artifact_bytes: u64,
}

impl WorkerOutputPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.allowed_kinds.is_empty() || self.maximum_artifact_bytes == 0 {
            return Err(CdfError::contract(
                "portable worker output policy requires artifact kinds and a nonzero byte budget",
            ));
        }
        if self.allowed_kinds.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(CdfError::contract(
                "portable worker output artifact kinds must be sorted and unique",
            ));
        }
        if self
            .allowed_kinds
            .iter()
            .any(|kind| !kind.is_worker_output())
        {
            return Err(CdfError::contract(
                "portable worker output policy cannot authorize input/control artifact kinds",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedPortablePartitionTask", deny_unknown_fields)]
pub struct PortablePartitionTask {
    pub version: u16,
    pub compatibility: WorkerCompatibility,
    pub pipeline_id: PipelineId,
    pub resource_id: ResourceId,
    pub plan_id: PlanId,
    pub source: PortableSourceBinding,
    pub partition: PortablePartitionBinding,
    pub execution: PortableExecutionBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_checkpoint: Option<WorkerInputCheckpointBinding>,
    pub secret_references: Vec<SecretReference>,
    pub input_artifacts: Vec<WorkerArtifactReference>,
    pub resources: WorkerResourceBudget,
    pub attempt_policy: WorkerAttemptPolicy,
    pub capabilities: WorkerCapabilityRequirements,
    pub output_policy: WorkerOutputPolicy,
    pub task_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedPortablePartitionTask {
    version: u16,
    compatibility: WorkerCompatibility,
    pipeline_id: PipelineId,
    resource_id: ResourceId,
    plan_id: PlanId,
    source: PortableSourceBinding,
    partition: PortablePartitionBinding,
    execution: PortableExecutionBinding,
    input_checkpoint: Option<WorkerInputCheckpointBinding>,
    secret_references: Vec<SecretReference>,
    input_artifacts: Vec<WorkerArtifactReference>,
    resources: WorkerResourceBudget,
    attempt_policy: WorkerAttemptPolicy,
    capabilities: WorkerCapabilityRequirements,
    output_policy: WorkerOutputPolicy,
    task_sha256: String,
}

impl TryFrom<UncheckedPortablePartitionTask> for PortablePartitionTask {
    type Error = CdfError;

    fn try_from(value: UncheckedPortablePartitionTask) -> Result<Self> {
        let task = Self {
            version: value.version,
            compatibility: value.compatibility,
            pipeline_id: value.pipeline_id,
            resource_id: value.resource_id,
            plan_id: value.plan_id,
            source: value.source,
            partition: value.partition,
            execution: value.execution,
            input_checkpoint: value.input_checkpoint,
            secret_references: value.secret_references,
            input_artifacts: value.input_artifacts,
            resources: value.resources,
            attempt_policy: value.attempt_policy,
            capabilities: value.capabilities,
            output_policy: value.output_policy,
            task_sha256: value.task_sha256,
        };
        task.validate()?;
        Ok(task)
    }
}

#[derive(Clone, Debug)]
pub struct PortablePartitionTaskInput {
    pub compatibility: WorkerCompatibility,
    pub pipeline_id: PipelineId,
    pub resource_id: ResourceId,
    pub plan_id: PlanId,
    pub source: PortableSourceBinding,
    pub partition: PortablePartitionBinding,
    pub execution: PortableExecutionBinding,
    pub input_checkpoint: Option<WorkerInputCheckpointBinding>,
    pub secret_references: Vec<SecretReference>,
    pub input_artifacts: Vec<WorkerArtifactReference>,
    pub resources: WorkerResourceBudget,
    pub attempt_policy: WorkerAttemptPolicy,
    pub capabilities: WorkerCapabilityRequirements,
    pub output_policy: WorkerOutputPolicy,
}

#[derive(Clone, Debug)]
pub struct ReconstructedWorkerTaskAuthority {
    source: crate::CompiledSourcePlan,
    partition: cdf_kernel::PartitionPlan,
    execution: ReconstructedExecutionAuthority,
}

impl ReconstructedWorkerTaskAuthority {
    /// Constructs one indivisible authority value from artifacts decoded and content-verified by
    /// the worker host. The protocol validates every field against the task immediately after the
    /// verifier returns it.
    pub fn from_verified_artifacts(
        source: crate::CompiledSourcePlan,
        partition: cdf_kernel::PartitionPlan,
        execution: ReconstructedExecutionAuthority,
    ) -> Self {
        Self {
            source,
            partition,
            execution,
        }
    }

    pub fn source(&self) -> &crate::CompiledSourcePlan {
        &self.source
    }

    pub fn partition(&self) -> &cdf_kernel::PartitionPlan {
        &self.partition
    }

    pub fn execution(&self) -> &ReconstructedExecutionAuthority {
        &self.execution
    }
}

/// Facts observed by a trusted content-store implementation while verifying one referenced
/// artifact. The reference is repeated so admission can prove the facts belong to exactly the
/// object named by the task or result, rather than another object with convenient metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedWorkerArtifactFacts {
    reference: WorkerArtifactReference,
    row_count: Option<u64>,
}

impl VerifiedWorkerArtifactFacts {
    pub fn new(reference: WorkerArtifactReference, row_count: Option<u64>) -> Result<Self> {
        reference.validate()?;
        if reference.kind == WorkerArtifactKind::CanonicalSegment && row_count == Some(0) {
            return Err(CdfError::contract(
                "verified canonical segment must contain at least one row",
            ));
        }
        Ok(Self {
            reference,
            row_count,
        })
    }

    fn validate_for(&self, reference: &WorkerArtifactReference) -> Result<()> {
        if &self.reference != reference {
            return Err(CdfError::contract(
                "verified artifact facts do not belong to the referenced object",
            ));
        }
        Ok(())
    }

    fn row_count(&self) -> Option<u64> {
        self.row_count
    }
}

/// Source facts independently observed by the source authority. Result counters and attestations
/// are compared with these values; agreement among worker-authored claims is never sufficient.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedWorkerSourceFacts {
    processed_position: WorkerPosition,
    physical_schema_hash: SchemaHash,
    input_rows: u64,
    source_bytes: u64,
}

impl VerifiedWorkerSourceFacts {
    pub fn new(
        processed_position: WorkerPosition,
        physical_schema_hash: SchemaHash,
        input_rows: u64,
        source_bytes: u64,
    ) -> Result<Self> {
        processed_position.validate()?;
        validate_sha256(
            "verified worker physical schema",
            physical_schema_hash.as_str(),
        )?;
        Ok(Self {
            processed_position,
            physical_schema_hash,
            input_rows,
            source_bytes,
        })
    }
}

/// Host-injected verification for content-store facts and source-specific admission semantics.
/// Implementations MUST decode compiler artifacts from their verified bytes in
/// `reconstruct_task_authority`, MUST verify actual object bytes/generation in `verify_artifact`,
/// and MUST independently observe source counts/positions/schema in `verify_source_authority`.
/// The worker result itself never gains any of this power.
pub trait WorkerAdmissionVerifier {
    fn reconstruct_task_authority(
        &self,
        task: &PortablePartitionTask,
    ) -> Result<ReconstructedWorkerTaskAuthority>;

    fn verify_artifact(
        &self,
        reference: &WorkerArtifactReference,
    ) -> Result<VerifiedWorkerArtifactFacts>;

    fn verify_source_authority(
        &self,
        task: &PortablePartitionTask,
        authority: &ReconstructedWorkerTaskAuthority,
        attestation: &WorkerSourceAttestation,
        observations: &[WorkerProcessedObservation],
    ) -> Result<VerifiedWorkerSourceFacts>;
}

impl PortablePartitionTask {
    pub fn new(input: PortablePartitionTaskInput) -> Result<Self> {
        let mut task = Self {
            version: PORTABLE_PARTITION_TASK_VERSION,
            compatibility: input.compatibility,
            pipeline_id: input.pipeline_id,
            resource_id: input.resource_id,
            plan_id: input.plan_id,
            source: input.source,
            partition: input.partition,
            execution: input.execution,
            input_checkpoint: input.input_checkpoint,
            secret_references: input.secret_references,
            input_artifacts: input.input_artifacts,
            resources: input.resources,
            attempt_policy: input.attempt_policy,
            capabilities: input.capabilities,
            output_policy: input.output_policy,
            task_sha256: String::new(),
        };
        task.task_sha256 = task.compute_hash()?;
        task.validate()?;
        Ok(task)
    }

    pub fn decode_bounded(
        bytes: &[u8],
        compatibility: &WorkerCompatibility,
        worker: &WorkerRuntimeCapabilities,
    ) -> Result<Self> {
        worker.validate()?;
        validate_raw_size(
            "portable partition task",
            bytes,
            worker.control.maximum_task_bytes,
        )?;
        let task: Self = serde_json::from_slice(bytes).map_err(|error| {
            CdfError::contract(format!("decode portable partition task: {error}"))
        })?;
        task.validate_for_worker(compatibility, worker)?;
        Ok(task)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != PORTABLE_PARTITION_TASK_VERSION {
            return Err(CdfError::contract(format!(
                "portable partition task version {} is unsupported",
                self.version
            )));
        }
        self.compatibility.validate()?;
        PipelineId::new(self.pipeline_id.as_str())?;
        ResourceId::new(self.resource_id.as_str())?;
        PlanId::new(self.plan_id.as_str())?;
        self.source.validate()?;
        self.partition.validate()?;
        self.execution.validate()?;
        if let Some(checkpoint) = &self.input_checkpoint {
            checkpoint.validate()?;
        }
        self.resources.validate()?;
        self.attempt_policy.validate()?;
        self.capabilities.validate()?;
        self.output_policy.validate()?;
        validate_sorted_unique_secret_references(&self.secret_references)?;
        validate_sorted_unique_artifacts("portable task input", &self.input_artifacts)?;
        if self
            .input_artifacts
            .iter()
            .any(|artifact| artifact.kind.is_worker_output())
        {
            return Err(CdfError::contract(
                "portable task input artifact list cannot contain worker output kinds",
            ));
        }
        let control_artifacts = 2_usize
            .saturating_add(self.execution.artifacts.references().len())
            .saturating_add(
                self.input_checkpoint
                    .as_ref()
                    .and_then(|value| value.position.artifact())
                    .is_some() as usize,
            );
        if self.input_artifacts.len().saturating_add(control_artifacts)
            > usize::try_from(self.resources.control.maximum_input_artifacts).unwrap_or(usize::MAX)
            || self.secret_references.len()
                > usize::try_from(self.resources.control.maximum_secret_references)
                    .unwrap_or(usize::MAX)
        {
            return Err(CdfError::contract(
                "portable partition task exceeds its control metadata reference budget",
            ));
        }
        if self.task_sha256 != self.compute_hash()? {
            return Err(CdfError::contract(
                "portable partition task digest does not match its canonical payload",
            ));
        }
        validate_encoded_size(
            "portable partition task",
            self,
            self.resources.control.maximum_task_bytes,
        )
    }

    pub fn validate_for_worker(
        &self,
        compatibility: &WorkerCompatibility,
        worker: &WorkerRuntimeCapabilities,
    ) -> Result<()> {
        self.validate()?;
        compatibility.validate()?;
        if &self.compatibility != compatibility {
            return Err(CdfError::contract(
                "portable partition task compatibility tuple is unsupported by this worker",
            ));
        }
        self.capabilities.validate_worker(&self.resources, worker)
    }

    pub fn reconstruct_and_validate_authority(
        &self,
        registry: &crate::SourceRegistry,
        verifier: &dyn WorkerAdmissionVerifier,
    ) -> Result<ReconstructedWorkerTaskAuthority> {
        self.validate()?;
        registry.validate_portable_source_binding(&self.source)?;
        let authority = verifier.reconstruct_task_authority(self)?;
        self.source.validate_reconstructed(authority.source())?;
        self.partition
            .validate_reconstructed(authority.partition())?;
        self.execution
            .validate_reconstructed(authority.execution())?;
        registry.validate_portable_source_plan(&self.source, authority.source())?;
        if authority.source().descriptor.resource_id != self.resource_id
            || self.partition.unit_authority_hash != authority.execution().unit_authority_hash
            || self.partition.segment_authority_hash != authority.execution().segment_authority_hash
        {
            return Err(CdfError::contract(
                "reconstructed source/partition execution authority does not match portable task",
            ));
        }
        let mut observed_secrets = BTreeSet::new();
        collect_secret_references(&authority.source().redacted_options, &mut observed_secrets)?;
        collect_secret_references(&authority.source().physical_plan, &mut observed_secrets)?;
        if observed_secrets.into_iter().collect::<Vec<_>>() != self.secret_references {
            return Err(CdfError::contract(
                "portable task secret references do not exactly match reconstructed source authority",
            ));
        }
        for reference in self.non_compiler_input_artifacts() {
            verifier
                .verify_artifact(reference)?
                .validate_for(reference)?;
        }
        Ok(authority)
    }

    fn non_compiler_input_artifacts(&self) -> Vec<&WorkerArtifactReference> {
        let mut references = Vec::new();
        if let Some(reference) = self
            .input_checkpoint
            .as_ref()
            .and_then(|value| value.position.artifact())
        {
            references.push(reference);
        }
        references.extend(&self.input_artifacts);
        references
    }

    fn compute_hash(&self) -> Result<String> {
        artifact_hash(&(
            self.version,
            &self.compatibility,
            &self.pipeline_id,
            &self.resource_id,
            &self.plan_id,
            &self.source,
            &self.partition,
            &self.execution,
            &self.input_checkpoint,
            &self.secret_references,
            &self.input_artifacts,
            &self.resources,
            &self.attempt_policy,
            &self.capabilities,
            &self.output_policy,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerArtifactWriteScope {
    pub store_namespace: ContentStoreNamespace,
    pub object_key_prefix: String,
    pub maximum_bytes: u64,
}

impl WorkerArtifactWriteScope {
    pub fn validate(&self) -> Result<()> {
        ContentStoreNamespace::new(self.store_namespace.as_str())?;
        validate_object_key_prefix(&self.object_key_prefix)?;
        if self.maximum_bytes == 0 {
            return Err(CdfError::contract(
                "portable worker artifact write scope requires a nonzero byte ceiling",
            ));
        }
        Ok(())
    }

    fn admits(&self, reference: &WorkerArtifactReference) -> bool {
        reference.store_namespace == self.store_namespace
            && reference
                .object_key
                .as_str()
                .starts_with(&self.object_key_prefix)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerObjectGenerationPrecondition {
    CreateOnly,
    CreateOrVerifyContent,
    MatchGeneration {
        generation: ContentProviderGeneration,
    },
}

impl WorkerObjectGenerationPrecondition {
    fn validate(&self) -> Result<()> {
        if let Self::MatchGeneration { generation } = self {
            ContentProviderGeneration::new(generation.as_str())?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerArtifactObjectState {
    Absent,
    Present {
        content_sha256: String,
        provider_generation: ContentProviderGeneration,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerArtifactWritePermit {
    pub task_sha256: String,
    pub lease_authority_domain_id: LeaseAuthorityDomainId,
    pub lease_scope: ScopeKey,
    pub fencing_token: FencingToken,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
    pub output: WorkerArtifactWriteScope,
    pub generation_precondition: WorkerObjectGenerationPrecondition,
}

impl WorkerArtifactWritePermit {
    pub fn validate(&self) -> Result<()> {
        validate_sha256("worker write permit task", &self.task_sha256)?;
        LeaseAuthorityDomainId::new(self.lease_authority_domain_id.as_str())?;
        validate_portable_scope(&self.lease_scope)?;
        FencingToken::new(self.fencing_token.get())?;
        if self.issued_at_ms < 0 || self.expires_at_ms <= self.issued_at_ms {
            return Err(CdfError::contract(
                "worker write permit expiry must follow nonnegative issuance",
            ));
        }
        self.output.validate()?;
        self.generation_precondition.validate()
    }

    fn validate_reference_before_write(
        &self,
        task: &PortablePartitionTask,
        current_lease: &WorkerLeaseState,
        reference: &WorkerArtifactReference,
        object_state: &WorkerArtifactObjectState,
        now_ms: i64,
    ) -> Result<()> {
        self.validate()?;
        task.validate()?;
        current_lease.validate()?;
        reference.validate()?;
        if self.task_sha256 != task.task_sha256
            || self.lease_scope != task.partition.scope
            || self.output.maximum_bytes > task.output_policy.maximum_artifact_bytes
            || !task.output_policy.allowed_kinds.contains(&reference.kind)
            || !self.output.admits(reference)
            || reference.byte_count > self.output.maximum_bytes
        {
            return Err(CdfError::contract(
                "worker artifact write is outside its task, scope, kind, or byte authority",
            ));
        }
        current_lease.validate_permit(self, now_ms)?;
        match (&self.generation_precondition, object_state) {
            (WorkerObjectGenerationPrecondition::CreateOnly, WorkerArtifactObjectState::Absent)
            | (
                WorkerObjectGenerationPrecondition::CreateOrVerifyContent,
                WorkerArtifactObjectState::Absent,
            ) => Ok(()),
            (
                WorkerObjectGenerationPrecondition::CreateOrVerifyContent,
                WorkerArtifactObjectState::Present {
                    content_sha256,
                    provider_generation,
                },
            ) if content_sha256 == &reference.content_sha256
                && reference
                    .provider_generation
                    .as_ref()
                    .is_none_or(|value| value == provider_generation) =>
            {
                Ok(())
            }
            (
                WorkerObjectGenerationPrecondition::MatchGeneration { generation },
                WorkerArtifactObjectState::Present {
                    content_sha256,
                    provider_generation,
                },
            ) if generation == provider_generation
                && content_sha256 == &reference.content_sha256 =>
            {
                Ok(())
            }
            _ => Err(CdfError::contract(
                "worker artifact write violates its create/generation precondition",
            )),
        }
    }

    fn is_live_at(&self, now_ms: i64) -> bool {
        self.issued_at_ms <= now_ms && now_ms < self.expires_at_ms
    }

    fn hash(&self) -> Result<String> {
        artifact_hash(self)
    }
}

/// A sink whose mutation primitive consumes a fully validated write authorization. Implementors
/// MUST apply the generation precondition and fencing token atomically with the object mutation
/// when the backing store supports conditional writes. Provider-specific atomicity is a storage
/// substrate concern; cumulative task authority is owned by `WorkerArtifactWriteSession`.
pub trait WorkerAuthorizedArtifactSink {
    fn write_authorized(
        &mut self,
        authorization: WorkerArtifactWriteAuthorization<'_>,
    ) -> Result<VerifiedWorkerArtifactFacts>;
}

pub struct WorkerArtifactWriteAuthorization<'a> {
    permit: &'a WorkerArtifactWritePermit,
    receipt: &'a WorkerArtifactReceipt,
}

impl WorkerArtifactWriteAuthorization<'_> {
    pub fn permit(&self) -> &WorkerArtifactWritePermit {
        self.permit
    }

    pub fn receipt(&self) -> &WorkerArtifactReceipt {
        self.receipt
    }
}

/// Stateful output authority for one partition attempt. Every mutation crosses this object, so
/// byte and object ceilings are checked cumulatively before the sink can observe a write request.
#[derive(Debug)]
pub struct WorkerArtifactWriteSession<'a> {
    task: &'a PortablePartitionTask,
    permit: &'a WorkerArtifactWritePermit,
    lease: &'a WorkerLeaseState,
    artifact_count: u32,
    artifact_bytes: u64,
    artifacts: BTreeSet<WorkerArtifactReference>,
}

impl<'a> WorkerArtifactWriteSession<'a> {
    pub fn new(
        task: &'a PortablePartitionTask,
        attempt: &'a PartitionAttemptEnvelope,
        lease: &'a WorkerLeaseState,
        now_ms: i64,
    ) -> Result<Self> {
        attempt.validate_for_task(task)?;
        lease.validate_permit(&attempt.write_permit, now_ms)?;
        Ok(Self {
            task,
            permit: &attempt.write_permit,
            lease,
            artifact_count: 0,
            artifact_bytes: 0,
            artifacts: BTreeSet::new(),
        })
    }

    pub fn write(
        &mut self,
        receipt: &WorkerArtifactReceipt,
        object_state: &WorkerArtifactObjectState,
        now_ms: i64,
        sink: &mut dyn WorkerAuthorizedArtifactSink,
    ) -> Result<VerifiedWorkerArtifactFacts> {
        receipt.validate()?;
        self.permit.validate_reference_before_write(
            self.task,
            self.lease,
            &receipt.artifact,
            object_state,
            now_ms,
        )?;
        let next_count = self
            .artifact_count
            .checked_add(1)
            .ok_or_else(|| CdfError::contract("worker output artifact count overflowed u32"))?;
        let next_bytes = self
            .artifact_bytes
            .checked_add(receipt.artifact.byte_count)
            .ok_or_else(|| CdfError::contract("worker output artifact bytes overflowed u64"))?;
        if next_count > self.task.resources.control.maximum_output_artifacts
            || next_bytes > self.task.output_policy.maximum_artifact_bytes
            || next_bytes > self.permit.output.maximum_bytes
            || self.artifacts.contains(&receipt.artifact)
        {
            return Err(CdfError::contract(
                "worker artifact write exceeds cumulative count/byte authority or repeats an object",
            ));
        }

        let facts = sink.write_authorized(WorkerArtifactWriteAuthorization {
            permit: self.permit,
            receipt,
        })?;
        facts.validate_for(&receipt.artifact)?;
        if let WorkerArtifactRole::CanonicalSegment { row_count, .. } = receipt.role
            && facts.row_count() != Some(row_count)
        {
            return Err(CdfError::contract(
                "written canonical segment row count does not match its receipt",
            ));
        }
        self.artifact_count = next_count;
        self.artifact_bytes = next_bytes;
        self.artifacts.insert(receipt.artifact.clone());
        Ok(facts)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerLeaseState {
    pub lease_authority_domain_id: LeaseAuthorityDomainId,
    pub lease_scope: ScopeKey,
    pub fencing_token: FencingToken,
    pub expires_at_ms: i64,
}

impl WorkerLeaseState {
    pub fn validate(&self) -> Result<()> {
        LeaseAuthorityDomainId::new(self.lease_authority_domain_id.as_str())?;
        validate_portable_scope(&self.lease_scope)?;
        FencingToken::new(self.fencing_token.get())?;
        if self.expires_at_ms < 0 {
            return Err(CdfError::contract("worker lease expiry cannot be negative"));
        }
        Ok(())
    }

    fn validate_permit(&self, permit: &WorkerArtifactWritePermit, now_ms: i64) -> Result<()> {
        if self.lease_authority_domain_id != permit.lease_authority_domain_id
            || self.lease_scope != permit.lease_scope
            || self.fencing_token != permit.fencing_token
            || !permit.is_live_at(now_ms)
            || now_ms >= self.expires_at_ms
        {
            return Err(CdfError::contract(
                "worker write permit has an expired or stale lease domain/scope/fence",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedPartitionAttemptEnvelope", deny_unknown_fields)]
pub struct PartitionAttemptEnvelope {
    pub version: u16,
    pub attempt_id: String,
    pub retry_ordinal: u16,
    pub trace_id: String,
    pub write_permit: WorkerArtifactWritePermit,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedPartitionAttemptEnvelope {
    version: u16,
    attempt_id: String,
    retry_ordinal: u16,
    trace_id: String,
    write_permit: WorkerArtifactWritePermit,
}

impl TryFrom<UncheckedPartitionAttemptEnvelope> for PartitionAttemptEnvelope {
    type Error = CdfError;

    fn try_from(value: UncheckedPartitionAttemptEnvelope) -> Result<Self> {
        let attempt = Self {
            version: value.version,
            attempt_id: value.attempt_id,
            retry_ordinal: value.retry_ordinal,
            trace_id: value.trace_id,
            write_permit: value.write_permit,
        };
        attempt.validate()?;
        Ok(attempt)
    }
}

impl PartitionAttemptEnvelope {
    pub fn decode_bounded(
        bytes: &[u8],
        task: &PortablePartitionTask,
        worker: &WorkerRuntimeCapabilities,
    ) -> Result<Self> {
        worker.validate()?;
        task.resources.control.validate_within(&worker.control)?;
        validate_raw_size(
            "partition attempt envelope",
            bytes,
            task.resources
                .control
                .maximum_attempt_bytes
                .min(worker.control.maximum_attempt_bytes),
        )?;
        let attempt: Self = serde_json::from_slice(bytes).map_err(|error| {
            CdfError::contract(format!("decode partition attempt envelope: {error}"))
        })?;
        attempt.validate_for_task(task)?;
        Ok(attempt)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != PARTITION_ATTEMPT_VERSION {
            return Err(CdfError::contract(format!(
                "partition attempt version {} is unsupported",
                self.version
            )));
        }
        validate_token("partition attempt id", &self.attempt_id)?;
        validate_token("partition attempt trace id", &self.trace_id)?;
        self.write_permit.validate()
    }

    pub fn validate_for_task(&self, task: &PortablePartitionTask) -> Result<()> {
        self.validate()?;
        task.validate()?;
        if self.write_permit.task_sha256 != task.task_sha256
            || self.write_permit.lease_scope != task.partition.scope
        {
            return Err(CdfError::contract(
                "partition attempt does not bind the current portable task digest and scope",
            ));
        }
        if self.retry_ordinal >= task.attempt_policy.maximum_attempts
            || u64::try_from(
                self.write_permit
                    .expires_at_ms
                    .saturating_sub(self.write_permit.issued_at_ms),
            )
            .unwrap_or(u64::MAX)
                > task.attempt_policy.maximum_attempt_duration_ms
        {
            return Err(CdfError::contract(
                "partition attempt exceeds the task retry or duration authority",
            ));
        }
        if self.write_permit.output.maximum_bytes > task.output_policy.maximum_artifact_bytes {
            return Err(CdfError::contract(
                "partition attempt widens the task artifact byte authority",
            ));
        }
        validate_encoded_size(
            "partition attempt envelope",
            self,
            task.resources.control.maximum_attempt_bytes,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerArtifactRole {
    CanonicalSegment {
        segment_id: SegmentId,
        partition_ordinal: u32,
        segment_ordinal: u32,
        row_count: u64,
    },
    Quarantine,
    Residual,
    Verdict,
    Lineage,
}

impl WorkerArtifactRole {
    fn expected_kind(&self) -> WorkerArtifactKind {
        match self {
            Self::CanonicalSegment { .. } => WorkerArtifactKind::CanonicalSegment,
            Self::Quarantine => WorkerArtifactKind::Quarantine,
            Self::Residual => WorkerArtifactKind::Residual,
            Self::Verdict => WorkerArtifactKind::Verdict,
            Self::Lineage => WorkerArtifactKind::Lineage,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerArtifactReceipt {
    pub role: WorkerArtifactRole,
    pub artifact: WorkerArtifactReference,
}

impl WorkerArtifactReceipt {
    pub fn validate(&self) -> Result<()> {
        self.artifact.validate()?;
        if self.artifact.kind != self.role.expected_kind() {
            return Err(CdfError::contract(
                "worker artifact receipt role does not match its typed artifact reference",
            ));
        }
        if let WorkerArtifactRole::CanonicalSegment {
            segment_id,
            row_count,
            ..
        } = &self.role
        {
            SegmentId::new(segment_id.as_str())?;
            if *row_count == 0 {
                return Err(CdfError::contract(
                    "worker canonical segment receipt requires at least one row",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSourceAttestation {
    pub processed_position: WorkerPosition,
    pub physical_schema_hash: SchemaHash,
}

impl WorkerSourceAttestation {
    pub fn validate(&self) -> Result<()> {
        self.processed_position.validate()?;
        validate_sha256("worker physical schema", self.physical_schema_hash.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerProcessedObservation {
    pub observation_id: String,
    pub outcome: ProcessedObservationOutcome,
    pub source_position: WorkerPosition,
}

impl WorkerProcessedObservation {
    pub fn new(
        observation_id: impl Into<String>,
        outcome: ProcessedObservationOutcome,
        source_position: WorkerPosition,
    ) -> Result<Self> {
        let observation = Self {
            observation_id: observation_id.into(),
            outcome,
            source_position,
        };
        observation.validate()?;
        Ok(observation)
    }

    fn validate(&self) -> Result<()> {
        validate_token("worker processed observation id", &self.observation_id)?;
        self.source_position.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerTerminalStatus {
    Succeeded,
    Cancelled,
    Failed { error_code: String },
}

impl WorkerTerminalStatus {
    fn validate(&self) -> Result<()> {
        if let Self::Failed { error_code } = self {
            validate_token("worker terminal error code", error_code)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResultCounts {
    pub input_rows: u64,
    pub output_rows: u64,
    pub quarantined_rows: u64,
    pub source_bytes: u64,
    pub artifact_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTelemetry {
    pub elapsed_ns: u64,
    pub cpu_ns: u64,
    pub peak_memory_bytes: u64,
    pub spill_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedPartitionWorkerResult", deny_unknown_fields)]
pub struct PartitionWorkerResult {
    pub version: u16,
    pub task_sha256: String,
    pub attempt_id: String,
    pub write_permit_sha256: String,
    pub status: WorkerTerminalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_attestation: Option<WorkerSourceAttestation>,
    pub processed_observations: Vec<WorkerProcessedObservation>,
    pub artifacts: Vec<WorkerArtifactReceipt>,
    pub counts: WorkerResultCounts,
    pub telemetry: WorkerTelemetry,
    pub result_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedPartitionWorkerResult {
    version: u16,
    task_sha256: String,
    attempt_id: String,
    write_permit_sha256: String,
    status: WorkerTerminalStatus,
    source_attestation: Option<WorkerSourceAttestation>,
    processed_observations: Vec<WorkerProcessedObservation>,
    artifacts: Vec<WorkerArtifactReceipt>,
    counts: WorkerResultCounts,
    telemetry: WorkerTelemetry,
    result_sha256: String,
}

impl TryFrom<UncheckedPartitionWorkerResult> for PartitionWorkerResult {
    type Error = CdfError;

    fn try_from(value: UncheckedPartitionWorkerResult) -> Result<Self> {
        let result = Self {
            version: value.version,
            task_sha256: value.task_sha256,
            attempt_id: value.attempt_id,
            write_permit_sha256: value.write_permit_sha256,
            status: value.status,
            source_attestation: value.source_attestation,
            processed_observations: value.processed_observations,
            artifacts: value.artifacts,
            counts: value.counts,
            telemetry: value.telemetry,
            result_sha256: value.result_sha256,
        };
        result.validate()?;
        Ok(result)
    }
}

#[derive(Clone, Debug)]
pub struct PartitionWorkerResultInput {
    pub status: WorkerTerminalStatus,
    pub source_attestation: Option<WorkerSourceAttestation>,
    pub processed_observations: Vec<WorkerProcessedObservation>,
    pub artifacts: Vec<WorkerArtifactReceipt>,
    pub counts: WorkerResultCounts,
    pub telemetry: WorkerTelemetry,
}

impl PartitionWorkerResult {
    pub fn new(
        attempt: &PartitionAttemptEnvelope,
        input: PartitionWorkerResultInput,
    ) -> Result<Self> {
        let mut result = Self {
            version: PARTITION_WORKER_RESULT_VERSION,
            task_sha256: attempt.write_permit.task_sha256.clone(),
            attempt_id: attempt.attempt_id.clone(),
            write_permit_sha256: attempt.write_permit.hash()?,
            status: input.status,
            source_attestation: input.source_attestation,
            processed_observations: input.processed_observations,
            artifacts: input.artifacts,
            counts: input.counts,
            telemetry: input.telemetry,
            result_sha256: String::new(),
        };
        result.result_sha256 = result.compute_semantic_hash()?;
        result.validate()?;
        Ok(result)
    }

    pub fn decode_bounded(
        bytes: &[u8],
        task: &PortablePartitionTask,
        worker: &WorkerRuntimeCapabilities,
    ) -> Result<Self> {
        worker.validate()?;
        task.resources.control.validate_within(&worker.control)?;
        validate_raw_size(
            "partition worker result",
            bytes,
            task.resources
                .control
                .maximum_result_bytes
                .min(worker.control.maximum_result_bytes),
        )?;
        serde_json::from_slice(bytes)
            .map_err(|error| CdfError::contract(format!("decode partition worker result: {error}")))
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != PARTITION_WORKER_RESULT_VERSION {
            return Err(CdfError::contract(format!(
                "partition worker result version {} is unsupported",
                self.version
            )));
        }
        validate_sha256("partition worker task", &self.task_sha256)?;
        validate_token("partition worker attempt id", &self.attempt_id)?;
        validate_sha256("partition worker write permit", &self.write_permit_sha256)?;
        self.status.validate()?;
        if let Some(attestation) = &self.source_attestation {
            attestation.validate()?;
        }
        if matches!(self.status, WorkerTerminalStatus::Succeeded)
            && self.source_attestation.is_none()
        {
            return Err(CdfError::contract(
                "successful partition worker result requires source attestation",
            ));
        }
        if !matches!(self.status, WorkerTerminalStatus::Succeeded)
            && (!self.processed_observations.is_empty() || !self.artifacts.is_empty())
        {
            return Err(CdfError::contract(
                "non-successful partition worker result cannot advance observations or artifacts",
            ));
        }
        validate_processed_observations(&self.processed_observations)?;
        validate_worker_receipts(&self.artifacts)?;
        if self
            .counts
            .output_rows
            .saturating_add(self.counts.quarantined_rows)
            > self.counts.input_rows
        {
            return Err(CdfError::contract(
                "partition worker output and quarantine counts exceed input rows",
            ));
        }
        let artifact_bytes = self.artifacts.iter().try_fold(0_u64, |total, receipt| {
            total
                .checked_add(receipt.artifact.byte_count)
                .ok_or_else(|| CdfError::contract("worker artifact byte count overflowed u64"))
        })?;
        if artifact_bytes != self.counts.artifact_bytes {
            return Err(CdfError::contract(
                "partition worker artifact receipts do not match the reported artifact bytes",
            ));
        }
        let segment_rows = self.artifacts.iter().try_fold(0_u64, |total, receipt| {
            let rows = match receipt.role {
                WorkerArtifactRole::CanonicalSegment { row_count, .. } => row_count,
                _ => 0,
            };
            total
                .checked_add(rows)
                .ok_or_else(|| CdfError::contract("worker segment row count overflowed u64"))
        })?;
        if segment_rows != self.counts.output_rows {
            return Err(CdfError::contract(
                "worker canonical segment rows do not match reported output rows",
            ));
        }
        if self.result_sha256 != self.compute_semantic_hash()? {
            return Err(CdfError::contract(
                "partition worker result digest does not match its canonical semantic payload",
            ));
        }
        Ok(())
    }

    pub fn validate_for_admission(
        &self,
        task: &PortablePartitionTask,
        attempt: &PartitionAttemptEnvelope,
        registry: &crate::SourceRegistry,
        current_lease: &WorkerLeaseState,
        verifier: &dyn WorkerAdmissionVerifier,
        now_ms: i64,
    ) -> Result<()> {
        self.validate()?;
        attempt.validate_for_task(task)?;
        let authority = task.reconstruct_and_validate_authority(registry, verifier)?;
        current_lease.validate_permit(&attempt.write_permit, now_ms)?;
        if self.task_sha256 != task.task_sha256
            || self.attempt_id != attempt.attempt_id
            || self.write_permit_sha256 != attempt.write_permit.hash()?
        {
            return Err(CdfError::contract(
                "partition worker result has a stale or mismatched task/attempt/write permit",
            ));
        }
        if !matches!(self.status, WorkerTerminalStatus::Succeeded) {
            return Err(CdfError::data(
                "only a successful partition worker result may advance coordinator authority",
            ));
        }
        if self.artifacts.len()
            > usize::try_from(task.resources.control.maximum_output_artifacts).unwrap_or(usize::MAX)
            || self.counts.artifact_bytes > task.output_policy.maximum_artifact_bytes
            || self.counts.artifact_bytes > attempt.write_permit.output.maximum_bytes
        {
            return Err(CdfError::contract(
                "partition worker result exceeds its artifact control or byte authority",
            ));
        }
        let mut verified_output_rows = 0_u64;
        let mut verified_quarantined_rows = 0_u64;
        for receipt in &self.artifacts {
            if !task
                .output_policy
                .allowed_kinds
                .contains(&receipt.artifact.kind)
                || !attempt.write_permit.output.admits(&receipt.artifact)
            {
                return Err(CdfError::contract(
                    "partition worker result contains an unauthorized artifact reference",
                ));
            }
            if let WorkerArtifactRole::CanonicalSegment {
                partition_ordinal, ..
            } = receipt.role
                && partition_ordinal != task.partition.canonical_partition_ordinal
            {
                return Err(CdfError::contract(
                    "partition worker segment receipt exceeds its canonical partition authority",
                ));
            }
            let facts = verifier.verify_artifact(&receipt.artifact)?;
            facts.validate_for(&receipt.artifact)?;
            match receipt.role {
                WorkerArtifactRole::CanonicalSegment { row_count, .. } => {
                    let observed_rows = facts.row_count().ok_or_else(|| {
                        CdfError::contract(
                            "verified canonical segment is missing an observed row count",
                        )
                    })?;
                    if observed_rows != row_count {
                        return Err(CdfError::contract(
                            "worker segment receipt row count does not match stored content",
                        ));
                    }
                    verified_output_rows = verified_output_rows
                        .checked_add(observed_rows)
                        .ok_or_else(|| {
                            CdfError::contract("verified worker output rows overflowed u64")
                        })?;
                }
                WorkerArtifactRole::Quarantine => {
                    verified_quarantined_rows = verified_quarantined_rows
                        .checked_add(facts.row_count().unwrap_or(0))
                        .ok_or_else(|| {
                            CdfError::contract("verified worker quarantine rows overflowed u64")
                        })?;
                }
                WorkerArtifactRole::Residual
                | WorkerArtifactRole::Verdict
                | WorkerArtifactRole::Lineage => {}
            }
        }
        let attestation = self.source_attestation.as_ref().ok_or_else(|| {
            CdfError::contract("successful partition worker result lacks source attestation")
        })?;
        let source_facts = verifier.verify_source_authority(
            task,
            &authority,
            attestation,
            &self.processed_observations,
        )?;
        if source_facts.processed_position != attestation.processed_position
            || source_facts.physical_schema_hash != attestation.physical_schema_hash
            || source_facts.input_rows != self.counts.input_rows
            || source_facts.source_bytes != self.counts.source_bytes
            || verified_output_rows != self.counts.output_rows
            || verified_quarantined_rows != self.counts.quarantined_rows
        {
            return Err(CdfError::contract(
                "worker result counts or source attestation do not match independently verified facts",
            ));
        }
        validate_encoded_size(
            "partition worker result",
            self,
            task.resources.control.maximum_result_bytes,
        )
    }

    fn compute_semantic_hash(&self) -> Result<String> {
        artifact_hash(&(
            self.version,
            &self.task_sha256,
            &self.attempt_id,
            &self.write_permit_sha256,
            &self.status,
            &self.source_attestation,
            &self.processed_observations,
            &self.artifacts,
            &self.counts,
        ))
    }
}

fn validate_processed_observations(observations: &[WorkerProcessedObservation]) -> Result<()> {
    for observation in observations {
        observation.validate()?;
    }
    if observations
        .windows(2)
        .any(|pair| pair[0].observation_id >= pair[1].observation_id)
    {
        return Err(CdfError::contract(
            "partition worker processed observations must be sorted and unique",
        ));
    }
    Ok(())
}

fn validate_worker_receipts(receipts: &[WorkerArtifactReceipt]) -> Result<()> {
    if receipts
        .windows(2)
        .any(|pair| pair[0].artifact >= pair[1].artifact)
    {
        return Err(CdfError::contract(
            "partition worker artifact receipts must be in canonical typed-reference order",
        ));
    }
    let mut artifact_identities = BTreeSet::new();
    let mut segment_ordinals = BTreeSet::new();
    let mut segment_ids = BTreeSet::new();
    for receipt in receipts {
        receipt.validate()?;
        if !artifact_identities.insert(&receipt.artifact) {
            return Err(CdfError::contract(
                "partition worker result contains a duplicate artifact receipt",
            ));
        }
        if let WorkerArtifactRole::CanonicalSegment {
            segment_id,
            segment_ordinal,
            ..
        } = &receipt.role
            && (!segment_ordinals.insert(*segment_ordinal)
                || !segment_ids.insert(segment_id.as_str()))
        {
            return Err(CdfError::contract(
                "partition worker result contains conflicting canonical segment authority",
            ));
        }
    }
    if segment_ordinals
        .iter()
        .copied()
        .ne(0..u32::try_from(segment_ordinals.len()).unwrap_or(u32::MAX))
    {
        return Err(CdfError::contract(
            "partition worker canonical segment ordinals must be contiguous from zero",
        ));
    }
    Ok(())
}

fn validate_sorted_unique_artifacts(
    label: &str,
    artifacts: &[WorkerArtifactReference],
) -> Result<()> {
    for artifact in artifacts {
        artifact.validate()?;
    }
    if artifacts.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CdfError::contract(format!(
            "{label} artifact references must be sorted and unique"
        )));
    }
    Ok(())
}

fn validate_artifact_kind(
    reference: &WorkerArtifactReference,
    expected: WorkerArtifactKind,
    label: &str,
) -> Result<()> {
    reference.validate()?;
    if reference.kind != expected {
        return Err(CdfError::contract(format!(
            "{label} must reference a {expected:?} artifact"
        )));
    }
    Ok(())
}

fn validate_sorted_unique_secret_references(references: &[SecretReference]) -> Result<()> {
    for reference in references {
        SecretReference::new(reference.as_str())?;
    }
    if references.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CdfError::contract(
            "portable task secret references must be sorted and unique",
        ));
    }
    Ok(())
}

fn validate_sorted_unique_tokens(label: &str, values: &[String]) -> Result<()> {
    for value in values {
        validate_token(label, value)?;
    }
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CdfError::contract(format!(
            "{label} declarations must be sorted and unique"
        )));
    }
    Ok(())
}

fn validate_token(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'+' | b'/')
        })
    {
        return Err(CdfError::contract(format!(
            "{label} must be a safe 1..=128 character token"
        )));
    }
    Ok(())
}

fn validate_object_key_prefix(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with('/')
        || !value.ends_with('/')
        || has_unsafe_object_key_component(value)
    {
        return Err(CdfError::contract(
            "worker artifact key prefix must be a non-empty portable relative key",
        ));
    }
    Ok(())
}

fn validate_object_key(value: &str) -> Result<()> {
    if value.is_empty() || value.starts_with('/') || has_unsafe_object_key_component(value) {
        return Err(CdfError::contract(
            "worker artifact object key must be a non-empty portable relative key",
        ));
    }
    Ok(())
}

fn has_unsafe_object_key_component(value: &str) -> bool {
    value.contains(['\\', '\0']) || value.split('/').any(|component| component == "..")
}

fn validate_exact_version(label: &str, version: u16) -> Result<()> {
    if version != PORTABLE_SOURCE_POSITION_VERSION {
        return Err(CdfError::contract(format!(
            "{label} version {version} is unsupported; expected {PORTABLE_SOURCE_POSITION_VERSION}"
        )));
    }
    Ok(())
}

fn validate_inline_source_position(position: Option<&SourcePosition>) -> Result<()> {
    let Some(position) = position else {
        return Ok(());
    };
    validate_exact_version("portable source position", position.version())?;
    match position {
        SourcePosition::Cursor(CursorPosition { field, value, .. }) => {
            validate_token("cursor field", field)?;
            if let CursorValue::TimestampMicros {
                timezone: Some(value),
                ..
            } = value
            {
                validate_token("cursor timezone", value)?;
            }
        }
        SourcePosition::Log(position) => {
            validate_token("log position name", &position.log)?;
            if let Some(sequence) = &position.sequence {
                validate_token("log position sequence", sequence)?;
            }
        }
        SourcePosition::FileManifest(manifest) => {
            for file in &manifest.files {
                validate_no_absolute_coordinator_path(&file.path)?;
            }
        }
        SourcePosition::PageToken(_) => {}
        SourcePosition::Composite(composite) => {
            for (key, nested) in &composite.positions {
                validate_token("composite position key", key)?;
                validate_inline_source_position(Some(nested))?;
            }
        }
        SourcePosition::ForeignState(_) => {
            return Err(CdfError::contract(
                "portable worker positions must externalize foreign state as a typed artifact",
            ));
        }
    }
    Ok(())
}

fn validate_portable_scope(scope: &ScopeKey) -> Result<()> {
    match scope {
        ScopeKey::File { path } => validate_no_absolute_coordinator_path(path),
        ScopeKey::Composite { parts } => {
            for part in parts {
                validate_portable_scope(part)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_no_absolute_coordinator_path(value: &str) -> Result<()> {
    let lowercase = value.to_ascii_lowercase();
    let windows_absolute = value.as_bytes().get(1) == Some(&b':')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic);
    if value.starts_with('/')
        || value.starts_with('\\')
        || lowercase.starts_with("file:")
        || windows_absolute
    {
        return Err(CdfError::contract(
            "portable worker authority cannot contain an absolute coordinator file path",
        ));
    }
    Ok(())
}

fn collect_secret_references(
    value: &serde_json::Value,
    output: &mut BTreeSet<SecretReference>,
) -> Result<()> {
    match value {
        serde_json::Value::String(value) if value.starts_with("secret://") => {
            output.insert(SecretReference::new(value.clone())?);
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_secret_references(value, output)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_secret_references(value, output)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CdfError::contract(format!(
            "{label} digest must use sha256:<64 lowercase hex>"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CdfError::contract(format!(
            "{label} digest must use sha256:<64 lowercase hex>"
        )));
    }
    Ok(())
}

fn validate_raw_size(label: &str, bytes: &[u8], maximum: u64) -> Result<()> {
    let actual = u64::try_from(bytes.len())
        .map_err(|_| CdfError::contract(format!("{label} size exceeds u64")))?;
    if actual > maximum {
        return Err(CdfError::contract(format!(
            "{label} requires {actual} bytes above its externally admitted {maximum}-byte control ceiling"
        )));
    }
    Ok(())
}

fn validate_encoded_size(label: &str, value: &impl Serialize, maximum: u64) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|error| CdfError::internal(error.to_string()))?;
    let actual = u64::try_from(bytes.len())
        .map_err(|_| CdfError::contract(format!("{label} size exceeds u64")))?;
    if actual > maximum {
        return Err(CdfError::contract(format!(
            "{label} requires {actual} bytes above its {maximum}-byte control budget"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
