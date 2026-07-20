use std::{any::Any, collections::BTreeSet};

use cdf_kernel::{
    BoxFuture, CdfError, CheckpointId, ContentObjectKey, ContentProviderGeneration,
    ContentStoreNamespace, CursorPosition, CursorValue, FencingToken, LeaseAuthorityDomainId,
    PartitionId, PipelineId, PlanId, ProcessedObservationOutcome, ResourceId, Result, SchemaHash,
    ScopeKey, SecretReference, SegmentId, SourcePosition, partition_source_identity_binding,
};
use serde::{Deserialize, Serialize};

use crate::{
    BlockingLaneBinding, BlockingLaneSpec, ExecutionHostCapabilities, SourceDriverId, artifact_hash,
};

pub const PORTABLE_PARTITION_TASK_VERSION: u16 = 1;
pub const PORTABLE_SEGMENT_TASK_VERSION: u16 = 1;
pub const PARTITION_ATTEMPT_VERSION: u16 = 1;
pub const PARTITION_WORKER_RESULT_VERSION: u16 = 1;
pub const SEGMENT_WORKER_RESULT_VERSION: u16 = 1;
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
    PlannedTaskSet,
    InputPayload,
    ForeignState,
    PreparedSegment,
    CanonicalSegment,
    Quarantine,
    Residual,
    Verdict,
    Lineage,
    PartitionEvidence,
}

impl WorkerArtifactKind {
    fn is_worker_output(self) -> bool {
        matches!(
            self,
            Self::PreparedSegment
                | Self::CanonicalSegment
                | Self::Quarantine
                | Self::Residual
                | Self::Verdict
                | Self::Lineage
                | Self::PartitionEvidence
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

impl From<&cdf_kernel::PlannedTaskSetReference> for WorkerArtifactReference {
    fn from(value: &cdf_kernel::PlannedTaskSetReference) -> Self {
        Self {
            kind: WorkerArtifactKind::PlannedTaskSet,
            store_namespace: value.store_namespace.clone(),
            object_key: value.object_key.clone(),
            byte_count: value.byte_count,
            content_sha256: value.content_sha256.clone(),
            provider_generation: Some(value.provider_generation.clone()),
        }
    }
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

/// Common lease, budget, and output contract shared by portable worker task kinds.
///
/// Semantic reconstruction remains task-specific. This trait exists only so one fenced attempt
/// and cumulative artifact-write implementation can protect both partition preparation and
/// canonical segment finalization.
pub trait PortableWorkerTask: std::fmt::Debug {
    fn validate_portable(&self) -> Result<()>;
    fn task_sha256(&self) -> &str;
    fn lease_scope(&self) -> &ScopeKey;
    fn resources(&self) -> &WorkerResourceBudget;
    fn attempt_policy(&self) -> &WorkerAttemptPolicy;
    fn output_policy(&self) -> &WorkerOutputPolicy;
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

/// One portable second-barrier task that turns an admitted prepared segment into package bytes.
///
/// It intentionally carries no source driver, options, secrets, or checkpoint authority. The
/// preparation result digest and prepared artifact bind the first barrier; the dense package-row
/// prefix and compiler artifacts bind the only identity-bearing work left to perform.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedPortableSegmentTask", deny_unknown_fields)]
pub struct PortableSegmentTask {
    pub version: u16,
    pub compatibility: WorkerCompatibility,
    pub pipeline_id: PipelineId,
    pub resource_id: ResourceId,
    pub plan_id: PlanId,
    pub partition_id: PartitionId,
    pub scope: ScopeKey,
    pub canonical_partition_ordinal: u32,
    pub segment_id: SegmentId,
    pub segment_ordinal: u32,
    pub row_count: u64,
    pub prepared_segment: WorkerArtifactReference,
    pub preparation_result_sha256: String,
    pub package_row_ord_start: u64,
    pub output_schema: WorkerArtifactReference,
    pub output_schema_hash: SchemaHash,
    pub segmentation_policy: WorkerArtifactReference,
    pub segmentation_policy_hash: String,
    pub resources: WorkerResourceBudget,
    pub attempt_policy: WorkerAttemptPolicy,
    pub capabilities: WorkerCapabilityRequirements,
    pub output_policy: WorkerOutputPolicy,
    pub task_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedPortableSegmentTask {
    version: u16,
    compatibility: WorkerCompatibility,
    pipeline_id: PipelineId,
    resource_id: ResourceId,
    plan_id: PlanId,
    partition_id: PartitionId,
    scope: ScopeKey,
    canonical_partition_ordinal: u32,
    segment_id: SegmentId,
    segment_ordinal: u32,
    row_count: u64,
    prepared_segment: WorkerArtifactReference,
    preparation_result_sha256: String,
    package_row_ord_start: u64,
    output_schema: WorkerArtifactReference,
    output_schema_hash: SchemaHash,
    segmentation_policy: WorkerArtifactReference,
    segmentation_policy_hash: String,
    resources: WorkerResourceBudget,
    attempt_policy: WorkerAttemptPolicy,
    capabilities: WorkerCapabilityRequirements,
    output_policy: WorkerOutputPolicy,
    task_sha256: String,
}

#[derive(Serialize)]
struct PortableSegmentTaskSemantic<'a> {
    version: u16,
    compatibility: &'a WorkerCompatibility,
    pipeline_id: &'a PipelineId,
    resource_id: &'a ResourceId,
    plan_id: &'a PlanId,
    partition_id: &'a PartitionId,
    scope: &'a ScopeKey,
    canonical_partition_ordinal: u32,
    segment_id: &'a SegmentId,
    segment_ordinal: u32,
    row_count: u64,
    prepared_segment: &'a WorkerArtifactReference,
    preparation_result_sha256: &'a str,
    package_row_ord_start: u64,
    output_schema: &'a WorkerArtifactReference,
    output_schema_hash: &'a SchemaHash,
    segmentation_policy: &'a WorkerArtifactReference,
    segmentation_policy_hash: &'a str,
    resources: &'a WorkerResourceBudget,
    attempt_policy: &'a WorkerAttemptPolicy,
    capabilities: &'a WorkerCapabilityRequirements,
    output_policy: &'a WorkerOutputPolicy,
}

impl TryFrom<UncheckedPortableSegmentTask> for PortableSegmentTask {
    type Error = CdfError;

    fn try_from(value: UncheckedPortableSegmentTask) -> Result<Self> {
        let task = Self {
            version: value.version,
            compatibility: value.compatibility,
            pipeline_id: value.pipeline_id,
            resource_id: value.resource_id,
            plan_id: value.plan_id,
            partition_id: value.partition_id,
            scope: value.scope,
            canonical_partition_ordinal: value.canonical_partition_ordinal,
            segment_id: value.segment_id,
            segment_ordinal: value.segment_ordinal,
            row_count: value.row_count,
            prepared_segment: value.prepared_segment,
            preparation_result_sha256: value.preparation_result_sha256,
            package_row_ord_start: value.package_row_ord_start,
            output_schema: value.output_schema,
            output_schema_hash: value.output_schema_hash,
            segmentation_policy: value.segmentation_policy,
            segmentation_policy_hash: value.segmentation_policy_hash,
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
pub struct PortableSegmentTaskInput {
    pub compatibility: WorkerCompatibility,
    pub pipeline_id: PipelineId,
    pub resource_id: ResourceId,
    pub plan_id: PlanId,
    pub partition_id: PartitionId,
    pub scope: ScopeKey,
    pub canonical_partition_ordinal: u32,
    pub segment_id: SegmentId,
    pub segment_ordinal: u32,
    pub row_count: u64,
    pub prepared_segment: WorkerArtifactReference,
    pub preparation_result_sha256: String,
    pub package_row_ord_start: u64,
    pub output_schema: WorkerArtifactReference,
    pub output_schema_hash: SchemaHash,
    pub segmentation_policy: WorkerArtifactReference,
    pub segmentation_policy_hash: String,
    pub resources: WorkerResourceBudget,
    pub attempt_policy: WorkerAttemptPolicy,
    pub capabilities: WorkerCapabilityRequirements,
    pub output_policy: WorkerOutputPolicy,
}

impl PortableSegmentTask {
    pub fn new(input: PortableSegmentTaskInput) -> Result<Self> {
        let mut task = Self {
            version: PORTABLE_SEGMENT_TASK_VERSION,
            compatibility: input.compatibility,
            pipeline_id: input.pipeline_id,
            resource_id: input.resource_id,
            plan_id: input.plan_id,
            partition_id: input.partition_id,
            scope: input.scope,
            canonical_partition_ordinal: input.canonical_partition_ordinal,
            segment_id: input.segment_id,
            segment_ordinal: input.segment_ordinal,
            row_count: input.row_count,
            prepared_segment: input.prepared_segment,
            preparation_result_sha256: input.preparation_result_sha256,
            package_row_ord_start: input.package_row_ord_start,
            output_schema: input.output_schema,
            output_schema_hash: input.output_schema_hash,
            segmentation_policy: input.segmentation_policy,
            segmentation_policy_hash: input.segmentation_policy_hash,
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
            "portable segment task",
            bytes,
            worker.control.maximum_task_bytes,
        )?;
        let task: Self = serde_json::from_slice(bytes).map_err(|error| {
            CdfError::contract(format!("decode portable segment task: {error}"))
        })?;
        task.validate_for_worker(compatibility, worker)?;
        Ok(task)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != PORTABLE_SEGMENT_TASK_VERSION {
            return Err(CdfError::contract(format!(
                "portable segment task version {} is unsupported",
                self.version
            )));
        }
        self.compatibility.validate()?;
        PipelineId::new(self.pipeline_id.as_str())?;
        ResourceId::new(self.resource_id.as_str())?;
        PlanId::new(self.plan_id.as_str())?;
        PartitionId::new(self.partition_id.as_str())?;
        SegmentId::new(self.segment_id.as_str())?;
        validate_portable_scope(&self.scope)?;
        if self.row_count == 0
            || self
                .package_row_ord_start
                .checked_add(self.row_count)
                .is_none()
        {
            return Err(CdfError::contract(
                "portable segment task requires a nonzero row count and nonoverflowing package ordinal range",
            ));
        }
        validate_artifact_kind(
            &self.prepared_segment,
            WorkerArtifactKind::PreparedSegment,
            "portable segment task input",
        )?;
        validate_sha256(
            "partition preparation result",
            &self.preparation_result_sha256,
        )?;
        validate_artifact_kind(
            &self.output_schema,
            WorkerArtifactKind::OutputSchema,
            "portable segment task output schema",
        )?;
        validate_sha256(
            "portable segment output schema",
            self.output_schema_hash.as_str(),
        )?;
        validate_artifact_kind(
            &self.segmentation_policy,
            WorkerArtifactKind::SegmentationPolicy,
            "portable segment task segmentation policy",
        )?;
        validate_sha256(
            "portable segment segmentation policy",
            &self.segmentation_policy_hash,
        )?;
        self.resources.validate()?;
        self.attempt_policy.validate()?;
        self.capabilities.validate()?;
        self.output_policy.validate()?;
        if self.output_policy.allowed_kinds != [WorkerArtifactKind::CanonicalSegment] {
            return Err(CdfError::contract(
                "portable segment task may authorize only canonical segment output",
            ));
        }
        if self.resources.control.maximum_input_artifacts < 3 {
            return Err(CdfError::contract(
                "portable segment task input-artifact budget must admit prepared rows, schema, and segmentation policy",
            ));
        }
        if self.task_sha256 != self.compute_hash()? {
            return Err(CdfError::contract(
                "portable segment task digest does not match its canonical payload",
            ));
        }
        validate_encoded_size(
            "portable segment task",
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
                "portable segment task compatibility tuple is unsupported by this worker",
            ));
        }
        self.capabilities.validate_worker(&self.resources, worker)
    }

    fn compute_hash(&self) -> Result<String> {
        artifact_hash(&PortableSegmentTaskSemantic {
            version: self.version,
            compatibility: &self.compatibility,
            pipeline_id: &self.pipeline_id,
            resource_id: &self.resource_id,
            plan_id: &self.plan_id,
            partition_id: &self.partition_id,
            scope: &self.scope,
            canonical_partition_ordinal: self.canonical_partition_ordinal,
            segment_id: &self.segment_id,
            segment_ordinal: self.segment_ordinal,
            row_count: self.row_count,
            prepared_segment: &self.prepared_segment,
            preparation_result_sha256: &self.preparation_result_sha256,
            package_row_ord_start: self.package_row_ord_start,
            output_schema: &self.output_schema,
            output_schema_hash: &self.output_schema_hash,
            segmentation_policy: &self.segmentation_policy,
            segmentation_policy_hash: &self.segmentation_policy_hash,
            resources: &self.resources,
            attempt_policy: &self.attempt_policy,
            capabilities: &self.capabilities,
            output_policy: &self.output_policy,
        })
    }
}

impl PortableWorkerTask for PortableSegmentTask {
    fn validate_portable(&self) -> Result<()> {
        self.validate()
    }

    fn task_sha256(&self) -> &str {
        &self.task_sha256
    }

    fn lease_scope(&self) -> &ScopeKey {
        &self.scope
    }

    fn resources(&self) -> &WorkerResourceBudget {
        &self.resources
    }

    fn attempt_policy(&self) -> &WorkerAttemptPolicy {
        &self.attempt_policy
    }

    fn output_policy(&self) -> &WorkerOutputPolicy {
        &self.output_policy
    }
}

pub struct ReconstructedWorkerTaskAuthority {
    source: crate::CompiledSourcePlan,
    partition: cdf_kernel::PartitionPlan,
    execution: ReconstructedExecutionAuthority,
    execution_program: Box<dyn ReconstructedWorkerExecutionProgram>,
}

impl std::fmt::Debug for ReconstructedWorkerTaskAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReconstructedWorkerTaskAuthority")
            .field("source", &self.source)
            .field("partition", &self.partition)
            .field("execution", &self.execution)
            .field("execution_program", &"<opaque>")
            .finish()
    }
}

/// Engine-owned executable program reconstructed from verified compiler artifacts.
///
/// The portable protocol deliberately treats this payload as opaque. A verifier constructs it
/// from the same bytes that establish the neutral authority tuple, and the matching worker
/// executor downcasts it to its own concrete program type. This keeps engine types out of
/// `cdf-runtime` without forcing a second artifact read or allowing the worker to recompile.
pub trait ReconstructedWorkerExecutionProgram: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

impl ReconstructedWorkerExecutionProgram for () {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReconstructedWorkerTaskAuthority {
    /// Constructs one indivisible authority value from artifacts decoded and content-verified by
    /// the worker host. The protocol validates every field against the task immediately after the
    /// verifier returns it.
    pub fn from_verified_artifacts(
        source: crate::CompiledSourcePlan,
        partition: cdf_kernel::PartitionPlan,
        execution: ReconstructedExecutionAuthority,
        execution_program: Box<dyn ReconstructedWorkerExecutionProgram>,
    ) -> Self {
        Self {
            source,
            partition,
            execution,
            execution_program,
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

    pub fn execution_program<T: Any>(&self) -> Result<&T> {
        self.execution_program
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "reconstructed worker execution program is not `{}`",
                    std::any::type_name::<T>()
                ))
            })
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

/// Canonical segment semantics independently observed from the stored object.
///
/// The coordinator compares these facts with the second-barrier task before admitting the
/// worker result. Format-specific verification belongs behind [`WorkerOutputVerifier`]; the
/// portable runtime only owns the exact schema/ordinal invariants that every canonical segment
/// must prove.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedCanonicalSegmentFacts {
    artifact: VerifiedWorkerArtifactFacts,
    logical_schema_hash: SchemaHash,
    package_row_ord_start: u64,
    package_row_ord_end: u64,
}

impl VerifiedCanonicalSegmentFacts {
    pub fn new(
        reference: WorkerArtifactReference,
        row_count: u64,
        logical_schema_hash: SchemaHash,
        package_row_ord_start: u64,
    ) -> Result<Self> {
        if reference.kind != WorkerArtifactKind::CanonicalSegment {
            return Err(CdfError::contract(
                "canonical segment facts require a CanonicalSegment reference",
            ));
        }
        let package_row_ord_end = package_row_ord_start
            .checked_add(row_count)
            .ok_or_else(|| CdfError::data("canonical segment package row ordinal overflow"))?;
        Ok(Self {
            artifact: VerifiedWorkerArtifactFacts::new(reference, Some(row_count))?,
            logical_schema_hash,
            package_row_ord_start,
            package_row_ord_end,
        })
    }

    fn validate_for(
        &self,
        task: &PortableSegmentTask,
        reference: &WorkerArtifactReference,
    ) -> Result<()> {
        self.artifact.validate_for(reference)?;
        let expected_end = task
            .package_row_ord_start
            .checked_add(task.row_count)
            .ok_or_else(|| CdfError::data("segment task package row ordinal overflow"))?;
        if self.artifact.row_count() != Some(task.row_count)
            || self.logical_schema_hash != task.output_schema_hash
            || self.package_row_ord_start != task.package_row_ord_start
            || self.package_row_ord_end != expected_end
        {
            return Err(CdfError::contract(
                "canonical segment content does not match the task schema or package row ordinal authority",
            ));
        }
        Ok(())
    }
}

impl VerifiedWorkerArtifactFacts {
    pub fn new(reference: WorkerArtifactReference, row_count: Option<u64>) -> Result<Self> {
        reference.validate()?;
        match (reference.kind, row_count) {
            (
                WorkerArtifactKind::PreparedSegment | WorkerArtifactKind::CanonicalSegment,
                None | Some(0),
            ) => {
                return Err(CdfError::contract(
                    "verified prepared or canonical segment must contain an observed nonzero row count",
                ));
            }
            (WorkerArtifactKind::Quarantine, None) => {
                return Err(CdfError::contract(
                    "verified quarantine artifact must contain an observed row count",
                ));
            }
            (
                WorkerArtifactKind::PreparedSegment
                | WorkerArtifactKind::CanonicalSegment
                | WorkerArtifactKind::Quarantine,
                Some(_),
            ) => {}
            (_, Some(_)) => {
                return Err(CdfError::contract(
                    "verified row count is only valid for prepared, canonical, or quarantine artifacts",
                ));
            }
            (_, None) => {}
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
        task: &PortablePartitionTask,
        reference: &WorkerArtifactReference,
    ) -> Result<VerifiedWorkerArtifactFacts>;

    fn verify_source_authority(
        &self,
        task: &PortablePartitionTask,
        authority: &ReconstructedWorkerTaskAuthority,
        attestation: &WorkerSourceAttestation,
        result: &PartitionWorkerResult,
    ) -> Result<VerifiedWorkerSourceFacts>;
}

/// One fully owned worker invocation reconstructed from serialized control messages.
///
/// This value deliberately contains no borrowed coordinator resource, store, path, secret, or
/// runtime object. Worker-host services remain injected through the executor implementation and
/// bulk data moves through the referenced source/artifact authorities rather than these control
/// messages.
pub struct IsolatedPartitionInvocation {
    task: PortablePartitionTask,
    attempt: PartitionAttemptEnvelope,
    authority: ReconstructedWorkerTaskAuthority,
}

impl IsolatedPartitionInvocation {
    pub fn task(&self) -> &PortablePartitionTask {
        &self.task
    }

    pub fn attempt(&self) -> &PartitionAttemptEnvelope {
        &self.attempt
    }

    pub fn authority(&self) -> &ReconstructedWorkerTaskAuthority {
        &self.authority
    }

    pub fn into_parts(
        self,
    ) -> (
        PortablePartitionTask,
        PartitionAttemptEnvelope,
        ReconstructedWorkerTaskAuthority,
    ) {
        (self.task, self.attempt, self.authority)
    }
}

/// Worker-owned execution behind the portable task boundary.
///
/// Implementations resolve secrets, source/format drivers, memory, and artifact sinks from their
/// own host services. The executor never receives coordinator execution objects.
pub trait IsolatedPartitionExecutor {
    fn execute(
        &self,
        invocation: IsolatedPartitionInvocation,
    ) -> BoxFuture<'_, Result<PartitionWorkerResult>>;
}

/// A local process host that exercises the exact serialization boundary required by remote hosts.
///
/// It accepts only task/attempt bytes, performs bounded decoding and independent authority
/// reconstruction, then returns only result bytes. Keeping this host transport-free makes it the
/// conformance implementation for future RPC, container, Spark, Flink, or Ballista adapters.
pub struct LocalIsolatedWorkerHost<'a> {
    compatibility: &'a WorkerCompatibility,
    capabilities: &'a WorkerRuntimeCapabilities,
    registry: &'a crate::SourceRegistry,
    verifier: &'a dyn WorkerAdmissionVerifier,
    executor: &'a dyn IsolatedPartitionExecutor,
}

impl<'a> LocalIsolatedWorkerHost<'a> {
    pub fn new(
        compatibility: &'a WorkerCompatibility,
        capabilities: &'a WorkerRuntimeCapabilities,
        registry: &'a crate::SourceRegistry,
        verifier: &'a dyn WorkerAdmissionVerifier,
        executor: &'a dyn IsolatedPartitionExecutor,
    ) -> Result<Self> {
        compatibility.validate()?;
        capabilities.validate()?;
        Ok(Self {
            compatibility,
            capabilities,
            registry,
            verifier,
            executor,
        })
    }

    /// Executes exactly one task so resident control metadata remains bounded independently of
    /// partition cardinality. Task-set enumeration and result persistence belong to external,
    /// content-addressed stores rather than a resident `Vec` in this host.
    pub async fn execute_serialized(
        &self,
        task_bytes: &[u8],
        attempt_bytes: &[u8],
    ) -> Result<Vec<u8>> {
        let task = PortablePartitionTask::decode_bounded(
            task_bytes,
            self.compatibility,
            self.capabilities,
        )?;
        let attempt =
            PartitionAttemptEnvelope::decode_bounded(attempt_bytes, &task, self.capabilities)?;
        let authority = task.reconstruct_and_validate_authority(self.registry, self.verifier)?;
        let result = self
            .executor
            .execute(IsolatedPartitionInvocation {
                task,
                attempt,
                authority,
            })
            .await?;
        result.validate()?;
        serde_json::to_vec(&result)
            .map_err(|error| CdfError::internal(format!("encode partition worker result: {error}")))
    }
}

/// A result that has passed independent coordinator admission.
///
/// The private constructor prevents orchestration from accidentally advancing package/state
/// authority from a merely well-formed worker claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedPartitionWorkerResult(PartitionWorkerResult);

impl AdmittedPartitionWorkerResult {
    pub fn result(&self) -> &PartitionWorkerResult {
        &self.0
    }

    pub fn into_result(self) -> PartitionWorkerResult {
        self.0
    }
}

/// Runs one local isolated-worker round trip and returns only an independently admitted result.
///
/// This function is intentionally one-at-a-time: callers may maintain a bounded concurrent
/// frontier, but neither this API nor the worker host accumulates task/result metadata in memory.
pub async fn execute_local_isolated_partition(
    task: &PortablePartitionTask,
    attempt: &PartitionAttemptEnvelope,
    worker: &LocalIsolatedWorkerHost<'_>,
    coordinator_registry: &crate::SourceRegistry,
    coordinator_verifier: &dyn WorkerAdmissionVerifier,
    current_lease: &WorkerLeaseState,
    now_ms: i64,
) -> Result<AdmittedPartitionWorkerResult> {
    task.validate_for_worker(worker.compatibility, worker.capabilities)?;
    attempt.validate_for_task(task)?;
    let task_bytes = serde_json::to_vec(task)
        .map_err(|error| CdfError::internal(format!("encode portable partition task: {error}")))?;
    let attempt_bytes = serde_json::to_vec(attempt)
        .map_err(|error| CdfError::internal(format!("encode partition attempt: {error}")))?;
    let result_bytes = worker
        .execute_serialized(&task_bytes, &attempt_bytes)
        .await?;
    let result = PartitionWorkerResult::decode_bounded(&result_bytes, task, worker.capabilities)?;
    result.validate_for_admission(
        task,
        attempt,
        coordinator_registry,
        current_lease,
        coordinator_verifier,
        now_ms,
    )?;
    Ok(AdmittedPartitionWorkerResult(result))
}

/// Opaque, owned second-barrier program reconstructed from verified prepared/schema/policy bytes.
pub struct ReconstructedSegmentTask {
    prepared_segment: VerifiedWorkerArtifactFacts,
    output_schema: VerifiedWorkerArtifactFacts,
    segmentation_policy: VerifiedWorkerArtifactFacts,
    execution_program: Box<dyn ReconstructedWorkerExecutionProgram>,
}

impl std::fmt::Debug for ReconstructedSegmentTask {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReconstructedSegmentTask")
            .field("prepared_segment", &self.prepared_segment)
            .field("output_schema", &self.output_schema)
            .field("segmentation_policy", &self.segmentation_policy)
            .field("execution_program", &"<opaque>")
            .finish()
    }
}

impl ReconstructedSegmentTask {
    pub fn from_verified_artifacts(
        prepared_segment: VerifiedWorkerArtifactFacts,
        output_schema: VerifiedWorkerArtifactFacts,
        segmentation_policy: VerifiedWorkerArtifactFacts,
        execution_program: Box<dyn ReconstructedWorkerExecutionProgram>,
    ) -> Self {
        Self {
            prepared_segment,
            output_schema,
            segmentation_policy,
            execution_program,
        }
    }

    pub fn execution_program<T: Any>(&self) -> Result<&T> {
        self.execution_program
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "reconstructed segment execution program is not `{}`",
                    std::any::type_name::<T>()
                ))
            })
    }

    fn validate_for(&self, task: &PortableSegmentTask) -> Result<()> {
        self.prepared_segment.validate_for(&task.prepared_segment)?;
        self.output_schema.validate_for(&task.output_schema)?;
        self.segmentation_policy
            .validate_for(&task.segmentation_policy)?;
        if self.prepared_segment.row_count() != Some(task.row_count)
            || self.output_schema.row_count().is_some()
            || self.segmentation_policy.row_count().is_some()
        {
            return Err(CdfError::contract(
                "reconstructed segment artifacts do not match the task row/schema/policy authority",
            ));
        }
        Ok(())
    }
}

/// Worker-host reconstruction for the source-free second barrier.
pub trait SegmentTaskReconstructor {
    fn reconstruct_segment_task(
        &self,
        task: &PortableSegmentTask,
    ) -> Result<ReconstructedSegmentTask>;
}

/// Coordinator-side verification of one worker output object.
pub trait WorkerOutputVerifier {
    fn verify_canonical_segment(
        &self,
        task: &PortableSegmentTask,
        reference: &WorkerArtifactReference,
    ) -> Result<VerifiedCanonicalSegmentFacts>;
}

pub struct IsolatedSegmentInvocation {
    task: PortableSegmentTask,
    attempt: PartitionAttemptEnvelope,
    reconstructed: ReconstructedSegmentTask,
}

impl IsolatedSegmentInvocation {
    pub fn task(&self) -> &PortableSegmentTask {
        &self.task
    }

    pub fn attempt(&self) -> &PartitionAttemptEnvelope {
        &self.attempt
    }

    pub fn reconstructed(&self) -> &ReconstructedSegmentTask {
        &self.reconstructed
    }

    pub fn into_parts(
        self,
    ) -> (
        PortableSegmentTask,
        PartitionAttemptEnvelope,
        ReconstructedSegmentTask,
    ) {
        (self.task, self.attempt, self.reconstructed)
    }
}

pub trait IsolatedSegmentExecutor {
    fn execute(
        &self,
        invocation: IsolatedSegmentInvocation,
    ) -> BoxFuture<'_, Result<SegmentWorkerResult>>;
}

/// Local serialization harness for source-free canonical segment finalization.
pub struct LocalIsolatedSegmentHost<'a> {
    compatibility: &'a WorkerCompatibility,
    capabilities: &'a WorkerRuntimeCapabilities,
    reconstructor: &'a dyn SegmentTaskReconstructor,
    executor: &'a dyn IsolatedSegmentExecutor,
}

impl<'a> LocalIsolatedSegmentHost<'a> {
    pub fn new(
        compatibility: &'a WorkerCompatibility,
        capabilities: &'a WorkerRuntimeCapabilities,
        reconstructor: &'a dyn SegmentTaskReconstructor,
        executor: &'a dyn IsolatedSegmentExecutor,
    ) -> Result<Self> {
        compatibility.validate()?;
        capabilities.validate()?;
        Ok(Self {
            compatibility,
            capabilities,
            reconstructor,
            executor,
        })
    }

    pub async fn execute_serialized(
        &self,
        task_bytes: &[u8],
        attempt_bytes: &[u8],
    ) -> Result<Vec<u8>> {
        let task =
            PortableSegmentTask::decode_bounded(task_bytes, self.compatibility, self.capabilities)?;
        let attempt =
            PartitionAttemptEnvelope::decode_bounded(attempt_bytes, &task, self.capabilities)?;
        let reconstructed = self.reconstructor.reconstruct_segment_task(&task)?;
        reconstructed.validate_for(&task)?;
        let result = self
            .executor
            .execute(IsolatedSegmentInvocation {
                task,
                attempt,
                reconstructed,
            })
            .await?;
        result.validate()?;
        serde_json::to_vec(&result)
            .map_err(|error| CdfError::internal(format!("encode segment worker result: {error}")))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedSegmentWorkerResult(SegmentWorkerResult);

impl AdmittedSegmentWorkerResult {
    pub fn result(&self) -> &SegmentWorkerResult {
        &self.0
    }

    pub fn into_result(self) -> SegmentWorkerResult {
        self.0
    }
}

pub async fn execute_local_isolated_segment(
    task: &PortableSegmentTask,
    attempt: &PartitionAttemptEnvelope,
    worker: &LocalIsolatedSegmentHost<'_>,
    coordinator_verifier: &dyn WorkerOutputVerifier,
    current_lease: &WorkerLeaseState,
    now_ms: i64,
) -> Result<AdmittedSegmentWorkerResult> {
    task.validate_for_worker(worker.compatibility, worker.capabilities)?;
    attempt.validate_for_task(task)?;
    let task_bytes = serde_json::to_vec(task)
        .map_err(|error| CdfError::internal(format!("encode portable segment task: {error}")))?;
    let attempt_bytes = serde_json::to_vec(attempt)
        .map_err(|error| CdfError::internal(format!("encode segment attempt: {error}")))?;
    let result_bytes = worker
        .execute_serialized(&task_bytes, &attempt_bytes)
        .await?;
    let result = SegmentWorkerResult::decode_bounded(&result_bytes, task, worker.capabilities)?;
    result.validate_for_admission(task, attempt, current_lease, coordinator_verifier, now_ms)?;
    Ok(AdmittedSegmentWorkerResult(result))
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
                .verify_artifact(self, reference)?
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

impl PortableWorkerTask for PortablePartitionTask {
    fn validate_portable(&self) -> Result<()> {
        self.validate()
    }

    fn task_sha256(&self) -> &str {
        &self.task_sha256
    }

    fn lease_scope(&self) -> &ScopeKey {
        &self.partition.scope
    }

    fn resources(&self) -> &WorkerResourceBudget {
        &self.resources
    }

    fn attempt_policy(&self) -> &WorkerAttemptPolicy {
        &self.attempt_policy
    }

    fn output_policy(&self) -> &WorkerOutputPolicy {
        &self.output_policy
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
        task: &dyn PortableWorkerTask,
        current_lease: &WorkerLeaseState,
        reference: &WorkerArtifactReference,
        object_state: &WorkerArtifactObjectState,
        now_ms: i64,
    ) -> Result<()> {
        self.validate()?;
        task.validate_portable()?;
        current_lease.validate()?;
        reference.validate()?;
        if self.task_sha256 != task.task_sha256()
            || &self.lease_scope != task.lease_scope()
            || self.output.maximum_bytes > task.output_policy().maximum_artifact_bytes
            || !task.output_policy().allowed_kinds.contains(&reference.kind)
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
    task: &'a dyn PortableWorkerTask,
    permit: &'a WorkerArtifactWritePermit,
    lease: &'a WorkerLeaseState,
    artifact_count: u32,
    artifact_bytes: u64,
    artifacts: BTreeSet<WorkerArtifactReference>,
}

impl<'a> WorkerArtifactWriteSession<'a> {
    pub fn new(
        task: &'a dyn PortableWorkerTask,
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
        if next_count > self.task.resources().control.maximum_output_artifacts
            || next_bytes > self.task.output_policy().maximum_artifact_bytes
            || next_bytes > self.permit.output.maximum_bytes
            || self.artifacts.contains(&receipt.artifact)
        {
            return Err(CdfError::contract(
                "worker artifact write exceeds cumulative count/byte authority or repeats an object",
            ));
        }

        // Reserve authority before mutation and never roll it back: a provider error can be
        // ambiguous after bytes reached external storage. Retrying consumes a new object/key under
        // the remaining permit instead of laundering writes through an uncharged error path.
        self.artifact_count = next_count;
        self.artifact_bytes = next_bytes;
        self.artifacts.insert(receipt.artifact.clone());
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
    pub fn decode_bounded<T: PortableWorkerTask + ?Sized>(
        bytes: &[u8],
        task: &T,
        worker: &WorkerRuntimeCapabilities,
    ) -> Result<Self> {
        worker.validate()?;
        task.resources().control.validate_within(&worker.control)?;
        validate_raw_size(
            "partition attempt envelope",
            bytes,
            task.resources()
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

    pub fn validate_for_task<T: PortableWorkerTask + ?Sized>(&self, task: &T) -> Result<()> {
        self.validate()?;
        task.validate_portable()?;
        if self.write_permit.task_sha256 != task.task_sha256()
            || &self.write_permit.lease_scope != task.lease_scope()
        {
            return Err(CdfError::contract(
                "partition attempt does not bind the current portable task digest and scope",
            ));
        }
        if self.retry_ordinal >= task.attempt_policy().maximum_attempts
            || u64::try_from(
                self.write_permit
                    .expires_at_ms
                    .saturating_sub(self.write_permit.issued_at_ms),
            )
            .unwrap_or(u64::MAX)
                > task.attempt_policy().maximum_attempt_duration_ms
        {
            return Err(CdfError::contract(
                "partition attempt exceeds the task retry or duration authority",
            ));
        }
        if self.write_permit.output.maximum_bytes > task.output_policy().maximum_artifact_bytes {
            return Err(CdfError::contract(
                "partition attempt widens the task artifact byte authority",
            ));
        }
        validate_encoded_size(
            "partition attempt envelope",
            self,
            task.resources().control.maximum_attempt_bytes,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerArtifactRole {
    PreparedSegment {
        segment_id: SegmentId,
        partition_ordinal: u32,
        segment_ordinal: u32,
        row_count: u64,
    },
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
    PartitionEvidence {
        partition_ordinal: u32,
    },
}

impl WorkerArtifactRole {
    fn expected_kind(&self) -> WorkerArtifactKind {
        match self {
            Self::PreparedSegment { .. } => WorkerArtifactKind::PreparedSegment,
            Self::CanonicalSegment { .. } => WorkerArtifactKind::CanonicalSegment,
            Self::Quarantine => WorkerArtifactKind::Quarantine,
            Self::Residual => WorkerArtifactKind::Residual,
            Self::Verdict => WorkerArtifactKind::Verdict,
            Self::Lineage => WorkerArtifactKind::Lineage,
            Self::PartitionEvidence { .. } => WorkerArtifactKind::PartitionEvidence,
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
        if let WorkerArtifactRole::PreparedSegment {
            segment_id,
            row_count,
            ..
        }
        | WorkerArtifactRole::CanonicalSegment {
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
        if !matches!(self.status, WorkerTerminalStatus::Succeeded) && !self.artifacts.is_empty() {
            return Err(CdfError::contract(
                "non-successful partition worker result cannot advance artifacts",
            ));
        }
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
                WorkerArtifactRole::PreparedSegment { row_count, .. }
                | WorkerArtifactRole::CanonicalSegment { row_count, .. } => row_count,
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
            if let WorkerArtifactRole::PreparedSegment {
                partition_ordinal, ..
            }
            | WorkerArtifactRole::CanonicalSegment {
                partition_ordinal, ..
            }
            | WorkerArtifactRole::PartitionEvidence { partition_ordinal } = receipt.role
                && partition_ordinal != task.partition.canonical_partition_ordinal
            {
                return Err(CdfError::contract(
                    "partition worker output receipt exceeds its canonical partition authority",
                ));
            }
            let facts = verifier.verify_artifact(task, &receipt.artifact)?;
            facts.validate_for(&receipt.artifact)?;
            match receipt.role {
                WorkerArtifactRole::PreparedSegment { row_count, .. }
                | WorkerArtifactRole::CanonicalSegment { row_count, .. } => {
                    let observed_rows = facts.row_count().ok_or_else(|| {
                        CdfError::contract(
                            "verified prepared or canonical segment is missing an observed row count",
                        )
                    })?;
                    if observed_rows != row_count {
                        return Err(CdfError::contract(
                            "worker prepared or canonical segment receipt row count does not match stored content",
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
                        .checked_add(facts.row_count().ok_or_else(|| {
                            CdfError::contract(
                                "verified quarantine artifact is missing an observed row count",
                            )
                        })?)
                        .ok_or_else(|| {
                            CdfError::contract("verified worker quarantine rows overflowed u64")
                        })?;
                }
                WorkerArtifactRole::Residual
                | WorkerArtifactRole::Verdict
                | WorkerArtifactRole::Lineage
                | WorkerArtifactRole::PartitionEvidence { .. } => {}
            }
        }
        let attestation = self.source_attestation.as_ref().ok_or_else(|| {
            CdfError::contract("successful partition worker result lacks source attestation")
        })?;
        let source_facts = verifier.verify_source_authority(task, &authority, attestation, self)?;
        for (matches, label, claimed, observed) in [
            (
                source_facts.processed_position == attestation.processed_position,
                "processed source position",
                None,
                None,
            ),
            (
                source_facts.physical_schema_hash == attestation.physical_schema_hash,
                "physical schema hash",
                None,
                None,
            ),
            (
                source_facts.input_rows == self.counts.input_rows,
                "input rows",
                Some(self.counts.input_rows),
                Some(source_facts.input_rows),
            ),
            (
                source_facts.source_bytes == self.counts.source_bytes,
                "source bytes",
                Some(self.counts.source_bytes),
                Some(source_facts.source_bytes),
            ),
            (
                verified_output_rows == self.counts.output_rows,
                "output rows",
                Some(self.counts.output_rows),
                Some(verified_output_rows),
            ),
            (
                verified_quarantined_rows == self.counts.quarantined_rows,
                "quarantined rows",
                Some(self.counts.quarantined_rows),
                Some(verified_quarantined_rows),
            ),
        ] {
            if !matches {
                let detail = match (claimed, observed) {
                    (Some(claimed), Some(observed)) => {
                        format!("; worker claimed {claimed}, authority observed {observed}")
                    }
                    _ => String::new(),
                };
                return Err(CdfError::contract(format!(
                    "worker result {label} does not match independently verified facts{detail}"
                )));
            }
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
            &self.artifacts,
            &self.counts,
        ))
    }
}

/// Bounded second-barrier result for exactly one canonical segment.
///
/// Source, verdict, and checkpoint evidence belongs to the admitted preparation result. This
/// result proves only that the prefix-bound prepared artifact became the one canonical segment
/// authorized by `PortableSegmentTask`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedSegmentWorkerResult", deny_unknown_fields)]
pub struct SegmentWorkerResult {
    pub version: u16,
    pub task_sha256: String,
    pub attempt_id: String,
    pub write_permit_sha256: String,
    pub status: WorkerTerminalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<WorkerArtifactReceipt>,
    pub telemetry: WorkerTelemetry,
    pub result_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedSegmentWorkerResult {
    version: u16,
    task_sha256: String,
    attempt_id: String,
    write_permit_sha256: String,
    status: WorkerTerminalStatus,
    artifact: Option<WorkerArtifactReceipt>,
    telemetry: WorkerTelemetry,
    result_sha256: String,
}

impl TryFrom<UncheckedSegmentWorkerResult> for SegmentWorkerResult {
    type Error = CdfError;

    fn try_from(value: UncheckedSegmentWorkerResult) -> Result<Self> {
        let result = Self {
            version: value.version,
            task_sha256: value.task_sha256,
            attempt_id: value.attempt_id,
            write_permit_sha256: value.write_permit_sha256,
            status: value.status,
            artifact: value.artifact,
            telemetry: value.telemetry,
            result_sha256: value.result_sha256,
        };
        result.validate()?;
        Ok(result)
    }
}

impl SegmentWorkerResult {
    pub fn new(
        attempt: &PartitionAttemptEnvelope,
        status: WorkerTerminalStatus,
        artifact: Option<WorkerArtifactReceipt>,
        telemetry: WorkerTelemetry,
    ) -> Result<Self> {
        let mut result = Self {
            version: SEGMENT_WORKER_RESULT_VERSION,
            task_sha256: attempt.write_permit.task_sha256.clone(),
            attempt_id: attempt.attempt_id.clone(),
            write_permit_sha256: attempt.write_permit.hash()?,
            status,
            artifact,
            telemetry,
            result_sha256: String::new(),
        };
        result.result_sha256 = result.compute_semantic_hash()?;
        result.validate()?;
        Ok(result)
    }

    pub fn decode_bounded(
        bytes: &[u8],
        task: &PortableSegmentTask,
        worker: &WorkerRuntimeCapabilities,
    ) -> Result<Self> {
        worker.validate()?;
        task.resources.control.validate_within(&worker.control)?;
        validate_raw_size(
            "segment worker result",
            bytes,
            task.resources
                .control
                .maximum_result_bytes
                .min(worker.control.maximum_result_bytes),
        )?;
        serde_json::from_slice(bytes)
            .map_err(|error| CdfError::contract(format!("decode segment worker result: {error}")))
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != SEGMENT_WORKER_RESULT_VERSION {
            return Err(CdfError::contract(format!(
                "segment worker result version {} is unsupported",
                self.version
            )));
        }
        validate_sha256("segment worker task", &self.task_sha256)?;
        validate_token("segment worker attempt id", &self.attempt_id)?;
        validate_sha256("segment worker write permit", &self.write_permit_sha256)?;
        self.status.validate()?;
        match (&self.status, &self.artifact) {
            (WorkerTerminalStatus::Succeeded, Some(receipt)) => receipt.validate()?,
            (WorkerTerminalStatus::Succeeded, None) => {
                return Err(CdfError::contract(
                    "successful segment worker result requires one artifact receipt",
                ));
            }
            (_, Some(_)) => {
                return Err(CdfError::contract(
                    "non-successful segment worker result cannot publish an artifact",
                ));
            }
            (_, None) => {}
        }
        if self.result_sha256 != self.compute_semantic_hash()? {
            return Err(CdfError::contract(
                "segment worker result digest does not match its canonical semantic payload",
            ));
        }
        Ok(())
    }

    pub fn validate_for_admission(
        &self,
        task: &PortableSegmentTask,
        attempt: &PartitionAttemptEnvelope,
        current_lease: &WorkerLeaseState,
        verifier: &dyn WorkerOutputVerifier,
        now_ms: i64,
    ) -> Result<()> {
        self.validate()?;
        task.validate()?;
        attempt.validate_for_task(task)?;
        current_lease.validate_permit(&attempt.write_permit, now_ms)?;
        if self.task_sha256 != task.task_sha256
            || self.attempt_id != attempt.attempt_id
            || self.write_permit_sha256 != attempt.write_permit.hash()?
        {
            return Err(CdfError::contract(
                "segment worker result has a stale or mismatched task/attempt/write permit",
            ));
        }
        if !matches!(self.status, WorkerTerminalStatus::Succeeded) {
            return Err(CdfError::data(
                "only a successful segment worker result may advance coordinator authority",
            ));
        }
        let receipt = self.artifact.as_ref().ok_or_else(|| {
            CdfError::contract("successful segment worker result lacks its artifact receipt")
        })?;
        let WorkerArtifactRole::CanonicalSegment {
            segment_id,
            partition_ordinal,
            segment_ordinal,
            row_count,
        } = &receipt.role
        else {
            return Err(CdfError::contract(
                "segment finalization result must publish one canonical segment",
            ));
        };
        if segment_id != &task.segment_id
            || *partition_ordinal != task.canonical_partition_ordinal
            || *segment_ordinal != task.segment_ordinal
            || *row_count != task.row_count
            || !task
                .output_policy
                .allowed_kinds
                .contains(&receipt.artifact.kind)
            || !attempt.write_permit.output.admits(&receipt.artifact)
            || receipt.artifact.byte_count > task.output_policy.maximum_artifact_bytes
        {
            return Err(CdfError::contract(
                "segment worker receipt exceeds its canonical segment authority",
            ));
        }
        verifier
            .verify_canonical_segment(task, &receipt.artifact)?
            .validate_for(task, &receipt.artifact)?;
        validate_encoded_size(
            "segment worker result",
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
            &self.artifact,
        ))
    }
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
        if let WorkerArtifactRole::PreparedSegment {
            segment_id,
            segment_ordinal,
            ..
        }
        | WorkerArtifactRole::CanonicalSegment {
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
    position.validate()?;
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
        SourcePosition::TableSnapshot(position) => {
            validate_no_absolute_coordinator_path(&position.metadata_location)?;
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
