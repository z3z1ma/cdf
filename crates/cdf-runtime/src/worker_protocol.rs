use std::collections::BTreeSet;

use cdf_kernel::{
    CdfError, CheckpointId, ContentObjectKey, ContentProviderGeneration, ContentStoreNamespace,
    FencingToken, LeaseAuthorityDomainId, PartitionId, PipelineId, PlanId,
    ProcessedObservationPosition, ResourceId, Result, SchemaHash, ScopeKey, SecretReference,
    SegmentId, SourcePosition,
};
use serde::{Deserialize, Serialize};

use crate::{
    BlockingLaneBinding, BlockingLaneSpec, ExecutionHostCapabilities, SourceDriverId, artifact_hash,
};

pub const PORTABLE_PARTITION_TASK_VERSION: u16 = 1;
pub const PARTITION_ATTEMPT_VERSION: u16 = 1;
pub const PARTITION_WORKER_RESULT_VERSION: u16 = 1;

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
    CompiledSourcePlan,
    PartitionPlan,
    InputPayload,
    CanonicalSegment,
    Quarantine,
    Residual,
    Verdict,
    Lineage,
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
        if self.byte_count == 0 {
            return Err(CdfError::contract(
                "portable worker artifact references require a nonzero byte count",
            ));
        }
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
    pub physical_plan_hash: String,
    pub source_semantics_hash: String,
    pub execution_capabilities_hash: String,
}

impl PortableSourceBinding {
    pub fn validate(&self) -> Result<()> {
        SourceDriverId::new(self.driver_id.as_str())?;
        validate_token("source driver version", &self.driver_version)?;
        validate_sha256("source option schema", &self.option_schema_hash)?;
        self.compiled_source_plan.validate()?;
        if self.compiled_source_plan.kind != WorkerArtifactKind::CompiledSourcePlan {
            return Err(CdfError::contract(
                "portable source binding must reference a compiled-source-plan artifact",
            ));
        }
        validate_sha256("compiled source physical plan", &self.physical_plan_hash)?;
        validate_sha256("compiled source semantics", &self.source_semantics_hash)?;
        validate_sha256(
            "compiled source execution capabilities",
            &self.execution_capabilities_hash,
        )
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
    pub segment_authority_hash: String,
}

impl PortablePartitionBinding {
    pub fn validate(&self) -> Result<()> {
        PartitionId::new(self.partition_id.as_str())?;
        self.partition_plan.validate()?;
        if self.partition_plan.kind != WorkerArtifactKind::PartitionPlan {
            return Err(CdfError::contract(
                "portable partition binding must reference a partition-plan artifact",
            ));
        }
        validate_sha256("partition source identity", &self.source_identity_hash)?;
        validate_sha256("partition segment authority", &self.segment_authority_hash)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableExecutionBinding {
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerInputCheckpointBinding {
    pub checkpoint_id: CheckpointId,
    pub scope: ScopeKey,
    pub state_version: u16,
    pub position: SourcePosition,
    pub content_sha256: String,
}

impl WorkerInputCheckpointBinding {
    pub fn validate(&self) -> Result<()> {
        CheckpointId::new(self.checkpoint_id.as_str())?;
        if self.state_version == 0 {
            return Err(CdfError::contract(
                "portable input checkpoint state version must be nonzero",
            ));
        }
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
            if lane.binding == BlockingLaneBinding::RuntimeResolvedRequired {
                return Err(CdfError::contract(format!(
                    "portable worker requirement `{}` is still awaiting runtime resolution",
                    lane.lane_id
                )));
            }
            if !lanes.insert(lane.lane_id.as_str()) {
                return Err(CdfError::contract(
                    "portable worker blocking-lane requirements must be unique",
                ));
            }
        }
        validate_sorted_unique_tokens("portable worker service", &self.services)
    }

    pub fn validate_host(
        &self,
        budget: &WorkerResourceBudget,
        host: &ExecutionHostCapabilities,
    ) -> Result<()> {
        self.validate()?;
        budget.validate()?;
        host.validate()?;
        if host.logical_cpu_slots < budget.cpu_slots || host.io_workers < budget.io_slots {
            return Err(CdfError::contract(
                "execution host does not satisfy portable worker CPU/I/O requirements",
            ));
        }
        for required in &self.required_blocking_lanes {
            let available = host
                .blocking_lanes
                .iter()
                .find(|candidate| candidate.lane_id == required.lane_id)
                .ok_or_else(|| {
                    CdfError::contract(format!(
                        "execution host is missing required blocking lane `{}`",
                        required.lane_id
                    ))
                })?;
            if available.maximum_concurrency < required.maximum_concurrency
                || available.cpu_slot_cost != required.cpu_slot_cost
                || available.native_internal_parallelism != required.native_internal_parallelism
                || available.affinity != required.affinity
                || available.interruption != required.interruption
                || available.binding == BlockingLaneBinding::RuntimeResolvedRequired
            {
                return Err(CdfError::contract(format!(
                    "execution host blocking lane `{}` does not satisfy the portable task requirement",
                    required.lane_id
                )));
            }
        }
        Ok(())
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
        if self.allowed_kinds.iter().any(|kind| {
            matches!(
                kind,
                WorkerArtifactKind::CompiledSourcePlan
                    | WorkerArtifactKind::PartitionPlan
                    | WorkerArtifactKind::InputPayload
            )
        }) {
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
            capabilities: value.capabilities,
            output_policy: value.output_policy,
            task_sha256: value.task_sha256,
        };
        task.validate()?;
        Ok(task)
    }
}

impl PortablePartitionTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
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
        capabilities: WorkerCapabilityRequirements,
        output_policy: WorkerOutputPolicy,
    ) -> Result<Self> {
        let mut task = Self {
            version: PORTABLE_PARTITION_TASK_VERSION,
            compatibility,
            pipeline_id,
            resource_id,
            plan_id,
            source,
            partition,
            execution,
            input_checkpoint,
            secret_references,
            input_artifacts,
            resources,
            capabilities,
            output_policy,
            task_sha256: String::new(),
        };
        task.task_sha256 = task.compute_hash()?;
        task.validate()?;
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
        self.capabilities.validate()?;
        self.output_policy.validate()?;
        validate_sorted_unique_secret_references(&self.secret_references)?;
        validate_sorted_unique_artifacts("portable task input", &self.input_artifacts)?;
        if self.input_artifacts.len().saturating_add(2)
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
        host: &ExecutionHostCapabilities,
    ) -> Result<()> {
        self.validate()?;
        compatibility.validate()?;
        if &self.compatibility != compatibility {
            return Err(CdfError::contract(
                "portable partition task compatibility tuple is unsupported by this worker",
            ));
        }
        self.capabilities.validate_host(&self.resources, host)
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
#[serde(deny_unknown_fields)]
pub struct PartitionAttemptEnvelope {
    pub version: u16,
    pub task_sha256: String,
    pub attempt_id: String,
    pub lease_authority_domain_id: LeaseAuthorityDomainId,
    pub fencing_token: FencingToken,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
    pub retry_ordinal: u16,
    pub trace_id: String,
    pub output: WorkerArtifactWriteScope,
}

impl PartitionAttemptEnvelope {
    pub fn validate_for_task(&self, task: &PortablePartitionTask) -> Result<()> {
        task.validate()?;
        if self.version != PARTITION_ATTEMPT_VERSION || self.task_sha256 != task.task_sha256 {
            return Err(CdfError::contract(
                "partition attempt does not bind the current portable task version and digest",
            ));
        }
        validate_token("partition attempt id", &self.attempt_id)?;
        LeaseAuthorityDomainId::new(self.lease_authority_domain_id.as_str())?;
        FencingToken::new(self.fencing_token.get())?;
        if self.expires_at_ms <= self.issued_at_ms {
            return Err(CdfError::contract(
                "partition attempt expiry must follow issuance",
            ));
        }
        validate_token("partition attempt trace id", &self.trace_id)?;
        self.output.validate()?;
        if self.output.maximum_bytes > task.output_policy.maximum_artifact_bytes {
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

    pub fn is_live_at(&self, now_ms: i64) -> bool {
        self.issued_at_ms <= now_ms && now_ms < self.expires_at_ms
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
    pub processed_position: SourcePosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_schema_hash: Option<SchemaHash>,
}

impl WorkerSourceAttestation {
    pub fn validate(&self) -> Result<()> {
        if let Some(hash) = &self.physical_schema_hash {
            validate_sha256("worker physical schema", hash.as_str())?;
        }
        Ok(())
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
    pub lease_authority_domain_id: LeaseAuthorityDomainId,
    pub fencing_token: FencingToken,
    pub status: WorkerTerminalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_attestation: Option<WorkerSourceAttestation>,
    pub processed_observations: Vec<ProcessedObservationPosition>,
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
    lease_authority_domain_id: LeaseAuthorityDomainId,
    fencing_token: FencingToken,
    status: WorkerTerminalStatus,
    source_attestation: Option<WorkerSourceAttestation>,
    processed_observations: Vec<ProcessedObservationPosition>,
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
            lease_authority_domain_id: value.lease_authority_domain_id,
            fencing_token: value.fencing_token,
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

impl PartitionWorkerResult {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        attempt: &PartitionAttemptEnvelope,
        status: WorkerTerminalStatus,
        source_attestation: Option<WorkerSourceAttestation>,
        processed_observations: Vec<ProcessedObservationPosition>,
        artifacts: Vec<WorkerArtifactReceipt>,
        counts: WorkerResultCounts,
        telemetry: WorkerTelemetry,
    ) -> Result<Self> {
        let mut result = Self {
            version: PARTITION_WORKER_RESULT_VERSION,
            task_sha256: attempt.task_sha256.clone(),
            attempt_id: attempt.attempt_id.clone(),
            lease_authority_domain_id: attempt.lease_authority_domain_id.clone(),
            fencing_token: attempt.fencing_token,
            status,
            source_attestation,
            processed_observations,
            artifacts,
            counts,
            telemetry,
            result_sha256: String::new(),
        };
        result.result_sha256 = result.compute_semantic_hash()?;
        result.validate()?;
        Ok(result)
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
        LeaseAuthorityDomainId::new(self.lease_authority_domain_id.as_str())?;
        FencingToken::new(self.fencing_token.get())?;
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
            && !self.processed_observations.is_empty()
        {
            return Err(CdfError::contract(
                "non-successful partition worker result cannot advance processed observations",
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
        current_fencing_token: FencingToken,
        now_ms: i64,
    ) -> Result<()> {
        self.validate()?;
        attempt.validate_for_task(task)?;
        if !attempt.is_live_at(now_ms) {
            return Err(CdfError::contract(
                "partition worker result belongs to an expired or not-yet-live attempt",
            ));
        }
        if self.task_sha256 != task.task_sha256
            || self.attempt_id != attempt.attempt_id
            || self.lease_authority_domain_id != attempt.lease_authority_domain_id
            || self.fencing_token != attempt.fencing_token
            || self.fencing_token != current_fencing_token
        {
            return Err(CdfError::contract(
                "partition worker result has a stale or mismatched task/attempt fence",
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
            || self.counts.artifact_bytes > attempt.output.maximum_bytes
        {
            return Err(CdfError::contract(
                "partition worker result exceeds its artifact control or byte authority",
            ));
        }
        for receipt in &self.artifacts {
            if !task
                .output_policy
                .allowed_kinds
                .contains(&receipt.artifact.kind)
                || !attempt.output.admits(&receipt.artifact)
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
            &self.lease_authority_domain_id,
            self.fencing_token,
            &self.status,
            &self.source_attestation,
            &self.processed_observations,
            &self.artifacts,
            &self.counts,
        ))
    }
}

fn validate_processed_observations(observations: &[ProcessedObservationPosition]) -> Result<()> {
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
    for receipt in receipts {
        receipt.validate()?;
        if !artifact_identities.insert(&receipt.artifact) {
            return Err(CdfError::contract(
                "partition worker result contains a duplicate artifact receipt",
            ));
        }
        if let WorkerArtifactRole::CanonicalSegment {
            segment_ordinal, ..
        } = receipt.role
            && !segment_ordinals.insert(segment_ordinal)
        {
            return Err(CdfError::contract(
                "partition worker result contains conflicting canonical segment authority",
            ));
        }
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
        || value.contains("..")
        || value.contains(['\\', '\0'])
    {
        return Err(CdfError::contract(
            "worker artifact key prefix must be a non-empty portable relative key",
        ));
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
mod tests {
    use super::*;
    use cdf_kernel::{
        CursorPosition, CursorValue, ProcessedObservationOutcome, ProcessedObservationPosition,
    };

    fn hash(seed: u8) -> String {
        format!("sha256:{}", format!("{seed:02x}").repeat(32))
    }

    fn artifact(
        kind: WorkerArtifactKind,
        key: &str,
        bytes: u64,
        seed: u8,
    ) -> WorkerArtifactReference {
        WorkerArtifactReference {
            kind,
            store_namespace: ContentStoreNamespace::new("worker-fixtures").unwrap(),
            object_key: ContentObjectKey::new(key).unwrap(),
            byte_count: bytes,
            content_sha256: hash(seed),
            provider_generation: Some(ContentProviderGeneration::new("generation-7").unwrap()),
        }
    }

    fn position(value: u64) -> SourcePosition {
        SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "offset".to_owned(),
            value: CursorValue::U64(value),
        })
    }

    fn compatibility() -> WorkerCompatibility {
        WorkerCompatibility {
            cdf_version: "0.1.0".to_owned(),
            artifact_version: "package-v2".to_owned(),
            arrow_version: "59.1.0".to_owned(),
            relational_engine: WorkerComponentVersion {
                component: "datafusion".to_owned(),
                version: "51.0.0".to_owned(),
            },
            normalizer_version: "namecase-v1".to_owned(),
        }
    }

    fn task() -> PortablePartitionTask {
        PortablePartitionTask::new(
            compatibility(),
            PipelineId::new("pipeline-fixture").unwrap(),
            ResourceId::new("mock.events").unwrap(),
            PlanId::new("plan-fixture").unwrap(),
            PortableSourceBinding {
                driver_id: SourceDriverId::new("mock_source").unwrap(),
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: hash(1),
                compiled_source_plan: artifact(
                    WorkerArtifactKind::CompiledSourcePlan,
                    "plans/source.json",
                    2048,
                    2,
                ),
                physical_plan_hash: hash(3),
                source_semantics_hash: hash(4),
                execution_capabilities_hash: hash(5),
            },
            PortablePartitionBinding {
                partition_id: PartitionId::new("partition-00000003").unwrap(),
                scope: ScopeKey::Partition {
                    partition_id: PartitionId::new("partition-00000003").unwrap(),
                },
                canonical_partition_ordinal: 3,
                epoch_ordinal: Some(9),
                partition_plan: artifact(
                    WorkerArtifactKind::PartitionPlan,
                    "plans/partition-00000003.json",
                    1024,
                    6,
                ),
                source_identity_hash: hash(7),
                segment_authority_hash: hash(8),
            },
            PortableExecutionBinding {
                output_schema_hash: SchemaHash::new(hash(9)).unwrap(),
                validation_program_hash: hash(10),
                normalization_policy_hash: hash(11),
                compiled_expression_plan_hash: hash(12),
                operator_graph_hash: hash(13),
                segmentation_policy_hash: hash(14),
                execution_extent_hash: hash(15),
            },
            Some(WorkerInputCheckpointBinding {
                checkpoint_id: CheckpointId::new("checkpoint-8").unwrap(),
                scope: ScopeKey::Resource,
                state_version: 1,
                position: position(100),
                content_sha256: hash(16),
            }),
            vec![SecretReference::new("secret://env/MOCK_TOKEN").unwrap()],
            vec![artifact(
                WorkerArtifactKind::InputPayload,
                "inputs/events.bin",
                4096,
                17,
            )],
            WorkerResourceBudget {
                memory_bytes: 256 * 1024 * 1024,
                disk_bytes: 2 * 1024 * 1024 * 1024,
                cpu_slots: 2,
                io_slots: 1,
                control: WorkerControlBudget {
                    maximum_task_bytes: 64 * 1024,
                    maximum_attempt_bytes: 16 * 1024,
                    maximum_result_bytes: 64 * 1024,
                    maximum_input_artifacts: 16,
                    maximum_output_artifacts: 16,
                    maximum_secret_references: 4,
                },
            },
            WorkerCapabilityRequirements {
                required_blocking_lanes: Vec::new(),
                services: vec![
                    "artifact-reader-v1".to_owned(),
                    "source-registry-v1".to_owned(),
                ],
            },
            WorkerOutputPolicy {
                allowed_kinds: vec![
                    WorkerArtifactKind::CanonicalSegment,
                    WorkerArtifactKind::Quarantine,
                    WorkerArtifactKind::Residual,
                    WorkerArtifactKind::Verdict,
                    WorkerArtifactKind::Lineage,
                ],
                maximum_artifact_bytes: 1024 * 1024 * 1024,
            },
        )
        .unwrap()
    }

    fn attempt(task: &PortablePartitionTask) -> PartitionAttemptEnvelope {
        PartitionAttemptEnvelope {
            version: PARTITION_ATTEMPT_VERSION,
            task_sha256: task.task_sha256.clone(),
            attempt_id: "attempt-4".to_owned(),
            lease_authority_domain_id: LeaseAuthorityDomainId::new("local-test-domain").unwrap(),
            fencing_token: FencingToken::new(4).unwrap(),
            issued_at_ms: 1_000,
            expires_at_ms: 10_000,
            retry_ordinal: 0,
            trace_id: "trace-4".to_owned(),
            output: WorkerArtifactWriteScope {
                store_namespace: ContentStoreNamespace::new("worker-fixtures").unwrap(),
                object_key_prefix: "attempts/attempt-4/".to_owned(),
                maximum_bytes: 1024 * 1024 * 1024,
            },
        }
    }

    fn result(
        task: &PortablePartitionTask,
        attempt: &PartitionAttemptEnvelope,
    ) -> PartitionWorkerResult {
        let segment = WorkerArtifactReceipt {
            role: WorkerArtifactRole::CanonicalSegment {
                segment_id: SegmentId::new("p00000003-s00000000").unwrap(),
                partition_ordinal: 3,
                segment_ordinal: 0,
                row_count: 50,
            },
            artifact: artifact(
                WorkerArtifactKind::CanonicalSegment,
                "attempts/attempt-4/data/p00000003-s00000000.arrow",
                8192,
                18,
            ),
        };
        let result = PartitionWorkerResult::new(
            attempt,
            WorkerTerminalStatus::Succeeded,
            Some(WorkerSourceAttestation {
                processed_position: position(150),
                physical_schema_hash: Some(task.execution.output_schema_hash.clone()),
            }),
            vec![
                ProcessedObservationPosition::new(
                    "partition-00000003",
                    ProcessedObservationOutcome::Admitted,
                    position(150),
                )
                .unwrap(),
            ],
            vec![segment],
            WorkerResultCounts {
                input_rows: 50,
                output_rows: 50,
                quarantined_rows: 0,
                source_bytes: 4096,
                artifact_bytes: 8192,
            },
            WorkerTelemetry {
                elapsed_ns: 100,
                cpu_ns: 80,
                peak_memory_bytes: 1024,
                spill_bytes: 0,
            },
        )
        .unwrap();
        result
            .validate_for_admission(task, attempt, FencingToken::new(4).unwrap(), 2_000)
            .unwrap();
        result
    }

    #[test]
    fn task_fixture_is_canonical_and_round_trips() {
        let task = task();
        let encoded = serde_json::to_vec(&task).unwrap();
        let decoded: PortablePartitionTask = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, task);
        assert_eq!(
            artifact_hash(&decoded).unwrap(),
            artifact_hash(&task).unwrap()
        );
        assert_eq!(
            serde_json::to_string_pretty(&task).unwrap(),
            include_str!("../tests/fixtures/portable_partition_task_v1.json").trim_end()
        );
    }

    #[test]
    fn task_tamper_version_and_compatibility_fail_closed() {
        let task = task();
        let mut tampered = serde_json::to_value(&task).unwrap();
        tampered["resource_id"] = serde_json::json!("mock.other");
        assert!(serde_json::from_value::<PortablePartitionTask>(tampered).is_err());

        let mut unsupported = serde_json::to_value(&task).unwrap();
        unsupported["version"] = serde_json::json!(2);
        assert!(serde_json::from_value::<PortablePartitionTask>(unsupported).is_err());

        let host = ExecutionHostCapabilities {
            logical_cpu_slots: 4,
            io_workers: 2,
            blocking_lanes: Vec::new(),
        };
        let mut incompatible = compatibility();
        incompatible.arrow_version = "60.0.0".to_owned();
        assert!(task.validate_for_worker(&incompatible, &host).is_err());
        task.validate_for_worker(&compatibility(), &host).unwrap();
    }

    #[test]
    fn result_is_semantically_hashed_and_rejects_stale_authority() {
        let task = task();
        let attempt = attempt(&task);
        attempt.validate_for_task(&task).unwrap();
        let result = result(&task, &attempt);

        let mut telemetry_only = result.clone();
        telemetry_only.telemetry.elapsed_ns += 1;
        telemetry_only.validate().unwrap();
        assert_eq!(telemetry_only.result_sha256, result.result_sha256);

        assert!(
            result
                .validate_for_admission(&task, &attempt, FencingToken::new(5).unwrap(), 2_000)
                .unwrap_err()
                .message
                .contains("stale")
        );
        assert!(
            result
                .validate_for_admission(&task, &attempt, FencingToken::new(4).unwrap(), 10_000,)
                .unwrap_err()
                .message
                .contains("expired")
        );

        let mut tampered = serde_json::to_value(&result).unwrap();
        tampered["counts"]["output_rows"] = serde_json::json!(49);
        assert!(serde_json::from_value::<PartitionWorkerResult>(tampered).is_err());
    }

    #[test]
    fn protocol_payload_has_no_data_or_coordinator_commit_authority() {
        let task = task();
        let attempt = attempt(&task);
        let result = result(&task, &attempt);
        let task_json = serde_json::to_string(&task).unwrap();
        let result_json = serde_json::to_string(&result).unwrap();
        assert!(!task_json.contains("/Users/"));
        assert!(!task_json.contains("plain-text-secret"));
        assert!(!result_json.contains("package_hash"));
        assert!(!result_json.contains("destination_receipt"));
        assert!(!result_json.contains("checkpoint_id"));
    }

    #[test]
    fn control_metadata_and_output_scope_are_explicit_knobs() {
        let mut undersized_task = task();
        undersized_task.resources.control.maximum_task_bytes = 1;
        undersized_task.task_sha256 = undersized_task.compute_hash().unwrap();
        assert!(
            undersized_task
                .validate()
                .unwrap_err()
                .message
                .contains("control budget")
        );

        let task = task();
        let attempt = attempt(&task);
        let mut result = result(&task, &attempt);
        result.artifacts[0].artifact.object_key =
            ContentObjectKey::new("another-attempt/data.arrow").unwrap();
        result.result_sha256 = result.compute_semantic_hash().unwrap();
        assert!(
            result
                .validate_for_admission(&task, &attempt, FencingToken::new(4).unwrap(), 2_000,)
                .unwrap_err()
                .message
                .contains("unauthorized")
        );
    }
}
