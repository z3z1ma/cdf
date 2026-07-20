use cdf_kernel::{
    CdfError, PipelineId, Result, SecretReference, partition_source_identity_binding,
};
use cdf_runtime::{
    PortableExecutionBinding, PortablePartitionBinding, PortablePartitionTask,
    PortablePartitionTaskInput, PortableSourceBinding, ReconstructedExecutionAuthority,
    ReconstructedWorkerTaskAuthority, VerifiedWorkerArtifactFacts, VerifiedWorkerSourceFacts,
    WorkerAdmissionVerifier, WorkerArtifactKind, WorkerArtifactReference, WorkerAttemptPolicy,
    WorkerCapabilityRequirements, WorkerCompatibility, WorkerExecutionArtifacts,
    WorkerInputCheckpointBinding, WorkerOutputPolicy, WorkerProcessedObservation,
    WorkerResourceBudget, WorkerSourceAttestation, artifact_hash,
};
use serde::{Deserialize, Serialize};
use std::any::Any;

use crate::{CanonicalSegmentationPolicy, EnginePlan};

/// Content-addressed compiler-artifact writer injected by the coordinator.
///
/// The engine owns artifact semantics and canonical bytes. Store location, durability, generation,
/// and transport stay outside the engine so local, object-store, and future distributed hosts use
/// the same compiler path.
pub trait WorkerCompilerArtifactWriter {
    fn write(
        &mut self,
        kind: WorkerArtifactKind,
        canonical_bytes: &[u8],
    ) -> Result<WorkerArtifactReference>;
}

/// Exact compiler bytes observed by a worker-owned content store.
pub struct VerifiedWorkerCompilerArtifact {
    bytes: Vec<u8>,
}

impl VerifiedWorkerCompilerArtifact {
    pub fn new(
        reference: &WorkerArtifactReference,
        bytes: Vec<u8>,
        observed_generation: Option<&cdf_kernel::ContentProviderGeneration>,
        maximum_bytes: u64,
    ) -> Result<Self> {
        reference.validate()?;
        if matches!(
            reference.kind,
            WorkerArtifactKind::PreparedSegment
                | WorkerArtifactKind::CanonicalSegment
                | WorkerArtifactKind::Quarantine
                | WorkerArtifactKind::Residual
                | WorkerArtifactKind::Verdict
                | WorkerArtifactKind::Lineage
        ) {
            return Err(CdfError::contract(
                "worker compiler reader cannot materialize output data artifacts",
            ));
        }
        let byte_count = u64::try_from(bytes.len())
            .map_err(|_| CdfError::contract("worker compiler artifact exceeds u64"))?;
        if byte_count != reference.byte_count || byte_count > maximum_bytes {
            return Err(CdfError::contract(
                "worker compiler artifact bytes exceed their reference or admitted memory bound",
            ));
        }
        let content_sha256 = format!(
            "sha256:{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(&bytes)
        );
        if content_sha256 != reference.content_sha256
            || reference.provider_generation.as_ref() != observed_generation
        {
            return Err(CdfError::contract(
                "worker compiler artifact bytes or generation do not match their reference",
            ));
        }
        Ok(Self { bytes })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Worker-host artifact authority used by the engine-owned protocol decoder.
///
/// Compiler artifacts are bounded and decoded by the engine. Output artifacts stay outside this
/// control path and are verified from hash-while-write/provider facts without forced rereads.
pub trait EngineWorkerArtifactAuthority {
    fn read_compiler_artifact(
        &self,
        reference: &WorkerArtifactReference,
        maximum_bytes: u64,
    ) -> Result<VerifiedWorkerCompilerArtifact>;

    fn verify_output_artifact(
        &self,
        reference: &WorkerArtifactReference,
    ) -> Result<VerifiedWorkerArtifactFacts>;
}

/// Source-specific observation verification behind the generic worker boundary.
pub trait EngineWorkerSourceAuthority {
    fn verify_source(
        &self,
        task: &PortablePartitionTask,
        source: &cdf_runtime::CompiledSourcePlan,
        partition: &cdf_kernel::PartitionPlan,
        attestation: &WorkerSourceAttestation,
        observations: &[WorkerProcessedObservation],
    ) -> Result<VerifiedWorkerSourceFacts>;
}

/// Engine-owned decoder and coordinator verifier for portable worker authority.
pub struct EngineWorkerAdmissionVerifier<'a> {
    artifacts: &'a dyn EngineWorkerArtifactAuthority,
    source: &'a dyn EngineWorkerSourceAuthority,
}

impl<'a> EngineWorkerAdmissionVerifier<'a> {
    pub fn new(
        artifacts: &'a dyn EngineWorkerArtifactAuthority,
        source: &'a dyn EngineWorkerSourceAuthority,
    ) -> Self {
        Self { artifacts, source }
    }

    fn decode<T: serde::de::DeserializeOwned>(
        &self,
        task: &PortablePartitionTask,
        reference: &WorkerArtifactReference,
    ) -> Result<T> {
        let artifact = self
            .artifacts
            .read_compiler_artifact(reference, task.resources.memory_bytes)?;
        serde_json::from_slice(artifact.bytes()).map_err(|error| {
            CdfError::contract(format!(
                "decode {:?} worker compiler artifact: {error}",
                reference.kind
            ))
        })
    }
}

impl WorkerAdmissionVerifier for EngineWorkerAdmissionVerifier<'_> {
    fn reconstruct_task_authority(
        &self,
        task: &PortablePartitionTask,
    ) -> Result<ReconstructedWorkerTaskAuthority> {
        let source: cdf_runtime::CompiledSourcePlan =
            self.decode(task, &task.source.compiled_source_plan)?;
        let partition: cdf_kernel::PartitionPlan =
            self.decode(task, &task.partition.partition_plan)?;
        let plan: EnginePlan = self.decode(task, &task.execution.artifacts.project_plan)?;
        let output_schema: crate::CompiledArrowSchema =
            self.decode(task, &task.execution.artifacts.output_schema)?;
        let validation_program: cdf_contract::ValidationProgram =
            self.decode(task, &task.execution.artifacts.validation_program)?;
        let normalization: WorkerNormalizationArtifact =
            self.decode(task, &task.execution.artifacts.normalization_policy)?;
        let compiled_expressions: cdf_contract::CompiledExpressionPlan =
            self.decode(task, &task.execution.artifacts.compiled_expression_plan)?;
        let operator_graph: cdf_runtime::CompiledOperatorGraph =
            self.decode(task, &task.execution.artifacts.operator_graph)?;
        let segmentation: CanonicalSegmentationPolicy =
            self.decode(task, &task.execution.artifacts.segmentation_policy)?;
        let execution_extent: cdf_kernel::ExecutionExtent =
            self.decode(task, &task.execution.artifacts.execution_extent)?;
        let decode_unit: WorkerDecodeUnitAuthorityArtifact =
            self.decode(task, &task.execution.artifacts.decode_unit_plan)?;
        let segment_plan: WorkerSegmentAuthorityArtifact =
            self.decode(task, &task.execution.artifacts.segment_plan)?;

        plan.validate_execution_extent_for_execution()?;
        plan.validate_compiled_expression_plan()?;
        plan.validate_partition_schedule()?;
        let planned_source = plan.compiled_source_execution.as_ref().ok_or_else(|| {
            CdfError::contract("worker project plan omitted compiled source execution")
        })?;
        let planned_graph = plan.operator_graph.as_ref().ok_or_else(|| {
            CdfError::contract("worker project plan omitted its compiled operator graph")
        })?;
        if plan.scan.plan_id != task.plan_id
            || plan.scan.request.resource_id != task.resource_id
            || plan.output_schema != output_schema
            || plan.validation_program != validation_program
            || normalization.normalizer_version != validation_program.normalizer_version
            || normalization.identifier_policy != validation_program.identifier_policy
            || plan.compiled_expression_plan != compiled_expressions
            || planned_graph != &operator_graph
            || plan.segmentation_policy()? != &segmentation
            || plan.execution_extent != execution_extent
            || planned_source != &decode_unit.compiled_source_execution
            || partition.scan_intent != decode_unit.partition_scan_intent
            || segment_plan.canonical_partition_ordinal
                != task.partition.canonical_partition_ordinal
            || segment_plan.segmentation != segmentation
            || planned_source.compiled_source_plan_hash() != artifact_hash(&source)?
        {
            return Err(CdfError::contract(
                "worker compiler artifacts do not form one coherent engine execution authority",
            ));
        }
        plan.partition_schedule
            .as_ref()
            .ok_or_else(|| CdfError::contract("worker project plan omitted partition schedule"))?
            .scheduled_partition(
                planned_source,
                usize::try_from(task.partition.canonical_partition_ordinal).map_err(|_| {
                    CdfError::contract("worker canonical partition ordinal exceeds usize")
                })?,
                &partition,
            )?;

        let execution = ReconstructedExecutionAuthority::from_verified_compiler_artifacts(
            artifact_hash(&plan)?,
            output_schema.arrow_schema_hash,
            artifact_hash(&validation_program)?,
            artifact_hash(&normalization)?,
            artifact_hash(&compiled_expressions)?,
            artifact_hash(&operator_graph)?,
            artifact_hash(&segmentation)?,
            artifact_hash(&execution_extent)?,
            artifact_hash(&decode_unit)?,
            artifact_hash(&segment_plan)?,
        )?;
        Ok(ReconstructedWorkerTaskAuthority::from_verified_artifacts(
            source,
            partition,
            execution,
            Box::new(ReconstructedEngineWorkerProgram { plan }),
        ))
    }

    fn verify_artifact(
        &self,
        reference: &WorkerArtifactReference,
    ) -> Result<VerifiedWorkerArtifactFacts> {
        match reference.kind {
            WorkerArtifactKind::PreparedSegment
            | WorkerArtifactKind::CanonicalSegment
            | WorkerArtifactKind::Quarantine
            | WorkerArtifactKind::Residual
            | WorkerArtifactKind::Verdict
            | WorkerArtifactKind::Lineage => self.artifacts.verify_output_artifact(reference),
            _ => {
                self.artifacts
                    .read_compiler_artifact(reference, reference.byte_count)?;
                VerifiedWorkerArtifactFacts::new(reference.clone(), None)
            }
        }
    }

    fn verify_source_authority(
        &self,
        task: &PortablePartitionTask,
        authority: &ReconstructedWorkerTaskAuthority,
        attestation: &WorkerSourceAttestation,
        observations: &[WorkerProcessedObservation],
    ) -> Result<VerifiedWorkerSourceFacts> {
        self.source.verify_source(
            task,
            authority.source(),
            authority.partition(),
            attestation,
            observations,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerNormalizationArtifact {
    pub normalizer_version: String,
    pub identifier_policy: cdf_contract::IdentifierPolicy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerDecodeUnitAuthorityArtifact {
    pub compiled_source_execution: cdf_runtime::CompiledSourceExecutionPlan,
    pub partition_scan_intent: cdf_kernel::CompiledScanIntent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSegmentAuthorityArtifact {
    pub canonical_partition_ordinal: u32,
    pub segmentation: CanonicalSegmentationPolicy,
}

/// Exact engine program handed to the isolated executor after artifact verification.
///
/// It is intentionally absent from the portable protocol type graph: the protocol carries the
/// canonical artifact references and hashes, while this owned value is reconstructed locally and
/// never serialized as worker control data.
pub struct ReconstructedEngineWorkerProgram {
    plan: EnginePlan,
}

impl cdf_runtime::ReconstructedWorkerExecutionProgram for ReconstructedEngineWorkerProgram {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReconstructedEngineWorkerProgram {
    pub fn plan(&self) -> &EnginePlan {
        &self.plan
    }
}

pub struct EnginePartitionTaskInput<'a> {
    pub compatibility: WorkerCompatibility,
    pub pipeline_id: PipelineId,
    pub source: &'a cdf_runtime::CompiledSourcePlan,
    pub plan: &'a EnginePlan,
    pub partition: &'a cdf_kernel::PartitionPlan,
    pub canonical_partition_ordinal: u32,
    pub epoch_ordinal: Option<u64>,
    pub input_checkpoint: Option<WorkerInputCheckpointBinding>,
    pub secret_references: Vec<SecretReference>,
    pub input_artifacts: Vec<WorkerArtifactReference>,
    pub resources: WorkerResourceBudget,
    pub attempt_policy: WorkerAttemptPolicy,
    pub capabilities: WorkerCapabilityRequirements,
    pub output_policy: WorkerOutputPolicy,
}

/// Compiles one engine partition into the neutral portable worker protocol.
///
/// Every semantic value is written as a typed content-addressed artifact. The returned task keeps
/// only bounded references and hashes; payload bytes never enter scheduler control messages.
pub fn compile_engine_partition_task(
    input: EnginePartitionTaskInput<'_>,
    writer: &mut dyn WorkerCompilerArtifactWriter,
) -> Result<PortablePartitionTask> {
    input.source.validate()?;
    input.plan.validate_execution_extent_for_execution()?;
    input.plan.validate_compiled_expression_plan()?;
    input.plan.validate_partition_schedule()?;
    input.partition.scan_intent.validate()?;
    let compiled_source_execution = input
        .plan
        .compiled_source_execution
        .as_ref()
        .ok_or_else(|| CdfError::contract("portable task requires compiled source execution"))?;
    if compiled_source_execution.compiled_source_plan_hash() != artifact_hash(input.source)? {
        return Err(CdfError::contract(
            "portable task source does not match the engine plan's compiler binding",
        ));
    }
    let schedule = input.plan.partition_schedule.as_ref().ok_or_else(|| {
        CdfError::contract("portable task requires a canonical partition schedule")
    })?;
    schedule.scheduled_partition(
        compiled_source_execution,
        usize::try_from(input.canonical_partition_ordinal)
            .map_err(|_| CdfError::contract("canonical partition ordinal exceeds usize"))?,
        input.partition,
    )?;
    let operator_graph =
        input.plan.operator_graph.as_ref().ok_or_else(|| {
            CdfError::contract("portable task requires a compiled operator graph")
        })?;
    let segmentation = input.plan.segmentation_policy()?.clone();
    let normalization = WorkerNormalizationArtifact {
        normalizer_version: input.plan.validation_program.normalizer_version.clone(),
        identifier_policy: input.plan.validation_program.identifier_policy.clone(),
    };
    let decode_unit = WorkerDecodeUnitAuthorityArtifact {
        compiled_source_execution: compiled_source_execution.clone(),
        partition_scan_intent: input.partition.scan_intent.clone(),
    };
    let segment_plan = WorkerSegmentAuthorityArtifact {
        canonical_partition_ordinal: input.canonical_partition_ordinal,
        segmentation: segmentation.clone(),
    };

    let source_reference =
        write_json_artifact(writer, WorkerArtifactKind::CompiledSourcePlan, input.source)?;
    let partition_reference =
        write_json_artifact(writer, WorkerArtifactKind::PartitionPlan, input.partition)?;
    let artifacts = WorkerExecutionArtifacts {
        project_plan: write_json_artifact(writer, WorkerArtifactKind::ProjectPlan, input.plan)?,
        output_schema: write_json_artifact(
            writer,
            WorkerArtifactKind::OutputSchema,
            &input.plan.output_schema,
        )?,
        validation_program: write_json_artifact(
            writer,
            WorkerArtifactKind::ValidationProgram,
            &input.plan.validation_program,
        )?,
        normalization_policy: write_json_artifact(
            writer,
            WorkerArtifactKind::NormalizationPolicy,
            &normalization,
        )?,
        compiled_expression_plan: write_json_artifact(
            writer,
            WorkerArtifactKind::CompiledExpressionPlan,
            &input.plan.compiled_expression_plan,
        )?,
        operator_graph: write_json_artifact(
            writer,
            WorkerArtifactKind::OperatorGraph,
            operator_graph,
        )?,
        segmentation_policy: write_json_artifact(
            writer,
            WorkerArtifactKind::SegmentationPolicy,
            &segmentation,
        )?,
        execution_extent: write_json_artifact(
            writer,
            WorkerArtifactKind::ExecutionExtent,
            &input.plan.execution_extent,
        )?,
        decode_unit_plan: write_json_artifact(
            writer,
            WorkerArtifactKind::DecodeUnitPlan,
            &decode_unit,
        )?,
        segment_plan: write_json_artifact(writer, WorkerArtifactKind::SegmentPlan, &segment_plan)?,
    };
    let project_identity_hash = artifact_hash(input.plan)?;
    let validation_program_hash = artifact_hash(&input.plan.validation_program)?;
    let normalization_policy_hash = artifact_hash(&normalization)?;
    let compiled_expression_plan_hash = artifact_hash(&input.plan.compiled_expression_plan)?;
    let operator_graph_hash = artifact_hash(operator_graph)?;
    let segmentation_policy_hash = artifact_hash(&segmentation)?;
    let execution_extent_hash = artifact_hash(&input.plan.execution_extent)?;
    let unit_authority_hash = artifact_hash(&decode_unit)?;
    let segment_authority_hash = artifact_hash(&segment_plan)?;

    PortablePartitionTask::new(PortablePartitionTaskInput {
        compatibility: input.compatibility,
        pipeline_id: input.pipeline_id,
        resource_id: input.plan.scan.request.resource_id.clone(),
        plan_id: input.plan.scan.plan_id.clone(),
        source: PortableSourceBinding {
            driver_id: input.source.driver.driver_id.clone(),
            driver_version: input.source.driver.driver_version.clone(),
            option_schema_hash: input.source.driver.option_schema_hash.clone(),
            compiled_source_plan: source_reference,
            redacted_options_hash: input.source.redacted_options_hash.clone(),
            physical_plan_hash: input.source.physical_plan_hash.clone(),
            source_semantics_hash: input.source.schema_binding_stable_hash()?,
            execution_capabilities_hash: artifact_hash(&input.source.execution_capabilities)?,
        },
        partition: PortablePartitionBinding {
            partition_id: input.partition.partition_id.clone(),
            scope: input.partition.scope.clone(),
            canonical_partition_ordinal: input.canonical_partition_ordinal,
            epoch_ordinal: input.epoch_ordinal,
            partition_plan: partition_reference,
            source_identity_hash: partition_source_identity_binding(input.partition)?,
            unit_authority_hash,
            segment_authority_hash,
        },
        execution: PortableExecutionBinding {
            project_identity_hash,
            artifacts,
            output_schema_hash: input.plan.output_schema.arrow_schema_hash.clone(),
            validation_program_hash,
            normalization_policy_hash,
            compiled_expression_plan_hash,
            operator_graph_hash,
            segmentation_policy_hash,
            execution_extent_hash,
        },
        input_checkpoint: input.input_checkpoint,
        secret_references: input.secret_references,
        input_artifacts: input.input_artifacts,
        resources: input.resources,
        attempt_policy: input.attempt_policy,
        capabilities: input.capabilities,
        output_policy: input.output_policy,
    })
}

fn write_json_artifact<T: Serialize>(
    writer: &mut dyn WorkerCompilerArtifactWriter,
    kind: WorkerArtifactKind,
    value: &T,
) -> Result<WorkerArtifactReference> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| CdfError::internal(format!("encode {kind:?} artifact: {error}")))?;
    let expected_hash = artifact_hash(value)?;
    let reference = writer.write(kind, &bytes)?;
    reference.validate()?;
    if reference.kind != kind
        || reference.content_sha256 != expected_hash
        || reference.byte_count
            != u64::try_from(bytes.len())
                .map_err(|_| CdfError::contract("worker compiler artifact exceeds u64"))?
    {
        return Err(CdfError::contract(format!(
            "worker compiler artifact store returned a receipt that does not match {kind:?} bytes"
        )));
    }
    Ok(reference)
}
