use cdf_kernel::{
    CdfError, PipelineId, Result, SecretReference, partition_source_identity_binding,
};
use cdf_runtime::{
    AdmittedPartitionWorkerResult, PortableExecutionBinding, PortablePartitionBinding,
    PortablePartitionTask, PortablePartitionTaskInput, PortableSegmentTask,
    PortableSegmentTaskInput, PortableSourceBinding, ReconstructedExecutionAuthority,
    ReconstructedSegmentTask, ReconstructedWorkerTaskAuthority, SegmentTaskReconstructor,
    VerifiedCanonicalSegmentFacts, VerifiedWorkerArtifactFacts, VerifiedWorkerSourceFacts,
    WorkerAdmissionVerifier, WorkerArtifactKind, WorkerArtifactReference, WorkerArtifactRole,
    WorkerArtifactWriteAuthorization, WorkerArtifactWriteSession, WorkerAttemptPolicy,
    WorkerAuthorizedArtifactSink, WorkerCapabilityRequirements, WorkerCompatibility,
    WorkerExecutionArtifacts, WorkerInputCheckpointBinding, WorkerLeaseState, WorkerOutputPolicy,
    WorkerOutputVerifier, WorkerProcessedObservation, WorkerResourceBudget,
    WorkerSourceAttestation, artifact_hash,
};
use serde::{Deserialize, Serialize};
use std::any::Any;

use crate::{
    CanonicalSegmentationPolicy, CompiledSchemaQuarantineEvidence, CompiledStreamAdmissionEvidence,
    ENGINE_PARTITION_EVIDENCE_VERSION, EnginePartitionDrainEvidence, EnginePartitionEvidence,
    EnginePlan, EngineRunOutputWithSegmentPositions, EngineSegmentPosition,
};

impl EnginePartitionEvidence {
    pub fn from_execution(
        task: &PortablePartitionTask,
        plan: &EnginePlan,
        output: &EngineRunOutputWithSegmentPositions,
        stream_admission: CompiledStreamAdmissionEvidence,
        schema_quarantine_evidence: Option<CompiledSchemaQuarantineEvidence>,
    ) -> Result<Self> {
        let segmentation = plan.segmentation_policy()?;
        let canonical_partition_ordinal = task.partition.canonical_partition_ordinal;
        let mut lineage = output.output.lineage.clone();
        lineage.output_segments = (0..output.output.segments.len())
            .map(|ordinal| {
                segmentation.segment_id(
                    canonical_partition_ordinal,
                    u32::try_from(ordinal)
                        .map_err(|_| CdfError::data("partition segment ordinal exceeds u32"))?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let segment_positions = output
            .segment_positions
            .iter()
            .enumerate()
            .map(|(ordinal, position)| {
                Ok(EngineSegmentPosition {
                    segment_id: segmentation.segment_id(
                        canonical_partition_ordinal,
                        u32::try_from(ordinal).map_err(|_| {
                            CdfError::data("partition segment position ordinal exceeds u32")
                        })?,
                    )?,
                    partition_ordinal: canonical_partition_ordinal,
                    output_position: position.output_position.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let drain = output
            .drain_epoch
            .as_ref()
            .map(|epoch| EnginePartitionDrainEvidence {
                frontier: epoch.closure.frontier.clone(),
                closure: epoch.closure.evidence.clone(),
                observed_at_unix_milliseconds: epoch.closure.observed_at_unix_milliseconds,
                terminate_after_settlement: epoch.closure.terminate_after_settlement,
                consumed_partition_count: epoch.consumed_partition_count,
                resume_partition: epoch.resume_partition.as_deref().cloned(),
                consumed_late_data_carryover: epoch.consumed_late_data_carryover.clone(),
                late_data_carryover: epoch.late_data_carryover.clone(),
                partition_watermarks: epoch.partition_watermarks.clone(),
            });
        let draft = Self {
            version: ENGINE_PARTITION_EVIDENCE_VERSION,
            partition_id: task.partition.partition_id.clone(),
            canonical_partition_ordinal,
            profile: output.output.profile.clone(),
            lineage,
            segment_positions,
            processed_observations: output
                .execution_evidence()
                .processed_observations()
                .to_vec(),
            source_retries: output.execution_evidence().source_retries().to_vec(),
            checkpoint_eligible: output.execution_evidence().checkpoint_eligible(),
            stream_admission,
            terminal_schema_quarantines: output.output.terminal_schema_quarantines.clone(),
            schema_quarantine_evidence,
            phase_metrics: output.phase_metrics.clone(),
            source_frontier: output.source_frontier.clone(),
            drain,
        };
        draft.validate(task, plan, None)?;
        Ok(draft)
    }

    pub fn validate(
        &self,
        task: &PortablePartitionTask,
        plan: &EnginePlan,
        result: Option<&cdf_runtime::PartitionWorkerResult>,
    ) -> Result<()> {
        self.stream_admission
            .validate(&plan.compiled_schema_admission)?;
        let partition_matches = self.version == ENGINE_PARTITION_EVIDENCE_VERSION
            && self.partition_id == task.partition.partition_id
            && self.canonical_partition_ordinal == task.partition.canonical_partition_ordinal
            && self
                .lineage
                .input_partitions
                .iter()
                .all(|partition| partition == &self.partition_id)
            && self
                .segment_positions
                .iter()
                .all(|position| position.partition_ordinal == self.canonical_partition_ordinal)
            && self.lineage.output_segments
                == self
                    .segment_positions
                    .iter()
                    .map(|position| position.segment_id.clone())
                    .collect::<Vec<_>>();
        if !partition_matches {
            return Err(CdfError::contract(
                "partition evidence does not match its portable task or canonical segments",
            ));
        }
        if self.terminal_schema_quarantines.is_empty() != self.schema_quarantine_evidence.is_none()
        {
            return Err(CdfError::contract(
                "partition schema quarantine rows and evidence must be present together",
            ));
        }
        if let Some(result) = result {
            let prepared_segments = result
                .artifacts
                .iter()
                .filter_map(|receipt| match &receipt.role {
                    WorkerArtifactRole::PreparedSegment { segment_id, .. } => {
                        Some(segment_id.clone())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let worker_observations = self
                .processed_observations
                .iter()
                .map(|observation| {
                    WorkerProcessedObservation::new(
                        observation.observation_id.clone(),
                        observation.outcome.clone(),
                        cdf_runtime::WorkerPosition::inline(observation.source_position.clone())?,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            if self.lineage.input_rows != result.counts.input_rows
                || self.profile.output_rows != result.counts.output_rows
                || worker_observations != result.processed_observations
                || self.lineage.output_segments != prepared_segments
            {
                return Err(CdfError::contract(
                    "partition evidence does not match its admitted worker result",
                ));
            }
        }
        Ok(())
    }
}

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

pub struct VerifiedEnginePartitionEvidenceArtifact {
    evidence: EnginePartitionEvidence,
}

impl VerifiedEnginePartitionEvidenceArtifact {
    pub fn new(
        reference: &WorkerArtifactReference,
        bytes: Vec<u8>,
        observed_generation: Option<&cdf_kernel::ContentProviderGeneration>,
        maximum_bytes: u64,
    ) -> Result<Self> {
        reference.validate()?;
        if reference.kind != WorkerArtifactKind::PartitionEvidence {
            return Err(CdfError::contract(
                "partition evidence reader requires a PartitionEvidence reference",
            ));
        }
        let byte_count = u64::try_from(bytes.len())
            .map_err(|_| CdfError::contract("partition evidence exceeds u64"))?;
        let content_sha256 = format!(
            "sha256:{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(&bytes)
        );
        if byte_count != reference.byte_count
            || byte_count > maximum_bytes
            || content_sha256 != reference.content_sha256
            || reference.provider_generation.as_ref() != observed_generation
        {
            return Err(CdfError::contract(
                "partition evidence bytes, generation, or size do not match authority",
            ));
        }
        let evidence = serde_json::from_slice(&bytes)
            .map_err(|error| CdfError::contract(format!("decode partition evidence: {error}")))?;
        Ok(Self { evidence })
    }

    pub fn into_evidence(self) -> EnginePartitionEvidence {
        self.evidence
    }
}

/// Prepared Arrow rows read and verified from the worker artifact authority.
pub struct VerifiedPreparedSegmentArtifact {
    facts: VerifiedWorkerArtifactFacts,
    batches: Vec<arrow_array::RecordBatch>,
}

/// Exact canonical-segment semantics observed from verified IPC bytes.
pub struct VerifiedCanonicalSegmentArtifact {
    facts: VerifiedCanonicalSegmentFacts,
    bytes: Vec<u8>,
}

impl VerifiedCanonicalSegmentArtifact {
    pub fn new(
        reference: &WorkerArtifactReference,
        bytes: Vec<u8>,
        observed_generation: Option<&cdf_kernel::ContentProviderGeneration>,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
    ) -> Result<Self> {
        reference.validate()?;
        if reference.kind != WorkerArtifactKind::CanonicalSegment {
            return Err(CdfError::contract(
                "canonical segment reader requires a CanonicalSegment reference",
            ));
        }
        let byte_count = u64::try_from(bytes.len())
            .map_err(|_| CdfError::contract("canonical segment exceeds u64"))?;
        let content_sha256 = format!(
            "sha256:{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(&bytes)
        );
        if byte_count != reference.byte_count
            || byte_count > maximum_encoded_bytes
            || content_sha256 != reference.content_sha256
            || reference.provider_generation.as_ref() != observed_generation
        {
            return Err(CdfError::contract(
                "canonical segment bytes, generation, or encoded size do not match authority",
            ));
        }

        let mut reader =
            arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes.as_slice()), None)
                .map_err(CdfError::from)?;
        let mut decoded_bytes = 0_u64;
        let mut row_count = 0_u64;
        let mut batches = Vec::new();
        for batch in &mut reader {
            let batch = batch.map_err(CdfError::from)?;
            row_count = row_count
                .checked_add(
                    u64::try_from(batch.num_rows())
                        .map_err(|_| CdfError::data("canonical segment row count exceeds u64"))?,
                )
                .ok_or_else(|| CdfError::data("canonical segment row count overflow"))?;
            decoded_bytes =
                decoded_bytes
                    .checked_add(u64::try_from(batch.get_array_memory_size()).map_err(|_| {
                        CdfError::data("canonical segment decoded bytes exceed u64")
                    })?)
                    .ok_or_else(|| CdfError::data("canonical segment decoded bytes overflow"))?;
            if decoded_bytes > maximum_decoded_bytes {
                return Err(CdfError::data(
                    "canonical segment exceeds its admitted decoded-memory budget",
                ));
            }
            batches.push(batch);
        }
        let first = batches
            .first()
            .ok_or_else(|| CdfError::data("canonical segment contains no rows"))?;
        let package_row_ord_start = cdf_package_contract::package_row_ord_array(first)?
            .values()
            .first()
            .copied()
            .ok_or_else(|| CdfError::data("canonical segment contains an empty first batch"))?;
        cdf_package_contract::validate_package_row_ord_batches(
            &batches,
            package_row_ord_start,
            row_count,
        )?;
        let logical_schema = cdf_package_contract::logical_output_schema(first.schema().as_ref())?;
        let logical_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&logical_schema)?;
        Ok(Self {
            facts: VerifiedCanonicalSegmentFacts::new(
                reference.clone(),
                row_count,
                logical_schema_hash,
                package_row_ord_start,
            )?,
            bytes,
        })
    }

    pub fn into_facts(self) -> VerifiedCanonicalSegmentFacts {
        self.facts
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl VerifiedPreparedSegmentArtifact {
    pub fn new(
        reference: &WorkerArtifactReference,
        bytes: Vec<u8>,
        observed_generation: Option<&cdf_kernel::ContentProviderGeneration>,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
    ) -> Result<Self> {
        reference.validate()?;
        if reference.kind != WorkerArtifactKind::PreparedSegment {
            return Err(CdfError::contract(
                "prepared segment reader requires a PreparedSegment reference",
            ));
        }
        let byte_count = u64::try_from(bytes.len())
            .map_err(|_| CdfError::contract("prepared segment exceeds u64"))?;
        let content_sha256 = format!(
            "sha256:{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(&bytes)
        );
        if byte_count != reference.byte_count
            || byte_count > maximum_encoded_bytes
            || content_sha256 != reference.content_sha256
            || reference.provider_generation.as_ref() != observed_generation
        {
            return Err(CdfError::contract(
                "prepared segment bytes, generation, or encoded size do not match authority",
            ));
        }
        let mut reader = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(CdfError::from)?;
        let mut decoded_bytes = 0_u64;
        let mut row_count = 0_u64;
        let mut batches = Vec::new();
        for batch in &mut reader {
            let batch = batch.map_err(CdfError::from)?;
            row_count = row_count
                .checked_add(
                    u64::try_from(batch.num_rows())
                        .map_err(|_| CdfError::data("prepared segment row count exceeds u64"))?,
                )
                .ok_or_else(|| CdfError::data("prepared segment row count overflow"))?;
            decoded_bytes = decoded_bytes
                .checked_add(
                    u64::try_from(batch.get_array_memory_size())
                        .map_err(|_| CdfError::data("prepared segment decoded bytes exceed u64"))?,
                )
                .ok_or_else(|| CdfError::data("prepared segment decoded bytes overflow"))?;
            if decoded_bytes > maximum_decoded_bytes {
                return Err(CdfError::data(
                    "prepared segment exceeds its admitted decoded-memory budget",
                ));
            }
            batches.push(batch);
        }
        if row_count == 0 || batches.is_empty() {
            return Err(CdfError::data("prepared segment contains no rows"));
        }
        Ok(Self {
            facts: VerifiedWorkerArtifactFacts::new(reference.clone(), Some(row_count))?,
            batches,
        })
    }

    pub fn facts(&self) -> &VerifiedWorkerArtifactFacts {
        &self.facts
    }

    pub fn batches(&self) -> &[arrow_array::RecordBatch] {
        &self.batches
    }

    pub fn into_batches(self) -> Vec<arrow_array::RecordBatch> {
        self.batches
    }
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

    fn read_prepared_segment(
        &self,
        reference: &WorkerArtifactReference,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
    ) -> Result<VerifiedPreparedSegmentArtifact>;

    fn read_canonical_segment(
        &self,
        reference: &WorkerArtifactReference,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
    ) -> Result<VerifiedCanonicalSegmentArtifact>;

    fn read_partition_evidence(
        &self,
        reference: &WorkerArtifactReference,
        maximum_bytes: u64,
    ) -> Result<VerifiedEnginePartitionEvidenceArtifact>;
}

/// Provider boundary for fenced worker output publication.
pub trait EngineWorkerOutputAuthority: EngineWorkerArtifactAuthority {
    fn reference_for_bytes(
        &self,
        kind: WorkerArtifactKind,
        namespace: &cdf_kernel::ContentStoreNamespace,
        object_key: cdf_kernel::ContentObjectKey,
        bytes: &[u8],
    ) -> Result<WorkerArtifactReference>;

    fn object_state(
        &self,
        reference: &WorkerArtifactReference,
    ) -> Result<cdf_runtime::WorkerArtifactObjectState>;

    fn write_authorized_bytes(
        &self,
        authorization: WorkerArtifactWriteAuthorization<'_>,
        bytes: Vec<u8>,
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

    pub fn read_partition_evidence(
        &self,
        task: &PortablePartitionTask,
        plan: &EnginePlan,
        result: &cdf_runtime::PartitionWorkerResult,
    ) -> Result<EnginePartitionEvidence> {
        let mut references = result.artifacts.iter().filter_map(|receipt| {
            matches!(receipt.role, WorkerArtifactRole::PartitionEvidence { .. })
                .then_some(&receipt.artifact)
        });
        let reference = references.next().ok_or_else(|| {
            CdfError::contract("admitted partition result lacks partition evidence")
        })?;
        if references.next().is_some() {
            return Err(CdfError::contract(
                "admitted partition result contains more than one partition evidence artifact",
            ));
        }
        let evidence = self
            .artifacts
            .read_partition_evidence(reference, task.resources.disk_bytes)?
            .into_evidence();
        evidence.validate(task, plan, Some(result))?;
        Ok(evidence)
    }

    fn decode<T: serde::de::DeserializeOwned>(
        &self,
        task: &PortablePartitionTask,
        reference: &WorkerArtifactReference,
    ) -> Result<T> {
        self.decode_reference(reference, task.resources.memory_bytes)
    }

    fn decode_reference<T: serde::de::DeserializeOwned>(
        &self,
        reference: &WorkerArtifactReference,
        maximum_bytes: u64,
    ) -> Result<T> {
        let artifact = self
            .artifacts
            .read_compiler_artifact(reference, maximum_bytes)?;
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
            partition.clone(),
            execution,
            Box::new(ReconstructedEngineWorkerProgram {
                plan,
                partition,
                canonical_partition_ordinal: task.partition.canonical_partition_ordinal,
            }),
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
            | WorkerArtifactKind::Lineage
            | WorkerArtifactKind::PartitionEvidence => {
                self.artifacts.verify_output_artifact(reference)
            }
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

/// Exact engine program reconstructed for source-free canonical segment finalization.
pub struct ReconstructedEngineSegmentProgram {
    logical_schema: std::sync::Arc<arrow_schema::Schema>,
    segmentation: CanonicalSegmentationPolicy,
    prepared_batches: Vec<arrow_array::RecordBatch>,
}

impl cdf_runtime::ReconstructedWorkerExecutionProgram for ReconstructedEngineSegmentProgram {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReconstructedEngineSegmentProgram {
    pub fn logical_schema(&self) -> &std::sync::Arc<arrow_schema::Schema> {
        &self.logical_schema
    }

    pub fn segmentation(&self) -> &CanonicalSegmentationPolicy {
        &self.segmentation
    }

    pub fn prepared_batches(&self) -> &[arrow_array::RecordBatch] {
        &self.prepared_batches
    }

    pub fn into_prepared_batches(self) -> Vec<arrow_array::RecordBatch> {
        self.prepared_batches
    }
}

struct PendingEngineWorkerOutput<'a> {
    store: &'a dyn EngineWorkerOutputAuthority,
    bytes: Option<Vec<u8>>,
}

impl WorkerAuthorizedArtifactSink for PendingEngineWorkerOutput<'_> {
    fn write_authorized(
        &mut self,
        authorization: WorkerArtifactWriteAuthorization<'_>,
    ) -> Result<VerifiedWorkerArtifactFacts> {
        self.store.write_authorized_bytes(
            authorization,
            self.bytes
                .take()
                .ok_or_else(|| CdfError::internal("worker output bytes were already consumed"))?,
        )
    }
}

/// Engine-owned source-free finalizer for one prefix-bound prepared segment.
pub struct EngineIsolatedSegmentExecutor<'a> {
    store: &'a dyn EngineWorkerOutputAuthority,
    lease: &'a WorkerLeaseState,
    now_ms: i64,
}

impl<'a> EngineIsolatedSegmentExecutor<'a> {
    pub fn new(
        store: &'a dyn EngineWorkerOutputAuthority,
        lease: &'a WorkerLeaseState,
        now_ms: i64,
    ) -> Self {
        Self {
            store,
            lease,
            now_ms,
        }
    }
}

impl cdf_runtime::IsolatedSegmentExecutor for EngineIsolatedSegmentExecutor<'_> {
    fn execute(
        &self,
        invocation: cdf_runtime::IsolatedSegmentInvocation,
    ) -> cdf_kernel::BoxFuture<'_, Result<cdf_runtime::SegmentWorkerResult>> {
        let result = (|| {
            let (task, attempt, reconstructed) = invocation.into_parts();
            let program = reconstructed.execution_program::<ReconstructedEngineSegmentProgram>()?;
            if program
                .segmentation()
                .segment_id(task.canonical_partition_ordinal, task.segment_ordinal)?
                != task.segment_id
            {
                return Err(CdfError::contract(
                    "segment finalizer task id exceeds reconstructed segmentation authority",
                ));
            }
            let observed_rows =
                program
                    .prepared_batches()
                    .iter()
                    .try_fold(0_u64, |total, batch| {
                        total
                            .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                                CdfError::data("prepared batch row count exceeds u64")
                            })?)
                            .ok_or_else(|| CdfError::data("prepared segment row count overflow"))
                    })?;
            if observed_rows != task.row_count {
                return Err(CdfError::contract(
                    "prepared segment rows changed after worker reconstruction",
                ));
            }
            let canonical = cdf_package_contract::append_package_row_ord(
                program.prepared_batches().to_vec(),
                task.package_row_ord_start,
            )?;
            let mut bytes = Vec::new();
            cdf_package::encode_canonical_segment_ipc(
                &mut bytes,
                canonical[0].schema().as_ref(),
                &canonical,
            )?;
            let object_key = cdf_kernel::ContentObjectKey::new(format!(
                "{}data/{}.arrow",
                attempt.write_permit.output.object_key_prefix,
                task.segment_id.as_str()
            ))?;
            let reference = self.store.reference_for_bytes(
                WorkerArtifactKind::CanonicalSegment,
                &attempt.write_permit.output.store_namespace,
                object_key,
                &bytes,
            )?;
            let receipt = cdf_runtime::WorkerArtifactReceipt {
                role: WorkerArtifactRole::CanonicalSegment {
                    segment_id: task.segment_id.clone(),
                    partition_ordinal: task.canonical_partition_ordinal,
                    segment_ordinal: task.segment_ordinal,
                    row_count: task.row_count,
                },
                artifact: reference,
            };
            let object_state = self.store.object_state(&receipt.artifact)?;
            let mut sink = PendingEngineWorkerOutput {
                store: self.store,
                bytes: Some(bytes),
            };
            let mut session =
                WorkerArtifactWriteSession::new(&task, &attempt, self.lease, self.now_ms)?;
            session.write(&receipt, &object_state, self.now_ms, &mut sink)?;
            cdf_runtime::SegmentWorkerResult::new(
                &attempt,
                cdf_runtime::WorkerTerminalStatus::Succeeded,
                Some(receipt),
                cdf_runtime::WorkerTelemetry::default(),
            )
        })();
        Box::pin(async move { result })
    }
}

impl SegmentTaskReconstructor for EngineWorkerAdmissionVerifier<'_> {
    fn reconstruct_segment_task(
        &self,
        task: &PortableSegmentTask,
    ) -> Result<ReconstructedSegmentTask> {
        let output_schema: crate::CompiledArrowSchema =
            self.decode_reference(&task.output_schema, task.resources.memory_bytes)?;
        let segmentation: CanonicalSegmentationPolicy =
            self.decode_reference(&task.segmentation_policy, task.resources.memory_bytes)?;
        if output_schema.arrow_schema_hash != task.output_schema_hash
            || artifact_hash(&segmentation)? != task.segmentation_policy_hash
        {
            return Err(CdfError::contract(
                "segment task compiler artifacts do not match their semantic bindings",
            ));
        }
        segmentation.validate()?;
        let logical_schema = output_schema.to_arrow()?;
        cdf_package_contract::validate_logical_output_schema(logical_schema.as_ref())?;
        let prepared = self.artifacts.read_prepared_segment(
            &task.prepared_segment,
            task.resources.disk_bytes,
            task.resources.memory_bytes,
        )?;
        if prepared.batches().iter().any(|batch| {
            batch.schema().as_ref() != logical_schema.as_ref()
                || batch
                    .schema()
                    .field_with_name(cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD)
                    .is_ok()
        }) {
            return Err(CdfError::contract(
                "prepared segment schema is not the task's exact logical output schema",
            ));
        }
        Ok(ReconstructedSegmentTask::from_verified_artifacts(
            prepared.facts().clone(),
            VerifiedWorkerArtifactFacts::new(task.output_schema.clone(), None)?,
            VerifiedWorkerArtifactFacts::new(task.segmentation_policy.clone(), None)?,
            Box::new(ReconstructedEngineSegmentProgram {
                logical_schema,
                segmentation,
                prepared_batches: prepared.into_batches(),
            }),
        ))
    }
}

impl WorkerOutputVerifier for EngineWorkerAdmissionVerifier<'_> {
    fn verify_canonical_segment(
        &self,
        task: &PortableSegmentTask,
        reference: &WorkerArtifactReference,
    ) -> Result<VerifiedCanonicalSegmentFacts> {
        self.artifacts
            .read_canonical_segment(
                reference,
                task.resources
                    .disk_bytes
                    .min(task.output_policy.maximum_artifact_bytes),
                task.resources.memory_bytes,
            )
            .map(VerifiedCanonicalSegmentArtifact::into_facts)
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
    partition: cdf_kernel::PartitionPlan,
    canonical_partition_ordinal: u32,
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

    /// Derives the nonidentity execution slice for exactly the partition bound by the capsule.
    ///
    /// The verified full plan remains the semantic authority. The slice only narrows source work;
    /// it is never serialized, hashed as a project/package plan, or accepted for package commit.
    pub fn partition_execution_plan(&self) -> Result<EnginePlan> {
        validate_partition_isolation(&self.plan)?;
        if self.plan.scan.planned_task_set.is_some() {
            return Err(CdfError::contract(
                "isolated external task-set execution requires source-owned retained task reconstruction",
            ));
        }
        let source = self
            .plan
            .compiled_source_execution
            .as_ref()
            .ok_or_else(|| CdfError::contract("isolated engine plan lacks compiled source"))?;
        self.plan
            .partition_schedule
            .as_ref()
            .ok_or_else(|| CdfError::contract("isolated engine plan lacks partition schedule"))?
            .scheduled_partition(
                source,
                usize::try_from(self.canonical_partition_ordinal)
                    .map_err(|_| CdfError::contract("isolated partition ordinal exceeds usize"))?,
                &self.partition,
            )?;
        let mut slice = self.plan.clone();
        slice.scan.partitions = vec![self.partition.clone()];
        let schedule = cdf_runtime::CanonicalPartitionSchedule::compile(source, &slice.scan)?;
        slice.partition_schedule = Some(schedule.clone());
        slice.explain.partition_schedule = Some(schedule);
        slice.validate_partition_schedule()?;
        Ok(slice)
    }
}

fn validate_partition_isolation(plan: &EnginePlan) -> Result<()> {
    if plan.scan.partition_count()? <= 1 {
        return Ok(());
    }
    if plan.scan.request.limit.is_some()
        || plan.validation_program.has_exact_row_dedup_rule()
        || (plan.write_disposition == cdf_kernel::WriteDisposition::Merge
            && plan.validation_program.has_keyed_dedup_rule())
        || !matches!(
            plan.execution_extent,
            cdf_kernel::ExecutionExtent::Bounded { .. }
        )
    {
        return Err(CdfError::contract(
            "multi-partition isolation requires partition-separable bounded semantics; package-global limit, deduplication, and drain policies require an explicit canonical global-operator or epoch task",
        ));
    }
    Ok(())
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
    validate_partition_isolation(input.plan)?;
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

pub struct EngineSegmentTaskInput<'a> {
    pub plan: &'a EnginePlan,
    pub preparation_task: &'a PortablePartitionTask,
    pub preparation_result: &'a AdmittedPartitionWorkerResult,
    pub segment_ordinal: u32,
    pub package_row_ord_start: u64,
    pub resources: WorkerResourceBudget,
    pub attempt_policy: WorkerAttemptPolicy,
    pub capabilities: WorkerCapabilityRequirements,
    pub output_policy: WorkerOutputPolicy,
}

/// Compiles one independently admitted preparation artifact into a source-free finalization task.
pub fn compile_engine_segment_task(
    input: EngineSegmentTaskInput<'_>,
) -> Result<PortableSegmentTask> {
    input.plan.validate_execution_extent_for_execution()?;
    input.plan.validate_partition_schedule()?;
    input.preparation_task.validate()?;
    let result = input.preparation_result.result();
    result.validate()?;
    if result.task_sha256 != input.preparation_task.task_sha256
        || input.plan.scan.plan_id != input.preparation_task.plan_id
        || input.plan.scan.request.resource_id != input.preparation_task.resource_id
        || input.plan.output_schema.arrow_schema_hash
            != input.preparation_task.execution.output_schema_hash
        || artifact_hash(input.plan.segmentation_policy()?)?
            != input.preparation_task.execution.segmentation_policy_hash
    {
        return Err(CdfError::contract(
            "admitted preparation result and engine plan do not form one finalization authority",
        ));
    }
    let partition_ordinal = input.preparation_task.partition.canonical_partition_ordinal;
    let expected_segment_id = input
        .plan
        .segmentation_policy()?
        .segment_id(partition_ordinal, input.segment_ordinal)?;
    let mut matching = result.artifacts.iter().filter(|receipt| {
        matches!(
            &receipt.role,
            WorkerArtifactRole::PreparedSegment {
                segment_id,
                partition_ordinal: receipt_partition,
                segment_ordinal,
                ..
            } if segment_id == &expected_segment_id
                && *receipt_partition == partition_ordinal
                && *segment_ordinal == input.segment_ordinal
        )
    });
    let receipt = matching.next().ok_or_else(|| {
        CdfError::contract("admitted preparation result lacks the requested prepared segment")
    })?;
    if matching.next().is_some() {
        return Err(CdfError::contract(
            "admitted preparation result contains duplicate prepared segment authority",
        ));
    }
    let WorkerArtifactRole::PreparedSegment { row_count, .. } = &receipt.role else {
        unreachable!("matching filter admits only prepared segment roles")
    };

    PortableSegmentTask::new(PortableSegmentTaskInput {
        compatibility: input.preparation_task.compatibility.clone(),
        pipeline_id: input.preparation_task.pipeline_id.clone(),
        resource_id: input.preparation_task.resource_id.clone(),
        plan_id: input.preparation_task.plan_id.clone(),
        partition_id: input.preparation_task.partition.partition_id.clone(),
        scope: input.preparation_task.partition.scope.clone(),
        canonical_partition_ordinal: partition_ordinal,
        segment_id: expected_segment_id,
        segment_ordinal: input.segment_ordinal,
        row_count: *row_count,
        prepared_segment: receipt.artifact.clone(),
        preparation_result_sha256: result.result_sha256.clone(),
        package_row_ord_start: input.package_row_ord_start,
        output_schema: input
            .preparation_task
            .execution
            .artifacts
            .output_schema
            .clone(),
        output_schema_hash: input.plan.output_schema.arrow_schema_hash.clone(),
        segmentation_policy: input
            .preparation_task
            .execution
            .artifacts
            .segmentation_policy
            .clone(),
        segmentation_policy_hash: artifact_hash(input.plan.segmentation_policy()?)?,
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
