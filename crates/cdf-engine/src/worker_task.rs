use cdf_kernel::{
    CdfError, EpochClosureCause, ExecutionExtent, PipelineId, Result, SecretReference,
    partition_schema_observation_binding,
};
use cdf_memory::{AccountedBytes, MemoryLease};
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
        let lineage = output.output.lineage.clone();
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
                .input_observations
                .iter()
                .all(|observation| observation.partition_id == self.partition_id)
            && self
                .segment_positions
                .iter()
                .all(|position| position.partition_ordinal == self.canonical_partition_ordinal);
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
        let admitted_observations = self
            .processed_observations
            .iter()
            .filter(|observation| {
                observation.outcome == cdf_kernel::ProcessedObservationOutcome::Admitted
            })
            .map(|observation| observation.observation_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let quarantined_observations = self
            .processed_observations
            .iter()
            .filter(|observation| {
                observation.outcome == cdf_kernel::ProcessedObservationOutcome::Quarantined
            })
            .map(|observation| observation.observation_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let stream_observations = self
            .stream_admission
            .observations
            .iter()
            .map(|observation| observation.observation_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let schema_quarantines = self
            .terminal_schema_quarantines
            .iter()
            .map(cdf_kernel::TerminalSchemaObservationQuarantine::observation_id)
            .collect::<std::collections::BTreeSet<_>>();
        if admitted_observations != stream_observations
            || quarantined_observations != schema_quarantines
            || self.processed_observations.len()
                != admitted_observations.len() + quarantined_observations.len()
        {
            return Err(CdfError::contract(
                "partition processed-observation outcomes do not exactly match admitted and quarantined schema evidence",
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
            if self.lineage.input_rows != result.counts.input_rows
                || self.profile.output_rows != result.counts.output_rows
                || self
                    .segment_positions
                    .iter()
                    .map(|position| &position.segment_id)
                    .ne(prepared_segments.iter())
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
    bytes: AccountedBytes,
}

pub struct VerifiedEnginePartitionEvidenceArtifact {
    evidence: EnginePartitionEvidence,
}

/// Engine evidence joined to a generically admitted result and frozen control authority.
///
/// The private payload prevents package assembly from accepting a merely decoded worker claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedEnginePartitionEvidence {
    plan_sha256: String,
    resource_id: cdf_kernel::ResourceId,
    plan_id: cdf_kernel::PlanId,
    task_sha256: String,
    result_sha256: String,
    evidence: EnginePartitionEvidence,
}

impl AdmittedEnginePartitionEvidence {
    pub(crate) fn validate_plan(&self, plan: &EnginePlan) -> Result<()> {
        if self.plan_sha256 != artifact_hash(plan)?
            || self.resource_id != plan.scan.request.resource_id
            || self.plan_id != plan.scan.plan_id
            || !self.task_sha256.starts_with("sha256:")
            || !self.result_sha256.starts_with("sha256:")
        {
            return Err(CdfError::contract(
                "admitted partition evidence belongs to a different engine plan",
            ));
        }
        Ok(())
    }

    pub(crate) fn preparation_result_sha256(&self) -> &str {
        &self.result_sha256
    }

    pub(crate) fn evidence(&self) -> &EnginePartitionEvidence {
        &self.evidence
    }

    pub(crate) fn into_evidence(self) -> EnginePartitionEvidence {
        self.evidence
    }
}

impl VerifiedEnginePartitionEvidenceArtifact {
    pub fn new(
        reference: &WorkerArtifactReference,
        bytes: AccountedBytes,
        observed_generation: Option<&cdf_kernel::ContentProviderGeneration>,
        maximum_bytes: u64,
    ) -> Result<Self> {
        reference.validate()?;
        if reference.kind != WorkerArtifactKind::PartitionEvidence {
            return Err(CdfError::contract(
                "partition evidence reader requires a PartitionEvidence reference",
            ));
        }
        let byte_count = u64::try_from(bytes.payload().len())
            .map_err(|_| CdfError::contract("partition evidence exceeds u64"))?;
        let content_sha256 = format!(
            "sha256:{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(bytes.payload())
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
        let evidence = serde_json::from_slice(bytes.payload())
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
    decoded_lease: MemoryLease,
}

/// Exact canonical-segment semantics observed from verified IPC bytes.
pub struct VerifiedCanonicalSegmentArtifact {
    facts: VerifiedCanonicalSegmentFacts,
    bytes: AccountedBytes,
}

impl VerifiedCanonicalSegmentArtifact {
    pub fn new(
        reference: &WorkerArtifactReference,
        bytes: AccountedBytes,
        decoded_lease: MemoryLease,
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
        let byte_count = u64::try_from(bytes.payload().len())
            .map_err(|_| CdfError::contract("canonical segment exceeds u64"))?;
        let content_sha256 = format!(
            "sha256:{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(bytes.payload())
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
            arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes.payload()), None)
                .map_err(CdfError::from)?;
        let first = reader
            .next()
            .transpose()
            .map_err(CdfError::from)?
            .ok_or_else(|| CdfError::data("canonical segment contains no rows"))?;
        let package_row_ord_start = cdf_package_contract::package_row_ord_array(&first)?
            .values()
            .first()
            .copied()
            .ok_or_else(|| CdfError::data("canonical segment contains an empty first batch"))?;
        let logical_schema = cdf_package_contract::logical_output_schema(first.schema().as_ref())?;
        let logical_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&logical_schema)?;
        let mut ordinal_validator =
            cdf_package_contract::PackageRowOrdinalValidator::new(package_row_ord_start);
        for batch in std::iter::once(Ok(first)).chain(&mut reader) {
            let batch = batch.map_err(CdfError::from)?;
            let decoded_bytes = cdf_memory::record_batch_retained_bytes(&batch)?;
            if decoded_bytes > maximum_decoded_bytes {
                return Err(CdfError::data(
                    "canonical segment batch exceeds its admitted decoded-memory budget",
                ));
            }
            ordinal_validator.observe(&batch)?;
        }
        let row_count = ordinal_validator.observed_rows()?;
        ordinal_validator.finish(row_count)?;
        drop(decoded_lease);
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

    pub fn into_bytes(self) -> AccountedBytes {
        self.bytes
    }
}

impl VerifiedPreparedSegmentArtifact {
    pub fn new(
        reference: &WorkerArtifactReference,
        bytes: AccountedBytes,
        decoded_lease: MemoryLease,
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
        let byte_count = u64::try_from(bytes.payload().len())
            .map_err(|_| CdfError::contract("prepared segment exceeds u64"))?;
        let content_sha256 = format!(
            "sha256:{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(bytes.payload())
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
        let mut reader =
            arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes.payload()), None)
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
                .checked_add(cdf_memory::record_batch_retained_bytes(&batch)?)
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
        decoded_lease.reconcile(decoded_bytes)?;
        Ok(Self {
            facts: VerifiedWorkerArtifactFacts::new(reference.clone(), Some(row_count))?,
            batches,
            decoded_lease,
        })
    }

    pub fn facts(&self) -> &VerifiedWorkerArtifactFacts {
        &self.facts
    }

    pub fn batches(&self) -> &[arrow_array::RecordBatch] {
        &self.batches
    }

    pub fn decoded_memory_bytes(&self) -> u64 {
        self.decoded_lease.bytes()
    }
}

impl VerifiedWorkerCompilerArtifact {
    pub fn new(
        reference: &WorkerArtifactReference,
        bytes: AccountedBytes,
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
        let byte_count = u64::try_from(bytes.payload().len())
            .map_err(|_| CdfError::contract("worker compiler artifact exceeds u64"))?;
        if byte_count != reference.byte_count || byte_count > maximum_bytes {
            return Err(CdfError::contract(
                "worker compiler artifact bytes exceed their reference or admitted memory bound",
            ));
        }
        let content_sha256 = format!(
            "sha256:{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(bytes.payload())
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
        self.bytes.payload()
    }
}

/// Worker-host artifact authority used by the engine-owned protocol decoder.
///
/// Compiler artifacts are bounded and decoded by the engine. Output artifacts stay outside this
/// control path and are verified from hash-while-write/provider facts without forced rereads.
pub trait EngineWorkerArtifactAuthority {
    fn memory(&self) -> std::sync::Arc<dyn cdf_memory::MemoryCoordinator>;

    fn read_compiler_artifact(
        &self,
        reference: &WorkerArtifactReference,
        maximum_bytes: u64,
    ) -> Result<VerifiedWorkerCompilerArtifact>;

    fn verify_output_artifact(
        &self,
        reference: &WorkerArtifactReference,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
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
        bytes: AccountedBytes,
    ) -> Result<VerifiedWorkerArtifactFacts>;
}

/// Engine-owned decoder and coordinator verifier for portable worker authority.
pub struct EngineWorkerAdmissionVerifier<'a> {
    artifacts: &'a dyn EngineWorkerArtifactAuthority,
}

fn validate_partition_drain_authority(
    plan: &EnginePlan,
    source_facts: &VerifiedWorkerSourceFacts,
    drain: Option<&EnginePartitionDrainEvidence>,
) -> Result<()> {
    match &plan.execution_extent {
        ExecutionExtent::Bounded { .. } => {
            if drain.is_some() {
                return Err(CdfError::contract(
                    "bounded partition evidence cannot carry drain authority",
                ));
            }
            Ok(())
        }
        ExecutionExtent::Drain {
            policy,
            termination,
            ..
        } => {
            let drain = drain.ok_or_else(|| {
                CdfError::contract("drain partition evidence requires closure authority")
            })?;
            if drain.frontier != drain.closure.frontier {
                return Err(CdfError::contract(
                    "drain partition frontier does not match its closure evidence",
                ));
            }
            let cdf_runtime::WorkerPosition::Inline { position } =
                source_facts.processed_position()
            else {
                return Err(CdfError::contract(
                    "drain admission requires an independently verified inline source frontier",
                ));
            };
            if position != &drain.frontier.frontier {
                return Err(CdfError::contract(
                    "drain frontier does not match independently verified source progress",
                ));
            }
            let cause_matches = match &drain.closure.cause {
                EpochClosureCause::CheckpointCadence { trigger } => {
                    trigger == &policy.checkpoint_cadence
                }
                EpochClosureCause::PackageRotation { trigger } => {
                    trigger == &policy.package_rotation
                }
                EpochClosureCause::DrainTermination {
                    termination: observed,
                } => observed == termination,
                EpochClosureCause::SourceExhausted => true,
            };
            let terminal = matches!(
                drain.closure.cause,
                EpochClosureCause::DrainTermination { .. } | EpochClosureCause::SourceExhausted
            );
            if !cause_matches || drain.terminate_after_settlement != terminal {
                return Err(CdfError::contract(
                    "drain closure does not match the compiled cadence, rotation, or termination authority",
                ));
            }
            if !matches!(policy.watermark, cdf_kernel::WatermarkPolicy::Disabled)
                || drain.consumed_partition_count != 1
                || drain.resume_partition.is_some()
                || !drain.consumed_late_data_carryover.is_empty()
                || !drain.late_data_carryover.is_empty()
                || !drain.partition_watermarks.is_empty()
                || drain.observed_at_unix_milliseconds == 0
            {
                return Err(CdfError::contract(
                    "isolated drain admission supports exactly one completed partition with disabled watermarks and no resume or carryover state",
                ));
            }
            Ok(())
        }
        ExecutionExtent::Resident { .. } => Err(CdfError::contract(
            "resident partition evidence requires the resident supervisor",
        )),
    }
}

impl<'a> EngineWorkerAdmissionVerifier<'a> {
    pub fn new(artifacts: &'a dyn EngineWorkerArtifactAuthority) -> Self {
        Self { artifacts }
    }

    fn read_unadmitted_partition_evidence(
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

    /// Reads engine evidence only from a result that crossed generic coordinator admission, then
    /// joins its checkpoint/drain control claims to independently verified source facts and the
    /// frozen execution extent. Transformation evidence remains worker-produced by design; its
    /// referenced bytes and row counts were already verified by generic admission.
    pub fn read_partition_evidence(
        &self,
        task: &PortablePartitionTask,
        plan: &EnginePlan,
        admitted: &AdmittedPartitionWorkerResult,
    ) -> Result<AdmittedEnginePartitionEvidence> {
        let plan_sha256 = artifact_hash(plan)?;
        if task.execution.project_identity_hash != plan_sha256
            || task.plan_id != plan.scan.plan_id
            || task.resource_id != plan.scan.request.resource_id
            || admitted.result().task_sha256 != task.task_sha256
        {
            return Err(CdfError::contract(
                "admitted partition result does not belong to the supplied task and engine plan",
            ));
        }
        let evidence = self.read_unadmitted_partition_evidence(task, plan, admitted.result())?;
        if evidence.checkpoint_eligible != admitted.source_facts().checkpoint_eligible() {
            return Err(CdfError::contract(
                "partition checkpoint eligibility does not match independently verified source authority",
            ));
        }
        validate_partition_drain_authority(plan, admitted.source_facts(), evidence.drain.as_ref())?;
        Ok(AdmittedEnginePartitionEvidence {
            plan_sha256,
            resource_id: task.resource_id.clone(),
            plan_id: task.plan_id.clone(),
            task_sha256: task.task_sha256.clone(),
            result_sha256: admitted.result().result_sha256.clone(),
            evidence,
        })
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
            || planned_source.compiled_source_plan_hash() != &source.compiled_source_plan_hash()?
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
        task: &PortablePartitionTask,
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
                let maximum_decoded_bytes = task
                    .resources
                    .memory_bytes
                    .checked_sub(reference.byte_count)
                    .ok_or_else(|| {
                        CdfError::data(
                            "worker artifact encoded bytes exceed the partition-task memory budget",
                        )
                    })?;
                self.artifacts.verify_output_artifact(
                    reference,
                    task.resources.memory_bytes,
                    maximum_decoded_bytes,
                )
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
        registry: &cdf_runtime::SourceRegistry,
        task: &PortablePartitionTask,
        authority: &ReconstructedWorkerTaskAuthority,
        attestation: &WorkerSourceAttestation,
        result: &cdf_runtime::PartitionWorkerResult,
    ) -> Result<VerifiedWorkerSourceFacts> {
        let plan = authority
            .execution_program::<ReconstructedEngineWorkerProgram>()?
            .plan();
        let evidence = self.read_unadmitted_partition_evidence(task, plan, result)?;
        let observations = evidence
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
        registry.verify_worker_source(
            task,
            authority.source(),
            authority.partition(),
            attestation,
            &observations,
        )
    }
}

/// Exact engine program reconstructed for source-free canonical segment finalization.
pub struct ReconstructedEngineSegmentProgram {
    logical_schema: std::sync::Arc<arrow_schema::Schema>,
    segmentation: CanonicalSegmentationPolicy,
    prepared: VerifiedPreparedSegmentArtifact,
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
        self.prepared.batches()
    }

    pub fn prepared_memory_bytes(&self) -> u64 {
        self.prepared.decoded_memory_bytes()
    }
}

struct PendingEngineWorkerOutput<'a> {
    store: &'a dyn EngineWorkerOutputAuthority,
    bytes: Option<AccountedBytes>,
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
            let ordinal_bytes = task
                .row_count
                .checked_mul(8)
                .ok_or_else(|| CdfError::data("canonical ordinal bytes overflowed u64"))?;
            let available_memory = task
                .resources
                .memory_bytes
                .checked_sub(program.prepared_memory_bytes())
                .and_then(|available| available.checked_sub(ordinal_bytes))
                .ok_or_else(|| {
                    CdfError::data(
                        "prepared segment and provenance ordinal exceed the segment-task memory budget",
                    )
                })?;
            let output_window = available_memory
                .min(task.output_policy.maximum_artifact_bytes)
                .min(attempt.write_permit.output.maximum_bytes);
            if output_window == 0 {
                return Err(CdfError::data(
                    "segment-task memory budget leaves no canonical output window",
                ));
            }
            let working_set = output_window
                .checked_add(ordinal_bytes)
                .ok_or_else(|| CdfError::data("canonical output working set overflowed u64"))?;
            let request = cdf_memory::ReservationRequest::new(
                cdf_memory::ConsumerKey::new(
                    "isolated-canonical-segment-output",
                    cdf_memory::MemoryClass::Package,
                )?,
                working_set,
            )?;
            let output_lease = self.store.memory().try_reserve(&request)?.ok_or_else(|| {
                CdfError::data(
                    "isolated canonical segment output memory is exhausted; reduce jobs or raise the worker memory budget",
                )
            })?;
            let canonical = cdf_package_contract::append_package_row_ord(
                program.prepared_batches().to_vec(),
                task.package_row_ord_start,
            )?;
            let mut bytes = BoundedArtifactBuffer::new(output_window)?;
            cdf_package::encode_canonical_segment_ipc(
                &mut bytes,
                canonical[0].schema().as_ref(),
                &canonical,
            )?;
            drop(canonical);
            let bytes = bytes.into_accounted(output_lease)?;
            let object_key = cdf_kernel::ContentObjectKey::new(format!(
                "{}data/{}.arrow",
                attempt.write_permit.output.object_key_prefix,
                task.segment_id.as_str()
            ))?;
            let reference = self.store.reference_for_bytes(
                WorkerArtifactKind::CanonicalSegment,
                &attempt.write_permit.output.store_namespace,
                object_key,
                bytes.payload(),
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

struct BoundedArtifactBuffer {
    bytes: Vec<u8>,
    maximum_bytes: usize,
}

impl BoundedArtifactBuffer {
    fn new(maximum_bytes: u64) -> Result<Self> {
        let maximum_bytes = usize::try_from(maximum_bytes)
            .map_err(|_| CdfError::data("worker artifact memory window exceeds usize"))?;
        Ok(Self {
            bytes: Vec::new(),
            maximum_bytes,
        })
    }

    fn into_accounted(self, lease: MemoryLease) -> Result<AccountedBytes> {
        let retained_bytes = u64::try_from(self.bytes.capacity())
            .map_err(|_| CdfError::data("worker artifact allocation exceeds u64"))?;
        lease.reconcile(retained_bytes)?;
        AccountedBytes::new_conservative(bytes::Bytes::from(self.bytes), lease)
    }
}

impl std::io::Write for BoundedArtifactBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next =
            self.bytes.len().checked_add(bytes.len()).ok_or_else(|| {
                std::io::Error::other("worker artifact byte count overflowed usize")
            })?;
        if next > self.maximum_bytes {
            return Err(std::io::Error::other(
                "worker artifact exceeds its admitted memory window",
            ));
        }
        if next > self.bytes.capacity() {
            let doubled = self.bytes.capacity().saturating_mul(2);
            let target = next.max(doubled).max(64 * 1_024).min(self.maximum_bytes);
            self.bytes
                .try_reserve_exact(target.saturating_sub(self.bytes.capacity()))
                .map_err(|error| {
                    std::io::Error::other(format!(
                        "reserve bounded worker artifact buffer: {error}"
                    ))
                })?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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
        let maximum_decoded_bytes = task
            .resources
            .memory_bytes
            .checked_sub(task.prepared_segment.byte_count)
            .ok_or_else(|| {
                CdfError::data(
                    "prepared segment encoded bytes exceed the segment-task memory budget",
                )
            })?;
        let prepared = self.artifacts.read_prepared_segment(
            &task.prepared_segment,
            task.resources.memory_bytes,
            maximum_decoded_bytes,
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
                prepared,
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
                task.resources.memory_bytes,
                task.resources
                    .memory_bytes
                    .checked_sub(reference.byte_count)
                    .ok_or_else(|| {
                        CdfError::data(
                            "canonical segment encoded bytes exceed the segment-task memory budget",
                        )
                    })?,
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
        if self.plan.scan.external_task_set().is_some() {
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
        slice
            .scan
            .replace_partition_authority(cdf_kernel::PartitionAuthority::Inline(vec![
                self.partition.clone(),
            ]));
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
    if compiled_source_execution.compiled_source_plan_hash()
        != &input.source.compiled_source_plan_hash()?
    {
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
            schema_observation_binding: partition_schema_observation_binding(input.partition)?,
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
