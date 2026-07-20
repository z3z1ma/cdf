use std::{cell::RefCell, collections::BTreeMap, rc::Rc, sync::Arc};

use arrow_schema::Schema;
use cdf_kernel::{
    CompiledScanIntent, ErrorKind, PartitionPlan, PartitionRetrySafety, ResourceCapabilities,
    ResourceDescriptor, SchemaSource, TrustLevel, TypePolicyAllowances, WriteDisposition,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use super::*;
use crate::{
    CompiledSourcePlan, CompiledSourcePlanInput, SourceAttestationStrength,
    SourceBatchMemoryContract, SourceCompileRequest, SourceDiscoverySession, SourceDriver,
    SourceDriverDescriptor, SourceExecutionCapabilities, SourceExecutorClass, SourceHealthRequest,
    SourceHealthSink, SourceRateLimit, SourceRegistry, SourceResolutionContext,
    SourceRetryGranularity, SourceRetryPolicy,
};

struct PortableMockDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl SourceDriver for PortableMockDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn compile(&self, _request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        Err(CdfError::internal(
            "portable protocol test does not compile sources",
        ))
    }

    fn validate_portable_plan(&self, plan: &CompiledSourcePlan) -> Result<()> {
        plan.validate()
    }

    fn verify_worker_source(
        &self,
        task: &PortablePartitionTask,
        plan: &CompiledSourcePlan,
        partition: &PartitionPlan,
        attestation: &WorkerSourceAttestation,
        observations: &[WorkerProcessedObservation],
    ) -> Result<VerifiedWorkerSourceFacts> {
        self.validate_portable_plan(plan)?;
        let planned = partition.planned_position.as_ref().ok_or_else(|| {
            CdfError::contract("mock partition lacks planned source-position authority")
        })?;
        if task.partition.partition_id != partition.partition_id
            || attestation.processed_position != WorkerPosition::inline(planned.clone())?
            || attestation.physical_schema_hash != task.execution.output_schema_hash
            || !observations.is_empty()
        {
            return Err(CdfError::contract(
                "worker source attestation exceeds reconstructed position/schema authority",
            ));
        }
        VerifiedWorkerSourceFacts::new(
            WorkerPosition::inline(planned.clone())?,
            task.execution.output_schema_hash.clone(),
            50,
            4096,
            true,
        )
    }

    fn health(
        &self,
        _request: SourceHealthRequest,
        _context: &SourceResolutionContext<'_>,
        _output: &mut dyn SourceHealthSink,
    ) -> Result<()> {
        Err(CdfError::internal(
            "portable protocol test does not probe source health",
        ))
    }

    fn discovery_session(
        &self,
        _plan: &CompiledSourcePlan,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        Err(CdfError::internal(
            "portable protocol test does not discover sources",
        ))
    }

    fn resolve(
        &self,
        _plan: &CompiledSourcePlan,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn cdf_kernel::QueryableResource>> {
        Err(CdfError::internal(
            "portable protocol test does not resolve sources",
        ))
    }
}

fn hash(seed: u8) -> String {
    format!("sha256:{}", format!("{seed:02x}").repeat(32))
}

fn position(value: u64) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: PORTABLE_SOURCE_POSITION_VERSION,
        field: "offset".to_owned(),
        value: CursorValue::U64(value),
    })
}

fn compatibility() -> WorkerCompatibility {
    WorkerCompatibility {
        cdf_version: "0.1.0".to_owned(),
        artifact_version: "package-v2".to_owned(),
        arrow_version: "58.3.0".to_owned(),
        relational_engine: WorkerComponentVersion {
            component: "datafusion".to_owned(),
            version: "51.0.0".to_owned(),
        },
        normalizer_version: "namecase-v1".to_owned(),
    }
}

fn control_budget() -> WorkerControlBudget {
    WorkerControlBudget {
        maximum_task_bytes: 128 * 1024,
        maximum_attempt_bytes: 32 * 1024,
        maximum_result_bytes: 128 * 1024,
        maximum_input_artifacts: 32,
        maximum_output_artifacts: 16,
        maximum_secret_references: 8,
    }
}

fn worker_capabilities() -> WorkerRuntimeCapabilities {
    WorkerRuntimeCapabilities {
        host: ExecutionHostCapabilities {
            logical_cpu_slots: 4,
            io_workers: 2,
            blocking_lanes: Vec::new(),
        },
        memory_bytes: 512 * 1024 * 1024,
        disk_bytes: 4 * 1024 * 1024 * 1024,
        control: control_budget(),
        services: vec![
            "artifact-reader-v1".to_owned(),
            "source-registry-v1".to_owned(),
        ],
    }
}

fn mock_option_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "source": {"type": "object", "additionalProperties": false, "properties": {}},
        "resource": {"type": "object", "additionalProperties": false, "properties": {}}
    })
}

fn source_plan() -> CompiledSourcePlan {
    let option_schema = mock_option_schema();
    let driver = SourceDriverDescriptor {
        driver_id: SourceDriverId::new("mock_source").unwrap(),
        driver_version: "1.0.0".to_owned(),
        option_schema_hash: artifact_hash(&option_schema).unwrap(),
        kinds: vec!["mock".to_owned()],
        schemes: vec!["mock".to_owned()],
    };
    CompiledSourcePlan::new(
        driver,
        ResourceCapabilities::default(),
        SourceExecutionCapabilities {
            minimum_poll_bytes: 1,
            maximum_poll_bytes: 1024,
            minimum_decode_bytes: 1,
            maximum_decode_bytes: 4096,
            maximum_concurrency: 2,
            useful_concurrency: 2,
            executor_class: SourceExecutorClass::Io,
            blocking_lane: None,
            pausable: true,
            spillable: false,
            idempotent_reads: true,
            reopenable: true,
            resumable: false,
            speculative_safe: false,
            retry_granularity: SourceRetryGranularity::Partition,
            retryable_errors: vec![ErrorKind::Transient],
            retry_policy: Some(SourceRetryPolicy::default()),
            attestation: SourceAttestationStrength::ImmutableContent,
            rate_limit: Some(SourceRateLimit {
                operations: 100,
                interval_ms: 1_000,
            }),
            quota_authority: Some("mock-account".to_owned()),
            canonical_order: true,
            bounded: true,
            batch_memory: SourceBatchMemoryContract::Preaccounted,
            telemetry_version: "v1".to_owned(),
        },
        CompiledSourcePlanInput {
            descriptor: ResourceDescriptor {
                resource_id: ResourceId::new("mock.events").unwrap(),
                schema_source: SchemaSource::Declared {
                    schema_hash: SchemaHash::new(hash(90)).unwrap(),
                    source: "mock://events".to_owned(),
                },
                primary_key: Vec::new(),
                merge_key: Vec::new(),
                cursor: None,
                write_disposition: WriteDisposition::Append,
                deduplication: None,
                contract: None,
                state_scope: ScopeKey::Resource,
                freshness: None,
                trust_level: TrustLevel::Governed,
            },
            schema: Schema::empty(),
            type_policy_allowances: TypePolicyAllowances::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
            redacted_options: serde_json::json!({"token": "secret://env/MOCK_TOKEN"}),
            physical_plan: serde_json::json!({"partitions": 1}),
        },
    )
    .unwrap()
}

fn partition_plan() -> PartitionPlan {
    PartitionPlan {
        partition_id: PartitionId::new("partition-00000003").unwrap(),
        scope: ScopeKey::Partition {
            partition_id: PartitionId::new("partition-00000003").unwrap(),
        },
        planned_position: Some(position(150)),
        start_position: Some(position(100)),
        scan_intent: CompiledScanIntent::full_scan(),
        retry_safety: PartitionRetrySafety::ImmutableContent,
        metadata: BTreeMap::new(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedSemanticArtifact {
    semantic_hash: String,
}

#[derive(Clone, Default)]
struct MockArtifactStore {
    values: Rc<RefCell<BTreeMap<(WorkerArtifactKind, String), serde_json::Value>>>,
}

impl MockArtifactStore {
    fn insert<T: Serialize>(
        &self,
        kind: WorkerArtifactKind,
        key: &str,
        value: &T,
    ) -> WorkerArtifactReference {
        let content_sha256 = artifact_hash(value).unwrap();
        let value = serde_json::to_value(value).unwrap();
        let bytes = serde_json::to_vec(&value).unwrap();
        let reference = WorkerArtifactReference {
            kind,
            store_namespace: ContentStoreNamespace::new("worker-fixtures").unwrap(),
            object_key: ContentObjectKey::new(key).unwrap(),
            byte_count: u64::try_from(bytes.len()).unwrap(),
            content_sha256,
            provider_generation: Some(ContentProviderGeneration::new("generation-7").unwrap()),
        };
        self.values
            .borrow_mut()
            .insert((kind, key.to_owned()), value);
        reference
    }

    fn insert_semantic(
        &self,
        kind: WorkerArtifactKind,
        key: &str,
        semantic_hash: String,
    ) -> WorkerArtifactReference {
        self.insert(kind, key, &RecordedSemanticArtifact { semantic_hash })
    }

    fn load<T: DeserializeOwned>(&self, reference: &WorkerArtifactReference) -> T {
        serde_json::from_value(
            self.values
                .borrow()
                .get(&(reference.kind, reference.object_key.as_str().to_owned()))
                .unwrap()
                .clone(),
        )
        .unwrap()
    }

    fn tamper(&self, reference: &WorkerArtifactReference) {
        self.values.borrow_mut().insert(
            (reference.kind, reference.object_key.as_str().to_owned()),
            serde_json::json!({"tampered": true}),
        );
    }

    fn verify_reference(
        &self,
        reference: &WorkerArtifactReference,
    ) -> Result<VerifiedWorkerArtifactFacts> {
        reference.validate()?;
        let values = self.values.borrow();
        let value = values
            .get(&(reference.kind, reference.object_key.as_str().to_owned()))
            .ok_or_else(|| CdfError::contract("mock artifact is missing"))?;
        let bytes = serde_json::to_vec(value).unwrap();
        let decode_error =
            |error| CdfError::contract(format!("mock artifact decode failed: {error}"));
        let observed_hash = match reference.kind {
            WorkerArtifactKind::CompiledSourcePlan => artifact_hash(
                &serde_json::from_value::<CompiledSourcePlan>(value.clone())
                    .map_err(decode_error)?,
            )?,
            WorkerArtifactKind::PartitionPlan => artifact_hash(
                &serde_json::from_value::<PartitionPlan>(value.clone()).map_err(decode_error)?,
            )?,
            _ => artifact_hash(
                &serde_json::from_value::<RecordedSemanticArtifact>(value.clone())
                    .map_err(decode_error)?,
            )?,
        };
        if observed_hash != reference.content_sha256
            || u64::try_from(bytes.len()).unwrap() != reference.byte_count
            || reference
                .provider_generation
                .as_ref()
                .map(|value| value.as_str())
                != Some("generation-7")
        {
            return Err(CdfError::contract(
                "mock artifact bytes/hash/generation do not match reference",
            ));
        }
        let row_count = matches!(
            reference.kind,
            WorkerArtifactKind::PreparedSegment | WorkerArtifactKind::CanonicalSegment
        )
        .then_some(50);
        VerifiedWorkerArtifactFacts::new(reference.clone(), row_count)
    }
}

impl WorkerAdmissionVerifier for MockArtifactStore {
    fn reconstruct_task_authority(
        &self,
        task: &PortablePartitionTask,
    ) -> Result<ReconstructedWorkerTaskAuthority> {
        for reference in [
            &task.source.compiled_source_plan,
            &task.partition.partition_plan,
        ]
        .into_iter()
        .chain(task.execution.artifacts.references())
        {
            self.verify_reference(reference)?;
        }
        let source = self.load(&task.source.compiled_source_plan);
        let partition = self.load(&task.partition.partition_plan);
        let load_hash = |reference: &WorkerArtifactReference| {
            self.load::<RecordedSemanticArtifact>(reference)
                .semantic_hash
        };
        let execution = ReconstructedExecutionAuthority::from_verified_compiler_artifacts(
            load_hash(&task.execution.artifacts.project_plan),
            SchemaHash::new(load_hash(&task.execution.artifacts.output_schema))?,
            load_hash(&task.execution.artifacts.validation_program),
            load_hash(&task.execution.artifacts.normalization_policy),
            load_hash(&task.execution.artifacts.compiled_expression_plan),
            load_hash(&task.execution.artifacts.operator_graph),
            load_hash(&task.execution.artifacts.segmentation_policy),
            load_hash(&task.execution.artifacts.execution_extent),
            load_hash(&task.execution.artifacts.decode_unit_plan),
            load_hash(&task.execution.artifacts.segment_plan),
        )?;
        Ok(ReconstructedWorkerTaskAuthority::from_verified_artifacts(
            source,
            partition,
            execution,
            Box::new(()),
        ))
    }

    fn verify_artifact(
        &self,
        _task: &PortablePartitionTask,
        reference: &WorkerArtifactReference,
    ) -> Result<VerifiedWorkerArtifactFacts> {
        self.verify_reference(reference)
    }

    fn verify_source_authority(
        &self,
        registry: &SourceRegistry,
        task: &PortablePartitionTask,
        authority: &ReconstructedWorkerTaskAuthority,
        attestation: &WorkerSourceAttestation,
        _result: &PartitionWorkerResult,
    ) -> Result<VerifiedWorkerSourceFacts> {
        registry.verify_worker_source(
            task,
            authority.source(),
            authority.partition(),
            attestation,
            &[],
        )
    }
}

impl SegmentTaskReconstructor for MockArtifactStore {
    fn reconstruct_segment_task(
        &self,
        task: &PortableSegmentTask,
    ) -> Result<ReconstructedSegmentTask> {
        let verify = |reference: &WorkerArtifactReference| self.verify_reference(reference);
        Ok(ReconstructedSegmentTask::from_verified_artifacts(
            verify(&task.prepared_segment)?,
            verify(&task.output_schema)?,
            verify(&task.segmentation_policy)?,
            Box::new(()),
        ))
    }
}

impl WorkerOutputVerifier for MockArtifactStore {
    fn verify_canonical_segment(
        &self,
        task: &PortableSegmentTask,
        reference: &WorkerArtifactReference,
    ) -> Result<VerifiedCanonicalSegmentFacts> {
        self.verify_reference(reference)?;
        VerifiedCanonicalSegmentFacts::new(
            reference.clone(),
            task.row_count,
            task.output_schema_hash.clone(),
            task.package_row_ord_start,
        )
    }
}

struct ShiftedCanonicalVerifier<'a>(&'a MockArtifactStore);

impl WorkerOutputVerifier for ShiftedCanonicalVerifier<'_> {
    fn verify_canonical_segment(
        &self,
        task: &PortableSegmentTask,
        reference: &WorkerArtifactReference,
    ) -> Result<VerifiedCanonicalSegmentFacts> {
        self.0.verify_reference(reference)?;
        VerifiedCanonicalSegmentFacts::new(
            reference.clone(),
            task.row_count,
            task.output_schema_hash.clone(),
            task.package_row_ord_start + 1,
        )
    }
}

struct Fixture {
    task: PortablePartitionTask,
    store: MockArtifactStore,
}

struct FixtureIsolatedExecutor<'a> {
    fixture: &'a Fixture,
}

impl IsolatedPartitionExecutor for FixtureIsolatedExecutor<'_> {
    fn execute(
        &self,
        invocation: IsolatedPartitionInvocation,
    ) -> cdf_kernel::BoxFuture<'_, Result<PartitionWorkerResult>> {
        let result = if invocation.task() != &self.fixture.task
            || invocation.authority().partition() != &partition_plan()
            || invocation.authority().source() != &source_plan()
        {
            Err(CdfError::contract(
                "isolated executor received authority other than its reconstructed task",
            ))
        } else {
            invocation
                .authority()
                .execution_program::<()>()
                .map(|_| self.fixture.result(invocation.attempt()))
        };
        Box::pin(async move { result })
    }
}

struct FixtureIsolatedSegmentExecutor<'a> {
    store: &'a MockArtifactStore,
}

impl IsolatedSegmentExecutor for FixtureIsolatedSegmentExecutor<'_> {
    fn execute(
        &self,
        invocation: IsolatedSegmentInvocation,
    ) -> cdf_kernel::BoxFuture<'_, Result<SegmentWorkerResult>> {
        let result = invocation
            .reconstructed()
            .execution_program::<()>()
            .and_then(|_| {
                let task = invocation.task();
                let canonical = self.store.insert_semantic(
                    WorkerArtifactKind::CanonicalSegment,
                    "attempts/segment-attempt-4/data/p00000003-s00000000.arrow",
                    hash(53),
                );
                SegmentWorkerResult::new(
                    invocation.attempt(),
                    WorkerTerminalStatus::Succeeded,
                    Some(WorkerArtifactReceipt {
                        role: WorkerArtifactRole::CanonicalSegment {
                            segment_id: task.segment_id.clone(),
                            partition_ordinal: task.canonical_partition_ordinal,
                            segment_ordinal: task.segment_ordinal,
                            row_count: task.row_count,
                        },
                        artifact: canonical,
                    }),
                    WorkerTelemetry::default(),
                )
            });
        Box::pin(async move { result })
    }
}

#[derive(Default)]
struct CountingArtifactSink {
    writes: usize,
    fail_after_write: bool,
}

struct ProviderRecheckingSink {
    current_lease: WorkerLeaseState,
    object_state: WorkerArtifactObjectState,
    writes: usize,
}

impl WorkerAuthorizedArtifactSink for ProviderRecheckingSink {
    fn write_authorized(
        &mut self,
        authorization: WorkerArtifactWriteAuthorization<'_>,
    ) -> Result<VerifiedWorkerArtifactFacts> {
        authorization.validate_provider_preconditions(
            &self.current_lease,
            &self.object_state,
            2_000,
        )?;
        self.writes += 1;
        VerifiedWorkerArtifactFacts::new(authorization.receipt().artifact.clone(), None)
    }
}

impl WorkerAuthorizedArtifactSink for CountingArtifactSink {
    fn write_authorized(
        &mut self,
        authorization: WorkerArtifactWriteAuthorization<'_>,
    ) -> Result<VerifiedWorkerArtifactFacts> {
        self.writes += 1;
        if self.fail_after_write {
            return Err(CdfError::transient("mock ambiguous provider failure"));
        }
        let receipt = authorization.receipt();
        let row_count = match receipt.role {
            WorkerArtifactRole::PreparedSegment { row_count, .. }
            | WorkerArtifactRole::CanonicalSegment { row_count, .. } => Some(row_count),
            _ => None,
        };
        VerifiedWorkerArtifactFacts::new(receipt.artifact.clone(), row_count)
    }
}

impl Fixture {
    fn new() -> Self {
        let store = MockArtifactStore::default();
        let source = source_plan();
        let partition = partition_plan();
        let source_reference = store.insert(
            WorkerArtifactKind::CompiledSourcePlan,
            "plans/source.json",
            &source,
        );
        let partition_reference = store.insert(
            WorkerArtifactKind::PartitionPlan,
            "plans/partition-00000003.json",
            &partition,
        );
        let project_identity_hash = hash(30);
        let output_schema_hash = SchemaHash::new(hash(31)).unwrap();
        let validation_program_hash = hash(32);
        let normalization_policy_hash = hash(33);
        let compiled_expression_plan_hash = hash(34);
        let operator_graph_hash = hash(35);
        let segmentation_policy_hash = hash(36);
        let execution_extent_hash = hash(37);
        let unit_authority_hash = hash(38);
        let segment_authority_hash = hash(39);
        let artifacts = WorkerExecutionArtifacts {
            project_plan: store.insert_semantic(
                WorkerArtifactKind::ProjectPlan,
                "plans/project.json",
                project_identity_hash.clone(),
            ),
            output_schema: store.insert_semantic(
                WorkerArtifactKind::OutputSchema,
                "plans/output-schema.json",
                output_schema_hash.as_str().to_owned(),
            ),
            validation_program: store.insert_semantic(
                WorkerArtifactKind::ValidationProgram,
                "plans/validation.json",
                validation_program_hash.clone(),
            ),
            normalization_policy: store.insert_semantic(
                WorkerArtifactKind::NormalizationPolicy,
                "plans/normalization.json",
                normalization_policy_hash.clone(),
            ),
            compiled_expression_plan: store.insert_semantic(
                WorkerArtifactKind::CompiledExpressionPlan,
                "plans/expressions.json",
                compiled_expression_plan_hash.clone(),
            ),
            operator_graph: store.insert_semantic(
                WorkerArtifactKind::OperatorGraph,
                "plans/operator-graph.json",
                operator_graph_hash.clone(),
            ),
            segmentation_policy: store.insert_semantic(
                WorkerArtifactKind::SegmentationPolicy,
                "plans/segmentation.json",
                segmentation_policy_hash.clone(),
            ),
            execution_extent: store.insert_semantic(
                WorkerArtifactKind::ExecutionExtent,
                "plans/extent.json",
                execution_extent_hash.clone(),
            ),
            decode_unit_plan: store.insert_semantic(
                WorkerArtifactKind::DecodeUnitPlan,
                "plans/decode-unit.json",
                unit_authority_hash.clone(),
            ),
            segment_plan: store.insert_semantic(
                WorkerArtifactKind::SegmentPlan,
                "plans/segment-plan.json",
                segment_authority_hash.clone(),
            ),
        };
        let input = store.insert_semantic(
            WorkerArtifactKind::InputPayload,
            "inputs/events.bin",
            hash(40),
        );
        let task = PortablePartitionTask::new(PortablePartitionTaskInput {
            compatibility: compatibility(),
            pipeline_id: PipelineId::new("pipeline-fixture").unwrap(),
            resource_id: source.descriptor.resource_id.clone(),
            plan_id: PlanId::new("plan-fixture").unwrap(),
            source: PortableSourceBinding {
                driver_id: source.driver.driver_id.clone(),
                driver_version: source.driver.driver_version.clone(),
                option_schema_hash: source.driver.option_schema_hash.clone(),
                compiled_source_plan: source_reference,
                redacted_options_hash: source.redacted_options_hash.clone(),
                physical_plan_hash: source.physical_plan_hash.clone(),
                source_semantics_hash: source.schema_binding_stable_hash().unwrap(),
                execution_capabilities_hash: artifact_hash(&source.execution_capabilities).unwrap(),
            },
            partition: PortablePartitionBinding {
                partition_id: partition.partition_id.clone(),
                scope: partition.scope.clone(),
                canonical_partition_ordinal: 3,
                epoch_ordinal: Some(9),
                partition_plan: partition_reference,
                source_identity_hash: partition_source_identity_binding(&partition).unwrap(),
                unit_authority_hash,
                segment_authority_hash,
            },
            execution: PortableExecutionBinding {
                project_identity_hash,
                artifacts,
                output_schema_hash,
                validation_program_hash,
                normalization_policy_hash,
                compiled_expression_plan_hash,
                operator_graph_hash,
                segmentation_policy_hash,
                execution_extent_hash,
            },
            input_checkpoint: Some(WorkerInputCheckpointBinding {
                checkpoint_id: CheckpointId::new("checkpoint-8").unwrap(),
                scope: ScopeKey::Resource,
                state_version: PORTABLE_CHECKPOINT_STATE_VERSION,
                position: WorkerPosition::inline(position(100)).unwrap(),
                content_sha256: hash(41),
            }),
            secret_references: vec![SecretReference::new("secret://env/MOCK_TOKEN").unwrap()],
            input_artifacts: vec![input],
            resources: WorkerResourceBudget {
                memory_bytes: 256 * 1024 * 1024,
                disk_bytes: 2 * 1024 * 1024 * 1024,
                cpu_slots: 2,
                io_slots: 1,
                control: control_budget(),
            },
            attempt_policy: WorkerAttemptPolicy {
                maximum_attempts: 3,
                maximum_attempt_duration_ms: 30_000,
            },
            capabilities: WorkerCapabilityRequirements {
                required_blocking_lanes: Vec::new(),
                services: vec![
                    "artifact-reader-v1".to_owned(),
                    "source-registry-v1".to_owned(),
                ],
            },
            output_policy: WorkerOutputPolicy {
                allowed_kinds: vec![
                    WorkerArtifactKind::CanonicalSegment,
                    WorkerArtifactKind::Quarantine,
                    WorkerArtifactKind::Residual,
                    WorkerArtifactKind::Verdict,
                    WorkerArtifactKind::Lineage,
                ],
                maximum_artifact_bytes: 1024 * 1024 * 1024,
            },
        })
        .unwrap();
        Self { task, store }
    }

    fn registry(&self) -> SourceRegistry {
        let mut registry = SourceRegistry::new();
        registry
            .register(PortableMockDriver {
                descriptor: source_plan().driver,
                option_schema: mock_option_schema(),
            })
            .unwrap();
        registry
    }

    fn attempt(&self) -> PartitionAttemptEnvelope {
        PartitionAttemptEnvelope {
            version: PARTITION_ATTEMPT_VERSION,
            attempt_id: "attempt-4".to_owned(),
            retry_ordinal: 0,
            trace_id: "trace-4".to_owned(),
            write_permit: WorkerArtifactWritePermit {
                task_sha256: self.task.task_sha256.clone(),
                lease_authority_domain_id: LeaseAuthorityDomainId::new("local-test-domain")
                    .unwrap(),
                lease_scope: self.task.partition.scope.clone(),
                fencing_token: FencingToken::new(4).unwrap(),
                issued_at_ms: 1_000,
                expires_at_ms: 10_000,
                output: WorkerArtifactWriteScope {
                    store_namespace: ContentStoreNamespace::new("worker-fixtures").unwrap(),
                    object_key_prefix: "attempts/attempt-4/".to_owned(),
                    maximum_bytes: 1024 * 1024 * 1024,
                },
                generation_precondition: WorkerObjectGenerationPrecondition::CreateOrVerifyContent,
            },
        }
    }

    fn lease(&self) -> WorkerLeaseState {
        WorkerLeaseState {
            lease_authority_domain_id: LeaseAuthorityDomainId::new("local-test-domain").unwrap(),
            lease_scope: self.task.partition.scope.clone(),
            fencing_token: FencingToken::new(4).unwrap(),
            expires_at_ms: 10_000,
        }
    }

    fn result(&self, attempt: &PartitionAttemptEnvelope) -> PartitionWorkerResult {
        let segment = self.store.insert_semantic(
            WorkerArtifactKind::CanonicalSegment,
            "attempts/attempt-4/data/p00000003-s00000000.arrow",
            hash(50),
        );
        let artifact_bytes = segment.byte_count;
        PartitionWorkerResult::new(
            attempt,
            PartitionWorkerResultInput {
                status: WorkerTerminalStatus::Succeeded,
                source_attestation: Some(WorkerSourceAttestation {
                    processed_position: WorkerPosition::inline(position(150)).unwrap(),
                    physical_schema_hash: self.task.execution.output_schema_hash.clone(),
                }),
                artifacts: vec![WorkerArtifactReceipt {
                    role: WorkerArtifactRole::CanonicalSegment {
                        segment_id: SegmentId::new("p00000003-s00000000").unwrap(),
                        partition_ordinal: 3,
                        segment_ordinal: 0,
                        row_count: 50,
                    },
                    artifact: segment,
                }],
                counts: WorkerResultCounts {
                    input_rows: 50,
                    output_rows: 50,
                    quarantined_rows: 0,
                    source_bytes: 4096,
                    artifact_bytes,
                },
                telemetry: WorkerTelemetry {
                    elapsed_ns: 100,
                    cpu_ns: 80,
                    peak_memory_bytes: 1024,
                    spill_bytes: 0,
                },
            },
        )
        .unwrap()
    }

    fn segment_task(
        &self,
        prepared_segment: WorkerArtifactReference,
        preparation_result_sha256: String,
    ) -> PortableSegmentTask {
        PortableSegmentTask::new(PortableSegmentTaskInput {
            compatibility: self.task.compatibility.clone(),
            pipeline_id: self.task.pipeline_id.clone(),
            resource_id: self.task.resource_id.clone(),
            plan_id: self.task.plan_id.clone(),
            partition_id: self.task.partition.partition_id.clone(),
            scope: self.task.partition.scope.clone(),
            canonical_partition_ordinal: self.task.partition.canonical_partition_ordinal,
            segment_id: SegmentId::new("p00000003-s00000000").unwrap(),
            segment_ordinal: 0,
            row_count: 50,
            prepared_segment,
            preparation_result_sha256,
            package_row_ord_start: 125,
            output_schema: self.task.execution.artifacts.output_schema.clone(),
            output_schema_hash: self.task.execution.output_schema_hash.clone(),
            segmentation_policy: self.task.execution.artifacts.segmentation_policy.clone(),
            segmentation_policy_hash: self.task.execution.segmentation_policy_hash.clone(),
            resources: self.task.resources.clone(),
            attempt_policy: self.task.attempt_policy.clone(),
            capabilities: WorkerCapabilityRequirements {
                required_blocking_lanes: Vec::new(),
                services: vec!["artifact-reader-v1".to_owned()],
            },
            output_policy: WorkerOutputPolicy {
                allowed_kinds: vec![WorkerArtifactKind::CanonicalSegment],
                maximum_artifact_bytes: self.task.output_policy.maximum_artifact_bytes,
            },
        })
        .unwrap()
    }

    fn segment_attempt(&self, task: &PortableSegmentTask) -> PartitionAttemptEnvelope {
        PartitionAttemptEnvelope {
            version: PARTITION_ATTEMPT_VERSION,
            attempt_id: "segment-attempt-4".to_owned(),
            retry_ordinal: 0,
            trace_id: "segment-trace-4".to_owned(),
            write_permit: WorkerArtifactWritePermit {
                task_sha256: task.task_sha256.clone(),
                lease_authority_domain_id: LeaseAuthorityDomainId::new("local-test-domain")
                    .unwrap(),
                lease_scope: task.scope.clone(),
                fencing_token: FencingToken::new(4).unwrap(),
                issued_at_ms: 1_000,
                expires_at_ms: 10_000,
                output: WorkerArtifactWriteScope {
                    store_namespace: ContentStoreNamespace::new("worker-fixtures").unwrap(),
                    object_key_prefix: "attempts/segment-attempt-4/".to_owned(),
                    maximum_bytes: 1024 * 1024 * 1024,
                },
                generation_precondition: WorkerObjectGenerationPrecondition::CreateOrVerifyContent,
            },
        }
    }
}

#[test]
fn canonical_task_attempt_and_result_fixtures_round_trip() {
    let fixture = Fixture::new();
    let attempt = fixture.attempt();
    let result = fixture.result(&attempt);
    let task_json = serde_json::to_string_pretty(&fixture.task).unwrap();
    let attempt_json = serde_json::to_string_pretty(&attempt).unwrap();
    let result_json = serde_json::to_string_pretty(&result).unwrap();
    if std::env::var_os("CDF_PRINT_WORKER_GOLDENS").is_some() {
        eprintln!("TASK\n{task_json}\nATTEMPT\n{attempt_json}\nRESULT\n{result_json}");
    }
    assert_eq!(
        task_json,
        include_str!("../../tests/fixtures/portable_partition_task_v1.json").trim_end()
    );
    assert_eq!(
        attempt_json,
        include_str!("../../tests/fixtures/partition_attempt_v1.json").trim_end()
    );
    assert_eq!(
        result_json,
        include_str!("../../tests/fixtures/partition_worker_result_v1.json").trim_end()
    );
    assert_eq!(
        PortablePartitionTask::decode_bounded(
            task_json.as_bytes(),
            &compatibility(),
            &worker_capabilities(),
        )
        .unwrap(),
        fixture.task
    );
    assert_eq!(
        PartitionAttemptEnvelope::decode_bounded(
            attempt_json.as_bytes(),
            &fixture.task,
            &worker_capabilities(),
        )
        .unwrap(),
        attempt
    );
    assert_eq!(
        PartitionWorkerResult::decode_bounded(
            result_json.as_bytes(),
            &fixture.task,
            &worker_capabilities(),
        )
        .unwrap(),
        result
    );
}

#[test]
fn prepared_segment_receipts_are_row_verified_without_becoming_package_identity() {
    let fixture = Fixture::new();
    let mut task = fixture.task.clone();
    task.output_policy
        .allowed_kinds
        .insert(0, WorkerArtifactKind::PreparedSegment);
    task.task_sha256 = task.compute_hash().unwrap();
    task.validate().unwrap();

    let mut attempt = fixture.attempt();
    attempt.write_permit.task_sha256 = task.task_sha256.clone();
    let prepared = fixture.store.insert_semantic(
        WorkerArtifactKind::PreparedSegment,
        "attempts/attempt-4/prepared/p00000003-s00000000.arrow",
        hash(51),
    );
    let artifact_bytes = prepared.byte_count;
    let result = PartitionWorkerResult::new(
        &attempt,
        PartitionWorkerResultInput {
            status: WorkerTerminalStatus::Succeeded,
            source_attestation: Some(WorkerSourceAttestation {
                processed_position: WorkerPosition::inline(position(150)).unwrap(),
                physical_schema_hash: task.execution.output_schema_hash.clone(),
            }),
            artifacts: vec![WorkerArtifactReceipt {
                role: WorkerArtifactRole::PreparedSegment {
                    segment_id: SegmentId::new("p00000003-s00000000").unwrap(),
                    partition_ordinal: 3,
                    segment_ordinal: 0,
                    row_count: 50,
                },
                artifact: prepared,
            }],
            counts: WorkerResultCounts {
                input_rows: 50,
                output_rows: 50,
                quarantined_rows: 0,
                source_bytes: 4096,
                artifact_bytes,
            },
            telemetry: WorkerTelemetry::default(),
        },
    )
    .unwrap();

    result
        .validate_for_admission(
            &task,
            &attempt,
            &fixture.registry(),
            &fixture.lease(),
            &fixture.store,
            2_000,
        )
        .unwrap();
    assert!(
        result
            .artifacts
            .iter()
            .all(|receipt| receipt.artifact.kind == WorkerArtifactKind::PreparedSegment)
    );
}

#[test]
fn segment_finalization_task_binds_dense_prefix_and_one_canonical_receipt() {
    let fixture = Fixture::new();
    let prepared = fixture.store.insert_semantic(
        WorkerArtifactKind::PreparedSegment,
        "attempts/attempt-4/prepared/p00000003-s00000000.arrow",
        hash(51),
    );
    let task = fixture.segment_task(prepared, hash(52));
    let encoded = serde_json::to_vec(&task).unwrap();
    assert_eq!(
        PortableSegmentTask::decode_bounded(&encoded, &compatibility(), &worker_capabilities())
            .unwrap(),
        task
    );
    let attempt = fixture.segment_attempt(&task);
    attempt.validate_for_task(&task).unwrap();

    let canonical = fixture.store.insert_semantic(
        WorkerArtifactKind::CanonicalSegment,
        "attempts/segment-attempt-4/data/p00000003-s00000000.arrow",
        hash(53),
    );
    let result = SegmentWorkerResult::new(
        &attempt,
        WorkerTerminalStatus::Succeeded,
        Some(WorkerArtifactReceipt {
            role: WorkerArtifactRole::CanonicalSegment {
                segment_id: task.segment_id.clone(),
                partition_ordinal: task.canonical_partition_ordinal,
                segment_ordinal: task.segment_ordinal,
                row_count: task.row_count,
            },
            artifact: canonical,
        }),
        WorkerTelemetry::default(),
    )
    .unwrap();
    let result_bytes = serde_json::to_vec(&result).unwrap();
    let decoded =
        SegmentWorkerResult::decode_bounded(&result_bytes, &task, &worker_capabilities()).unwrap();
    decoded
        .validate_for_admission(&task, &attempt, &fixture.lease(), &fixture.store, 2_000)
        .unwrap();

    let shifted_error = decoded
        .validate_for_admission(
            &task,
            &attempt,
            &fixture.lease(),
            &ShiftedCanonicalVerifier(&fixture.store),
            2_000,
        )
        .unwrap_err();
    assert!(shifted_error.message.contains("package row ordinal"));

    let mut forged = task.clone();
    forged.package_row_ord_start += 1;
    forged.task_sha256 = forged.compute_hash().unwrap();
    assert!(
        decoded
            .validate_for_admission(&forged, &attempt, &fixture.lease(), &fixture.store, 2_000,)
            .unwrap_err()
            .message
            .contains("task")
    );
}

#[test]
fn local_isolated_segment_host_round_trips_source_free_finalization() {
    let fixture = Fixture::new();
    let prepared = fixture.store.insert_semantic(
        WorkerArtifactKind::PreparedSegment,
        "attempts/attempt-4/prepared/p00000003-s00000000.arrow",
        hash(51),
    );
    let task = fixture.segment_task(prepared, hash(52));
    let attempt = fixture.segment_attempt(&task);
    let compatibility = compatibility();
    let capabilities = worker_capabilities();
    let worker_store = fixture.store.clone();
    let coordinator_store = fixture.store.clone();
    let executor = FixtureIsolatedSegmentExecutor {
        store: &worker_store,
    };
    let worker =
        LocalIsolatedSegmentHost::new(&compatibility, &capabilities, &worker_store, &executor)
            .unwrap();

    let admitted = futures_executor::block_on(execute_local_isolated_segment(
        &task,
        &attempt,
        &worker,
        &coordinator_store,
        &fixture.lease(),
        2_000,
    ))
    .unwrap();
    let artifact = admitted.result().artifact.as_ref().unwrap();
    assert_eq!(artifact.artifact.kind, WorkerArtifactKind::CanonicalSegment);
    assert!(matches!(
        artifact.role,
        WorkerArtifactRole::CanonicalSegment {
            partition_ordinal: 3,
            segment_ordinal: 0,
            row_count: 50,
            ..
        }
    ));

    let stale = WorkerLeaseState {
        fencing_token: FencingToken::new(5).unwrap(),
        ..fixture.lease()
    };
    let error = futures_executor::block_on(execute_local_isolated_segment(
        &task,
        &attempt,
        &worker,
        &coordinator_store,
        &stale,
        2_000,
    ))
    .unwrap_err();
    assert!(error.message.contains("stale"));
}

#[test]
fn isolated_worker_reconstructs_every_authority_from_artifacts() {
    let fixture = Fixture::new();
    let task_bytes = serde_json::to_vec(&fixture.task).unwrap();
    let task = PortablePartitionTask::decode_bounded(
        &task_bytes,
        &compatibility(),
        &worker_capabilities(),
    )
    .unwrap();
    let registry = fixture.registry();
    registry
        .validate_portable_source_binding(&task.source)
        .unwrap();
    task.reconstruct_and_validate_authority(&registry, &fixture.store)
        .unwrap();

    let attempt = fixture.attempt();
    let result = fixture.result(&attempt);
    result
        .validate_for_admission(
            &task,
            &attempt,
            &registry,
            &fixture.lease(),
            &fixture.store,
            2_000,
        )
        .unwrap();
}

#[test]
fn local_isolated_host_round_trips_only_an_admitted_result() {
    let fixture = Fixture::new();
    let registry = fixture.registry();
    let compatibility = compatibility();
    let capabilities = worker_capabilities();
    let executor = FixtureIsolatedExecutor { fixture: &fixture };
    let worker = LocalIsolatedWorkerHost::new(
        &compatibility,
        &capabilities,
        &registry,
        &fixture.store,
        &executor,
    )
    .unwrap();
    let attempt = fixture.attempt();
    let admitted = futures_executor::block_on(execute_local_isolated_partition(
        &fixture.task,
        &attempt,
        &worker,
        &registry,
        &fixture.store,
        &fixture.lease(),
        2_000,
    ))
    .unwrap();
    assert_eq!(admitted.result(), &fixture.result(&attempt));

    let stale = WorkerLeaseState {
        fencing_token: FencingToken::new(5).unwrap(),
        ..fixture.lease()
    };
    let error = futures_executor::block_on(execute_local_isolated_partition(
        &fixture.task,
        &attempt,
        &worker,
        &registry,
        &fixture.store,
        &stale,
        2_000,
    ))
    .unwrap_err();
    assert!(error.message.contains("stale"));
}

#[test]
fn isolated_worker_rejects_missing_capability_and_source_binding_before_execution() {
    let fixture = Fixture::new();
    let compatibility = compatibility();
    let capabilities = worker_capabilities();
    let registry = fixture.registry();
    let executor = FixtureIsolatedExecutor { fixture: &fixture };

    let mut missing_capability = fixture.task.clone();
    missing_capability
        .capabilities
        .services
        .push("missing-worker-service-v1".to_owned());
    missing_capability.capabilities.services.sort();
    missing_capability.task_sha256 = missing_capability.compute_hash().unwrap();
    let mut capability_attempt = fixture.attempt();
    capability_attempt.write_permit.task_sha256 = missing_capability.task_sha256.clone();
    let worker = LocalIsolatedWorkerHost::new(
        &compatibility,
        &capabilities,
        &registry,
        &fixture.store,
        &executor,
    )
    .unwrap();
    let error = futures_executor::block_on(execute_local_isolated_partition(
        &missing_capability,
        &capability_attempt,
        &worker,
        &registry,
        &fixture.store,
        &fixture.lease(),
        2_000,
    ))
    .unwrap_err();
    assert!(
        error.message.contains("missing required service"),
        "{}",
        error.message
    );

    let empty_registry = SourceRegistry::new();
    let worker = LocalIsolatedWorkerHost::new(
        &compatibility,
        &capabilities,
        &empty_registry,
        &fixture.store,
        &executor,
    )
    .unwrap();
    let error = futures_executor::block_on(execute_local_isolated_partition(
        &fixture.task,
        &fixture.attempt(),
        &worker,
        &registry,
        &fixture.store,
        &fixture.lease(),
        2_000,
    ))
    .unwrap_err();
    assert!(error.message.contains("unregistered source driver"));

    let mut stale_driver = fixture.task.clone();
    stale_driver.source.driver_version = "2.0.0".to_owned();
    stale_driver.task_sha256 = stale_driver.compute_hash().unwrap();
    let mut stale_attempt = fixture.attempt();
    stale_attempt.write_permit.task_sha256 = stale_driver.task_sha256.clone();
    let worker = LocalIsolatedWorkerHost::new(
        &compatibility,
        &capabilities,
        &registry,
        &fixture.store,
        &executor,
    )
    .unwrap();
    let error = futures_executor::block_on(execute_local_isolated_partition(
        &stale_driver,
        &stale_attempt,
        &worker,
        &registry,
        &fixture.store,
        &fixture.lease(),
        2_000,
    ))
    .unwrap_err();
    assert!(error.message.contains("version/schema"));
}

#[test]
fn forged_execution_artifact_and_semantic_authority_fail_closed() {
    let mut fixture = Fixture::new();
    let replacement = fixture.store.insert_semantic(
        WorkerArtifactKind::OperatorGraph,
        "plans/operator-graph.json",
        hash(99),
    );
    fixture.task.execution.artifacts.operator_graph = replacement;
    fixture.task.task_sha256 = fixture.task.compute_hash().unwrap();
    assert!(
        fixture
            .task
            .reconstruct_and_validate_authority(&fixture.registry(), &fixture.store)
            .unwrap_err()
            .message
            .contains("execution program")
    );

    let fixture = Fixture::new();
    fixture
        .store
        .tamper(&fixture.task.execution.artifacts.operator_graph);
    assert!(
        fixture
            .task
            .reconstruct_and_validate_authority(&fixture.registry(), &fixture.store)
            .unwrap_err()
            .message
            .contains("mock artifact")
    );
}

#[test]
fn write_permit_is_checked_before_every_object_write() {
    let fixture = Fixture::new();
    let mut attempt = fixture.attempt();
    attempt.write_permit.output.maximum_bytes = 6_000;
    let segment = WorkerArtifactReference {
        kind: WorkerArtifactKind::CanonicalSegment,
        store_namespace: ContentStoreNamespace::new("worker-fixtures").unwrap(),
        object_key: ContentObjectKey::new("attempts/attempt-4/data/segment.arrow").unwrap(),
        byte_count: 4096,
        content_sha256: hash(70),
        provider_generation: None,
    };
    let receipt = WorkerArtifactReceipt {
        role: WorkerArtifactRole::CanonicalSegment {
            segment_id: SegmentId::new("p00000003-s00000000").unwrap(),
            partition_ordinal: 3,
            segment_ordinal: 0,
            row_count: 50,
        },
        artifact: segment.clone(),
    };
    let mut sink = CountingArtifactSink::default();
    let lease = fixture.lease();
    let mut session =
        WorkerArtifactWriteSession::new(&fixture.task, &attempt, &lease, 2_000).unwrap();
    session
        .write(
            &receipt,
            &WorkerArtifactObjectState::Absent,
            2_000,
            &mut sink,
        )
        .unwrap();
    assert_eq!(sink.writes, 1);

    let second = WorkerArtifactReceipt {
        role: WorkerArtifactRole::CanonicalSegment {
            segment_id: SegmentId::new("p00000003-s00000001").unwrap(),
            partition_ordinal: 3,
            segment_ordinal: 1,
            row_count: 50,
        },
        artifact: WorkerArtifactReference {
            object_key: ContentObjectKey::new("attempts/attempt-4/data/segment-2.arrow").unwrap(),
            content_sha256: hash(71),
            ..segment.clone()
        },
    };
    assert!(
        session
            .write(
                &second,
                &WorkerArtifactObjectState::Absent,
                2_000,
                &mut sink,
            )
            .unwrap_err()
            .message
            .contains("cumulative")
    );
    assert_eq!(sink.writes, 1, "rejected write must not reach the sink");

    let mut ambiguous_sink = CountingArtifactSink {
        writes: 0,
        fail_after_write: true,
    };
    let mut ambiguous_session =
        WorkerArtifactWriteSession::new(&fixture.task, &attempt, &lease, 2_000).unwrap();
    assert!(
        ambiguous_session
            .write(
                &receipt,
                &WorkerArtifactObjectState::Absent,
                2_000,
                &mut ambiguous_sink,
            )
            .unwrap_err()
            .message
            .contains("ambiguous")
    );
    assert!(
        ambiguous_session
            .write(
                &second,
                &WorkerArtifactObjectState::Absent,
                2_000,
                &mut ambiguous_sink,
            )
            .unwrap_err()
            .message
            .contains("cumulative")
    );
    assert_eq!(
        ambiguous_sink.writes, 1,
        "ambiguous provider failure must consume permit authority"
    );

    let stale = WorkerLeaseState {
        fencing_token: FencingToken::new(5).unwrap(),
        ..fixture.lease()
    };
    assert!(
        WorkerArtifactWriteSession::new(&fixture.task, &attempt, &stale, 2_000)
            .unwrap_err()
            .message
            .contains("stale")
    );
    let conflicting = WorkerArtifactObjectState::Present {
        content_sha256: hash(71),
        provider_generation: ContentProviderGeneration::new("generation-old").unwrap(),
    };
    assert!(
        session
            .write(&receipt, &conflicting, 2_000, &mut sink)
            .unwrap_err()
            .message
            .contains("generation precondition")
    );
}

#[test]
fn provider_recheck_rejects_a_fence_advanced_after_session_preflight() {
    let fixture = Fixture::new();
    let attempt = fixture.attempt();
    let original_lease = fixture.lease();
    let mut session =
        WorkerArtifactWriteSession::new(&fixture.task, &attempt, &original_lease, 2_000).unwrap();
    let result = fixture.result(&attempt);
    let mut sink = ProviderRecheckingSink {
        current_lease: WorkerLeaseState {
            fencing_token: FencingToken::new(5).unwrap(),
            ..original_lease.clone()
        },
        object_state: WorkerArtifactObjectState::Absent,
        writes: 0,
    };

    let error = session
        .write(
            &result.artifacts[0],
            &WorkerArtifactObjectState::Absent,
            2_000,
            &mut sink,
        )
        .unwrap_err();
    assert!(error.message.contains("stale"));
    assert_eq!(sink.writes, 0, "stale fence must fail before mutation");
}

#[test]
fn worker_result_encoder_stops_at_its_control_ceiling() {
    let payload = "x".repeat(64 * 1024);
    let error = encode_json_bounded("worker result fixture", &payload, 1_024).unwrap_err();
    assert!(error.message.contains("control ceiling"));
}

#[test]
fn semantically_rehashed_bad_result_still_fails_admission() {
    let fixture = Fixture::new();
    let attempt = fixture.attempt();
    let mut result = fixture.result(&attempt);
    result.counts.output_rows = 49;
    let WorkerArtifactRole::CanonicalSegment { row_count, .. } = &mut result.artifacts[0].role
    else {
        unreachable!();
    };
    *row_count = 49;
    result.result_sha256 = result.compute_semantic_hash().unwrap();
    result.validate().unwrap();
    assert!(
        result
            .validate_for_admission(
                &fixture.task,
                &attempt,
                &fixture.registry(),
                &fixture.lease(),
                &fixture.store,
                2_000,
            )
            .unwrap_err()
            .message
            .contains("stored content")
    );

    let mut result = fixture.result(&attempt);
    result
        .source_attestation
        .as_mut()
        .unwrap()
        .processed_position = WorkerPosition::inline(position(151)).unwrap();
    result.result_sha256 = result.compute_semantic_hash().unwrap();
    assert!(
        result
            .validate_for_admission(
                &fixture.task,
                &attempt,
                &fixture.registry(),
                &fixture.lease(),
                &fixture.store,
                2_000,
            )
            .unwrap_err()
            .message
            .contains("position/schema authority")
    );
}

#[test]
fn quarantine_artifacts_require_independently_observed_rows() {
    let reference = WorkerArtifactReference {
        kind: WorkerArtifactKind::Quarantine,
        store_namespace: ContentStoreNamespace::new("worker-fixtures").unwrap(),
        object_key: ContentObjectKey::new("attempts/attempt-4/quarantine.arrow").unwrap(),
        byte_count: 1024,
        content_sha256: hash(72),
        provider_generation: Some(ContentProviderGeneration::new("generation-7").unwrap()),
    };
    assert!(
        VerifiedWorkerArtifactFacts::new(reference, None)
            .unwrap_err()
            .message
            .contains("observed row count")
    );
}

#[test]
fn externally_configured_control_ceiling_precedes_deserialization() {
    let fixture = Fixture::new();
    let mut worker = worker_capabilities();
    worker.control.maximum_task_bytes = 8;
    let oversized_invalid = br#"{\"not_even\":\"valid enough to decode\"}"#;
    assert!(
        PortablePartitionTask::decode_bounded(oversized_invalid, &compatibility(), &worker)
            .unwrap_err()
            .message
            .contains("externally admitted")
    );

    let mut self_widened = fixture.task.clone();
    self_widened.resources.control.maximum_result_bytes =
        worker_capabilities().control.maximum_result_bytes + 1;
    self_widened.task_sha256 = self_widened.compute_hash().unwrap();
    let bytes = serde_json::to_vec(&self_widened).unwrap();
    assert!(
        PortablePartitionTask::decode_bounded(&bytes, &compatibility(), &worker_capabilities(),)
            .unwrap_err()
            .message
            .contains("externally configured ceiling")
    );
}

#[test]
fn positions_are_exact_version_portable_and_foreign_state_is_external() {
    let absolute = SourcePosition::FileManifest(cdf_kernel::FileManifest {
        version: PORTABLE_SOURCE_POSITION_VERSION,
        files: vec![cdf_kernel::FilePosition {
            path: "/coordinator/private/events.parquet".to_owned(),
            size_bytes: 1,
            source_generation: Some("generation-1".to_owned()),
            etag: None,
            object_version: None,
            sha256: None,
        }],
    });
    assert!(WorkerPosition::inline(absolute).is_err());
    for path in [
        r"\\server\share\events.parquet",
        r"\\?\C:\private\events.parquet",
        "FILE:///private/events.parquet",
        "File://localhost/private/events.parquet",
    ] {
        let position = SourcePosition::FileManifest(cdf_kernel::FileManifest {
            version: PORTABLE_SOURCE_POSITION_VERSION,
            files: vec![cdf_kernel::FilePosition {
                path: path.to_owned(),
                size_bytes: 1,
                source_generation: Some("generation-1".to_owned()),
                etag: None,
                object_version: None,
                sha256: None,
            }],
        });
        assert!(
            WorkerPosition::inline(position).is_err(),
            "portable authority admitted absolute path {path}"
        );
    }

    let remote_table = SourcePosition::TableSnapshot(Box::new(cdf_kernel::TableSnapshotPosition {
        version: PORTABLE_SOURCE_POSITION_VERSION,
        protocol: "iceberg".to_owned(),
        catalog: "glue:us-east-1:123456789012".to_owned(),
        namespace: vec!["analytics".to_owned()],
        table: "orders".to_owned(),
        selector: cdf_kernel::TableSnapshotSelector::Current,
        snapshot_id: 42,
        sequence_number: 7,
        parent_snapshot_id: Some(41),
        metadata_location: "s3://warehouse/analytics/orders/metadata/v42.json".to_owned(),
        metadata_generation: "version-id:v42".to_owned(),
    }));
    WorkerPosition::inline(remote_table.clone()).unwrap();
    let SourcePosition::TableSnapshot(mut local_table) = remote_table else {
        unreachable!();
    };
    local_table.metadata_location = "/coordinator/private/v42.metadata.json".to_owned();
    assert!(
        WorkerPosition::inline(SourcePosition::TableSnapshot(local_table))
            .unwrap_err()
            .message
            .contains("absolute coordinator file path")
    );

    let foreign = SourcePosition::ForeignState(cdf_kernel::ForeignState {
        version: PORTABLE_SOURCE_POSITION_VERSION,
        protocol: "python".to_owned(),
        opaque_blob: vec![1, 2, 3],
        blob_sha256: format!("sha256:{}", hex::encode(Sha256::digest([1, 2, 3]))),
    });
    assert!(
        WorkerPosition::inline(foreign)
            .unwrap_err()
            .message
            .contains("externalize foreign state")
    );
    let external = WorkerPosition::ExternalForeign {
        version: PORTABLE_SOURCE_POSITION_VERSION,
        protocol: "python".to_owned(),
        artifact: WorkerArtifactReference {
            kind: WorkerArtifactKind::ForeignState,
            store_namespace: ContentStoreNamespace::new("worker-fixtures").unwrap(),
            object_key: ContentObjectKey::new("positions/python-state.json").unwrap(),
            byte_count: 3,
            content_sha256: hash(80),
            provider_generation: Some(ContentProviderGeneration::new("generation-7").unwrap()),
        },
    };
    external.validate().unwrap();
}

#[test]
fn runtime_resolved_lane_stays_portable_until_worker_admission() {
    let fixture = Fixture::new();
    let mut task = fixture.task;
    let portable_lane = BlockingLaneSpec {
        lane_id: "python".to_owned(),
        binding: BlockingLaneBinding::RuntimeResolvedRequired,
        maximum_concurrency: 8,
        cpu_slot_cost: 1,
        native_internal_parallelism: 1,
        affinity: crate::LaneAffinity::Shared,
        interruption: crate::InterruptionSafety::CooperativeOnly,
    };
    task.capabilities.required_blocking_lanes = vec![portable_lane.clone()];
    task.task_sha256 = task.compute_hash().unwrap();
    task.validate().unwrap();

    let mut worker = worker_capabilities();
    worker.host.blocking_lanes = vec![BlockingLaneSpec {
        binding: BlockingLaneBinding::RuntimeResolved,
        maximum_concurrency: 1,
        ..portable_lane
    }];
    task.validate_for_worker(&compatibility(), &worker).unwrap();

    task.capabilities.required_blocking_lanes[0].binding = BlockingLaneBinding::RuntimeResolved;
    task.task_sha256 = task.compute_hash().unwrap();
    assert!(
        task.validate()
            .unwrap_err()
            .message
            .contains("runtime-resolved")
    );
}

#[test]
fn protocol_contains_no_payload_secret_or_coordinator_commit_authority() {
    let fixture = Fixture::new();
    let attempt = fixture.attempt();
    let result = fixture.result(&attempt);
    let task_json = serde_json::to_string(&fixture.task).unwrap();
    let result_json = serde_json::to_string(&result).unwrap();
    assert!(!task_json.contains("/Users/"));
    assert!(!task_json.contains("plain-text-secret"));
    assert!(!task_json.contains("opaque_blob"));
    assert!(!result_json.contains("package_hash"));
    assert!(!result_json.contains("destination_receipt"));
    assert!(!result_json.contains("checkpoint_id"));
    assert!(!result_json.contains("processed_observations"));
    assert!(result_json.len() < 2_048);

    let mut legacy_inline = serde_json::to_value(result).unwrap();
    legacy_inline["processed_observations"] = serde_json::json!([
        {
            "observation_id": "partition-00000003",
            "outcome": "admitted",
            "source_position": {
                "kind": "inline",
                "position": position(150),
            },
        }
    ]);
    assert!(serde_json::from_value::<PartitionWorkerResult>(legacy_inline).is_err());
}
