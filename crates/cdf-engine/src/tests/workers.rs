use super::support::Result;
use super::support::{
    Arc, ArrayRef, BTreeMap, Batch, DataType, DrainTermination, EXECUTION_EXTENT_VERSION,
    EngineExecutionConfig, EngineIsolatedSegmentExecutor, EnginePackageDraft,
    EnginePartitionEvidence, EnginePartitionTaskInput, EngineSegmentTaskInput,
    EngineWorkerAdmissionVerifier, EngineWorkerArtifactAuthority, EngineWorkerOutputAuthority,
    EpochClosureTrigger, ExecutionExtent, Field, Int64Array, MockResource, Mutex, Ordering,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionPlan, PhysicalObservationEvidence, Planner,
    QueryableResource, ReconstructedEngineWorkerProgram, RecordBatch, RetainedEngineRun,
    STREAM_EPOCH_POLICY_VERSION, SafeFrontierPolicy, Schema, StandaloneExecutionHost,
    StreamEpochPolicy, TempDir, VerifiedCanonicalSegmentArtifact,
    VerifiedEnginePartitionEvidenceArtifact, VerifiedPreparedSegmentArtifact,
    VerifiedWorkerCompilerArtifact, WatermarkPolicy, WorkerCompilerArtifactWriter,
    assemble_isolated_worker_package, block_on, compile_engine_partition_task,
    compile_engine_segment_task, missing_control_field_batch, mock_compiled_source_plan,
    mock_unbounded_source_plan, plan_input, sample_batches, sample_schema, terminal_file_position,
};

#[test]
fn lineage_summary_rejects_superseded_duplicate_identity_fields() {
    for field in ["input_partitions", "output_segments"] {
        let mut value = serde_json::json!({
            "input_rows": 0,
            "input_observations": []
        });
        value[field] = serde_json::json!([]);
        let error = serde_json::from_value::<crate::LineageSummary>(value).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }
}

#[test]
fn engine_partition_task_compiles_every_authority_as_typed_artifacts() {
    let resource = MockResource::tier_a(sample_batches());
    let source = mock_compiled_source_plan(&resource, None);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let partition = plan.scan.inline_partitions().unwrap().first().unwrap();
    let mut artifacts = MemoryWorkerCompilerArtifacts::default();
    let task = compile_engine_partition_task(
        EnginePartitionTaskInput {
            compatibility: cdf_runtime::WorkerCompatibility {
                cdf_version: "0.1.0".to_owned(),
                artifact_version: "package-v1".to_owned(),
                arrow_version: "58.3.0".to_owned(),
                relational_engine: cdf_runtime::WorkerComponentVersion {
                    component: "datafusion".to_owned(),
                    version: "54.0.0".to_owned(),
                },
                normalizer_version: plan.validation_program.normalizer_version.clone(),
            },
            pipeline_id: cdf_kernel::PipelineId::new("engine-worker-test").unwrap(),
            source: &source,
            plan: &plan,
            partition,
            canonical_partition_ordinal: 0,
            epoch_ordinal: None,
            input_checkpoint: None,
            secret_references: Vec::new(),
            input_artifacts: Vec::new(),
            resources: cdf_runtime::WorkerResourceBudget {
                memory_bytes: 64 * 1024 * 1024,
                disk_bytes: 64 * 1024 * 1024,
                cpu_slots: 1,
                io_slots: 1,
                control: cdf_runtime::WorkerControlBudget {
                    maximum_task_bytes: 64 * 1024,
                    maximum_attempt_bytes: 16 * 1024,
                    maximum_result_bytes: 64 * 1024,
                    maximum_input_artifacts: 32,
                    maximum_output_artifacts: 32,
                    maximum_secret_references: 8,
                },
            },
            attempt_policy: cdf_runtime::WorkerAttemptPolicy {
                maximum_attempts: 3,
                maximum_attempt_duration_ms: 30_000,
            },
            capabilities: cdf_runtime::WorkerCapabilityRequirements {
                required_blocking_lanes: Vec::new(),
                services: Vec::new(),
            },
            output_policy: cdf_runtime::WorkerOutputPolicy {
                allowed_kinds: vec![
                    cdf_runtime::WorkerArtifactKind::CanonicalSegment,
                    cdf_runtime::WorkerArtifactKind::Quarantine,
                    cdf_runtime::WorkerArtifactKind::Residual,
                    cdf_runtime::WorkerArtifactKind::Verdict,
                    cdf_runtime::WorkerArtifactKind::Lineage,
                ],
                maximum_artifact_bytes: 64 * 1024 * 1024,
            },
        },
        &mut artifacts,
    )
    .unwrap();

    assert_eq!(artifacts.values.len(), 12);
    assert_eq!(task.partition.partition_id, partition.partition_id);
    assert_eq!(
        task.execution.project_identity_hash,
        cdf_runtime::artifact_hash(&plan).unwrap()
    );
    assert_eq!(
        task.partition.unit_authority_hash,
        task.execution.artifacts.decode_unit_plan.content_sha256
    );
    assert_eq!(
        task.partition.segment_authority_hash,
        task.execution.artifacts.segment_plan.content_sha256
    );
    let serialized = serde_json::to_string(&task).unwrap();
    assert!(!serialized.contains("operator_chain"));
    assert!(!serialized.contains("compiled_source_execution"));

    let verifier = EngineWorkerAdmissionVerifier::new(&artifacts);
    let reconstructed =
        cdf_runtime::WorkerAdmissionVerifier::reconstruct_task_authority(&verifier, &task).unwrap();
    assert_eq!(reconstructed.source(), &source);
    assert_eq!(reconstructed.partition(), partition);

    artifacts
        .values
        .get_mut(&cdf_runtime::WorkerArtifactKind::ProjectPlan)
        .unwrap()[0] ^= 1;
    let verifier = EngineWorkerAdmissionVerifier::new(&artifacts);
    let error = cdf_runtime::WorkerAdmissionVerifier::reconstruct_task_authority(&verifier, &task)
        .unwrap_err();
    assert!(error.message.contains("bytes or generation"));
}

#[test]
fn engine_partition_task_rejects_package_global_work_before_writing_control_artifacts() {
    let resource = MockResource::tier_a(sample_batches())
        .without_control_keys()
        .with_partition_count(2);
    let source = mock_compiled_source_plan(&resource, None);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut artifacts = MemoryWorkerCompilerArtifacts::default();
    let error = compile_engine_partition_task(
        EnginePartitionTaskInput {
            compatibility: cdf_runtime::WorkerCompatibility {
                cdf_version: "0.1.0".to_owned(),
                artifact_version: "package-v2".to_owned(),
                arrow_version: "58.3.0".to_owned(),
                relational_engine: cdf_runtime::WorkerComponentVersion {
                    component: "datafusion".to_owned(),
                    version: "54.0.0".to_owned(),
                },
                normalizer_version: plan.validation_program.normalizer_version.clone(),
            },
            pipeline_id: cdf_kernel::PipelineId::new("global-operator-guard").unwrap(),
            source: &source,
            plan: &plan,
            partition: &plan.scan.inline_partitions().unwrap()[0],
            canonical_partition_ordinal: 0,
            epoch_ordinal: None,
            input_checkpoint: None,
            secret_references: Vec::new(),
            input_artifacts: Vec::new(),
            resources: cdf_runtime::WorkerResourceBudget {
                memory_bytes: 64 * 1024 * 1024,
                disk_bytes: 64 * 1024 * 1024,
                cpu_slots: 1,
                io_slots: 1,
                control: cdf_runtime::WorkerControlBudget {
                    maximum_task_bytes: 64 * 1024,
                    maximum_attempt_bytes: 16 * 1024,
                    maximum_result_bytes: 64 * 1024,
                    maximum_input_artifacts: 32,
                    maximum_output_artifacts: 32,
                    maximum_secret_references: 8,
                },
            },
            attempt_policy: cdf_runtime::WorkerAttemptPolicy {
                maximum_attempts: 2,
                maximum_attempt_duration_ms: 30_000,
            },
            capabilities: cdf_runtime::WorkerCapabilityRequirements {
                required_blocking_lanes: Vec::new(),
                services: Vec::new(),
            },
            output_policy: cdf_runtime::WorkerOutputPolicy {
                allowed_kinds: vec![cdf_runtime::WorkerArtifactKind::PreparedSegment],
                maximum_artifact_bytes: 64 * 1024 * 1024,
            },
        },
        &mut artifacts,
    )
    .unwrap_err();

    assert!(
        error
            .message
            .contains("canonical global-operator or epoch task")
    );
    assert!(artifacts.values.is_empty());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn actual_engine_capsule_publishes_direct_segments_across_cpu_budgets() {
    for cpu_slots in [1, 4] {
        let (direct, isolated, admitted, finalized) =
            run_actual_isolated_engine_equivalence(cpu_slots, 1, ExecutionExtent::bounded());
        let admitted = &admitted[0];
        assert_eq!(isolated.output.manifest, direct.output.manifest);
        assert_eq!(
            isolated.output.verification.package_hash(),
            direct.output.verification.package_hash()
        );
        assert_eq!(
            isolated.output.identity_segments(),
            direct.output.identity_segments()
        );
        assert_eq!(isolated.output.profile, direct.output.profile);
        assert_eq!(isolated.output.lineage, direct.output.lineage);
        assert_eq!(isolated.segment_positions, direct.segment_positions);
        assert_eq!(isolated.execution_evidence(), direct.execution_evidence());
        assert_eq!(admitted.counts.input_rows, direct.output.lineage.input_rows);
        assert_eq!(
            admitted.counts.output_rows,
            direct.output.profile.output_rows
        );
        assert_eq!(
            admitted.artifacts.len(),
            direct.output.identity_segments().len() + 1
        );
        assert_eq!(
            finalized
                .iter()
                .map(|result| {
                    result
                        .artifact
                        .as_ref()
                        .unwrap()
                        .artifact
                        .content_sha256
                        .as_str()
                })
                .collect::<Vec<_>>(),
            direct
                .output
                .identity_segments()
                .iter()
                .map(|segment| format!("sha256:{}", segment.sha256))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn actual_engine_capsules_are_jobs_invariant_for_multiple_partitions() {
    let (direct_serial, isolated_serial, admitted_serial, finalized_serial) =
        run_actual_isolated_engine_equivalence(1, 2, ExecutionExtent::bounded());
    let (direct_parallel, isolated_parallel, admitted_parallel, finalized_parallel) =
        run_actual_isolated_engine_equivalence(4, 2, ExecutionExtent::bounded());

    assert_eq!(
        direct_parallel.output.identity_segments(),
        direct_serial.output.identity_segments()
    );
    assert_eq!(
        direct_parallel.output.manifest,
        direct_serial.output.manifest
    );
    assert_eq!(
        direct_parallel.output.verification.package_hash(),
        direct_serial.output.verification.package_hash()
    );
    assert_eq!(direct_parallel.output.profile, direct_serial.output.profile);
    assert_eq!(direct_parallel.output.lineage, direct_serial.output.lineage);
    assert_eq!(
        direct_parallel.segment_positions,
        direct_serial.segment_positions
    );
    assert_eq!(
        direct_parallel.execution_evidence(),
        direct_serial.execution_evidence()
    );
    assert_eq!(
        isolated_serial.output.manifest,
        direct_serial.output.manifest
    );
    assert_eq!(
        isolated_parallel.output.manifest,
        direct_parallel.output.manifest
    );
    assert_eq!(
        admitted_serial
            .iter()
            .map(|result| result.counts.output_rows)
            .sum::<u64>(),
        direct_serial.output.profile.output_rows
    );
    assert_eq!(
        admitted_parallel
            .iter()
            .map(|result| result.counts.output_rows)
            .sum::<u64>(),
        direct_parallel.output.profile.output_rows
    );

    let finalized_hashes = |results: &[cdf_runtime::SegmentWorkerResult]| {
        results
            .iter()
            .map(|result| {
                result
                    .artifact
                    .as_ref()
                    .unwrap()
                    .artifact
                    .content_sha256
                    .clone()
            })
            .collect::<Vec<_>>()
    };
    let direct_hashes = direct_serial
        .output
        .identity_segments()
        .iter()
        .map(|segment| format!("sha256:{}", segment.sha256))
        .collect::<Vec<_>>();
    assert_eq!(finalized_hashes(&finalized_serial), direct_hashes);
    assert_eq!(finalized_hashes(&finalized_parallel), direct_hashes);
}

#[test]
fn actual_engine_capsule_preserves_a_finite_drain_epoch() {
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 64 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: cdf_kernel::LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 3 },
    };
    let (direct, isolated, admitted, finalized) =
        run_actual_isolated_engine_equivalence(4, 1, extent);
    assert_eq!(isolated.output.manifest, direct.output.manifest);
    assert_eq!(
        isolated.output.verification.package_hash(),
        direct.output.verification.package_hash()
    );
    assert_eq!(
        isolated.output.identity_segments(),
        direct.output.identity_segments()
    );
    assert_eq!(isolated.output.profile, direct.output.profile);
    assert_eq!(isolated.output.lineage, direct.output.lineage);
    assert_eq!(isolated.segment_positions, direct.segment_positions);
    assert_eq!(isolated.execution_evidence(), direct.execution_evidence());
    let direct_epoch = direct.drain_epoch.as_ref().unwrap();
    let isolated_epoch = isolated.drain_epoch.as_ref().unwrap();
    assert_eq!(
        isolated_epoch.closure.frontier,
        direct_epoch.closure.frontier
    );
    assert_eq!(
        isolated_epoch.closure.evidence,
        direct_epoch.closure.evidence
    );
    assert_eq!(isolated_epoch.consumed_partition_count, 1);
    assert_eq!(
        admitted[0].counts.output_rows,
        direct.output.profile.output_rows
    );
    assert_eq!(
        finalized[0]
            .artifact
            .as_ref()
            .unwrap()
            .artifact
            .content_sha256,
        format!("sha256:{}", direct.output.identity_segments()[0].sha256)
    );
}

#[test]
fn actual_engine_capsule_preserves_terminal_schema_quarantine_evidence() {
    let mut batch =
        missing_control_field_batch("isolated-schema-quarantine", "part-0", vec!["one", "two"]);
    batch.header.source_position = Some(terminal_file_position());
    let resource = MockResource::tier_a(vec![batch])
        .with_schema(sample_schema())
        .without_control_keys();
    let (direct, isolated, admitted, finalized) =
        run_actual_isolated_engine_equivalence_for_resource(
            4,
            resource,
            ExecutionExtent::bounded(),
        );

    assert_eq!(isolated.output.manifest, direct.output.manifest);
    assert_eq!(
        isolated.output.verification.package_hash(),
        direct.output.verification.package_hash()
    );
    assert_eq!(
        isolated.output.terminal_schema_quarantines,
        direct.output.terminal_schema_quarantines
    );
    assert_eq!(isolated.execution_evidence(), direct.execution_evidence());
    assert_eq!(admitted[0].counts.output_rows, 0);
    assert!(finalized.is_empty());
}

#[test]
fn isolated_worker_artifact_reads_hold_and_release_real_memory_leases() {
    let store = SharedEngineWorkerArtifacts::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(0..1_024)) as ArrayRef],
    )
    .unwrap();
    let mut encoded = Vec::new();
    cdf_package::encode_canonical_segment_ipc(&mut encoded, schema.as_ref(), &[batch]).unwrap();
    let reference = SharedEngineWorkerArtifacts::reference(
        cdf_runtime::WorkerArtifactKind::PreparedSegment,
        "worker-memory-test",
        "prepared/segment.arrow".to_owned(),
        &encoded,
    )
    .unwrap();
    store.values.lock().unwrap().insert(
        (
            reference.store_namespace.as_str().to_owned(),
            reference.object_key.as_str().to_owned(),
        ),
        bytes::Bytes::from(encoded),
    );

    let artifact = store
        .read_prepared_segment(&reference, reference.byte_count, 64 * 1_024)
        .unwrap();
    assert_eq!(
        artifact
            .batches()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1_024
    );
    assert!(store.memory.snapshot().current_bytes > 0);
    drop(artifact);
    assert_eq!(store.memory.snapshot().current_bytes, 0);

    let error = match store.read_prepared_segment(&reference, reference.byte_count, 1) {
        Ok(_) => panic!("oversized decoded artifact unexpectedly acquired a lease"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("exceeds its admitted decoded-memory budget")
    );
    assert_eq!(store.memory.snapshot().current_bytes, 0);
}

struct MemoryWorkerCompilerArtifacts {
    values: BTreeMap<cdf_runtime::WorkerArtifactKind, Vec<u8>>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
}

impl Default for MemoryWorkerCompilerArtifacts {
    fn default() -> Self {
        Self {
            values: BTreeMap::new(),
            memory: Arc::new(
                cdf_memory::DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new())
                    .unwrap(),
            ),
        }
    }
}

impl WorkerCompilerArtifactWriter for MemoryWorkerCompilerArtifacts {
    fn write(
        &mut self,
        kind: cdf_runtime::WorkerArtifactKind,
        canonical_bytes: &[u8],
    ) -> Result<cdf_runtime::WorkerArtifactReference> {
        let content_sha256 = format!(
            "sha256:{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(canonical_bytes)
        );
        self.values.insert(kind, canonical_bytes.to_vec());
        Ok(cdf_runtime::WorkerArtifactReference {
            kind,
            store_namespace: cdf_kernel::ContentStoreNamespace::new("engine-worker-test")?,
            object_key: cdf_kernel::ContentObjectKey::new(format!("compiler/{kind:?}.json"))?,
            byte_count: u64::try_from(canonical_bytes.len())
                .map_err(|_| cdf_kernel::CdfError::contract("test artifact exceeds u64"))?,
            content_sha256,
            provider_generation: Some(cdf_kernel::ContentProviderGeneration::new("generation-1")?),
        })
    }
}

impl EngineWorkerArtifactAuthority for MemoryWorkerCompilerArtifacts {
    fn memory(&self) -> Arc<dyn cdf_memory::MemoryCoordinator> {
        Arc::clone(&self.memory)
    }

    fn read_compiler_artifact(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_bytes: u64,
    ) -> Result<VerifiedWorkerCompilerArtifact> {
        let lease = reserve_worker_artifact_memory(
            &self.memory,
            reference.byte_count,
            maximum_bytes,
            cdf_memory::MemoryClass::Control,
        )?;
        let bytes =
            self.values.get(&reference.kind).cloned().ok_or_else(|| {
                cdf_kernel::CdfError::contract("test compiler artifact is missing")
            })?;
        VerifiedWorkerCompilerArtifact::new(
            reference,
            cdf_memory::AccountedBytes::new(bytes::Bytes::from(bytes), lease)?,
            reference.provider_generation.as_ref(),
            maximum_bytes,
        )
    }

    fn verify_output_artifact(
        &self,
        _reference: &cdf_runtime::WorkerArtifactReference,
        _maximum_encoded_bytes: u64,
        _maximum_decoded_bytes: u64,
    ) -> Result<cdf_runtime::VerifiedWorkerArtifactFacts> {
        Err(cdf_kernel::CdfError::internal(
            "compiler fixture contains no worker output artifacts",
        ))
    }

    fn read_prepared_segment(
        &self,
        _reference: &cdf_runtime::WorkerArtifactReference,
        _maximum_encoded_bytes: u64,
        _maximum_decoded_bytes: u64,
    ) -> Result<VerifiedPreparedSegmentArtifact> {
        Err(cdf_kernel::CdfError::internal(
            "compiler fixture contains no prepared segment artifacts",
        ))
    }

    fn read_canonical_segment(
        &self,
        _reference: &cdf_runtime::WorkerArtifactReference,
        _maximum_encoded_bytes: u64,
        _maximum_decoded_bytes: u64,
    ) -> Result<VerifiedCanonicalSegmentArtifact> {
        Err(cdf_kernel::CdfError::internal(
            "compiler fixture contains no canonical segment artifacts",
        ))
    }

    fn read_partition_evidence(
        &self,
        _reference: &cdf_runtime::WorkerArtifactReference,
        _maximum_bytes: u64,
    ) -> Result<VerifiedEnginePartitionEvidenceArtifact> {
        Err(cdf_kernel::CdfError::internal(
            "compiler fixture contains no partition evidence artifacts",
        ))
    }
}

type SharedEngineWorkerArtifactMap = BTreeMap<(String, String), bytes::Bytes>;
type SharedEngineWorkerLeaseMap =
    BTreeMap<(cdf_kernel::LeaseAuthorityDomainId, String), (cdf_runtime::WorkerLeaseState, i64)>;

#[derive(Clone)]
struct SharedEngineWorkerArtifacts {
    values: Arc<Mutex<SharedEngineWorkerArtifactMap>>,
    leases: Arc<Mutex<SharedEngineWorkerLeaseMap>>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
}

impl Default for SharedEngineWorkerArtifacts {
    fn default() -> Self {
        Self {
            values: Arc::new(Mutex::new(BTreeMap::new())),
            leases: Arc::new(Mutex::new(BTreeMap::new())),
            memory: Arc::new(
                cdf_memory::DeterministicMemoryCoordinator::new(
                    2 * 1024 * 1024 * 1024,
                    BTreeMap::new(),
                )
                .unwrap(),
            ),
        }
    }
}

impl SharedEngineWorkerArtifacts {
    fn lease_key(
        domain: &cdf_kernel::LeaseAuthorityDomainId,
        scope: &cdf_kernel::ScopeKey,
    ) -> Result<(cdf_kernel::LeaseAuthorityDomainId, String)> {
        Ok((domain.clone(), cdf_runtime::artifact_hash(scope)?))
    }

    fn admit_lease(&self, lease: cdf_runtime::WorkerLeaseState, now_ms: i64) -> Result<()> {
        lease.validate()?;
        let key = Self::lease_key(&lease.lease_authority_domain_id, &lease.lease_scope)?;
        let mut leases = self.leases.lock().unwrap();
        match leases.get(&key) {
            Some((current, current_now_ms)) if current == &lease && *current_now_ms == now_ms => {}
            Some((current, _)) if current.fencing_token.get() < lease.fencing_token.get() => {
                leases.insert(key, (lease, now_ms));
            }
            Some(_) => {
                return Err(cdf_kernel::CdfError::contract(
                    "worker artifact lease authority cannot regress or rewrite an existing fence",
                ));
            }
            None => {
                leases.insert(key, (lease, now_ms));
            }
        }
        Ok(())
    }

    fn bytes_for(&self, reference: &cdf_runtime::WorkerArtifactReference) -> Result<bytes::Bytes> {
        self.values
            .lock()
            .unwrap()
            .get(&(
                reference.store_namespace.as_str().to_owned(),
                reference.object_key.as_str().to_owned(),
            ))
            .cloned()
            .ok_or_else(|| cdf_kernel::CdfError::contract("worker artifact is missing"))
    }

    fn accounted_bytes_for(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_bytes: u64,
        class: cdf_memory::MemoryClass,
    ) -> Result<cdf_memory::AccountedBytes> {
        let lease = reserve_worker_artifact_memory(
            &self.memory,
            reference.byte_count,
            maximum_bytes,
            class,
        )?;
        cdf_memory::AccountedBytes::new(self.bytes_for(reference)?, lease)
    }

    fn decoded_lease(&self, maximum_bytes: u64) -> Result<cdf_memory::MemoryLease> {
        reserve_worker_artifact_memory(
            &self.memory,
            maximum_bytes,
            maximum_bytes,
            cdf_memory::MemoryClass::Decode,
        )
    }

    fn object_state(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
    ) -> cdf_runtime::WorkerArtifactObjectState {
        match self.bytes_for(reference) {
            Ok(bytes) => cdf_runtime::WorkerArtifactObjectState::Present {
                content_sha256: format!(
                    "sha256:{:x}",
                    <sha2::Sha256 as sha2::Digest>::digest(bytes.as_ref())
                ),
                provider_generation: reference
                    .provider_generation
                    .clone()
                    .expect("memory worker artifacts always bind a generation"),
            },
            Err(_) => cdf_runtime::WorkerArtifactObjectState::Absent,
        }
    }

    fn atomic_write_authorized(
        &self,
        authorization: cdf_runtime::WorkerArtifactWriteAuthorization<'_>,
        bytes: cdf_memory::AccountedBytes,
    ) -> Result<cdf_runtime::VerifiedWorkerArtifactFacts> {
        let reference = &authorization.receipt().artifact;
        let observed = Self::reference(
            reference.kind,
            reference.store_namespace.as_str(),
            reference.object_key.as_str().to_owned(),
            bytes.payload(),
        )?;
        if &observed != reference {
            return Err(cdf_kernel::CdfError::contract(
                "authorized worker output bytes do not match their receipt",
            ));
        }

        // Test-provider analogue of an object-store conditional transaction. Lease/fence updates
        // use the same first lock, and object inspection plus mutation use the same second lock.
        let leases = self.leases.lock().unwrap();
        let (current_lease, now_ms) = leases
            .get(&Self::lease_key(
                &authorization.permit().lease_authority_domain_id,
                &authorization.permit().lease_scope,
            )?)
            .ok_or_else(|| {
                cdf_kernel::CdfError::contract(
                    "worker artifact provider has no current lease authority",
                )
            })?;
        let key = (
            reference.store_namespace.as_str().to_owned(),
            reference.object_key.as_str().to_owned(),
        );
        let mut values = self.values.lock().unwrap();
        let object_state =
            values
                .get(&key)
                .map_or(cdf_runtime::WorkerArtifactObjectState::Absent, |existing| {
                    cdf_runtime::WorkerArtifactObjectState::Present {
                        content_sha256: format!(
                            "sha256:{:x}",
                            <sha2::Sha256 as sha2::Digest>::digest(existing.as_ref())
                        ),
                        provider_generation: cdf_kernel::ContentProviderGeneration::new(
                            "memory-generation-1",
                        )
                        .unwrap(),
                    }
                });
        authorization.validate_provider_preconditions(current_lease, &object_state, *now_ms)?;
        if matches!(object_state, cdf_runtime::WorkerArtifactObjectState::Absent) {
            values.insert(key, bytes.into_retained_bytes());
        }
        drop(values);
        drop(leases);

        let row_count = match &authorization.receipt().role {
            cdf_runtime::WorkerArtifactRole::PreparedSegment { row_count, .. }
            | cdf_runtime::WorkerArtifactRole::CanonicalSegment { row_count, .. } => {
                Some(*row_count)
            }
            _ => None,
        };
        cdf_runtime::VerifiedWorkerArtifactFacts::new(reference.clone(), row_count)
    }

    fn reference(
        kind: cdf_runtime::WorkerArtifactKind,
        namespace: &str,
        key: String,
        bytes: &[u8],
    ) -> Result<cdf_runtime::WorkerArtifactReference> {
        Ok(cdf_runtime::WorkerArtifactReference {
            kind,
            store_namespace: cdf_kernel::ContentStoreNamespace::new(namespace)?,
            object_key: cdf_kernel::ContentObjectKey::new(key)?,
            byte_count: u64::try_from(bytes.len())
                .map_err(|_| cdf_kernel::CdfError::contract("worker artifact exceeds u64"))?,
            content_sha256: format!("sha256:{:x}", <sha2::Sha256 as sha2::Digest>::digest(bytes)),
            provider_generation: Some(cdf_kernel::ContentProviderGeneration::new(
                "memory-generation-1",
            )?),
        })
    }

    fn observed_output_rows(
        bytes: cdf_memory::AccountedBytes,
        decoded_lease: cdf_memory::MemoryLease,
        maximum_decoded_bytes: u64,
    ) -> Result<u64> {
        let bytes = bytes.into_retained_bytes();
        let mut reader = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(cdf_kernel::CdfError::from)?;
        let rows =
            reader.try_fold(0_u64, |rows, batch| {
                let batch = batch?;
                if cdf_memory::record_batch_retained_bytes(&batch)? > maximum_decoded_bytes {
                    return Err(cdf_kernel::CdfError::data(
                        "worker segment batch exceeds its admitted decoded-memory budget",
                    ));
                }
                rows.checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    cdf_kernel::CdfError::data("worker segment row count exceeds u64")
                })?)
                .ok_or_else(|| cdf_kernel::CdfError::data("worker segment row count overflow"))
            })?;
        drop(decoded_lease);
        Ok(rows)
    }
}

fn reserve_worker_artifact_memory(
    memory: &Arc<dyn cdf_memory::MemoryCoordinator>,
    bytes: u64,
    maximum_bytes: u64,
    class: cdf_memory::MemoryClass,
) -> Result<cdf_memory::MemoryLease> {
    if bytes == 0 || bytes > maximum_bytes {
        return Err(cdf_kernel::CdfError::data(
            "worker artifact exceeds its admitted memory window",
        ));
    }
    let request = cdf_memory::ReservationRequest::new(
        cdf_memory::ConsumerKey::new("isolated-worker-artifact", class)?,
        bytes,
    )?;
    memory.try_reserve(&request)?.ok_or_else(|| {
        cdf_kernel::CdfError::data(
            "isolated worker artifact memory is exhausted; reduce jobs or raise the worker memory budget",
        )
    })
}

fn account_worker_artifact_bytes(
    memory: &Arc<dyn cdf_memory::MemoryCoordinator>,
    bytes: Vec<u8>,
    maximum_bytes: u64,
    class: cdf_memory::MemoryClass,
) -> Result<cdf_memory::AccountedBytes> {
    let byte_count = u64::try_from(bytes.len())
        .map_err(|_| cdf_kernel::CdfError::data("worker artifact exceeds u64"))?;
    let lease = reserve_worker_artifact_memory(memory, byte_count, maximum_bytes, class)?;
    cdf_memory::AccountedBytes::new(bytes::Bytes::from(bytes), lease)
}

fn prepared_segment_bytes(
    canonical_bytes: Vec<u8>,
    package_row_ord_start: u64,
    row_count: u64,
) -> Result<Vec<u8>> {
    let reader =
        arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(canonical_bytes), None)
            .map_err(cdf_kernel::CdfError::from)?;
    let canonical = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(cdf_kernel::CdfError::from)?;
    cdf_package_contract::validate_package_row_ord_batches(
        &canonical,
        package_row_ord_start,
        row_count,
    )?;
    let first = canonical
        .first()
        .ok_or_else(|| cdf_kernel::CdfError::data("canonical fixture segment is empty"))?;
    let logical_schema = Arc::new(cdf_package_contract::logical_output_schema(
        first.schema().as_ref(),
    )?);
    let logical = canonical
        .into_iter()
        .map(|batch| {
            arrow_array::RecordBatch::try_new(
                Arc::clone(&logical_schema),
                batch.columns()[..batch.num_columns() - 1].to_vec(),
            )
            .map_err(cdf_kernel::CdfError::from)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut bytes = Vec::new();
    cdf_package::encode_canonical_segment_ipc(&mut bytes, logical_schema.as_ref(), &logical)?;
    Ok(bytes)
}

impl WorkerCompilerArtifactWriter for SharedEngineWorkerArtifacts {
    fn write(
        &mut self,
        kind: cdf_runtime::WorkerArtifactKind,
        canonical_bytes: &[u8],
    ) -> Result<cdf_runtime::WorkerArtifactReference> {
        let digest = format!(
            "{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(canonical_bytes)
        );
        let reference = Self::reference(
            kind,
            "engine-worker-compiler",
            format!("compiler/{kind:?}/{digest}.json"),
            canonical_bytes,
        )?;
        self.values.lock().unwrap().insert(
            (
                reference.store_namespace.as_str().to_owned(),
                reference.object_key.as_str().to_owned(),
            ),
            bytes::Bytes::copy_from_slice(canonical_bytes),
        );
        Ok(reference)
    }
}

impl EngineWorkerArtifactAuthority for SharedEngineWorkerArtifacts {
    fn memory(&self) -> Arc<dyn cdf_memory::MemoryCoordinator> {
        Arc::clone(&self.memory)
    }

    fn read_compiler_artifact(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_bytes: u64,
    ) -> Result<VerifiedWorkerCompilerArtifact> {
        VerifiedWorkerCompilerArtifact::new(
            reference,
            self.accounted_bytes_for(reference, maximum_bytes, cdf_memory::MemoryClass::Control)?,
            reference.provider_generation.as_ref(),
            maximum_bytes,
        )
    }

    fn verify_output_artifact(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
    ) -> Result<cdf_runtime::VerifiedWorkerArtifactFacts> {
        let bytes = self.accounted_bytes_for(
            reference,
            maximum_encoded_bytes,
            cdf_memory::MemoryClass::Package,
        )?;
        let observed = Self::reference(
            reference.kind,
            reference.store_namespace.as_str(),
            reference.object_key.as_str().to_owned(),
            bytes.payload(),
        )?;
        if &observed != reference {
            return Err(cdf_kernel::CdfError::contract(
                "stored worker output bytes do not match their result reference",
            ));
        }
        let row_count = matches!(
            reference.kind,
            cdf_runtime::WorkerArtifactKind::PreparedSegment
                | cdf_runtime::WorkerArtifactKind::CanonicalSegment
        )
        .then(|| {
            Self::observed_output_rows(
                bytes,
                self.decoded_lease(maximum_decoded_bytes)?,
                maximum_decoded_bytes,
            )
        })
        .transpose()?;
        cdf_runtime::VerifiedWorkerArtifactFacts::new(reference.clone(), row_count)
    }

    fn read_prepared_segment(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
    ) -> Result<VerifiedPreparedSegmentArtifact> {
        VerifiedPreparedSegmentArtifact::new(
            reference,
            self.accounted_bytes_for(
                reference,
                maximum_encoded_bytes,
                cdf_memory::MemoryClass::Package,
            )?,
            self.decoded_lease(maximum_decoded_bytes)?,
            reference.provider_generation.as_ref(),
            maximum_encoded_bytes,
            maximum_decoded_bytes,
        )
    }

    fn read_canonical_segment(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
    ) -> Result<VerifiedCanonicalSegmentArtifact> {
        VerifiedCanonicalSegmentArtifact::new(
            reference,
            self.accounted_bytes_for(
                reference,
                maximum_encoded_bytes,
                cdf_memory::MemoryClass::Package,
            )?,
            self.decoded_lease(maximum_decoded_bytes)?,
            reference.provider_generation.as_ref(),
            maximum_encoded_bytes,
            maximum_decoded_bytes,
        )
    }

    fn read_partition_evidence(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_bytes: u64,
    ) -> Result<VerifiedEnginePartitionEvidenceArtifact> {
        VerifiedEnginePartitionEvidenceArtifact::new(
            reference,
            self.accounted_bytes_for(reference, maximum_bytes, cdf_memory::MemoryClass::Control)?,
            reference.provider_generation.as_ref(),
            maximum_bytes,
        )
    }
}

impl EngineWorkerOutputAuthority for SharedEngineWorkerArtifacts {
    fn reference_for_bytes(
        &self,
        kind: cdf_runtime::WorkerArtifactKind,
        namespace: &cdf_kernel::ContentStoreNamespace,
        object_key: cdf_kernel::ContentObjectKey,
        bytes: &[u8],
    ) -> Result<cdf_runtime::WorkerArtifactReference> {
        Self::reference(
            kind,
            namespace.as_str(),
            object_key.as_str().to_owned(),
            bytes,
        )
    }

    fn object_state(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
    ) -> Result<cdf_runtime::WorkerArtifactObjectState> {
        Ok(Self::object_state(self, reference))
    }

    fn write_authorized_bytes(
        &self,
        authorization: cdf_runtime::WorkerArtifactWriteAuthorization<'_>,
        bytes: cdf_memory::AccountedBytes,
    ) -> Result<cdf_runtime::VerifiedWorkerArtifactFacts> {
        self.atomic_write_authorized(authorization, bytes)
    }
}

struct PendingEngineWorkerWrite<'a> {
    store: &'a SharedEngineWorkerArtifacts,
    bytes: Option<cdf_memory::AccountedBytes>,
}

impl cdf_runtime::WorkerAuthorizedArtifactSink for PendingEngineWorkerWrite<'_> {
    fn write_authorized(
        &mut self,
        authorization: cdf_runtime::WorkerArtifactWriteAuthorization<'_>,
    ) -> Result<cdf_runtime::VerifiedWorkerArtifactFacts> {
        let bytes = self
            .bytes
            .take()
            .ok_or_else(|| cdf_kernel::CdfError::internal("worker output was already consumed"))?;
        self.store.atomic_write_authorized(authorization, bytes)
    }
}

fn isolated_mock_option_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "source": {"type": "object", "additionalProperties": false, "properties": {}},
        "resource": {"type": "object", "additionalProperties": false, "properties": {}}
    })
}

#[derive(Clone)]
struct IsolatedEngineMockDriver {
    descriptor: cdf_runtime::SourceDriverDescriptor,
    option_schema: serde_json::Value,
    dataset_id: String,
    resource: MockResource,
}

impl cdf_runtime::SourceDriver for IsolatedEngineMockDriver {
    fn descriptor(&self) -> &cdf_runtime::SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn compile(
        &self,
        _request: cdf_runtime::SourceCompileRequest,
    ) -> Result<cdf_runtime::CompiledSourcePlan> {
        Err(cdf_kernel::CdfError::internal(
            "isolated engine fixture compiles its source explicitly",
        ))
    }

    fn validate_portable_plan(&self, plan: &cdf_runtime::CompiledSourcePlan) -> Result<()> {
        plan.validate()?;
        let expected_source_bytes =
            self.resource
                .batches
                .iter()
                .try_fold(0_u64, |total, batch| {
                    total.checked_add(batch.header.byte_count).ok_or_else(|| {
                        cdf_kernel::CdfError::data("isolated mock source byte count overflow")
                    })
                })?;
        if plan.physical_plan["dataset_id"].as_str() != Some(self.dataset_id.as_str())
            || plan.physical_plan["source_bytes"].as_u64() != Some(expected_source_bytes)
        {
            return Err(cdf_kernel::CdfError::contract(
                "isolated mock source authority does not match the worker-owned dataset",
            ));
        }
        Ok(())
    }

    fn verify_worker_source(
        &self,
        task: &cdf_runtime::PortablePartitionTask,
        plan: &cdf_runtime::CompiledSourcePlan,
        partition: &PartitionPlan,
        attestation: &cdf_runtime::WorkerSourceAttestation,
        observations: &[cdf_runtime::WorkerProcessedObservation],
    ) -> Result<cdf_runtime::VerifiedWorkerSourceFacts> {
        self.validate_portable_plan(plan)?;
        let batches = self
            .resource
            .batches
            .iter()
            .filter(|batch| batch.header.partition_id == partition.partition_id)
            .collect::<Vec<_>>();
        let first = batches.first().ok_or_else(|| {
            cdf_kernel::CdfError::contract(
                "isolated mock driver cannot verify an unknown partition",
            )
        })?;
        let physical_schema_hash = first.header.observed_schema_hash.clone();
        let processed_position = first.header.source_position.clone().ok_or_else(|| {
            cdf_kernel::CdfError::contract(
                "isolated mock driver requires an exact processed source position",
            )
        })?;
        if batches.iter().any(|batch| {
            batch.header.source_position.as_ref() != Some(&processed_position)
                || batch.header.observed_schema_hash != physical_schema_hash
        }) {
            return Err(cdf_kernel::CdfError::contract(
                "isolated mock partition batches disagree on position or schema",
            ));
        }
        let expected_observation_id = partition
            .metadata
            .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
            .map(String::as_str)
            .unwrap_or_else(|| partition.partition_id.as_str());
        let input_rows = batches.iter().try_fold(0_u64, |total, batch| {
            total
                .checked_add(batch.header.row_count)
                .ok_or_else(|| cdf_kernel::CdfError::data("mock input rows overflowed u64"))
        })?;
        let source_bytes = batches.iter().try_fold(0_u64, |total, batch| {
            total
                .checked_add(batch.header.byte_count)
                .ok_or_else(|| cdf_kernel::CdfError::data("mock source bytes overflowed u64"))
        })?;
        if task.partition.partition_id != partition.partition_id
            || attestation.processed_position
                != cdf_runtime::WorkerPosition::inline(processed_position.clone())?
            || attestation.physical_schema_hash != physical_schema_hash
            || observations.len() != 1
            || observations[0].observation_id != expected_observation_id
            || observations[0].source_position
                != cdf_runtime::WorkerPosition::inline(processed_position.clone())?
        {
            return Err(cdf_kernel::CdfError::contract(format!(
                "isolated worker source result exceeds registered driver authority: task_partition_match={}, position_match={}, schema_match={}, observation_count={}, observation_id={:?}, expected_observation_id={expected_observation_id:?}, observation_position_match={}",
                task.partition.partition_id == partition.partition_id,
                attestation.processed_position
                    == cdf_runtime::WorkerPosition::inline(processed_position.clone())?,
                attestation.physical_schema_hash == physical_schema_hash,
                observations.len(),
                observations
                    .first()
                    .map(|observation| observation.observation_id.as_str()),
                observations
                    .first()
                    .is_some_and(|observation| observation.source_position
                        == cdf_runtime::WorkerPosition::inline(processed_position.clone())
                            .expect("validated fixture position")),
            )));
        }
        cdf_runtime::VerifiedWorkerSourceFacts::new(
            cdf_runtime::WorkerPosition::inline(processed_position)?,
            physical_schema_hash,
            input_rows,
            source_bytes,
            true,
        )
    }

    fn health(
        &self,
        _request: cdf_runtime::SourceHealthRequest,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
        _output: &mut dyn cdf_runtime::SourceHealthSink,
    ) -> Result<()> {
        Err(cdf_kernel::CdfError::internal(
            "isolated engine fixture does not probe source health",
        ))
    }

    fn discovery_session(
        &self,
        _plan: &cdf_runtime::CompiledSourcePlan,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
    ) -> Result<Box<dyn cdf_runtime::SourceDiscoverySession>> {
        Err(cdf_kernel::CdfError::internal(
            "isolated engine fixture does not discover sources",
        ))
    }

    fn resolve(
        &self,
        plan: &cdf_runtime::CompiledSourcePlan,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        self.validate_portable_plan(plan)?;
        let resource = self.resource.clone();
        resource.bind_compiled_source(plan);
        Ok(Arc::new(resource))
    }
}

struct RejectingEngineWorkerSecrets;

impl cdf_http::SecretProvider for RejectingEngineWorkerSecrets {
    fn resolve(&self, _uri: &cdf_http::SecretUri) -> Result<cdf_http::SecretValue> {
        Err(cdf_kernel::CdfError::contract(
            "isolated mock source does not accept secrets",
        ))
    }
}

struct ActualEngineIsolatedExecutor<'a> {
    registry: &'a cdf_runtime::SourceRegistry,
    source_context: &'a cdf_runtime::SourceResolutionContext<'a>,
    services: cdf_runtime::ExecutionServices,
    artifacts: SharedEngineWorkerArtifacts,
    lease: cdf_runtime::WorkerLeaseState,
    package_root: &'a std::path::Path,
    now_ms: i64,
}

impl cdf_runtime::IsolatedPartitionExecutor for ActualEngineIsolatedExecutor<'_> {
    fn execute(
        &self,
        invocation: cdf_runtime::IsolatedPartitionInvocation,
    ) -> cdf_kernel::BoxFuture<'_, Result<cdf_runtime::PartitionWorkerResult>> {
        let result = (|| {
            let (task, attempt, authority) = invocation.into_parts();
            let program = authority.execution_program::<ReconstructedEngineWorkerProgram>()?;
            if program
                .plan()
                .scan
                .inline_partitions()
                .expect("isolated execution uses inline partition authority")
                .get(task.partition.canonical_partition_ordinal as usize)
                != Some(authority.partition())
            {
                return Err(cdf_kernel::CdfError::contract(
                    "local isolated execution partition does not match its full plan authority",
                ));
            }
            let resource = self
                .registry
                .resolve(authority.source(), self.source_context)?;
            let source_bytes = authority.source().physical_plan["partition_source_bytes"]
                [authority.partition().partition_id.as_str()]
            .as_u64()
            .ok_or_else(|| {
                cdf_kernel::CdfError::contract(
                    "isolated mock source plan omits its partition source byte count",
                )
            })?;
            let scheduler = cdf_runtime::resolve_runtime_scheduler(
                1,
                &authority.source().execution_capabilities,
                &cdf_runtime::DestinationRuntimeCapabilities::default(),
                &self.services,
                Some(task.resources.cpu_slots),
            )?;
            let plan = program.partition_execution_plan()?;
            let package_dir = self.package_root.join(&attempt.attempt_id);
            let execution_plan = plan.clone();
            let execution_package_dir = package_dir.clone();
            let execution_services = self.services.clone();
            let output = std::thread::scope(|scope| {
                scope
                    .spawn(move || {
                        let pre_finalize =
                            |_builder: &cdf_package::PackageBuilder,
                             _draft: EnginePackageDraft<'_>| Ok(());
                        let options = EngineExecutionConfig::default()
                            .with_execution_services(execution_services)
                            .with_scheduler_resolution(scheduler)
                            .new_invocation();
                        match &execution_plan.execution_extent {
                            ExecutionExtent::Bounded { .. } => block_on(
                                super::execute_to_package_with_segment_positions_and_pre_finalize(
                                    &execution_plan,
                                    resource.as_ref(),
                                    execution_package_dir,
                                    &pre_finalize,
                                    options,
                                ),
                            ),
                            ExecutionExtent::Drain { .. } => {
                                let mut controller = cdf_runtime::DrainEpochController::new(
                                    &execution_plan.execution_extent,
                                )?;
                                block_on(super::execute_drain_epoch_with_hooks(
                                    &execution_plan,
                                    resource.as_ref(),
                                    execution_package_dir,
                                    &pre_finalize,
                                    super::DrainEpochExecution::new(&mut controller),
                                    options,
                                ))?
                                .into_package()
                            }
                            ExecutionExtent::Resident { .. } => {
                                Err(cdf_kernel::CdfError::contract(
                                    "isolated resident execution is not enabled",
                                ))
                            }
                        }
                    })
                    .join()
                    .map_err(|_| {
                        cdf_kernel::CdfError::internal("isolated engine execution thread panicked")
                    })?
            })?;
            let processed = output.execution_evidence().processed_observations();
            let processed_position = if processed.is_empty() {
                output
                    .segment_positions
                    .iter()
                    .rev()
                    .find_map(|position| position.output_position.clone())
                    .ok_or_else(|| {
                        cdf_kernel::CdfError::data(
                            "isolated worker produced no exact processed source position",
                        )
                    })?
            } else {
                cdf_kernel::aggregate_processed_observation_positions(
                    None,
                    processed,
                    &plan.write_disposition,
                )?
            };
            let stream_admission_bytes = std::fs::read(
                package_dir.join("schema/stream-admission-evidence.json"),
            )
            .map_err(|error| {
                cdf_kernel::CdfError::internal(format!(
                    "read isolated partition stream-admission evidence: {error}"
                ))
            })?;
            let stream_admission =
                serde_json::from_slice(&stream_admission_bytes).map_err(|error| {
                    cdf_kernel::CdfError::contract(format!(
                        "decode isolated partition stream-admission evidence: {error}"
                    ))
                })?;
            let schema_quarantine_path =
                package_dir.join("quarantine/schema-admission-evidence.json");
            let schema_quarantine_evidence = schema_quarantine_path
                .exists()
                .then(|| {
                    std::fs::read(&schema_quarantine_path)
                        .map_err(|error| {
                            cdf_kernel::CdfError::internal(format!(
                                "read isolated partition schema-quarantine evidence: {error}"
                            ))
                        })
                        .and_then(|bytes| {
                            serde_json::from_slice(&bytes).map_err(|error| {
                                cdf_kernel::CdfError::contract(format!(
                                    "decode isolated partition schema-quarantine evidence: {error}"
                                ))
                            })
                        })
                })
                .transpose()?;
            let partition_evidence = EnginePartitionEvidence::from_execution(
                &task,
                &plan,
                &output,
                stream_admission,
                schema_quarantine_evidence,
            )?;
            let mut outcome_tamper = partition_evidence.clone();
            if let Some(observation) = outcome_tamper.processed_observations.first_mut() {
                observation.outcome = match observation.outcome {
                    cdf_kernel::ProcessedObservationOutcome::Admitted => {
                        cdf_kernel::ProcessedObservationOutcome::Quarantined
                    }
                    cdf_kernel::ProcessedObservationOutcome::Quarantined => {
                        cdf_kernel::ProcessedObservationOutcome::Admitted
                    }
                    _ => {
                        return Err(cdf_kernel::CdfError::contract(
                            "fixture encountered an unsupported processed-observation outcome",
                        ));
                    }
                };
                let error = outcome_tamper.validate(&task, &plan, None).unwrap_err();
                if !error.message.contains("outcomes") {
                    return Err(cdf_kernel::CdfError::contract(
                        "partition outcome tamper did not fail at engine evidence admission",
                    ));
                }
            }
            let mut physical_schema_hashes = partition_evidence
                .stream_admission
                .physical_observation_catalog
                .values()
                .map(PhysicalObservationEvidence::identity_hash)
                .chain(
                    partition_evidence
                        .schema_quarantine_evidence
                        .iter()
                        .flat_map(|evidence| evidence.physical_observation_catalog.values())
                        .map(PhysicalObservationEvidence::identity_hash),
                )
                .collect::<Result<Vec<_>>>()?;
            physical_schema_hashes.sort();
            physical_schema_hashes.dedup();
            let [physical_schema_hash] = physical_schema_hashes.as_slice() else {
                return Err(cdf_kernel::CdfError::contract(
                    "isolated partition source attestation requires one exact physical-schema identity",
                ));
            };
            let physical_schema_hash = physical_schema_hash.clone();
            let partition_evidence_bytes = cdf_package::canonical_json_bytes(&partition_evidence)?;
            let mut write_session = cdf_runtime::WorkerArtifactWriteSession::new(
                &task,
                &attempt,
                &self.lease,
                self.now_ms,
            )?;
            let mut receipts = Vec::with_capacity(output.output.identity_segments().len());
            for (segment_ordinal, segment) in output.output.identity_segments().iter().enumerate() {
                let segment_ordinal = u32::try_from(segment_ordinal).map_err(|_| {
                    cdf_kernel::CdfError::data("isolated worker segment ordinal exceeds u32")
                })?;
                let segment_id = program
                    .plan()
                    .segmentation_policy()?
                    .segment_id(task.partition.canonical_partition_ordinal, segment_ordinal)?;
                let canonical_bytes =
                    std::fs::read(package_dir.join(&segment.path)).map_err(|error| {
                        cdf_kernel::CdfError::internal(format!(
                            "read isolated worker segment {}: {error}",
                            segment.path
                        ))
                    })?;
                let bytes = prepared_segment_bytes(
                    canonical_bytes,
                    segment.package_row_ord_start,
                    segment.row_count,
                )?;
                let reference = SharedEngineWorkerArtifacts::reference(
                    cdf_runtime::WorkerArtifactKind::PreparedSegment,
                    attempt.write_permit.output.store_namespace.as_str(),
                    format!(
                        "{}prepared/{}.arrow",
                        attempt.write_permit.output.object_key_prefix,
                        segment_id.as_str()
                    ),
                    &bytes,
                )?;
                let receipt = cdf_runtime::WorkerArtifactReceipt {
                    role: cdf_runtime::WorkerArtifactRole::PreparedSegment {
                        segment_id,
                        partition_ordinal: task.partition.canonical_partition_ordinal,
                        segment_ordinal,
                        row_count: segment.row_count,
                    },
                    artifact: reference,
                };
                let object_state = self.artifacts.object_state(&receipt.artifact);
                let maximum_bytes = task
                    .resources
                    .memory_bytes
                    .min(task.output_policy.maximum_artifact_bytes)
                    .min(attempt.write_permit.output.maximum_bytes);
                let mut sink = PendingEngineWorkerWrite {
                    store: &self.artifacts,
                    bytes: Some(account_worker_artifact_bytes(
                        &self.artifacts.memory,
                        bytes,
                        maximum_bytes,
                        cdf_memory::MemoryClass::Package,
                    )?),
                };
                write_session.write(&receipt, &object_state, self.now_ms, &mut sink)?;
                receipts.push(receipt);
            }
            let evidence_reference = SharedEngineWorkerArtifacts::reference(
                cdf_runtime::WorkerArtifactKind::PartitionEvidence,
                attempt.write_permit.output.store_namespace.as_str(),
                format!(
                    "{}evidence/partition-{:08}.json",
                    attempt.write_permit.output.object_key_prefix,
                    task.partition.canonical_partition_ordinal
                ),
                &partition_evidence_bytes,
            )?;
            let evidence_receipt = cdf_runtime::WorkerArtifactReceipt {
                role: cdf_runtime::WorkerArtifactRole::PartitionEvidence {
                    partition_ordinal: task.partition.canonical_partition_ordinal,
                },
                artifact: evidence_reference,
            };
            let evidence_state = self.artifacts.object_state(&evidence_receipt.artifact);
            let maximum_bytes = task
                .resources
                .memory_bytes
                .min(task.output_policy.maximum_artifact_bytes)
                .min(attempt.write_permit.output.maximum_bytes);
            let mut evidence_sink = PendingEngineWorkerWrite {
                store: &self.artifacts,
                bytes: Some(account_worker_artifact_bytes(
                    &self.artifacts.memory,
                    partition_evidence_bytes,
                    maximum_bytes,
                    cdf_memory::MemoryClass::Control,
                )?),
            };
            write_session.write(
                &evidence_receipt,
                &evidence_state,
                self.now_ms,
                &mut evidence_sink,
            )?;
            receipts.push(evidence_receipt);
            receipts.sort_by(|left, right| left.artifact.cmp(&right.artifact));
            let artifact_bytes = receipts.iter().try_fold(0_u64, |total, receipt| {
                total
                    .checked_add(receipt.artifact.byte_count)
                    .ok_or_else(|| cdf_kernel::CdfError::data("worker artifact bytes overflow"))
            })?;
            let result = cdf_runtime::PartitionWorkerResult::new(
                &attempt,
                cdf_runtime::PartitionWorkerResultInput {
                    status: cdf_runtime::WorkerTerminalStatus::Succeeded,
                    source_attestation: Some(cdf_runtime::WorkerSourceAttestation {
                        processed_position: cdf_runtime::WorkerPosition::inline(
                            processed_position,
                        )?,
                        physical_schema_hash,
                    }),
                    artifacts: receipts,
                    counts: cdf_runtime::WorkerResultCounts {
                        input_rows: output.output.lineage.input_rows,
                        output_rows: output.output.profile.output_rows,
                        quarantined_rows: 0,
                        source_bytes,
                        artifact_bytes,
                    },
                    telemetry: cdf_runtime::WorkerTelemetry::default(),
                },
            )?;
            Ok(result)
        })();
        Box::pin(async move { result })
    }
}

fn isolated_engine_services(cpu_slots: u16) -> cdf_runtime::ExecutionServices {
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    cdf_runtime::ExecutionServices::new(Arc::new(
        StandaloneExecutionHost::new(
            cdf_runtime::ExecutionHostCapabilities {
                logical_cpu_slots: cpu_slots,
                io_workers: cpu_slots.max(1),
                blocking_lanes: Vec::new(),
            },
            memory,
        )
        .unwrap(),
    ))
    .unwrap()
}

fn isolated_engine_source_plan(
    resource: &MockResource,
    descriptor: cdf_runtime::SourceDriverDescriptor,
    dataset_id: &str,
    batches: &[Batch],
    extent: &ExecutionExtent,
) -> cdf_runtime::CompiledSourcePlan {
    let source_bytes = batches
        .iter()
        .map(|batch| batch.header.byte_count)
        .sum::<u64>();
    let partition_source_bytes = batches
        .iter()
        .map(|batch| {
            (
                batch.header.partition_id.as_str().to_owned(),
                batch.header.byte_count,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut source = if extent.is_bounded() {
        mock_compiled_source_plan(resource, None)
    } else {
        mock_unbounded_source_plan(resource)
    };
    source.driver = descriptor;
    source.redacted_options = serde_json::json!({});
    source.redacted_options_hash = cdf_runtime::artifact_hash(&source.redacted_options).unwrap();
    source.physical_plan = serde_json::json!({
        "dataset_id": dataset_id,
        "source_bytes": source_bytes,
        "partition_source_bytes": partition_source_bytes,
    });
    source.physical_plan_hash = cdf_kernel::PhysicalSourcePlanHash::new(
        cdf_runtime::artifact_hash(&source.physical_plan).unwrap(),
    )
    .unwrap();
    source.validate().unwrap();
    source
}

fn isolated_engine_attempt<T: cdf_runtime::PortableWorkerTask>(
    task: &T,
    attempt_id: &str,
) -> (
    cdf_runtime::PartitionAttemptEnvelope,
    cdf_runtime::WorkerLeaseState,
) {
    let domain = cdf_kernel::LeaseAuthorityDomainId::new("isolated-engine-test").unwrap();
    let fence = cdf_kernel::FencingToken::new(7).unwrap();
    let attempt = cdf_runtime::PartitionAttemptEnvelope {
        version: cdf_runtime::PARTITION_ATTEMPT_VERSION,
        attempt_id: attempt_id.to_owned(),
        retry_ordinal: 0,
        trace_id: format!("trace-{attempt_id}"),
        write_permit: cdf_runtime::WorkerArtifactWritePermit {
            task_sha256: task.task_sha256().to_owned(),
            lease_authority_domain_id: domain.clone(),
            lease_scope: task.lease_scope().clone(),
            fencing_token: fence,
            issued_at_ms: 1_000,
            expires_at_ms: 30_000,
            output: cdf_runtime::WorkerArtifactWriteScope {
                store_namespace: cdf_kernel::ContentStoreNamespace::new("engine-worker-output")
                    .unwrap(),
                object_key_prefix: format!("attempts/{attempt_id}/"),
                maximum_bytes: 128 * 1024 * 1024,
            },
            generation_precondition: cdf_runtime::WorkerObjectGenerationPrecondition::CreateOnly,
        },
    };
    let lease = cdf_runtime::WorkerLeaseState {
        lease_authority_domain_id: domain,
        lease_scope: task.lease_scope().clone(),
        fencing_token: fence,
        expires_at_ms: 30_000,
    };
    (attempt, lease)
}

fn assert_engine_store_rechecks_fence_at_mutation(
    task: &cdf_runtime::PortablePartitionTask,
    attempt: &cdf_runtime::PartitionAttemptEnvelope,
    lease: &cdf_runtime::WorkerLeaseState,
) -> Result<()> {
    let artifacts = SharedEngineWorkerArtifacts::default();
    artifacts.admit_lease(lease.clone(), 2_000)?;
    let payload = bytes::Bytes::from_static(b"fence-probe");
    let reference = SharedEngineWorkerArtifacts::reference(
        cdf_runtime::WorkerArtifactKind::PartitionEvidence,
        attempt.write_permit.output.store_namespace.as_str(),
        format!(
            "{}fence-probe.json",
            attempt.write_permit.output.object_key_prefix
        ),
        &payload,
    )?;
    let receipt = cdf_runtime::WorkerArtifactReceipt {
        role: cdf_runtime::WorkerArtifactRole::PartitionEvidence {
            partition_ordinal: task.partition.canonical_partition_ordinal,
        },
        artifact: reference.clone(),
    };
    let memory_lease = reserve_worker_artifact_memory(
        &artifacts.memory,
        reference.byte_count,
        reference.byte_count,
        cdf_memory::MemoryClass::Control,
    )?;
    let accounted = cdf_memory::AccountedBytes::new(payload, memory_lease)?;
    let mut session = cdf_runtime::WorkerArtifactWriteSession::new(task, attempt, lease, 2_000)?;
    artifacts.admit_lease(
        cdf_runtime::WorkerLeaseState {
            fencing_token: cdf_kernel::FencingToken::new(
                lease.fencing_token.get().checked_add(1).ok_or_else(|| {
                    cdf_kernel::CdfError::contract("test fencing token overflowed")
                })?,
            )?,
            ..lease.clone()
        },
        2_000,
    )?;
    let mut sink = PendingEngineWorkerWrite {
        store: &artifacts,
        bytes: Some(accounted),
    };
    let error = session
        .write(
            &receipt,
            &cdf_runtime::WorkerArtifactObjectState::Absent,
            2_000,
            &mut sink,
        )
        .unwrap_err();
    if !error.message.contains("stale") || artifacts.bytes_for(&reference).is_ok() {
        return Err(cdf_kernel::CdfError::contract(
            "engine artifact provider mutated under a stale fence",
        ));
    }
    let rollback = artifacts.admit_lease(lease.clone(), 2_000).unwrap_err();
    if !rollback.message.contains("cannot regress") {
        return Err(cdf_kernel::CdfError::contract(
            "engine artifact provider admitted a fencing rollback",
        ));
    }
    Ok(())
}

fn run_actual_isolated_engine_equivalence(
    cpu_slots: u16,
    partition_count: usize,
    extent: ExecutionExtent,
) -> (
    RetainedEngineRun,
    RetainedEngineRun,
    Vec<cdf_runtime::PartitionWorkerResult>,
    Vec<cdf_runtime::SegmentWorkerResult>,
) {
    let mut batches = sample_batches()
        .into_iter()
        .take(partition_count)
        .collect::<Vec<_>>();
    for batch in &mut batches {
        batch.header.source_position = Some(terminal_file_position());
    }
    let direct_resource = MockResource::tier_a(batches.clone())
        .without_control_keys()
        .with_partition_count(partition_count);
    run_actual_isolated_engine_equivalence_for_resource(cpu_slots, direct_resource, extent)
}

fn run_actual_isolated_engine_equivalence_for_resource(
    cpu_slots: u16,
    direct_resource: MockResource,
    extent: ExecutionExtent,
) -> (
    RetainedEngineRun,
    RetainedEngineRun,
    Vec<cdf_runtime::PartitionWorkerResult>,
    Vec<cdf_runtime::SegmentWorkerResult>,
) {
    let batches = direct_resource.batches.clone();
    let partition_count = direct_resource.partition_count;
    let dataset_id = "isolated-orders-v1";
    let option_schema = isolated_mock_option_schema();
    let descriptor = cdf_runtime::SourceDriverDescriptor {
        driver_id: cdf_runtime::SourceDriverId::new("isolated_engine_mock").unwrap(),
        driver_version: "1.0.0".to_owned(),
        option_schema_hash: cdf_runtime::artifact_hash(&option_schema).unwrap(),
        kinds: vec!["isolated_engine_mock".to_owned()],
        schemes: vec!["isolated-engine-mock".to_owned()],
    };
    let source = isolated_engine_source_plan(
        &direct_resource,
        descriptor.clone(),
        dataset_id,
        &batches,
        &extent,
    );
    direct_resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &direct_resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    assert_eq!(plan.scan.partition_count().unwrap(), partition_count as u64);

    let direct_services = isolated_engine_services(cpu_slots);
    let direct_scheduler = cdf_runtime::resolve_runtime_scheduler(
        u64::try_from(partition_count).unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &direct_services,
        Some(cpu_slots),
    )
    .unwrap();
    let direct_root = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let direct_options = EngineExecutionConfig::default()
        .with_execution_services(direct_services)
        .with_scheduler_resolution(direct_scheduler)
        .new_invocation();
    let direct = match &plan.execution_extent {
        ExecutionExtent::Bounded { .. } => block_on(
            super::execute_to_package_with_segment_positions_and_pre_finalize(
                &plan,
                &direct_resource,
                direct_root.path(),
                &pre_finalize,
                direct_options,
            ),
        )
        .unwrap(),
        ExecutionExtent::Drain { .. } => {
            let mut controller =
                cdf_runtime::DrainEpochController::new(&plan.execution_extent).unwrap();
            block_on(super::execute_drain_epoch_with_hooks(
                &plan,
                &direct_resource,
                direct_root.path(),
                &pre_finalize,
                super::DrainEpochExecution::new(&mut controller),
                direct_options,
            ))
            .unwrap()
            .into_package()
            .unwrap()
        }
        ExecutionExtent::Resident { .. } => unreachable!("resident plans do not compile"),
    };

    let compatibility = cdf_runtime::WorkerCompatibility {
        cdf_version: "0.1.0".to_owned(),
        artifact_version: "package-v2".to_owned(),
        arrow_version: "58.3.0".to_owned(),
        relational_engine: cdf_runtime::WorkerComponentVersion {
            component: "datafusion".to_owned(),
            version: "54.0.0".to_owned(),
        },
        normalizer_version: plan.validation_program.normalizer_version.clone(),
    };
    let control = cdf_runtime::WorkerControlBudget {
        maximum_task_bytes: 128 * 1024,
        maximum_attempt_bytes: 32 * 1024,
        maximum_result_bytes: 128 * 1024,
        maximum_input_artifacts: 32,
        maximum_output_artifacts: 64,
        maximum_secret_references: 8,
    };
    let mut artifacts = SharedEngineWorkerArtifacts::default();
    let resources = cdf_runtime::WorkerResourceBudget {
        memory_bytes: 512 * 1024 * 1024,
        disk_bytes: 512 * 1024 * 1024,
        cpu_slots,
        io_slots: cpu_slots,
        control: control.clone(),
    };
    let attempt_policy = cdf_runtime::WorkerAttemptPolicy {
        maximum_attempts: 2,
        maximum_attempt_duration_ms: 29_000,
    };
    let partition_tasks = plan
        .scan
        .inline_partitions()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(ordinal, partition)| {
            compile_engine_partition_task(
                EnginePartitionTaskInput {
                    compatibility: compatibility.clone(),
                    pipeline_id: cdf_kernel::PipelineId::new("isolated-engine-test").unwrap(),
                    source: &source,
                    plan: &plan,
                    partition,
                    canonical_partition_ordinal: u64::try_from(ordinal).unwrap(),
                    epoch_ordinal: None,
                    input_checkpoint: None,
                    secret_references: Vec::new(),
                    input_artifacts: Vec::new(),
                    resources: resources.clone(),
                    attempt_policy: attempt_policy.clone(),
                    capabilities: cdf_runtime::WorkerCapabilityRequirements {
                        required_blocking_lanes: Vec::new(),
                        services: Vec::new(),
                    },
                    output_policy: cdf_runtime::WorkerOutputPolicy {
                        allowed_kinds: vec![
                            cdf_runtime::WorkerArtifactKind::PreparedSegment,
                            cdf_runtime::WorkerArtifactKind::PartitionEvidence,
                        ],
                        maximum_artifact_bytes: 128 * 1024 * 1024,
                    },
                },
                &mut artifacts,
            )
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let worker_services = isolated_engine_services(cpu_slots);
    let worker_resource = direct_resource.clone();
    let worker_capabilities = cdf_runtime::WorkerRuntimeCapabilities {
        host: worker_services.capabilities().clone(),
        memory_bytes: 512 * 1024 * 1024,
        disk_bytes: 512 * 1024 * 1024,
        control,
        services: Vec::new(),
    };

    struct PartitionFixture {
        task: cdf_runtime::PortablePartitionTask,
    }

    let fixtures = partition_tasks
        .into_iter()
        .map(|task| PartitionFixture { task })
        .collect::<Vec<_>>();

    let mut preparations = Vec::with_capacity(fixtures.len());
    let mut partition_evidence = Vec::with_capacity(fixtures.len());
    let jobs = usize::from(cpu_slots).max(1);
    let plan_authority = &plan;
    for chunk in fixtures.chunks(jobs) {
        let completed = std::thread::scope(|scope| {
            chunk
                .iter()
                .enumerate()
                .map(|(chunk_ordinal, fixture)| {
                    let artifacts = artifacts.clone();
                    let compatibility = compatibility.clone();
                    let worker_capabilities = worker_capabilities.clone();
                    let worker_services = worker_services.clone();
                    let descriptor = descriptor.clone();
                    let option_schema = option_schema.clone();
                    let worker_resource = worker_resource.clone();
                    scope.spawn(move || {
                        let attempt_id = format!(
                            "jobs-{cpu_slots}-partition-{}",
                            fixture.task.partition.canonical_partition_ordinal
                        );
                        let (attempt, lease) = isolated_engine_attempt(&fixture.task, &attempt_id);
                        artifacts.admit_lease(lease.clone(), 2_000)?;
                        if fixture.task.partition.canonical_partition_ordinal == 0 {
                            assert_engine_store_rechecks_fence_at_mutation(
                                &fixture.task,
                                &attempt,
                                &lease,
                            )?;
                        }
                        let mut registry = cdf_runtime::SourceRegistry::new();
                        registry.register(IsolatedEngineMockDriver {
                            descriptor,
                            option_schema,
                            dataset_id: dataset_id.to_owned(),
                            resource: worker_resource,
                        })?;
                        let worker_root = TempDir::new().map_err(|error| {
                            cdf_kernel::CdfError::internal(format!(
                                "create isolated worker root: {error}"
                            ))
                        })?;
                        let secrets: Arc<dyn cdf_http::SecretProvider + Send + Sync> =
                            Arc::new(RejectingEngineWorkerSecrets);
                        let source_context = cdf_runtime::SourceResolutionContext::new(
                            worker_root.path(),
                            secrets,
                            &worker_services,
                            Arc::new(cdf_http::EgressAllowlist::allow_any()),
                        );
                        let worker_verifier = EngineWorkerAdmissionVerifier::new(&artifacts);
                        let coordinator_verifier = EngineWorkerAdmissionVerifier::new(&artifacts);
                        let executor = ActualEngineIsolatedExecutor {
                            registry: &registry,
                            source_context: &source_context,
                            services: worker_services.clone(),
                            artifacts: artifacts.clone(),
                            lease: lease.clone(),
                            package_root: worker_root.path(),
                            now_ms: 2_000,
                        };
                        let host = cdf_runtime::LocalIsolatedWorkerHost::new(
                            &compatibility,
                            &worker_capabilities,
                            &registry,
                            &worker_verifier,
                            &executor,
                        )?;
                        let admitted = block_on(cdf_runtime::execute_local_isolated_partition(
                            &fixture.task,
                            &attempt,
                            &host,
                            &registry,
                            &coordinator_verifier,
                            &lease,
                            2_000,
                        ))?;
                        let evidence = coordinator_verifier.read_partition_evidence(
                            &fixture.task,
                            plan_authority,
                            &admitted,
                        )?;
                        Ok::<_, cdf_kernel::CdfError>((chunk_ordinal, admitted, evidence))
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        cdf_kernel::CdfError::internal("isolated partition thread panicked")
                    })?
                })
                .collect::<Result<Vec<_>>>()
        })
        .unwrap();
        for (_, admitted, evidence) in completed {
            preparations.push(admitted);
            partition_evidence.push(evidence);
        }
    }

    let mut package_row_ord_start = 0_u64;
    let mut finalized = Vec::with_capacity(direct.output.identity_segments().len());
    let segment_verifier = EngineWorkerAdmissionVerifier::new(&artifacts);
    for (fixture, preparation) in fixtures.iter().zip(&preparations) {
        let mut prepared_segments = preparation
            .result()
            .artifacts
            .iter()
            .filter_map(|receipt| match receipt.role {
                cdf_runtime::WorkerArtifactRole::PreparedSegment {
                    segment_ordinal,
                    row_count,
                    ..
                } => Some((segment_ordinal, row_count)),
                _ => None,
            })
            .collect::<Vec<_>>();
        prepared_segments.sort_unstable_by_key(|(segment_ordinal, _)| *segment_ordinal);
        for (segment_ordinal, row_count) in prepared_segments {
            let segment_task = compile_engine_segment_task(EngineSegmentTaskInput {
                plan: &plan,
                preparation_task: &fixture.task,
                preparation_result: preparation,
                segment_ordinal,
                package_row_ord_start,
                resources: fixture.task.resources.clone(),
                attempt_policy: fixture.task.attempt_policy.clone(),
                capabilities: cdf_runtime::WorkerCapabilityRequirements {
                    required_blocking_lanes: Vec::new(),
                    services: Vec::new(),
                },
                output_policy: cdf_runtime::WorkerOutputPolicy {
                    allowed_kinds: vec![cdf_runtime::WorkerArtifactKind::CanonicalSegment],
                    maximum_artifact_bytes: fixture.task.output_policy.maximum_artifact_bytes,
                },
            })
            .unwrap();
            let (segment_attempt, segment_lease) = isolated_engine_attempt(
                &segment_task,
                &format!(
                    "jobs-{cpu_slots}-partition-{}-segment-{segment_ordinal}",
                    fixture.task.partition.canonical_partition_ordinal
                ),
            );
            artifacts.admit_lease(segment_lease.clone(), 2_000).unwrap();
            let segment_executor =
                EngineIsolatedSegmentExecutor::new(&artifacts, &segment_lease, 2_000);
            let segment_host = cdf_runtime::LocalIsolatedSegmentHost::new(
                &compatibility,
                &worker_capabilities,
                &segment_verifier,
                &segment_executor,
            )
            .unwrap();
            finalized.push(
                block_on(cdf_runtime::execute_local_isolated_segment(
                    &segment_task,
                    &segment_attempt,
                    &segment_host,
                    &segment_verifier,
                    &segment_lease,
                    2_000,
                ))
                .unwrap(),
            );
            package_row_ord_start = package_row_ord_start.checked_add(row_count).unwrap();
        }
    }
    let assembled_root = TempDir::new().unwrap();
    if cpu_slots == 1 && partition_count == 1 && plan.execution_extent.is_bounded() {
        let mut replay_plan = plan.clone();
        replay_plan.package_id.push_str("-cross-plan-replay");
        let replay_root = TempDir::new().unwrap();
        let replay_error = assemble_isolated_worker_package(
            &replay_plan,
            replay_root.path(),
            partition_evidence.clone(),
            &finalized,
            &artifacts,
            &resources,
            &worker_services,
        )
        .unwrap_err();
        assert!(replay_error.message.contains("different engine plan"));
    }
    let assembled = assemble_isolated_worker_package(
        &plan,
        assembled_root.path(),
        partition_evidence,
        &finalized,
        &artifacts,
        &resources,
        &worker_services,
    )
    .unwrap();
    (
        RetainedEngineRun {
            run: direct,
            _package: direct_root,
        },
        RetainedEngineRun {
            run: assembled,
            _package: assembled_root,
        },
        preparations
            .into_iter()
            .map(cdf_runtime::AdmittedPartitionWorkerResult::into_result)
            .collect(),
        finalized
            .into_iter()
            .map(cdf_runtime::AdmittedSegmentWorkerResult::into_result)
            .collect(),
    )
}
