use crate::prelude::*;
use cdf_kernel::{
    BatchStream, BoxFuture, CommitCounts, CommitSession, ConcurrencyLimit, DeliveryGuarantee,
    DestinationId, ErrorKind, IdempotencySupport, IdempotencyToken, IdentifierRules,
    MigrationRecord, PackageHash, PartitionId, PartitionPlan, PlanId, QueryableResource, ReceiptId,
    ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream, ScanPlan, ScanRequest,
    SchemaHash, SchemaSource, ScopeKey, SegmentAck, SegmentId, TargetName, TransactionMetadata,
    TransactionSupport, TrustLevel, TypeMapping, VerifyClause,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use super::*;

struct MockProtocol {
    sheet: DestinationSheet,
}

impl DestinationProtocol for MockProtocol {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        Ok(CommitPlan {
            plan_id: PlanId::new(format!("plan-{}", self.sheet.destination))?,
            target: request.target.clone(),
            disposition: request.disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: Vec::<MigrationRecord>::new(),
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
        })
    }

    fn begin(
        &self,
        _request: DestinationCommitRequest,
        _plan: CommitPlan,
    ) -> Result<Box<dyn CommitSession + '_>> {
        Err(CdfError::destination("mock commit session is not used"))
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        Ok(ReceiptVerification {
            verified: true,
            receipt_id: receipt.receipt_id.clone(),
            reason: None,
        })
    }
}

struct MockRuntime {
    protocol: MockProtocol,
    description: DestinationDescription,
}

impl DestinationRuntime for MockRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.protocol
    }

    fn describe(&self) -> DestinationDescription {
        self.description.clone()
    }

    fn prepare_package_commit(
        &mut self,
        _package_dir: &Path,
        _reader: &PackageReader,
        _inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        Err(CdfError::destination(
            "mock package preparation is not used",
        ))
    }

    fn bind_prepared_commit(&mut self, _prepared: &mut PreparedDestinationCommit) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct MockStagedRuntime {
    protocol: Arc<MockProtocol>,
    attempts: Arc<Mutex<BTreeMap<LoadAttemptId, MockAttempt>>>,
    committed: Arc<Mutex<BTreeMap<PackageHash, Receipt>>>,
}

#[derive(Clone)]
struct MockAttempt {
    binding: StagingAttemptBinding,
    accepted_segments: Vec<StagedSegmentIdentity>,
}

impl MockStagedRuntime {
    fn new() -> Self {
        Self {
            protocol: Arc::new(MockProtocol {
                sheet: mock_sheet("mock_staged"),
            }),
            attempts: Arc::new(Mutex::new(BTreeMap::new())),
            committed: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl DestinationRuntime for MockStagedRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        self.protocol.as_ref()
    }

    fn describe(&self) -> DestinationDescription {
        DestinationDescription::new(
            self.protocol.sheet.destination.clone(),
            &["mock-staged"],
            "mock staged",
        )
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        DestinationRuntimeCapabilities {
            blocking_lanes: Vec::new(),
            ingress_mode: DestinationIngressMode::StagedDurableSegments,
            staged_ingress: Some(StagedIngressCapabilities {
                recovery: StagingRecoveryMode::Resumable,
                visibility: StagingVisibility::IsolatedUntilFinalBinding,
                abort_idempotent: true,
                lifecycle_cleanup: true,
                final_binding_requires_exclusive_writer: false,
            }),
            writer_model: DestinationWriterModel::SingleWriter,
            max_in_flight_segments: Some(2),
            max_in_flight_bytes: Some(1024),
            bulk_path: Some("mock_staged".to_owned()),
            bulk_evidence_version: None,
            replay_requires_explicit_target: false,
            replay_target_hint: None,
            replay_policy_values: Default::default(),
        }
    }

    fn begin_staged_ingress(
        &mut self,
        request: StagedIngressRequest,
    ) -> Result<Box<dyn StagedIngressSession>> {
        let mut attempts = self.attempts.lock().unwrap();
        match attempts.get(&request.attempt_id) {
            Some(existing) if existing.binding != request.binding => {
                return Err(CdfError::destination(
                    "staging attempt id is already bound to different authority",
                ));
            }
            Some(_) => {}
            None => {
                attempts.insert(
                    request.attempt_id.clone(),
                    MockAttempt {
                        binding: request.binding.clone(),
                        accepted_segments: Vec::new(),
                    },
                );
            }
        }
        drop(attempts);
        Ok(Box::new(MockStagedSession {
            request,
            attempts: Arc::clone(&self.attempts),
            committed: Arc::clone(&self.committed),
        }))
    }

    fn inspect_staged_ingress(
        &mut self,
        attempt_id: &LoadAttemptId,
    ) -> Result<Option<StagingSnapshot>> {
        Ok(self
            .attempts
            .lock()
            .unwrap()
            .get(attempt_id)
            .cloned()
            .map(|attempt| StagingSnapshot {
                attempt_id: attempt_id.clone(),
                binding: attempt.binding,
                recovery: StagingRecoveryMode::Resumable,
                accepted_segments: attempt.accepted_segments,
            }))
    }

    fn prepare_package_commit(
        &mut self,
        _package_dir: &Path,
        _reader: &PackageReader,
        _inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        Err(CdfError::destination(
            "mock staged runtime uses final binding",
        ))
    }

    fn bind_prepared_commit(&mut self, _prepared: &mut PreparedDestinationCommit) -> Result<()> {
        Err(CdfError::destination(
            "mock staged runtime uses final binding",
        ))
    }
}

struct MockStagedSession {
    request: StagedIngressRequest,
    attempts: Arc<Mutex<BTreeMap<LoadAttemptId, MockAttempt>>>,
    committed: Arc<Mutex<BTreeMap<PackageHash, Receipt>>>,
}

impl StagedIngressSession for MockStagedSession {
    fn stage_segment(&mut self, mut segment: StagedSegmentRequest) -> Result<StagedSegmentAck> {
        while segment.reader_mut().next_batch()?.is_some() {}
        let mut attempts = self.attempts.lock().unwrap();
        let accepted = attempts
            .get_mut(&self.request.attempt_id)
            .ok_or_else(|| CdfError::destination("mock staging attempt is not attached"))?;
        if segment.identity.ordinal != u32::try_from(accepted.accepted_segments.len()).unwrap() {
            return Err(CdfError::destination(
                "mock staging received a noncanonical ordinal",
            ));
        }
        accepted.accepted_segments.push(segment.identity.clone());
        Ok(StagedSegmentAck {
            attempt_id: self.request.attempt_id.clone(),
            identity: segment.identity,
            external_durable: true,
        })
    }

    fn snapshot(&self) -> Result<StagingSnapshot> {
        let accepted_segments = self
            .attempts
            .lock()
            .unwrap()
            .get(&self.request.attempt_id)
            .cloned()
            .ok_or_else(|| CdfError::destination("mock staging attempt is absent"))?
            .accepted_segments;
        Ok(StagingSnapshot {
            attempt_id: self.request.attempt_id.clone(),
            binding: self.request.binding.clone(),
            recovery: StagingRecoveryMode::Resumable,
            accepted_segments,
        })
    }

    fn bind_final(self: Box<Self>, binding: VerifiedFinalBinding) -> Result<Receipt> {
        if binding.attempt_id() != &self.request.attempt_id {
            return Err(CdfError::destination(
                "final binding attempt does not match staged session",
            ));
        }
        if binding.commit().target != self.request.binding.target
            || binding.commit().disposition != self.request.binding.disposition
            || binding.schema_hash() != &self.request.binding.schema_hash
            || binding.plan().plan_id != self.request.binding.plan_id
        {
            return Err(CdfError::destination(
                "final binding does not match the staged attempt authority",
            ));
        }
        let accepted = self
            .attempts
            .lock()
            .unwrap()
            .get(&self.request.attempt_id)
            .cloned()
            .ok_or_else(|| CdfError::destination("mock staging attempt is absent"))?
            .accepted_segments;
        binding.validate_staged_identities(&accepted)?;
        if let Some(receipt) = self
            .committed
            .lock()
            .unwrap()
            .get(&binding.commit().package_hash)
            .cloned()
        {
            return Ok(receipt);
        }
        let receipt = Receipt {
            receipt_id: ReceiptId::new(format!(
                "receipt-{}",
                binding.commit().package_hash.as_str().replace(':', "-")
            ))?,
            destination: self.request.binding.destination_id.clone(),
            target: binding.commit().target.clone(),
            package_hash: binding.commit().package_hash.clone(),
            segment_acks: accepted
                .iter()
                .map(|identity| SegmentAck {
                    segment_id: identity.segment_id.clone(),
                    row_count: identity.row_count,
                    byte_count: identity.byte_count,
                })
                .collect(),
            disposition: binding.commit().disposition.clone(),
            idempotency_token: binding.commit().idempotency_token.clone(),
            transaction: Some(TransactionMetadata {
                system: "mock_staged".to_owned(),
                values: [(
                    "load_attempt_id".to_owned(),
                    self.request.attempt_id.to_string(),
                )]
                .into_iter()
                .collect(),
            }),
            counts: CommitCounts {
                rows_written: accepted.iter().map(|identity| identity.row_count).sum(),
                ..CommitCounts::default()
            },
            schema_hash: binding.schema_hash().clone(),
            migrations: binding.plan().migrations.clone(),
            committed_at_ms: 0,
            verify: VerifyClause {
                kind: "mock".to_owned(),
                statement: "verify mock receipt".to_owned(),
                parameters: Default::default(),
            },
        };
        self.committed
            .lock()
            .unwrap()
            .insert(receipt.package_hash.clone(), receipt.clone());
        self.attempts
            .lock()
            .unwrap()
            .remove(&self.request.attempt_id);
        Ok(receipt)
    }

    fn abort(self: Box<Self>) -> Result<()> {
        self.attempts
            .lock()
            .unwrap()
            .remove(&self.request.attempt_id);
        Ok(())
    }
}

struct EmptySegmentReader {
    identity: StagedSegmentIdentity,
}

impl DurableSegmentReader for EmptySegmentReader {
    fn identity(&self) -> &StagedSegmentIdentity {
        &self.identity
    }

    fn next_batch(&mut self) -> Result<Option<arrow_array::RecordBatch>> {
        Ok(None)
    }
}

struct MockDriver {
    schemes: &'static [&'static str],
    destination: &'static str,
}

impl DestinationDriver for MockDriver {
    fn schemes(&self) -> &'static [&'static str] {
        self.schemes
    }

    fn inspect(
        &self,
        _uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        let sheet = mock_sheet(self.destination);
        let sheet_artifact = DestinationSheetArtifact::new(
            sheet.clone(),
            cdf_kernel::DestinationProtocolCapabilities::default(),
        )?;
        Ok(DestinationInspection {
            description: DestinationDescription::new(
                sheet.destination.clone(),
                self.schemes,
                self.destination,
            ),
            sheet_artifact_hash: artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: DestinationRuntimeCapabilities {
                blocking_lanes: Vec::new(),
                ingress_mode: DestinationIngressMode::StagedDurableSegments,
                staged_ingress: Some(StagedIngressCapabilities {
                    recovery: StagingRecoveryMode::Resumable,
                    visibility: StagingVisibility::IsolatedUntilFinalBinding,
                    abort_idempotent: true,
                    lifecycle_cleanup: true,
                    final_binding_requires_exclusive_writer: false,
                }),
                writer_model: DestinationWriterModel::ConcurrentSegments,
                max_in_flight_segments: Some(4),
                max_in_flight_bytes: Some(64 * 1024 * 1024),
                bulk_path: Some("mock_arrow".to_owned()),
                bulk_evidence_version: Some("v1".to_owned()),
                replay_requires_explicit_target: false,
                replay_target_hint: None,
                replay_policy_values: Default::default(),
            },
            health_probes: vec![DestinationHealthProbe {
                probe_id: "reachable".to_owned(),
                description: "mock reachability".to_owned(),
                requires_credentials: false,
                mutates_destination: false,
            }],
        })
    }

    fn resolve(
        &self,
        _uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>> {
        let sheet = mock_sheet(self.destination);
        Ok(Box::new(MockRuntime {
            description: DestinationDescription::new(
                sheet.destination.clone(),
                self.schemes,
                self.destination,
            ),
            protocol: MockProtocol { sheet },
        }))
    }
}

fn mock_sheet(destination: &str) -> DestinationSheet {
    DestinationSheet {
        destination: DestinationId::new(destination).unwrap(),
        supported_dispositions: vec![WriteDisposition::Append],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: Vec::<TypeMapping>::new(),
        identifier_rules: IdentifierRules {
            normalizer: "namecase-v1".to_owned(),
            max_length: None,
            allowed_pattern: None,
        },
        migration_support: CapabilitySupport::Unsupported,
        quarantine_tables: CapabilitySupport::Unsupported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    }
}

#[test]
fn registry_resolves_and_inspects_without_order_authority() {
    static ALPHA: &[&str] = &["alpha"];
    static BETA: &[&str] = &["beta"];
    let mut forward = DestinationRegistry::new();
    forward
        .register(MockDriver {
            schemes: ALPHA,
            destination: "alpha_destination",
        })
        .unwrap();
    forward
        .register(MockDriver {
            schemes: BETA,
            destination: "beta_destination",
        })
        .unwrap();
    let mut reverse = DestinationRegistry::new();
    reverse
        .register(MockDriver {
            schemes: BETA,
            destination: "beta_destination",
        })
        .unwrap();
    reverse
        .register(MockDriver {
            schemes: ALPHA,
            destination: "alpha_destination",
        })
        .unwrap();

    assert_eq!(forward.registered_schemes(), reverse.registered_schemes());
    let context = DestinationResolutionContext::new();
    let inspected = forward.inspect("alpha://target", &context).unwrap();
    assert_eq!(
        inspected.description.destination_id.as_str(),
        "alpha_destination"
    );
    assert_eq!(
        inspected.runtime.ingress_mode,
        DestinationIngressMode::StagedDurableSegments
    );
    assert!(!inspected.health_probes[0].mutates_destination);
    let resolved = reverse.resolve("ALPHA://target", &context).unwrap();
    assert_eq!(
        resolved.describe().destination_id.as_str(),
        "alpha_destination"
    );
}

#[test]
fn registry_rejects_empty_malformed_and_duplicate_schemes() {
    static EMPTY: &[&str] = &[];
    static MALFORMED: &[&str] = &["bad_scheme"];
    static ALPHA: &[&str] = &["alpha"];
    let mut registry = DestinationRegistry::new();
    assert!(
        registry
            .register(MockDriver {
                schemes: EMPTY,
                destination: "empty",
            })
            .is_err()
    );
    assert!(
        registry
            .register(MockDriver {
                schemes: MALFORMED,
                destination: "malformed",
            })
            .is_err()
    );
    registry
        .register(MockDriver {
            schemes: ALPHA,
            destination: "alpha",
        })
        .unwrap();
    assert!(
        registry
            .register(MockDriver {
                schemes: ALPHA,
                destination: "duplicate",
            })
            .is_err()
    );
    assert!(
        registry
            .resolve("unknown://target", &DestinationResolutionContext::new())
            .is_err()
    );
}

#[test]
fn runtime_capabilities_are_serializable_plan_evidence() {
    let capabilities = DestinationRuntimeCapabilities {
        blocking_lanes: Vec::new(),
        ingress_mode: DestinationIngressMode::StagedDurableSegments,
        staged_ingress: Some(StagedIngressCapabilities {
            recovery: StagingRecoveryMode::RollbackRedrive,
            visibility: StagingVisibility::IsolatedUntilFinalBinding,
            abort_idempotent: true,
            lifecycle_cleanup: true,
            final_binding_requires_exclusive_writer: true,
        }),
        writer_model: DestinationWriterModel::ConcurrentSegments,
        max_in_flight_segments: Some(8),
        max_in_flight_bytes: Some(128 * 1024 * 1024),
        bulk_path: Some("arrow".to_owned()),
        bulk_evidence_version: Some("2026-07".to_owned()),
        replay_requires_explicit_target: true,
        replay_target_hint: Some("schema.table".to_owned()),
        replay_policy_values: [("merge_dedup".to_owned(), vec!["fail".to_owned()])]
            .into_iter()
            .collect(),
    };
    let json = serde_json::to_string(&capabilities).unwrap();
    assert_eq!(
        serde_json::from_str::<DestinationRuntimeCapabilities>(&json).unwrap(),
        capabilities
    );
}

struct MockSourceDriver {
    descriptor: SourceDriverDescriptor,
}

impl SourceDriver for MockSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            request.descriptor,
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
                retry_granularity: SourceRetryGranularity::Partition,
                retryable_errors: vec![ErrorKind::Transient],
                attestation: SourceAttestationStrength::ImmutableContent,
                rate_limit_per_second: Some(100),
                quota_authority: Some("mock-account".to_owned()),
                canonical_order: true,
                bounded: true,
                telemetry_version: "v1".to_owned(),
            },
            request.schema,
            request.type_policy_allowances,
            request.effective_schema_runtime,
            serde_json::json!({"token": "secret://env/MOCK_TOKEN"}),
            serde_json::json!({"partitions": 2}),
        )
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        Ok(Arc::new(MockSourceResource {
            descriptor: plan.descriptor.clone(),
            schema: Arc::new(plan.schema.clone()),
            capabilities: plan.resource_capabilities.clone(),
        }))
    }
}

struct MockSourceResource {
    descriptor: ResourceDescriptor,
    schema: Arc<Schema>,
    capabilities: ResourceCapabilities,
}

impl ResourceStream for MockSourceResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        Ok(vec![PartitionPlan {
            partition_id: PartitionId::new("mock-000001")?,
            scope: request.scope.clone(),
            start_position: None,
            metadata: BTreeMap::new(),
        }])
    }

    fn open(&self, _partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>> {
        Box::pin(async {
            let stream: BatchStream = Box::pin(futures_util::stream::empty());
            Ok(stream)
        })
    }
}

impl QueryableResource for MockSourceResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        Ok(ScanPlan {
            plan_id: PlanId::new("mock-source-plan")?,
            request: request.clone(),
            partitions: self.plan_partitions(request)?,
            pushed_predicates: Vec::new(),
            unsupported_predicates: request.filters.clone(),
            estimated_rows: Some(0),
            estimated_bytes: Some(0),
            delivery_guarantee: DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        })
    }
}

struct NoopSourceHost {
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
}

impl ExecutionHost for NoopSourceHost {
    fn capabilities(&self) -> ExecutionHostCapabilities {
        ExecutionHostCapabilities {
            logical_cpu_slots: 1,
            io_workers: 1,
            blocking_lanes: Vec::new(),
        }
    }

    fn memory(&self) -> Arc<dyn cdf_memory::MemoryCoordinator> {
        Arc::clone(&self.memory)
    }

    fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
        Err(CdfError::internal(
            "mock source host does not execute scopes",
        ))
    }

    fn run_io_blocking(&self, _task: IoValueTask) -> Result<IoValue> {
        Err(CdfError::internal("mock source host does not execute I/O"))
    }

    fn ensure_blocking_lanes(&self, _lanes: &[BlockingLaneSpec]) -> Result<()> {
        Ok(())
    }

    fn run_blocking_value(&self, _lane: &str, _task: BlockingValueTask) -> Result<IoValue> {
        Err(CdfError::internal(
            "mock source host does not execute blocking work",
        ))
    }
}

struct NoopSecretProvider;

impl cdf_http::SecretProvider for NoopSecretProvider {
    fn resolve(&self, _uri: &cdf_http::SecretUri) -> Result<cdf_http::SecretValue> {
        Err(CdfError::auth("mock secret resolution is not used"))
    }
}

#[test]
fn source_registry_compiles_hashes_and_resolves_mock_without_order_authority() {
    let descriptor = SourceDriverDescriptor {
        driver_id: SourceDriverId::new("mock_source").unwrap(),
        driver_version: "1.0.0".to_owned(),
        option_schema_hash: format!("sha256:{}", "a".repeat(64)),
        kinds: vec!["mock".to_owned()],
        schemes: vec!["mock".to_owned()],
    };
    let mut registry = SourceRegistry::new();
    registry
        .register(MockSourceDriver {
            descriptor: descriptor.clone(),
        })
        .unwrap();
    let resource_descriptor = ResourceDescriptor {
        resource_id: ResourceId::new("mock.events").unwrap(),
        schema_source: SchemaSource::Declared {
            schema_hash: SchemaHash::new("schema-mock").unwrap(),
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
        trust_level: TrustLevel::Experimental,
    };
    let plan = registry
        .compile(SourceCompileRequest {
            source_kind: "mock".to_owned(),
            source_options: BTreeMap::new(),
            resource_options: BTreeMap::new(),
            descriptor: resource_descriptor,
            schema: Schema::empty(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
        })
        .unwrap();
    assert_eq!(
        registry
            .driver_for_uri("mock://events")
            .unwrap()
            .descriptor(),
        &descriptor
    );
    let encoded = serde_json::to_vec(&plan).unwrap();
    assert_eq!(
        serde_json::from_slice::<CompiledSourcePlan>(&encoded).unwrap(),
        plan
    );
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> =
        Arc::new(cdf_memory::DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap());
    let services = ExecutionServices::new(Arc::new(NoopSourceHost { memory })).unwrap();
    let secrets: Arc<dyn cdf_http::SecretProvider + Send + Sync> = Arc::new(NoopSecretProvider);
    let root = tempfile::tempdir().unwrap();
    let context = SourceResolutionContext::new(root.path(), secrets, &services);
    let resource = registry.resolve(&plan, &context).unwrap();
    assert_eq!(resource.descriptor().resource_id.as_str(), "mock.events");

    let mut reordered = SourceRegistry::new();
    reordered.register(MockSourceDriver { descriptor }).unwrap();
    assert_eq!(reordered.descriptors(), registry.descriptors());
    assert!(
        reordered
            .register(MockSourceDriver {
                descriptor: reordered.descriptors()[0].clone()
            })
            .is_err()
    );
}

#[test]
fn staged_ingress_types_cannot_claim_package_commit_authority() {
    let attempt_id = LoadAttemptId::new("attempt_01").unwrap();
    let schema_hash = SchemaHash::new("schema-v1").unwrap();
    let request = StagedIngressRequest {
        attempt_id: attempt_id.clone(),
        binding: StagingAttemptBinding {
            destination_id: DestinationId::new("mock").unwrap(),
            target: TargetName::new("events").unwrap(),
            disposition: WriteDisposition::Append,
            schema_hash: schema_hash.clone(),
            plan_id: PlanId::new("plan-staging").unwrap(),
        },
        scheduling: StagingSchedulingContext::new(2, 1024).unwrap(),
    };
    let value = serde_json::to_value(&request).unwrap();
    assert_eq!(value["attempt_id"], "attempt_01");
    assert!(value.get("package_hash").is_none());
    assert!(value.get("idempotency_token").is_none());

    let identity = staged_identity("seg-a", 0, schema_hash);
    let ack = StagedSegmentAck {
        attempt_id,
        identity,
        external_durable: true,
    };
    let ack_value = serde_json::to_value(&ack).unwrap();
    assert!(ack_value.get("receipt_id").is_none());
    assert!(ack_value.get("package_hash").is_none());
}

#[test]
fn final_binding_requires_exact_ordered_staged_identities() {
    let schema_hash = SchemaHash::new("schema-v1").unwrap();
    let first = staged_identity("seg-a", 0, schema_hash.clone());
    let second = staged_identity("seg-b", 1, schema_hash.clone());
    let package_hash = PackageHash::new("sha256:package").unwrap();
    let target = TargetName::new("events").unwrap();
    let binding = VerifiedFinalBinding {
        attempt_id: LoadAttemptId::new("attempt_02").unwrap(),
        commit: DestinationCommitRequest {
            package_hash: package_hash.clone(),
            target: target.clone(),
            disposition: WriteDisposition::Append,
            segments: Vec::new(),
            idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
        },
        schema_hash,
        plan: CommitPlan {
            plan_id: PlanId::new("plan-staged").unwrap(),
            target,
            disposition: WriteDisposition::Append,
            idempotency: IdempotencySupport::PackageToken,
            migrations: Vec::new(),
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
        },
        ordered_segments: vec![first.clone(), second.clone()],
    };
    binding
        .validate_staged_identities(&[first.clone(), second.clone()])
        .unwrap();
    assert!(
        binding
            .validate_staged_identities(&[second.clone(), first.clone()])
            .is_err()
    );
    assert!(binding.validate_staged_identities(&[first]).is_err());
    assert!(
        binding
            .validate_staged_identities(&[second.clone(), second])
            .is_err()
    );
}

#[test]
fn staged_session_reattaches_rejects_mismatch_and_binds_receipt_only_at_finalization() {
    let schema_hash = SchemaHash::new("schema-v1").unwrap();
    let first = staged_identity("seg-a", 0, schema_hash.clone());
    let second = staged_identity("seg-b", 1, schema_hash.clone());
    let attempt = LoadAttemptId::new("attempt_session").unwrap();
    let mut runtime = MockStagedRuntime::new();
    let mut session = runtime
        .begin_staged_ingress(staged_request(attempt.clone(), schema_hash.clone()))
        .unwrap();
    for identity in [first.clone(), second.clone()] {
        let ack = session
            .stage_segment(
                StagedSegmentRequest::new(
                    identity.clone(),
                    Box::new(EmptySegmentReader { identity }),
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(ack.attempt_id, attempt);
    }
    assert!(runtime.committed.lock().unwrap().is_empty());
    assert_eq!(
        session.snapshot().unwrap().accepted_segments,
        vec![first.clone(), second.clone()]
    );
    assert_eq!(
        runtime
            .inspect_staged_ingress(&attempt)
            .unwrap()
            .unwrap()
            .accepted_segments,
        vec![first.clone(), second.clone()]
    );
    let mut wrong_authority = staged_request(attempt.clone(), schema_hash.clone());
    wrong_authority.binding.target = TargetName::new("other_events").unwrap();
    assert!(runtime.begin_staged_ingress(wrong_authority).is_err());

    let mismatch = test_final_binding(
        attempt.clone(),
        schema_hash.clone(),
        vec![second.clone(), first.clone()],
    );
    assert!(session.bind_final(mismatch).is_err());

    let reattached = runtime
        .begin_staged_ingress(staged_request(attempt.clone(), schema_hash.clone()))
        .unwrap();
    let binding = test_final_binding(
        attempt.clone(),
        schema_hash.clone(),
        vec![first.clone(), second.clone()],
    );
    assert!(
        !serde_json::to_string(binding.commit())
            .unwrap()
            .contains("attempt_session")
    );
    assert!(
        !serde_json::to_string(binding.plan())
            .unwrap()
            .contains("attempt_session")
    );
    let receipt = reattached.bind_final(binding).unwrap();
    assert_eq!(receipt.package_hash.as_str(), "sha256:package-final");
    assert_eq!(receipt.segment_acks.len(), 2);
    assert!(runtime.inspect_staged_ingress(&attempt).unwrap().is_none());
    assert_eq!(
        receipt
            .transaction
            .as_ref()
            .unwrap()
            .values
            .get("load_attempt_id")
            .unwrap(),
        "attempt_session"
    );
    let duplicate_attempt = LoadAttemptId::new("attempt_duplicate").unwrap();
    let mut duplicate_session = runtime
        .begin_staged_ingress(staged_request(
            duplicate_attempt.clone(),
            schema_hash.clone(),
        ))
        .unwrap();
    for identity in [first.clone(), second.clone()] {
        duplicate_session
            .stage_segment(
                StagedSegmentRequest::new(
                    identity.clone(),
                    Box::new(EmptySegmentReader { identity }),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let duplicate = duplicate_session
        .bind_final(test_final_binding(
            duplicate_attempt,
            schema_hash,
            vec![first, second],
        ))
        .unwrap();
    assert_eq!(duplicate.receipt_id, receipt.receipt_id);
}

#[test]
fn staged_abort_is_repeatable_and_finalized_only_runtime_fails_closed() {
    let schema_hash = SchemaHash::new("schema-v1").unwrap();
    let attempt = LoadAttemptId::new("attempt_abort").unwrap();
    let mut runtime = MockStagedRuntime::new();
    runtime
        .begin_staged_ingress(staged_request(attempt.clone(), schema_hash.clone()))
        .unwrap()
        .abort()
        .unwrap();
    runtime
        .begin_staged_ingress(staged_request(attempt.clone(), schema_hash.clone()))
        .unwrap()
        .abort()
        .unwrap();
    assert!(runtime.inspect_staged_ingress(&attempt).unwrap().is_none());

    let mut finalized = MockRuntime {
        protocol: MockProtocol {
            sheet: mock_sheet("finalized_only"),
        },
        description: DestinationDescription::new(
            DestinationId::new("finalized_only").unwrap(),
            &["finalized"],
            "finalized only",
        ),
    };
    assert!(
        finalized
            .begin_staged_ingress(staged_request(attempt, schema_hash))
            .is_err()
    );
}

#[test]
fn staged_capability_requires_cleanup_abort_and_byte_bounds() {
    let mut capabilities = DestinationRuntimeCapabilities {
        blocking_lanes: Vec::new(),
        ingress_mode: DestinationIngressMode::StagedDurableSegments,
        staged_ingress: Some(StagedIngressCapabilities {
            recovery: StagingRecoveryMode::Resumable,
            visibility: StagingVisibility::IsolatedUntilFinalBinding,
            abort_idempotent: true,
            lifecycle_cleanup: true,
            final_binding_requires_exclusive_writer: false,
        }),
        writer_model: DestinationWriterModel::ConcurrentSegments,
        max_in_flight_segments: Some(2),
        max_in_flight_bytes: Some(1024),
        bulk_path: None,
        bulk_evidence_version: None,
        replay_requires_explicit_target: false,
        replay_target_hint: None,
        replay_policy_values: Default::default(),
    };
    capabilities.validate().unwrap();
    capabilities.max_in_flight_bytes = None;
    assert!(capabilities.validate().is_err());
    capabilities.max_in_flight_bytes = Some(1024);
    capabilities
        .staged_ingress
        .as_mut()
        .unwrap()
        .abort_idempotent = false;
    assert!(capabilities.validate().is_err());
}

fn staged_identity(
    segment_id: &str,
    ordinal: u32,
    schema_hash: SchemaHash,
) -> StagedSegmentIdentity {
    StagedSegmentIdentity {
        segment_id: SegmentId::new(segment_id).unwrap(),
        sha256: format!("sha256:{segment_id}"),
        row_count: 1,
        byte_count: 8,
        schema_hash,
        ordinal,
    }
}

fn staged_request(attempt_id: LoadAttemptId, schema_hash: SchemaHash) -> StagedIngressRequest {
    StagedIngressRequest {
        attempt_id,
        binding: StagingAttemptBinding {
            destination_id: DestinationId::new("mock_staged").unwrap(),
            target: TargetName::new("events").unwrap(),
            disposition: WriteDisposition::Append,
            schema_hash,
            plan_id: PlanId::new("plan-staged").unwrap(),
        },
        scheduling: StagingSchedulingContext::new(2, 1024).unwrap(),
    }
}

fn test_final_binding(
    attempt_id: LoadAttemptId,
    schema_hash: SchemaHash,
    ordered_segments: Vec<StagedSegmentIdentity>,
) -> VerifiedFinalBinding {
    let package_hash = PackageHash::new("sha256:package-final").unwrap();
    let target = TargetName::new("events").unwrap();
    VerifiedFinalBinding {
        attempt_id,
        commit: DestinationCommitRequest {
            package_hash: package_hash.clone(),
            target: target.clone(),
            disposition: WriteDisposition::Append,
            segments: Vec::new(),
            idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
        },
        schema_hash,
        plan: CommitPlan {
            plan_id: PlanId::new("plan-staged").unwrap(),
            target,
            disposition: WriteDisposition::Append,
            idempotency: IdempotencySupport::PackageToken,
            migrations: Vec::new(),
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
        },
        ordered_segments,
    }
}

#[test]
fn manifest_has_no_upward_or_concrete_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    for forbidden in [
        "cdf-project",
        "cdf-engine",
        "cdf-dest-",
        "datafusion",
        "duckdb",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "cdf-runtime manifest contains forbidden dependency `{forbidden}`"
        );
    }
}

#[test]
fn execution_host_capabilities_validate_generic_cpu_and_blocking_lanes() {
    let capabilities = ExecutionHostCapabilities {
        logical_cpu_slots: 8,
        io_workers: 2,
        blocking_lanes: vec![BlockingLaneSpec {
            lane_id: "native-affine".to_owned(),
            maximum_concurrency: 1,
            cpu_slot_cost: 2,
            native_internal_parallelism: 2,
            affinity: LaneAffinity::Pinned,
            interruption: InterruptionSafety::CooperativeOnly,
        }],
    };
    capabilities.validate().unwrap();
    let encoded = serde_json::to_string(&capabilities).unwrap();
    assert!(!encoded.contains("duckdb"));
    assert!(!encoded.contains("python"));
    assert!(!encoded.contains("tokio"));

    let mut duplicate = capabilities.clone();
    duplicate
        .blocking_lanes
        .push(duplicate.blocking_lanes[0].clone());
    assert!(duplicate.validate().is_err());
    assert!(
        CpuTaskSpec {
            task_kind: "decode".to_owned(),
            cpu_slot_cost: 0,
            native_internal_parallelism: 1,
        }
        .validate()
        .is_err()
    );
}
