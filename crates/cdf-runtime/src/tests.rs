use crate::prelude::*;
use arrow_schema::{DataType, Field};
use cdf_kernel::{
    BatchStream, CommitCounts, CommitSegment, ConcurrencyLimit, DeliveryGuarantee, DestinationId,
    ErrorKind, IdempotencySupport, IdempotencyToken, IdentifierRules, MigrationRecord, PackageHash,
    PartitionId, PartitionPlan, PlanId, QueryableResource, ReceiptId, ResourceCapabilities,
    ResourceDescriptor, ResourceId, ResourceStream, ScanPlan, ScanRequest, SchemaHash,
    SchemaSource, ScopeKey, SegmentAck, SegmentId, TargetName, TransactionMetadata,
    TransactionSupport, TrustLevel, TypeMapping, VerifyClause,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
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

    fn ingress(&mut self) -> DestinationIngress<'_> {
        DestinationIngress::FinalizedPackage(self)
    }

    fn describe(&self) -> DestinationDescription {
        self.description.clone()
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        let descriptor = mock_bulk_descriptor(
            "mock_finalized",
            "mock-finalized-v1",
            DestinationIngressMode::FinalizedPackageOnly,
            DestinationWriterModel::SingleWriter,
        );
        DestinationRuntimeCapabilities {
            commit_payload_mode: DestinationCommitPayloadMode::SegmentStreaming,
            max_in_flight_segments: Some(1),
            max_in_flight_bytes: Some(64 * 1024 * 1024),
            bulk_paths: vec![descriptor],
            bulk_path: Some("mock_finalized".to_owned()),
            bulk_evidence_version: Some("mock-finalized-v1".to_owned()),
            ..Default::default()
        }
    }
}

impl FinalizedPackageIngress for MockRuntime {
    fn prepare_package_commit(
        &mut self,
        inputs: &PackageReplayInputs,
        context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        let plan = self.protocol.plan_commit(&inputs.destination_commit)?;
        Ok(PreparedDestinationCommit::from_verified_inputs(
            inputs,
            plan,
            context.bulk_path.clone(),
            DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
        )?
        .with_pending_context(inputs.schema_hash.clone()))
    }

    fn begin_prepared_commit(
        &mut self,
        prepared: &mut PreparedDestinationCommit,
    ) -> Result<Box<dyn CommitSession + '_>> {
        let schema_hash = prepared.take_pending_context::<SchemaHash>("mock schema")?;
        if prepared.plan().target != prepared.commit().target
            || prepared.plan().disposition != prepared.commit().disposition
        {
            return Err(CdfError::contract(
                "mock finalized commit plan does not match commit authority",
            ));
        }
        Ok(Box::new(MockFinalizedSession {
            destination_id: self.protocol.sheet.destination.clone(),
            request: prepared.commit().clone(),
            plan: prepared.plan().clone(),
            schema_hash,
            acknowledgements: Vec::new(),
        }))
    }
}

struct MockFinalizedSession {
    destination_id: DestinationId,
    request: DestinationCommitRequest,
    plan: CommitPlan,
    schema_hash: SchemaHash,
    acknowledgements: Vec<SegmentAck>,
}

impl CommitSession for MockFinalizedSession {
    fn apply_migrations(&mut self) -> Result<()> {
        Ok(())
    }

    fn write_segment(&mut self, segment: CommitSegment) -> Result<SegmentAck> {
        let expected = self
            .request
            .segments
            .get(self.acknowledgements.len())
            .ok_or_else(|| {
                CdfError::contract("mock finalized session received an extra segment")
            })?;
        if expected != &segment.state {
            return Err(CdfError::contract(
                "mock finalized session segment does not match commit authority",
            ));
        }
        let package_byte_count = segment.package_byte_count;
        let state = segment.state.clone();
        let rows = segment.into_batches()?.try_fold(0_u64, |rows, batch| {
            rows.checked_add(batch.batch.num_rows() as u64)
                .ok_or_else(|| CdfError::data("mock finalized row count overflowed"))
        })?;
        if rows != state.row_count {
            return Err(CdfError::data(
                "mock finalized segment rows do not match commit authority",
            ));
        }
        let ack = SegmentAck {
            segment_id: state.segment_id,
            row_count: state.row_count,
            byte_count: package_byte_count,
        };
        self.acknowledgements.push(ack.clone());
        Ok(ack)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        if self.acknowledgements.len() != self.request.segments.len() {
            return Err(CdfError::destination(
                "mock finalized session is missing committed segments",
            ));
        }
        Ok(Receipt {
            receipt_id: ReceiptId::new(format!(
                "receipt-{}",
                self.request.package_hash.as_str().replace(':', "-")
            ))?,
            destination: self.destination_id,
            target: self.request.target,
            package_hash: self.request.package_hash,
            segment_acks: self.acknowledgements.clone(),
            disposition: self.request.disposition,
            idempotency_token: self.request.idempotency_token,
            transaction: None,
            counts: CommitCounts {
                rows_written: self.acknowledgements.iter().map(|ack| ack.row_count).sum(),
                ..CommitCounts::default()
            },
            schema_hash: self.schema_hash,
            migrations: self.plan.migrations,
            committed_at_ms: 0,
            verify: VerifyClause {
                kind: "mock".to_owned(),
                statement: "verify mock receipt".to_owned(),
                parameters: Default::default(),
            },
        })
    }

    fn abort(self: Box<Self>) -> Result<()> {
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

    fn ingress(&mut self) -> DestinationIngress<'_> {
        DestinationIngress::StagedSegments(self)
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
            staged_ingress_lane: None,
            final_binding_lane: None,
            ingress_mode: DestinationIngressMode::StagedDurableSegments,
            staged_ingress: Some(StagedIngressCapabilities {
                recovery: StagingRecoveryMode::Resumable,
                visibility: StagingVisibility::IsolatedUntilFinalBinding,
                abort_idempotent: true,
                lifecycle_cleanup: true,
                final_binding_requires_exclusive_writer: false,
            }),
            writer_model: DestinationWriterModel::SingleWriter,
            commit_payload_mode: DestinationCommitPayloadMode::SegmentStreaming,
            max_in_flight_segments: Some(2),
            max_in_flight_bytes: Some(1024),
            bulk_paths: vec![test_prepared_bulk_path().descriptor],
            bulk_path: Some("mock_staged".to_owned()),
            bulk_evidence_version: Some("mock-staged-v1".to_owned()),
            replay_requires_explicit_target: false,
            replay_target_hint: None,
            replay_policy_values: Default::default(),
        }
    }
}

impl StagedSegmentIngress for MockStagedRuntime {
    fn begin_staged_ingress(
        &mut self,
        request: StagedIngressRequest,
    ) -> Result<Box<dyn StagedIngressSession>> {
        self.runtime_capabilities()
            .validate_prepared_bulk_path(request.bulk_path())?;
        let mut attempts = self.attempts.lock().unwrap();
        match attempts.get(request.attempt_id()) {
            Some(existing) if &existing.binding != request.binding() => {
                return Err(CdfError::destination(
                    "staging attempt id is already bound to different authority",
                ));
            }
            Some(_) => {}
            None => {
                attempts.insert(
                    request.attempt_id().clone(),
                    MockAttempt {
                        binding: request.binding().clone(),
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
}

struct MockStagedSession {
    request: StagedIngressRequest,
    attempts: Arc<Mutex<BTreeMap<LoadAttemptId, MockAttempt>>>,
    committed: Arc<Mutex<BTreeMap<PackageHash, Receipt>>>,
}

impl StagedIngressSession for MockStagedSession {
    fn stage_stream(&mut self, stream: &mut dyn StagedSegmentStream) -> Result<()> {
        while let Some(mut segment) = stream.next_segment()? {
            while segment.reader_mut().next_batch()?.is_some() {}
            let identity = segment.identity;
            let mut attempts = self.attempts.lock().unwrap();
            let accepted = attempts
                .get_mut(self.request.attempt_id())
                .ok_or_else(|| CdfError::destination("mock staging attempt is not attached"))?;
            if identity.ordinal != u32::try_from(accepted.accepted_segments.len()).unwrap() {
                return Err(CdfError::destination(
                    "mock staging received a noncanonical ordinal",
                ));
            }
            accepted.accepted_segments.push(identity.clone());
            drop(attempts);
            stream.acknowledge(StagedSegmentAck {
                attempt_id: self.request.attempt_id().clone(),
                identity,
                external_durable: true,
            })?;
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<StagingSnapshot> {
        let accepted_segments = self
            .attempts
            .lock()
            .unwrap()
            .get(self.request.attempt_id())
            .cloned()
            .ok_or_else(|| CdfError::destination("mock staging attempt is absent"))?
            .accepted_segments;
        Ok(StagingSnapshot {
            attempt_id: self.request.attempt_id().clone(),
            binding: self.request.binding().clone(),
            recovery: StagingRecoveryMode::Resumable,
            accepted_segments,
        })
    }

    fn bind_final(
        self: Box<Self>,
        binding: VerifiedFinalBinding,
    ) -> Result<DestinationCommitOutcome> {
        if binding.attempt_id() != self.request.attempt_id() {
            return Err(CdfError::destination(
                "final binding attempt does not match staged session",
            ));
        }
        if binding.commit().target != self.request.binding().target
            || binding.commit().disposition != self.request.binding().disposition
            || binding.schema_hash() != &self.request.binding().schema_hash
            || binding.output_arrow_schema_hash()
                != &self.request.binding().output_arrow_schema_hash
            || binding.merge_keys() != self.request.binding().merge_keys
            || binding.execution_plan_id() != &self.request.binding().execution_plan_id
        {
            return Err(CdfError::destination(
                "final binding does not match the staged attempt authority",
            ));
        }
        let accepted = self
            .attempts
            .lock()
            .unwrap()
            .get(self.request.attempt_id())
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
            return Ok(DestinationCommitOutcome::new(
                receipt,
                DestinationReceiptReportingPolicy::DestinationCommit { duplicate: true },
            ));
        }
        let receipt = Receipt {
            receipt_id: ReceiptId::new(format!(
                "receipt-{}",
                binding.commit().package_hash.as_str().replace(':', "-")
            ))?,
            destination: self.request.binding().destination_id.clone(),
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
                    self.request.attempt_id().to_string(),
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
            .remove(self.request.attempt_id());
        Ok(DestinationCommitOutcome::new(
            receipt,
            DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
        ))
    }

    fn abort(self: Box<Self>) -> Result<()> {
        self.attempts
            .lock()
            .unwrap()
            .remove(self.request.attempt_id());
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

struct TestStagedSegmentStream {
    requests: std::vec::IntoIter<StagedSegmentRequest>,
    acknowledgements: Vec<StagedSegmentAck>,
}

impl StagedSegmentStream for TestStagedSegmentStream {
    fn next_segment(&mut self) -> Result<Option<StagedSegmentRequest>> {
        Ok(self.requests.next())
    }

    fn acknowledge(&mut self, acknowledgement: StagedSegmentAck) -> Result<()> {
        self.acknowledgements.push(acknowledgement);
        Ok(())
    }
}

fn stage_test_identities(
    session: &mut dyn StagedIngressSession,
    identities: impl IntoIterator<Item = StagedSegmentIdentity>,
) -> Vec<StagedSegmentAck> {
    let requests = identities
        .into_iter()
        .map(|identity| {
            StagedSegmentRequest::new(identity.clone(), Box::new(EmptySegmentReader { identity }))
                .unwrap()
        })
        .collect::<Vec<_>>()
        .into_iter();
    let mut stream = TestStagedSegmentStream {
        requests,
        acknowledgements: Vec::new(),
    };
    session.stage_stream(&mut stream).unwrap();
    stream.acknowledgements
}

struct MockDriver {
    schemes: &'static [&'static str],
    destination: &'static str,
    product_location_field: Option<&'static str>,
    product_receipt_source: &'static str,
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
            description: self.description(sheet.destination.clone()),
            sheet_artifact_hash: artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: DestinationRuntimeCapabilities {
                blocking_lanes: Vec::new(),
                staged_ingress_lane: None,
                final_binding_lane: None,
                ingress_mode: DestinationIngressMode::StagedDurableSegments,
                staged_ingress: Some(StagedIngressCapabilities {
                    recovery: StagingRecoveryMode::Resumable,
                    visibility: StagingVisibility::IsolatedUntilFinalBinding,
                    abort_idempotent: true,
                    lifecycle_cleanup: true,
                    final_binding_requires_exclusive_writer: false,
                }),
                writer_model: DestinationWriterModel::ConcurrentSegments,
                commit_payload_mode: DestinationCommitPayloadMode::SegmentStreaming,
                max_in_flight_segments: Some(4),
                max_in_flight_bytes: Some(64 * 1024 * 1024),
                bulk_paths: vec![mock_bulk_descriptor(
                    "mock_arrow",
                    "v1",
                    DestinationIngressMode::StagedDurableSegments,
                    DestinationWriterModel::ConcurrentSegments,
                )],
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
            description: self.description(sheet.destination.clone()),
            protocol: MockProtocol { sheet },
        }))
    }
}

impl MockDriver {
    fn description(&self, destination_id: DestinationId) -> DestinationDescription {
        let mut description =
            DestinationDescription::new(destination_id, self.schemes, self.destination)
                .with_product_receipt_source(self.product_receipt_source);
        if let Some(field) = self.product_location_field {
            description = description.with_product_location_field(field);
        }
        description
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
            product_location_field: None,
            product_receipt_source: "destination_commit",
        })
        .unwrap();
    forward
        .register(MockDriver {
            schemes: BETA,
            destination: "beta_destination",
            product_location_field: None,
            product_receipt_source: "destination_commit",
        })
        .unwrap();
    let mut reverse = DestinationRegistry::new();
    reverse
        .register(MockDriver {
            schemes: BETA,
            destination: "beta_destination",
            product_location_field: None,
            product_receipt_source: "destination_commit",
        })
        .unwrap();
    reverse
        .register(MockDriver {
            schemes: ALPHA,
            destination: "alpha_destination",
            product_location_field: None,
            product_receipt_source: "destination_commit",
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
                product_location_field: None,
                product_receipt_source: "destination_commit",
            })
            .is_err()
    );
    assert!(
        registry
            .register(MockDriver {
                schemes: MALFORMED,
                destination: "malformed",
                product_location_field: None,
                product_receipt_source: "destination_commit",
            })
            .is_err()
    );
    registry
        .register(MockDriver {
            schemes: ALPHA,
            destination: "alpha",
            product_location_field: None,
            product_receipt_source: "destination_commit",
        })
        .unwrap();
    assert!(
        registry
            .register(MockDriver {
                schemes: ALPHA,
                destination: "duplicate",
                product_location_field: None,
                product_receipt_source: "destination_commit",
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
fn registry_rejects_product_metadata_that_cannot_compose_with_stable_reports() {
    static RESERVED: &[&str] = &["reserved"];
    let mut registry = DestinationRegistry::new();
    registry
        .register(MockDriver {
            schemes: RESERVED,
            destination: "reserved_destination",
            product_location_field: Some("target"),
            product_receipt_source: "destination_commit",
        })
        .unwrap();
    let context = DestinationResolutionContext::new();

    let inspect_error = registry.inspect("reserved://target", &context).unwrap_err();
    assert!(inspect_error.message.contains("reserved report field"));
    let resolve_error = registry
        .resolve("reserved://target", &context)
        .err()
        .expect("reserved metadata must fail resolution");
    assert!(resolve_error.message.contains("reserved report field"));

    let invalid_source = DestinationDescription::new(
        DestinationId::new("invalid_source").unwrap(),
        &["invalid-source"],
        "invalid source",
    )
    .with_product_receipt_source("Destination Commit");
    assert!(
        invalid_source
            .validate()
            .unwrap_err()
            .message
            .contains("non-empty snake_case identifier")
    );
}

#[test]
fn runtime_capabilities_are_serializable_plan_evidence() {
    let capabilities = DestinationRuntimeCapabilities {
        blocking_lanes: Vec::new(),
        staged_ingress_lane: None,
        final_binding_lane: None,
        ingress_mode: DestinationIngressMode::StagedDurableSegments,
        staged_ingress: Some(StagedIngressCapabilities {
            recovery: StagingRecoveryMode::RollbackRedrive,
            visibility: StagingVisibility::IsolatedUntilFinalBinding,
            abort_idempotent: true,
            lifecycle_cleanup: true,
            final_binding_requires_exclusive_writer: true,
        }),
        writer_model: DestinationWriterModel::ConcurrentSegments,
        commit_payload_mode: DestinationCommitPayloadMode::SegmentStreaming,
        max_in_flight_segments: Some(8),
        max_in_flight_bytes: Some(128 * 1024 * 1024),
        bulk_paths: vec![mock_bulk_descriptor(
            "arrow",
            "2026-07",
            DestinationIngressMode::StagedDurableSegments,
            DestinationWriterModel::ConcurrentSegments,
        )],
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

#[test]
fn bulk_descriptors_compose_two_paths_without_runtime_dispatch_changes() {
    let descriptor = |path_id: &str, fallback| BulkPathDescriptor {
        path_id: path_id.to_owned(),
        version: 1,
        ingress_mode: DestinationIngressMode::StagedDurableSegments,
        writer_model: DestinationWriterModel::ConcurrentSegments,
        ordering: BulkOrdering::SegmentIndependent,
        rows: BulkSizeRange {
            minimum: 8_192,
            preferred: 65_536,
            maximum: 1_048_576,
        },
        bytes: BulkSizeRange {
            minimum: 1_048_576,
            preferred: 16_777_216,
            maximum: 67_108_864,
        },
        max_useful_writers: 4,
        blocking_lane: None,
        native_internal_parallelism: 1,
        external_staging: true,
        fallback,
        schema_preflight_version: "mock-schema@1".to_owned(),
        measured_evidence_version: Some("mock-v1".to_owned()),
    };
    let capabilities = DestinationRuntimeCapabilities {
        ingress_mode: DestinationIngressMode::StagedDurableSegments,
        staged_ingress: Some(StagedIngressCapabilities {
            recovery: StagingRecoveryMode::RollbackRedrive,
            visibility: StagingVisibility::IsolatedUntilFinalBinding,
            abort_idempotent: true,
            lifecycle_cleanup: true,
            final_binding_requires_exclusive_writer: false,
        }),
        commit_payload_mode: DestinationCommitPayloadMode::SegmentStreaming,
        writer_model: DestinationWriterModel::ConcurrentSegments,
        max_in_flight_segments: Some(4),
        max_in_flight_bytes: Some(64 * 1024 * 1024),
        bulk_paths: vec![
            descriptor("mock_arrow", BulkFallbackMode::PreflightOnly),
            descriptor("mock_scalar", BulkFallbackMode::PreflightOnly),
        ],
        bulk_path: Some("mock_arrow".to_owned()),
        bulk_evidence_version: Some("mock-v1".to_owned()),
        ..Default::default()
    };
    capabilities.validate().unwrap();
    let json = serde_json::to_value(&capabilities).unwrap();
    assert_eq!(json["bulk_paths"][0]["path_id"], "mock_arrow");
    assert_eq!(json["bulk_paths"][1]["fallback"], "preflight_only");
    assert_eq!(json["bulk_paths"][0]["max_useful_writers"], 4);
}

#[test]
fn bulk_selection_rejects_descriptor_and_evidence_drift() {
    let descriptor = mock_bulk_descriptor(
        "native",
        "native-v1",
        DestinationIngressMode::FinalizedPackageOnly,
        DestinationWriterModel::SingleWriter,
    );
    let capabilities = DestinationRuntimeCapabilities {
        bulk_paths: vec![descriptor.clone()],
        bulk_path: Some("native".to_owned()),
        bulk_evidence_version: Some("native-v1".to_owned()),
        ..Default::default()
    };
    capabilities.validate().unwrap();

    let mut mismatched_alternative = descriptor.clone();
    mismatched_alternative.path_id = "compat".to_owned();
    mismatched_alternative.ingress_mode = DestinationIngressMode::StagedDurableSegments;
    let mut incoherent_ladder = capabilities.clone();
    incoherent_ladder.bulk_paths.push(mismatched_alternative);
    assert!(
        incoherent_ladder
            .validate()
            .unwrap_err()
            .message
            .contains("ingress/writer model differs")
    );

    let mut stale_evidence = capabilities.clone();
    stale_evidence.bulk_evidence_version = Some("stale".to_owned());
    assert!(
        stale_evidence
            .validate()
            .unwrap_err()
            .message
            .contains("evidence version differs")
    );

    let mut undeclared = capabilities.clone();
    undeclared.bulk_path = Some("other".to_owned());
    assert!(
        undeclared
            .validate()
            .unwrap_err()
            .message
            .contains("is not declared")
    );

    let prepared = PreparedBulkPath {
        descriptor: BulkPathDescriptor {
            version: 2,
            ..descriptor
        },
        rows_per_batch: 64 * 1024,
        bytes_per_batch: 16 * 1024 * 1024,
        writers: 1,
    };
    assert!(
        capabilities
            .validate_prepared_bulk_path(&prepared)
            .unwrap_err()
            .message
            .contains("differs from its inspected descriptor")
    );
}

struct MockSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl SourceDriver for MockSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        CompiledSourcePlan::new(
            self.descriptor.clone(),
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
                rate_limit_per_second: Some(100),
                quota_authority: Some("mock-account".to_owned()),
                canonical_order: true,
                bounded: true,
                batch_memory: SourceBatchMemoryContract::Preaccounted,
                telemetry_version: "v1".to_owned(),
            },
            CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                redacted_options: serde_json::json!({"token": "secret://env/MOCK_TOKEN"}),
                physical_plan: serde_json::json!({"partitions": 2}),
            },
        )
    }

    fn discovery_session(
        &self,
        _plan: &CompiledSourcePlan,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        Ok(Box::new(MockSourceDiscoverySession))
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

struct MockSourceDiscoverySession;

impl SourceDiscoverySession for MockSourceDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::BoundedContent
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            "mock://events",
            Some(1),
            None,
            BTreeMap::new(),
        )?])
    }

    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<SourceSchemaObservation> {
        request.validate()?;
        SourceSchemaObservation::new(
            candidate,
            Schema::empty(),
            BTreeMap::from([("source_kind".to_owned(), "mock".to_owned())]),
            1,
            0,
        )
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
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::new(),
        }])
    }

    fn open(&self, _partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
            let stream: BatchStream = Box::pin(futures_util::stream::empty());
            Ok(cdf_kernel::PartitionStreamPayload::batches(stream))
        }))
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
    spill: Arc<dyn SpillBudgetCoordinator>,
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

    fn spill(&self) -> Arc<dyn SpillBudgetCoordinator> {
        Arc::clone(&self.spill)
    }

    fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
        Err(CdfError::internal(
            "mock source host does not execute scopes",
        ))
    }

    fn run_io_blocking(&self, _task: IoValueTask) -> Result<IoValue> {
        Err(CdfError::internal("mock source host does not execute I/O"))
    }

    fn delay(
        &self,
        _duration: std::time::Duration,
        cancellation: RunCancellation,
    ) -> cdf_kernel::BoxFuture<'static, Result<()>> {
        Box::pin(async move { cancellation.check() })
    }

    fn monotonic_now(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    fn entropy_u64(&self) -> u64 {
        0
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
    let option_schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "source": {"type": "object", "additionalProperties": false, "properties": {}},
        "resource": {"type": "object", "additionalProperties": false, "properties": {}}
    });
    let descriptor = SourceDriverDescriptor {
        driver_id: SourceDriverId::new("mock_source").unwrap(),
        driver_version: "1.0.0".to_owned(),
        option_schema_hash: artifact_hash(&option_schema).unwrap(),
        kinds: vec!["mock".to_owned()],
        schemes: vec!["mock".to_owned()],
    };
    let mut registry = SourceRegistry::new();
    registry
        .register(MockSourceDriver {
            descriptor: descriptor.clone(),
            option_schema: option_schema.clone(),
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
    let request = SourceCompileRequest {
        source_kind: "mock".to_owned(),
        context: SourceCompileContext {
            source_name: "mock".to_owned(),
            project_root: None,
            cursor_pushdown: None,
        },
        source_options: BTreeMap::new(),
        resource_options: BTreeMap::new(),
        descriptor: resource_descriptor,
        schema: Schema::empty(),
        type_policy_allowances: Default::default(),
        effective_schema_runtime: None,
    };
    let mut invalid_request = request.clone();
    invalid_request.context.source_name.clear();
    let error = registry.compile(invalid_request).unwrap_err();
    assert!(error.message.contains("requires a source name"));

    let plan = registry.compile(request).unwrap();
    let original_plan_hash = plan.physical_plan_hash.clone();
    let original_stable_hash = plan.schema_binding_stable_hash().unwrap();
    let mut discovered_descriptor = plan.descriptor.clone();
    discovered_descriptor.schema_source = SchemaSource::Discover;
    let discovered_schema = Schema::new(vec![Field::new("event_id", DataType::Int64, false)]);
    let bound = plan
        .clone()
        .bind_schema_authority(&discovered_descriptor, &discovered_schema, None)
        .unwrap();
    assert_eq!(bound.physical_plan_hash, original_plan_hash);
    assert_eq!(
        bound.schema_binding_stable_hash().unwrap(),
        original_stable_hash
    );
    assert_eq!(bound.physical_plan, plan.physical_plan);
    assert_eq!(bound.schema, discovered_schema);

    let mut invalid_binding = discovered_descriptor;
    invalid_binding.primary_key.push("event_id".to_owned());
    assert!(
        plan.clone()
            .bind_schema_authority(&invalid_binding, &bound.schema, None)
            .unwrap_err()
            .message
            .contains("changed non-schema resource authority")
    );
    let mut changed_options = plan.clone();
    changed_options.redacted_options = serde_json::json!({"token": "secret://env/OTHER_TOKEN"});
    changed_options.redacted_options_hash =
        artifact_hash(&changed_options.redacted_options).unwrap();
    assert_ne!(
        changed_options.schema_binding_stable_hash().unwrap(),
        original_stable_hash
    );
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
    let services = ExecutionServices::new(Arc::new(NoopSourceHost {
        memory,
        spill: Arc::new(FixedSpillBudget::new(1024).unwrap()),
    }))
    .unwrap();
    let secrets: Arc<dyn cdf_http::SecretProvider + Send + Sync> = Arc::new(NoopSecretProvider);
    let root = tempfile::tempdir().unwrap();
    let context = SourceResolutionContext::new(root.path(), secrets, &services);
    let discovery = registry.discovery_session(&plan, &context).unwrap();
    assert_eq!(discovery.kind(), SourceDiscoveryKind::BoundedContent);
    let candidates = discovery.candidates().unwrap();
    assert_eq!(candidates.len(), 1);
    let observation = discovery
        .observe(
            &candidates[0],
            &SourceDiscoveryRequest::new(1024, 10).unwrap(),
        )
        .unwrap();
    observation.validate().unwrap();
    assert_eq!(observation.evidence_location.as_str(), "mock://events");
    let resource = registry.resolve(&plan, &context).unwrap();
    assert_eq!(resource.descriptor().resource_id.as_str(), "mock.events");

    assert_eq!(
        registry.option_schemas(),
        BTreeMap::from([("mock_source".to_owned(), option_schema.clone())])
    );

    let mut invalid = SourceRegistry::new();
    let mut mismatched = descriptor.clone();
    mismatched.option_schema_hash = format!("sha256:{}", "f".repeat(64));
    let error = invalid
        .register(MockSourceDriver {
            descriptor: mismatched,
            option_schema: option_schema.clone(),
        })
        .unwrap_err();
    assert!(error.message.contains("does not match its declared hash"));

    let invalid_schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "source": {"type": "object", "additionalProperties": true},
        "resource": {"type": "object", "additionalProperties": false, "properties": {}}
    });
    let mut invalid = SourceRegistry::new();
    let mut invalid_descriptor = descriptor.clone();
    invalid_descriptor.option_schema_hash = artifact_hash(&invalid_schema).unwrap();
    let error = invalid
        .register(MockSourceDriver {
            descriptor: invalid_descriptor,
            option_schema: invalid_schema,
        })
        .unwrap_err();
    assert!(error.message.contains("must be a closed object"));

    let mut reordered = SourceRegistry::new();
    reordered
        .register(MockSourceDriver {
            descriptor,
            option_schema: option_schema.clone(),
        })
        .unwrap();
    assert_eq!(reordered.descriptors(), registry.descriptors());
    assert!(
        reordered
            .register(MockSourceDriver {
                descriptor: reordered.descriptors()[0].clone(),
                option_schema,
            })
            .is_err()
    );
}

#[test]
fn staged_ingress_types_cannot_claim_package_commit_authority() {
    let attempt_id = LoadAttemptId::new("attempt_01").unwrap();
    let schema_hash = SchemaHash::new("schema-v1").unwrap();
    let (lease, guard) = supervised_test_staging_lease(&attempt_id, "mock", "events");
    let request = StagedIngressRequest::new(
        attempt_id.clone(),
        StagingAttemptBinding {
            destination_id: DestinationId::new("mock").unwrap(),
            target: TargetName::new("events").unwrap(),
            disposition: WriteDisposition::Append,
            schema_hash: schema_hash.clone(),
            output_arrow_schema_hash: cdf_kernel::canonical_arrow_schema_hash(
                &arrow_schema::Schema::empty(),
            )
            .unwrap(),
            merge_keys: Vec::new(),
            execution_plan_id: PlanId::new("plan-staging").unwrap(),
        },
        lease,
        guard,
        test_prepared_bulk_path(),
        StagingSchedulingContext::new(2, 1024).unwrap(),
        arrow_schema::Schema::empty(),
    )
    .unwrap();
    assert_eq!(request.attempt_id(), &attempt_id);
    assert_eq!(request.binding().schema_hash, schema_hash);

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
fn staged_ingress_request_rejects_schema_payload_outside_binding_authority() {
    let attempt_id = LoadAttemptId::new("attempt_schema_mismatch").unwrap();
    let (lease, guard) = supervised_test_staging_lease(&attempt_id, "mock_staged", "events");
    let error = StagedIngressRequest::new(
        attempt_id.clone(),
        StagingAttemptBinding {
            destination_id: DestinationId::new("mock_staged").unwrap(),
            target: TargetName::new("events").unwrap(),
            disposition: WriteDisposition::Append,
            schema_hash: SchemaHash::new("schema-v1").unwrap(),
            output_arrow_schema_hash: SchemaHash::new("wrong-output-schema").unwrap(),
            merge_keys: Vec::new(),
            execution_plan_id: PlanId::new("plan-staged").unwrap(),
        },
        lease,
        guard,
        test_prepared_bulk_path(),
        StagingSchedulingContext::new(2, 1024).unwrap(),
        arrow_schema::Schema::empty(),
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not match binding wrong-output-schema")
    );
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
        execution_plan_id: PlanId::new("plan-staged").unwrap(),
        commit: DestinationCommitRequest {
            package_hash: package_hash.clone(),
            target: target.clone(),
            disposition: WriteDisposition::Append,
            segments: Vec::new(),
            idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
        },
        schema_hash,
        output_arrow_schema_hash: cdf_kernel::canonical_arrow_schema_hash(
            &arrow_schema::Schema::empty(),
        )
        .unwrap(),
        merge_keys: Vec::new(),
        plan: CommitPlan {
            plan_id: PlanId::new("destination-plan-staged").unwrap(),
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
    for ack in stage_test_identities(session.as_mut(), [first.clone(), second.clone()]) {
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
    let wrong_authority =
        staged_request_for_target(attempt.clone(), schema_hash.clone(), "other_events");
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
    let outcome = reattached.bind_final(binding).unwrap();
    assert_eq!(
        outcome.reporting_policy,
        DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false }
    );
    let receipt = outcome.receipt;
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
    stage_test_identities(duplicate_session.as_mut(), [first.clone(), second.clone()]);
    let duplicate = duplicate_session
        .bind_final(test_final_binding(
            duplicate_attempt,
            schema_hash,
            vec![first, second],
        ))
        .unwrap();
    assert_eq!(
        duplicate.reporting_policy,
        DestinationReceiptReportingPolicy::DestinationCommit { duplicate: true }
    );
    assert_eq!(duplicate.receipt.receipt_id, receipt.receipt_id);
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
    assert!(matches!(
        finalized.ingress(),
        DestinationIngress::FinalizedPackage(_)
    ));
}

#[test]
fn staged_capability_requires_cleanup_abort_and_byte_bounds() {
    let mut capabilities = DestinationRuntimeCapabilities {
        blocking_lanes: Vec::new(),
        staged_ingress_lane: None,
        final_binding_lane: None,
        ingress_mode: DestinationIngressMode::StagedDurableSegments,
        staged_ingress: Some(StagedIngressCapabilities {
            recovery: StagingRecoveryMode::Resumable,
            visibility: StagingVisibility::IsolatedUntilFinalBinding,
            abort_idempotent: true,
            lifecycle_cleanup: true,
            final_binding_requires_exclusive_writer: false,
        }),
        writer_model: DestinationWriterModel::ConcurrentSegments,
        commit_payload_mode: DestinationCommitPayloadMode::SegmentStreaming,
        max_in_flight_segments: Some(2),
        max_in_flight_bytes: Some(1024),
        bulk_paths: Vec::new(),
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
    staged_request_for_target(attempt_id, schema_hash, "events")
}

fn staged_request_for_target(
    attempt_id: LoadAttemptId,
    schema_hash: SchemaHash,
    target: &str,
) -> StagedIngressRequest {
    let (lease, guard) = supervised_test_staging_lease(&attempt_id, "mock_staged", target);
    StagedIngressRequest::new(
        attempt_id,
        StagingAttemptBinding {
            destination_id: DestinationId::new("mock_staged").unwrap(),
            target: TargetName::new(target).unwrap(),
            disposition: WriteDisposition::Append,
            schema_hash,
            output_arrow_schema_hash: cdf_kernel::canonical_arrow_schema_hash(
                &arrow_schema::Schema::empty(),
            )
            .unwrap(),
            merge_keys: Vec::new(),
            execution_plan_id: PlanId::new("plan-staged").unwrap(),
        },
        lease,
        guard,
        test_prepared_bulk_path(),
        StagingSchedulingContext::new(2, 1024).unwrap(),
        arrow_schema::Schema::empty(),
    )
    .unwrap()
}

fn test_staging_lease(attempt_id: &LoadAttemptId, destination: &str, target: &str) -> StagingLease {
    let identity = StagingLeaseIdentity::new(
        DestinationId::new(destination).unwrap(),
        TargetName::new(target).unwrap(),
        attempt_id.clone(),
    );
    StagingLease {
        authority_domain_id: cdf_kernel::LeaseAuthorityDomainId::new("runtime-test-domain")
            .unwrap(),
        scope_lease: cdf_kernel::ScopeLease {
            scope: ScopeKey::Composite {
                parts: vec![
                    ScopeKey::DestinationLoad {
                        destination: identity.destination_id.clone(),
                        target: identity.target.clone(),
                    },
                    ScopeKey::Stream {
                        name: format!("staging:{}", identity.attempt_id),
                    },
                ],
            },
            owner: cdf_kernel::LeaseOwnerId::new("test-owner").unwrap(),
            fencing_token: cdf_kernel::FencingToken::new(1).unwrap(),
            acquired_at_ms: 1,
            expires_at_ms: i64::MAX,
        },
        identity,
    }
}

struct LeaseTestHost;

struct LeaseTestScope {
    cancellation: RunCancellation,
    tasks: Vec<std::thread::JoinHandle<Result<()>>>,
}

impl ExecutionTaskScope for LeaseTestScope {
    fn cancellation(&self) -> RunCancellation {
        self.cancellation.clone()
    }

    fn spawn_io(&mut self, task: IoTask) -> Result<()> {
        self.tasks
            .push(std::thread::spawn(move || futures_executor::block_on(task)));
        Ok(())
    }

    fn spawn_cpu(&mut self, _spec: CpuTaskSpec, _task: BlockingTask) -> Result<()> {
        Err(CdfError::internal("lease test host does not run CPU work"))
    }

    fn spawn_cpu_future(&mut self, _spec: CpuTaskSpec, _task: CpuFutureTask) -> Result<()> {
        Err(CdfError::internal(
            "lease test host does not run CPU futures",
        ))
    }

    fn spawn_blocking(&mut self, _lane: &str, _task: BlockingTask) -> Result<()> {
        Err(CdfError::internal(
            "lease test host does not run blocking lanes",
        ))
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn join(self: Box<Self>) -> cdf_kernel::BoxFuture<'static, Result<TaskScopeReport>> {
        Box::pin(async move {
            let mut report = TaskScopeReport {
                submitted_io: u64::try_from(self.tasks.len()).unwrap(),
                ..TaskScopeReport::default()
            };
            for task in self.tasks {
                match task.join() {
                    Ok(Ok(())) => report.completed += 1,
                    Ok(Err(error)) => return Err(error),
                    Err(_) => return Err(CdfError::internal("lease test I/O task panicked")),
                }
            }
            Ok(report)
        })
    }
}

impl ExecutionHost for LeaseTestHost {
    fn capabilities(&self) -> ExecutionHostCapabilities {
        ExecutionHostCapabilities {
            logical_cpu_slots: 1,
            io_workers: 1,
            blocking_lanes: Vec::new(),
        }
    }

    fn memory(&self) -> Arc<dyn cdf_memory::MemoryCoordinator> {
        panic!("lease test host does not use memory")
    }

    fn spill(&self) -> Arc<dyn SpillBudgetCoordinator> {
        panic!("lease test host does not use spill")
    }

    fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
        Ok(Box::new(LeaseTestScope {
            cancellation: RunCancellation::default(),
            tasks: Vec::new(),
        }))
    }

    fn run_io_blocking(&self, task: IoValueTask) -> Result<IoValue> {
        futures_executor::block_on(task)
    }

    fn delay(
        &self,
        duration: std::time::Duration,
        cancellation: RunCancellation,
    ) -> cdf_kernel::BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let deadline = std::time::Instant::now() + duration;
            while std::time::Instant::now() < deadline {
                cancellation.check()?;
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            cancellation.check()
        })
    }

    fn monotonic_now(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    fn entropy_u64(&self) -> u64 {
        1
    }

    fn ensure_blocking_lanes(&self, _lanes: &[BlockingLaneSpec]) -> Result<()> {
        Ok(())
    }

    fn run_blocking_value(&self, _lane: &str, _task: BlockingValueTask) -> Result<IoValue> {
        Err(CdfError::internal(
            "lease test host does not run blocking work",
        ))
    }
}

fn supervised_test_staging_lease(
    attempt_id: &LoadAttemptId,
    destination: &str,
    target: &str,
) -> (StagingLease, StagingMutationGuard) {
    let supervisor = StagingLeaseSupervisor::new(
        Arc::new(RecordingStagingLeaseAuthority {
            renewals: AtomicUsize::new(0),
            released: AtomicBool::new(false),
            fail_renewal: AtomicBool::new(false),
        }),
        Arc::new(LeaseTestHost),
    )
    .unwrap();
    let managed = supervisor
        .acquire(
            StagingLeaseIdentity::new(
                DestinationId::new(destination).unwrap(),
                TargetName::new(target).unwrap(),
                attempt_id.clone(),
            ),
            cdf_kernel::LeaseOwnerId::new("runtime-request-test-owner").unwrap(),
        )
        .unwrap();
    let lease = managed.snapshot().unwrap();
    let guard = managed.mutation_guard().unwrap();
    drop(managed);
    (lease, guard)
}

struct RecordingStagingLeaseAuthority {
    renewals: AtomicUsize,
    released: AtomicBool,
    fail_renewal: AtomicBool,
}

impl StagingLeaseAuthority for RecordingStagingLeaseAuthority {
    fn authority_domain_id(&self) -> cdf_kernel::LeaseAuthorityDomainId {
        cdf_kernel::LeaseAuthorityDomainId::new("runtime-test-domain").unwrap()
    }

    fn acquire(
        &self,
        identity: StagingLeaseIdentity,
        owner: cdf_kernel::LeaseOwnerId,
        lease_duration_ms: u64,
    ) -> Result<StagingLease> {
        let mut lease = test_staging_lease(
            &identity.attempt_id,
            identity.destination_id.as_str(),
            identity.target.as_str(),
        );
        lease.scope_lease.owner = owner;
        lease.scope_lease.expires_at_ms = i64::try_from(lease_duration_ms).unwrap();
        Ok(lease)
    }

    fn renew(&self, lease: &StagingLease, lease_duration_ms: u64) -> Result<StagingLease> {
        self.renewals.fetch_add(1, Ordering::SeqCst);
        if self.fail_renewal.load(Ordering::SeqCst) {
            return Err(CdfError::transient("injected staging renewal failure"));
        }
        let mut renewed = lease.clone();
        renewed.scope_lease.expires_at_ms = renewed
            .scope_lease
            .expires_at_ms
            .saturating_add(i64::try_from(lease_duration_ms).unwrap());
        Ok(renewed)
    }

    fn release(&self, _lease: &StagingLease) -> Result<()> {
        self.released.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn assert_current(&self, _lease: &StagingLease) -> Result<()> {
        Ok(())
    }

    fn prove_expired(
        &self,
        _lease: &StagingLease,
        _collector: cdf_kernel::LeaseOwnerId,
        _cleanup_lease_duration_ms: u64,
    ) -> Result<Option<ExpiredStagingLeaseProof>> {
        Ok(None)
    }
}

#[test]
fn staging_lease_supervisor_renews_independently_and_releases_structurally() {
    let authority = Arc::new(RecordingStagingLeaseAuthority {
        renewals: AtomicUsize::new(0),
        released: AtomicBool::new(false),
        fail_renewal: AtomicBool::new(false),
    });
    let supervisor = StagingLeaseSupervisor::with_timing(
        authority.clone(),
        Arc::new(LeaseTestHost),
        StagingLeaseTiming {
            lease_duration: std::time::Duration::from_millis(100),
            renew_interval: std::time::Duration::from_millis(10),
        },
    )
    .unwrap();
    let lease = supervisor
        .acquire(
            StagingLeaseIdentity::new(
                DestinationId::new("mock_staged").unwrap(),
                TargetName::new("events").unwrap(),
                LoadAttemptId::new("supervised-attempt").unwrap(),
            ),
            cdf_kernel::LeaseOwnerId::new("runtime-owner").unwrap(),
        )
        .unwrap();
    let initial_expiry = lease.snapshot().unwrap().scope_lease.expires_at_ms;
    let churn_deadline = std::time::Instant::now() + std::time::Duration::from_millis(45);
    let mut churn = 0_u64;
    while std::time::Instant::now() < churn_deadline {
        let transient = supervisor
            .acquire(
                StagingLeaseIdentity::new(
                    DestinationId::new("mock_staged").unwrap(),
                    TargetName::new("events").unwrap(),
                    LoadAttemptId::new(format!("transient-{churn}")).unwrap(),
                ),
                cdf_kernel::LeaseOwnerId::new(format!("transient-owner-{churn}")).unwrap(),
            )
            .unwrap();
        transient.finish().unwrap();
        churn += 1;
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert!(authority.renewals.load(Ordering::SeqCst) >= 2);
    assert!(lease.snapshot().unwrap().scope_lease.expires_at_ms > initial_expiry);
    lease.finish().unwrap();
    assert!(authority.released.load(Ordering::SeqCst));
}

#[test]
fn staging_lease_renewal_failure_cancels_mutation_guard_before_more_work() {
    let authority = Arc::new(RecordingStagingLeaseAuthority {
        renewals: AtomicUsize::new(0),
        released: AtomicBool::new(false),
        fail_renewal: AtomicBool::new(false),
    });
    let supervisor = StagingLeaseSupervisor::with_timing(
        authority.clone(),
        Arc::new(LeaseTestHost),
        StagingLeaseTiming {
            lease_duration: std::time::Duration::from_millis(100),
            renew_interval: std::time::Duration::from_millis(5),
        },
    )
    .unwrap();
    let lease = supervisor
        .acquire(
            StagingLeaseIdentity::new(
                DestinationId::new("mock_staged").unwrap(),
                TargetName::new("events").unwrap(),
                LoadAttemptId::new("renewal-failure").unwrap(),
            ),
            cdf_kernel::LeaseOwnerId::new("runtime-owner").unwrap(),
        )
        .unwrap();
    let guard = lease.mutation_guard().unwrap();
    guard.assert_current().unwrap();
    authority.fail_renewal.store(true, Ordering::SeqCst);
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
    while guard.assert_current().is_ok() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let error = guard.assert_current().unwrap_err();
    assert!(
        error.message.contains("cancelled") || error.message.contains("renewal failure"),
        "unexpected mutation guard error: {error}"
    );
    drop(guard);
    assert!(lease.finish().is_err());
}

fn test_prepared_bulk_path() -> PreparedBulkPath {
    PreparedBulkPath {
        descriptor: BulkPathDescriptor {
            path_id: "mock_staged".to_owned(),
            version: 1,
            ingress_mode: DestinationIngressMode::StagedDurableSegments,
            writer_model: DestinationWriterModel::SingleWriter,
            ordering: BulkOrdering::ManifestOrder,
            rows: BulkSizeRange {
                minimum: 1,
                preferred: 64 * 1024,
                maximum: 1024 * 1024,
            },
            bytes: BulkSizeRange {
                minimum: 1,
                preferred: 16 * 1024 * 1024,
                maximum: 64 * 1024 * 1024,
            },
            max_useful_writers: 1,
            blocking_lane: None,
            native_internal_parallelism: 1,
            external_staging: false,
            fallback: BulkFallbackMode::PreflightOnly,
            schema_preflight_version: "mock-schema@1".to_owned(),
            measured_evidence_version: Some("mock-staged-v1".to_owned()),
        },
        rows_per_batch: 64 * 1024,
        bytes_per_batch: 16 * 1024 * 1024,
        writers: 1,
    }
}

fn mock_bulk_descriptor(
    path_id: &str,
    evidence_version: &str,
    ingress_mode: DestinationIngressMode,
    writer_model: DestinationWriterModel,
) -> BulkPathDescriptor {
    BulkPathDescriptor {
        path_id: path_id.to_owned(),
        version: 1,
        ingress_mode,
        writer_model,
        ordering: BulkOrdering::ManifestOrder,
        rows: BulkSizeRange {
            minimum: 1,
            preferred: 64 * 1024,
            maximum: 1024 * 1024,
        },
        bytes: BulkSizeRange {
            minimum: 1,
            preferred: 16 * 1024 * 1024,
            maximum: 64 * 1024 * 1024,
        },
        max_useful_writers: 1,
        blocking_lane: None,
        native_internal_parallelism: 1,
        external_staging: false,
        fallback: BulkFallbackMode::PreflightOnly,
        schema_preflight_version: "mock-schema@1".to_owned(),
        measured_evidence_version: Some(evidence_version.to_owned()),
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
        execution_plan_id: PlanId::new("plan-staged").unwrap(),
        commit: DestinationCommitRequest {
            package_hash: package_hash.clone(),
            target: target.clone(),
            disposition: WriteDisposition::Append,
            segments: Vec::new(),
            idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
        },
        schema_hash,
        output_arrow_schema_hash: cdf_kernel::canonical_arrow_schema_hash(
            &arrow_schema::Schema::empty(),
        )
        .unwrap(),
        merge_keys: Vec::new(),
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
        "cdf-package =",
        "parquet =",
        "arrow-ipc =",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "cdf-runtime manifest contains forbidden dependency `{forbidden}`"
        );
    }
}

#[test]
fn production_graph_edges_cannot_carry_naked_data_payloads() {
    fn visit(directory: &Path, violations: &mut Vec<String>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                visit(&path, violations);
                continue;
            }
            if path.extension().and_then(|value| value.to_str()) != Some("rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path).unwrap();
            let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
            for forbidden in [
                "mpsc::Sender<RecordBatch",
                "mpsc::Receiver<RecordBatch",
                "Sender<Vec<u8",
                "Receiver<Vec<u8",
                "channel::<RecordBatch",
                "channel::<Vec<u8",
            ] {
                if production.contains(forbidden) {
                    violations.push(format!("{} contains {forbidden}", path.display()));
                }
            }
        }
    }

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap();
    let mut violations = Vec::new();
    visit(&workspace.join("crates"), &mut violations);
    assert!(
        violations.is_empty(),
        "production graph edges must carry accounted envelopes, never naked Arrow/byte payloads:\n{}",
        violations.join("\n")
    );
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
