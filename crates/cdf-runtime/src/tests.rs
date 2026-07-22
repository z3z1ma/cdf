use crate::prelude::*;
use arrow_schema::{DataType, Field};
use cdf_kernel::{
    BatchStream, CommitCounts, ConcurrencyLimit, DeliveryGuarantee, DestinationId,
    EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime, ErrorKind, IdempotencySupport,
    IdempotencyToken, IdentifierRules, MigrationRecord, PackageHash, PartitionAuthority,
    PartitionId, PartitionPlan, PlanId, QueryableResource, ReceiptId, ResourceCapabilities,
    ResourceDescriptor, ResourceId, ResourceStream, ScanPlan, ScanRequest, SchemaHash,
    SchemaSource, ScopeKey, SegmentAck, SegmentId, TargetName, TransactionMetadata,
    TransactionSupport, TrustLevel, TypeMapping, TypePolicyAllowances, VerifyClause,
};
use sha2::{Digest, Sha256};
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

    fn write_segments(
        &mut self,
        segments: cdf_kernel::CommitSegmentIterator,
    ) -> Result<Vec<SegmentAck>> {
        let mut acknowledgements = Vec::new();
        for segment in segments {
            let segment = segment?;
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
            acknowledgements.push(ack);
        }
        Ok(acknowledgements)
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

struct LocalFileSegmentReader {
    identity: StagedSegmentIdentity,
    path: Option<std::path::PathBuf>,
}

impl DurableSegmentReader for LocalFileSegmentReader {
    fn identity(&self) -> &StagedSegmentIdentity {
        &self.identity
    }

    fn take_durable_local_file_access(&mut self) -> Result<Option<DurableLocalFileAccess>> {
        let Some(path) = self.path.take() else {
            return Ok(None);
        };
        let open_path = path.clone();
        Ok(Some(DurableLocalFileAccess::new(
            path,
            self.identity.byte_count,
            self.identity.sha256.clone(),
            move || {
                std::fs::File::open(&open_path)
                    .map_err(|error| CdfError::data(format!("open test durable segment: {error}")))
            },
        )))
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
    tamper_baseline: bool,
    tamper_resolve: bool,
    tamper_resolved_runtime: bool,
    omit_partition_binding: bool,
}

impl SourceDriver for MockSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn validate_portable_plan(&self, plan: &CompiledSourcePlan) -> Result<()> {
        plan.validate()
    }

    fn health(
        &self,
        request: SourceHealthRequest,
        _context: &SourceResolutionContext<'_>,
        output: &mut dyn SourceHealthSink,
    ) -> Result<()> {
        request.budget.consume_work(1)?;
        output.emit(SourceHealthResult {
            probe_id: "mock".to_owned(),
            status: SourceHealthStatus::Passed,
            message: "mock source health probe passed".to_owned(),
            details: serde_json::json!({"compiled_resources": request.compiled_plans.len()}),
        })
    }

    fn add_planner(&self) -> Option<&dyn SourceAddPlanner> {
        Some(self)
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        let mut baseline_observation_schema_catalog = request.baseline_observation_schema_catalog;
        if self.tamper_baseline {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "tampered",
                DataType::Utf8,
                true,
            )]));
            baseline_observation_schema_catalog = vec![EffectiveSchemaCatalogEntry::new(
                cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap(),
                schema,
            )];
        }
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            ResourceCapabilities::default(),
            SourceExecutionCapabilities {
                minimum_poll_bytes: 1,
                maximum_poll_bytes: 1024,
                minimum_decode_bytes: 1,
                maximum_decode_bytes: 4096,
                maximum_emitted_batch_bytes: 4096,
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
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog,
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
        let schema = if self.tamper_resolve {
            Arc::new(Schema::new(vec![Field::new(
                "tampered",
                DataType::Utf8,
                true,
            )]))
        } else {
            Arc::new(plan.schema.clone())
        };
        Ok(Arc::new(MockSourceResource {
            descriptor: plan.descriptor.clone(),
            schema,
            capabilities: plan.resource_capabilities.clone(),
            type_policy_allowances: plan.type_policy_allowances,
            effective_schema_runtime: if self.tamper_resolved_runtime {
                None
            } else {
                plan.effective_schema_runtime.clone()
            },
            baseline_observation_schema_catalog: plan.baseline_observation_schema_catalog.clone(),
            compiled_source_plan_hash: plan.compiled_source_plan_hash()?,
            omit_partition_binding: self.omit_partition_binding,
        }))
    }
}

impl SourceAddPlanner for MockSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        if request.location != "mock://add" {
            return Ok(None);
        }
        Ok(Some(SourceAddProposal {
            source_kind: self.descriptor.kinds[0].clone(),
            source_options: BTreeMap::new(),
            resource_options: BTreeMap::new(),
            cursor: None,
            display_location: SourceEvidenceLocation::from_operational(&request.location)?,
            display_selection: request.resource_name.clone(),
            private_files: Vec::new(),
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
    type_policy_allowances: TypePolicyAllowances,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    compiled_source_plan_hash: cdf_kernel::CompiledSourcePlanHash,
    omit_partition_binding: bool,
}

impl ResourceStream for MockSourceResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        Some(&self.compiled_source_plan_hash)
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }

    fn baseline_observation_schema_catalog(&self) -> &[EffectiveSchemaCatalogEntry] {
        &self.baseline_observation_schema_catalog
    }

    fn type_policy_allowances(&self) -> TypePolicyAllowances {
        self.type_policy_allowances
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        let mut partition = PartitionPlan {
            partition_id: PartitionId::new("mock-000001")?,
            scope: request.scope.clone(),
            planned_position: None,
            start_position: None,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::new(),
        };
        if !self.omit_partition_binding
            && let Some(runtime) = &self.effective_schema_runtime
        {
            cdf_kernel::bind_partition_schema_observation(
                &mut partition,
                runtime,
                self.descriptor.resource_id.as_str(),
            )?;
        }
        Ok(vec![partition])
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
        Ok(ScanPlan::from_partition_authority(
            PlanId::new("mock-source-plan")?,
            request.clone(),
            PartitionAuthority::Inline(self.plan_partitions(request)?),
            Vec::new(),
            request.filters.clone(),
            Some(0),
            Some(0),
            DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        ))
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

    fn unix_now(&self) -> std::time::Duration {
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
fn source_registry_add_hook_selects_one_driver_and_rejects_ambiguity() {
    let option_schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "source": {"type": "object", "additionalProperties": false, "properties": {}},
        "resource": {"type": "object", "additionalProperties": false, "properties": {}}
    });
    let driver = |id: &str, kind: &str| MockSourceDriver {
        descriptor: SourceDriverDescriptor {
            driver_id: SourceDriverId::new(id).unwrap(),
            driver_version: "1.0.0".to_owned(),
            option_schema_hash: artifact_hash(&option_schema).unwrap(),
            kinds: vec![kind.to_owned()],
            schemes: vec![id.to_owned()],
        },
        option_schema: option_schema.clone(),
        tamper_baseline: false,
        tamper_resolve: false,
        tamper_resolved_runtime: false,
        omit_partition_binding: false,
    };
    let request = SourceAddRequest {
        source_name: "mock".to_owned(),
        resource_name: "events".to_owned(),
        location: "mock://add".to_owned(),
        project_root: std::path::PathBuf::from("/project"),
        current_dir: std::path::PathBuf::from("/working"),
        options: BTreeMap::new(),
        project_options: None,
    };
    let mut registry = SourceRegistry::new();
    registry.register(driver("mock_one", "mock")).unwrap();
    let planned = registry
        .plan_add(request.clone(), &BTreeMap::new())
        .unwrap();
    assert_eq!(planned.driver.driver_id.as_str(), "mock_one");
    assert_eq!(planned.proposal.source_kind, "mock");

    registry.register(driver("mock_two", "mock_two")).unwrap();
    let error = registry.plan_add(request, &BTreeMap::new()).unwrap_err();
    assert!(error.message.contains("ambiguous"));
    assert!(error.message.contains("mock_one, mock_two"));
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
            tamper_baseline: false,
            tamper_resolve: false,
            tamper_resolved_runtime: false,
            omit_partition_binding: false,
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
    let baseline_schema = Arc::new(Schema::empty());
    let baseline_observation = EffectiveSchemaCatalogEntry::new(
        cdf_kernel::canonical_arrow_schema_hash(baseline_schema.as_ref()).unwrap(),
        baseline_schema,
    );
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
        baseline_observation_schema_catalog: vec![baseline_observation.clone()],
    };
    let mut invalid_request = request.clone();
    invalid_request.context.source_name.clear();
    let error = registry.compile(invalid_request).unwrap_err();
    assert!(error.message.contains("requires a source name"));

    let mut invalid_options = request.clone();
    invalid_options.source_options.insert(
        "driver_would_ignore_this".to_owned(),
        serde_json::json!(true),
    );
    let error = registry.compile(invalid_options).unwrap_err();
    assert!(
        error
            .message
            .contains("does not allow field `driver_would_ignore_this`")
    );

    let plan = registry.compile(request.clone()).unwrap();
    let portable_plan_bytes = serde_json::to_vec(&plan).unwrap();
    let portable_source = PortableSourceBinding {
        driver_id: descriptor.driver_id.clone(),
        driver_version: descriptor.driver_version.clone(),
        option_schema_hash: descriptor.option_schema_hash.clone(),
        compiled_source_plan: WorkerArtifactReference {
            kind: WorkerArtifactKind::CompiledSourcePlan,
            store_namespace: cdf_kernel::ContentStoreNamespace::new("worker-test").unwrap(),
            object_key: cdf_kernel::ContentObjectKey::new("plans/mock-source.json").unwrap(),
            byte_count: u64::try_from(portable_plan_bytes.len()).unwrap(),
            content_sha256: artifact_hash(&plan).unwrap(),
            provider_generation: Some(
                cdf_kernel::ContentProviderGeneration::new("generation-1").unwrap(),
            ),
        },
        redacted_options_hash: plan.redacted_options_hash.clone(),
        physical_plan_hash: plan.physical_plan_hash.clone(),
        source_semantics_hash: plan.schema_binding_stable_hash().unwrap(),
        execution_capabilities_hash: artifact_hash(&plan.execution_capabilities).unwrap(),
    };
    registry
        .validate_portable_source_binding(&portable_source)
        .unwrap();
    registry
        .validate_portable_source_plan(&portable_source, &plan)
        .unwrap();
    let mut stale_portable_source = portable_source.clone();
    stale_portable_source.driver_version = "2.0.0".to_owned();
    assert!(
        registry
            .validate_portable_source_binding(&stale_portable_source)
            .unwrap_err()
            .message
            .contains("does not match")
    );
    let mut tampering_registry = SourceRegistry::new();
    tampering_registry
        .register(MockSourceDriver {
            descriptor: descriptor.clone(),
            option_schema: option_schema.clone(),
            tamper_baseline: true,
            tamper_resolve: false,
            tamper_resolved_runtime: false,
            omit_partition_binding: false,
        })
        .unwrap();
    assert!(
        tampering_registry
            .compile(request)
            .unwrap_err()
            .message
            .contains("changed compiler-owned schema or resource authority")
    );
    let original_plan_hash = plan.physical_plan_hash.clone();
    let original_stable_hash = plan.schema_binding_stable_hash().unwrap();
    let mut discovered_descriptor = plan.descriptor.clone();
    discovered_descriptor.schema_source = SchemaSource::Discover;
    let discovered_schema = Schema::new(vec![Field::new("event_id", DataType::Int64, false)]);
    let bound = plan
        .clone()
        .bind_schema_authority(
            &discovered_descriptor,
            &discovered_schema,
            None,
            vec![baseline_observation.clone()],
        )
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
            .bind_schema_authority(
                &invalid_binding,
                &bound.schema,
                None,
                vec![baseline_observation.clone()],
            )
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
    let mut malformed_physical_identity = serde_json::to_value(&plan).unwrap();
    malformed_physical_identity["physical_plan_hash"] = serde_json::json!("not-a-hash");
    assert!(serde_json::from_value::<CompiledSourcePlan>(malformed_physical_identity).is_err());
    let mut forged_physical_identity = serde_json::to_value(&plan).unwrap();
    forged_physical_identity["physical_plan_hash"] =
        serde_json::json!(format!("sha256:{}", "f".repeat(64)));
    let forged_physical_identity: CompiledSourcePlan =
        serde_json::from_value(forged_physical_identity).unwrap();
    assert!(
        forged_physical_identity
            .validate()
            .unwrap_err()
            .message
            .contains("physical plan hash does not match")
    );
    let mut invalid_descriptor = serde_json::to_value(&plan).unwrap();
    invalid_descriptor["descriptor"]["resource_id"] = serde_json::json!("");
    let invalid_descriptor: CompiledSourcePlan =
        serde_json::from_value(invalid_descriptor).unwrap();
    assert!(
        invalid_descriptor
            .validate()
            .unwrap_err()
            .message
            .contains("ResourceId cannot be empty")
    );

    let mut invalid_capabilities = serde_json::to_value(&plan).unwrap();
    invalid_capabilities["resource_capabilities"]["filters"]["default_fidelity"] =
        serde_json::json!("unsupported");
    invalid_capabilities["resource_capabilities"]["filters"]["supported_operators"] =
        serde_json::json!(["="]);
    let invalid_capabilities: CompiledSourcePlan =
        serde_json::from_value(invalid_capabilities).unwrap();
    assert!(
        invalid_capabilities
            .validate()
            .unwrap_err()
            .message
            .contains("unsupported pushdown fidelity")
    );

    let mut scalar_options = plan.clone();
    scalar_options.redacted_options = serde_json::json!("not-an-options-object");
    scalar_options.redacted_options_hash = artifact_hash(&scalar_options.redacted_options).unwrap();
    assert!(
        scalar_options
            .validate()
            .unwrap_err()
            .message
            .contains("must be JSON objects")
    );

    for unsafe_uri in [
        "https://alice:secret@example.test/items",
        "https://example.test/items?token=secret",
        "https://example.test/items#fragment",
    ] {
        let mut credential_uri = plan.clone();
        credential_uri.physical_plan = serde_json::json!({"endpoint": unsafe_uri});
        credential_uri.physical_plan_hash = cdf_kernel::PhysicalSourcePlanHash::new(
            artifact_hash(&credential_uri.physical_plan).unwrap(),
        )
        .unwrap();
        let error = credential_uri.validate().unwrap_err();
        assert!(
            error
                .message
                .contains("must not contain user information, query parameters, or a fragment"),
            "accepted unsafe compiled source URI {unsafe_uri:?}"
        );
    }

    let mut malformed_uri = plan.clone();
    malformed_uri.physical_plan = serde_json::json!({"endpoint": "https:///items"});
    malformed_uri.physical_plan_hash = cdf_kernel::PhysicalSourcePlanHash::new(
        artifact_hash(&malformed_uri.physical_plan).unwrap(),
    )
    .unwrap();
    assert!(
        malformed_uri
            .validate()
            .unwrap_err()
            .message
            .contains("malformed absolute URI")
    );

    let mut local_file_uri = plan.clone();
    local_file_uri.physical_plan = serde_json::json!({"endpoint": "file:///tmp/events.parquet"});
    local_file_uri.physical_plan_hash = cdf_kernel::PhysicalSourcePlanHash::new(
        artifact_hash(&local_file_uri.physical_plan).unwrap(),
    )
    .unwrap();
    local_file_uri.validate().unwrap();

    let mut raw_secret = plan.clone();
    raw_secret.physical_plan = serde_json::json!({"api_key": "plain-text-secret"});
    raw_secret.physical_plan_hash =
        cdf_kernel::PhysicalSourcePlanHash::new(artifact_hash(&raw_secret.physical_plan).unwrap())
            .unwrap();
    let error = raw_secret.validate().unwrap_err();
    assert!(error.message.contains("must contain a secret:// reference"));

    let mut invalid_snapshot = serde_json::to_value(&plan).unwrap();
    invalid_snapshot["descriptor"]["schema_source"] = serde_json::json!({
        "kind": "discovered",
        "snapshot": {
            "schema_hash": "not-the-empty-arrow-schema",
            "path": "",
            "metadata": {}
        }
    });
    let invalid_snapshot: CompiledSourcePlan = serde_json::from_value(invalid_snapshot).unwrap();
    assert!(
        invalid_snapshot
            .validate()
            .unwrap_err()
            .message
            .contains("schema snapshot path")
    );

    let mut invalid_runtime = plan.clone();
    let evidence = cdf_kernel::EffectiveSchemaEvidence::new(
        invalid_runtime
            .descriptor
            .schema_source
            .baseline_reference()
            .unwrap(),
        SchemaHash::new("effective-schema-fixture").unwrap(),
        cdf_kernel::DiscoveryManifestReference {
            manifest_hash: cdf_kernel::DiscoveryManifestHash::new("manifest-fixture").unwrap(),
            path: ".cdf/discovery/mock.json".to_owned(),
        },
        Vec::new(),
    )
    .unwrap();
    let mut runtime = EffectiveSchemaRuntime::new(evidence, Vec::new()).unwrap();
    let mut invalid_budget = cdf_kernel::DiscoveryExecutorBudgetEvidence::new(1, 1, 1, 1).unwrap();
    invalid_budget.max_bytes_per_file = 0;
    runtime.discovery_executor_budget = Some(invalid_budget);
    invalid_runtime.effective_schema_runtime = Some(runtime);
    invalid_runtime.baseline_observation_schema_catalog.clear();
    assert!(
        invalid_runtime
            .validate()
            .unwrap_err()
            .message
            .contains("discovery executor budget")
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
    let context = SourceResolutionContext::new(
        root.path(),
        secrets,
        &services,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let reconstructed_plan: CompiledSourcePlan =
        serde_json::from_slice(&portable_plan_bytes).unwrap();
    assert_eq!(
        artifact_hash(&reconstructed_plan).unwrap(),
        portable_source.compiled_source_plan.content_sha256
    );
    registry.resolve(&reconstructed_plan, &context).unwrap();
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
        resource.baseline_observation_schema_catalog(),
        std::slice::from_ref(&baseline_observation)
    );

    let mut hostile_registry = SourceRegistry::new();
    hostile_registry
        .register(MockSourceDriver {
            descriptor: descriptor.clone(),
            option_schema: option_schema.clone(),
            tamper_baseline: false,
            tamper_resolve: true,
            tamper_resolved_runtime: false,
            omit_partition_binding: false,
        })
        .unwrap();
    let error = match hostile_registry.resolve(&plan, &context) {
        Ok(_) => panic!("hostile resolved source authority must fail closed"),
        Err(error) => error,
    };
    assert!(
        error
            .message
            .contains("resolved executable authority that differs from its compiled plan")
    );

    let physical_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&plan.schema).unwrap();
    let observation_binding = cdf_kernel::SchemaObservationBinding::new(
        artifact_hash(&serde_json::json!({"mock_partition": "mock.events"})).unwrap(),
    )
    .unwrap();
    let runtime_evidence = cdf_kernel::EffectiveSchemaEvidence::new(
        plan.descriptor.schema_source.baseline_reference().unwrap(),
        physical_schema_hash.clone(),
        cdf_kernel::DiscoveryManifestReference {
            manifest_hash: cdf_kernel::DiscoveryManifestHash::new("mock-runtime-manifest").unwrap(),
            path: ".cdf/discovery/mock-runtime.json".to_owned(),
        },
        vec![cdf_kernel::EffectiveSchemaObservationEvidence::new(
            plan.descriptor.resource_id.as_str(),
            physical_schema_hash,
            observation_binding,
        )],
    )
    .unwrap();
    let effective_runtime =
        EffectiveSchemaRuntime::new(runtime_evidence, vec![baseline_observation.clone()]).unwrap();
    let runtime_bound_plan = plan
        .clone()
        .bind_schema_authority(
            &plan.descriptor,
            &plan.schema,
            Some(effective_runtime),
            vec![baseline_observation.clone()],
        )
        .unwrap();

    let mut runtime_tampering_registry = SourceRegistry::new();
    runtime_tampering_registry
        .register(MockSourceDriver {
            descriptor: descriptor.clone(),
            option_schema: option_schema.clone(),
            tamper_baseline: false,
            tamper_resolve: false,
            tamper_resolved_runtime: true,
            omit_partition_binding: false,
        })
        .unwrap();
    let error = match runtime_tampering_registry.resolve(&runtime_bound_plan, &context) {
        Ok(_) => panic!("registry must reject adapter-local effective schema authority"),
        Err(error) => error,
    };
    assert!(
        error.message.contains("effective schema runtime"),
        "{error}"
    );

    let mut missing_binding_registry = SourceRegistry::new();
    missing_binding_registry
        .register(MockSourceDriver {
            descriptor: descriptor.clone(),
            option_schema: option_schema.clone(),
            tamper_baseline: false,
            tamper_resolve: false,
            tamper_resolved_runtime: false,
            omit_partition_binding: true,
        })
        .unwrap();
    let missing_binding_resource = missing_binding_registry
        .resolve(&runtime_bound_plan, &context)
        .unwrap();
    let error = missing_binding_resource
        .negotiate(&ScanRequest {
            resource_id: runtime_bound_plan.descriptor.resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        })
        .unwrap_err();
    assert!(
        error
            .message
            .contains("omitted its effective-schema observation identity"),
        "{error}"
    );

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
            tamper_baseline: false,
            tamper_resolve: false,
            tamper_resolved_runtime: false,
            omit_partition_binding: false,
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
            tamper_baseline: false,
            tamper_resolve: false,
            tamper_resolved_runtime: false,
            omit_partition_binding: false,
        })
        .unwrap_err();
    assert!(error.message.contains("must be a closed object"));

    let mut reordered = SourceRegistry::new();
    reordered
        .register(MockSourceDriver {
            descriptor,
            option_schema: option_schema.clone(),
            tamper_baseline: false,
            tamper_resolve: false,
            tamper_resolved_runtime: false,
            omit_partition_binding: false,
        })
        .unwrap();
    assert_eq!(reordered.descriptors(), registry.descriptors());
    assert!(
        reordered
            .register(MockSourceDriver {
                descriptor: reordered.descriptors()[0].clone(),
                option_schema,
                tamper_baseline: false,
                tamper_resolve: false,
                tamper_resolved_runtime: false,
                omit_partition_binding: false,
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
fn staged_segment_request_defers_exact_durable_local_file_verification() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("segment.arrow");
    std::fs::write(&path, b"12345678").unwrap();
    let mut identity = staged_identity("seg-local", 0, SchemaHash::new("schema-v1").unwrap());
    identity.sha256 = hex::encode(Sha256::digest(b"12345678"));
    let mut request = StagedSegmentRequest::new(
        identity.clone(),
        Box::new(LocalFileSegmentReader {
            identity: identity.clone(),
            path: Some(path.clone()),
        }),
    )
    .unwrap();
    let access = request.take_durable_local_file_access().unwrap();
    assert_eq!(access.path(), path.as_path());
    let (_, mut file) = access.open().unwrap().into_parts();
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut bytes).unwrap();
    assert_eq!(bytes, b"12345678");

    std::fs::write(&path, b"replaced").unwrap();
    let mut request = StagedSegmentRequest::new(
        identity.clone(),
        Box::new(LocalFileSegmentReader {
            identity,
            path: Some(path),
        }),
    )
    .unwrap();
    let error = request
        .take_durable_local_file_access()
        .unwrap()
        .open()
        .unwrap_err();
    assert!(error.message.contains("changed after publication"));
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
        package_statistics: None,
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
        package_row_ord_start: u64::from(ordinal),
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

#[derive(Default)]
struct LeaseTestHost {
    fail_delay: bool,
}

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
        let fail_delay = self.fail_delay;
        Box::pin(async move {
            let deadline = std::time::Instant::now() + duration;
            while std::time::Instant::now() < deadline {
                cancellation.check()?;
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            cancellation.check()?;
            if fail_delay {
                Err(CdfError::transient(
                    "injected staging supervisor delay failure",
                ))
            } else {
                Ok(())
            }
        })
    }

    fn monotonic_now(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    fn unix_now(&self) -> std::time::Duration {
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
        Arc::new(LeaseTestHost::default()),
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

#[test]
fn staging_lease_builds_fenced_content_publication_claim() {
    let attempt = LoadAttemptId::new("content-attempt").unwrap();
    let lease = test_staging_lease(&attempt, "parquet", "target");
    let content = cdf_kernel::ImmutableContentIdentity::new(
        cdf_kernel::ContentStoreNamespace::new("store").unwrap(),
        cdf_kernel::ContentObjectKey::new("objects/a.parquet").unwrap(),
        42,
        cdf_kernel::ContentDigest::new(
            cdf_kernel::ContentDigestAlgorithm::new("sha256").unwrap(),
            cdf_kernel::ContentDigestValue::new("a".repeat(64)).unwrap(),
        )
        .unwrap(),
        Some(cdf_kernel::ContentProviderGeneration::new("etag-a").unwrap()),
    )
    .unwrap();
    let claim = lease
        .content_publication_claim(
            content.clone(),
            cdf_kernel::ContentPublicationClaimId::new("claim-a").unwrap(),
            1,
            cdf_kernel::ContentPublicationClaimState::Published,
        )
        .unwrap();

    assert_eq!(claim.destination_id, lease.identity.destination_id);
    assert_eq!(claim.target, lease.identity.target);
    assert_eq!(claim.attempt_id.as_str(), attempt.as_str());
    assert_eq!(claim.lease_authority_domain_id, lease.authority_domain_id);
    assert_eq!(claim.lease, lease.scope_lease);
    assert_eq!(claim.content, content);
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
        Arc::new(LeaseTestHost::default()),
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
    let initial = lease.snapshot().unwrap();
    let initial_expiry = initial.scope_lease.expires_at_ms;
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
    let renewed = lease.snapshot().unwrap();
    assert!(renewed.scope_lease.expires_at_ms > initial_expiry);
    assert_ne!(initial, renewed);
    assert!(initial.same_generation(&renewed));
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
        Arc::new(LeaseTestHost::default()),
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

#[test]
fn staging_cleanup_preserves_storage_and_release_failures() {
    let error = combine_cleanup_release::<()>(
        Err(CdfError::destination("injected staging cleanup failure")),
        Err(CdfError::transient("injected lease release failure")),
    )
    .unwrap_err();
    assert!(error.message.contains("injected staging cleanup failure"));
    assert!(error.message.contains("injected lease release failure"));
}

#[test]
fn staging_supervisor_terminal_failure_cancels_live_and_rejects_new_leases() {
    let authority = Arc::new(RecordingStagingLeaseAuthority {
        renewals: AtomicUsize::new(0),
        released: AtomicBool::new(false),
        fail_renewal: AtomicBool::new(false),
    });
    let supervisor = StagingLeaseSupervisor::with_timing(
        authority,
        Arc::new(LeaseTestHost { fail_delay: true }),
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
                LoadAttemptId::new("terminal-supervisor-failure").unwrap(),
            ),
            cdf_kernel::LeaseOwnerId::new("runtime-owner").unwrap(),
        )
        .unwrap();
    let guard = lease.mutation_guard().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
    while guard.assert_current().is_ok() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let error = guard.assert_current().unwrap_err();
    assert!(error.message.contains("supervisor delay failure"));
    let rejected = match supervisor.acquire(
        StagingLeaseIdentity::new(
            DestinationId::new("mock_staged").unwrap(),
            TargetName::new("events").unwrap(),
            LoadAttemptId::new("after-terminal-failure").unwrap(),
        ),
        cdf_kernel::LeaseOwnerId::new("runtime-owner-2").unwrap(),
    ) {
        Ok(_) => panic!("terminal staging supervisor accepted a new lease"),
        Err(error) => error,
    };
    assert!(rejected.message.contains("supervisor delay failure"));
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
        package_statistics: None,
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
            binding: BlockingLaneBinding::Static,
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

#[test]
fn source_egress_scope_exposes_only_normalized_credential_free_authority() {
    #[derive(Default)]
    struct RecordingAuthorizer {
        requests: Mutex<Vec<SourceEgressRequest>>,
    }

    impl SourceEgressAuthorizer for RecordingAuthorizer {
        fn authorize(&self, request: &SourceEgressRequest) -> Result<()> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(())
        }
    }

    let authorizer = Arc::new(RecordingAuthorizer::default());
    let scope =
        SourceEgressScope::new(SourceDriverId::new("postgres").unwrap(), authorizer.clone());
    scope
        .authorize("postgres://operator:secret@[2001:db8::1]:5432/catalog?token=hidden")
        .unwrap();

    let requests = authorizer.requests.lock().unwrap();
    let [request] = requests.as_slice() else {
        panic!("expected one source-egress request")
    };
    assert_eq!(request.driver_id.as_str(), "postgres");
    assert_eq!(request.target.scheme(), "postgres");
    assert_eq!(request.target.host(), "2001:db8::1");
    assert_eq!(request.target.port(), Some(5432));
    assert!(!format!("{request:?}").contains("operator"));
    assert!(!format!("{request:?}").contains("secret"));
    assert!(!format!("{request:?}").contains("hidden"));

    let normalized = SourceEgressTarget::parse("HTTPS://EXAMPLE.TEST./data").unwrap();
    assert_eq!(normalized.host(), "example.test");
    assert_eq!(normalized.port(), None);
    assert_eq!(normalized.canonical_authority(), "https://example.test:443");
    let explicit_default = SourceEgressTarget::parse("https://example.test:443/other").unwrap();
    assert_eq!(explicit_default.port(), None);
    assert_eq!(
        explicit_default.canonical_authority(),
        normalized.canonical_authority()
    );
    assert_eq!(
        SourceEgressTarget::parse("http://192.0.2.1/events?secret=value#fragment")
            .unwrap()
            .canonical_authority(),
        "http://192.0.2.1:80"
    );
    assert_eq!(
        SourceEgressTarget::parse("s3://WAREHOUSE-BUCKET./prefix/object.parquet")
            .unwrap()
            .canonical_authority(),
        "s3://warehouse-bucket"
    );
    assert_eq!(
        SourceEgressTarget::parse("gs://analytics-bucket:8443/prefix")
            .unwrap()
            .canonical_authority(),
        "gs://analytics-bucket:8443"
    );
    assert_eq!(
        SourceEgressTarget::parse("az://[2001:0db8:0000:0000:0000:0000:0000:0002]/container")
            .unwrap()
            .canonical_authority(),
        "az://[2001:db8::2]"
    );

    for invalid in [
        "",
        "example.test/path",
        "https://",
        "https:///path",
        "https://@example.test/path",
        "https://alice@bob@example.test/path",
        "https://2001:db8::1/data",
        "https://example.test:0/data",
        "https://example.test:/data",
        "https://example.test:not-a-port/data",
        "https://example.test:65536/data",
        "https://example.test/path with spaces",
        "https://example.test/path#mixed text",
    ] {
        assert!(
            SourceEgressTarget::parse(invalid).is_err(),
            "accepted malformed egress URI {invalid:?}"
        );
    }
}

#[test]
fn source_evidence_locations_redact_uri_secrets_and_preserve_local_paths() {
    assert_eq!(
        SourceEvidenceLocation::from_operational(
            "https://alice:secret@example.test/events?token=hidden#fragment"
        )
        .unwrap()
        .as_str(),
        "https://example.test/events?<redacted>"
    );
    assert_eq!(
        SourceEvidenceLocation::from_operational("file:///tmp/events.parquet")
            .unwrap()
            .as_str(),
        "file:///tmp/events.parquet"
    );
    assert_eq!(
        SourceEvidenceLocation::from_operational("/tmp/events?token=hidden#fragment")
            .unwrap()
            .as_str(),
        "/tmp/events?<redacted>"
    );
    assert!(SourceEvidenceLocation::from_operational("https:///missing-host").is_err());
}
