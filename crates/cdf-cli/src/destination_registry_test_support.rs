use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use cdf_kernel::{
    CapabilitySupport, CdfError, CommitCounts, CommitPlan, CommitSession, ConcurrencyLimit,
    DeliveryGuarantee, DestinationCommitRequest, DestinationId, DestinationProtocol,
    DestinationSheet, IdempotencySupport, IdentifierRules, Receipt, ReceiptId, ReceiptVerification,
    Result, SchemaHash, SegmentAck, TransactionSupport, TypeMapping, TypeMappingFidelity,
    VerifyClause, WriteDisposition,
};
use cdf_runtime::{
    BulkFallbackMode, BulkOrdering, BulkPathDescriptor, BulkSizeRange, DestinationDescription,
    DestinationDriver, DestinationHealthProbe, DestinationHealthResult, DestinationHealthStatus,
    DestinationIngress, DestinationIngressMode, DestinationInspection, DestinationRegistry,
    DestinationResolutionContext, DestinationRuntime, DestinationRuntimeCapabilities,
    DestinationWriterModel, FinalizedPackageIngress, PreparedDestinationCommit,
};

const SCHEME: &str = "fourth";
const SECRET_SENTINEL: &str = "fourth-registry-secret";

#[derive(Clone, Default)]
pub(crate) struct FourthDestinationState {
    inner: Arc<FourthDestinationStateInner>,
}

#[derive(Default)]
struct FourthDestinationStateInner {
    inspections: AtomicU64,
    health_checks: AtomicU64,
    resolutions: AtomicU64,
    plans: AtomicU64,
    commit_begins: AtomicU64,
    durable_commits: AtomicU64,
    receipt_verifications: AtomicU64,
    receipts: Mutex<BTreeMap<String, Receipt>>,
}

impl FourthDestinationState {
    pub(crate) fn inspections(&self) -> u64 {
        self.inner.inspections.load(Ordering::SeqCst)
    }

    pub(crate) fn health_checks(&self) -> u64 {
        self.inner.health_checks.load(Ordering::SeqCst)
    }

    pub(crate) fn resolutions(&self) -> u64 {
        self.inner.resolutions.load(Ordering::SeqCst)
    }

    pub(crate) fn plans(&self) -> u64 {
        self.inner.plans.load(Ordering::SeqCst)
    }

    pub(crate) fn commit_begins(&self) -> u64 {
        self.inner.commit_begins.load(Ordering::SeqCst)
    }

    pub(crate) fn durable_commits(&self) -> u64 {
        self.inner.durable_commits.load(Ordering::SeqCst)
    }

    pub(crate) fn receipt_verifications(&self) -> u64 {
        self.inner.receipt_verifications.load(Ordering::SeqCst)
    }

    fn receipt(&self, token: &str) -> Option<Receipt> {
        self.inner.receipts.lock().unwrap().get(token).cloned()
    }

    fn record_receipt(&self, receipt: Receipt) {
        self.inner
            .receipts
            .lock()
            .unwrap()
            .insert(receipt.idempotency_token.as_str().to_owned(), receipt);
    }
}

pub(crate) fn registry_with_fourth_destination()
-> Result<(DestinationRegistry, FourthDestinationState)> {
    let mut registry = crate::destination_registry::builtin_destination_registry()?;
    let state = FourthDestinationState::default();
    registry.register(FourthDestinationDriver {
        state: state.clone(),
    })?;
    Ok((registry, state))
}

pub(crate) fn destination_uri() -> String {
    format!("{SCHEME}://fixture.local/events")
}

pub(crate) fn destination_uri_with_userinfo() -> String {
    format!("{SCHEME}://operator:{SECRET_SENTINEL}@fixture.local/events")
}

pub(crate) fn secret_sentinel() -> &'static str {
    SECRET_SENTINEL
}

struct FourthDestinationDriver {
    state: FourthDestinationState,
}

impl DestinationDriver for FourthDestinationDriver {
    fn schemes(&self) -> &'static [&'static str] {
        &[SCHEME]
    }

    fn inspect(
        &self,
        uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        validate_uri(uri)?;
        self.state.inner.inspections.fetch_add(1, Ordering::SeqCst);
        let protocol = FourthDestinationProtocol::new(self.state.clone());
        let sheet_artifact = protocol.sheet_artifact()?;
        Ok(DestinationInspection {
            description: description(),
            sheet_artifact_hash: cdf_runtime::artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: capabilities(),
            health_probes: vec![DestinationHealthProbe {
                probe_id: "fourth_ready".to_owned(),
                description: "fourth destination readiness".to_owned(),
                requires_credentials: true,
                mutates_destination: false,
            }],
        })
    }

    fn health(
        &self,
        uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<Vec<DestinationHealthResult>> {
        validate_uri(uri)?;
        self.state
            .inner
            .health_checks
            .fetch_add(1, Ordering::SeqCst);
        Ok(vec![DestinationHealthResult {
            probe_id: "fourth_ready".to_owned(),
            status: DestinationHealthStatus::Passed,
            message: "fourth destination is ready".to_owned(),
            details: BTreeMap::new(),
        }])
    }

    fn resolve(
        &self,
        uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>> {
        let secret = validate_uri(uri)?;
        self.state.inner.resolutions.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FourthDestinationRuntime {
            protocol: FourthDestinationProtocol::new(self.state.clone()),
            secret,
        }))
    }
}

fn validate_uri(uri: &str) -> Result<Option<String>> {
    let parsed = url::Url::parse(uri)
        .map_err(|error| CdfError::contract(format!("invalid fourth destination URI: {error}")))?;
    if parsed.scheme() != SCHEME {
        return Err(CdfError::contract(
            "fourth destination driver received another URI scheme",
        ));
    }
    Ok(parsed.password().map(str::to_owned))
}

fn description() -> DestinationDescription {
    DestinationDescription::new(
        DestinationId::new(SCHEME).unwrap(),
        &[SCHEME],
        "fourth registry fixture",
    )
}

#[derive(Clone)]
struct FourthDestinationProtocol {
    sheet: DestinationSheet,
    state: FourthDestinationState,
}

impl FourthDestinationProtocol {
    fn new(state: FourthDestinationState) -> Self {
        Self {
            sheet: DestinationSheet {
                destination: DestinationId::new(SCHEME).unwrap(),
                supported_dispositions: vec![WriteDisposition::Append],
                transactions: TransactionSupport::AtomicPackage,
                idempotency: IdempotencySupport::PackageToken,
                type_mappings: vec![TypeMapping {
                    arrow_type: "int64".to_owned(),
                    destination_type: "BIGINT".to_owned(),
                    fidelity: TypeMappingFidelity::Lossless,
                }],
                identifier_rules: IdentifierRules {
                    normalizer: "namecase-v1".to_owned(),
                    max_length: Some(63),
                    allowed_pattern: None,
                },
                migration_support: CapabilitySupport::Unsupported,
                quarantine_tables: CapabilitySupport::Unsupported,
                concurrency: ConcurrencyLimit {
                    max_writers: Some(1),
                },
            },
            state,
        }
    }
}

impl DestinationProtocol for FourthDestinationProtocol {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        if !self
            .sheet
            .supported_dispositions
            .contains(&request.disposition)
        {
            return Err(CdfError::contract(format!(
                "fourth destination does not support {:?}",
                request.disposition
            )));
        }
        self.state.inner.plans.fetch_add(1, Ordering::SeqCst);
        Ok(CommitPlan {
            plan_id: cdf_kernel::PlanId::new(format!(
                "fourth:{}:{}",
                request.target, request.idempotency_token
            ))?,
            target: request.target.clone(),
            disposition: request.disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: Vec::new(),
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
        })
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        self.state
            .inner
            .receipt_verifications
            .fetch_add(1, Ordering::SeqCst);
        let verified = self
            .state
            .receipt(receipt.idempotency_token.as_str())
            .as_ref()
            == Some(receipt);
        Ok(ReceiptVerification {
            verified,
            receipt_id: receipt.receipt_id.clone(),
            reason: (!verified).then(|| "fourth receipt is not durable".to_owned()),
        })
    }
}

struct FourthDestinationRuntime {
    protocol: FourthDestinationProtocol,
    secret: Option<String>,
}

impl DestinationRuntime for FourthDestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.protocol
    }

    fn ingress(&mut self) -> DestinationIngress<'_> {
        DestinationIngress::FinalizedPackage(self)
    }

    fn describe(&self) -> DestinationDescription {
        description()
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        capabilities()
    }

    fn secret_redaction(&self) -> Option<&str> {
        self.secret.as_deref()
    }
}

impl FinalizedPackageIngress for FourthDestinationRuntime {
    fn prepare_package_commit(
        &mut self,
        inputs: &cdf_package_contract::PackageReplayInputs,
        context: &cdf_runtime::DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        capabilities().validate_prepared_bulk_path(context.bulk_path)?;
        let duplicate = self
            .protocol
            .state
            .receipt(inputs.destination_commit.idempotency_token.as_str())
            .is_some();
        let plan = self.protocol.plan_commit(&inputs.destination_commit)?;
        PreparedDestinationCommit::from_verified_inputs(
            inputs,
            plan,
            context.bulk_path.clone(),
            cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate },
        )
    }

    fn begin_prepared_commit(
        &mut self,
        prepared: &mut PreparedDestinationCommit,
    ) -> Result<Box<dyn CommitSession + '_>> {
        if prepared.has_pending_context() {
            return Err(CdfError::internal(
                "fourth destination received unexpected pending commit context",
            ));
        }
        self.protocol
            .state
            .inner
            .commit_begins
            .fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FourthCommitSession {
            state: self.protocol.state.clone(),
            request: prepared.commit().clone(),
            plan: prepared.plan().clone(),
            schema_hash: prepared.schema_hash().clone(),
            duplicate: self
                .protocol
                .state
                .receipt(prepared.commit().idempotency_token.as_str()),
            migrations_applied: false,
            acknowledgements: Vec::new(),
        }))
    }
}

struct FourthCommitSession {
    state: FourthDestinationState,
    request: DestinationCommitRequest,
    plan: CommitPlan,
    schema_hash: SchemaHash,
    duplicate: Option<Receipt>,
    migrations_applied: bool,
    acknowledgements: Vec<SegmentAck>,
}

impl CommitSession for FourthCommitSession {
    fn apply_migrations(&mut self) -> Result<()> {
        self.migrations_applied = true;
        Ok(())
    }

    fn write_segments(
        &mut self,
        segments: cdf_kernel::CommitSegmentIterator,
    ) -> Result<Vec<SegmentAck>> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "fourth destination requires migration application before segment ingress",
            ));
        }
        let mut acknowledgements = Vec::new();
        for segment in segments {
            let segment = segment?;
            let expected = self
                .request
                .segments
                .iter()
                .find(|expected| expected.segment_id == segment.state.segment_id)
                .ok_or_else(|| CdfError::data("fourth destination received undeclared segment"))?;
            if expected != &segment.state {
                return Err(CdfError::data(
                    "fourth destination segment identity differs from commit authority",
                ));
            }
            let acknowledgement = SegmentAck {
                segment_id: expected.segment_id.clone(),
                row_count: expected.row_count,
                byte_count: expected.byte_count,
            };
            self.acknowledgements.push(acknowledgement.clone());
            acknowledgements.push(acknowledgement);
        }
        Ok(acknowledgements)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        if let Some(receipt) = self.duplicate {
            return Ok(receipt);
        }
        if self.acknowledgements.len() != self.request.segments.len() {
            return Err(CdfError::destination(
                "fourth destination did not acknowledge every segment",
            ));
        }
        let rows_written = self
            .acknowledgements
            .iter()
            .map(|acknowledgement| acknowledgement.row_count)
            .sum();
        let receipt = Receipt {
            receipt_id: ReceiptId::new(format!("fourth:{}", self.request.idempotency_token))?,
            destination: DestinationId::new(SCHEME)?,
            target: self.request.target.clone(),
            package_hash: self.request.package_hash.clone(),
            segment_acks: self.acknowledgements,
            disposition: self.request.disposition.clone(),
            idempotency_token: self.request.idempotency_token.clone(),
            transaction: None,
            counts: CommitCounts {
                rows_written,
                rows_inserted: Some(rows_written),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: self.schema_hash,
            migrations: self.plan.migrations,
            committed_at_ms: 1_700_000_000_000,
            verify: VerifyClause {
                kind: "fourth_receipt_v1".to_owned(),
                statement: "verify by package idempotency token".to_owned(),
                parameters: BTreeMap::from([(
                    "idempotency_token".to_owned(),
                    self.request.idempotency_token.to_string(),
                )]),
            },
        };
        self.state.record_receipt(receipt.clone());
        self.state
            .inner
            .durable_commits
            .fetch_add(1, Ordering::SeqCst);
        Ok(receipt)
    }

    fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

fn capabilities() -> DestinationRuntimeCapabilities {
    let path = BulkPathDescriptor {
        path_id: "fourth_native".to_owned(),
        version: 1,
        ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
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
        fallback: BulkFallbackMode::Forbidden,
        schema_preflight_version: "fourth-schema-v1".to_owned(),
        measured_evidence_version: Some("fourth-test-evidence-v1".to_owned()),
    };
    DestinationRuntimeCapabilities {
        commit_payload_mode: cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming,
        max_in_flight_segments: Some(1),
        max_in_flight_bytes: Some(64 * 1024 * 1024),
        bulk_paths: vec![path],
        bulk_path: Some("fourth_native".to_owned()),
        bulk_evidence_version: Some("fourth-test-evidence-v1".to_owned()),
        ..Default::default()
    }
}
