use std::{any::Any, collections::BTreeMap};

use arrow_schema::Schema;
use cdf_kernel::{
    CapabilitySupport, CdfError, CommitPlan, CommitSegmentIterator, CommitSession,
    DestinationCommitRequest, DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest,
    DestinationProtocol, DestinationSheet, Receipt, ReceiptVerification, ResourceStream, Result,
    SchemaHash, StateDelta, TargetName, WriteDisposition,
};
use cdf_package_contract::{PackageReplayInputs, SharedVerifiedPackageAccess};

use crate::{
    bulk::{BulkPathPreparation, BulkPathPreparationInput, PreparedBulkPath},
    capabilities::{DestinationDescription, DestinationRuntimeCapabilities},
    capability_types::DestinationIngressMode,
    staging::{
        StagedIngressRequest, StagedIngressSession, StagingCleanupCandidate, StagingSnapshot,
    },
    staging_identity::LoadAttemptId,
    staging_lease::ExpiredStagingLeaseProof,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DestinationReceiptReportingPolicy {
    DestinationCommit { duplicate: bool },
    DestinationCommitReceiptOnly,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DestinationCommitVerification {
    Independent,
    VerifiedAtCommit(ReceiptVerification),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationCommitOutcome {
    pub receipt: Receipt,
    pub reporting_policy: DestinationReceiptReportingPolicy,
    pub verification: DestinationCommitVerification,
}

impl DestinationCommitOutcome {
    pub fn new(receipt: Receipt, reporting_policy: DestinationReceiptReportingPolicy) -> Self {
        Self {
            receipt,
            reporting_policy,
            verification: DestinationCommitVerification::Independent,
        }
    }

    pub fn with_commit_verification(mut self, verification: ReceiptVerification) -> Result<Self> {
        if !verification.verified || verification.receipt_id != self.receipt.receipt_id {
            return Err(CdfError::contract(
                "commit-bound destination verification must verify the exact returned receipt",
            ));
        }
        self.verification = DestinationCommitVerification::VerifiedAtCommit(verification);
        Ok(self)
    }
}

pub struct PreparedDestinationCommit {
    commit: DestinationCommitRequest,
    schema_hash: SchemaHash,
    plan: CommitPlan,
    bulk_path: PreparedBulkPath,
    reporting_policy: DestinationReceiptReportingPolicy,
    pending_context: Option<Box<dyn Any + Send + Sync>>,
}

impl PreparedDestinationCommit {
    pub fn from_verified_inputs(
        inputs: &PackageReplayInputs,
        plan: CommitPlan,
        bulk_path: PreparedBulkPath,
        reporting_policy: DestinationReceiptReportingPolicy,
    ) -> Result<Self> {
        let prepared = Self {
            commit: inputs.destination_commit.clone(),
            schema_hash: inputs.schema_hash.clone(),
            plan,
            bulk_path,
            reporting_policy,
            pending_context: None,
        };
        prepared.validate_verified_inputs(inputs)?;
        Ok(prepared)
    }

    pub fn commit(&self) -> &DestinationCommitRequest {
        &self.commit
    }

    pub fn schema_hash(&self) -> &SchemaHash {
        &self.schema_hash
    }

    pub fn plan(&self) -> &CommitPlan {
        &self.plan
    }

    pub fn bulk_path(&self) -> &PreparedBulkPath {
        &self.bulk_path
    }

    pub fn reporting_policy(&self) -> &DestinationReceiptReportingPolicy {
        &self.reporting_policy
    }

    pub fn validate_verified_inputs(&self, inputs: &PackageReplayInputs) -> Result<()> {
        if self.commit != inputs.destination_commit {
            return Err(CdfError::contract(
                "prepared destination commit does not match verified package commit authority",
            ));
        }
        if self.schema_hash != inputs.schema_hash {
            return Err(CdfError::contract(
                "prepared destination schema does not match verified package schema authority",
            ));
        }
        if self.plan.target != self.commit.target
            || self.plan.disposition != self.commit.disposition
        {
            return Err(CdfError::contract(
                "prepared destination plan target/disposition does not match verified package commit authority",
            ));
        }
        Ok(())
    }

    pub fn with_pending_context(
        mut self,
        pending_context: impl Any + Send + Sync + 'static,
    ) -> Self {
        self.pending_context = Some(Box::new(pending_context));
        self
    }

    pub fn take_pending_context<T>(&mut self, label: &str) -> Result<T>
    where
        T: Any + Send + Sync + 'static,
    {
        let pending = self.pending_context.take().ok_or_else(|| {
            CdfError::internal(format!(
                "prepared destination commit is missing {label} pending context"
            ))
        })?;
        pending.downcast::<T>().map(|boxed| *boxed).map_err(|_| {
            CdfError::internal(format!(
                "prepared destination commit pending context did not match {label}"
            ))
        })
    }

    pub fn has_pending_context(&self) -> bool {
        self.pending_context.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationCommitPlanningInputs {
    pub state_delta: StateDelta,
    pub destination_commit: DestinationCommitRequest,
    pub schema_hash: SchemaHash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationCommitPlanningOutcome {
    pub sheet: DestinationSheet,
    pub plan: CommitPlan,
}

impl DestinationCommitPlanningOutcome {
    pub fn new(sheet: DestinationSheet, plan: CommitPlan) -> Self {
        Self { sheet, plan }
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub struct DestinationPlanningContext<'a> {
    pub verified_package: SharedVerifiedPackageAccess,
    pub bulk_path: &'a PreparedBulkPath,
}

impl<'a> DestinationPlanningContext<'a> {
    pub fn new(
        verified_package: SharedVerifiedPackageAccess,
        bulk_path: &'a PreparedBulkPath,
    ) -> Self {
        Self {
            verified_package,
            bulk_path,
        }
    }
}

/// The one prepared ingress category exposed by a resolved destination runtime.
///
/// Generic orchestration selects this variant from declared capabilities, never destination
/// identity. A capability/variant mismatch fails before mutation.
pub enum DestinationIngress<'a> {
    FinalizedPackage(&'a mut dyn FinalizedPackageIngress),
    StagedSegments(&'a mut dyn StagedSegmentIngress),
}

impl DestinationIngress<'_> {
    pub fn mode(&self) -> DestinationIngressMode {
        match self {
            Self::FinalizedPackage(_) => DestinationIngressMode::FinalizedPackageOnly,
            Self::StagedSegments(_) => DestinationIngressMode::StagedDurableSegments,
        }
    }
}

/// Borrowed, serial ingress over a verified finalized package.
///
/// The returned [`CommitSession`] borrows its exclusively owned runtime and is intentionally not
/// required to cross threads. An adapter may schedule bounded internal work only through injected
/// execution services while keeping thread-affine native handles confined by the serial ingress
/// protocol or their declared lane.
pub trait FinalizedPackageIngress {
    fn prepare_package_commit(
        &mut self,
        inputs: &PackageReplayInputs,
        context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit>;

    fn begin_prepared_commit(
        &mut self,
        prepared: &mut PreparedDestinationCommit,
    ) -> Result<Box<dyn CommitSession + '_>>;
}

/// Factory for owned staged-segment sessions.
///
/// The returned [`StagedIngressSession`] is `Send` because orchestration may move durable segment
/// delivery between admitted tasks. That movement grants no private executor or unbounded
/// concurrency: sheet declarations, memory authority, stage pressure, and injected host services
/// remain controlling.
pub trait StagedSegmentIngress {
    fn begin_staged_ingress(
        &mut self,
        request: StagedIngressRequest,
    ) -> Result<Box<dyn StagedIngressSession>>;

    fn inspect_staged_ingress(
        &mut self,
        attempt_id: &LoadAttemptId,
    ) -> Result<Option<StagingSnapshot>>;

    fn staging_cleanup_candidates(
        &mut self,
        _target: &TargetName,
    ) -> Result<Vec<StagingCleanupCandidate>> {
        Ok(Vec::new())
    }

    fn cleanup_expired_staging(
        &mut self,
        _candidate: &StagingCleanupCandidate,
        _proof: &ExpiredStagingLeaseProof,
        _mutation_guard: &crate::StagingMutationGuard,
    ) -> Result<u64> {
        Err(CdfError::contract(
            "destination returned a staging cleanup candidate without implementing proof-gated cleanup",
        ))
    }
}

/// Exclusively owned destination state for one logical run.
///
/// A runtime is deliberately not required to be `Send` or `Sync`; it may own thread-affine native
/// handles when its ingress protocol or declared blocking lane confines them safely. It exposes
/// exactly one ingress category matching [`DestinationRuntimeCapabilities::ingress_mode`] and
/// receives concurrency, cancellation, memory, spill, and timing authority through injected
/// execution services. Host thread safety does not make a native handle movable, and the runtime
/// must not create a competing async runtime or unaccounted worker pool.
pub trait DestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol;

    fn ingress(&mut self) -> DestinationIngress<'_>;

    fn bind_execution_services(&mut self, _execution: &crate::ExecutionServices) -> Result<()> {
        Ok(())
    }

    fn destination_sheet(&self) -> Result<DestinationSheet> {
        Ok(self.protocol().sheet().clone())
    }

    fn destination_sheet_artifact(&self) -> Result<cdf_kernel::DestinationSheetArtifact> {
        self.protocol().sheet_artifact()
    }

    fn describe(&self) -> DestinationDescription;

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        DestinationRuntimeCapabilities::default()
    }

    /// Adapter-owned semantic policy that must participate in finalized package identity.
    fn commit_policy(&self, _disposition: &WriteDisposition) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    fn prepare_bulk_paths(
        &mut self,
        _input: &BulkPathPreparationInput<'_>,
    ) -> Result<BulkPathPreparation> {
        BulkPathPreparation::from_capabilities(&self.runtime_capabilities())
    }

    fn prepare_selected_bulk_path(
        &mut self,
        input: &BulkPathPreparationInput<'_>,
    ) -> Result<PreparedBulkPath> {
        let capabilities = self.runtime_capabilities();
        self.prepare_bulk_paths(input)?.into_selected(&capabilities)
    }

    fn supported_dispositions(&self) -> &[WriteDisposition] {
        &self.protocol().sheet().supported_dispositions
    }

    fn quarantine_table_support(&self) -> CapabilitySupport {
        self.protocol().sheet().quarantine_tables.clone()
    }

    fn validate_run_preflight(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        _schema_hash: &SchemaHash,
    ) -> Result<()> {
        Ok(())
    }

    fn plan_resource_commit(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let plan = self.protocol().plan_commit(&inputs.destination_commit)?;
        Ok(DestinationCommitPlanningOutcome::new(
            self.protocol().sheet().clone(),
            plan,
        ))
    }

    /// Plans an already verified package when no live source resource remains available.
    ///
    /// Destinations whose migrations depend on the canonical output schema must override this
    /// path so replay and staged final binding carry the same typed migration authority as live
    /// resource planning.
    fn plan_verified_package_commit(
        &mut self,
        _output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let plan = self.protocol().plan_commit(&inputs.destination_commit)?;
        Ok(DestinationCommitPlanningOutcome::new(
            self.protocol().sheet().clone(),
            plan,
        ))
    }

    fn prepare_correction_commit(
        &mut self,
        _package: SharedVerifiedPackageAccess,
        request: &DestinationCorrectionCommitRequest,
    ) -> Result<DestinationCorrectionCommitPlan> {
        self.ensure_protocol_ready()?;
        self.protocol().plan_correction(request)
    }

    fn ensure_protocol_ready(&mut self) -> Result<()> {
        Ok(())
    }

    fn verify_receipt(&mut self, receipt: &Receipt) -> Result<ReceiptVerification> {
        self.ensure_protocol_ready()?;
        self.protocol().verify(receipt)
    }

    /// Atomically applies a manifest-bound routed target family.
    ///
    /// This path is separate from single-target ingress because one package may contain multiple
    /// Arrow schemas and every physical target must share one destination transaction.
    fn plan_routed_package(&mut self, _commit: &DestinationCommitRequest) -> Result<CommitPlan> {
        Err(CdfError::destination(format!(
            "destination {} does not support routed target-family planning",
            self.describe().destination_id
        )))
    }

    fn commit_routed_package(
        &mut self,
        _inputs: &PackageReplayInputs,
        _segments: CommitSegmentIterator,
    ) -> Result<DestinationCommitOutcome> {
        Err(CdfError::destination(format!(
            "destination {} does not support atomic routed target families",
            self.describe().destination_id
        )))
    }

    /// Atomically applies one finalized single-target CDC package.
    ///
    /// Destinations whose ordinary bulk path is row-oriented may override this boundary rather
    /// than pretending key-only delete segments are row batches. The default remains closed.
    fn commit_cdc_package(
        &mut self,
        _inputs: &PackageReplayInputs,
        _output_schema: &Schema,
        _segments: CommitSegmentIterator,
    ) -> Result<DestinationCommitOutcome> {
        Err(CdfError::destination(format!(
            "destination {} does not support finalized single-target CDC application",
            self.describe().destination_id
        )))
    }

    fn secret_redaction(&self) -> Option<&str> {
        None
    }
}
