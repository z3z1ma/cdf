use crate::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DestinationReceiptReportingPolicy {
    DestinationCommit { duplicate: bool },
    DestinationCommitReceiptOnly,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationCommitOutcome {
    pub receipt: Receipt,
    pub reporting_policy: DestinationReceiptReportingPolicy,
}

impl DestinationCommitOutcome {
    pub fn new(receipt: Receipt, reporting_policy: DestinationReceiptReportingPolicy) -> Self {
        Self {
            receipt,
            reporting_policy,
        }
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

#[derive(Clone, Debug)]
pub struct DestinationOutputSchema {
    pub schema: SchemaRef,
    pub schema_hash: SchemaHash,
    pub identifier_policy: Option<IdentifierPolicy>,
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

pub trait StagedSegmentIngress {
    fn begin_staged_ingress(
        &mut self,
        request: StagedIngressRequest,
    ) -> Result<Box<dyn StagedIngressSession>>;

    fn inspect_staged_ingress(
        &mut self,
        attempt_id: &LoadAttemptId,
    ) -> Result<Option<StagingSnapshot>>;
}

pub trait DestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol;

    fn ingress(&mut self) -> DestinationIngress<'_>;

    fn destination_sheet(&self) -> Result<DestinationSheet> {
        Ok(self.protocol().sheet().clone())
    }

    fn describe(&self) -> DestinationDescription;

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        DestinationRuntimeCapabilities::default()
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

    fn secret_redaction(&self) -> Option<&str> {
        None
    }
}

pub struct ResolvedDestination {
    target: TargetName,
    runtime: Box<dyn DestinationRuntime>,
}

impl std::fmt::Debug for ResolvedDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedDestination")
            .field("target", &self.target)
            .field("description", &self.describe())
            .finish_non_exhaustive()
    }
}

impl ResolvedDestination {
    pub fn new(runtime: Box<dyn DestinationRuntime>, target: TargetName) -> Self {
        Self { target, runtime }
    }

    pub fn target(&self) -> &TargetName {
        &self.target
    }

    pub fn column_identifier_policy(&self) -> Result<Option<IdentifierPolicy>> {
        let sheet = self.runtime.destination_sheet()?;
        identifier_policy_from_destination_rules(&sheet.identifier_rules).map(Some)
    }

    pub fn describe(&self) -> DestinationDescription {
        self.runtime.describe()
    }

    pub fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        self.runtime.runtime_capabilities()
    }

    pub fn secret_redaction(&self) -> Option<&str> {
        self.runtime.secret_redaction()
    }

    pub fn runtime_mut(&mut self) -> &mut dyn DestinationRuntime {
        self.runtime.as_mut()
    }
}

pub fn destination_output_schema(
    destination: &ResolvedDestination,
    schema: SchemaRef,
    schema_hash: SchemaHash,
    planned_identifier_policy: &IdentifierPolicy,
) -> Result<DestinationOutputSchema> {
    let identifier_policy = destination.column_identifier_policy()?;
    if let Some(identifier_policy) = &identifier_policy
        && planned_identifier_policy != identifier_policy
    {
        return Err(CdfError::contract(format!(
            "run plan identifier policy does not match resolved destination sheet: planned {planned_identifier_policy:?}, destination {identifier_policy:?}; rebuild the plan for the selected destination"
        )));
    }
    Ok(DestinationOutputSchema {
        schema,
        schema_hash,
        identifier_policy,
    })
}
