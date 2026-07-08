use super::{hooks::RuntimeStage, prelude::*};

pub(super) struct ProjectRunRecorderContext {
    pub(super) resource_id: ResourceId,
    pub(super) scope: ScopeKey,
    pub(super) package_id: String,
    pub(super) package_path: String,
    pub(super) destination_id: DestinationId,
    pub(super) plan_id: PlanId,
    pub(super) pipeline_id: PipelineId,
}

pub(super) struct ProjectRunRecorder<'a> {
    pub(super) ledger: &'a SqliteRunLedger,
    pub(super) run_id: RunId,
    pub(super) context: ProjectRunRecorderContext,
}

impl<'a> ProjectRunRecorder<'a> {
    pub(super) fn new(
        ledger: &'a SqliteRunLedger,
        run_id: RunId,
        context: ProjectRunRecorderContext,
    ) -> Self {
        Self {
            ledger,
            run_id,
            context,
        }
    }

    pub(super) fn append_run_started(&self) -> Result<()> {
        let mut event = self.base_event(RunEventKind::RunStarted);
        event.details = RunEventDetails::new([(
            "pipeline_id",
            RunEventValue::String(self.context.pipeline_id.as_str().to_owned()),
        )]);
        self.append(event)
    }

    pub(super) fn append_plan_recorded(&self) -> Result<()> {
        let mut event = self.base_event(RunEventKind::PlanRecorded);
        event.details = RunEventDetails::new([("planned_packages", RunEventValue::U64(1))]);
        self.append(event)
    }

    pub(super) fn append_package_started(&self) -> Result<()> {
        self.append(self.base_event(RunEventKind::PackageStarted))
    }

    pub(super) fn append_package_finalized(
        &self,
        package_hash: &PackageHash,
        row_count: u64,
        segment_count: usize,
    ) -> Result<()> {
        let mut event = self.base_event(RunEventKind::PackageFinalized);
        event.package_hash = Some(package_hash.clone());
        event.details = RunEventDetails::new([
            ("row_count", RunEventValue::U64(row_count)),
            (
                "segment_count",
                RunEventValue::U64(
                    u64::try_from(segment_count)
                        .map_err(|error| CdfError::internal(error.to_string()))?,
                ),
            ),
        ]);
        self.append(event)
    }

    pub(super) fn append_replay_stage(&self, stage: RuntimeStage<'_>) -> Result<()> {
        match stage {
            RuntimeStage::PackageReplayVerified | RuntimeStage::DestinationWriteReady => Ok(()),
            RuntimeStage::CheckpointProposed { delta } => {
                let mut event = self.base_event(RunEventKind::CheckpointProposed);
                event.checkpoint_id = Some(delta.checkpoint_id.clone());
                event.package_hash = Some(delta.package_hash.clone());
                self.append(event)
            }
            RuntimeStage::DestinationCommitStarted { plan_id } => {
                let mut event = self.base_event(RunEventKind::DestinationCommitStarted);
                event.plan_id = Some(plan_id.clone());
                self.append(event)
            }
            RuntimeStage::DestinationReceiptRecorded { receipt } => {
                let mut event = self.base_event(RunEventKind::DestinationReceiptRecorded);
                event.package_hash = Some(receipt.package_hash.clone());
                event.receipt_id = Some(receipt.receipt_id.clone());
                event.destination_id = Some(receipt.destination.clone());
                self.append(event)
            }
            RuntimeStage::CheckpointCommitted { checkpoint } => {
                let mut event = self.base_event(RunEventKind::CheckpointCommitted);
                event.checkpoint_id = Some(checkpoint.delta.checkpoint_id.clone());
                event.package_hash = Some(checkpoint.delta.package_hash.clone());
                event.receipt_id = checkpoint
                    .receipt
                    .as_ref()
                    .map(|receipt| receipt.receipt_id.clone());
                self.append(event)
            }
            RuntimeStage::PackageStatusUpdated { status } => {
                let mut event = self.base_event(RunEventKind::PackageStatusUpdated);
                event.details = RunEventDetails::new([(
                    "status",
                    RunEventValue::String(status.as_str().to_owned()),
                )]);
                self.append(event)
            }
        }
    }

    pub(super) fn append_run_succeeded(&self) -> Result<()> {
        self.append(self.base_event(RunEventKind::RunSucceeded))
    }

    pub(super) fn append_run_failed(&self) -> Result<()> {
        self.append(self.base_event(RunEventKind::RunFailed))
    }

    pub(super) fn snapshot(&self) -> Result<RunLedgerSnapshot> {
        self.ledger.snapshot(&self.run_id)?.ok_or_else(|| {
            CdfError::internal(format!(
                "run {} disappeared from the run ledger",
                self.run_id
            ))
        })
    }

    fn base_event(&self, kind: RunEventKind) -> RunEventAppend {
        let mut event = RunEventAppend::new(kind);
        event.resource_id = Some(self.context.resource_id.clone());
        event.scope = Some(self.context.scope.clone());
        event.partition_id = partition_id_for_scope(&self.context.scope);
        event.package_id = Some(self.context.package_id.clone());
        event.package_path = Some(self.context.package_path.clone());
        event.destination_id = Some(self.context.destination_id.clone());
        event.plan_id = Some(self.context.plan_id.clone());
        event
    }

    fn append(&self, event: RunEventAppend) -> Result<()> {
        self.ledger.append_event(&self.run_id, event)?;
        Ok(())
    }
}

fn partition_id_for_scope(scope: &ScopeKey) -> Option<cdf_kernel::PartitionId> {
    match scope {
        ScopeKey::Partition { partition_id } => Some(partition_id.clone()),
        _ => None,
    }
}
