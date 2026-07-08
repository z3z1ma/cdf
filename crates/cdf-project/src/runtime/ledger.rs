use super::{hooks::RuntimeStage, prelude::*};
use cdf_contract::{AnomalyFact, ValidationDepth, ValidationTransitionTrigger};

pub(super) struct ValidationDepthTransitionRecord<'a> {
    pub(super) from_depth: ValidationDepth,
    pub(super) to_depth: ValidationDepth,
    pub(super) trigger: ValidationTransitionTrigger,
    pub(super) schema_hash: Option<&'a SchemaHash>,
    pub(super) previous_schema_hash: Option<&'a SchemaHash>,
    pub(super) anomaly: Option<&'a AnomalyFact>,
}

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

    pub(super) fn append_validation_depth_transition_recorded(
        &self,
        package_hash: &PackageHash,
        checkpoint_id: &CheckpointId,
        transition: ValidationDepthTransitionRecord<'_>,
    ) -> Result<()> {
        let mut event = self.base_event(RunEventKind::ValidationDepthTransitionRecorded);
        event.package_hash = Some(package_hash.clone());
        event.checkpoint_id = Some(checkpoint_id.clone());
        event.details = validation_depth_transition_details(transition);
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

fn validation_depth_transition_details(
    transition: ValidationDepthTransitionRecord<'_>,
) -> RunEventDetails {
    let mut attributes = BTreeMap::new();
    attributes.insert(
        "from_depth".to_owned(),
        RunEventValue::String(validation_depth_name(&transition.from_depth).to_owned()),
    );
    attributes.insert(
        "to_depth".to_owned(),
        RunEventValue::String(validation_depth_name(&transition.to_depth).to_owned()),
    );
    if let Some(clean_runs_required) = sampled_fast_path_clean_runs(&transition.from_depth)
        .or_else(|| sampled_fast_path_clean_runs(&transition.to_depth))
    {
        attributes.insert(
            "clean_runs_required".to_owned(),
            RunEventValue::U64(u64::from(clean_runs_required)),
        );
    }
    attributes.insert(
        "trigger".to_owned(),
        RunEventValue::String(validation_transition_trigger_name(&transition.trigger).to_owned()),
    );
    if let ValidationTransitionTrigger::CleanStableRuns { count } = transition.trigger {
        attributes.insert(
            "clean_run_count".to_owned(),
            RunEventValue::U64(u64::from(count)),
        );
    }
    if let Some(schema_hash) = transition.schema_hash {
        attributes.insert(
            "schema_hash".to_owned(),
            RunEventValue::String(schema_hash.as_str().to_owned()),
        );
    }
    if let Some(previous_schema_hash) = transition.previous_schema_hash {
        attributes.insert(
            "previous_schema_hash".to_owned(),
            RunEventValue::String(previous_schema_hash.as_str().to_owned()),
        );
    }
    if let Some(anomaly) = transition.anomaly {
        attributes.insert(
            "metric".to_owned(),
            RunEventValue::String(anomaly.metric.clone()),
        );
        attributes.insert(
            "observed".to_owned(),
            RunEventValue::String(anomaly.observed.clone()),
        );
        attributes.insert(
            "threshold".to_owned(),
            RunEventValue::String(anomaly.threshold.clone()),
        );
        attributes.insert(
            "window".to_owned(),
            RunEventValue::String(anomaly.window.clone()),
        );
    }
    RunEventDetails { attributes }
}

fn sampled_fast_path_clean_runs(depth: &ValidationDepth) -> Option<u32> {
    match depth {
        ValidationDepth::SampledFastPath {
            clean_runs_required,
        } => Some(*clean_runs_required),
        ValidationDepth::Discovery | ValidationDepth::Full | ValidationDepth::Sampled => None,
    }
}

fn validation_depth_name(depth: &ValidationDepth) -> &'static str {
    match depth {
        ValidationDepth::Discovery => "discovery",
        ValidationDepth::Full => "full",
        ValidationDepth::Sampled => "sampled",
        ValidationDepth::SampledFastPath { .. } => "sampled_fast_path",
    }
}

fn validation_transition_trigger_name(trigger: &ValidationTransitionTrigger) -> &'static str {
    match trigger {
        ValidationTransitionTrigger::NewResource => "new_resource",
        ValidationTransitionTrigger::CleanStableRuns { .. } => "clean_stable_runs",
        ValidationTransitionTrigger::Drift => "drift",
        ValidationTransitionTrigger::AnomalySpike => "anomaly_spike",
        ValidationTransitionTrigger::QuarantineEvent => "quarantine_event",
        ValidationTransitionTrigger::Manual => "manual",
    }
}
