use super::types::RunTelemetryConfig;
use super::{hooks::RuntimeStage, prelude::*};
use cdf_contract::{AnomalyFact, ValidationDepth, ValidationTransitionTrigger};
use std::{collections::BTreeMap, sync::Mutex, time::Instant};

#[derive(Debug)]
struct ActivePhase {
    started_at: Instant,
    input_bytes: u64,
    output_bytes: u64,
    operations: u64,
}

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
    pub(super) events: RunEventFanout<'a>,
    pub(super) run_id: RunId,
    pub(super) context: ProjectRunRecorderContext,
    started_at: Instant,
    telemetry: RunTelemetryConfig,
    active_phases: Mutex<BTreeMap<RunPhase, ActivePhase>>,
    phase_event_count: Mutex<u16>,
}

impl<'a> ProjectRunRecorder<'a> {
    pub(super) fn new(
        ledger: &'a SqliteRunLedger,
        run_id: RunId,
        context: ProjectRunRecorderContext,
        event_sink: Option<&'a dyn RunEventSink>,
        telemetry: RunTelemetryConfig,
    ) -> Self {
        Self {
            events: RunEventFanout::new(ledger, event_sink),
            run_id,
            context,
            started_at: Instant::now(),
            telemetry,
            active_phases: Mutex::new(BTreeMap::new()),
            phase_event_count: Mutex::new(0),
        }
    }

    pub(super) fn append_run_started(&self) -> Result<()> {
        let mut event = self.base_event(RunEventKind::RunStarted);
        let mut details = details_for_phase("run");
        details.insert(
            "pipeline_id".to_owned(),
            RunEventValue::String(self.context.pipeline_id.as_str().to_owned()),
        );
        event.details = RunEventDetails {
            attributes: details,
        };
        self.append(event)
    }

    pub(super) fn append_plan_recorded(&self, planned_packages: u64) -> Result<()> {
        let mut event = self.base_event(RunEventKind::PlanRecorded);
        let mut details = details_for_phase("planning");
        details.insert(
            "planned_packages".to_owned(),
            RunEventValue::U64(planned_packages),
        );
        event.details = RunEventDetails {
            attributes: details,
        };
        self.append(event)
    }

    pub(super) fn append_package_started(&self) -> Result<()> {
        let mut event = self.base_event(RunEventKind::PackageStarted);
        event.details = RunEventDetails {
            attributes: details_for_phase("package"),
        };
        self.append(event)?;
        self.start_phase(RunPhase::PackageExecution)
    }

    pub(super) fn append_package_segment_recorded(
        &self,
        segment: &SegmentEntry,
        segment_index: u64,
        segment_count: u64,
    ) -> Result<()> {
        let mut event = self.base_event(RunEventKind::PackageSegmentRecorded);
        let mut details = details_for_phase("package");
        details.insert(
            "segment_id".to_owned(),
            RunEventValue::String(segment.segment_id.as_str().to_owned()),
        );
        details.insert(
            "row_count".to_owned(),
            RunEventValue::U64(segment.row_count),
        );
        details.insert(
            "byte_count".to_owned(),
            RunEventValue::U64(segment.byte_count),
        );
        details.insert(
            "segment_index".to_owned(),
            RunEventValue::U64(segment_index),
        );
        details.insert(
            "segment_count".to_owned(),
            RunEventValue::U64(segment_count),
        );
        event.details = RunEventDetails {
            attributes: details,
        };
        self.append(event)
    }

    pub(super) fn append_package_finalized(
        &self,
        package_hash: &PackageHash,
        row_count: u64,
        byte_count: u64,
        batch_count: u64,
        segment_count: u64,
        quarantine_record_count: u64,
    ) -> Result<()> {
        let mut event = self.base_event(RunEventKind::PackageFinalized);
        event.package_hash = Some(package_hash.clone());
        let mut details = details_for_phase("package");
        details.insert("row_count".to_owned(), RunEventValue::U64(row_count));
        details.insert("byte_count".to_owned(), RunEventValue::U64(byte_count));
        details.insert("batch_count".to_owned(), RunEventValue::U64(batch_count));
        details.insert(
            "segment_count".to_owned(),
            RunEventValue::U64(segment_count),
        );
        details.insert(
            "quarantine_record_count".to_owned(),
            RunEventValue::U64(quarantine_record_count),
        );
        event.details = RunEventDetails {
            attributes: details,
        };
        self.append(event)
    }

    pub(super) fn append_source_retries(
        &self,
        retries: &[SourceRetryEvidence],
        schedule: Option<&cdf_runtime::CanonicalPartitionSchedule>,
    ) -> Result<()> {
        for retry in retries {
            let schedule = schedule.ok_or_else(|| {
                CdfError::internal("source retry event requires its compiled partition schedule")
            })?;
            retry.validate_against_schedule(schedule)?;
            let mut event = self.base_event(RunEventKind::SourceRetryRecorded);
            let mut details = details_for_phase("source");
            details.insert(
                "plan_id".to_owned(),
                RunEventValue::String(retry.plan_id().to_owned()),
            );
            details.insert(
                "partition_ordinal".to_owned(),
                RunEventValue::U64(retry.partition_ordinal()),
            );
            details.insert(
                "partition_id".to_owned(),
                RunEventValue::String(retry.partition_id().to_owned()),
            );
            details.insert(
                "immutable_identity_hash".to_owned(),
                RunEventValue::String(retry.immutable_identity_hash().to_owned()),
            );
            details.insert(
                "partition_binding_hash".to_owned(),
                RunEventValue::String(retry.partition_binding_hash().to_owned()),
            );
            details.insert(
                "compiled_retry".to_owned(),
                compiled_source_retry_value(retry.compiled_retry()),
            );
            details.insert(
                "history".to_owned(),
                source_retry_history_value(retry.history()),
            );
            event.details = RunEventDetails {
                attributes: details,
            };
            self.append(event)?;
        }
        Ok(())
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
                event.details = checkpoint_delta_details("checkpoint", delta)?;
                self.append(event)
            }
            RuntimeStage::DestinationCommitStarted {
                plan_id,
                segment_count,
                bulk_path,
            } => {
                let mut event = self.base_event(RunEventKind::DestinationCommitStarted);
                event.plan_id = Some(plan_id.clone());
                let mut details = details_for_phase("destination");
                details.insert(
                    "segment_count".to_owned(),
                    RunEventValue::U64(segment_count),
                );
                details.insert(
                    "bulk_path_id".to_owned(),
                    RunEventValue::String(bulk_path.descriptor.path_id.clone()),
                );
                details.insert(
                    "bulk_path_version".to_owned(),
                    RunEventValue::U64(u64::from(bulk_path.descriptor.version)),
                );
                details.insert(
                    "bulk_rows_per_batch".to_owned(),
                    RunEventValue::U64(bulk_path.rows_per_batch),
                );
                details.insert(
                    "bulk_bytes_per_batch".to_owned(),
                    RunEventValue::U64(bulk_path.bytes_per_batch),
                );
                details.insert(
                    "bulk_writers".to_owned(),
                    RunEventValue::U64(u64::from(bulk_path.writers)),
                );
                details.insert(
                    "bulk_evidence_version".to_owned(),
                    RunEventValue::String(
                        bulk_path
                            .descriptor
                            .measured_evidence_version
                            .clone()
                            .expect("validated prepared bulk path"),
                    ),
                );
                event.details = RunEventDetails {
                    attributes: details,
                };
                self.append(event)?;
                self.start_phase(RunPhase::DestinationWriteReceipt)
            }
            RuntimeStage::DestinationSegmentAcknowledged { ack } => {
                let mut event = self.base_event(RunEventKind::DestinationSegmentAcknowledged);
                let mut details = details_for_phase("destination");
                details.insert(
                    "segment_id".to_owned(),
                    RunEventValue::String(ack.segment_id.as_str().to_owned()),
                );
                details.insert("row_count".to_owned(), RunEventValue::U64(ack.row_count));
                details.insert("byte_count".to_owned(), RunEventValue::U64(ack.byte_count));
                event.details = RunEventDetails {
                    attributes: details,
                };
                self.append(event)?;
                self.add_phase_work(
                    RunPhase::DestinationWriteReceipt,
                    ack.byte_count,
                    ack.byte_count,
                    1,
                )
            }
            RuntimeStage::DestinationReceiptRecorded { receipt } => {
                let mut event = self.base_event(RunEventKind::DestinationReceiptRecorded);
                event.package_hash = Some(receipt.package_hash.clone());
                event.receipt_id = Some(receipt.receipt_id.clone());
                event.destination_id = Some(receipt.destination.clone());
                event.details = receipt_details(receipt)?;
                self.append(event)?;
                self.complete_phase(RunPhase::DestinationWriteReceipt, 0, 0, 0)?;
                self.start_phase(RunPhase::CheckpointGate)
            }
            RuntimeStage::CheckpointCommitted { checkpoint } => {
                let mut event = self.base_event(RunEventKind::CheckpointCommitted);
                event.checkpoint_id = Some(checkpoint.delta.checkpoint_id.clone());
                event.package_hash = Some(checkpoint.delta.package_hash.clone());
                event.receipt_id = checkpoint
                    .receipt
                    .as_ref()
                    .map(|receipt| receipt.receipt_id.clone());
                event.details = checkpoint_details(checkpoint)?;
                self.append(event)?;
                self.complete_phase(RunPhase::CheckpointGate, 0, 0, 1)
            }
            RuntimeStage::PackageStatusUpdated { status } => {
                let mut event = self.base_event(RunEventKind::PackageStatusUpdated);
                let mut details = details_for_phase("package");
                details.insert(
                    "status".to_owned(),
                    RunEventValue::String(status.as_str().to_owned()),
                );
                event.details = RunEventDetails {
                    attributes: details,
                };
                self.append(event)
            }
        }
    }

    pub(super) fn append_run_succeeded(&self) -> Result<()> {
        let mut event = self.base_event(RunEventKind::RunSucceeded);
        event.details = self.run_terminal_details("run", None)?;
        self.append(event)
    }

    pub(super) fn append_run_failed(&self, error: &CdfError) -> Result<()> {
        self.fail_active_phases()?;
        let mut event = self.base_event(RunEventKind::RunFailed);
        event.details = self.run_terminal_details("run", Some(error))?;
        self.append(event)
    }

    pub(super) fn phase_telemetry_enabled(&self) -> bool {
        self.telemetry.phase_metrics && self.telemetry.max_phase_events > 0
    }

    pub(super) fn append_phase_metric(&self, metric: RunPhaseMetric) -> Result<()> {
        if !self.phase_telemetry_enabled() {
            return Ok(());
        }
        let mut count = self
            .phase_event_count
            .lock()
            .map_err(|_| CdfError::internal("phase event counter lock poisoned"))?;
        if *count >= self.telemetry.max_phase_events {
            return Ok(());
        }
        *count = count.saturating_add(1);
        drop(count);

        let mut event = self.base_event(RunEventKind::PhaseMeasured);
        let mut attributes = details_for_phase(metric.phase.as_str());
        attributes.insert("metric".to_owned(), RunEventValue::PhaseMetric(metric));
        event.details = RunEventDetails { attributes };
        self.append(event)
    }

    pub(super) fn complete_phase(
        &self,
        phase: RunPhase,
        input_bytes: u64,
        output_bytes: u64,
        operations: u64,
    ) -> Result<()> {
        if !self.phase_telemetry_enabled() {
            return Ok(());
        }
        let active = self
            .active_phases
            .lock()
            .map_err(|_| CdfError::internal("active phase lock poisoned"))?
            .remove(&phase);
        let Some(mut active) = active else {
            return Ok(());
        };
        active.input_bytes = active.input_bytes.saturating_add(input_bytes);
        active.output_bytes = active.output_bytes.saturating_add(output_bytes);
        active.operations = active.operations.saturating_add(operations);
        self.append_phase_metric(phase_metric(phase, RunPhaseStatus::Completed, active)?)
    }

    fn start_phase(&self, phase: RunPhase) -> Result<()> {
        if !self.phase_telemetry_enabled() {
            return Ok(());
        }
        self.active_phases
            .lock()
            .map_err(|_| CdfError::internal("active phase lock poisoned"))?
            .insert(
                phase,
                ActivePhase {
                    started_at: Instant::now(),
                    input_bytes: 0,
                    output_bytes: 0,
                    operations: 0,
                },
            );
        Ok(())
    }

    fn add_phase_work(
        &self,
        phase: RunPhase,
        input_bytes: u64,
        output_bytes: u64,
        operations: u64,
    ) -> Result<()> {
        if !self.phase_telemetry_enabled() {
            return Ok(());
        }
        if let Some(active) = self
            .active_phases
            .lock()
            .map_err(|_| CdfError::internal("active phase lock poisoned"))?
            .get_mut(&phase)
        {
            active.input_bytes = active.input_bytes.saturating_add(input_bytes);
            active.output_bytes = active.output_bytes.saturating_add(output_bytes);
            active.operations = active.operations.saturating_add(operations);
        }
        Ok(())
    }

    fn fail_active_phases(&self) -> Result<()> {
        if !self.phase_telemetry_enabled() {
            return Ok(());
        }
        let phases = std::mem::take(
            &mut *self
                .active_phases
                .lock()
                .map_err(|_| CdfError::internal("active phase lock poisoned"))?,
        );
        for (phase, active) in phases {
            self.append_phase_metric(phase_metric(phase, RunPhaseStatus::Failed, active)?)?;
        }
        Ok(())
    }

    pub(super) fn snapshot(&self) -> Result<RunLedgerSnapshot> {
        self.events.durable.snapshot(&self.run_id)?.ok_or_else(|| {
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
        self.events.publish(&self.run_id, event)?;
        Ok(())
    }

    fn run_terminal_details(
        &self,
        phase: &str,
        error: Option<&CdfError>,
    ) -> Result<RunEventDetails> {
        let mut details = details_for_phase(phase);
        details.insert(
            "elapsed_ms".to_owned(),
            RunEventValue::U64(
                u64::try_from(self.started_at.elapsed().as_millis()).map_err(|error| {
                    CdfError::internal(format!("run elapsed time overflow: {error}"))
                })?,
            ),
        );
        if let Some(error) = error {
            details.insert(
                "error_kind".to_owned(),
                RunEventValue::String(format!("{:?}", error.kind).to_ascii_lowercase()),
            );
            if let Some(retry_after_ms) = error.retry_after_ms {
                details.insert(
                    "retry_after_ms".to_owned(),
                    RunEventValue::U64(retry_after_ms),
                );
                details.insert("backoff_notice".to_owned(), RunEventValue::Bool(true));
            }
        }
        Ok(RunEventDetails {
            attributes: details,
        })
    }
}

fn compiled_source_retry_value(retry: &cdf_runtime::CompiledSourceRetry) -> RunEventValue {
    RunEventValue::Object(BTreeMap::from([
        (
            "granularity".to_owned(),
            RunEventValue::String(
                match retry.granularity {
                    cdf_runtime::SourceRetryGranularity::None => "none",
                    cdf_runtime::SourceRetryGranularity::Partition => "partition",
                    cdf_runtime::SourceRetryGranularity::Unit => "unit",
                }
                .to_owned(),
            ),
        ),
        (
            "retryable_errors".to_owned(),
            RunEventValue::List(
                retry
                    .retryable_errors
                    .iter()
                    .map(|kind| RunEventValue::String(error_kind_name(kind).to_owned()))
                    .collect(),
            ),
        ),
        (
            "policy".to_owned(),
            RunEventValue::Object(BTreeMap::from([
                (
                    "max_total_attempts".to_owned(),
                    RunEventValue::U64(u64::from(retry.policy.max_total_attempts)),
                ),
                (
                    "max_elapsed_ms".to_owned(),
                    RunEventValue::U64(retry.policy.max_elapsed_ms),
                ),
                (
                    "base_delay_ms".to_owned(),
                    RunEventValue::U64(retry.policy.base_delay_ms),
                ),
                (
                    "max_delay_ms".to_owned(),
                    RunEventValue::U64(retry.policy.max_delay_ms),
                ),
            ])),
        ),
        (
            "attestation".to_owned(),
            RunEventValue::String(
                match retry.attestation {
                    cdf_runtime::SourceAttestationStrength::None => "none",
                    cdf_runtime::SourceAttestationStrength::Metadata => "metadata",
                    cdf_runtime::SourceAttestationStrength::ImmutableContent => "immutable_content",
                    cdf_runtime::SourceAttestationStrength::Snapshot => "snapshot",
                }
                .to_owned(),
            ),
        ),
        ("resumable".to_owned(), RunEventValue::Bool(retry.resumable)),
    ]))
}

fn source_retry_history_value(history: &[cdf_runtime::SourceRetryHistoryEntry]) -> RunEventValue {
    RunEventValue::List(
        history
            .iter()
            .map(|entry| {
                let mut fields = BTreeMap::from([
                    (
                        "failed_attempt".to_owned(),
                        RunEventValue::U64(u64::from(entry.failed_attempt)),
                    ),
                    (
                        "cause".to_owned(),
                        RunEventValue::String(error_kind_name(&entry.cause).to_owned()),
                    ),
                ]);
                if let Some(delay_ms) = entry.selected_delay_ms {
                    fields.insert("selected_delay_ms".to_owned(), RunEventValue::U64(delay_ms));
                }
                if let Some(exhaustion) = entry.exhaustion {
                    fields.insert(
                        "exhaustion".to_owned(),
                        RunEventValue::String(retry_exhaustion_name(exhaustion).to_owned()),
                    );
                }
                RunEventValue::Object(fields)
            })
            .collect(),
    )
}

fn error_kind_name(kind: &cdf_kernel::ErrorKind) -> &'static str {
    match kind {
        cdf_kernel::ErrorKind::Transient => "transient",
        cdf_kernel::ErrorKind::RateLimited => "rate_limited",
        cdf_kernel::ErrorKind::Auth => "auth",
        cdf_kernel::ErrorKind::Contract => "contract",
        cdf_kernel::ErrorKind::Data => "data",
        cdf_kernel::ErrorKind::Destination => "destination",
        cdf_kernel::ErrorKind::Internal => "internal",
    }
}

fn retry_exhaustion_name(exhaustion: cdf_runtime::SourceRetryExhaustion) -> &'static str {
    match exhaustion {
        cdf_runtime::SourceRetryExhaustion::Ineligible => "ineligible",
        cdf_runtime::SourceRetryExhaustion::AttemptLimit => "attempt_limit",
        cdf_runtime::SourceRetryExhaustion::ElapsedDeadline => "elapsed_deadline",
    }
}

fn phase_metric(
    phase: RunPhase,
    status: RunPhaseStatus,
    active: ActivePhase,
) -> Result<RunPhaseMetric> {
    Ok(RunPhaseMetric {
        phase,
        context: None,
        status,
        duration_ns: u64::try_from(active.started_at.elapsed().as_nanos()).map_err(|error| {
            CdfError::internal(format!("phase duration does not fit in u64: {error}"))
        })?,
        input_bytes: active.input_bytes,
        output_bytes: active.output_bytes,
        operations: active.operations,
    })
}

pub(super) struct RunEventFanout<'a> {
    durable: DurableRunLedgerSubscriber<'a>,
    live: LiveRunEventSubscribers<'a>,
}

impl<'a> RunEventFanout<'a> {
    fn new(ledger: &'a SqliteRunLedger, event_sink: Option<&'a dyn RunEventSink>) -> Self {
        Self {
            durable: DurableRunLedgerSubscriber { ledger },
            live: LiveRunEventSubscribers::new(event_sink),
        }
    }

    fn publish(&self, run_id: &RunId, event: RunEventAppend) -> Result<()> {
        let stored = self.durable.append(run_id, event)?;
        self.live.publish(&stored);
        Ok(())
    }
}

struct DurableRunLedgerSubscriber<'a> {
    ledger: &'a SqliteRunLedger,
}

impl DurableRunLedgerSubscriber<'_> {
    fn append(&self, run_id: &RunId, event: RunEventAppend) -> Result<cdf_kernel::RunEvent> {
        self.ledger.append_event(run_id, event)
    }

    fn snapshot(&self, run_id: &RunId) -> Result<Option<RunLedgerSnapshot>> {
        self.ledger.snapshot(run_id)
    }
}

struct LiveRunEventSubscribers<'a> {
    subscribers: Vec<&'a dyn RunEventSink>,
}

impl<'a> LiveRunEventSubscribers<'a> {
    fn new(event_sink: Option<&'a dyn RunEventSink>) -> Self {
        Self {
            subscribers: event_sink.into_iter().collect(),
        }
    }

    fn publish(&self, stored: &cdf_kernel::RunEvent) {
        for subscriber in &self.subscribers {
            let _ = subscriber.try_emit(stored);
        }
    }
}

fn partition_id_for_scope(scope: &ScopeKey) -> Option<cdf_kernel::PartitionId> {
    match scope {
        ScopeKey::Partition { partition_id } => Some(partition_id.clone()),
        _ => None,
    }
}

fn details_for_phase(phase: &str) -> BTreeMap<String, RunEventValue> {
    BTreeMap::from([("phase".to_owned(), RunEventValue::String(phase.to_owned()))])
}

fn u64_from_usize(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|error| CdfError::internal(error.to_string()))
}

fn checkpoint_delta_details(phase: &str, delta: &StateDelta) -> Result<RunEventDetails> {
    let mut details = details_for_phase(phase);
    details.insert(
        "segment_count".to_owned(),
        RunEventValue::U64(u64_from_usize(delta.segments.len())?),
    );
    details.insert(
        "row_count".to_owned(),
        RunEventValue::U64(delta.segments.iter().map(|segment| segment.row_count).sum()),
    );
    details.insert(
        "byte_count".to_owned(),
        RunEventValue::U64(
            delta
                .segments
                .iter()
                .map(|segment| segment.byte_count)
                .sum(),
        ),
    );
    Ok(RunEventDetails {
        attributes: details,
    })
}

fn receipt_details(receipt: &Receipt) -> Result<RunEventDetails> {
    let mut details = details_for_phase("destination");
    details.insert(
        "segment_ack_count".to_owned(),
        RunEventValue::U64(u64_from_usize(receipt.segment_acks.len())?),
    );
    details.insert(
        "rows_written".to_owned(),
        RunEventValue::U64(receipt.counts.rows_written),
    );
    if let Some(rows_inserted) = receipt.counts.rows_inserted {
        details.insert(
            "rows_inserted".to_owned(),
            RunEventValue::U64(rows_inserted),
        );
    }
    if let Some(rows_updated) = receipt.counts.rows_updated {
        details.insert("rows_updated".to_owned(), RunEventValue::U64(rows_updated));
    }
    if let Some(rows_deleted) = receipt.counts.rows_deleted {
        details.insert("rows_deleted".to_owned(), RunEventValue::U64(rows_deleted));
    }
    details.insert(
        "migration_count".to_owned(),
        RunEventValue::U64(u64_from_usize(receipt.migrations.len())?),
    );
    Ok(RunEventDetails {
        attributes: details,
    })
}

fn checkpoint_details(checkpoint: &Checkpoint) -> Result<RunEventDetails> {
    let mut details = checkpoint_delta_details("checkpoint", &checkpoint.delta)?.attributes;
    details.insert(
        "status".to_owned(),
        RunEventValue::String(checkpoint.status.as_str().to_owned()),
    );
    Ok(RunEventDetails {
        attributes: details,
    })
}

fn validation_depth_transition_details(
    transition: ValidationDepthTransitionRecord<'_>,
) -> RunEventDetails {
    let mut attributes = BTreeMap::new();
    attributes.insert(
        "phase".to_owned(),
        RunEventValue::String("validation".to_owned()),
    );
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingSink {
        events: Mutex<Vec<cdf_kernel::RunEvent>>,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
            }
        }

        fn events(&self) -> Vec<cdf_kernel::RunEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl RunEventSink for RecordingSink {
        fn try_emit(&self, event: &cdf_kernel::RunEvent) -> cdf_kernel::RunEventSinkResult {
            self.events.lock().unwrap().push(event.clone());
            cdf_kernel::RunEventSinkResult::Accepted
        }
    }

    #[test]
    fn project_run_recorder_live_sink_rejects_raw_secret_details_before_emit() {
        let ledger = SqliteRunLedger::open_in_memory().unwrap();
        let run = ledger
            .create_run(Some(RunId::new("run-recorder-secret-guard").unwrap()))
            .unwrap();
        let sink = RecordingSink::new();
        let recorder = ProjectRunRecorder::new(
            &ledger,
            run.run_id.clone(),
            ProjectRunRecorderContext {
                resource_id: ResourceId::new("local.events").unwrap(),
                scope: ScopeKey::Resource,
                package_id: "pkg-recorder-secret-guard".to_owned(),
                package_path: "pkg-recorder-secret-guard".to_owned(),
                destination_id: DestinationId::new("duckdb").unwrap(),
                plan_id: PlanId::new("plan-recorder-secret-guard").unwrap(),
                pipeline_id: PipelineId::new("pipeline-recorder-secret-guard").unwrap(),
            },
            Some(&sink),
            RunTelemetryConfig::disabled(),
        );

        let mut raw_secret = recorder.base_event(RunEventKind::RunStarted);
        raw_secret.details = RunEventDetails::new([(
            "token",
            RunEventValue::String("super-secret-token".to_owned()),
        )]);
        assert!(recorder.append(raw_secret).is_err());
        assert!(sink.events().is_empty());
        assert!(ledger.events(&run.run_id).unwrap().is_empty());

        let mut untyped_secret_ref = recorder.base_event(RunEventKind::RunStarted);
        untyped_secret_ref.details = RunEventDetails::new([(
            "note",
            RunEventValue::String("secret://env/API_TOKEN".to_owned()),
        )]);
        assert!(recorder.append(untyped_secret_ref).is_err());
        assert!(sink.events().is_empty());
        assert!(ledger.events(&run.run_id).unwrap().is_empty());

        let mut typed_secret = recorder.base_event(RunEventKind::RunStarted);
        typed_secret.details = RunEventDetails::new([(
            "token",
            RunEventValue::SecretRef(
                cdf_kernel::SecretReference::new("secret://env/API_TOKEN").unwrap(),
            ),
        )]);
        recorder.append(typed_secret).unwrap();

        let live_events = sink.events();
        let persisted_events = ledger.events(&run.run_id).unwrap();
        assert_eq!(live_events, persisted_events);
        assert!(matches!(
            live_events[0].details.attributes.get("token"),
            Some(RunEventValue::SecretRef(_))
        ));
    }

    #[test]
    fn project_run_recorder_does_not_emit_when_durable_ledger_append_fails() {
        let ledger = SqliteRunLedger::open_in_memory().unwrap();
        let run_id = RunId::new("run-recorder-missing-ledger-run").unwrap();
        let sink = RecordingSink::new();
        let recorder = ProjectRunRecorder::new(
            &ledger,
            run_id.clone(),
            ProjectRunRecorderContext {
                resource_id: ResourceId::new("local.events").unwrap(),
                scope: ScopeKey::Resource,
                package_id: "pkg-recorder-missing-ledger-run".to_owned(),
                package_path: "pkg-recorder-missing-ledger-run".to_owned(),
                destination_id: DestinationId::new("duckdb").unwrap(),
                plan_id: PlanId::new("plan-recorder-missing-ledger-run").unwrap(),
                pipeline_id: PipelineId::new("pipeline-recorder-missing-ledger-run").unwrap(),
            },
            Some(&sink),
            RunTelemetryConfig::disabled(),
        );

        let error = recorder.append_run_started().unwrap_err();

        assert!(error.to_string().contains("does not exist"));
        assert!(sink.events().is_empty());
        assert!(ledger.events(&run_id).unwrap().is_empty());
    }

    #[test]
    fn phase_telemetry_is_bounded_and_failure_closes_active_phase() {
        let ledger = SqliteRunLedger::open_in_memory().unwrap();
        let run = ledger
            .create_run(Some(RunId::new("run-recorder-phase-failure").unwrap()))
            .unwrap();
        let recorder = ProjectRunRecorder::new(
            &ledger,
            run.run_id.clone(),
            ProjectRunRecorderContext {
                resource_id: ResourceId::new("local.events").unwrap(),
                scope: ScopeKey::Resource,
                package_id: "pkg-recorder-phase-failure".to_owned(),
                package_path: "pkg-recorder-phase-failure".to_owned(),
                destination_id: DestinationId::new("duckdb").unwrap(),
                plan_id: PlanId::new("plan-recorder-phase-failure").unwrap(),
                pipeline_id: PipelineId::new("pipeline-recorder-phase-failure").unwrap(),
            },
            None,
            RunTelemetryConfig {
                phase_metrics: true,
                max_phase_events: 1,
                statistics_profile: false,
            },
        );

        recorder.append_package_started().unwrap();
        recorder
            .append_run_failed(&CdfError::data("fixture failure"))
            .unwrap();
        recorder
            .append_phase_metric(RunPhaseMetric {
                phase: RunPhase::Decode,
                context: None,
                status: RunPhaseStatus::Completed,
                duration_ns: 1,
                input_bytes: 1,
                output_bytes: 1,
                operations: 1,
            })
            .unwrap();

        let events = ledger.events(&run.run_id).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == RunEventKind::PhaseMeasured)
                .count(),
            1
        );
        let metric = events
            .iter()
            .find_map(|event| match event.details.attributes.get("metric") {
                Some(RunEventValue::PhaseMetric(metric)) => Some(metric),
                _ => None,
            })
            .unwrap();
        assert_eq!(metric.phase, RunPhase::PackageExecution);
        assert_eq!(metric.status, RunPhaseStatus::Failed);
        assert!(metric.duration_ns > 0);
        assert_eq!(events.last().unwrap().kind, RunEventKind::RunFailed);
    }

    #[test]
    fn source_retry_history_is_a_durable_nonpackage_run_event() {
        let ledger = SqliteRunLedger::open_in_memory().unwrap();
        let run = ledger
            .create_run(Some(RunId::new("run-recorder-source-retry").unwrap()))
            .unwrap();
        let recorder = ProjectRunRecorder::new(
            &ledger,
            run.run_id.clone(),
            ProjectRunRecorderContext {
                resource_id: ResourceId::new("local.events").unwrap(),
                scope: ScopeKey::Resource,
                package_id: "pkg-recorder-source-retry".to_owned(),
                package_path: "pkg-recorder-source-retry".to_owned(),
                destination_id: DestinationId::new("duckdb").unwrap(),
                plan_id: PlanId::new("plan-recorder-source-retry").unwrap(),
                pipeline_id: PipelineId::new("pipeline-recorder-source-retry").unwrap(),
            },
            None,
            RunTelemetryConfig::disabled(),
        );
        let partition_id = cdf_kernel::PartitionId::new("file-2").unwrap();
        let scheduled = cdf_runtime::ScheduledPartition {
            ordinal: cdf_runtime::CanonicalPartitionOrdinal::new(0),
            partition: cdf_kernel::PartitionPlan {
                partition_id: partition_id.clone(),
                scope: ScopeKey::Partition {
                    partition_id: partition_id.clone(),
                },
                planned_position: None,
                start_position: None,
                scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
                retry_safety: cdf_kernel::PartitionRetrySafety::ImmutableContent,
                metadata: BTreeMap::new(),
            },
            immutable_identity_hash: "sha256:fixture".to_owned(),
            schedule_identity_hash: "sha256:schedule-fixture".to_owned(),
            minimum_working_set_bytes: 1,
            maximum_working_set_bytes: 1,
            executor_class: cdf_runtime::SourceExecutorClass::Io,
            retry: Some(cdf_runtime::CompiledSourceRetry {
                granularity: cdf_runtime::SourceRetryGranularity::Partition,
                retryable_errors: vec![cdf_kernel::ErrorKind::Transient],
                policy: cdf_runtime::SourceRetryPolicy::default(),
                attestation: cdf_runtime::SourceAttestationStrength::ImmutableContent,
                resumable: true,
            }),
            rate_limit: None,
            quota_authority: None,
            speculative_safe: true,
            canonical_order: true,
            bounded_source: true,
        };
        let schedule = cdf_runtime::CanonicalPartitionSchedule {
            plan_id: "plan-recorder-source-retry".to_owned(),
            schedule_identity_hash: scheduled.schedule_identity_hash.clone(),
            admission: cdf_runtime::PartitionAdmissionTemplate {
                minimum_working_set_bytes: scheduled.minimum_working_set_bytes,
                maximum_working_set_bytes: scheduled.maximum_working_set_bytes,
                executor_class: scheduled.executor_class,
                retry: scheduled.retry.clone(),
                rate_limit: scheduled.rate_limit,
                quota_authority: scheduled.quota_authority.clone(),
                speculative_safe: scheduled.speculative_safe,
                canonical_order: scheduled.canonical_order,
                bounded_source: scheduled.bounded_source,
            },
            authority: cdf_runtime::PartitionScheduleAuthority::Inline {
                partitions: vec![cdf_runtime::CanonicalPartitionBinding {
                    ordinal: scheduled.ordinal,
                    partition: scheduled.partition.clone(),
                    immutable_identity_hash: scheduled.immutable_identity_hash.clone(),
                }],
            },
        };
        let journal = cdf_runtime::SourceRetryJournal::default();
        journal
            .record(
                &schedule.plan_id,
                &scheduled,
                &[cdf_runtime::SourceRetryHistoryEntry {
                    failed_attempt: 1,
                    cause: cdf_kernel::ErrorKind::Transient,
                    selected_delay_ms: Some(17),
                    exhaustion: None,
                }],
            )
            .unwrap();
        let evidence = journal.snapshot().unwrap();

        recorder
            .append_source_retries(&evidence, Some(&schedule))
            .unwrap();

        let events = ledger.events(&run.run_id).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, RunEventKind::SourceRetryRecorded);
        assert_eq!(
            events[0].details.attributes.get("partition_id"),
            Some(&RunEventValue::String("file-2".to_owned()))
        );
        assert!(matches!(
            events[0].details.attributes.get("history"),
            Some(RunEventValue::List(history)) if history.len() == 1
        ));
    }
}
