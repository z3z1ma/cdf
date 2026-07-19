use super::{
    destinations::{
        DestinationPlanningContext, ProjectDestinationRuntime, ResolvedProjectDestination,
    },
    hooks::{ReceiptVerifiedHook, RuntimeStage, RuntimeStageHook},
    prelude::*,
    receipts::validate_destination_receipt_before_checkpoint,
    types::*,
};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use cdf_kernel::PushdownFidelity;
use cdf_memory::{DEFAULT_PROCESS_BUDGET_BYTES, DeterministicMemoryCoordinator, MemoryCoordinator};
use sha2::{Digest, Sha256};

type DestinationReplayStageHook<'a> = RuntimeStageHook<'a>;
pub(crate) type PackageReplayStageHook<'a> = &'a dyn Fn(PackageReplayStage<'_>) -> Result<()>;

pub(crate) struct ActiveStagedIngress {
    attempt_id: cdf_runtime::LoadAttemptId,
    execution_plan_id: PlanId,
    schema_hash: SchemaHash,
    session: Option<Box<dyn cdf_runtime::StagedIngressSession>>,
    staged: Vec<cdf_runtime::StagedSegmentIdentity>,
    next_ordinal: u32,
    background: Option<BackgroundStaging>,
    execution: Option<ExecutionServices>,
    final_binding_lane: Option<String>,
    bulk_path: cdf_runtime::PreparedBulkPath,
    staging_lease: Option<cdf_runtime::ManagedStagingLease>,
    ingress_duration: Duration,
    ingress_input_bytes: u64,
    ingress_operations: u64,
}

pub(crate) struct StagedIngressPlan {
    pub(crate) checkpoint_id: CheckpointId,
    pub(crate) execution_plan_id: PlanId,
    pub(crate) target: TargetName,
    pub(crate) disposition: WriteDisposition,
    pub(crate) schema_hash: SchemaHash,
    pub(crate) output_schema: Schema,
    pub(crate) merge_keys: Vec<String>,
}

struct BackgroundStaging {
    sender: Option<mpsc::SyncSender<BackgroundStagedSegment>>,
    scope: Option<Box<dyn cdf_runtime::ExecutionTaskScope>>,
    completed: Arc<Mutex<CompletedBackgroundStaging>>,
    bytes: Arc<InFlightByteBudget>,
    services: ExecutionServices,
}

struct BackgroundStagedSegment {
    request: cdf_runtime::StagedSegmentRequest,
    identity: cdf_runtime::StagedSegmentIdentity,
    bytes: InFlightBytePermit,
}

struct BackgroundStagingGuard {
    identity: cdf_runtime::StagedSegmentIdentity,
    _bytes: InFlightBytePermit,
}

struct BackgroundStagingStream {
    receiver: mpsc::Receiver<BackgroundStagedSegment>,
    attempt_id: cdf_runtime::LoadAttemptId,
    in_progress: BTreeMap<SegmentId, BackgroundStagingGuard>,
    staged: BTreeMap<u32, cdf_runtime::StagedSegmentIdentity>,
    receive_wait: Duration,
}

impl BackgroundStagingStream {
    fn finish(self) -> Result<(Vec<cdf_runtime::StagedSegmentIdentity>, Duration)> {
        if !self.in_progress.is_empty() {
            return Err(CdfError::destination(
                "staged destination returned with unacknowledged segments",
            ));
        }
        Ok((self.staged.into_values().collect(), self.receive_wait))
    }
}

impl cdf_runtime::StagedSegmentStream for BackgroundStagingStream {
    fn next_segment(&mut self) -> Result<Option<cdf_runtime::StagedSegmentRequest>> {
        let waiting = Instant::now();
        let work = self.receiver.recv().ok();
        self.receive_wait = self.receive_wait.saturating_add(waiting.elapsed());
        let Some(work) = work else {
            return Ok(None);
        };
        let BackgroundStagedSegment {
            request,
            identity,
            bytes,
        } = work;
        if self
            .in_progress
            .insert(
                identity.segment_id.clone(),
                BackgroundStagingGuard {
                    identity,
                    _bytes: bytes,
                },
            )
            .is_some()
        {
            return Err(CdfError::destination(
                "staged ingress received a duplicate in-flight segment",
            ));
        }
        Ok(Some(request))
    }

    fn acknowledge(&mut self, acknowledgement: cdf_runtime::StagedSegmentAck) -> Result<()> {
        let guard = self
            .in_progress
            .remove(&acknowledgement.identity.segment_id)
            .ok_or_else(|| {
                CdfError::destination(
                    "staged destination acknowledged a segment that is not in flight",
                )
            })?;
        if acknowledgement.attempt_id != self.attempt_id
            || acknowledgement.identity != guard.identity
        {
            return Err(CdfError::destination(
                "staged ingress acknowledgement did not bind the exact durable segment",
            ));
        }
        if self
            .staged
            .insert(guard.identity.ordinal, guard.identity)
            .is_some()
        {
            return Err(CdfError::destination(
                "staged ingress acknowledged a duplicate segment ordinal",
            ));
        }
        Ok(())
    }
}

struct OneStagedSegmentStream {
    request: Option<cdf_runtime::StagedSegmentRequest>,
    attempt_id: cdf_runtime::LoadAttemptId,
    identity: cdf_runtime::StagedSegmentIdentity,
    acknowledged: bool,
}

impl cdf_runtime::StagedSegmentStream for OneStagedSegmentStream {
    fn next_segment(&mut self) -> Result<Option<cdf_runtime::StagedSegmentRequest>> {
        if self.request.is_none() && !self.acknowledged {
            return Err(CdfError::destination(
                "staged destination requested another segment before acknowledging the current segment",
            ));
        }
        Ok(self.request.take())
    }

    fn acknowledge(&mut self, acknowledgement: cdf_runtime::StagedSegmentAck) -> Result<()> {
        if self.acknowledged
            || acknowledgement.attempt_id != self.attempt_id
            || acknowledgement.identity != self.identity
        {
            return Err(CdfError::destination(
                "staged ingress acknowledgement did not bind the exact durable segment",
            ));
        }
        self.acknowledged = true;
        Ok(())
    }
}

struct InFlightByteBudget {
    maximum: u64,
    current: Mutex<u64>,
    released: Condvar,
}

impl InFlightByteBudget {
    fn acquire(self: &Arc<Self>, bytes: u64) -> Result<InFlightBytePermit> {
        if bytes > self.maximum {
            return Err(CdfError::data(format!(
                "staged segment retains {bytes} bytes above the destination in-flight bound {}; rebuild with smaller canonical segments or raise the destination bound",
                self.maximum
            )));
        }
        let mut current = self
            .current
            .lock()
            .map_err(|_| CdfError::internal("staged byte budget lock is poisoned"))?;
        while current.saturating_add(bytes) > self.maximum {
            current = self
                .released
                .wait(current)
                .map_err(|_| CdfError::internal("staged byte budget lock is poisoned"))?;
        }
        *current = current.saturating_add(bytes);
        Ok(InFlightBytePermit {
            budget: Arc::clone(self),
            bytes,
        })
    }
}

struct InFlightBytePermit {
    budget: Arc<InFlightByteBudget>,
    bytes: u64,
}

impl Drop for InFlightBytePermit {
    fn drop(&mut self) {
        if let Ok(mut current) = self.budget.current.lock() {
            *current = current.saturating_sub(self.bytes);
            self.budget.released.notify_all();
        }
    }
}

#[derive(Default)]
struct CompletedBackgroundStaging {
    session: Option<Box<dyn cdf_runtime::StagedIngressSession>>,
    staged: Vec<cdf_runtime::StagedSegmentIdentity>,
    error: Option<CdfError>,
    ingress_duration: Duration,
    ingress_input_bytes: u64,
    ingress_operations: u64,
}

impl ActiveStagedIngress {
    pub(crate) fn begin(
        runtime: &mut dyn ProjectDestinationRuntime,
        plan: StagedIngressPlan,
        services: &ExecutionServices,
    ) -> Result<Option<Self>> {
        let capabilities = runtime.runtime_capabilities();
        capabilities.validate()?;
        if capabilities.ingress_mode == cdf_runtime::DestinationIngressMode::FinalizedPackageOnly {
            return Ok(None);
        }
        let mut preparation = cdf_runtime::BulkPathPreparationInput::new(&plan.output_schema);
        preparation = preparation.with_execution(services.capabilities());
        let bulk_path = runtime.prepare_selected_bulk_path(&preparation)?;
        let destination_id = runtime.describe().destination_id;
        let attempt_id = staging_attempt_id(&plan.checkpoint_id, &destination_id)?;
        let scheduling = cdf_runtime::StagingSchedulingContext::new(
            capabilities
                .max_in_flight_segments
                .expect("validated staged segment bound"),
            capabilities
                .max_in_flight_bytes
                .expect("validated staged byte bound"),
        )?;
        let cdf_runtime::DestinationIngress::StagedSegments(staged_runtime) = runtime.ingress()
        else {
            return Err(CdfError::contract(
                "staged ingress plan reached a finalized destination runtime",
            ));
        };
        for candidate in staged_runtime.staging_cleanup_candidates(&plan.target)? {
            if let Some(proof) = services.prove_expired_staging_lease(candidate.lease())? {
                proof.execute(|proof, guard| {
                    staged_runtime.cleanup_expired_staging(&candidate, proof, guard)
                })?;
            }
        }
        let execution = if capabilities.staged_ingress_lane.is_some()
            || capabilities.final_binding_lane.is_some()
        {
            services.ensure_blocking_lanes(&capabilities.blocking_lanes)?;
            Some(services.clone())
        } else {
            None
        };
        let output_arrow_schema_hash =
            cdf_kernel::canonical_arrow_schema_hash(&plan.output_schema)?;
        let staging_lease =
            services.acquire_staging_lease(cdf_runtime::StagingLeaseIdentity::new(
                destination_id.clone(),
                plan.target.clone(),
                attempt_id.clone(),
            ))?;
        let lease_snapshot = match staging_lease.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => return Err(release_staging_lease_after_error(error, staging_lease)),
        };
        let mutation_guard = match staging_lease.mutation_guard() {
            Ok(guard) => guard,
            Err(error) => return Err(release_staging_lease_after_error(error, staging_lease)),
        };
        let request = cdf_runtime::StagedIngressRequest::new(
            attempt_id.clone(),
            cdf_runtime::StagingAttemptBinding {
                destination_id,
                target: plan.target,
                disposition: plan.disposition,
                schema_hash: plan.schema_hash.clone(),
                output_arrow_schema_hash,
                merge_keys: plan.merge_keys.clone(),
                execution_plan_id: plan.execution_plan_id.clone(),
            },
            lease_snapshot,
            mutation_guard,
            bulk_path.clone(),
            scheduling.clone(),
            plan.output_schema,
        );
        let request = match request {
            Ok(request) => request,
            Err(error) => return Err(release_staging_lease_after_error(error, staging_lease)),
        };
        let session = match staged_runtime.begin_staged_ingress(request) {
            Ok(session) => session,
            Err(error) => return Err(release_staging_lease_after_error(error, staging_lease)),
        };
        let mut active = Self {
            attempt_id,
            execution_plan_id: plan.execution_plan_id,
            schema_hash: plan.schema_hash,
            session: Some(session),
            staged: Vec::new(),
            next_ordinal: 0,
            background: None,
            execution,
            final_binding_lane: capabilities.final_binding_lane.clone(),
            bulk_path,
            staging_lease: Some(staging_lease),
            ingress_duration: Duration::ZERO,
            ingress_input_bytes: 0,
            ingress_operations: 0,
        };
        if let Some(lane) = capabilities.staged_ingress_lane.as_deref()
            && let Err(mut error) = active.start_background(lane, &capabilities)
        {
            if let Err(cleanup) = active.abort() {
                error = attach_cleanup_failure(error, "staged ingress construction", cleanup);
            }
            return Err(error);
        }
        Ok(Some(active))
    }

    fn start_background(
        &mut self,
        lane: &str,
        capabilities: &cdf_runtime::DestinationRuntimeCapabilities,
    ) -> Result<()> {
        let services = self
            .execution
            .clone()
            .ok_or_else(|| CdfError::internal("staged blocking lane has no execution services"))?;
        let mut scope = services.open_scope(self.attempt_id.as_str())?;
        // The staged session owns the in-flight window by pulling exact segments. A rendezvous
        // channel prevents orchestration from retaining a second hidden queue beyond that bound.
        let (sender, receiver) = mpsc::sync_channel::<BackgroundStagedSegment>(0);
        let session = self
            .session
            .take()
            .ok_or_else(|| CdfError::internal("staged session is absent before worker start"))?;
        let completed = Arc::new(Mutex::new(CompletedBackgroundStaging {
            session: Some(session),
            ..CompletedBackgroundStaging::default()
        }));
        let bytes = Arc::new(InFlightByteBudget {
            maximum: capabilities
                .max_in_flight_bytes
                .expect("validated staged byte bound"),
            current: Mutex::new(0),
            released: Condvar::new(),
        });
        let completed_worker = Arc::clone(&completed);
        let attempt_id = self.attempt_id.clone();
        let submission = scope.spawn_blocking(
            lane,
            Box::new(move || {
                // Keep ownership in the shared completion cell until this task actually begins.
                // A host that rejects submission therefore cannot strand the adapter session.
                let mut session = completed_worker
                    .lock()
                    .map_err(|_| {
                        CdfError::internal("background staging completion lock is poisoned")
                    })?
                    .session
                    .take()
                    .ok_or_else(|| {
                        CdfError::internal("background staging session was already consumed")
                    })?;
                let mut stream = BackgroundStagingStream {
                    receiver,
                    attempt_id,
                    in_progress: BTreeMap::new(),
                    staged: BTreeMap::new(),
                    receive_wait: Duration::ZERO,
                };
                let started = Instant::now();
                if let Err(mut error) = session.stage_stream(&mut stream) {
                    if let Err(cleanup) = session.abort() {
                        error = attach_cleanup_failure(error, "staged worker abort", cleanup);
                    }
                    if let Ok(mut output) = completed_worker.lock() {
                        output.error = Some(error.clone());
                    }
                    return Err(error);
                }
                let elapsed = started.elapsed();
                let (staged, receive_wait) = match stream.finish() {
                    Ok(finished) => finished,
                    Err(mut error) => {
                        if let Err(cleanup) = session.abort() {
                            error = attach_cleanup_failure(error, "staged worker abort", cleanup);
                        }
                        if let Ok(mut output) = completed_worker.lock() {
                            output.error = Some(error.clone());
                        }
                        return Err(error);
                    }
                };
                let mut output = completed_worker.lock().map_err(|_| {
                    CdfError::internal("background staging completion lock is poisoned")
                })?;
                output.ingress_duration = elapsed.saturating_sub(receive_wait);
                output.ingress_input_bytes = staged.iter().fold(0_u64, |total, identity| {
                    total.saturating_add(identity.byte_count)
                });
                output.ingress_operations = u64::try_from(staged.len())
                    .map_err(|_| CdfError::data("staged segment count exceeds u64"))?;
                output.session = Some(session);
                output.staged = staged;
                Ok(())
            }),
        );
        if let Err(error) = submission {
            // Submission failure means the task did not start. Recover the session so the
            // caller's ordinary abort path can clean adapter-owned staging and release its lease.
            let mut output = completed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.session = output.session.take();
            if self.session.is_none() {
                return Err(attach_cleanup_failure(
                    error,
                    "staged worker submission",
                    CdfError::internal(
                        "rejected background staging task consumed its session before starting",
                    ),
                ));
            }
            return Err(error);
        }
        self.background = Some(BackgroundStaging {
            sender: Some(sender),
            scope: Some(scope),
            completed,
            bytes,
            services,
        });
        Ok(())
    }

    pub(crate) fn stage_segment(
        &mut self,
        entry: &SegmentEntry,
        payload: cdf_engine::DurableSegmentPayload,
    ) -> Result<()> {
        self.staging_lease
            .as_ref()
            .ok_or_else(|| CdfError::internal("staged ingress lease is absent"))?
            .snapshot()?;
        let ordinal = self.next_ordinal;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("staged package has too many segments"))?;
        let identity = cdf_runtime::StagedSegmentIdentity::from_manifest_entry(
            entry,
            self.schema_hash.clone(),
            ordinal,
        )?;
        let (durable_local_file, batches, memory_leases) = payload.into_parts();
        let retained_bytes = batches
            .iter()
            .try_fold(0_u64, |total, batch| {
                total
                    .checked_add(cdf_memory::record_batch_retained_bytes(batch)?)
                    .ok_or_else(|| CdfError::data("staged segment retained bytes overflow"))
            })?
            .max(1);
        let request = cdf_runtime::StagedSegmentRequest::new(
            identity.clone(),
            Box::new(LiveStagedSegmentReader {
                identity: identity.clone(),
                durable_local_path: Some(durable_local_file),
                batches: batches.into_iter(),
                _memory_leases: memory_leases,
            }),
        )?;
        if let Some(background) = &mut self.background {
            let maximum_bytes = background.services.memory().snapshot().budget_bytes;
            if retained_bytes > maximum_bytes {
                return Err(CdfError::data(format!(
                    "staged segment retains {retained_bytes} bytes above the managed memory budget {maximum_bytes}; rebuild with smaller canonical segments or raise the budget"
                )));
            }
            let bytes = background.bytes.acquire(retained_bytes)?;
            let send = background
                .sender
                .as_ref()
                .ok_or_else(|| CdfError::internal("staged ingress worker is closed"))?
                .send(BackgroundStagedSegment {
                    request,
                    identity,
                    bytes,
                });
            if send.is_err() {
                let error = background
                    .completed
                    .lock()
                    .ok()
                    .and_then(|output| output.error.clone())
                    .unwrap_or_else(|| CdfError::destination("staged ingress worker stopped"));
                return Err(error);
            }
            return Ok(());
        }
        let mut stream = OneStagedSegmentStream {
            request: Some(request),
            attempt_id: self.attempt_id.clone(),
            identity: identity.clone(),
            acknowledged: false,
        };
        let started = Instant::now();
        self.session
            .as_mut()
            .ok_or_else(|| CdfError::internal("staged session is no longer active"))?
            .stage_stream(&mut stream)?;
        self.ingress_duration = self.ingress_duration.saturating_add(started.elapsed());
        if !stream.acknowledged {
            return Err(CdfError::destination(
                "staged destination returned without acknowledging its current segment",
            ));
        }
        self.staged.push(identity);
        self.ingress_input_bytes = self.ingress_input_bytes.saturating_add(entry.byte_count);
        self.ingress_operations = self.ingress_operations.saturating_add(1);
        Ok(())
    }

    pub(crate) fn finish_background(&mut self) -> Result<()> {
        self.staging_lease
            .as_ref()
            .ok_or_else(|| CdfError::internal("staged ingress lease is absent"))?
            .snapshot()?;
        let Some(mut background) = self.background.take() else {
            return Ok(());
        };
        drop(background.sender.take());
        let report = background.services.run_io(
            background
                .scope
                .take()
                .expect("background staging scope is consumed exactly once")
                .join(),
        )?;
        if report.failed > 0 || report.cancelled > 0 {
            return Err(CdfError::destination(
                "background staged ingress did not complete cleanly",
            ));
        }
        let mut completed = background
            .completed
            .lock()
            .map_err(|_| CdfError::internal("background staging completion lock is poisoned"))?;
        self.session = completed.session.take();
        self.staged = std::mem::take(&mut completed.staged);
        self.ingress_duration = completed.ingress_duration;
        self.ingress_input_bytes = completed.ingress_input_bytes;
        self.ingress_operations = completed.ingress_operations;
        Ok(())
    }

    pub(crate) fn ingress_metric(&self) -> Result<Option<RunPhaseMetric>> {
        if self.ingress_operations == 0 {
            return Ok(None);
        }
        Ok(Some(RunPhaseMetric {
            phase: RunPhase::DestinationIngress,
            context: None,
            status: RunPhaseStatus::Completed,
            duration_ns: u64::try_from(self.ingress_duration.as_nanos()).map_err(|error| {
                CdfError::internal(format!(
                    "destination ingress duration does not fit in u64: {error}"
                ))
            })?,
            input_bytes: self.ingress_input_bytes,
            output_bytes: self.ingress_input_bytes,
            operations: self.ingress_operations,
        }))
    }

    fn bind_final(
        &mut self,
        binding: cdf_runtime::VerifiedFinalBinding,
    ) -> Result<cdf_runtime::DestinationCommitOutcome> {
        self.staging_lease
            .as_ref()
            .ok_or_else(|| CdfError::internal("staged ingress lease is absent"))?
            .snapshot()?;
        let session = self
            .session
            .take()
            .ok_or_else(|| CdfError::internal("staged session is no longer active"))?;
        let outcome = match &self.final_binding_lane {
            Some(lane) => {
                let execution = self.execution.as_ref().ok_or_else(|| {
                    CdfError::internal("staged final-binding lane has no execution services")
                })?;
                let lane = lane.clone();
                execution.run_blocking(&lane, move || session.bind_final(binding))
            }
            None => session.bind_final(binding),
        }?;
        self.staging_lease
            .take()
            .ok_or_else(|| CdfError::internal("staged ingress lease is absent"))?
            .finish()?;
        Ok(outcome)
    }

    pub(crate) fn abort(mut self) -> Result<()> {
        let mut failure = None;
        if let Some(mut background) = self.background.take() {
            drop(background.sender.take());
            if let Some(scope) = background.scope.take()
                && let Err(error) = background.services.run_io(scope.join())
            {
                record_cleanup_failure(&mut failure, "join staged ingress worker", error);
            }
            if let Ok(mut completed) = background.completed.lock() {
                self.session = completed.session.take();
            } else {
                record_cleanup_failure(
                    &mut failure,
                    "recover staged ingress session",
                    CdfError::internal("background staging completion lock is poisoned"),
                );
            }
        }
        if let Some(session) = self.session.take() {
            let aborted = match (&self.execution, &self.final_binding_lane) {
                (Some(execution), Some(lane)) => {
                    let lane = lane.clone();
                    execution.run_blocking(&lane, move || session.abort())
                }
                _ => session.abort(),
            };
            if let Err(error) = aborted {
                record_cleanup_failure(&mut failure, "abort staged ingress session", error);
            }
        }
        if let Some(lease) = self.staging_lease.take()
            && let Err(error) = lease.finish()
        {
            record_cleanup_failure(&mut failure, "release staging lease", error);
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn record_cleanup_failure(failure: &mut Option<CdfError>, context: &str, error: CdfError) {
    match failure {
        Some(primary) => {
            primary.message = format!(
                "{}; {context} also failed: {}",
                primary.message, error.message
            );
        }
        None => {
            let mut error = error;
            error.message = format!("{context}: {}", error.message);
            *failure = Some(error);
        }
    }
}

fn attach_cleanup_failure(mut primary: CdfError, context: &str, cleanup: CdfError) -> CdfError {
    primary.message = format!(
        "{}; {context} also failed: {}",
        primary.message, cleanup.message
    );
    primary
}

fn release_staging_lease_after_error(
    mut error: CdfError,
    lease: cdf_runtime::ManagedStagingLease,
) -> CdfError {
    if let Err(release) = lease.finish() {
        error = attach_cleanup_failure(error, "staging lease release", release);
    }
    error
}

struct LiveStagedSegmentReader {
    identity: cdf_runtime::StagedSegmentIdentity,
    durable_local_path: Option<PathBuf>,
    batches: std::vec::IntoIter<arrow_array::RecordBatch>,
    _memory_leases: Vec<cdf_memory::MemoryLease>,
}

impl cdf_runtime::DurableSegmentReader for LiveStagedSegmentReader {
    fn identity(&self) -> &cdf_runtime::StagedSegmentIdentity {
        &self.identity
    }

    fn take_durable_local_file(&mut self) -> Result<Option<cdf_runtime::DurableLocalFile>> {
        let Some(path) = self.durable_local_path.take() else {
            return Ok(None);
        };
        let file = std::fs::File::open(&path).map_err(|error| {
            CdfError::data(format!(
                "open durable staged segment {} at {}: {error}",
                self.identity.segment_id,
                path.display()
            ))
        })?;
        Ok(Some(cdf_runtime::DurableLocalFile::new(path, file)))
    }

    fn next_batch(&mut self) -> Result<Option<arrow_array::RecordBatch>> {
        Ok(self.batches.next())
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PackageReplayStage<'a> {
    PackageReplayVerified,
    CheckpointProposed {
        delta: &'a StateDelta,
    },
    DestinationWriteReady,
    DestinationCommitStarted {
        plan_id: &'a PlanId,
        segment_count: usize,
        bulk_path: &'a cdf_runtime::PreparedBulkPath,
    },
    DestinationSegmentAcknowledged {
        ack: &'a SegmentAck,
    },
    DestinationReceiptRecorded {
        receipt: &'a Receipt,
    },
    CheckpointCommitted {
        checkpoint: &'a Checkpoint,
    },
    PackageStatusUpdated {
        status: &'a PackageStatus,
    },
}

#[derive(Default)]
pub(crate) struct PackageReplayHooks<'a> {
    pub(crate) after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    pub(crate) stage: Option<PackageReplayStageHook<'a>>,
}

pub fn replay_package_from_artifacts<Store>(
    request: PackageArtifactReplayRequest<'_, Store>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    replay_package_from_artifacts_with_stage_hook(request, None)
}

pub fn replay_package_from_artifacts_with_stage_hook<Store>(
    request: PackageArtifactReplayRequest<'_, Store>,
    stage_hook: Option<RuntimeStageHook<'_>>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let package = PackageReader::open(&request.package_dir)?.into_verified()?;
    validate_package_compiled_expression_plan(&package)?;
    validate_package_compiled_schema_admission(&package)?;
    let runtime_stage_hook =
        |stage: PackageReplayStage<'_>| notify_runtime_replay_stage(stage_hook, stage);
    replay_package_with_resolved_destination(
        package,
        request.destination,
        request.checkpoint_store,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: Some(&runtime_stage_hook),
        },
    )
}

pub fn recover_package_from_artifacts<Store>(
    request: PackageArtifactRecoveryRequest<'_, Store>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let package = PackageReader::open(&request.package_dir)?.into_verified()?;
    validate_package_compiled_expression_plan(&package)?;
    validate_package_compiled_schema_admission(&package)?;
    recover_package_with_resolved_destination(
        package,
        request.destination,
        request.checkpoint_store,
        request.receipt,
        PackageReplayHooks {
            after_receipt_verified: request.after_receipt_verified,
            stage: None,
        },
    )
}

fn validate_package_compiled_expression_plan(package: &VerifiedPackageReader) -> Result<()> {
    let program: cdf_contract::ValidationProgram = package
        .reader()
        .verified_json_artifact(package.verification(), "plan/validation-program.json")?;
    let compiled = program.compiled_expression_plan.as_ref().ok_or_else(|| {
        CdfError::contract(
            "package validation program has no recorded compiled expression plan; rebuild the package",
        )
    })?;
    compiled.validate_program_binding(&program)?;

    let scan = package
        .reader()
        .recorded_scan_plan_verified(package.verification())?;
    compiled.validate_predicate_bindings(scan.request.filters.iter().map(|predicate| {
        (
            predicate.expression.as_str(),
            &predicate.canonical_expression,
            scan.pushed_predicates.iter().any(|pushed| {
                pushed.predicate.predicate_id == predicate.predicate_id
                    && pushed.fidelity == PushdownFidelity::Exact
            }),
        )
    }))?;
    let mut residuals = scan.unsupported_predicates.iter().collect::<Vec<_>>();
    residuals.extend(
        scan.pushed_predicates
            .iter()
            .filter(|predicate| predicate.fidelity == PushdownFidelity::Inexact)
            .map(|predicate| &predicate.predicate),
    );
    compiled.validate_residual_bindings(residuals.into_iter().map(|predicate| {
        (
            predicate.expression.as_str(),
            &predicate.canonical_expression,
        )
    }))
}

fn validate_package_compiled_schema_admission(package: &VerifiedPackageReader) -> Result<()> {
    let program: cdf_contract::ValidationProgram = package
        .reader()
        .verified_json_artifact(package.verification(), "plan/validation-program.json")?;
    let admission: cdf_engine::CompiledSchemaAdmissionPlan = package
        .reader()
        .verified_json_artifact(package.verification(), "plan/schema-admission.json")?;
    admission.validate_recorded(&program)?;

    let inputs = package
        .reader()
        .replay_inputs_verified(package.verification())?;
    if admission.effective_schema_hash != inputs.state_delta.schema_hash {
        return Err(CdfError::data(format!(
            "compiled admission effective schema {} does not match StateDelta schema {}",
            admission.effective_schema_hash, inputs.state_delta.schema_hash
        )));
    }

    let files = &package.reader().manifest().identity.files;
    let has_stream_evidence = files
        .iter()
        .any(|entry| entry.path == "schema/stream-admission-evidence.json");
    if !has_stream_evidence {
        return Err(CdfError::data(
            "package omitted mandatory stream-admission evidence",
        ));
    }
    let evidence: cdf_engine::CompiledStreamAdmissionEvidence =
        package.reader().verified_json_artifact(
            package.verification(),
            "schema/stream-admission-evidence.json",
        )?;
    evidence.validate(&admission)?;
    let scan: cdf_kernel::ScanPlan = package
        .reader()
        .verified_json_artifact(package.verification(), cdf_package_contract::SCAN_PLAN_FILE)?;
    cdf_kernel::validate_scan_partition_observation_identities(&scan)?;
    let mut admitted = std::collections::BTreeMap::new();
    let mut partial_admissions = std::collections::BTreeMap::new();
    let mut unpositioned_admissions = std::collections::BTreeMap::new();
    for observation in evidence.observations {
        match observation.completion {
            cdf_engine::StreamAdmissionCompletion::Partial {
                attempted_position: Some(attempted_position),
                observed_rows,
                partition_binding,
            } => {
                partial_admissions.insert(
                    observation.observation_id,
                    (attempted_position, observed_rows, partition_binding),
                );
            }
            cdf_engine::StreamAdmissionCompletion::Partial {
                attempted_position: None,
                ..
            } => unreachable!("validated stream evidence requires a partial position"),
            cdf_engine::StreamAdmissionCompletion::Complete { source_position } => {
                admitted.insert(observation.observation_id, source_position);
            }
            cdf_engine::StreamAdmissionCompletion::CompleteUnpositioned { partition_binding } => {
                unpositioned_admissions.insert(observation.observation_id, partition_binding);
            }
        }
    }
    let lineage: cdf_engine::LineageSummary = package
        .reader()
        .verified_json_artifact(package.verification(), "lineage/lineage.json")?;
    validate_stream_admission_lineage_coverage(
        admitted.keys().map(String::as_str),
        partial_admissions.keys().map(String::as_str),
        unpositioned_admissions.keys().map(String::as_str),
        &lineage,
    )?;
    for (observation_id, source_position) in &admitted {
        let matching_partitions = scan
            .partitions
            .iter()
            .filter(|partition| {
                cdf_kernel::partition_schema_observation_id(partition) == observation_id
            })
            .collect::<Vec<_>>();
        if matching_partitions.len() != 1 {
            return Err(CdfError::data(format!(
                "complete stream-admission observation {observation_id:?} is not bound to one planned partition"
            )));
        }
        let partition = matching_partitions[0];
        if lineage
            .input_observations
            .iter()
            .filter(|observation| {
                observation.observation_id == *observation_id
                    && observation.partition_id == partition.partition_id
                    && observation.output_position.as_ref() == Some(source_position)
            })
            .count()
            != 1
        {
            return Err(CdfError::data(format!(
                "complete stream-admission observation {observation_id:?} does not match its planned partition and execution lineage"
            )));
        }
    }
    if !partial_admissions.is_empty() {
        for (observation_id, (attempted_position, observed_rows, partition_binding)) in
            &partial_admissions
        {
            let matching_partitions = scan
                .partitions
                .iter()
                .filter(|partition| {
                    cdf_kernel::partition_schema_observation_id(partition) == observation_id
                        && cdf_kernel::partition_source_identity_binding(partition)
                            .is_ok_and(|expected| &expected == partition_binding)
                        && partial_position_matches_partition_scope(attempted_position, partition)
                })
                .collect::<Vec<_>>();
            if matching_partitions.len() != 1 {
                return Err(CdfError::data(format!(
                    "partial stream-admission observation {observation_id:?} is not bound to a planned partition and source generation"
                )));
            }
            let partition = matching_partitions[0];
            if !partial_lineage_matches_exactly(
                &lineage,
                observation_id,
                partition,
                *observed_rows,
                attempted_position,
            ) {
                return Err(CdfError::data(format!(
                    "partial stream-admission observation {observation_id:?} row extent or attempted position does not match execution lineage"
                )));
            }
        }
    }
    if !unpositioned_admissions.is_empty() {
        for (observation_id, partition_binding) in &unpositioned_admissions {
            let matching_partitions = scan
                .partitions
                .iter()
                .filter(|partition| {
                    cdf_kernel::partition_schema_observation_id(partition) == observation_id
                        && cdf_kernel::partition_source_identity_binding(partition)
                            .is_ok_and(|expected| &expected == partition_binding)
                })
                .collect::<Vec<_>>();
            if matching_partitions.len() != 1 {
                return Err(CdfError::data(format!(
                    "unpositioned stream-admission observation {observation_id:?} is not bound to a planned partition"
                )));
            }
            let partition = matching_partitions[0];
            if lineage
                .input_observations
                .iter()
                .filter(|observation| {
                    observation.observation_id == *observation_id
                        && observation.partition_id == partition.partition_id
                        && observation.output_position.is_none()
                })
                .count()
                != 1
            {
                return Err(CdfError::data(format!(
                    "unpositioned stream-admission observation {observation_id:?} does not match execution lineage"
                )));
            }
        }
    }
    let quarantine_path = "quarantine/schema-observations.json";
    let (quarantined, quarantine_records) =
        if files.iter().any(|entry| entry.path == quarantine_path) {
            let quarantines: Vec<cdf_kernel::TerminalSchemaObservationQuarantine> = package
                .reader()
                .verified_json_artifact(package.verification(), quarantine_path)?;
            let mut observations = std::collections::BTreeMap::new();
            let mut records = std::collections::BTreeMap::new();
            for quarantine in quarantines {
                quarantine.validate()?;
                cdf_kernel::SchemaHash::new(quarantine.physical_schema_hash().to_string())?;
                let source_position = quarantine.source_position().cloned().ok_or_else(|| {
                    CdfError::data(format!(
                        "schema quarantine {:?} omitted its processed source position",
                        quarantine.observation_id()
                    ))
                })?;
                let observation_id = quarantine.observation_id().to_owned();
                if observations
                    .insert(observation_id.clone(), source_position)
                    .is_some()
                {
                    return Err(CdfError::data(
                        "schema quarantine evidence contains duplicate observation identities",
                    ));
                }
                records.insert(observation_id, quarantine);
            }
            (observations, records)
        } else {
            (
                std::collections::BTreeMap::new(),
                std::collections::BTreeMap::new(),
            )
        };
    let quarantine_admission_path = "quarantine/schema-admission-evidence.json";
    if quarantine_records.is_empty() {
        if files
            .iter()
            .any(|entry| entry.path == quarantine_admission_path)
        {
            return Err(CdfError::data(
                "schema-quarantine admission evidence exists without quarantined observations",
            ));
        }
    } else {
        let evidence: cdf_engine::CompiledSchemaQuarantineEvidence = package
            .reader()
            .verified_json_artifact(package.verification(), quarantine_admission_path)?;
        evidence.validate_admission(&admission)?;
        let mut evidence_ids = std::collections::BTreeSet::new();
        for observation in &evidence.observations {
            let quarantine = quarantine_records
                .get(&observation.observation_id)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "schema-quarantine admission evidence {:?} has no quarantine record",
                        observation.observation_id
                    ))
                })?;
            observation.validate(quarantine)?;
            let physical_observation = evidence
                .physical_observation_catalog
                .get(&observation.physical_observation_hash)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "schema-quarantine admission evidence {:?} references an absent physical observation",
                        observation.observation_id
                    ))
                })?;
            admission.validate_quarantined_observation(quarantine, physical_observation)?;
            evidence_ids.insert(observation.observation_id.as_str());
        }
        if evidence_ids
            != quarantine_records
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>()
        {
            return Err(CdfError::data(
                "schema-quarantine admission evidence does not exactly cover quarantined observations",
            ));
        }
    }
    if admitted.keys().any(|id| quarantined.contains_key(id))
        || partial_admissions
            .keys()
            .any(|id| quarantined.contains_key(id))
        || unpositioned_admissions
            .keys()
            .any(|id| quarantined.contains_key(id))
    {
        return Err(CdfError::data(
            "schema observation is recorded as both admitted and quarantined",
        ));
    }
    let processed_path = cdf_package_contract::PROCESSED_OBSERVATIONS_FILE;
    let processed = if files.iter().any(|entry| entry.path == processed_path) {
        let evidence: cdf_package_contract::ProcessedObservationEvidenceArtifact = package
            .reader()
            .verified_json_artifact(package.verification(), processed_path)?;
        evidence.validate()?;
        Some(evidence)
    } else {
        None
    };
    if admitted.is_empty() && quarantined.is_empty() {
        if processed.is_some() {
            return Err(CdfError::data(
                "processed observations have no matching admission or quarantine evidence",
            ));
        }
        return Ok(());
    }
    let processed = processed.ok_or_else(|| {
        CdfError::data(
            "schema admission/quarantine evidence requires typed processed-observation evidence",
        )
    })?;
    let processed_admitted = processed
        .observations
        .iter()
        .filter(|observation| {
            observation.outcome == cdf_kernel::ProcessedObservationOutcome::Admitted
        })
        .map(|observation| observation.observation_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let processed_quarantined = processed
        .observations
        .iter()
        .filter(|observation| {
            observation.outcome == cdf_kernel::ProcessedObservationOutcome::Quarantined
        })
        .map(|observation| observation.observation_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    if admitted
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        != processed_admitted
        || quarantined
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            != processed_quarantined
    {
        return Err(CdfError::data(
            "processed observation outcomes do not exactly match admission/quarantine evidence",
        ));
    }
    for observation in processed.observations.iter().filter(|observation| {
        observation.outcome == cdf_kernel::ProcessedObservationOutcome::Admitted
    }) {
        if admitted.get(&observation.observation_id) != Some(&observation.source_position) {
            return Err(CdfError::data(format!(
                "processed observation {:?} does not match its stream-admission source position",
                observation.observation_id
            )));
        }
    }
    for observation in processed.observations.iter().filter(|observation| {
        observation.outcome == cdf_kernel::ProcessedObservationOutcome::Quarantined
    }) {
        if quarantined.get(&observation.observation_id) != Some(&observation.source_position) {
            return Err(CdfError::data(format!(
                "processed quarantine {:?} does not match its schema-quarantine source position",
                observation.observation_id
            )));
        }
    }
    Ok(())
}

fn partial_position_matches_partition_scope(
    position: &cdf_kernel::SourcePosition,
    partition: &cdf_kernel::PartitionPlan,
) -> bool {
    match (position, &partition.scope) {
        (
            cdf_kernel::SourcePosition::FileManifest(manifest),
            cdf_kernel::ScopeKey::File { path },
        ) => {
            let Some(cdf_kernel::SourcePosition::FileManifest(planned)) =
                partition.planned_position.as_ref()
            else {
                return false;
            };
            let ([file], [planned_file]) = (manifest.files.as_slice(), planned.files.as_slice())
            else {
                return false;
            };
            file.path.as_str() == path.as_str()
                && planned.version == manifest.version
                && cdf_kernel::merge_file_position_evidence(planned_file, file)
                    .is_ok_and(|merged| merged == *file)
        }
        (_, cdf_kernel::ScopeKey::File { .. }) => false,
        (cdf_kernel::SourcePosition::Cursor(cursor), _) => partition
            .metadata
            .get("cursor_field")
            .is_some_and(|field| field == &cursor.field),
        (cdf_kernel::SourcePosition::Log(log), cdf_kernel::ScopeKey::Stream { name }) => {
            &log.log == name
        }
        (position, _) => partition.start_position.as_ref() == Some(position),
    }
}

fn partial_lineage_matches_exactly(
    lineage: &cdf_engine::LineageSummary,
    observation_id: &str,
    partition: &cdf_kernel::PartitionPlan,
    observed_rows: u64,
    attempted_position: &cdf_kernel::SourcePosition,
) -> bool {
    lineage
        .input_observations
        .iter()
        .filter(|observation| {
            observation.observation_id == observation_id
                && observation.partition_id == partition.partition_id
                && observation.observed_rows == observed_rows
                && observation.output_position.as_ref() == Some(attempted_position)
        })
        .count()
        == 1
}

fn validate_stream_admission_lineage_coverage<'a>(
    admitted: impl Iterator<Item = &'a str>,
    partial: impl Iterator<Item = &'a str>,
    unpositioned: impl Iterator<Item = &'a str>,
    lineage: &cdf_engine::LineageSummary,
) -> Result<()> {
    let evidence_ids = admitted
        .chain(partial)
        .chain(unpositioned)
        .collect::<std::collections::BTreeSet<_>>();
    let mut lineage_ids = std::collections::BTreeSet::new();
    for observation in &lineage.input_observations {
        if observation.observation_id.is_empty()
            || !lineage_ids.insert(observation.observation_id.as_str())
        {
            return Err(CdfError::data(
                "execution lineage contains an empty or duplicate stream-admission observation identity",
            ));
        }
    }
    if evidence_ids != lineage_ids {
        return Err(CdfError::data(
            "stream-admission evidence does not exactly cover execution lineage observations",
        ));
    }
    Ok(())
}

fn replay_package_with_resolved_destination<Store>(
    package: VerifiedPackageReader,
    mut destination: ResolvedProjectDestination,
    checkpoint_store: &Store,
    hooks: PackageReplayHooks<'_>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let inputs = package
        .reader()
        .replay_inputs_verified(package.verification())?;
    validate_resolved_destination_target(&destination, &inputs)?;
    let execution = destination.execution_services().cloned();
    if let Some(execution) = &execution {
        destination.bind_execution_services(execution.clone())?;
    }
    let memory = match execution.as_ref() {
        Some(execution) => execution.memory(),
        None => default_replay_memory()?,
    };
    replay_package_with_runtime(
        package,
        destination.runtime_mut(),
        checkpoint_store,
        memory,
        hooks,
        execution.as_ref(),
    )
}

fn recover_package_with_resolved_destination<Store>(
    package: VerifiedPackageReader,
    mut destination: ResolvedProjectDestination,
    checkpoint_store: &Store,
    receipt: Receipt,
    hooks: PackageReplayHooks<'_>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let inputs = package
        .reader()
        .replay_inputs_verified(package.verification())?;
    validate_resolved_destination_target(&destination, &inputs)?;
    recover_package_with_runtime(
        package,
        destination.runtime_mut(),
        checkpoint_store,
        receipt,
        hooks,
    )
}

fn validate_resolved_destination_target(
    destination: &ResolvedProjectDestination,
    inputs: &PackageReplayInputs,
) -> Result<()> {
    if destination.target() != &inputs.destination_commit.target {
        return Err(CdfError::contract(format!(
            "resolved destination target {} does not match package destination commit target {}",
            destination.target(),
            inputs.destination_commit.target
        )));
    }
    Ok(())
}

pub(crate) fn replay_package_with_runtime<Store>(
    package: VerifiedPackageReader,
    runtime: &mut dyn ProjectDestinationRuntime,
    checkpoint_store: &Store,
    memory: Arc<dyn MemoryCoordinator>,
    hooks: PackageReplayHooks<'_>,
    execution: Option<&ExecutionServices>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    replay_package_with_runtime_and_staged(
        package,
        runtime,
        checkpoint_store,
        memory,
        hooks,
        None,
        execution,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn replay_package_with_runtime_and_staged<Store>(
    mut package: VerifiedPackageReader,
    runtime: &mut dyn ProjectDestinationRuntime,
    checkpoint_store: &Store,
    memory: Arc<dyn MemoryCoordinator>,
    hooks: PackageReplayHooks<'_>,
    active_staged: Option<ActiveStagedIngress>,
    execution: Option<&ExecutionServices>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let inputs = package
        .reader()
        .replay_inputs_verified(package.verification())?;
    validate_package_replay_inputs(package.reader(), &inputs)?;
    let capabilities = runtime.runtime_capabilities();
    capabilities.validate()?;
    runtime.ensure_protocol_ready()?;
    let implemented_ingress_mode = runtime.ingress().mode();
    if capabilities.ingress_mode != implemented_ingress_mode {
        return Err(CdfError::contract(format!(
            "destination {} declares {:?} ingress but implements {:?}",
            runtime.describe().destination_id,
            capabilities.ingress_mode,
            implemented_ingress_mode,
        )));
    }
    let output_schema = package
        .reader()
        .runtime_arrow_schema_verified(package.verification())?;
    // A live staged attempt has already crossed the destination mutation boundary under this
    // exact prepared path. Final package binding supplies identities that did not exist earlier;
    // it must not re-plan writer or batching policy. Artifact-only replay has no live attempt and
    // therefore prepares from the verified package inputs here.
    let selected_bulk_path = match active_staged.as_ref() {
        Some(active) => {
            capabilities.validate_prepared_bulk_path(&active.bulk_path)?;
            active.bulk_path.clone()
        }
        None => runtime.prepare_selected_bulk_path(
            &cdf_runtime::BulkPathPreparationInput::new(output_schema.as_ref())
                .with_commit(&inputs.destination_commit),
        )?,
    };
    let active_staged = active_staged;
    notify_destination_replay_stage(&hooks, PackageReplayStage::PackageReplayVerified)?;

    let checkpoint_id = inputs.state_delta.checkpoint_id.clone();
    propose_or_reuse_exact_checkpoint(checkpoint_store, &inputs.state_delta)?;
    if let Err(error) = notify_destination_replay_stage(
        &hooks,
        PackageReplayStage::CheckpointProposed {
            delta: &inputs.state_delta,
        },
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    if let Err(error) = package.reader_mut().update_status(PackageStatus::Loading) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }
    notify_destination_replay_stage(&hooks, PackageReplayStage::DestinationWriteReady)?;

    let (receipt, receipt_policy, commit_verification) = match capabilities.ingress_mode {
        cdf_runtime::DestinationIngressMode::StagedDurableSegments => {
            let outcome = match match active_staged {
                Some(active) => finalize_active_staged_ingress(
                    runtime,
                    package.reader(),
                    package.verification(),
                    &inputs,
                    active,
                    &hooks,
                ),
                None => commit_package_through_staged_ingress(
                    runtime,
                    &package,
                    &inputs,
                    &capabilities,
                    &selected_bulk_path,
                    &hooks,
                    execution.ok_or_else(|| {
                        CdfError::contract(
                            "artifact replay through externally durable staging requires execution services",
                        )
                    })?,
                ),
            } {
                Ok(outcome) => outcome,
                Err(error) => {
                    let _ = checkpoint_store.abandon(&checkpoint_id);
                    return Err(error);
                }
            };
            (
                outcome.receipt,
                outcome.reporting_policy,
                outcome.verification,
            )
        }
        cdf_runtime::DestinationIngressMode::FinalizedPackageOnly => {
            let prepared_result = match runtime.ingress() {
                cdf_runtime::DestinationIngress::FinalizedPackage(finalized) => finalized
                    .prepare_package_commit(
                        &inputs,
                        &DestinationPlanningContext::new(
                            Arc::new(package.clone()),
                            &selected_bulk_path,
                        ),
                    ),
                cdf_runtime::DestinationIngress::StagedSegments(_) => Err(CdfError::contract(
                    "finalized package reached a staged destination runtime",
                )),
            };
            let mut prepared = match prepared_result {
                Ok(prepared) => prepared,
                Err(error) => {
                    let _ = checkpoint_store.abandon(&checkpoint_id);
                    return Err(error);
                }
            };
            if let Err(error) = prepared.validate_verified_inputs(&inputs) {
                let _ = checkpoint_store.abandon(&checkpoint_id);
                return Err(error);
            }
            if prepared.bulk_path() != &selected_bulk_path {
                let _ = checkpoint_store.abandon(&checkpoint_id);
                return Err(CdfError::contract(
                    "destination prepared a commit for a different bulk path than schema preflight selected",
                ));
            }
            if let Err(error) = capabilities.validate_prepared_bulk_path(prepared.bulk_path()) {
                let _ = checkpoint_store.abandon(&checkpoint_id);
                return Err(error);
            }
            let receipt_policy = prepared.reporting_policy().clone();
            if let Err(error) = notify_destination_replay_stage(
                &hooks,
                PackageReplayStage::DestinationCommitStarted {
                    plan_id: &prepared.plan().plan_id,
                    segment_count: prepared.commit().segments.len(),
                    bulk_path: prepared.bulk_path(),
                },
            ) {
                let _ = checkpoint_store.abandon(&checkpoint_id);
                return Err(error);
            }
            let receipt = match commit_prepared_package_through_session(
                runtime,
                package.reader(),
                &mut prepared,
                memory,
                &hooks,
                package.verification(),
            ) {
                Ok(receipt) => receipt,
                Err(error) => {
                    let _ = checkpoint_store.abandon(&checkpoint_id);
                    return Err(error);
                }
            };
            (
                receipt,
                receipt_policy,
                cdf_runtime::DestinationCommitVerification::Independent,
            )
        }
    };

    if let Err(error) = verify_destination_receipt_before_checkpoint_with_runtime(
        runtime,
        &inputs.state_delta,
        &inputs.destination_commit.target,
        &inputs.destination_commit.disposition,
        &receipt,
        &commit_verification,
    ) {
        let _ = checkpoint_store.abandon(&checkpoint_id);
        return Err(error);
    }

    let package_receipt_recorded = record_package_receipt_once(package.reader(), &receipt)?;
    let receipt_source =
        super::destinations::project_receipt_source(receipt_policy, package_receipt_recorded);
    notify_verified_receipt(&receipt, &hooks)?;

    let checkpoint = checkpoint_store.commit(&inputs.state_delta.checkpoint_id, receipt.clone())?;
    let package_status =
        mark_package_checkpointed_after_commit(package.reader_mut(), &checkpoint, &hooks)?;

    Ok(PackageReplayReport {
        checkpoint,
        receipt,
        receipt_source,
        package_status,
    })
}

pub(crate) fn record_package_receipt_once(
    reader: &PackageReader,
    receipt: &Receipt,
) -> Result<bool> {
    let matching = reader
        .receipts()?
        .into_iter()
        .filter(|existing| existing.receipt_id == receipt.receipt_id)
        .collect::<Vec<_>>();
    match matching.as_slice() {
        [] => {
            reader.append_receipt(receipt.clone())?;
            Ok(true)
        }
        [existing] if logically_equivalent_receipts(existing, receipt) => Ok(false),
        [..] => Err(CdfError::data(format!(
            "package receipt id {} is already recorded with conflicting logical commit evidence",
            receipt.receipt_id
        ))),
    }
}

fn logically_equivalent_receipts(left: &Receipt, right: &Receipt) -> bool {
    left.receipt_id == right.receipt_id
        && left.destination == right.destination
        && left.target == right.target
        && left.package_hash == right.package_hash
        && left.segment_acks == right.segment_acks
        && left.disposition == right.disposition
        && left.idempotency_token == right.idempotency_token
        && left.counts == right.counts
        && left.schema_hash == right.schema_hash
        && left.migrations == right.migrations
}

struct PackageStagedSegmentReader {
    identity: cdf_runtime::StagedSegmentIdentity,
    durable_local_file: Option<cdf_runtime::DurableLocalFile>,
    segment: Option<cdf_package::VerifiedSegmentObject<()>>,
    decoded: Option<cdf_package::VerifiedSegment<()>>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    maximum_segment_bytes: u64,
    next_batch: usize,
}

struct PackageStagingStream<'a> {
    segments: cdf_package::VerifiedSegmentObjectStream<()>,
    schema_hash: SchemaHash,
    attempt_id: cdf_runtime::LoadAttemptId,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    maximum_segment_bytes: u64,
    next_ordinal: u32,
    in_progress: BTreeMap<SegmentId, cdf_runtime::StagedSegmentIdentity>,
    staged: BTreeMap<u32, cdf_runtime::StagedSegmentIdentity>,
    acknowledge_hook: &'a mut dyn FnMut(&cdf_runtime::StagedSegmentIdentity) -> Result<()>,
}

impl PackageStagingStream<'_> {
    fn finish(self) -> Result<Vec<cdf_runtime::StagedSegmentIdentity>> {
        if !self.in_progress.is_empty() {
            return Err(CdfError::destination(
                "staged destination returned with unacknowledged package segments",
            ));
        }
        Ok(self.staged.into_values().collect())
    }
}

impl cdf_runtime::StagedSegmentStream for PackageStagingStream<'_> {
    fn next_segment(&mut self) -> Result<Option<cdf_runtime::StagedSegmentRequest>> {
        let Some(segment) = self.segments.next() else {
            return Ok(None);
        };
        let ordinal = self.next_ordinal;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("staged package has too many segments"))?;
        let identity = cdf_runtime::StagedSegmentIdentity::from_manifest_entry(
            &segment.entry,
            self.schema_hash.clone(),
            ordinal,
        )?;
        if self
            .in_progress
            .insert(identity.segment_id.clone(), identity.clone())
            .is_some()
        {
            return Err(CdfError::destination(
                "staged package stream produced a duplicate in-flight segment",
            ));
        }
        let display_path = segment.display_path().to_path_buf();
        let local_file = segment.open_file()?;
        cdf_runtime::StagedSegmentRequest::new(
            identity.clone(),
            Box::new(PackageStagedSegmentReader {
                identity,
                durable_local_file: Some(cdf_runtime::DurableLocalFile::new(
                    display_path,
                    local_file,
                )),
                segment: Some(segment),
                decoded: None,
                memory: Arc::clone(&self.memory),
                maximum_segment_bytes: self.maximum_segment_bytes,
                next_batch: 0,
            }),
        )
        .map(Some)
    }

    fn acknowledge(&mut self, acknowledgement: cdf_runtime::StagedSegmentAck) -> Result<()> {
        let expected = self
            .in_progress
            .remove(&acknowledgement.identity.segment_id)
            .ok_or_else(|| {
                CdfError::destination(
                    "staged destination acknowledged a package segment that is not in flight",
                )
            })?;
        if acknowledgement.attempt_id != self.attempt_id || acknowledgement.identity != expected {
            return Err(CdfError::destination(
                "staged ingress acknowledgement did not bind the exact durable segment",
            ));
        }
        (self.acknowledge_hook)(&expected)?;
        if self.staged.insert(expected.ordinal, expected).is_some() {
            return Err(CdfError::destination(
                "staged package stream acknowledged a duplicate segment ordinal",
            ));
        }
        Ok(())
    }
}

impl cdf_runtime::DurableSegmentReader for PackageStagedSegmentReader {
    fn identity(&self) -> &cdf_runtime::StagedSegmentIdentity {
        &self.identity
    }

    fn take_durable_local_file(&mut self) -> Result<Option<cdf_runtime::DurableLocalFile>> {
        Ok(self.durable_local_file.take())
    }

    fn next_batch(&mut self) -> Result<Option<arrow_array::RecordBatch>> {
        if self.decoded.is_none() {
            let segment = self
                .segment
                .take()
                .ok_or_else(|| CdfError::internal("verified staged segment object is absent"))?
                .read(Arc::clone(&self.memory), self.maximum_segment_bytes)?;
            self.decoded = Some(segment);
        }
        let batch = self
            .decoded
            .as_ref()
            .expect("decoded segment initialized above")
            .batches
            .get(self.next_batch)
            .cloned();
        self.next_batch = self.next_batch.saturating_add(usize::from(batch.is_some()));
        Ok(batch)
    }
}

fn commit_package_through_staged_ingress(
    runtime: &mut dyn ProjectDestinationRuntime,
    package: &VerifiedPackageReader,
    inputs: &PackageReplayInputs,
    capabilities: &cdf_runtime::DestinationRuntimeCapabilities,
    bulk_path: &cdf_runtime::PreparedBulkPath,
    hooks: &PackageReplayHooks<'_>,
    services: &ExecutionServices,
) -> Result<cdf_runtime::DestinationCommitOutcome> {
    let reader = package.reader();
    let verified = package.verification();
    runtime.ensure_protocol_ready()?;
    let plan = runtime.protocol().plan_commit(&inputs.destination_commit)?;
    let destination_id = runtime.describe().destination_id;
    let attempt_id = staging_attempt_id(&inputs.state_delta.checkpoint_id, &destination_id)?;
    let output_schema = reader.runtime_arrow_schema_verified(verified)?;
    let memory = services.memory();
    match runtime.ingress() {
        cdf_runtime::DestinationIngress::StagedSegments(staged) => {
            for candidate in staged.staging_cleanup_candidates(&inputs.destination_commit.target)? {
                if let Some(proof) = services.prove_expired_staging_lease(candidate.lease())? {
                    proof.execute(|proof, guard| {
                        staged.cleanup_expired_staging(&candidate, proof, guard)
                    })?;
                }
            }
        }
        cdf_runtime::DestinationIngress::FinalizedPackage(_) => {
            return Err(CdfError::contract(
                "staged package commit reached a finalized destination runtime",
            ));
        }
    }
    let output_arrow_schema_hash = cdf_kernel::canonical_arrow_schema_hash(output_schema.as_ref())?;
    let execution_plan_id = reader.recorded_scan_plan_verified(verified)?.plan_id;
    let scheduling = cdf_runtime::StagingSchedulingContext::new(
        capabilities
            .max_in_flight_segments
            .ok_or_else(|| CdfError::contract("staged ingress omitted its segment bound"))?,
        capabilities
            .max_in_flight_bytes
            .ok_or_else(|| CdfError::contract("staged ingress omitted its byte bound"))?,
    )?;
    let staging_lease = services.acquire_staging_lease(cdf_runtime::StagingLeaseIdentity::new(
        destination_id.clone(),
        inputs.destination_commit.target.clone(),
        attempt_id.clone(),
    ))?;
    let lease_snapshot = match staging_lease.snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => return Err(release_staging_lease_after_error(error, staging_lease)),
    };
    let mutation_guard = match staging_lease.mutation_guard() {
        Ok(guard) => guard,
        Err(error) => return Err(release_staging_lease_after_error(error, staging_lease)),
    };
    let request = cdf_runtime::StagedIngressRequest::new(
        attempt_id.clone(),
        cdf_runtime::StagingAttemptBinding {
            destination_id,
            target: inputs.destination_commit.target.clone(),
            disposition: inputs.destination_commit.disposition.clone(),
            schema_hash: inputs.schema_hash.clone(),
            output_arrow_schema_hash,
            merge_keys: inputs.merge_keys.clone(),
            execution_plan_id,
        },
        lease_snapshot,
        mutation_guard,
        bulk_path.clone(),
        scheduling,
        output_schema.as_ref().clone(),
    );
    let request = match request {
        Ok(request) => request,
        Err(error) => return Err(release_staging_lease_after_error(error, staging_lease)),
    };
    let session = match runtime.ingress() {
        cdf_runtime::DestinationIngress::StagedSegments(staged) => {
            match staged.begin_staged_ingress(request) {
                Ok(session) => session,
                Err(error) => {
                    return Err(release_staging_lease_after_error(error, staging_lease));
                }
            }
        }
        cdf_runtime::DestinationIngress::FinalizedPackage(_) => {
            return Err(release_staging_lease_after_error(
                CdfError::contract("staged package commit reached a finalized destination runtime"),
                staging_lease,
            ));
        }
    };
    let mut session = Some(session);
    let result = (|| {
        notify_destination_replay_stage(
            hooks,
            PackageReplayStage::DestinationCommitStarted {
                plan_id: &plan.plan_id,
                segment_count: inputs.destination_commit.segments.len(),
                bulk_path,
            },
        )?;
        let maximum_segment_bytes = capabilities
            .max_in_flight_bytes
            .expect("validated staged byte bound")
            .min(memory.snapshot().budget_bytes);
        let segments = reader.verified_canonical_segment_object_stream_with(verified)?;
        let mut acknowledge = |identity: &cdf_runtime::StagedSegmentIdentity| {
            let segment_ack = SegmentAck {
                segment_id: identity.segment_id.clone(),
                row_count: identity.row_count,
                byte_count: identity.byte_count,
            };
            notify_destination_replay_stage(
                hooks,
                PackageReplayStage::DestinationSegmentAcknowledged { ack: &segment_ack },
            )
        };
        let mut stream = PackageStagingStream {
            segments,
            schema_hash: inputs.schema_hash.clone(),
            attempt_id: attempt_id.clone(),
            memory: memory.clone(),
            maximum_segment_bytes,
            next_ordinal: 0,
            in_progress: BTreeMap::new(),
            staged: BTreeMap::new(),
            acknowledge_hook: &mut acknowledge,
        };
        session
            .as_mut()
            .expect("staged session remains owned until final binding")
            .stage_stream(&mut stream)?;
        let staged = stream.finish()?;
        let snapshot = session
            .as_ref()
            .expect("staged session remains owned until final binding")
            .snapshot()?;
        if snapshot.attempt_id != attempt_id || snapshot.accepted_segments != staged {
            return Err(CdfError::destination(
                "staged ingress snapshot does not exactly match acknowledged segments",
            ));
        }
        let package = reader.clone().with_verification(verified.clone())?;
        let binding =
            cdf_runtime::VerifiedFinalBinding::from_verified_package(attempt_id, &package, plan)?;
        binding.validate_staged_identities(&staged)?;
        staging_lease.snapshot()?;
        let outcome = session
            .take()
            .expect("staged session is consumed exactly once")
            .bind_final(binding)?;
        Ok(outcome)
    })();
    match result {
        Ok(outcome) => {
            staging_lease.finish()?;
            Ok(outcome)
        }
        Err(mut error) => {
            if let Some(session) = session
                && let Err(cleanup) = session.abort()
            {
                error = attach_cleanup_failure(error, "staged session abort", cleanup);
            }
            if let Err(cleanup) = staging_lease.finish() {
                error = attach_cleanup_failure(error, "staging lease release", cleanup);
            }
            Err(error)
        }
    }
}

fn finalize_active_staged_ingress(
    runtime: &mut dyn ProjectDestinationRuntime,
    reader: &PackageReader,
    verified: &VerifiedPackage,
    inputs: &PackageReplayInputs,
    mut active: ActiveStagedIngress,
    hooks: &PackageReplayHooks<'_>,
) -> Result<cdf_runtime::DestinationCommitOutcome> {
    let result = (|| {
        active.finish_background()?;
        runtime.ensure_protocol_ready()?;
        let plan = runtime.protocol().plan_commit(&inputs.destination_commit)?;
        notify_destination_replay_stage(
            hooks,
            PackageReplayStage::DestinationCommitStarted {
                plan_id: &plan.plan_id,
                segment_count: active.staged.len(),
                bulk_path: &active.bulk_path,
            },
        )?;
        let snapshot = active
            .session
            .as_ref()
            .ok_or_else(|| CdfError::internal("staged session is no longer active"))?
            .snapshot()?;
        if snapshot.attempt_id != active.attempt_id || snapshot.accepted_segments != active.staged {
            return Err(CdfError::destination(
                "staged ingress snapshot does not exactly match acknowledged segments",
            ));
        }
        for identity in &active.staged {
            let ack = SegmentAck {
                segment_id: identity.segment_id.clone(),
                row_count: identity.row_count,
                byte_count: identity.byte_count,
            };
            notify_destination_replay_stage(
                hooks,
                PackageReplayStage::DestinationSegmentAcknowledged { ack: &ack },
            )?;
        }
        let package = reader.clone().with_verification(verified.clone())?;
        let binding =
            cdf_runtime::VerifiedFinalBinding::from_verified_package_with_execution_authority(
                active.attempt_id.clone(),
                active.execution_plan_id.clone(),
                &package,
                plan,
            )?;
        binding.validate_staged_identities(&active.staged)?;
        active.bind_final(binding)
    })();
    match result {
        Ok(outcome) => Ok(outcome),
        Err(error) => match active.abort() {
            Ok(()) => Err(error),
            Err(cleanup) => Err(attach_cleanup_failure(
                error,
                "staged ingress cleanup",
                cleanup,
            )),
        },
    }
}

pub(crate) fn staging_attempt_id(
    checkpoint_id: &CheckpointId,
    destination_id: &DestinationId,
) -> Result<cdf_runtime::LoadAttemptId> {
    let digest = Sha256::digest(format!("{}\0{}", checkpoint_id, destination_id).as_bytes());
    cdf_runtime::LoadAttemptId::new(format!("attempt_{}", hex::encode(&digest[..16])))
}

pub(crate) fn recover_package_with_runtime<Store>(
    mut package: VerifiedPackageReader,
    runtime: &mut dyn ProjectDestinationRuntime,
    checkpoint_store: &Store,
    receipt: Receipt,
    hooks: PackageReplayHooks<'_>,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let inputs = package
        .reader()
        .replay_inputs_verified(package.verification())?;
    validate_package_replay_inputs(package.reader(), &inputs)?;
    verify_destination_receipt_before_checkpoint_with_runtime(
        runtime,
        &inputs.state_delta,
        &inputs.destination_commit.target,
        &inputs.destination_commit.disposition,
        &receipt,
        &cdf_runtime::DestinationCommitVerification::Independent,
    )?;
    record_package_receipt_once(package.reader(), &receipt)?;
    notify_verified_receipt(&receipt, &hooks)?;

    let checkpoint = commit_or_reuse_committed_checkpoint(
        checkpoint_store,
        &inputs.state_delta,
        receipt.clone(),
    )?;
    let package_status =
        mark_package_checkpointed_after_commit(package.reader_mut(), &checkpoint, &hooks)?;

    Ok(PackageReplayReport {
        checkpoint,
        receipt,
        receipt_source: ProjectReceiptSource::SuppliedDurableReceipt,
        package_status,
    })
}

fn commit_prepared_package_through_session(
    runtime: &mut dyn ProjectDestinationRuntime,
    reader: &PackageReader,
    prepared: &mut super::destinations::PreparedDestinationCommit,
    memory: Arc<dyn MemoryCoordinator>,
    hooks: &PackageReplayHooks<'_>,
    verified: &VerifiedPackage,
) -> Result<Receipt> {
    let capabilities = runtime.runtime_capabilities();
    let mut session = match runtime.ingress() {
        cdf_runtime::DestinationIngress::FinalizedPackage(finalized) => {
            finalized.begin_prepared_commit(prepared)?
        }
        cdf_runtime::DestinationIngress::StagedSegments(_) => {
            return Err(CdfError::contract(
                "finalized package commit reached a staged destination runtime",
            ));
        }
    };
    if let Err(error) = session.apply_migrations() {
        let _ = session.abort();
        return Err(error);
    }
    if let Err(error) = write_package_segments_to_session(
        session.as_mut(),
        reader,
        prepared.commit(),
        &capabilities,
        memory,
        hooks,
        verified,
    ) {
        let _ = session.abort();
        return Err(error);
    }
    session.finalize()
}

fn notify_verified_receipt(receipt: &Receipt, hooks: &PackageReplayHooks<'_>) -> Result<()> {
    notify_destination_replay_stage(
        hooks,
        PackageReplayStage::DestinationReceiptRecorded { receipt },
    )?;
    if let Some(hook) = hooks.after_receipt_verified {
        hook(receipt)?;
    }
    Ok(())
}

fn verify_destination_receipt_before_checkpoint_with_runtime(
    runtime: &mut dyn ProjectDestinationRuntime,
    delta: &StateDelta,
    target: &TargetName,
    disposition: &WriteDisposition,
    receipt: &Receipt,
    commit_verification: &cdf_runtime::DestinationCommitVerification,
) -> Result<()> {
    validate_destination_receipt_before_checkpoint(delta, target, disposition, receipt)?;
    let verification = match commit_verification {
        cdf_runtime::DestinationCommitVerification::Independent => {
            runtime.verify_receipt(receipt)?
        }
        cdf_runtime::DestinationCommitVerification::VerifiedAtCommit(verification) => {
            verification.clone()
        }
    };
    if verification.receipt_id != receipt.receipt_id {
        return Err(CdfError::destination(format!(
            "destination verification names receipt {} instead of {}",
            verification.receipt_id, receipt.receipt_id
        )));
    }
    if !verification.verified {
        return Err(CdfError::destination(format!(
            "destination receipt {} did not verify: {}",
            verification.receipt_id,
            verification
                .reason
                .unwrap_or_else(|| "verification returned false".to_owned())
        )));
    }
    Ok(())
}

fn mark_package_checkpointed_after_commit(
    reader: &mut PackageReader,
    checkpoint: &Checkpoint,
    hooks: &PackageReplayHooks<'_>,
) -> Result<PackageStatus> {
    notify_destination_replay_stage(
        hooks,
        PackageReplayStage::CheckpointCommitted { checkpoint },
    )?;
    let package_status = reader
        .update_status(PackageStatus::Checkpointed)?
        .lifecycle
        .status
        .clone();
    notify_destination_replay_stage(
        hooks,
        PackageReplayStage::PackageStatusUpdated {
            status: &package_status,
        },
    )?;
    Ok(package_status)
}

fn write_package_segments_to_session(
    session: &mut dyn cdf_kernel::CommitSession,
    reader: &PackageReader,
    commit: &DestinationCommitRequest,
    capabilities: &cdf_runtime::DestinationRuntimeCapabilities,
    memory: Arc<dyn MemoryCoordinator>,
    hooks: &PackageReplayHooks<'_>,
    verified: &VerifiedPackage,
) -> Result<()> {
    if commit.segments.is_empty() {
        return Ok(());
    }
    let acknowledge = |ack: &SegmentAck| {
        notify_destination_replay_stage(
            hooks,
            PackageReplayStage::DestinationSegmentAcknowledged { ack },
        )
    };
    let budget = memory.snapshot().budget_bytes;
    let maximum_segment_bytes = capabilities
        .max_in_flight_bytes
        .unwrap_or(64 * 1024 * 1024)
        .min(budget);
    let stream = reader.verified_commit_segment_stream_with(
        verified,
        &commit.segments,
        memory,
        maximum_segment_bytes,
    )?;
    let segments = stream.map(|segment| segment.and_then(|segment| segment.into_commit_segment()));
    let acknowledgements = session.write_segments(Box::new(segments))?;
    validate_finalized_segment_acknowledgements(&commit.segments, &acknowledgements)?;
    for acknowledgement in &acknowledgements {
        acknowledge(acknowledgement)?;
    }
    Ok(())
}

fn validate_finalized_segment_acknowledgements(
    expected: &[cdf_kernel::StateSegment],
    acknowledgements: &[SegmentAck],
) -> Result<()> {
    if acknowledgements.len() != expected.len() {
        return Err(CdfError::destination(format!(
            "finalized destination acknowledged {} segments but the commit requires {}",
            acknowledgements.len(),
            expected.len()
        )));
    }
    for (index, (acknowledgement, segment)) in acknowledgements.iter().zip(expected).enumerate() {
        if acknowledgement.segment_id != segment.segment_id {
            return Err(CdfError::destination(format!(
                "finalized destination acknowledgement {index} names segment {} but the commit requires {}",
                acknowledgement.segment_id.as_str(),
                segment.segment_id.as_str()
            )));
        }
        if acknowledgement.row_count != segment.row_count {
            return Err(CdfError::destination(format!(
                "finalized destination acknowledgement for segment {} reports {} rows but the commit requires {}",
                segment.segment_id.as_str(),
                acknowledgement.row_count,
                segment.row_count
            )));
        }
    }
    Ok(())
}

fn default_replay_memory() -> Result<Arc<dyn MemoryCoordinator>> {
    Ok(Arc::new(DeterministicMemoryCoordinator::new(
        DEFAULT_PROCESS_BUDGET_BYTES,
        Default::default(),
    )?))
}

fn validate_package_replay_inputs(
    reader: &PackageReader,
    inputs: &PackageReplayInputs,
) -> Result<cdf_package_contract::ReplayView> {
    let replay = reader.replay_view()?;
    if replay.package_hash != inputs.state_delta.package_hash {
        return Err(CdfError::data(format!(
            "package hash {} does not match StateDelta package hash {}",
            replay.package_hash, inputs.state_delta.package_hash
        )));
    }
    if inputs.schema_hash != inputs.state_delta.schema_hash {
        return Err(CdfError::contract(format!(
            "destination schema hash {} does not match StateDelta schema hash {}",
            inputs.schema_hash, inputs.state_delta.schema_hash
        )));
    }
    if inputs.destination_commit.package_hash != inputs.state_delta.package_hash {
        return Err(CdfError::contract(format!(
            "destination commit package hash {} does not match StateDelta package hash {}",
            inputs.destination_commit.package_hash, inputs.state_delta.package_hash
        )));
    }
    if inputs.destination_commit.segments != inputs.state_delta.segments {
        return Err(CdfError::contract(
            "destination commit segments do not match StateDelta segments",
        ));
    }
    if inputs.destination_commit.idempotency_token.as_str()
        != inputs.state_delta.package_hash.as_str()
    {
        return Err(CdfError::contract(format!(
            "destination commit idempotency token {} does not match package hash {}",
            inputs.destination_commit.idempotency_token, inputs.state_delta.package_hash
        )));
    }
    validate_package_segments_match_delta(reader, &replay.segments, &inputs.state_delta)?;
    Ok(replay)
}

fn notify_runtime_replay_stage(
    hook: Option<DestinationReplayStageHook<'_>>,
    stage: PackageReplayStage<'_>,
) -> Result<()> {
    let Some(hook) = hook else {
        return Ok(());
    };
    match stage {
        PackageReplayStage::PackageReplayVerified => hook(RuntimeStage::PackageReplayVerified),
        PackageReplayStage::CheckpointProposed { delta } => {
            hook(RuntimeStage::CheckpointProposed { delta })
        }
        PackageReplayStage::DestinationWriteReady => hook(RuntimeStage::DestinationWriteReady),
        PackageReplayStage::DestinationCommitStarted {
            plan_id,
            segment_count,
            bulk_path,
        } => hook(RuntimeStage::DestinationCommitStarted {
            plan_id,
            segment_count,
            bulk_path,
        }),
        PackageReplayStage::DestinationSegmentAcknowledged { ack } => {
            hook(RuntimeStage::DestinationSegmentAcknowledged { ack })
        }
        PackageReplayStage::DestinationReceiptRecorded { receipt } => {
            hook(RuntimeStage::DestinationReceiptRecorded { receipt })
        }
        PackageReplayStage::CheckpointCommitted { checkpoint } => {
            hook(RuntimeStage::CheckpointCommitted { checkpoint })
        }
        PackageReplayStage::PackageStatusUpdated { status } => {
            hook(RuntimeStage::PackageStatusUpdated { status })
        }
    }
}

fn notify_destination_replay_stage(
    hooks: &PackageReplayHooks<'_>,
    stage: PackageReplayStage<'_>,
) -> Result<()> {
    if let Some(hook) = hooks.stage {
        hook(stage)?;
    }
    Ok(())
}

fn commit_or_reuse_committed_checkpoint<Store>(
    checkpoint_store: &Store,
    delta: &StateDelta,
    receipt: Receipt,
) -> Result<Checkpoint>
where
    Store: CheckpointStore + ?Sized,
{
    match checkpoint_store.commit(&delta.checkpoint_id, receipt.clone()) {
        Ok(checkpoint) => Ok(checkpoint),
        Err(error) => {
            let Some(head) =
                checkpoint_store.head(&delta.pipeline_id, &delta.resource_id, &delta.scope)?
            else {
                return Err(error);
            };
            if head.status == CheckpointStatus::Committed
                && head.is_head
                && head.delta == *delta
                && head.receipt.as_ref() == Some(&receipt)
            {
                Ok(head)
            } else {
                Err(error)
            }
        }
    }
}

fn propose_or_reuse_exact_checkpoint<Store>(
    checkpoint_store: &Store,
    delta: &StateDelta,
) -> Result<Checkpoint>
where
    Store: CheckpointStore + ?Sized,
{
    match checkpoint_store.propose(delta.clone()) {
        Ok(checkpoint) => Ok(checkpoint),
        Err(propose_error) => {
            let existing = checkpoint_store
                .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)?
                .into_iter()
                .find(|checkpoint| checkpoint.delta.checkpoint_id == delta.checkpoint_id);
            match existing {
                Some(checkpoint)
                    if checkpoint.status == CheckpointStatus::Proposed
                        && !checkpoint.is_head
                        && checkpoint.receipt.is_none()
                        && checkpoint.delta == *delta =>
                {
                    Ok(checkpoint)
                }
                Some(checkpoint) => Err(CdfError::contract(format!(
                    "checkpoint {} already exists but is not the exact reusable proposal: status={:?}, is_head={}, receipt_present={}, delta_matches={}",
                    delta.checkpoint_id,
                    checkpoint.status,
                    checkpoint.is_head,
                    checkpoint.receipt.is_some(),
                    checkpoint.delta == *delta,
                ))),
                None => Err(propose_error),
            }
        }
    }
}

fn validate_package_segments_match_delta(
    reader: &PackageReader,
    package_segments: &[SegmentEntry],
    state_delta: &StateDelta,
) -> Result<()> {
    let state_segments = &state_delta.segments;
    if state_segments.is_empty() {
        if !package_segments.is_empty() {
            return Err(CdfError::contract(
                "zero-segment StateDelta cannot cover package data segments",
            ));
        }
        let processed = reader.processed_observation_evidence()?.ok_or_else(|| {
            CdfError::contract(
                "zero-segment StateDelta requires typed processed-observation package evidence",
            )
        })?;
        processed.validate()?;
        if processed.input_position != state_delta.input_position
            || processed.output_position != state_delta.output_position
        {
            return Err(CdfError::contract(
                "processed-observation package evidence does not match StateDelta positions",
            ));
        }
        return Ok(());
    }
    if package_segments.len() != state_segments.len() {
        return Err(CdfError::data(format!(
            "package has {} segment(s) but StateDelta has {} segment(s)",
            package_segments.len(),
            state_segments.len()
        )));
    }

    let package_by_id = package_segments
        .iter()
        .map(|segment| (&segment.segment_id, segment))
        .collect::<BTreeMap<_, _>>();
    if package_by_id.len() != package_segments.len() {
        return Err(CdfError::data(
            "package manifest contains duplicate segment ids",
        ));
    }

    let mut seen_state_segments = BTreeSet::<&SegmentId>::new();
    for segment in state_segments {
        if !seen_state_segments.insert(&segment.segment_id) {
            return Err(CdfError::contract(format!(
                "StateDelta contains duplicate segment {}",
                segment.segment_id
            )));
        }
        let Some(package_segment) = package_by_id.get(&segment.segment_id) else {
            return Err(CdfError::data(format!(
                "StateDelta segment {} is not present in the package manifest",
                segment.segment_id
            )));
        };
        if package_segment.row_count != segment.row_count
            || package_segment.byte_count != segment.byte_count
        {
            return Err(CdfError::data(format!(
                "StateDelta segment {} has {} rows/{} bytes but package manifest has {} rows/{} bytes",
                segment.segment_id,
                segment.row_count,
                segment.byte_count,
                package_segment.row_count,
                package_segment.byte_count
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod stream_admission_replay_tests {
    use std::collections::BTreeMap;

    use cdf_kernel::{
        CursorPosition, CursorValue, FileManifest, FilePosition, PartitionId, PartitionPlan,
        ScopeKey, SegmentAck, SegmentId, SourcePosition, StateSegment,
    };

    use cdf_engine::{LineageInputObservation, LineageSummary};

    use super::{
        partial_lineage_matches_exactly, partial_position_matches_partition_scope,
        validate_finalized_segment_acknowledgements, validate_stream_admission_lineage_coverage,
    };

    fn state_segment(id: &str, rows: u64) -> StateSegment {
        StateSegment {
            segment_id: SegmentId::new(id).unwrap(),
            scope: ScopeKey::Resource,
            output_position: SourcePosition::FileManifest(FileManifest {
                version: 1,
                files: Vec::new(),
            }),
            row_count: rows,
            byte_count: 100,
        }
    }

    fn acknowledgement(id: &str, rows: u64) -> SegmentAck {
        SegmentAck {
            segment_id: SegmentId::new(id).unwrap(),
            row_count: rows,
            byte_count: 200,
        }
    }

    #[test]
    fn finalized_acknowledgements_require_exact_order_cardinality_and_rows() {
        let expected = [
            state_segment("segment-1", 7),
            state_segment("segment-2", 11),
        ];
        validate_finalized_segment_acknowledgements(
            &expected,
            &[
                acknowledgement("segment-1", 7),
                acknowledgement("segment-2", 11),
            ],
        )
        .unwrap();

        let missing = validate_finalized_segment_acknowledgements(
            &expected,
            &[acknowledgement("segment-1", 7)],
        )
        .unwrap_err();
        assert!(missing.to_string().contains("acknowledged 1 segments"));

        let reordered = validate_finalized_segment_acknowledgements(
            &expected,
            &[
                acknowledgement("segment-2", 11),
                acknowledgement("segment-1", 7),
            ],
        )
        .unwrap_err();
        assert!(reordered.to_string().contains("acknowledgement 0"));

        let wrong_rows = validate_finalized_segment_acknowledgements(
            &expected,
            &[
                acknowledgement("segment-1", 8),
                acknowledgement("segment-2", 11),
            ],
        )
        .unwrap_err();
        assert!(wrong_rows.to_string().contains("reports 8 rows"));
    }

    #[test]
    fn partial_position_binding_rejects_wrong_file_generation_and_cursor_field() {
        let file_position = |etag: &str| {
            SourcePosition::FileManifest(FileManifest {
                version: 1,
                files: vec![FilePosition {
                    path: "events.json".to_owned(),
                    size_bytes: 12,
                    source_generation: None,
                    etag: Some(etag.to_owned()),
                    object_version: Some("v1".to_owned()),
                    sha256: Some("abc".to_owned()),
                }],
            })
        };
        let file_partition = PartitionPlan {
            partition_id: PartitionId::new("file").unwrap(),
            scope: ScopeKey::File {
                path: "events.json".to_owned(),
            },
            planned_position: Some(file_position("etag-1")),
            start_position: None,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::ImmutableContent,
            metadata: BTreeMap::new(),
        };
        assert!(partial_position_matches_partition_scope(
            &file_position("etag-1"),
            &file_partition
        ));
        assert!(!partial_position_matches_partition_scope(
            &file_position("etag-2"),
            &file_partition
        ));

        let cursor_partition = PartitionPlan {
            partition_id: PartitionId::new("rest").unwrap(),
            scope: ScopeKey::Partition {
                partition_id: PartitionId::new("rest").unwrap(),
            },
            planned_position: None,
            start_position: None,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::from([("cursor_field".to_owned(), "updated_at".to_owned())]),
        };
        let cursor_position = |field: &str| {
            SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: field.to_owned(),
                value: CursorValue::I64(7),
            })
        };
        assert!(partial_position_matches_partition_scope(
            &cursor_position("updated_at"),
            &cursor_partition
        ));
        assert!(!partial_position_matches_partition_scope(
            &cursor_position("wrong"),
            &cursor_partition
        ));

        let attempted = cursor_position("updated_at");
        let lineage = LineageSummary {
            input_partitions: vec![PartitionId::new("rest").unwrap()],
            input_rows: 7,
            input_observations: vec![LineageInputObservation {
                observation_id: "rest".to_owned(),
                partition_id: PartitionId::new("rest").unwrap(),
                observed_rows: 7,
                output_position: Some(attempted.clone()),
            }],
            output_segments: Vec::new(),
        };
        assert!(partial_lineage_matches_exactly(
            &lineage,
            "rest",
            &cursor_partition,
            7,
            &attempted,
        ));
        assert!(!partial_lineage_matches_exactly(
            &lineage,
            "rest",
            &cursor_partition,
            6,
            &attempted,
        ));
        assert!(!partial_lineage_matches_exactly(
            &lineage,
            "rest",
            &cursor_partition,
            7,
            &SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "updated_at".to_owned(),
                value: CursorValue::I64(8),
            }),
        ));

        validate_stream_admission_lineage_coverage(
            std::iter::empty(),
            ["rest"].into_iter(),
            std::iter::empty(),
            &lineage,
        )
        .unwrap();
        let error = validate_stream_admission_lineage_coverage(
            std::iter::empty(),
            std::iter::empty(),
            std::iter::empty(),
            &lineage,
        )
        .unwrap_err();
        assert!(error.to_string().contains("exactly cover"), "{error}");

        let mut duplicate_lineage = lineage;
        duplicate_lineage
            .input_observations
            .push(LineageInputObservation {
                observation_id: "rest".to_owned(),
                partition_id: PartitionId::new("rest-page-2").unwrap(),
                observed_rows: 1,
                output_position: Some(attempted),
            });
        let error = validate_stream_admission_lineage_coverage(
            std::iter::empty(),
            ["rest"].into_iter(),
            std::iter::empty(),
            &duplicate_lineage,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("duplicate stream-admission"),
            "{error}"
        );
    }
}
