use cdf_kernel::{
    CdfError, DrainTermination, EPOCH_CLOSURE_EVIDENCE_VERSION, EPOCH_FRONTIER_VERSION,
    EpochClosureCause, EpochClosureEvidence, EpochClosureObservation, EpochClosureTrigger,
    EpochFrontier, ExecutionExtent, PartitionWatermarkState, Result, STREAM_EPOCH_POLICY_VERSION,
    SourcePosition, StreamEpochPolicy, WatermarkClaim, WatermarkPolicy, WatermarkValue,
    validate_partition_watermark_states,
};

/// One canonical point at which every admitted source position at or below
/// `frontier` has drained from the operator graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainSafeFrontierObservation {
    pub frontier: SourcePosition,
    pub carryover: Option<SourcePosition>,
    pub admitted_batches: u64,
    pub admitted_rows: u64,
    pub admitted_bytes: u64,
    pub admitted_positions: u64,
    pub global_watermark: Option<WatermarkClaim>,
    pub source_exhausted: bool,
    pub monotonic_milliseconds: u64,
    pub observed_at_unix_milliseconds: u64,
}

impl DrainSafeFrontierObservation {
    fn validate(&self) -> Result<()> {
        self.frontier.validate()?;
        if let Some(carryover) = &self.carryover {
            carryover.validate()?;
        }
        if let Some(watermark) = &self.global_watermark {
            watermark.validate()?;
        }
        if self.observed_at_unix_milliseconds == 0 {
            return Err(CdfError::contract(
                "drain safe-frontier observation time must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainEpochClosure {
    pub frontier: EpochFrontier,
    pub evidence: EpochClosureEvidence,
    pub observed_at_unix_milliseconds: u64,
    pub terminate_after_settlement: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DrainEpochDecision {
    Continue,
    Close(Box<DrainEpochClosure>),
    FinishedNoOp,
}

/// Every magnitude an [`EpochClosureTrigger`] can be evaluated against, at one instant.
///
/// This exists so the epoch controller and any component that must anticipate a closure compare
/// against **one** definition of "has this trigger been reached". Two independent evaluators of the
/// same policy is the drift hazard the CDC foundation names directly: separate pattern matches with
/// subtly different semantics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EpochTriggerMagnitudes {
    pub batches: u64,
    pub rows: u64,
    pub bytes: u64,
    /// Elapsed milliseconds since the epoch began.
    pub elapsed_milliseconds: u64,
    /// Watermark advance since the epoch's start claim.
    ///
    /// `None` when no advance is measurable — no epoch-start claim, no current claim, or two claims
    /// whose value domains cannot be differenced. An unmeasurable magnitude never trips a trigger,
    /// which keeps a missing watermark from silently forcing or suppressing closure.
    pub watermark_advance: Option<u64>,
}

impl EpochTriggerMagnitudes {
    /// The magnitude this trigger measures, or `None` when it is not currently measurable.
    #[must_use]
    pub const fn measured(&self, trigger: &EpochClosureTrigger) -> Option<u64> {
        match trigger {
            EpochClosureTrigger::Batches { .. } => Some(self.batches),
            EpochClosureTrigger::Rows { .. } => Some(self.rows),
            EpochClosureTrigger::Bytes { .. } => Some(self.bytes),
            EpochClosureTrigger::Elapsed { .. } => Some(self.elapsed_milliseconds),
            EpochClosureTrigger::WatermarkAdvance { .. } => self.watermark_advance,
        }
    }

    /// Whether this trigger has been reached.
    #[must_use]
    pub fn trips(&self, trigger: &EpochClosureTrigger) -> bool {
        self.measured(trigger)
            .is_some_and(|observed| observed >= trigger.threshold())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counts {
    batches: u64,
    rows: u64,
    bytes: u64,
    positions: u64,
}

impl Counts {
    fn checked_add(&mut self, observation: &DrainSafeFrontierObservation) -> Result<()> {
        let batches = self
            .batches
            .checked_add(observation.admitted_batches)
            .ok_or_else(|| CdfError::internal("drain epoch batch count overflow"))?;
        let rows = self
            .rows
            .checked_add(observation.admitted_rows)
            .ok_or_else(|| CdfError::internal("drain epoch row count overflow"))?;
        let bytes = self
            .bytes
            .checked_add(observation.admitted_bytes)
            .ok_or_else(|| CdfError::internal("drain epoch byte count overflow"))?;
        let positions = self
            .positions
            .checked_add(observation.admitted_positions)
            .ok_or_else(|| CdfError::internal("drain epoch position count overflow"))?;
        self.batches = batches;
        self.rows = rows;
        self.bytes = bytes;
        self.positions = positions;
        Ok(())
    }

    const fn is_empty(self) -> bool {
        self.positions == 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ControllerState {
    Open,
    AwaitingSettlement(Box<DrainEpochClosure>),
    Finished,
}

/// Runtime-owned finite epoch gate for one drain execution.
///
/// The controller sees only canonical safe frontiers. Once it requests a
/// close, it rejects further observations until the caller proves that exact
/// frontier was package-verified, receipt-verified, and checkpoint-committed
/// through [`Self::acknowledge_settlement`]. This makes later source progress
/// structurally impossible through the API while an epoch is unsettled.
pub struct DrainEpochController {
    policy: StreamEpochPolicy,
    termination: DrainTermination,
    state: ControllerState,
    epoch_ordinal: u64,
    epoch: Counts,
    total: Counts,
    command_started_monotonic_milliseconds: u64,
    epoch_started_monotonic_milliseconds: u64,
    last_monotonic_milliseconds: Option<u64>,
    committed_frontier: Option<SourcePosition>,
    committed_source_continuation: Option<SourcePosition>,
    committed_watermark: Option<WatermarkClaim>,
    committed_partition_watermarks: Vec<PartitionWatermarkState>,
    pending_partition_watermarks: Option<Vec<PartitionWatermarkState>>,
    epoch_watermark_start: Option<WatermarkClaim>,
    last_observed_watermark: Option<WatermarkClaim>,
    last_safe_frontier: Option<DrainSafeFrontierObservation>,
}

impl DrainEpochController {
    pub fn new(extent: &ExecutionExtent) -> Result<Self> {
        let ExecutionExtent::Drain {
            policy,
            termination,
            ..
        } = extent
        else {
            return Err(CdfError::contract(
                "drain epoch controller requires a drain execution extent",
            ));
        };
        extent.validate_for_plan()?;
        Ok(Self {
            policy: policy.clone(),
            termination: termination.clone(),
            state: ControllerState::Open,
            epoch_ordinal: 0,
            epoch: Counts::default(),
            total: Counts::default(),
            // The controller clock is elapsed time since the drain began. Starting at zero keeps
            // time-trigger accounting honest even when the first canonical safe frontier is
            // reached only after a long-running partition has drained.
            command_started_monotonic_milliseconds: 0,
            epoch_started_monotonic_milliseconds: 0,
            last_monotonic_milliseconds: Some(0),
            committed_frontier: None,
            committed_source_continuation: None,
            committed_watermark: None,
            committed_partition_watermarks: Vec::new(),
            pending_partition_watermarks: None,
            epoch_watermark_start: None,
            last_observed_watermark: None,
            last_safe_frontier: None,
        })
    }

    pub const fn epoch_ordinal(&self) -> u64 {
        self.epoch_ordinal
    }

    pub fn committed_frontier(&self) -> Option<&SourcePosition> {
        self.committed_frontier.as_ref()
    }

    pub fn committed_source_continuation(&self) -> Option<&SourcePosition> {
        self.committed_source_continuation.as_ref()
    }

    /// Receipt-gated global completeness floor inherited by the next epoch.
    pub fn committed_watermark(&self) -> Option<&WatermarkClaim> {
        self.committed_watermark.as_ref()
    }

    pub fn committed_partition_watermarks(&self) -> &[PartitionWatermarkState] {
        &self.committed_partition_watermarks
    }

    /// Strongest global completeness claim observed by the open epoch.
    ///
    /// Batch admission compares event time against this value before admitting the batch's new
    /// claim, so a source cannot classify its own rows using a future completeness assertion.
    pub fn late_data_watermark(&self) -> Option<&WatermarkClaim> {
        self.last_observed_watermark
            .as_ref()
            .or(self.committed_watermark.as_ref())
    }

    /// Seeds the input-low frontier and next package ordinal from the durable prefix recovered
    /// before this process admits source work. Record/byte termination counters remain
    /// invocation-local; only package identity and source-position authority resume.
    pub fn bind_initial_committed_state(
        &mut self,
        committed_frontier: Option<SourcePosition>,
        committed_source_continuation: Option<SourcePosition>,
        committed_watermark: Option<WatermarkClaim>,
        committed_partition_watermarks: Vec<PartitionWatermarkState>,
        next_epoch_ordinal: u64,
    ) -> Result<()> {
        if !matches!(self.state, ControllerState::Open)
            || self.epoch_ordinal != 0
            || !self.epoch.is_empty()
            || !self.total.is_empty()
            || self.committed_frontier.is_some()
            || self.committed_source_continuation.is_some()
            || self.committed_watermark.is_some()
            || !self.committed_partition_watermarks.is_empty()
            || self.pending_partition_watermarks.is_some()
        {
            return Err(CdfError::contract(
                "initial drain frontier must be bound before the first source observation",
            ));
        }
        if let Some(frontier) = &committed_frontier {
            frontier.validate()?;
        }
        if let Some(continuation) = &committed_source_continuation {
            continuation.validate()?;
        }
        if let Some(watermark) = &committed_watermark {
            self.observe_watermark(Some(watermark))?;
        }
        validate_partition_watermark_states(&committed_partition_watermarks)?;
        if next_epoch_ordinal != 0 && committed_frontier.is_none() {
            return Err(CdfError::contract(
                "recovered drain epoch ordinal requires a committed source frontier",
            ));
        }
        self.committed_frontier = committed_frontier;
        self.committed_source_continuation = committed_source_continuation;
        self.committed_watermark = committed_watermark.clone();
        self.committed_partition_watermarks = committed_partition_watermarks;
        self.epoch_watermark_start = committed_watermark.clone();
        self.last_observed_watermark = committed_watermark;
        self.epoch_ordinal = next_epoch_ordinal;
        Ok(())
    }

    pub fn stage_partition_watermarks(
        &mut self,
        states: Vec<PartitionWatermarkState>,
    ) -> Result<()> {
        if !matches!(self.state, ControllerState::AwaitingSettlement(_))
            || self.pending_partition_watermarks.is_some()
        {
            return Err(CdfError::contract(
                "partition watermark state requires one unstaged pending epoch closure",
            ));
        }
        validate_partition_watermark_states(&states)?;
        for previous in &self.committed_partition_watermarks {
            let next = states
                .binary_search_by(|candidate| candidate.partition_id.cmp(&previous.partition_id))
                .ok()
                .and_then(|index| states.get(index))
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "pending epoch cannot erase watermark state for partition `{}`",
                        previous.partition_id
                    ))
                })?;
            next.validate_monotone_successor(previous)?;
        }
        self.pending_partition_watermarks = Some(states);
        Ok(())
    }

    pub const fn monotonic_milliseconds(&self) -> u64 {
        match self.last_monotonic_milliseconds {
            Some(value) => value,
            None => 0,
        }
    }

    /// Advances the command clock while source admission is paused for package settlement.
    /// This keeps command-duration termination honest without charging destination/checkpoint
    /// latency to the next epoch's cadence interval.
    pub fn advance_monotonic_clock(&mut self, monotonic_milliseconds: u64) -> Result<()> {
        self.observe_clock(monotonic_milliseconds)
    }

    pub fn pending_closure(&self) -> Option<&DrainEpochClosure> {
        match &self.state {
            ControllerState::AwaitingSettlement(closure) => Some(closure),
            ControllerState::Open | ControllerState::Finished => None,
        }
    }

    /// Returns the exact remaining host-monotonic delay before a time policy can change the
    /// controller decision. Non-time policies do not manufacture a polling cadence.
    pub fn next_timer_delay_milliseconds(&self) -> Result<Option<u64>> {
        self.validate_ready_for_epoch()?;
        let now = self.monotonic_milliseconds();
        let mut deadline = match &self.termination {
            DrainTermination::Duration { milliseconds } => Some(
                self.command_started_monotonic_milliseconds
                    .checked_add(*milliseconds)
                    .ok_or_else(|| CdfError::internal("drain command deadline overflow"))?,
            ),
            _ => None,
        };
        if !self.epoch.is_empty() {
            for trigger in [
                &self.policy.package_rotation,
                &self.policy.checkpoint_cadence,
            ] {
                if let EpochClosureTrigger::Elapsed { milliseconds } = trigger {
                    let candidate = self
                        .epoch_started_monotonic_milliseconds
                        .checked_add(*milliseconds)
                        .ok_or_else(|| CdfError::internal("drain epoch deadline overflow"))?;
                    deadline = Some(deadline.map_or(candidate, |current| current.min(candidate)));
                }
            }
        }
        Ok(deadline.map(|deadline| deadline.saturating_sub(now)))
    }

    /// Observes a host timer without inventing new source progress. A nonempty epoch may close
    /// only at its last recorded canonical safe frontier; an empty duration-bounded drain becomes
    /// a verified no-op.
    pub fn observe_timer(
        &mut self,
        monotonic_milliseconds: u64,
        observed_at_unix_milliseconds: u64,
    ) -> Result<DrainEpochDecision> {
        self.validate_ready_for_epoch()?;
        if observed_at_unix_milliseconds == 0 {
            return Err(CdfError::contract(
                "drain timer observation time must be greater than zero",
            ));
        }
        let Some(last) = self.last_safe_frontier.clone() else {
            self.observe_clock(monotonic_milliseconds)?;
            if let DrainTermination::Duration { milliseconds } = &self.termination
                && self.command_elapsed(monotonic_milliseconds)? >= *milliseconds
            {
                self.state = ControllerState::Finished;
                return Ok(DrainEpochDecision::FinishedNoOp);
            }
            return Ok(DrainEpochDecision::Continue);
        };
        self.observe_safe_frontier(DrainSafeFrontierObservation {
            frontier: last.frontier,
            carryover: last.carryover,
            admitted_batches: 0,
            admitted_rows: 0,
            admitted_bytes: 0,
            admitted_positions: 0,
            global_watermark: last.global_watermark,
            source_exhausted: false,
            monotonic_milliseconds,
            observed_at_unix_milliseconds,
        })
    }

    /// Completes a drain whose source exhausted without exposing any processable position.
    /// Empty source exhaustion is a successful no-op and carries no invented frontier.
    pub fn finish_empty_source(&mut self, monotonic_milliseconds: u64) -> Result<()> {
        self.validate_ready_for_epoch()?;
        self.observe_clock(monotonic_milliseconds)?;
        if !self.epoch.is_empty() || self.last_safe_frontier.is_some() {
            return Err(CdfError::contract(
                "empty-source completion cannot discard an observed drain frontier",
            ));
        }
        self.state = ControllerState::Finished;
        Ok(())
    }

    pub const fn is_finished(&self) -> bool {
        matches!(self.state, ControllerState::Finished)
    }

    pub fn validate_ready_for_epoch(&self) -> Result<()> {
        match &self.state {
            ControllerState::Open => Ok(()),
            ControllerState::AwaitingSettlement(closure) => Err(CdfError::contract(format!(
                "drain epoch {} cannot admit later progress before frontier settlement",
                closure.frontier.epoch_ordinal
            ))),
            ControllerState::Finished => Err(CdfError::contract(
                "finished drain execution cannot admit another source frontier",
            )),
        }
    }

    pub fn observe_safe_frontier(
        &mut self,
        mut observation: DrainSafeFrontierObservation,
    ) -> Result<DrainEpochDecision> {
        self.validate_ready_for_epoch()?;
        observation.validate()?;
        self.observe_clock(observation.monotonic_milliseconds)?;
        self.observe_watermark(observation.global_watermark.as_ref())?;
        // A missing current observation cannot retract an already admitted completeness floor.
        // Normalize the safe frontier to the controller's monotone authority before it can be
        // retained, tested by a trigger, or serialized into an epoch closure.
        observation.global_watermark = self.last_observed_watermark.clone();
        let mut next_epoch = self.epoch;
        next_epoch.checked_add(&observation)?;
        let mut next_total = self.total;
        next_total.checked_add(&observation)?;
        self.epoch = next_epoch;
        self.total = next_total;
        self.last_safe_frontier = Some(observation.clone());

        let closure = self.closure_at(&observation)?;
        let Some((cause, closure_observation, terminate_after_settlement)) = closure else {
            return Ok(DrainEpochDecision::Continue);
        };
        if self.epoch.is_empty() && terminate_after_settlement {
            self.state = ControllerState::Finished;
            return Ok(DrainEpochDecision::FinishedNoOp);
        }

        let frontier = EpochFrontier {
            version: EPOCH_FRONTIER_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            epoch_ordinal: self.epoch_ordinal,
            frontier: observation.frontier.clone(),
            input_low: self.committed_frontier.clone(),
            input_high: observation.frontier,
            carryover: observation.carryover,
            watermark: observation.global_watermark,
        };
        frontier.validate()?;
        let evidence = EpochClosureEvidence {
            version: EPOCH_CLOSURE_EVIDENCE_VERSION,
            frontier: frontier.clone(),
            cause,
            observation: closure_observation,
        };
        evidence.validate()?;
        let closure = DrainEpochClosure {
            frontier,
            evidence,
            observed_at_unix_milliseconds: observation.observed_at_unix_milliseconds,
            terminate_after_settlement,
        };
        self.state = ControllerState::AwaitingSettlement(Box::new(closure.clone()));
        Ok(DrainEpochDecision::Close(Box::new(closure)))
    }

    /// Advances epoch authority only after the caller has verified the
    /// package, destination receipt, and committed checkpoint for this exact
    /// frontier.
    pub fn acknowledge_settlement(&mut self, committed_frontier: &SourcePosition) -> Result<()> {
        let ControllerState::AwaitingSettlement(closure) = &self.state else {
            return Err(CdfError::contract(
                "drain epoch settlement requires one pending closure",
            ));
        };
        if committed_frontier != &closure.frontier.frontier {
            return Err(CdfError::data(
                "drain epoch settlement frontier does not match the pending canonical frontier",
            ));
        }
        let terminate = closure.terminate_after_settlement;
        self.committed_frontier = Some(committed_frontier.clone());
        self.committed_source_continuation = closure.frontier.carryover.clone();
        self.committed_watermark = closure.frontier.watermark.clone();
        if let Some(states) = self.pending_partition_watermarks.take() {
            self.committed_partition_watermarks = states;
        }
        self.epoch_watermark_start = self.committed_watermark.clone();
        self.last_observed_watermark = self.committed_watermark.clone();
        self.epoch = Counts::default();
        self.last_safe_frontier = None;
        self.epoch_ordinal = self
            .epoch_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::internal("drain epoch ordinal overflow"))?;
        self.epoch_started_monotonic_milliseconds = self.monotonic_milliseconds();
        self.state = if terminate {
            ControllerState::Finished
        } else {
            ControllerState::Open
        };
        Ok(())
    }

    fn observe_clock(&mut self, monotonic_milliseconds: u64) -> Result<()> {
        if self
            .last_monotonic_milliseconds
            .is_some_and(|last| monotonic_milliseconds < last)
        {
            return Err(CdfError::internal(
                "drain epoch monotonic clock moved backwards",
            ));
        }
        self.last_monotonic_milliseconds = Some(monotonic_milliseconds);
        Ok(())
    }

    fn observe_watermark(&mut self, observed: Option<&WatermarkClaim>) -> Result<()> {
        let WatermarkPolicy::Enabled {
            event_time_field,
            domain,
            authority,
            ..
        } = &self.policy.watermark
        else {
            if observed.is_some() {
                return Err(CdfError::data(
                    "drain source emitted a watermark while the compiled policy disables watermarks",
                ));
            }
            return Ok(());
        };
        let Some(observed) = observed else {
            return Ok(());
        };
        if observed.event_time_field.as_ref() != event_time_field.as_ref()
            || &observed.domain != domain
            || &observed.authority != authority
        {
            return Err(CdfError::data(
                "drain watermark claim does not match the compiled field/domain/authority",
            ));
        }
        if let Some(previous) = self.last_observed_watermark.as_ref()
            && watermark_distance(&previous.value, &observed.value).is_none()
        {
            return Err(CdfError::data(
                "drain watermark regressed behind its committed or epoch baseline",
            ));
        }
        self.epoch_watermark_start
            .get_or_insert_with(|| observed.clone());
        self.last_observed_watermark = Some(observed.clone());
        Ok(())
    }

    fn closure_at(
        &self,
        observation: &DrainSafeFrontierObservation,
    ) -> Result<Option<(EpochClosureCause, EpochClosureObservation, bool)>> {
        if let Some(observed) = self.termination_observation(observation)? {
            return Ok(Some((
                EpochClosureCause::DrainTermination {
                    termination: self.termination.clone(),
                },
                observed,
                true,
            )));
        }
        if observation.source_exhausted {
            return Ok(Some((
                EpochClosureCause::SourceExhausted,
                EpochClosureObservation::Quiescent,
                true,
            )));
        }
        // One magnitude snapshot drives both triggers, so package rotation and checkpoint cadence
        // can never disagree about what was observed at this instant.
        let magnitudes = self.epoch_magnitudes(
            observation.monotonic_milliseconds,
            observation.global_watermark.as_ref(),
        )?;
        if let Some(observed) =
            Self::trigger_observation(&self.policy.package_rotation, &magnitudes)
        {
            return Ok(Some((
                EpochClosureCause::PackageRotation {
                    trigger: self.policy.package_rotation.clone(),
                },
                observed,
                false,
            )));
        }
        if let Some(observed) =
            Self::trigger_observation(&self.policy.checkpoint_cadence, &magnitudes)
        {
            return Ok(Some((
                EpochClosureCause::CheckpointCadence {
                    trigger: self.policy.checkpoint_cadence.clone(),
                },
                observed,
                false,
            )));
        }
        Ok(None)
    }

    /// Snapshots every trigger magnitude for this epoch at one instant.
    pub fn epoch_magnitudes(
        &self,
        monotonic_milliseconds: u64,
        watermark: Option<&WatermarkClaim>,
    ) -> Result<EpochTriggerMagnitudes> {
        Ok(EpochTriggerMagnitudes {
            batches: self.epoch.batches,
            rows: self.epoch.rows,
            bytes: self.epoch.bytes,
            elapsed_milliseconds: self.epoch_elapsed(monotonic_milliseconds)?,
            watermark_advance: self.watermark_advance_since_epoch_start(watermark),
        })
    }

    /// Evaluates the first closure cause that would be reached if one still-open source
    /// settlement unit admitted the supplied counts.
    ///
    /// This is the sole anticipation path for transaction-aligned CDC. It derives elapsed time,
    /// watermark advance, epoch totals, and command totals from this controller so an adapter
    /// cannot supply a second, disagreeing interpretation of the same policy. Source-frontier and
    /// quiescence termination remain terminal-boundary observations and therefore do not fire
    /// here.
    pub fn prospective_closure_cause(
        &self,
        additional_batches: u64,
        additional_rows: u64,
        additional_bytes: u64,
        monotonic_milliseconds: u64,
        watermark: Option<&WatermarkClaim>,
    ) -> Result<Option<EpochClosureCause>> {
        self.validate_ready_for_epoch()?;
        if self
            .last_monotonic_milliseconds
            .is_some_and(|last| monotonic_milliseconds < last)
        {
            return Err(CdfError::internal(
                "drain epoch monotonic clock moved backwards",
            ));
        }
        let checked = |current: u64, additional: u64, label: &str| {
            current
                .checked_add(additional)
                .ok_or_else(|| CdfError::internal(format!("drain {label} count overflow")))
        };
        let epoch_batches = checked(self.epoch.batches, additional_batches, "epoch batch")?;
        let epoch_rows = checked(self.epoch.rows, additional_rows, "epoch row")?;
        let epoch_bytes = checked(self.epoch.bytes, additional_bytes, "epoch byte")?;
        let total_rows = checked(self.total.rows, additional_rows, "total row")?;
        let total_bytes = checked(self.total.bytes, additional_bytes, "total byte")?;

        let termination_reached = match &self.termination {
            DrainTermination::Duration { milliseconds } => {
                self.command_elapsed(monotonic_milliseconds)? >= *milliseconds
            }
            DrainTermination::Records { count } => total_rows >= *count,
            DrainTermination::Bytes { count } => total_bytes >= *count,
            DrainTermination::Quiescent | DrainTermination::SourceFrontier { .. } => false,
        };
        if termination_reached {
            return Ok(Some(EpochClosureCause::DrainTermination {
                termination: self.termination.clone(),
            }));
        }

        let magnitudes = EpochTriggerMagnitudes {
            batches: epoch_batches,
            rows: epoch_rows,
            bytes: epoch_bytes,
            elapsed_milliseconds: self.epoch_elapsed(monotonic_milliseconds)?,
            watermark_advance: self.watermark_advance_since_epoch_start(watermark),
        };
        if magnitudes.trips(&self.policy.package_rotation) {
            return Ok(Some(EpochClosureCause::PackageRotation {
                trigger: self.policy.package_rotation.clone(),
            }));
        }
        if magnitudes.trips(&self.policy.checkpoint_cadence) {
            return Ok(Some(EpochClosureCause::CheckpointCadence {
                trigger: self.policy.checkpoint_cadence.clone(),
            }));
        }
        Ok(None)
    }

    /// Watermark advance from this epoch's start claim to `observed`.
    ///
    /// The epoch-start claim lives here, so this is the single place that measures advance. A
    /// component anticipating closure must ask the controller rather than track its own start.
    #[must_use]
    pub fn watermark_advance_since_epoch_start(
        &self,
        observed: Option<&WatermarkClaim>,
    ) -> Option<u64> {
        self.epoch_watermark_start
            .as_ref()
            .zip(observed)
            .and_then(|(start, observed)| watermark_distance(&start.value, &observed.value))
    }

    /// Builds the typed observation for a trigger that has been reached.
    ///
    /// The trip decision itself belongs to [`EpochTriggerMagnitudes::trips`]; this only shapes the
    /// evidence, so the two can never disagree about *whether* a trigger fired.
    fn trigger_observation(
        trigger: &EpochClosureTrigger,
        magnitudes: &EpochTriggerMagnitudes,
    ) -> Option<EpochClosureObservation> {
        let observed = magnitudes.measured(trigger)?;
        let overshoot = observed.checked_sub(trigger.threshold())?;
        Some(match trigger {
            EpochClosureTrigger::Batches { .. } => EpochClosureObservation::Batches {
                observed,
                overshoot,
            },
            EpochClosureTrigger::Rows { .. } => EpochClosureObservation::Rows {
                observed,
                overshoot,
            },
            EpochClosureTrigger::Bytes { .. } => EpochClosureObservation::Bytes {
                observed,
                overshoot,
            },
            EpochClosureTrigger::Elapsed { .. } => EpochClosureObservation::Elapsed {
                observed_milliseconds: observed,
                overshoot_milliseconds: overshoot,
            },
            EpochClosureTrigger::WatermarkAdvance { .. } => {
                EpochClosureObservation::WatermarkAdvance {
                    observed_units: observed,
                    overshoot_units: overshoot,
                }
            }
        })
    }

    fn termination_observation(
        &self,
        observation: &DrainSafeFrontierObservation,
    ) -> Result<Option<EpochClosureObservation>> {
        let observed = match &self.termination {
            DrainTermination::Quiescent => observation
                .source_exhausted
                .then_some(EpochClosureObservation::Quiescent),
            DrainTermination::Duration { milliseconds } => {
                let elapsed = self.command_elapsed(observation.monotonic_milliseconds)?;
                threshold_observation(elapsed, *milliseconds, |observed, overshoot| {
                    EpochClosureObservation::Elapsed {
                        observed_milliseconds: observed,
                        overshoot_milliseconds: overshoot,
                    }
                })
            }
            DrainTermination::Records { count } => {
                threshold_observation(self.total.rows, *count, |observed, overshoot| {
                    EpochClosureObservation::Rows {
                        observed,
                        overshoot,
                    }
                })
            }
            DrainTermination::Bytes { count } => {
                threshold_observation(self.total.bytes, *count, |observed, overshoot| {
                    EpochClosureObservation::Bytes {
                        observed,
                        overshoot,
                    }
                })
            }
            DrainTermination::SourceFrontier { position } => {
                source_position_reaches(&observation.frontier, position)?.then_some(
                    EpochClosureObservation::SourceFrontier {
                        observed: observation.frontier.clone(),
                    },
                )
            }
        };
        Ok(observed)
    }

    fn command_elapsed(&self, monotonic_milliseconds: u64) -> Result<u64> {
        monotonic_milliseconds
            .checked_sub(self.command_started_monotonic_milliseconds)
            .ok_or_else(|| CdfError::internal("drain command monotonic clock moved backwards"))
    }

    fn epoch_elapsed(&self, monotonic_milliseconds: u64) -> Result<u64> {
        monotonic_milliseconds
            .checked_sub(self.epoch_started_monotonic_milliseconds)
            .ok_or_else(|| CdfError::internal("drain epoch monotonic clock moved backwards"))
    }
}

fn threshold_observation<T>(
    observed: u64,
    threshold: u64,
    build: impl FnOnce(u64, u64) -> T,
) -> Option<T> {
    observed
        .checked_sub(threshold)
        .map(|overshoot| build(observed, overshoot))
}

fn watermark_distance(start: &WatermarkValue, observed: &WatermarkValue) -> Option<u64> {
    fn signed_distance(start: i128, observed: i128) -> Option<u64> {
        u64::try_from(observed.checked_sub(start)?).ok()
    }
    match (start, observed) {
        (WatermarkValue::Signed(start), WatermarkValue::Signed(observed)) => {
            signed_distance(i128::from(*start), i128::from(*observed))
        }
        (WatermarkValue::Unsigned(start), WatermarkValue::Unsigned(observed)) => {
            observed.checked_sub(*start)
        }
        (WatermarkValue::Decimal(start), WatermarkValue::Decimal(observed)) => {
            signed_distance(*start, *observed)
        }
        (WatermarkValue::Date32(start), WatermarkValue::Date32(observed)) => {
            signed_distance(i128::from(*start), i128::from(*observed))
        }
        (WatermarkValue::Date64(start), WatermarkValue::Date64(observed))
        | (WatermarkValue::Timestamp(start), WatermarkValue::Timestamp(observed)) => {
            signed_distance(i128::from(*start), i128::from(*observed))
        }
        _ => None,
    }
}

fn source_position_reaches(observed: &SourcePosition, target: &SourcePosition) -> Result<bool> {
    observed.reaches(target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_kernel::{
        CursorPosition, CursorValue, EventTimeDomain, LateDataAction, PartitionId,
        PartitionWatermarkAggregation, SOURCE_POSITION_VERSION, STREAM_EPOCH_POLICY_VERSION,
        SafeFrontierPolicy, WATERMARK_CLAIM_VERSION, WatermarkAuthority,
        WatermarkObservationContext,
    };

    #[test]
    fn barrier_blocks_later_progress_until_exact_settlement() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Rows { count: 10 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::Records { count: 30 },
        ))
        .unwrap();
        assert_eq!(
            controller
                .observe_safe_frontier(observation(5, 50, 5, false))
                .unwrap(),
            DrainEpochDecision::Continue
        );
        let DrainEpochDecision::Close(closure) = controller
            .observe_safe_frontier(observation(7, 70, 12, false))
            .unwrap()
        else {
            panic!("row cadence must close at the next safe frontier");
        };
        assert_eq!(closure.frontier.epoch_ordinal, 0);
        assert!(matches!(
            closure.evidence.cause,
            EpochClosureCause::CheckpointCadence {
                trigger: EpochClosureTrigger::Rows { count: 10 }
            }
        ));
        assert_eq!(
            closure.evidence.observation,
            EpochClosureObservation::Rows {
                observed: 12,
                overshoot: 2
            }
        );
        assert!(
            controller
                .observe_safe_frontier(observation(1, 10, 13, false))
                .unwrap_err()
                .message
                .contains("before frontier settlement")
        );
        assert!(
            controller
                .acknowledge_settlement(&cursor(11))
                .unwrap_err()
                .message
                .contains("does not match")
        );
        controller
            .acknowledge_settlement(&closure.frontier.frontier)
            .unwrap();
        assert_eq!(controller.epoch_ordinal(), 1);
    }

    #[test]
    fn termination_closes_and_finishes_only_after_settlement() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Rows { count: 100 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::Records { count: 10 },
        ))
        .unwrap();
        let DrainEpochDecision::Close(closure) = controller
            .observe_safe_frontier(observation(12, 120, 12, false))
            .unwrap()
        else {
            panic!("record termination must close");
        };
        assert!(closure.terminate_after_settlement);
        assert!(matches!(
            closure.evidence.cause,
            EpochClosureCause::DrainTermination {
                termination: DrainTermination::Records { count: 10 }
            }
        ));
        assert!(!controller.is_finished());
        controller
            .acknowledge_settlement(&closure.frontier.frontier)
            .unwrap();
        assert!(controller.is_finished());
    }

    #[test]
    fn exhausted_empty_drain_is_a_verified_noop_without_package() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Rows { count: 10 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::Quiescent,
        ))
        .unwrap();
        assert_eq!(
            controller
                .observe_safe_frontier(observation(0, 0, 0, true))
                .unwrap(),
            DrainEpochDecision::FinishedNoOp
        );
        assert!(controller.is_finished());
        assert!(controller.pending_closure().is_none());
    }

    #[test]
    fn package_rotation_precedes_cadence_at_same_safe_frontier() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Rows { count: 10 },
            EpochClosureTrigger::Bytes { count: 100 },
            DrainTermination::Records { count: 1_000 },
        ))
        .unwrap();
        let DrainEpochDecision::Close(closure) = controller
            .observe_safe_frontier(observation(12, 120, 12, false))
            .unwrap()
        else {
            panic!("both thresholds are reached");
        };
        assert!(matches!(
            closure.evidence.cause,
            EpochClosureCause::PackageRotation { .. }
        ));
    }

    #[test]
    fn source_exhaustion_terminates_instead_of_opening_an_empty_followup_epoch() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Rows { count: 1 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::Records { count: 10 },
        ))
        .unwrap();
        let DrainEpochDecision::Close(closure) = controller
            .observe_safe_frontier(observation(1, 10, 1, true))
            .unwrap()
        else {
            panic!("source exhaustion must close the final nonempty epoch");
        };
        assert!(matches!(
            closure.evidence.cause,
            EpochClosureCause::SourceExhausted
        ));
        assert!(closure.terminate_after_settlement);
    }

    #[test]
    fn source_frontier_termination_accepts_ordered_overshoot() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Rows { count: 100 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::SourceFrontier {
                position: cursor(10),
            },
        ))
        .unwrap();
        let DrainEpochDecision::Close(closure) = controller
            .observe_safe_frontier(observation(1, 10, 12, false))
            .unwrap()
        else {
            panic!("cursor frontier must be reached");
        };
        assert_eq!(closure.frontier.frontier, cursor(12));
    }

    #[test]
    fn elapsed_trigger_includes_work_before_the_first_safe_frontier() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Elapsed { milliseconds: 100 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::Duration {
                milliseconds: 1_000,
            },
        ))
        .unwrap();
        let DrainEpochDecision::Close(closure) = controller
            .observe_safe_frontier(observation(1, 10, 120, false))
            .unwrap()
        else {
            panic!("elapsed work before the first frontier must request closure");
        };
        assert_eq!(
            closure.evidence.observation,
            EpochClosureObservation::Elapsed {
                observed_milliseconds: 120,
                overshoot_milliseconds: 20,
            }
        );
    }

    #[test]
    fn settlement_time_counts_toward_command_duration_but_not_next_epoch_cadence() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Elapsed { milliseconds: 100 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::Duration { milliseconds: 200 },
        ))
        .unwrap();
        let DrainEpochDecision::Close(first) = controller
            .observe_safe_frontier(observation(1, 10, 120, false))
            .unwrap()
        else {
            panic!("the first epoch must close on elapsed cadence");
        };
        controller.advance_monotonic_clock(205).unwrap();
        controller
            .acknowledge_settlement(&first.frontier.frontier)
            .unwrap();

        let DrainEpochDecision::Close(second) = controller
            .observe_safe_frontier(observation(1, 10, 206, false))
            .unwrap()
        else {
            panic!("settlement time must count toward command duration");
        };
        assert!(matches!(
            second.evidence.cause,
            EpochClosureCause::DrainTermination {
                termination: DrainTermination::Duration { milliseconds: 200 }
            }
        ));
        assert_eq!(
            second.evidence.observation,
            EpochClosureObservation::Elapsed {
                observed_milliseconds: 206,
                overshoot_milliseconds: 6,
            }
        );
    }

    #[test]
    fn timer_closes_a_nonempty_epoch_at_its_last_safe_frontier() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Elapsed { milliseconds: 100 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::Duration {
                milliseconds: 1_000,
            },
        ))
        .unwrap();
        assert_eq!(
            controller
                .observe_safe_frontier(observation(1, 10, 10, false))
                .unwrap(),
            DrainEpochDecision::Continue
        );
        assert_eq!(
            controller.next_timer_delay_milliseconds().unwrap(),
            Some(90)
        );
        let DrainEpochDecision::Close(closure) =
            controller.observe_timer(110, 1_700_000_000_110).unwrap()
        else {
            panic!("elapsed timer must close at the last safe frontier");
        };
        assert_eq!(closure.frontier.frontier, cursor(10));
        assert_eq!(
            closure.evidence.observation,
            EpochClosureObservation::Elapsed {
                observed_milliseconds: 110,
                overshoot_milliseconds: 10,
            }
        );
    }

    #[test]
    fn duration_timer_finishes_a_silent_empty_drain_without_cadence_polling() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Elapsed { milliseconds: 10 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::Duration { milliseconds: 50 },
        ))
        .unwrap();
        assert_eq!(
            controller.next_timer_delay_milliseconds().unwrap(),
            Some(50)
        );
        assert_eq!(
            controller.observe_timer(50, 1_700_000_000_050).unwrap(),
            DrainEpochDecision::FinishedNoOp
        );
        assert!(controller.is_finished());
    }

    #[test]
    fn prior_checkpoint_frontier_seeds_input_low_without_consuming_command_budget() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Rows { count: 1 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::Records { count: 2 },
        ))
        .unwrap();
        controller
            .bind_initial_committed_state(Some(cursor(40)), None, None, Vec::new(), 0)
            .unwrap();
        let DrainEpochDecision::Close(closure) = controller
            .observe_safe_frontier(observation(1, 10, 41, false))
            .unwrap()
        else {
            panic!("row cadence must close");
        };
        assert_eq!(closure.frontier.input_low, Some(cursor(40)));
        assert!(!closure.terminate_after_settlement);
    }

    #[test]
    fn watermark_claims_must_be_monotone_within_one_open_epoch() {
        let extent = watermark_extent();
        let mut controller = DrainEpochController::new(&extent).unwrap();
        assert_eq!(
            controller
                .observe_safe_frontier(watermark_observation(1, 100))
                .unwrap(),
            DrainEpochDecision::Continue
        );
        assert_eq!(
            controller
                .observe_safe_frontier(watermark_observation(2, 120))
                .unwrap(),
            DrainEpochDecision::Continue
        );
        let error = controller
            .observe_safe_frontier(watermark_observation(3, 110))
            .unwrap_err();
        assert!(error.message.contains("watermark regressed"));
    }

    #[test]
    fn restored_watermark_is_the_next_epoch_late_data_floor() {
        let mut controller = DrainEpochController::new(&watermark_extent()).unwrap();
        let committed = watermark_observation(40, 90).global_watermark.unwrap();
        controller
            .bind_initial_committed_state(
                Some(cursor(40)),
                None,
                Some(committed.clone()),
                Vec::new(),
                1,
            )
            .unwrap();

        assert_eq!(controller.committed_watermark(), Some(&committed));
        assert_eq!(controller.late_data_watermark(), Some(&committed));
        assert_eq!(controller.epoch_ordinal(), 1);
    }

    #[test]
    fn missing_observation_cannot_erase_the_committed_watermark_floor() {
        let mut extent = watermark_extent();
        let ExecutionExtent::Drain { policy, .. } = &mut extent else {
            unreachable!("watermark fixture is a drain extent");
        };
        policy.checkpoint_cadence = EpochClosureTrigger::Rows { count: 1 };
        let mut controller = DrainEpochController::new(&extent).unwrap();
        let committed = watermark_observation(40, 90).global_watermark.unwrap();
        controller
            .bind_initial_committed_state(
                Some(cursor(40)),
                None,
                Some(committed.clone()),
                Vec::new(),
                1,
            )
            .unwrap();

        let DrainEpochDecision::Close(closure) = controller
            .observe_safe_frontier(observation(1, 10, 41, false))
            .unwrap()
        else {
            panic!("row cadence must close at the normalized safe frontier");
        };
        assert_eq!(closure.frontier.watermark, Some(committed.clone()));
        controller
            .acknowledge_settlement(&closure.frontier.frontier)
            .unwrap();
        assert_eq!(controller.committed_watermark(), Some(&committed));
    }

    fn watermark_extent() -> ExecutionExtent {
        ExecutionExtent::Drain {
            version: 1,
            policy: StreamEpochPolicy {
                version: STREAM_EPOCH_POLICY_VERSION,
                checkpoint_cadence: EpochClosureTrigger::WatermarkAdvance { units: 100 },
                package_rotation: EpochClosureTrigger::Bytes { count: 1_000 },
                watermark: WatermarkPolicy::Enabled {
                    event_time_field: "occurred_at".into(),
                    domain: EventTimeDomain::UnsignedInteger,
                    authority: WatermarkAuthority::Source,
                    partition_aggregation: PartitionWatermarkAggregation::MinimumAll,
                },
                late_data: LateDataAction::Quarantine,
                safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
                transaction_limit_bytes: None,
            },
            termination: DrainTermination::Records { count: 100 },
        }
    }

    fn extent(
        checkpoint_cadence: EpochClosureTrigger,
        package_rotation: EpochClosureTrigger,
        termination: DrainTermination,
    ) -> ExecutionExtent {
        ExecutionExtent::Drain {
            version: 1,
            policy: StreamEpochPolicy {
                version: STREAM_EPOCH_POLICY_VERSION,
                checkpoint_cadence,
                package_rotation,
                watermark: WatermarkPolicy::Disabled,
                late_data: LateDataAction::Quarantine,
                safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
                transaction_limit_bytes: None,
            },
            termination,
        }
    }

    fn observation(
        rows: u64,
        bytes: u64,
        position: u64,
        source_exhausted: bool,
    ) -> DrainSafeFrontierObservation {
        DrainSafeFrontierObservation {
            frontier: cursor(position),
            carryover: None,
            admitted_batches: u64::from(rows != 0),
            admitted_rows: rows,
            admitted_bytes: bytes,
            admitted_positions: u64::from(rows != 0),
            global_watermark: None,
            source_exhausted,
            monotonic_milliseconds: position,
            observed_at_unix_milliseconds: 1_700_000_000_000 + position,
        }
    }

    fn cursor(value: u64) -> SourcePosition {
        SourcePosition::Cursor(CursorPosition {
            version: SOURCE_POSITION_VERSION,
            field: "offset".to_owned(),
            value: CursorValue::U64(value),
        })
    }

    fn watermark_observation(position: u64, watermark: u64) -> DrainSafeFrontierObservation {
        let mut observation = observation(1, 10, position, false);
        observation.global_watermark = Some(WatermarkClaim {
            version: WATERMARK_CLAIM_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            event_time_field: "occurred_at".into(),
            domain: EventTimeDomain::UnsignedInteger,
            value: WatermarkValue::Unsigned(watermark),
            partition_id: PartitionId::new("partition-0").unwrap(),
            source_position: cursor(position),
            authority: WatermarkAuthority::Source,
            observation_context: WatermarkObservationContext::SourcePoll,
        });
        observation
    }
}
