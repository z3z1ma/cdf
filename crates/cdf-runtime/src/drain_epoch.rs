use std::collections::BTreeMap;

use cdf_kernel::{
    CdfError, CursorValue, DrainTermination, EPOCH_CLOSURE_EVIDENCE_VERSION,
    EPOCH_FRONTIER_VERSION, EpochClosureCause, EpochClosureEvidence, EpochClosureObservation,
    EpochClosureTrigger, EpochFrontier, ExecutionExtent, FilePosition, Result,
    STREAM_EPOCH_POLICY_VERSION, SourcePosition, StreamEpochPolicy, WatermarkClaim,
    WatermarkPolicy, WatermarkValue,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counts {
    batches: u64,
    rows: u64,
    bytes: u64,
    positions: u64,
}

impl Counts {
    fn checked_add(&mut self, observation: &DrainSafeFrontierObservation) -> Result<()> {
        self.batches = self
            .batches
            .checked_add(observation.admitted_batches)
            .ok_or_else(|| CdfError::data("drain epoch batch count overflow"))?;
        self.rows = self
            .rows
            .checked_add(observation.admitted_rows)
            .ok_or_else(|| CdfError::data("drain epoch row count overflow"))?;
        self.bytes = self
            .bytes
            .checked_add(observation.admitted_bytes)
            .ok_or_else(|| CdfError::data("drain epoch byte count overflow"))?;
        self.positions = self
            .positions
            .checked_add(observation.admitted_positions)
            .ok_or_else(|| CdfError::data("drain epoch position count overflow"))?;
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
    started_monotonic_milliseconds: Option<u64>,
    last_monotonic_milliseconds: Option<u64>,
    committed_frontier: Option<SourcePosition>,
    committed_watermark: Option<WatermarkClaim>,
    epoch_watermark_start: Option<WatermarkClaim>,
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
            started_monotonic_milliseconds: Some(0),
            last_monotonic_milliseconds: Some(0),
            committed_frontier: None,
            committed_watermark: None,
            epoch_watermark_start: None,
        })
    }

    pub const fn epoch_ordinal(&self) -> u64 {
        self.epoch_ordinal
    }

    pub fn committed_frontier(&self) -> Option<&SourcePosition> {
        self.committed_frontier.as_ref()
    }

    /// Seeds the input-low frontier from the checkpoint committed before this drain command.
    /// The new command still begins at epoch zero and its record/byte termination counters begin
    /// at zero; only source-position aggregation inherits the prior durable head.
    pub fn bind_initial_committed_frontier(
        &mut self,
        committed_frontier: Option<SourcePosition>,
    ) -> Result<()> {
        if !matches!(self.state, ControllerState::Open)
            || self.epoch_ordinal != 0
            || !self.epoch.is_empty()
            || !self.total.is_empty()
            || self.committed_frontier.is_some()
        {
            return Err(CdfError::contract(
                "initial drain frontier must be bound before the first source observation",
            ));
        }
        if let Some(frontier) = &committed_frontier {
            frontier.validate()?;
        }
        self.committed_frontier = committed_frontier;
        Ok(())
    }

    pub const fn monotonic_milliseconds(&self) -> u64 {
        match self.last_monotonic_milliseconds {
            Some(value) => value,
            None => 0,
        }
    }

    pub fn pending_closure(&self) -> Option<&DrainEpochClosure> {
        match &self.state {
            ControllerState::AwaitingSettlement(closure) => Some(closure),
            ControllerState::Open | ControllerState::Finished => None,
        }
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
        observation: DrainSafeFrontierObservation,
    ) -> Result<DrainEpochDecision> {
        self.validate_ready_for_epoch()?;
        observation.validate()?;
        self.observe_clock(observation.monotonic_milliseconds)?;
        self.observe_watermark(observation.global_watermark.as_ref())?;
        self.epoch.checked_add(&observation)?;
        self.total.checked_add(&observation)?;

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
        self.committed_watermark = closure.frontier.watermark.clone();
        self.epoch_watermark_start = self.committed_watermark.clone();
        self.epoch = Counts::default();
        self.epoch_ordinal = self
            .epoch_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("drain epoch ordinal overflow"))?;
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
        self.started_monotonic_milliseconds
            .get_or_insert(monotonic_milliseconds);
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
        if let Some(previous) = self
            .committed_watermark
            .as_ref()
            .or(self.epoch_watermark_start.as_ref())
            && watermark_distance(&previous.value, &observed.value).is_none()
        {
            return Err(CdfError::data(
                "drain watermark regressed behind its committed or epoch baseline",
            ));
        }
        self.epoch_watermark_start
            .get_or_insert_with(|| observed.clone());
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
        if let Some(observed) = self.trigger_observation(
            &self.policy.package_rotation,
            observation.monotonic_milliseconds,
            observation.global_watermark.as_ref(),
        )? {
            return Ok(Some((
                EpochClosureCause::PackageRotation {
                    trigger: self.policy.package_rotation.clone(),
                },
                observed,
                false,
            )));
        }
        if let Some(observed) = self.trigger_observation(
            &self.policy.checkpoint_cadence,
            observation.monotonic_milliseconds,
            observation.global_watermark.as_ref(),
        )? {
            return Ok(Some((
                EpochClosureCause::CheckpointCadence {
                    trigger: self.policy.checkpoint_cadence.clone(),
                },
                observed,
                false,
            )));
        }
        if observation.source_exhausted {
            return Ok(Some((
                EpochClosureCause::SourceExhausted,
                EpochClosureObservation::Quiescent,
                true,
            )));
        }
        Ok(None)
    }

    fn trigger_observation(
        &self,
        trigger: &EpochClosureTrigger,
        monotonic_milliseconds: u64,
        watermark: Option<&WatermarkClaim>,
    ) -> Result<Option<EpochClosureObservation>> {
        let observed = match trigger {
            EpochClosureTrigger::Batches { count } => {
                threshold_observation(self.epoch.batches, *count, |observed, overshoot| {
                    EpochClosureObservation::Batches {
                        observed,
                        overshoot,
                    }
                })
            }
            EpochClosureTrigger::Rows { count } => {
                threshold_observation(self.epoch.rows, *count, |observed, overshoot| {
                    EpochClosureObservation::Rows {
                        observed,
                        overshoot,
                    }
                })
            }
            EpochClosureTrigger::Bytes { count } => {
                threshold_observation(self.epoch.bytes, *count, |observed, overshoot| {
                    EpochClosureObservation::Bytes {
                        observed,
                        overshoot,
                    }
                })
            }
            EpochClosureTrigger::Elapsed { milliseconds } => {
                let elapsed = self.elapsed(monotonic_milliseconds)?;
                threshold_observation(elapsed, *milliseconds, |observed, overshoot| {
                    EpochClosureObservation::Elapsed {
                        observed_milliseconds: observed,
                        overshoot_milliseconds: overshoot,
                    }
                })
            }
            EpochClosureTrigger::WatermarkAdvance { units } => self
                .epoch_watermark_start
                .as_ref()
                .zip(watermark)
                .and_then(|(start, observed)| watermark_distance(&start.value, &observed.value))
                .and_then(|advance| {
                    threshold_observation(advance, *units, |observed, overshoot| {
                        EpochClosureObservation::WatermarkAdvance {
                            observed_units: observed,
                            overshoot_units: overshoot,
                        }
                    })
                }),
        };
        Ok(observed)
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
                let elapsed = self.elapsed(observation.monotonic_milliseconds)?;
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

    fn elapsed(&self, monotonic_milliseconds: u64) -> Result<u64> {
        monotonic_milliseconds
            .checked_sub(
                self.started_monotonic_milliseconds
                    .unwrap_or(monotonic_milliseconds),
            )
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
    observed.validate()?;
    target.validate()?;
    Ok(match (observed, target) {
        (SourcePosition::Cursor(observed), SourcePosition::Cursor(target))
            if observed.field == target.field =>
        {
            cursor_reaches(&observed.value, &target.value)
        }
        (SourcePosition::Log(observed), SourcePosition::Log(target))
            if observed.log == target.log =>
        {
            observed.offset >= target.offset
                && target
                    .sequence
                    .as_ref()
                    .is_none_or(|sequence| observed.sequence.as_ref() == Some(sequence))
        }
        (SourcePosition::FileManifest(observed), SourcePosition::FileManifest(target)) => {
            file_manifest_reaches(&observed.files, &target.files)
        }
        (SourcePosition::PageToken(observed), SourcePosition::PageToken(target)) => {
            observed.token == target.token
        }
        (SourcePosition::Composite(observed), SourcePosition::Composite(target)) => {
            for (name, target) in &target.positions {
                let Some(observed) = observed.positions.get(name) else {
                    return Ok(false);
                };
                if !source_position_reaches(observed, target)? {
                    return Ok(false);
                }
            }
            true
        }
        (SourcePosition::ForeignState(observed), SourcePosition::ForeignState(target)) => {
            observed.protocol == target.protocol && observed.blob_sha256 == target.blob_sha256
        }
        _ => false,
    })
}

fn cursor_reaches(observed: &CursorValue, target: &CursorValue) -> bool {
    match (observed, target) {
        (CursorValue::I64(observed), CursorValue::I64(target)) => observed >= target,
        (CursorValue::U64(observed), CursorValue::U64(target)) => observed >= target,
        (
            CursorValue::TimestampMicros {
                micros: observed,
                timezone: observed_timezone,
            },
            CursorValue::TimestampMicros {
                micros: target,
                timezone: target_timezone,
            },
        ) => observed_timezone == target_timezone && observed >= target,
        (CursorValue::String(observed), CursorValue::String(target)) => observed == target,
        (CursorValue::DecimalString(observed), CursorValue::DecimalString(target)) => {
            observed == target
        }
        _ => false,
    }
}

fn file_manifest_reaches(observed: &[FilePosition], target: &[FilePosition]) -> bool {
    let observed = observed
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect::<BTreeMap<_, _>>();
    target.iter().all(|target| {
        observed
            .get(target.path.as_str())
            .is_some_and(|observed| *observed == target)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_kernel::{
        CursorPosition, LateDataAction, SOURCE_POSITION_VERSION, STREAM_EPOCH_POLICY_VERSION,
        SafeFrontierPolicy,
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
    fn prior_checkpoint_frontier_seeds_input_low_without_consuming_command_budget() {
        let mut controller = DrainEpochController::new(&extent(
            EpochClosureTrigger::Rows { count: 1 },
            EpochClosureTrigger::Bytes { count: 1_000 },
            DrainTermination::Records { count: 2 },
        ))
        .unwrap();
        controller
            .bind_initial_committed_frontier(Some(cursor(40)))
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
}
