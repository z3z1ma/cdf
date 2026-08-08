//! Neutral finite-drain runtime archetype for first-party CDC log sources.
//!
//! This module owns the contract between a CDC adapter and the existing drain, package, receipt,
//! and checkpoint authorities. It deliberately knows nothing about PostgreSQL, MySQL, or MongoDB:
//! a source is described only by which evidence proves one settlement unit terminal
//! ([`SettlementUnitKind`]) and by the typed positions the kernel position algebra already
//! validates.
//!
//! The central invariant, from `.10x/specs/cdc-log-source-foundation.md`:
//!
//! > A package/checkpoint epoch may close only at a source-proven complete transaction boundary,
//! > and every row admitted before that boundary belongs to a transaction at or before that
//! > boundary.
//!
//! [`DrainEpochController`](crate::DrainEpochController) already refuses to admit progress after it
//! requests a close, so this archetype does not re-implement that gate. What it adds is the part
//! the controller structurally cannot see: because a frontier is published only at a proven unit
//! boundary, a cadence trigger that is crossed *inside* a unit never reaches the controller. The
//! archetype records that crossing as phase-local overshoot so the epoch can report it truthfully.

use cdf_kernel::{
    CdcMetadata, CdfError, EpochClosureTrigger, ExecutionExtent, KeyedEffectWinnerPolicy, Result,
    SourcePosition, WatermarkClaim,
};
use cdf_memory::SpillBudgetCoordinator;

use crate::drain_epoch::DrainSafeFrontierObservation;

/// What evidence proves one settlement unit terminal.
///
/// This is the only source-shaped discriminant in generic runtime code. It distinguishes the two
/// position categories the CDC foundation admits, not a database product.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettlementUnitKind {
    /// An ordered, linearly comparable committed-transaction boundary.
    ///
    /// The terminal position is constructible only after the source proves the transaction
    /// committed, and it must not regress against positions already admitted in the unit.
    CommittedTransaction,
    /// An opaque, adapter-ordered event prefix terminated by a source-issued token.
    ///
    /// CDF preserves the token exactly and never claims numeric ordering from its internals, so no
    /// reachability comparison is performed for this kind.
    EventPrefix,
}

/// Why closure was requested while a settlement unit was still open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettlementClosureCause {
    /// An accumulated row threshold was crossed.
    Rows,
    /// An accumulated byte threshold was crossed.
    Bytes,
    /// An accumulated batch threshold was crossed.
    Batches,
    /// Elapsed wall time crossed the configured cadence.
    Elapsed,
    /// The command asked to terminate.
    Termination,
}

/// The hard byte ceiling for one settlement unit.
///
/// `maximum_transaction_bytes` is a mandatory compiled CDC capability bounded by host spill and
/// replay policy. A resource may lower it but never raise it above host authority, and the kernel
/// invents no numeric default — the host profile supplies the concrete bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransactionByteCeiling {
    host_maximum_bytes: u64,
    effective_bytes: u64,
}

impl TransactionByteCeiling {
    /// Resolves the effective ceiling from host authority and an optional resource request.
    ///
    /// The host maximum is the hard bound. A resource request above it is a configuration error,
    /// not a silent clamp, because silently raising a spill bound would let one transaction exceed
    /// the memory envelope the host proved.
    pub fn resolve(host_maximum_bytes: u64, resource_maximum_bytes: Option<u64>) -> Result<Self> {
        if host_maximum_bytes == 0 {
            return Err(CdfError::contract(
                "CDC maximum transaction bytes requires a non-zero resolved host spill budget",
            ));
        }
        let effective_bytes = match resource_maximum_bytes {
            None => host_maximum_bytes,
            Some(0) => {
                return Err(CdfError::contract(
                    "CDC resource maximum transaction bytes must be greater than zero",
                ));
            }
            Some(requested) if requested > host_maximum_bytes => {
                return Err(CdfError::contract(format!(
                    "CDC resource maximum transaction bytes {requested} exceeds the resolved host \
                     spill budget {host_maximum_bytes}; a resource may only lower this bound"
                )));
            }
            Some(requested) => requested,
        };
        Ok(Self {
            host_maximum_bytes,
            effective_bytes,
        })
    }

    /// Resolves the ceiling against the live host spill authority.
    ///
    /// The spill coordinator's budget is the host maximum, so a CDC resource can never admit a
    /// transaction larger than the memory envelope the host already proved it can hold.
    pub fn from_spill_budget(
        coordinator: &dyn SpillBudgetCoordinator,
        resource_maximum_bytes: Option<u64>,
    ) -> Result<Self> {
        Self::resolve(coordinator.snapshot().budget_bytes, resource_maximum_bytes)
    }

    #[must_use]
    pub const fn effective_bytes(&self) -> u64 {
        self.effective_bytes
    }

    #[must_use]
    pub const fn host_maximum_bytes(&self) -> u64 {
        self.host_maximum_bytes
    }
}

/// Cadence thresholds the archetype evaluates itself so a mid-unit crossing is observable.
///
/// The [`DrainEpochController`](crate::DrainEpochController) remains the closure authority. These
/// thresholds exist only to locate *where* inside a unit a trigger was crossed, which the
/// controller cannot see because it is only ever shown proven boundaries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SettlementClosureThresholds {
    rows: Option<u64>,
    bytes: Option<u64>,
    batches: Option<u64>,
    elapsed_milliseconds: Option<u64>,
}

impl SettlementClosureThresholds {
    /// Derives thresholds from a drain extent's checkpoint cadence and package rotation.
    ///
    /// When both triggers constrain the same dimension the smaller bound wins, because either one
    /// crossing is enough to make the epoch want to close.
    pub fn from_extent(extent: &ExecutionExtent) -> Result<Self> {
        let ExecutionExtent::Drain { policy, .. } = extent else {
            return Err(CdfError::contract(
                "CDC log-source archetype requires a drain execution extent",
            ));
        };
        let mut thresholds = Self::default();
        thresholds.absorb(&policy.checkpoint_cadence);
        thresholds.absorb(&policy.package_rotation);
        Ok(thresholds)
    }

    fn absorb(&mut self, trigger: &EpochClosureTrigger) {
        fn lower(slot: &mut Option<u64>, value: u64) {
            *slot = Some(slot.map_or(value, |current| current.min(value)));
        }
        match trigger {
            EpochClosureTrigger::Rows { count } => lower(&mut self.rows, *count),
            EpochClosureTrigger::Bytes { count } => lower(&mut self.bytes, *count),
            EpochClosureTrigger::Batches { count } => lower(&mut self.batches, *count),
            EpochClosureTrigger::Elapsed { milliseconds } => {
                lower(&mut self.elapsed_milliseconds, *milliseconds);
            }
            // Watermark advance is not a settlement-unit-local dimension: it is evaluated by the
            // controller against canonical watermark claims, which only exist at boundaries.
            EpochClosureTrigger::WatermarkAdvance { .. } => {}
        }
    }

    fn crossing_cause(
        &self,
        epoch: Counters,
        elapsed_milliseconds: Option<u64>,
    ) -> Option<SettlementClosureCause> {
        if self.rows.is_some_and(|limit| epoch.rows >= limit) {
            return Some(SettlementClosureCause::Rows);
        }
        if self.bytes.is_some_and(|limit| epoch.bytes >= limit) {
            return Some(SettlementClosureCause::Bytes);
        }
        if self.batches.is_some_and(|limit| epoch.batches >= limit) {
            return Some(SettlementClosureCause::Batches);
        }
        match (self.elapsed_milliseconds, elapsed_milliseconds) {
            (Some(limit), Some(elapsed)) if elapsed >= limit => {
                Some(SettlementClosureCause::Elapsed)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counters {
    batches: u64,
    rows: u64,
    bytes: u64,
}

impl Counters {
    fn add(&mut self, batches: u64, rows: u64, bytes: u64) -> Result<()> {
        self.batches = self
            .batches
            .checked_add(batches)
            .ok_or_else(|| CdfError::data("CDC settlement batch count overflow"))?;
        self.rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| CdfError::data("CDC settlement row count overflow"))?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| CdfError::data("CDC settlement byte count overflow"))?;
        Ok(())
    }
}

/// Exact phase-local overshoot admitted after closure was requested inside a settlement unit.
///
/// Row and batch counts are telemetry; bytes are the dimension bounded by
/// [`TransactionByteCeiling`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SettlementOvershoot {
    pub cause: SettlementClosureCause,
    /// Unit-local counts observed at the moment closure was requested.
    pub requested_at_batches: u64,
    pub requested_at_rows: u64,
    pub requested_at_bytes: u64,
    /// Counts admitted after that moment, before the proven terminal boundary.
    pub overshoot_batches: u64,
    pub overshoot_rows: u64,
    pub overshoot_bytes: u64,
}

/// One settlement unit proven terminal by its source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedSettlementUnit {
    pub kind: SettlementUnitKind,
    pub terminal_position: SourcePosition,
    pub batches: u64,
    pub rows: u64,
    pub bytes: u64,
    /// Present only when a closure request arrived while this unit was still open.
    pub overshoot: Option<SettlementOvershoot>,
}

impl CompletedSettlementUnit {
    /// The winner policy CDC always reduces under.
    ///
    /// Source protocol order makes last-change-wins the *truthful* answer for a keyed CDC stream,
    /// which is precisely why CDC differs from ordinary merge: an unordered merge has no
    /// authoritative winner and fails on duplicate keys instead of silently picking one.
    pub const WINNER_POLICY: KeyedEffectWinnerPolicy = KeyedEffectWinnerPolicy::Last;

    /// The `(protocol, scope_sha256)` order identity this unit hands to package finalization.
    ///
    /// This deliberately returns the identity tuple rather than a built
    /// [`KeyedEffectInputOrder`](cdf_kernel::KeyedEffectInputOrder). The engine already owns the
    /// single construction site for `SourceProtocol` order during package finalization; building a
    /// second one here would be exactly the "separate pattern matches with subtly different log
    /// semantics" the CDC foundation forbids, and the two could drift without any test noticing.
    ///
    /// The identity comes from the *terminal* position, so the reduction is scoped to the log
    /// lineage that actually proved the ordering. A position that is not an admitted CDC kind
    /// cannot produce one.
    pub fn cdc_order_identity(&self) -> Result<(String, String)> {
        self.terminal_position.cdc_protocol_order_identity()
    }

    /// Lowers a proven terminal unit into the one canonical safe frontier the drain controller
    /// accepts.
    ///
    /// Exactly one position is admitted per settlement unit: the boundary the source proved. That
    /// is what makes a mid-unit checkpoint structurally impossible rather than merely discouraged —
    /// the controller is never offered an interior position to close on.
    #[must_use]
    pub fn into_observation(
        self,
        carryover: Option<SourcePosition>,
        global_watermark: Option<WatermarkClaim>,
        source_exhausted: bool,
        monotonic_milliseconds: u64,
        observed_at_unix_milliseconds: u64,
    ) -> DrainSafeFrontierObservation {
        DrainSafeFrontierObservation {
            frontier: self.terminal_position,
            carryover,
            admitted_batches: self.batches,
            admitted_rows: self.rows,
            admitted_bytes: self.bytes,
            admitted_positions: 1,
            global_watermark,
            source_exhausted,
            monotonic_milliseconds,
            observed_at_unix_milliseconds,
        }
    }
}

#[derive(Clone, Debug)]
struct OpenUnit {
    counters: Counters,
    scope_anchor: SourcePosition,
    last_position: SourcePosition,
    requested: Option<(SettlementClosureCause, Counters)>,
}

#[derive(Clone, Debug)]
enum UnitState {
    /// Between units. A new unit may begin unless closure was already requested.
    Idle,
    /// A unit is accumulating and must run to its proven terminal boundary.
    Open(Box<OpenUnit>),
    /// Closure was requested and no unit is open. No further admission is possible.
    Sealed,
}

/// The neutral finite-drain runtime archetype.
///
/// One instance owns exactly one ordered source stream. Admission, closure, and boundary
/// publication are sequenced so that publishing a frontier without a completed unit, and beginning
/// a unit after closure was requested, are both rejected by construction rather than by convention.
#[derive(Debug)]
pub struct CdcLogSourceRuntime {
    kind: SettlementUnitKind,
    ceiling: TransactionByteCeiling,
    thresholds: SettlementClosureThresholds,
    state: UnitState,
    epoch: Counters,
    units_completed: u64,
}

impl CdcLogSourceRuntime {
    /// Binds the archetype to one drain extent, unit kind, and resolved byte ceiling.
    pub fn new(
        extent: &ExecutionExtent,
        kind: SettlementUnitKind,
        ceiling: TransactionByteCeiling,
    ) -> Result<Self> {
        Ok(Self {
            kind,
            ceiling,
            thresholds: SettlementClosureThresholds::from_extent(extent)?,
            state: UnitState::Idle,
            epoch: Counters::default(),
            units_completed: 0,
        })
    }

    #[must_use]
    pub const fn kind(&self) -> SettlementUnitKind {
        self.kind
    }

    #[must_use]
    pub const fn ceiling(&self) -> TransactionByteCeiling {
        self.ceiling
    }

    #[must_use]
    pub const fn units_completed(&self) -> u64 {
        self.units_completed
    }

    /// Whether a settlement unit is currently open.
    #[must_use]
    pub const fn unit_open(&self) -> bool {
        matches!(self.state, UnitState::Open(_))
    }

    /// Whether closure has been requested, whether or not a unit is still draining.
    #[must_use]
    pub const fn closure_requested(&self) -> bool {
        match &self.state {
            UnitState::Sealed => true,
            UnitState::Open(unit) => unit.requested.is_some(),
            UnitState::Idle => false,
        }
    }

    /// Whether the archetype will accept another settlement unit.
    #[must_use]
    pub const fn admits_further_units(&self) -> bool {
        matches!(self.state, UnitState::Idle)
    }

    /// Opens a settlement unit anchored on the position of its first change.
    ///
    /// Fails when a unit is already open, or once closure has been requested — no later unit may
    /// enter an epoch whose closure is pending.
    pub fn begin_unit(&mut self, anchor: &SourcePosition) -> Result<()> {
        match &self.state {
            UnitState::Open(_) => {
                return Err(CdfError::contract(
                    "CDC settlement unit is already open; one ordered stream admits one unit at a \
                     time",
                ));
            }
            UnitState::Sealed => {
                return Err(CdfError::contract(
                    "CDC settlement unit cannot begin after closure was requested",
                ));
            }
            UnitState::Idle => {}
        }
        anchor.validate()?;
        anchor.cdc_protocol_order_identity()?;
        self.state = UnitState::Open(Box::new(OpenUnit {
            counters: Counters::default(),
            scope_anchor: anchor.clone(),
            last_position: anchor.clone(),
            requested: None,
        }));
        Ok(())
    }

    /// Admits one homogeneous CDC batch into the open settlement unit.
    ///
    /// Validates typed operation and exact position metadata, enforces the byte ceiling before any
    /// publication can occur, and records where a cadence threshold was first crossed.
    pub fn admit_batch(
        &mut self,
        metadata: &CdcMetadata,
        batch_position: &SourcePosition,
        rows: u64,
        bytes: u64,
        elapsed_milliseconds: Option<u64>,
    ) -> Result<()> {
        let ceiling = self.ceiling;
        let thresholds = self.thresholds;
        let epoch_before = self.epoch;
        let UnitState::Open(unit) = &mut self.state else {
            return Err(CdfError::contract(
                "CDC batch admission requires an open settlement unit",
            ));
        };

        metadata.validate(rows, Some(batch_position))?;
        if !unit.scope_anchor.same_scope(batch_position)? {
            return Err(CdfError::data(
                "CDC batch position scope does not match the open settlement unit",
            ));
        }

        let mut projected = unit.counters;
        projected.add(1, rows, bytes)?;
        if projected.bytes > ceiling.effective_bytes() {
            return Err(CdfError::data(format!(
                "CDC settlement unit reached {} bytes, exceeding the admitted maximum {}; no state \
                 advances",
                projected.bytes,
                ceiling.effective_bytes()
            )));
        }
        unit.counters = projected;
        unit.last_position = batch_position.clone();

        if unit.requested.is_none() {
            // Thresholds are epoch-scoped, so the projection must combine units already settled in
            // this epoch with everything accumulated by the open unit — not merely this batch.
            // Comparing one batch against the epoch limit would silently miss the crossing and
            // under-report overshoot.
            let mut epoch_projection = epoch_before;
            epoch_projection.add(
                unit.counters.batches,
                unit.counters.rows,
                unit.counters.bytes,
            )?;
            if let Some(cause) = thresholds.crossing_cause(epoch_projection, elapsed_milliseconds) {
                unit.requested = Some((cause, unit.counters));
            }
        }
        Ok(())
    }

    /// Records an externally requested closure, such as command termination.
    ///
    /// When no unit is open the archetype seals immediately. When a unit is open it continues to
    /// its proven terminal boundary and the resulting overshoot is recorded.
    pub fn request_closure(&mut self, cause: SettlementClosureCause) {
        match &mut self.state {
            UnitState::Idle => self.state = UnitState::Sealed,
            UnitState::Open(unit) => {
                if unit.requested.is_none() {
                    unit.requested = Some((cause, unit.counters));
                }
            }
            UnitState::Sealed => {}
        }
    }

    /// Completes the open unit at a source-proven terminal position.
    ///
    /// This is the only path that yields a publishable boundary. For an ordered committed
    /// transaction the terminal position must not regress against the last admitted position; for
    /// an opaque event prefix only scope is checked, because CDF never claims numeric ordering from
    /// token internals.
    pub fn complete_unit(
        &mut self,
        terminal_position: &SourcePosition,
    ) -> Result<CompletedSettlementUnit> {
        let kind = self.kind;
        let UnitState::Open(unit) = &self.state else {
            return Err(CdfError::contract(
                "CDC settlement completion requires an open settlement unit",
            ));
        };
        let unit = unit.clone();

        terminal_position.validate()?;
        terminal_position.cdc_protocol_order_identity()?;
        if !unit.scope_anchor.same_scope(terminal_position)? {
            return Err(CdfError::data(
                "CDC terminal position scope does not match the settlement unit",
            ));
        }
        if unit.counters.rows == 0 {
            return Err(CdfError::data(
                "CDC settlement unit cannot complete without an admitted change",
            ));
        }
        if kind == SettlementUnitKind::CommittedTransaction
            && !terminal_position.reaches(&unit.last_position)?
        {
            return Err(CdfError::data(
                "CDC committed-transaction terminal position regresses against an admitted change",
            ));
        }

        let overshoot = unit.requested.map(|(cause, at)| SettlementOvershoot {
            cause,
            requested_at_batches: at.batches,
            requested_at_rows: at.rows,
            requested_at_bytes: at.bytes,
            overshoot_batches: unit.counters.batches.saturating_sub(at.batches),
            overshoot_rows: unit.counters.rows.saturating_sub(at.rows),
            overshoot_bytes: unit.counters.bytes.saturating_sub(at.bytes),
        });

        self.epoch.add(
            unit.counters.batches,
            unit.counters.rows,
            unit.counters.bytes,
        )?;
        self.units_completed = self
            .units_completed
            .checked_add(1)
            .ok_or_else(|| CdfError::data("CDC settlement unit count overflow"))?;
        self.state = if overshoot.is_some() {
            UnitState::Sealed
        } else {
            UnitState::Idle
        };

        Ok(CompletedSettlementUnit {
            kind,
            terminal_position: terminal_position.clone(),
            batches: unit.counters.batches,
            rows: unit.counters.rows,
            bytes: unit.counters.bytes,
            overshoot,
        })
    }

    /// Rejects a source event outside the admitted insert/update/delete vocabulary.
    ///
    /// Snapshot/read, truncate, DDL, and schema events have no truthful lowering into a keyed
    /// effect, so they are never silently mapped onto an admitted operation and never quarantined —
    /// dropping one would break transaction completeness. The open unit is abandoned so nothing
    /// partially observed can reach a package, receipt, or checkpoint.
    pub fn reject_unsupported_event(&mut self, descriptor: &str) -> CdfError {
        self.abandon_unit();
        CdfError::data(format!(
            "CDC source event `{descriptor}` is outside the admitted insert/update/delete \
             vocabulary; it requires explicit semantics rather than being mapped onto an admitted \
             operation, and no state advances"
        ))
    }

    /// Abandons the open unit without publishing a frontier.
    ///
    /// A partially observed unit has no committed effect: the prior checkpoint stays authoritative
    /// and its accumulated counts never reach the epoch.
    pub fn abandon_unit(&mut self) {
        if let UnitState::Open(unit) = &self.state {
            let sealed = unit.requested.is_some();
            self.state = if sealed {
                UnitState::Sealed
            } else {
                UnitState::Idle
            };
        }
    }

    /// Resets unit accounting after the epoch's frontier settled.
    pub fn acknowledge_settlement(&mut self) {
        self.epoch = Counters::default();
        if matches!(self.state, UnitState::Sealed) {
            self.state = UnitState::Idle;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drain_epoch::{DrainEpochController, DrainEpochDecision};
    use cdf_kernel::{
        CdcOperation, CommittedLogPosition, DrainTermination, LateDataAction,
        MongoChangeStreamResumeToken, MongoChangeStreamScope, MongoResumeMode,
        MongoResumeTokenSource, MongoWatchLevel, PostgresCommitPosition, PostgresLogScope,
        ResumeTokenPosition, SOURCE_POSITION_VERSION, STREAM_EPOCH_POLICY_VERSION,
        SafeFrontierPolicy, StreamEpochPolicy, WatermarkPolicy,
    };
    use cdf_memory::FixedSpillBudget;

    const SEMANTICS: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn extent(
        checkpoint_cadence: EpochClosureTrigger,
        package_rotation: EpochClosureTrigger,
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
            termination: DrainTermination::Records { count: 1_000_000 },
        }
    }

    /// A drain extent with thresholds far above anything the tests admit.
    fn quiet_extent() -> ExecutionExtent {
        extent(
            EpochClosureTrigger::Rows {
                count: 1_000_000_000,
            },
            EpochClosureTrigger::Bytes {
                count: 1_000_000_000,
            },
        )
    }

    fn pg(end_lsn: u64, slot: &str) -> SourcePosition {
        SourcePosition::committed_log(CommittedLogPosition::PostgreSql(PostgresCommitPosition {
            version: SOURCE_POSITION_VERSION,
            scope: PostgresLogScope {
                system_identifier: "7421938841407953395".to_owned(),
                database_oid: 16_384,
                slot: slot.to_owned(),
                output_plugin: "pgoutput".to_owned(),
                semantics_sha256: SEMANTICS.to_owned(),
            },
            commit_lsn: end_lsn.saturating_sub(1).max(1),
            end_lsn,
            xid: 7,
        }))
    }

    fn mongo(token_base64: &str, token_sha256: &str) -> SourcePosition {
        SourcePosition::resume_token(ResumeTokenPosition::MongoChangeStream(
            MongoChangeStreamResumeToken {
                version: SOURCE_POSITION_VERSION,
                scope: MongoChangeStreamScope {
                    source_binding: "orders-stream".to_owned(),
                    watch_level: MongoWatchLevel::Collection,
                    database: Some("sales".to_owned()),
                    collection: Some("orders".to_owned()),
                    pipeline_sha256: SEMANTICS.to_owned(),
                    options_sha256: SEMANTICS.to_owned(),
                },
                token_bson_base64: token_base64.to_owned(),
                token_sha256: token_sha256.to_owned(),
                resume_mode: MongoResumeMode::ResumeAfter,
                token_source: MongoResumeTokenSource::PostBatch,
            },
        ))
    }

    fn mongo_first() -> SourcePosition {
        mongo(
            "FgAAAAJfZGF0YQAGAAAAdG9rZW4AAA==",
            "sha256:2861e1850c87f3c48b875671d9fc0ca97b9c268ad17ff0b713a116989f2a68a2",
        )
    }

    fn mongo_second() -> SourcePosition {
        mongo(
            "FgAAAAJfZGF0YQAGAAAAdG9rMm4AAA==",
            "sha256:7d2f87737386e2a9297a4feb2c1dbccb00d44bf7b211080a9dadd0a98d75a2a0",
        )
    }

    fn meta(operation: CdcOperation, position: &SourcePosition) -> CdcMetadata {
        CdcMetadata {
            operation,
            position: position.clone(),
        }
    }

    fn runtime(kind: SettlementUnitKind, extent: &ExecutionExtent) -> CdcLogSourceRuntime {
        CdcLogSourceRuntime::new(
            extent,
            kind,
            TransactionByteCeiling::resolve(1_000_000, None).unwrap(),
        )
        .unwrap()
    }

    // --- byte ceiling -----------------------------------------------------------------------

    #[test]
    fn resource_may_lower_the_host_transaction_ceiling() {
        let ceiling = TransactionByteCeiling::resolve(4_096, Some(1_024)).unwrap();
        assert_eq!(ceiling.effective_bytes(), 1_024);
        assert_eq!(ceiling.host_maximum_bytes(), 4_096);
    }

    #[test]
    fn resource_may_not_raise_the_host_transaction_ceiling() {
        let error = TransactionByteCeiling::resolve(1_024, Some(4_096)).unwrap_err();
        assert!(
            error.message.contains("may only lower"),
            "unexpected message: {}",
            error.message
        );
    }

    #[test]
    fn host_ceiling_has_no_kernel_default() {
        assert!(TransactionByteCeiling::resolve(0, None).is_err());
        assert!(TransactionByteCeiling::resolve(1_024, Some(0)).is_err());
    }

    #[test]
    fn settlement_unit_over_the_ceiling_fails_before_any_publication() {
        let extent = quiet_extent();
        let mut source = CdcLogSourceRuntime::new(
            &extent,
            SettlementUnitKind::CommittedTransaction,
            TransactionByteCeiling::resolve(1_000, Some(100)).unwrap(),
        )
        .unwrap();
        let position = pg(10, "orders");
        source.begin_unit(&position).unwrap();
        let error = source
            .admit_batch(
                &meta(CdcOperation::Insert, &position),
                &position,
                1,
                101,
                None,
            )
            .unwrap_err();
        assert!(
            error.message.contains("exceeding the admitted maximum"),
            "unexpected message: {}",
            error.message
        );
        // Nothing was published and no unit completed.
        assert_eq!(source.units_completed(), 0);
    }

    // --- transaction-aligned closure --------------------------------------------------------

    #[test]
    fn closure_requested_mid_unit_waits_for_the_boundary_and_records_exact_overshoot() {
        // A 10-row threshold is crossed by the second batch, but the transaction commits only
        // after a third.
        let extent = extent(
            EpochClosureTrigger::Rows { count: 10 },
            EpochClosureTrigger::Bytes {
                count: 1_000_000_000,
            },
        );
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();

        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 4, 40, None)
            .unwrap();
        assert!(!source.closure_requested(), "threshold not yet crossed");

        source
            .admit_batch(&meta(CdcOperation::Update, &start), &start, 8, 80, None)
            .unwrap();
        assert!(
            source.closure_requested(),
            "crossing 10 rows must request closure"
        );
        assert!(
            source.unit_open(),
            "the unit must stay open until the source proves the boundary"
        );

        // The transaction continues past the request; this is the overshoot.
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 5, 50, None)
            .unwrap();

        let commit = pg(20, "orders");
        let completed = source.complete_unit(&commit).unwrap();

        assert_eq!(completed.rows, 17);
        assert_eq!(completed.bytes, 170);
        assert_eq!(completed.batches, 3);
        assert_eq!(completed.terminal_position, commit);

        let overshoot = completed.overshoot.expect("overshoot must be recorded");
        assert_eq!(overshoot.cause, SettlementClosureCause::Rows);
        assert_eq!(overshoot.requested_at_rows, 12);
        assert_eq!(overshoot.requested_at_bytes, 120);
        assert_eq!(overshoot.requested_at_batches, 2);
        assert_eq!(overshoot.overshoot_rows, 5);
        assert_eq!(overshoot.overshoot_bytes, 50);
        assert_eq!(overshoot.overshoot_batches, 1);
    }

    #[test]
    fn no_later_unit_is_admitted_once_closure_was_requested() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 1, 10, None)
            .unwrap();
        source.request_closure(SettlementClosureCause::Termination);

        // The open unit still finishes at its proven boundary.
        let completed = source.complete_unit(&pg(20, "orders")).unwrap();
        assert_eq!(
            completed.overshoot.map(|overshoot| overshoot.cause),
            Some(SettlementClosureCause::Termination)
        );

        assert!(!source.admits_further_units());
        let error = source.begin_unit(&pg(30, "orders")).unwrap_err();
        assert!(
            error.message.contains("after closure was requested"),
            "unexpected message: {}",
            error.message
        );
    }

    #[test]
    fn termination_with_no_open_unit_seals_immediately() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        source.request_closure(SettlementClosureCause::Termination);
        assert!(source.closure_requested());
        assert!(source.begin_unit(&pg(10, "orders")).is_err());
    }

    // --- partial units never publish --------------------------------------------------------

    #[test]
    fn abandoned_unit_publishes_nothing_and_retains_prior_authority() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 9, 90, None)
            .unwrap();
        source.abandon_unit();

        assert_eq!(source.units_completed(), 0);
        assert!(!source.unit_open());
        // A fresh unit may still begin: nothing was sealed and nothing advanced.
        assert!(source.begin_unit(&pg(20, "orders")).is_ok());
    }

    #[test]
    fn completion_requires_an_open_unit_and_an_admitted_change() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        assert!(source.complete_unit(&pg(10, "orders")).is_err());

        source.begin_unit(&pg(10, "orders")).unwrap();
        let error = source.complete_unit(&pg(20, "orders")).unwrap_err();
        assert!(
            error.message.contains("without an admitted change"),
            "unexpected message: {}",
            error.message
        );
    }

    #[test]
    fn a_second_unit_cannot_open_while_one_is_open() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        source.begin_unit(&pg(10, "orders")).unwrap();
        assert!(source.begin_unit(&pg(11, "orders")).is_err());
    }

    // --- typed metadata and narrow provenance -----------------------------------------------

    #[test]
    fn batch_position_scope_mismatch_is_rejected() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();

        let other_scope = pg(11, "shipments");
        let error = source
            .admit_batch(
                &meta(CdcOperation::Insert, &other_scope),
                &other_scope,
                1,
                10,
                None,
            )
            .unwrap_err();
        assert!(
            error.message.contains("scope does not match"),
            "unexpected message: {}",
            error.message
        );
    }

    #[test]
    fn metadata_position_must_match_the_batch_position() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();

        let drifted = pg(11, "orders");
        assert!(
            source
                .admit_batch(&meta(CdcOperation::Insert, &drifted), &start, 1, 10, None)
                .is_err(),
            "operation position must corroborate the batch source position"
        );
    }

    #[test]
    fn zero_row_batches_are_rejected() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        assert!(
            source
                .admit_batch(&meta(CdcOperation::Delete, &start), &start, 0, 0, None)
                .is_err()
        );
    }

    #[test]
    fn committed_transaction_terminal_position_may_not_regress() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(30, "orders");
        source.begin_unit(&start).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 1, 10, None)
            .unwrap();

        let error = source.complete_unit(&pg(20, "orders")).unwrap_err();
        assert!(
            error.message.contains("regresses"),
            "unexpected message: {}",
            error.message
        );
    }

    #[test]
    fn terminal_scope_mismatch_is_rejected() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 1, 10, None)
            .unwrap();
        assert!(source.complete_unit(&pg(20, "shipments")).is_err());
    }

    // --- opaque event prefixes ---------------------------------------------------------------

    #[test]
    fn event_prefix_accepts_an_adapter_proven_terminal_token_without_ordering_claims() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::EventPrefix, &extent);
        let first = mongo_first();
        source.begin_unit(&first).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &first), &first, 3, 30, None)
            .unwrap();

        // A different opaque token terminates the prefix. No numeric reachability is asserted,
        // which is exactly why this succeeds where a committed-log regression would fail.
        let completed = source.complete_unit(&mongo_second()).unwrap();
        assert_eq!(completed.kind, SettlementUnitKind::EventPrefix);
        assert_eq!(completed.terminal_position, mongo_second());
        assert_eq!(completed.rows, 3);
        assert!(completed.overshoot.is_none());
    }

    #[test]
    fn event_prefix_still_enforces_scope() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::EventPrefix, &extent);
        let first = mongo_first();
        source.begin_unit(&first).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &first), &first, 1, 10, None)
            .unwrap();
        assert!(source.complete_unit(&pg(10, "orders")).is_err());
    }

    // --- rechunking invariance ----------------------------------------------------------------

    #[test]
    fn arbitrary_arrow_rechunking_yields_an_identical_settled_unit() {
        // The same 24 rows / 240 bytes split three different ways must settle identically. This is
        // the property a source cannot control: upstream Arrow batch boundaries are arbitrary.
        let splits: [&[u64]; 3] = [&[24], &[1, 23], &[6, 6, 6, 3, 3]];
        let mut settled = Vec::new();
        for split in splits {
            let extent = quiet_extent();
            let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
            let start = pg(10, "orders");
            source.begin_unit(&start).unwrap();
            for rows in split {
                source
                    .admit_batch(
                        &meta(CdcOperation::Insert, &start),
                        &start,
                        *rows,
                        rows * 10,
                        None,
                    )
                    .unwrap();
            }
            let completed = source.complete_unit(&pg(20, "orders")).unwrap();
            assert_eq!(completed.rows, 24);
            assert_eq!(completed.bytes, 240);
            assert_eq!(completed.terminal_position, pg(20, "orders"));
            settled.push((completed.rows, completed.bytes, completed.terminal_position));
        }
        assert!(
            settled.windows(2).all(|pair| pair[0] == pair[1]),
            "rechunking changed the settled unit"
        );
    }

    #[test]
    fn elapsed_cadence_crossing_is_recorded_as_a_mid_unit_request() {
        let extent = extent(
            EpochClosureTrigger::Elapsed { milliseconds: 50 },
            EpochClosureTrigger::Bytes {
                count: 1_000_000_000,
            },
        );
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 1, 10, Some(10))
            .unwrap();
        assert!(!source.closure_requested());
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 1, 10, Some(80))
            .unwrap();
        assert!(source.closure_requested());

        let completed = source.complete_unit(&pg(20, "orders")).unwrap();
        let overshoot = completed.overshoot.expect("overshoot must be recorded");
        assert_eq!(overshoot.cause, SettlementClosureCause::Elapsed);
        assert_eq!(overshoot.overshoot_rows, 0);
    }

    // --- integration with the real drain epoch controller -------------------------------------

    fn observe(
        controller: &mut DrainEpochController,
        completed: CompletedSettlementUnit,
        clock: u64,
    ) -> DrainEpochDecision {
        controller
            .observe_safe_frontier(completed.into_observation(
                None,
                None,
                false,
                clock,
                1_700_000_000_000 + clock,
            ))
            .unwrap()
    }

    /// Drive one transaction that crosses the cadence threshold partway through, and assert the
    /// controller closes on the commit position rather than any interior position.
    #[test]
    fn controller_closes_only_at_the_proven_transaction_boundary() {
        let extent = extent(
            EpochClosureTrigger::Rows { count: 10 },
            EpochClosureTrigger::Bytes {
                count: 1_000_000_000,
            },
        );
        let mut controller = DrainEpochController::new(&extent).unwrap();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);

        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        for _ in 0..3 {
            source
                .admit_batch(&meta(CdcOperation::Insert, &start), &start, 6, 60, None)
                .unwrap();
        }
        let commit = pg(20, "orders");
        let completed = source.complete_unit(&commit).unwrap();
        assert!(completed.overshoot.is_some(), "threshold crossed mid-unit");

        let decision = observe(&mut controller, completed, 100);
        let DrainEpochDecision::Close(closure) = decision else {
            panic!("18 rows past a 10-row cadence must close the epoch");
        };
        assert_eq!(
            closure.frontier.frontier, commit,
            "the epoch must close on the committed boundary, never an interior position"
        );
    }

    /// An abandoned partial unit must be invisible to the controller: no observation, therefore no
    /// frontier, therefore the prior committed authority stands.
    #[test]
    fn abandoned_partial_unit_never_reaches_the_controller() {
        let extent = extent(
            EpochClosureTrigger::Rows { count: 5 },
            EpochClosureTrigger::Bytes {
                count: 1_000_000_000,
            },
        );
        let mut controller = DrainEpochController::new(&extent).unwrap();
        let prior = pg(5, "orders");
        controller
            .bind_initial_committed_state(Some(prior.clone()), None, None, Vec::new(), 0)
            .unwrap();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);

        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 50, 500, None)
            .unwrap();
        source.abandon_unit();

        assert_eq!(
            controller.committed_frontier(),
            Some(&prior),
            "a partially observed transaction must leave the prior checkpoint authoritative"
        );
    }

    /// Several small transactions accumulate across the epoch; only the one that carries the epoch
    /// past the cadence closes it, and it still closes on its own commit boundary.
    #[test]
    fn multiple_units_accumulate_until_the_cadence_boundary() {
        let extent = extent(
            EpochClosureTrigger::Rows { count: 10 },
            EpochClosureTrigger::Bytes {
                count: 1_000_000_000,
            },
        );
        let mut controller = DrainEpochController::new(&extent).unwrap();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);

        for (index, lsn) in [10_u64, 20].into_iter().enumerate() {
            let position = pg(lsn, "orders");
            source.begin_unit(&position).unwrap();
            source
                .admit_batch(
                    &meta(CdcOperation::Insert, &position),
                    &position,
                    4,
                    40,
                    None,
                )
                .unwrap();
            let completed = source.complete_unit(&position).unwrap();
            assert!(
                completed.overshoot.is_none(),
                "unit {index} is below the cadence"
            );
            assert_eq!(
                observe(&mut controller, completed, 100 + lsn),
                DrainEpochDecision::Continue
            );
        }

        // The third transaction carries the epoch to 12 rows, past the 10-row cadence.
        let third = pg(30, "orders");
        source.begin_unit(&third).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &third), &third, 4, 40, None)
            .unwrap();
        let completed = source.complete_unit(&third).unwrap();
        assert!(
            completed.overshoot.is_some(),
            "the epoch cadence is crossed inside this unit"
        );
        let DrainEpochDecision::Close(closure) = observe(&mut controller, completed, 200) else {
            panic!("crossing the epoch cadence must close");
        };
        assert_eq!(closure.frontier.frontier, third);
    }

    /// Full round trip: close, settle the exact frontier, and resume admission.
    #[test]
    fn settlement_round_trip_resumes_admission() {
        let extent = extent(
            EpochClosureTrigger::Rows { count: 2 },
            EpochClosureTrigger::Bytes {
                count: 1_000_000_000,
            },
        );
        let mut controller = DrainEpochController::new(&extent).unwrap();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);

        let first = pg(10, "orders");
        source.begin_unit(&first).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &first), &first, 3, 30, None)
            .unwrap();
        let completed = source.complete_unit(&first).unwrap();
        let commit = completed.terminal_position.clone();
        let DrainEpochDecision::Close(_) = observe(&mut controller, completed, 100) else {
            panic!("must close");
        };

        assert!(
            !source.admits_further_units(),
            "no later unit while the epoch is unsettled"
        );
        controller.acknowledge_settlement(&commit).unwrap();
        source.acknowledge_settlement();

        assert_eq!(controller.committed_frontier(), Some(&commit));
        assert!(source.admits_further_units());
        source.begin_unit(&pg(20, "orders")).unwrap();
    }

    // --- A1.5 keyed-effect delegation ---------------------------------------------------------

    #[test]
    fn committed_log_unit_lowers_to_protocol_ordered_keyed_effects() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Delete, &start), &start, 2, 20, None)
            .unwrap();
        let completed = source.complete_unit(&pg(20, "orders")).unwrap();

        let (protocol, scope_sha256) = completed.cdc_order_identity().unwrap();
        assert_eq!(protocol, "postgresql");
        assert!(scope_sha256.starts_with("sha256:"));
        // The engine turns this tuple into `KeyedEffectInputOrder::SourceProtocol` under
        // last-change-wins; CDC must never reduce as unordered.
        assert_eq!(
            CompletedSettlementUnit::WINNER_POLICY,
            KeyedEffectWinnerPolicy::Last
        );
    }

    #[test]
    fn event_prefix_unit_lowers_to_its_own_protocol_order_scope() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::EventPrefix, &extent);
        let first = mongo_first();
        source.begin_unit(&first).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Update, &first), &first, 1, 10, None)
            .unwrap();
        let completed = source.complete_unit(&mongo_second()).unwrap();

        let (protocol, _) = completed.cdc_order_identity().unwrap();
        assert_eq!(protocol, "mongodb_change_stream");
    }

    /// The reduction scope must not drift across a unit: every admitted position and the terminal
    /// position share one protocol order identity, or the reduction would be scoped to a lineage
    /// that did not prove the ordering.
    #[test]
    fn reduction_scope_is_stable_across_the_settlement_unit() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 1, 10, None)
            .unwrap();
        let completed = source.complete_unit(&pg(20, "orders")).unwrap();

        assert_eq!(
            start.cdc_protocol_order_identity().unwrap(),
            completed
                .terminal_position
                .cdc_protocol_order_identity()
                .unwrap(),
            "scope identity drifted between admission and the proven boundary"
        );
        // A different slot is a different lineage and must not compare equal.
        assert_ne!(
            start.cdc_protocol_order_identity().unwrap(),
            pg(20, "shipments").cdc_protocol_order_identity().unwrap()
        );
    }

    #[test]
    fn ceiling_resolves_from_the_live_spill_budget() {
        let budget = FixedSpillBudget::new(8_192).unwrap();
        let ceiling = TransactionByteCeiling::from_spill_budget(&budget, None).unwrap();
        assert_eq!(ceiling.effective_bytes(), 8_192);
        assert_eq!(ceiling.host_maximum_bytes(), 8_192);

        let lowered = TransactionByteCeiling::from_spill_budget(&budget, Some(512)).unwrap();
        assert_eq!(lowered.effective_bytes(), 512);

        assert!(
            TransactionByteCeiling::from_spill_budget(&budget, Some(8_193)).is_err(),
            "a resource may not raise the ceiling above live host spill authority"
        );
    }

    // --- deterministic synthetic log model ------------------------------------------------------

    /// Deterministic generator. No external `rand` dependency and no wall clock, so a failing
    /// schedule is reproducible from its seed alone.
    struct Lcg(u64);

    impl Lcg {
        fn next_in(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 33) % bound.max(1)
        }
    }

    /// One synthetic transaction: a row count, split across batches by the schedule under test.
    fn synthetic_transactions(seed: u64, count: usize) -> Vec<u64> {
        let mut rng = Lcg(seed);
        (0..count).map(|_| 1 + rng.next_in(9)).collect()
    }

    /// Replays a transaction schedule through the archetype and a real controller, returning the
    /// settled frontier sequence. `chunk` controls how each transaction's rows are split into Arrow
    /// batches — the dimension a source cannot control.
    fn replay(transactions: &[u64], chunk: u64) -> Vec<(SourcePosition, u64, u64)> {
        let extent = extent(
            EpochClosureTrigger::Rows { count: 25 },
            EpochClosureTrigger::Bytes {
                count: 1_000_000_000,
            },
        );
        let mut controller = DrainEpochController::new(&extent).unwrap();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let mut settled = Vec::new();

        for (index, rows) in transactions.iter().enumerate() {
            if !source.admits_further_units() {
                // The epoch closed; settle it and continue with the next one.
                let frontier = settled
                    .last()
                    .map(|(position, _, _): &(SourcePosition, u64, u64)| position.clone());
                if let Some(frontier) = frontier {
                    controller.acknowledge_settlement(&frontier).unwrap();
                }
                source.acknowledge_settlement();
            }
            let lsn = (index as u64 + 1) * 10;
            let position = pg(lsn, "orders");
            source.begin_unit(&position).unwrap();

            let mut remaining = *rows;
            while remaining > 0 {
                let batch = remaining.min(chunk.max(1));
                source
                    .admit_batch(
                        &meta(CdcOperation::Insert, &position),
                        &position,
                        batch,
                        batch * 10,
                        None,
                    )
                    .unwrap();
                remaining -= batch;
            }

            let completed = source.complete_unit(&position).unwrap();
            let record = (
                completed.terminal_position.clone(),
                completed.rows,
                completed.bytes,
            );
            let _ = observe(&mut controller, completed, 1_000 + lsn);
            settled.push(record);
        }
        settled
    }

    /// The settled frontier sequence must depend only on the source's transactions, never on how
    /// upstream Arrow batches happened to be chunked.
    #[test]
    fn settled_sequence_is_invariant_under_arbitrary_rechunking() {
        for seed in [1_u64, 7, 42, 1_337, 90_210] {
            let transactions = synthetic_transactions(seed, 12);
            let baseline = replay(&transactions, 1);
            for chunk in [2_u64, 3, 5, 8, 64] {
                assert_eq!(
                    replay(&transactions, chunk),
                    baseline,
                    "seed {seed} chunk {chunk} changed the settled sequence"
                );
            }
        }
    }

    /// Cancellation injected at any point inside a transaction must publish nothing for that
    /// transaction and must leave the previously committed frontier authoritative.
    #[test]
    fn cancellation_inside_any_transaction_publishes_nothing() {
        let transactions = synthetic_transactions(2_024, 6);
        for cancel_at in 0..transactions.len() {
            let extent = quiet_extent();
            let mut controller = DrainEpochController::new(&extent).unwrap();
            let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
            let mut last_settled: Option<SourcePosition> = None;

            for (index, rows) in transactions.iter().enumerate() {
                let lsn = (index as u64 + 1) * 10;
                let position = pg(lsn, "orders");
                source.begin_unit(&position).unwrap();
                source
                    .admit_batch(
                        &meta(CdcOperation::Insert, &position),
                        &position,
                        *rows,
                        rows * 10,
                        None,
                    )
                    .unwrap();

                if index == cancel_at {
                    source.abandon_unit();
                    break;
                }
                let completed = source.complete_unit(&position).unwrap();
                last_settled = Some(completed.terminal_position.clone());
                let _ = observe(&mut controller, completed, 1_000 + lsn);
            }

            assert_eq!(
                source.units_completed() as usize,
                cancel_at,
                "cancelling transaction {cancel_at} must settle exactly the units before it"
            );
            // The cancelled transaction contributed no frontier of its own.
            let cancelled_position = pg((cancel_at as u64 + 1) * 10, "orders");
            assert_ne!(last_settled.as_ref(), Some(&cancelled_position));
        }
    }

    /// A transaction at the ceiling settles; one byte more fails, and failing leaves the already
    /// settled frontier untouched.
    #[test]
    fn within_limit_settles_and_over_limit_fails_without_advancing() {
        let extent = quiet_extent();
        let mut source = CdcLogSourceRuntime::new(
            &extent,
            SettlementUnitKind::CommittedTransaction,
            TransactionByteCeiling::resolve(10_000, Some(100)).unwrap(),
        )
        .unwrap();

        let first = pg(10, "orders");
        source.begin_unit(&first).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &first), &first, 10, 100, None)
            .unwrap();
        assert!(
            source.complete_unit(&first).is_ok(),
            "exactly at the ceiling"
        );
        assert_eq!(source.units_completed(), 1);

        let second = pg(20, "orders");
        source.begin_unit(&second).unwrap();
        assert!(
            source
                .admit_batch(&meta(CdcOperation::Insert, &second), &second, 11, 101, None)
                .is_err(),
            "one byte over the ceiling must fail"
        );
        assert_eq!(
            source.units_completed(),
            1,
            "the failure must not advance settled state"
        );
    }

    /// Crash before the commit was observed, then restart.
    ///
    /// A fresh controller and archetype are rebuilt from the last committed frontier — the state a
    /// restart would recover — and must resume from it, with the interrupted transaction leaving no
    /// trace. Limit: this exercises the runtime boundary only. It does not touch the SQLite
    /// checkpoint store, the package workspace, or a real process restart.
    #[test]
    fn restart_after_an_unobserved_commit_resumes_from_the_prior_checkpoint() {
        // A 4-row cadence so the first transaction actually closes an epoch and can be settled;
        // a quiet extent would leave the controller open with nothing to acknowledge.
        let extent = extent(
            EpochClosureTrigger::Rows { count: 4 },
            EpochClosureTrigger::Bytes {
                count: 1_000_000_000,
            },
        );

        // --- first process: settle one transaction, then crash inside the next ---
        let mut controller = DrainEpochController::new(&extent).unwrap();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);

        let settled_position = pg(10, "orders");
        source.begin_unit(&settled_position).unwrap();
        source
            .admit_batch(
                &meta(CdcOperation::Insert, &settled_position),
                &settled_position,
                4,
                40,
                None,
            )
            .unwrap();
        let completed = source.complete_unit(&settled_position).unwrap();
        let _ = observe(&mut controller, completed, 100);
        controller
            .acknowledge_settlement(&settled_position)
            .unwrap();
        source.acknowledge_settlement();

        // A second transaction spools rows but its commit is never observed.
        let interrupted = pg(20, "orders");
        source.begin_unit(&interrupted).unwrap();
        source
            .admit_batch(
                &meta(CdcOperation::Insert, &interrupted),
                &interrupted,
                99,
                990,
                None,
            )
            .unwrap();
        let durable_frontier = controller.committed_frontier().cloned();
        drop(controller);
        drop(source);

        assert_eq!(
            durable_frontier.as_ref(),
            Some(&settled_position),
            "only the observed commit may be durable"
        );

        // --- second process: rebuild from the recovered frontier ---
        let mut controller = DrainEpochController::new(&extent).unwrap();
        controller
            .bind_initial_committed_state(durable_frontier.clone(), None, None, Vec::new(), 1)
            .unwrap();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);

        assert_eq!(
            controller.committed_frontier(),
            Some(&settled_position),
            "restart must resume from the prior checkpoint, not the interrupted transaction"
        );
        assert_eq!(
            source.units_completed(),
            0,
            "a rebuilt archetype carries no interrupted work"
        );

        // Replaying the interrupted transaction from the recovered position settles normally.
        source.begin_unit(&interrupted).unwrap();
        source
            .admit_batch(
                &meta(CdcOperation::Insert, &interrupted),
                &interrupted,
                99,
                990,
                None,
            )
            .unwrap();
        let replayed = source.complete_unit(&interrupted).unwrap();
        assert_eq!(replayed.rows, 99);
        let _ = observe(&mut controller, replayed, 200);
    }

    #[test]
    fn unsupported_events_fail_typed_and_abandon_the_unit() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 1, 10, None)
            .unwrap();

        let error = source.reject_unsupported_event("TRUNCATE");
        assert!(
            error.message.contains("outside the admitted"),
            "unexpected message: {}",
            error.message
        );
        assert!(!source.unit_open(), "the partial unit must be abandoned");
        assert_eq!(source.units_completed(), 0, "no state advances");
    }

    #[test]
    fn settlement_acknowledgement_reopens_admission() {
        let extent = quiet_extent();
        let mut source = runtime(SettlementUnitKind::CommittedTransaction, &extent);
        let start = pg(10, "orders");
        source.begin_unit(&start).unwrap();
        source
            .admit_batch(&meta(CdcOperation::Insert, &start), &start, 1, 10, None)
            .unwrap();
        source.request_closure(SettlementClosureCause::Termination);
        source.complete_unit(&pg(20, "orders")).unwrap();
        assert!(!source.admits_further_units());

        source.acknowledge_settlement();
        assert!(source.admits_further_units());
        assert!(source.begin_unit(&pg(30, "orders")).is_ok());
    }
}
