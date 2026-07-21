use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use cdf_kernel::{
    CdfError, PARTITION_WATERMARK_STATE_VERSION, PartitionId, PartitionIdlenessClaim,
    PartitionWatermarkAggregation, PartitionWatermarkState as RecordedPartitionWatermarkState,
    Result, WatermarkClaim, WatermarkPolicy, WatermarkValue, validate_partition_watermark_states,
};

#[derive(Clone, Debug)]
struct PartitionWatermarkState {
    claim: Option<WatermarkClaim>,
    idleness: Option<PartitionIdlenessClaim>,
    eligible: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum WatermarkOrderKey {
    Signed(i64),
    Unsigned(u64),
    Decimal(i128),
    Date32(i32),
    Date64(i64),
    Timestamp(i64),
}

impl From<&WatermarkValue> for WatermarkOrderKey {
    fn from(value: &WatermarkValue) -> Self {
        match value {
            WatermarkValue::Signed(value) => Self::Signed(*value),
            WatermarkValue::Unsigned(value) => Self::Unsigned(*value),
            WatermarkValue::Decimal(value) => Self::Decimal(*value),
            WatermarkValue::Date32(value) => Self::Date32(*value),
            WatermarkValue::Date64(value) => Self::Date64(*value),
            WatermarkValue::Timestamp(value) => Self::Timestamp(*value),
        }
    }
}

/// Bounded, execution-local aggregation of partition watermark claims.
///
/// The tracker never invents a global claim from the latest partition observed. It emits only
/// the minimum claim admitted by the compiled partition policy, retaining one claim and one
/// activity timestamp per planned partition.
#[derive(Clone, Debug)]
pub struct PartitionWatermarkTracker {
    policy: WatermarkPolicy,
    partitions: BTreeMap<PartitionId, PartitionWatermarkState>,
    eligible_claims: BTreeSet<(WatermarkOrderKey, PartitionId)>,
    eligible_missing_claims: usize,
    historical: BTreeMap<PartitionId, RecordedPartitionWatermarkState>,
    last_observation_milliseconds: u64,
    effective_floor: Option<WatermarkClaim>,
}

impl PartitionWatermarkTracker {
    /// Registers one partition from a streamed external task authority.
    ///
    /// Inline plans register their complete bounded topology at construction. External plans call
    /// this as each canonical task is opened so watermark tracking remains proportional to active
    /// and observed partitions rather than requiring task-set materialization before execution.
    pub fn register_partition(&mut self, partition_id: &PartitionId) -> Result<()> {
        if self.partitions.contains_key(partition_id) {
            return Err(CdfError::contract(format!(
                "watermark tracker received duplicate partition `{partition_id}`"
            )));
        }
        let restored = self.historical.remove(partition_id);
        let state = match restored {
            Some(restored) => PartitionWatermarkState {
                claim: restored.claim,
                idleness: restored.idleness.clone(),
                eligible: restored.idleness.is_none()
                    && matches!(self.policy, WatermarkPolicy::Enabled { .. }),
            },
            None => PartitionWatermarkState {
                claim: None,
                idleness: None,
                eligible: matches!(self.policy, WatermarkPolicy::Enabled { .. }),
            },
        };
        if state.eligible {
            if let Some(claim) = &state.claim {
                self.eligible_claims
                    .insert((WatermarkOrderKey::from(&claim.value), partition_id.clone()));
            } else {
                self.eligible_missing_claims = self
                    .eligible_missing_claims
                    .checked_add(1)
                    .ok_or_else(|| CdfError::internal("eligible watermark count overflow"))?;
            }
        }
        self.partitions.insert(partition_id.clone(), state);
        Ok(())
    }

    pub fn new<'a>(
        policy: &WatermarkPolicy,
        partitions: impl IntoIterator<Item = &'a PartitionId>,
        started_milliseconds: u64,
    ) -> Result<Self> {
        Self::new_with_state(policy, partitions, started_milliseconds, None, &[])
    }

    /// Restores the last receipt-gated global watermark before observing a new epoch.
    ///
    /// A newly eligible or resumed partition may be behind this floor. Its rows are then late;
    /// the already-committed completeness claim must never be retracted to accommodate it.
    pub fn new_with_floor<'a>(
        policy: &WatermarkPolicy,
        partitions: impl IntoIterator<Item = &'a PartitionId>,
        started_milliseconds: u64,
        effective_floor: Option<WatermarkClaim>,
    ) -> Result<Self> {
        Self::new_with_state(
            policy,
            partitions,
            started_milliseconds,
            effective_floor,
            &[],
        )
    }

    /// Restores the receipt-gated partition claims and source-authored idleness evidence.
    pub fn new_with_state<'a>(
        policy: &WatermarkPolicy,
        partitions: impl IntoIterator<Item = &'a PartitionId>,
        started_milliseconds: u64,
        effective_floor: Option<WatermarkClaim>,
        restored: &[RecordedPartitionWatermarkState],
    ) -> Result<Self> {
        validate_floor(policy, effective_floor.as_ref())?;
        validate_partition_watermark_states(restored)?;
        let mut states = BTreeMap::new();
        for partition_id in partitions {
            if states
                .insert(
                    partition_id.clone(),
                    PartitionWatermarkState {
                        claim: None,
                        idleness: None,
                        eligible: matches!(policy, WatermarkPolicy::Enabled { .. }),
                    },
                )
                .is_some()
            {
                return Err(CdfError::contract(format!(
                    "watermark tracker received duplicate partition `{partition_id}`"
                )));
            }
        }
        let mut historical = BTreeMap::new();
        for recorded in restored {
            validate_recorded_state_against_policy(policy, recorded)?;
            if let Some(state) = states.get_mut(&recorded.partition_id) {
                state.claim = recorded.claim.clone();
                state.idleness = recorded.idleness.clone();
                state.eligible = recorded.idleness.is_none()
                    && matches!(policy, WatermarkPolicy::Enabled { .. });
            } else {
                historical.insert(recorded.partition_id.clone(), recorded.clone());
            }
        }
        let mut eligible_claims = BTreeSet::new();
        let mut eligible_missing_claims = 0_usize;
        for (partition_id, state) in &states {
            if !state.eligible {
                continue;
            }
            if let Some(claim) = &state.claim {
                eligible_claims
                    .insert((WatermarkOrderKey::from(&claim.value), partition_id.clone()));
            } else {
                eligible_missing_claims = eligible_missing_claims
                    .checked_add(1)
                    .ok_or_else(|| CdfError::internal("eligible watermark count overflow"))?;
            }
        }
        Ok(Self {
            policy: policy.clone(),
            partitions: states,
            eligible_claims,
            eligible_missing_claims,
            historical,
            last_observation_milliseconds: started_milliseconds,
            effective_floor,
        })
    }

    pub fn observe_partition_progress(
        &mut self,
        partition_id: &PartitionId,
        claim: Option<&WatermarkClaim>,
        monotonic_milliseconds: u64,
    ) -> Result<Option<WatermarkClaim>> {
        self.advance_clock(monotonic_milliseconds)?;
        if !self.partitions.contains_key(partition_id) {
            return Err(CdfError::data(format!(
                "watermark claim references unplanned partition `{partition_id}`"
            )));
        }

        match (&self.policy, claim) {
            (WatermarkPolicy::Disabled, Some(_)) => {
                return Err(CdfError::data(
                    "source emitted a watermark while the compiled policy disables watermarks",
                ));
            }
            (WatermarkPolicy::Disabled, None) => return Ok(None),
            (WatermarkPolicy::Enabled { .. }, None) => {}
            (WatermarkPolicy::Enabled { .. }, Some(claim)) => {
                let previous = self
                    .partitions
                    .get(partition_id)
                    .and_then(|state| state.claim.as_ref());
                self.validate_partition_claim(partition_id, previous, claim)?;
            }
        }

        let state = self.partitions.get_mut(partition_id).ok_or_else(|| {
            CdfError::data(format!(
                "watermark claim references unplanned partition `{partition_id}`"
            ))
        })?;
        if !state.eligible {
            state.eligible = true;
            if let Some(previous) = state.claim.as_ref() {
                self.eligible_claims.insert((
                    WatermarkOrderKey::from(&previous.value),
                    partition_id.clone(),
                ));
            } else {
                self.eligible_missing_claims = self
                    .eligible_missing_claims
                    .checked_add(1)
                    .ok_or_else(|| CdfError::internal("eligible watermark count overflow"))?;
            }
        }
        state.idleness = None;
        if let Some(claim) = claim {
            if let Some(previous) = state.claim.as_ref() {
                self.eligible_claims.remove(&(
                    WatermarkOrderKey::from(&previous.value),
                    partition_id.clone(),
                ));
            } else {
                self.eligible_missing_claims = self
                    .eligible_missing_claims
                    .checked_sub(1)
                    .ok_or_else(|| CdfError::internal("eligible watermark count underflow"))?;
            }
            if state.eligible {
                self.eligible_claims
                    .insert((WatermarkOrderKey::from(&claim.value), partition_id.clone()));
            }
            state.claim = Some(claim.clone());
        }
        self.effective_watermark(monotonic_milliseconds)
    }

    /// Excludes one partition only from source-authored idleness evidence admitted by the exact
    /// compiled capability. Scheduler delay and host timers never manufacture eligibility.
    pub fn observe_partition_idle(
        &mut self,
        partition_id: &PartitionId,
        idleness: &PartitionIdlenessClaim,
        monotonic_milliseconds: u64,
    ) -> Result<Option<WatermarkClaim>> {
        self.advance_clock(monotonic_milliseconds)?;
        validate_idleness_against_policy(&self.policy, partition_id, idleness)?;
        let state = self.partitions.get_mut(partition_id).ok_or_else(|| {
            CdfError::data(format!(
                "partition idleness references unplanned partition `{partition_id}`"
            ))
        })?;
        if state.eligible {
            state.eligible = false;
            if let Some(claim) = &state.claim {
                self.eligible_claims
                    .remove(&(WatermarkOrderKey::from(&claim.value), partition_id.clone()));
            } else {
                self.eligible_missing_claims = self
                    .eligible_missing_claims
                    .checked_sub(1)
                    .ok_or_else(|| CdfError::internal("eligible watermark count underflow"))?;
            }
        }
        state.idleness = Some(idleness.clone());
        self.effective_watermark(monotonic_milliseconds)
    }

    /// Canonical receipt-gated state for the next epoch or process.
    pub fn snapshot(&self) -> Result<Vec<RecordedPartitionWatermarkState>> {
        let mut snapshot = self.historical.clone();
        for (partition_id, state) in &self.partitions {
            if state.claim.is_none() && state.idleness.is_none() {
                continue;
            }
            snapshot.insert(
                partition_id.clone(),
                RecordedPartitionWatermarkState {
                    version: PARTITION_WATERMARK_STATE_VERSION,
                    partition_id: partition_id.clone(),
                    claim: state.claim.clone(),
                    idleness: state.idleness.clone(),
                },
            );
        }
        let snapshot = snapshot.into_values().collect::<Vec<_>>();
        validate_partition_watermark_states(&snapshot)?;
        Ok(snapshot)
    }

    pub fn effective_watermark(
        &mut self,
        monotonic_milliseconds: u64,
    ) -> Result<Option<WatermarkClaim>> {
        if matches!(self.policy, WatermarkPolicy::Disabled) {
            return Ok(None);
        }
        self.advance_clock(monotonic_milliseconds)?;
        if self.eligible_missing_claims != 0 {
            return Ok(self.effective_floor.clone());
        }
        let Some((_, minimum_partition)) = self.eligible_claims.first() else {
            return Ok(self.effective_floor.clone());
        };
        let candidate = self
            .partitions
            .get(minimum_partition)
            .and_then(|state| state.claim.clone())
            .ok_or_else(|| {
                CdfError::internal("eligible watermark index references a missing partition claim")
            })?;
        if let Some(floor) = self.effective_floor.as_ref()
            && compare_claims(&candidate, floor)? == Ordering::Less
        {
            return Ok(Some(floor.clone()));
        }
        self.effective_floor = Some(candidate.clone());
        Ok(Some(candidate))
    }

    /// The strongest global completeness claim already admitted in this execution.
    pub fn effective_floor(&self) -> Option<&WatermarkClaim> {
        self.effective_floor.as_ref()
    }

    /// Validates one source claim without mutating aggregate state.
    ///
    /// Callers use this for every claim in an ordered batch header before passing the final claim
    /// to [`Self::observe_partition_progress`]. That prevents a later valid claim from hiding an
    /// earlier malformed or regressing claim in the same batch.
    pub fn validate_partition_claim(
        &self,
        partition_id: &PartitionId,
        previous: Option<&WatermarkClaim>,
        claim: &WatermarkClaim,
    ) -> Result<()> {
        if !self.partitions.contains_key(partition_id) {
            return Err(CdfError::data(format!(
                "watermark claim references unplanned partition `{partition_id}`"
            )));
        }
        validate_claim_against_policy(&self.policy, claim)?;
        if &claim.partition_id != partition_id {
            return Err(CdfError::data(format!(
                "watermark claim partition `{}` does not match batch partition `{partition_id}`",
                claim.partition_id
            )));
        }
        let previous = previous.or_else(|| {
            self.partitions
                .get(partition_id)
                .and_then(|state| state.claim.as_ref())
        });
        if let Some(previous) = previous
            && compare_claims(previous, claim)? == Ordering::Greater
        {
            return Err(CdfError::data(format!(
                "watermark regressed within partition `{partition_id}`"
            )));
        }
        Ok(())
    }

    fn advance_clock(&mut self, monotonic_milliseconds: u64) -> Result<()> {
        if monotonic_milliseconds < self.last_observation_milliseconds {
            return Err(CdfError::internal(
                "watermark observation clock moved backwards",
            ));
        }
        self.last_observation_milliseconds = monotonic_milliseconds;
        Ok(())
    }
}

fn idleness_policy(policy: &WatermarkPolicy) -> Option<(u64, &str)> {
    match policy {
        WatermarkPolicy::Enabled {
            partition_aggregation:
                PartitionWatermarkAggregation::MinimumEligible {
                    idle_after_milliseconds,
                    capability_id,
                },
            ..
        } => Some((*idle_after_milliseconds, capability_id)),
        _ => None,
    }
}

fn validate_idleness_against_policy(
    policy: &WatermarkPolicy,
    partition_id: &PartitionId,
    idleness: &PartitionIdlenessClaim,
) -> Result<()> {
    idleness.validate()?;
    let Some((minimum_idle, capability_id)) = idleness_policy(policy) else {
        return Err(CdfError::data(
            "source emitted partition idleness without minimum_eligible authority",
        ));
    };
    if &idleness.partition_id != partition_id
        || idleness.capability_id.as_ref() != capability_id
        || idleness.idle_for_milliseconds < minimum_idle
    {
        return Err(CdfError::data(
            "partition idleness does not match the compiled partition/capability/window authority",
        ));
    }
    Ok(())
}

fn validate_recorded_state_against_policy(
    policy: &WatermarkPolicy,
    state: &RecordedPartitionWatermarkState,
) -> Result<()> {
    if let Some(claim) = &state.claim {
        validate_claim_against_policy(policy, claim)?;
    }
    if let Some(idleness) = &state.idleness {
        validate_idleness_against_policy(policy, &state.partition_id, idleness)?;
    }
    Ok(())
}

fn validate_floor(policy: &WatermarkPolicy, floor: Option<&WatermarkClaim>) -> Result<()> {
    match (policy, floor) {
        (WatermarkPolicy::Disabled, Some(_)) => Err(CdfError::contract(
            "disabled watermark policy cannot restore a committed watermark",
        )),
        (WatermarkPolicy::Disabled, None) | (WatermarkPolicy::Enabled { .. }, None) => Ok(()),
        (
            WatermarkPolicy::Enabled {
                event_time_field,
                domain,
                authority,
                ..
            },
            Some(floor),
        ) => {
            floor.validate()?;
            if floor.event_time_field.as_ref() != event_time_field.as_ref()
                || &floor.domain != domain
                || &floor.authority != authority
            {
                return Err(CdfError::data(
                    "committed watermark floor does not match the compiled field/domain/authority",
                ));
            }
            Ok(())
        }
    }
}

fn validate_claim_against_policy(policy: &WatermarkPolicy, claim: &WatermarkClaim) -> Result<()> {
    claim.validate()?;
    let WatermarkPolicy::Enabled {
        event_time_field,
        domain,
        authority,
        ..
    } = policy
    else {
        return Err(CdfError::data(
            "source emitted a watermark while the compiled policy disables watermarks",
        ));
    };
    if claim.event_time_field.as_ref() != event_time_field.as_ref()
        || &claim.domain != domain
        || &claim.authority != authority
    {
        return Err(CdfError::data(
            "partition watermark claim does not match the compiled field/domain/authority",
        ));
    }
    Ok(())
}

fn compare_claims(left: &WatermarkClaim, right: &WatermarkClaim) -> Result<Ordering> {
    if left.event_time_field != right.event_time_field
        || left.domain != right.domain
        || left.authority != right.authority
        || left.policy_version != right.policy_version
    {
        return Err(CdfError::data(
            "partition watermark claims do not share one compiled semantic domain",
        ));
    }
    let ordering = match (&left.value, &right.value) {
        (WatermarkValue::Signed(left), WatermarkValue::Signed(right)) => left.cmp(right),
        (WatermarkValue::Unsigned(left), WatermarkValue::Unsigned(right)) => left.cmp(right),
        (WatermarkValue::Decimal(left), WatermarkValue::Decimal(right)) => left.cmp(right),
        (WatermarkValue::Date32(left), WatermarkValue::Date32(right)) => left.cmp(right),
        (WatermarkValue::Date64(left), WatermarkValue::Date64(right))
        | (WatermarkValue::Timestamp(left), WatermarkValue::Timestamp(right)) => left.cmp(right),
        _ => {
            return Err(CdfError::data(
                "partition watermark values do not share one type",
            ));
        }
    };
    Ok(ordering)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_kernel::{
        CursorPosition, CursorValue, EventTimeDomain, SOURCE_POSITION_VERSION,
        STREAM_EPOCH_POLICY_VERSION, SourcePosition, WATERMARK_CLAIM_VERSION, WatermarkAuthority,
        WatermarkObservationContext,
    };

    fn claim(partition: &str, value: u64) -> WatermarkClaim {
        WatermarkClaim {
            version: WATERMARK_CLAIM_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            event_time_field: "occurred_at".into(),
            domain: EventTimeDomain::UnsignedInteger,
            value: WatermarkValue::Unsigned(value),
            partition_id: PartitionId::new(partition).unwrap(),
            source_position: SourcePosition::Cursor(CursorPosition {
                version: SOURCE_POSITION_VERSION,
                field: "offset".to_owned(),
                value: CursorValue::U64(value),
            }),
            authority: WatermarkAuthority::Source,
            observation_context: WatermarkObservationContext::SourcePoll,
        }
    }

    fn idleness(partition: &str, idle_for_milliseconds: u64) -> PartitionIdlenessClaim {
        PartitionIdlenessClaim {
            version: cdf_kernel::PARTITION_IDLENESS_CLAIM_VERSION,
            partition_id: PartitionId::new(partition).unwrap(),
            source_position: SourcePosition::Cursor(CursorPosition {
                version: SOURCE_POSITION_VERSION,
                field: "offset".to_owned(),
                value: CursorValue::U64(0),
            }),
            capability_id: "source-idleness-v1".into(),
            idle_for_milliseconds,
        }
    }

    fn policy(aggregation: PartitionWatermarkAggregation) -> WatermarkPolicy {
        WatermarkPolicy::Enabled {
            event_time_field: "occurred_at".into(),
            domain: EventTimeDomain::UnsignedInteger,
            authority: WatermarkAuthority::Source,
            partition_aggregation: aggregation,
        }
    }

    #[test]
    fn minimum_all_never_promotes_one_fast_partition_to_global() {
        let a = PartitionId::new("a").unwrap();
        let b = PartitionId::new("b").unwrap();
        let mut tracker = PartitionWatermarkTracker::new(
            &policy(PartitionWatermarkAggregation::MinimumAll),
            [&a, &b],
            0,
        )
        .unwrap();
        assert_eq!(
            tracker
                .observe_partition_progress(&a, Some(&claim("a", 100)), 1)
                .unwrap(),
            None
        );
        assert_eq!(
            tracker
                .observe_partition_progress(&b, Some(&claim("b", 5)), 2)
                .unwrap()
                .unwrap()
                .value,
            WatermarkValue::Unsigned(5)
        );
    }

    #[test]
    fn minimum_eligible_excludes_only_source_attested_idle_partitions() {
        let a = PartitionId::new("a").unwrap();
        let b = PartitionId::new("b").unwrap();
        let mut tracker = PartitionWatermarkTracker::new(
            &policy(PartitionWatermarkAggregation::MinimumEligible {
                idle_after_milliseconds: 10,
                capability_id: "source-idleness-v1".into(),
            }),
            [&a, &b],
            0,
        )
        .unwrap();
        assert_eq!(
            tracker
                .observe_partition_progress(&a, Some(&claim("a", 100)), 1)
                .unwrap(),
            None
        );
        assert_eq!(tracker.effective_watermark(10).unwrap(), None);
        assert_eq!(
            tracker
                .observe_partition_idle(&b, &idleness("b", 10), 10)
                .unwrap()
                .unwrap()
                .value,
            WatermarkValue::Unsigned(100)
        );
    }

    #[test]
    fn resumed_partition_cannot_retract_a_committed_global_watermark() {
        let a = PartitionId::new("a").unwrap();
        let b = PartitionId::new("b").unwrap();
        let mut tracker = PartitionWatermarkTracker::new_with_floor(
            &policy(PartitionWatermarkAggregation::MinimumEligible {
                idle_after_milliseconds: 10,
                capability_id: "source-idleness-v1".into(),
            }),
            [&a, &b],
            100,
            Some(claim("a", 50)),
        )
        .unwrap();

        tracker
            .observe_partition_idle(&b, &idleness("b", 10), 110)
            .unwrap();
        tracker
            .observe_partition_idle(&a, &idleness("a", 10), 110)
            .unwrap();
        assert_eq!(
            tracker.effective_watermark(110).unwrap().unwrap().value,
            WatermarkValue::Unsigned(50)
        );
        assert_eq!(
            tracker
                .observe_partition_progress(&b, Some(&claim("b", 5)), 111)
                .unwrap()
                .unwrap()
                .value,
            WatermarkValue::Unsigned(50)
        );
        assert_eq!(
            tracker.effective_floor().unwrap().value,
            WatermarkValue::Unsigned(50)
        );
        assert_eq!(
            tracker
                .observe_partition_progress(&b, Some(&claim("b", 55)), 112)
                .unwrap()
                .unwrap()
                .value,
            WatermarkValue::Unsigned(55)
        );
        assert_eq!(
            tracker
                .observe_partition_progress(&a, Some(&claim("a", 60)), 113)
                .unwrap()
                .unwrap()
                .value,
            WatermarkValue::Unsigned(55)
        );
    }

    #[test]
    fn every_ordered_claim_is_validated_before_the_batch_tail() {
        let partition = PartitionId::new("p").unwrap();
        let tracker = PartitionWatermarkTracker::new(
            &policy(PartitionWatermarkAggregation::MinimumAll),
            [&partition],
            0,
        )
        .unwrap();
        let baseline = claim("p", 100);
        let regressed = claim("p", 90);
        let recovered_tail = claim("p", 110);

        assert!(
            tracker
                .validate_partition_claim(&partition, Some(&baseline), &regressed)
                .unwrap_err()
                .message
                .contains("regressed")
        );
        assert!(
            tracker
                .validate_partition_claim(&partition, Some(&regressed), &recovered_tail)
                .is_ok()
        );
    }

    #[test]
    fn missing_new_partition_claim_does_not_erase_committed_completeness() {
        let a = PartitionId::new("a").unwrap();
        let b = PartitionId::new("b").unwrap();
        let mut tracker = PartitionWatermarkTracker::new_with_floor(
            &policy(PartitionWatermarkAggregation::MinimumAll),
            [&a, &b],
            0,
            Some(claim("a", 50)),
        )
        .unwrap();

        assert_eq!(
            tracker
                .observe_partition_progress(&a, Some(&claim("a", 60)), 1)
                .unwrap()
                .unwrap()
                .value,
            WatermarkValue::Unsigned(50)
        );
        assert_eq!(
            tracker
                .observe_partition_progress(&b, Some(&claim("b", 70)), 2)
                .unwrap()
                .unwrap()
                .value,
            WatermarkValue::Unsigned(60)
        );
    }

    #[test]
    fn a_single_partition_claim_must_match_the_compiled_semantic_authority() {
        let partition = PartitionId::new("p").unwrap();
        let mut tracker = PartitionWatermarkTracker::new(
            &policy(PartitionWatermarkAggregation::MinimumAll),
            [&partition],
            0,
        )
        .unwrap();
        let mut mismatched = claim("p", 1);
        mismatched.event_time_field = "other_time".into();

        let error = tracker
            .observe_partition_progress(&partition, Some(&mismatched), 1)
            .unwrap_err();
        assert!(error.message.contains("compiled field/domain/authority"));
        assert_eq!(tracker.eligible_missing_claims, 1);
        assert!(tracker.eligible_claims.is_empty());
    }

    #[test]
    fn incremental_indexes_remain_linear_under_repeated_partition_updates() {
        const PARTITIONS: usize = 4_096;
        let partitions = (0..PARTITIONS)
            .map(|index| PartitionId::new(format!("p-{index:05}")).unwrap())
            .collect::<Vec<_>>();
        let policy = policy(PartitionWatermarkAggregation::MinimumEligible {
            idle_after_milliseconds: 1_000_000,
            capability_id: "source-idleness-v1".into(),
        });
        let mut tracker = PartitionWatermarkTracker::new(&policy, partitions.iter(), 0).unwrap();

        for (index, partition) in partitions.iter().enumerate() {
            tracker
                .observe_partition_progress(
                    partition,
                    Some(&claim(partition.as_str(), index as u64)),
                    1,
                )
                .unwrap();
        }
        for value in 1..=PARTITIONS as u64 {
            tracker
                .observe_partition_progress(
                    &partitions[0],
                    Some(&claim(partitions[0].as_str(), value)),
                    value + 1,
                )
                .unwrap();
        }

        assert_eq!(tracker.partitions.len(), PARTITIONS);
        assert_eq!(tracker.eligible_claims.len(), PARTITIONS);
        assert!(tracker.historical.is_empty());
        assert_eq!(tracker.eligible_missing_claims, 0);
    }

    #[test]
    fn tracker_rejects_a_non_monotone_control_clock() {
        let partition = PartitionId::new("p").unwrap();
        let mut tracker = PartitionWatermarkTracker::new(
            &policy(PartitionWatermarkAggregation::MinimumAll),
            [&partition],
            10,
        )
        .unwrap();

        let error = tracker.effective_watermark(9).unwrap_err();
        assert!(error.message.contains("clock moved backwards"));
    }

    #[test]
    #[ignore = "performance lab benchmark; run explicitly in release mode"]
    fn incremental_watermark_tracker_benchmark() {
        let partition_count = std::env::var("CDF_A9_WATERMARK_PARTITIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(100_000);
        let update_count = std::env::var("CDF_A9_WATERMARK_UPDATES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1_000_000);
        let partitions = (0..partition_count)
            .map(|index| PartitionId::new(format!("p-{index:08}")).unwrap())
            .collect::<Vec<_>>();
        let mut tracker = PartitionWatermarkTracker::new(
            &policy(PartitionWatermarkAggregation::MinimumAll),
            partitions.iter(),
            0,
        )
        .unwrap();
        for (index, partition) in partitions.iter().enumerate() {
            tracker
                .observe_partition_progress(
                    partition,
                    Some(&claim(partition.as_str(), index as u64)),
                    1,
                )
                .unwrap();
        }

        let mut update = claim(partitions[0].as_str(), partition_count as u64);
        let started = std::time::Instant::now();
        for sequence in 0..update_count {
            let value = (partition_count as u64).saturating_add(sequence);
            update.value = WatermarkValue::Unsigned(value);
            let SourcePosition::Cursor(position) = &mut update.source_position else {
                unreachable!()
            };
            position.value = CursorValue::U64(value);
            std::hint::black_box(
                tracker
                    .observe_partition_progress(&partitions[0], Some(&update), sequence + 2)
                    .unwrap(),
            );
        }
        let elapsed = started.elapsed();
        let nanoseconds_per_update = elapsed.as_nanos() as f64 / update_count as f64;
        eprintln!(
            "watermark-tracker partitions={partition_count} updates={update_count} elapsed_seconds={:.6} ns_per_update={nanoseconds_per_update:.2} updates_per_second={:.0} metadata_entries={}",
            elapsed.as_secs_f64(),
            update_count as f64 / elapsed.as_secs_f64(),
            tracker.partitions.len() + tracker.eligible_claims.len() + tracker.historical.len(),
        );
    }

    #[test]
    fn partition_claims_and_idleness_survive_epoch_reconstruction() {
        let a = PartitionId::new("a").unwrap();
        let b = PartitionId::new("b").unwrap();
        let policy = policy(PartitionWatermarkAggregation::MinimumEligible {
            idle_after_milliseconds: 10,
            capability_id: "source-idleness-v1".into(),
        });
        let mut first = PartitionWatermarkTracker::new(&policy, [&a, &b], 0).unwrap();
        first
            .observe_partition_progress(&a, Some(&claim("a", 20)), 1)
            .unwrap();
        first
            .observe_partition_idle(&b, &idleness("b", 10), 2)
            .unwrap();
        let snapshot = first.snapshot().unwrap();

        let second = PartitionWatermarkTracker::new_with_state(
            &policy,
            [&a, &b],
            3,
            Some(claim("a", 20)),
            &snapshot,
        )
        .unwrap();
        assert!(
            second
                .validate_partition_claim(&a, None, &claim("a", 19))
                .unwrap_err()
                .message
                .contains("regressed")
        );
        assert_eq!(second.snapshot().unwrap(), snapshot);
    }
}
