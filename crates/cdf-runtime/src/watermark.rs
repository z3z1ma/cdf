use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use cdf_kernel::{
    CdfError, PartitionId, PartitionWatermarkAggregation, Result, WatermarkClaim, WatermarkPolicy,
    WatermarkValue,
};

#[derive(Clone, Debug)]
struct PartitionWatermarkState {
    claim: Option<WatermarkClaim>,
    last_activity_milliseconds: u64,
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
    idle_deadlines: BTreeSet<(u64, PartitionId)>,
    last_observation_milliseconds: u64,
    effective_floor: Option<WatermarkClaim>,
}

impl PartitionWatermarkTracker {
    pub fn new<'a>(
        policy: &WatermarkPolicy,
        partitions: impl IntoIterator<Item = &'a PartitionId>,
        started_milliseconds: u64,
    ) -> Result<Self> {
        Self::new_with_floor(policy, partitions, started_milliseconds, None)
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
        validate_floor(policy, effective_floor.as_ref())?;
        let mut states = BTreeMap::new();
        let mut idle_deadlines = BTreeSet::new();
        let idle_after_milliseconds = idle_after(policy);
        for partition_id in partitions {
            if states
                .insert(
                    partition_id.clone(),
                    PartitionWatermarkState {
                        claim: None,
                        last_activity_milliseconds: started_milliseconds,
                        eligible: matches!(policy, WatermarkPolicy::Enabled { .. }),
                    },
                )
                .is_some()
            {
                return Err(CdfError::contract(format!(
                    "watermark tracker received duplicate partition `{partition_id}`"
                )));
            }
            if let Some(deadline) = idle_after_milliseconds
                .and_then(|idle_after| started_milliseconds.checked_add(idle_after))
            {
                idle_deadlines.insert((deadline, partition_id.clone()));
            }
        }
        let eligible_missing_claims = if matches!(policy, WatermarkPolicy::Enabled { .. }) {
            states.len()
        } else {
            0
        };
        Ok(Self {
            policy: policy.clone(),
            partitions: states,
            eligible_claims: BTreeSet::new(),
            eligible_missing_claims,
            idle_deadlines,
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
                validate_claim_against_policy(&self.policy, claim)?;
                if &claim.partition_id != partition_id {
                    return Err(CdfError::data(format!(
                        "watermark claim partition `{}` does not match batch partition `{partition_id}`",
                        claim.partition_id
                    )));
                }
                if let Some(previous) = self
                    .partitions
                    .get(partition_id)
                    .and_then(|state| state.claim.as_ref())
                    && compare_claims(previous, claim)? == Ordering::Greater
                {
                    return Err(CdfError::data(format!(
                        "watermark regressed within partition `{partition_id}`"
                    )));
                }
            }
        }

        let idle_after_milliseconds = idle_after(&self.policy);
        let state = self.partitions.get_mut(partition_id).ok_or_else(|| {
            CdfError::data(format!(
                "watermark claim references unplanned partition `{partition_id}`"
            ))
        })?;
        if let Some(deadline) = idle_after_milliseconds
            .and_then(|idle_after| state.last_activity_milliseconds.checked_add(idle_after))
        {
            self.idle_deadlines
                .remove(&(deadline, partition_id.clone()));
        }
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
        state.last_activity_milliseconds = monotonic_milliseconds;
        if let Some(deadline) = idle_after_milliseconds
            .and_then(|idle_after| monotonic_milliseconds.checked_add(idle_after))
        {
            self.idle_deadlines.insert((deadline, partition_id.clone()));
        }
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

    fn advance_clock(&mut self, monotonic_milliseconds: u64) -> Result<()> {
        if monotonic_milliseconds < self.last_observation_milliseconds {
            return Err(CdfError::internal(
                "watermark observation clock moved backwards",
            ));
        }
        self.last_observation_milliseconds = monotonic_milliseconds;
        while let Some((deadline, partition_id)) = self.idle_deadlines.first().cloned() {
            if deadline > monotonic_milliseconds {
                break;
            }
            self.idle_deadlines
                .remove(&(deadline, partition_id.clone()));
            let state = self.partitions.get_mut(&partition_id).ok_or_else(|| {
                CdfError::internal("watermark idle index references an unknown partition")
            })?;
            if !state.eligible {
                continue;
            }
            state.eligible = false;
            if let Some(claim) = state.claim.as_ref() {
                self.eligible_claims
                    .remove(&(WatermarkOrderKey::from(&claim.value), partition_id));
            } else {
                self.eligible_missing_claims = self
                    .eligible_missing_claims
                    .checked_sub(1)
                    .ok_or_else(|| CdfError::internal("eligible watermark count underflow"))?;
            }
        }
        Ok(())
    }
}

fn idle_after(policy: &WatermarkPolicy) -> Option<u64> {
    match policy {
        WatermarkPolicy::Enabled {
            partition_aggregation:
                PartitionWatermarkAggregation::MinimumEligible {
                    idle_after_milliseconds,
                    ..
                },
            ..
        } => Some(*idle_after_milliseconds),
        _ => None,
    }
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
    fn minimum_eligible_excludes_only_partitions_past_the_compiled_idle_window() {
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
        assert_eq!(
            tracker.effective_watermark(10).unwrap().unwrap().value,
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

        assert_eq!(
            tracker
                .observe_partition_progress(&b, Some(&claim("b", 5)), 101)
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
                .observe_partition_progress(&b, Some(&claim("b", 55)), 102)
                .unwrap()
                .unwrap()
                .value,
            WatermarkValue::Unsigned(50)
        );
        assert_eq!(
            tracker
                .observe_partition_progress(&a, Some(&claim("a", 60)), 103)
                .unwrap()
                .unwrap()
                .value,
            WatermarkValue::Unsigned(55)
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
        assert_eq!(tracker.idle_deadlines.len(), PARTITIONS);
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
            tracker.partitions.len() + tracker.eligible_claims.len() + tracker.idle_deadlines.len(),
        );
    }
}
