use std::{cmp::Ordering, collections::BTreeMap};

use cdf_kernel::{
    CdfError, PartitionId, PartitionWatermarkAggregation, Result, WatermarkClaim, WatermarkPolicy,
    WatermarkValue,
};

#[derive(Clone, Debug)]
struct PartitionWatermarkState {
    claim: Option<WatermarkClaim>,
    last_activity_milliseconds: u64,
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
        for partition_id in partitions {
            if states
                .insert(
                    partition_id.clone(),
                    PartitionWatermarkState {
                        claim: None,
                        last_activity_milliseconds: started_milliseconds,
                    },
                )
                .is_some()
            {
                return Err(CdfError::contract(format!(
                    "watermark tracker received duplicate partition `{partition_id}`"
                )));
            }
        }
        Ok(Self {
            policy: policy.clone(),
            partitions: states,
            effective_floor,
        })
    }

    pub fn observe_partition_progress(
        &mut self,
        partition_id: &PartitionId,
        claim: Option<&WatermarkClaim>,
        monotonic_milliseconds: u64,
    ) -> Result<Option<WatermarkClaim>> {
        let state = self.partitions.get_mut(partition_id).ok_or_else(|| {
            CdfError::data(format!(
                "watermark claim references unplanned partition `{partition_id}`"
            ))
        })?;
        if monotonic_milliseconds < state.last_activity_milliseconds {
            return Err(CdfError::internal(
                "watermark partition activity clock moved backwards",
            ));
        }
        state.last_activity_milliseconds = monotonic_milliseconds;

        match (&self.policy, claim) {
            (WatermarkPolicy::Disabled, Some(_)) => {
                return Err(CdfError::data(
                    "source emitted a watermark while the compiled policy disables watermarks",
                ));
            }
            (WatermarkPolicy::Disabled, None) => return Ok(None),
            (WatermarkPolicy::Enabled { .. }, None) => {}
            (WatermarkPolicy::Enabled { .. }, Some(claim)) => {
                claim.validate()?;
                if &claim.partition_id != partition_id {
                    return Err(CdfError::data(format!(
                        "watermark claim partition `{}` does not match batch partition `{partition_id}`",
                        claim.partition_id
                    )));
                }
                if let Some(previous) = state.claim.as_ref()
                    && compare_claims(previous, claim)? == Ordering::Greater
                {
                    return Err(CdfError::data(format!(
                        "watermark regressed within partition `{partition_id}`"
                    )));
                }
                state.claim = Some(claim.clone());
            }
        }
        self.effective_watermark(monotonic_milliseconds)
    }

    pub fn effective_watermark(
        &mut self,
        monotonic_milliseconds: u64,
    ) -> Result<Option<WatermarkClaim>> {
        let WatermarkPolicy::Enabled {
            partition_aggregation,
            ..
        } = &self.policy
        else {
            return Ok(None);
        };
        let mut minimum = None::<&WatermarkClaim>;
        let mut eligible_count = 0_usize;
        for state in self.partitions.values() {
            let eligible = match partition_aggregation {
                PartitionWatermarkAggregation::MinimumAll => true,
                PartitionWatermarkAggregation::MinimumEligible {
                    idle_after_milliseconds,
                    ..
                } => {
                    monotonic_milliseconds.saturating_sub(state.last_activity_milliseconds)
                        < *idle_after_milliseconds
                }
            };
            if !eligible {
                continue;
            }
            eligible_count = eligible_count.saturating_add(1);
            let Some(claim) = state.claim.as_ref() else {
                return Ok(self.effective_floor.clone());
            };
            if match minimum {
                None => true,
                Some(current) => compare_claims(claim, current)? == Ordering::Less,
            } {
                minimum = Some(claim);
            }
        }
        if eligible_count == 0 {
            return Ok(self.effective_floor.clone());
        }
        let Some(candidate) = minimum.cloned() else {
            return Ok(self.effective_floor.clone());
        };
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
}
