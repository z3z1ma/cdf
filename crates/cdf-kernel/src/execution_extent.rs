use serde::{Deserialize, Serialize};

use crate::{CanonicalArrowTimeUnit, CdfError, PartitionId, Result, SourcePosition};

pub const EXECUTION_EXTENT_VERSION: u16 = 1;
pub const STREAM_EPOCH_POLICY_VERSION: u16 = 1;
pub const WATERMARK_CLAIM_VERSION: u16 = 1;
pub const EPOCH_FRONTIER_VERSION: u16 = 1;
pub const EPOCH_CLOSURE_EVIDENCE_VERSION: u16 = 1;

/// The complete, identity-bearing execution lifetime of a plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "UncheckedExecutionExtent",
    tag = "kind",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExecutionExtent {
    Bounded {
        version: u16,
    },
    Drain {
        version: u16,
        policy: StreamEpochPolicy,
        termination: DrainTermination,
    },
    Resident {
        version: u16,
        policy: StreamEpochPolicy,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum UncheckedExecutionExtent {
    Bounded {
        version: u16,
    },
    Drain {
        version: u16,
        policy: StreamEpochPolicy,
        termination: DrainTermination,
    },
    Resident {
        version: u16,
        policy: StreamEpochPolicy,
    },
}

impl TryFrom<UncheckedExecutionExtent> for ExecutionExtent {
    type Error = CdfError;

    fn try_from(value: UncheckedExecutionExtent) -> Result<Self> {
        let extent = match value {
            UncheckedExecutionExtent::Bounded { version } => Self::Bounded { version },
            UncheckedExecutionExtent::Drain {
                version,
                policy,
                termination,
            } => Self::Drain {
                version,
                policy,
                termination,
            },
            UncheckedExecutionExtent::Resident { version, policy } => {
                Self::Resident { version, policy }
            }
        };
        extent.validate()?;
        Ok(extent)
    }
}

impl ExecutionExtent {
    pub const fn bounded() -> Self {
        Self::Bounded {
            version: EXECUTION_EXTENT_VERSION,
        }
    }

    pub fn validate(&self) -> Result<()> {
        let version = match self {
            Self::Bounded { version }
            | Self::Drain { version, .. }
            | Self::Resident { version, .. } => *version,
        };
        require_version("execution extent", version, EXECUTION_EXTENT_VERSION)?;

        match self {
            Self::Bounded { .. } => Ok(()),
            Self::Drain {
                policy,
                termination,
                ..
            } => {
                policy.validate()?;
                termination.validate()
            }
            Self::Resident { policy, .. } => policy.validate(),
        }
    }

    /// P3 records resident policy but does not yet execute a resident supervisor.
    pub fn validate_for_plan(&self) -> Result<()> {
        self.validate()?;
        if matches!(self, Self::Resident { .. }) {
            return Err(CdfError::contract(
                "resident execution is not enabled; use a finite drain termination or wait for the resident supervisor",
            ));
        }
        Ok(())
    }

    /// Only bounded execution is installed until P3 A8 supplies the finite
    /// epoch executor. Recording and planning a drain policy cannot silently
    /// select the bounded one-package path.
    pub fn validate_for_execution(&self) -> Result<()> {
        self.validate_for_plan()?;
        if matches!(self, Self::Drain { .. }) {
            return Err(CdfError::contract(
                "drain execution is not enabled; compile the policy for inspection only until the finite epoch executor is installed",
            ));
        }
        Ok(())
    }

    pub const fn is_bounded(&self) -> bool {
        matches!(self, Self::Bounded { .. })
    }
}

/// Complete policy required by every drain or resident execution extent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedStreamEpochPolicy", deny_unknown_fields)]
pub struct StreamEpochPolicy {
    pub version: u16,
    pub checkpoint_cadence: EpochClosureTrigger,
    pub package_rotation: EpochClosureTrigger,
    pub watermark: WatermarkPolicy,
    pub late_data: LateDataAction,
    pub safe_frontier: SafeFrontierPolicy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedStreamEpochPolicy {
    version: u16,
    checkpoint_cadence: EpochClosureTrigger,
    package_rotation: EpochClosureTrigger,
    watermark: WatermarkPolicy,
    late_data: LateDataAction,
    safe_frontier: SafeFrontierPolicy,
}

impl TryFrom<UncheckedStreamEpochPolicy> for StreamEpochPolicy {
    type Error = CdfError;

    fn try_from(value: UncheckedStreamEpochPolicy) -> Result<Self> {
        let policy = Self {
            version: value.version,
            checkpoint_cadence: value.checkpoint_cadence,
            package_rotation: value.package_rotation,
            watermark: value.watermark,
            late_data: value.late_data,
            safe_frontier: value.safe_frontier,
        };
        policy.validate()?;
        Ok(policy)
    }
}

impl StreamEpochPolicy {
    pub fn validate(&self) -> Result<()> {
        require_version(
            "stream epoch policy",
            self.version,
            STREAM_EPOCH_POLICY_VERSION,
        )?;
        self.checkpoint_cadence.validate("checkpoint cadence")?;
        self.package_rotation.validate("package rotation")?;
        self.watermark.validate()?;

        if (matches!(
            self.checkpoint_cadence,
            EpochClosureTrigger::WatermarkAdvance { .. }
        ) || matches!(
            self.package_rotation,
            EpochClosureTrigger::WatermarkAdvance { .. }
        )) && matches!(self.watermark, WatermarkPolicy::Disabled)
        {
            return Err(CdfError::contract(
                "watermark-advance epoch closure requires an enabled watermark policy",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EpochClosureTrigger {
    Batches { count: u64 },
    Rows { count: u64 },
    Bytes { count: u64 },
    Elapsed { milliseconds: u64 },
    WatermarkAdvance { units: u64 },
}

impl EpochClosureTrigger {
    fn validate(&self, field: &str) -> Result<()> {
        let value = match self {
            Self::Batches { count } | Self::Rows { count } | Self::Bytes { count } => *count,
            Self::Elapsed { milliseconds } => *milliseconds,
            Self::WatermarkAdvance { units } => *units,
        };
        if value == 0 {
            return Err(CdfError::contract(format!(
                "{field} trigger must be greater than zero"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DrainTermination {
    Quiescent,
    Duration { milliseconds: u64 },
    Records { count: u64 },
    Bytes { count: u64 },
    SourceFrontier { position: SourcePosition },
}

impl DrainTermination {
    fn validate(&self) -> Result<()> {
        let nonzero = match self {
            Self::Duration { milliseconds } => Some(("duration milliseconds", *milliseconds)),
            Self::Records { count } => Some(("record count", *count)),
            Self::Bytes { count } => Some(("byte count", *count)),
            Self::Quiescent | Self::SourceFrontier { .. } => None,
        };
        if let Some((name, value)) = nonzero
            && value == 0
        {
            return Err(CdfError::contract(format!(
                "drain termination {name} must be greater than zero"
            )));
        }
        if let Self::SourceFrontier { position } = self
            && position.version() == 0
        {
            return Err(CdfError::contract(
                "drain source frontier position version must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WatermarkPolicy {
    Disabled,
    Enabled {
        event_time_field: Box<str>,
        domain: EventTimeDomain,
        authority: WatermarkAuthority,
        partition_aggregation: PartitionWatermarkAggregation,
    },
}

impl WatermarkPolicy {
    fn validate(&self) -> Result<()> {
        let Self::Enabled {
            event_time_field,
            domain,
            authority,
            partition_aggregation,
        } = self
        else {
            return Ok(());
        };
        require_nonempty("watermark event-time field", event_time_field)?;
        domain.validate()?;
        authority.validate()?;
        partition_aggregation.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventTimeDomain {
    SignedInteger,
    UnsignedInteger,
    Decimal {
        precision: u8,
        scale: i8,
    },
    Date32,
    Date64,
    Timestamp {
        unit: CanonicalArrowTimeUnit,
        timezone: Option<Box<str>>,
    },
}

impl EventTimeDomain {
    fn validate(&self) -> Result<()> {
        if let Self::Decimal { precision, scale } = self {
            if *precision == 0 || *precision > 38 {
                return Err(CdfError::contract(
                    "watermark decimal precision must be between 1 and 38",
                ));
            }
            if scale.unsigned_abs() > *precision {
                return Err(CdfError::contract(
                    "watermark decimal scale magnitude cannot exceed precision",
                ));
            }
        }
        if let Self::Timestamp {
            timezone: Some(timezone),
            ..
        } = self
        {
            require_nonempty("watermark timezone", timezone)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WatermarkAuthority {
    Source,
    Derived { mapping_id: Box<str> },
}

impl WatermarkAuthority {
    fn validate(&self) -> Result<()> {
        if let Self::Derived { mapping_id } = self {
            require_nonempty("watermark mapping id", mapping_id)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PartitionWatermarkAggregation {
    MinimumAll,
    MinimumEligible {
        idle_after_milliseconds: u64,
        capability_id: Box<str>,
    },
}

impl PartitionWatermarkAggregation {
    fn validate(&self) -> Result<()> {
        if let Self::MinimumEligible {
            idle_after_milliseconds,
            capability_id,
        } = self
        {
            if *idle_after_milliseconds == 0 {
                return Err(CdfError::contract(
                    "watermark idle exclusion must wait more than zero milliseconds",
                ));
            }
            require_nonempty("watermark idleness capability id", capability_id)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LateDataAction {
    RecaptureNextEpoch,
    Quarantine,
    AdmitWithAnnotation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafeFrontierPolicy {
    CanonicalAdmittedSourcePosition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorWatermarkBehavior {
    Preserve,
    Transform { mapping_id: Box<str> },
    Drop,
}

impl OperatorWatermarkBehavior {
    pub fn validate(&self) -> Result<()> {
        if let Self::Transform { mapping_id } = self {
            require_nonempty("operator watermark mapping id", mapping_id)?;
        }
        Ok(())
    }
}

/// A typed monotone completeness claim attached to a batch or epoch frontier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedWatermarkClaim", deny_unknown_fields)]
pub struct WatermarkClaim {
    pub version: u16,
    pub policy_version: u16,
    pub event_time_field: Box<str>,
    pub domain: EventTimeDomain,
    pub value: WatermarkValue,
    pub partition_id: PartitionId,
    pub source_position: SourcePosition,
    pub authority: WatermarkAuthority,
    pub observation_context: WatermarkObservationContext,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedWatermarkClaim {
    version: u16,
    policy_version: u16,
    event_time_field: Box<str>,
    domain: EventTimeDomain,
    value: WatermarkValue,
    partition_id: PartitionId,
    source_position: SourcePosition,
    authority: WatermarkAuthority,
    observation_context: WatermarkObservationContext,
}

impl TryFrom<UncheckedWatermarkClaim> for WatermarkClaim {
    type Error = CdfError;

    fn try_from(value: UncheckedWatermarkClaim) -> Result<Self> {
        let claim = Self {
            version: value.version,
            policy_version: value.policy_version,
            event_time_field: value.event_time_field,
            domain: value.domain,
            value: value.value,
            partition_id: value.partition_id,
            source_position: value.source_position,
            authority: value.authority,
            observation_context: value.observation_context,
        };
        claim.validate()?;
        Ok(claim)
    }
}

impl WatermarkClaim {
    pub fn validate(&self) -> Result<()> {
        require_version("watermark claim", self.version, WATERMARK_CLAIM_VERSION)?;
        require_version(
            "watermark claim policy",
            self.policy_version,
            STREAM_EPOCH_POLICY_VERSION,
        )?;
        require_nonempty("watermark event-time field", &self.event_time_field)?;
        require_nonempty("watermark partition id", self.partition_id.as_str())?;
        if self.source_position.version() == 0 {
            return Err(CdfError::contract(
                "watermark source position version must be greater than zero",
            ));
        }
        self.domain.validate()?;
        self.authority.validate()?;
        self.observation_context.validate()?;
        self.value.validate_against(&self.domain)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WatermarkObservationContext {
    SourcePoll,
    EpochBarrier,
    Operator { operator_id: Box<str> },
}

impl WatermarkObservationContext {
    fn validate(&self) -> Result<()> {
        if let Self::Operator { operator_id } = self {
            require_nonempty("watermark observation operator id", operator_id)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum WatermarkValue {
    Signed(i64),
    Unsigned(u64),
    Decimal(i128),
    Date32(i32),
    Date64(i64),
    Timestamp(i64),
}

impl WatermarkValue {
    fn validate_against(&self, domain: &EventTimeDomain) -> Result<()> {
        let matches = matches!(
            (self, domain),
            (Self::Signed(_), EventTimeDomain::SignedInteger)
                | (Self::Unsigned(_), EventTimeDomain::UnsignedInteger)
                | (Self::Decimal(_), EventTimeDomain::Decimal { .. })
                | (Self::Date32(_), EventTimeDomain::Date32)
                | (Self::Date64(_), EventTimeDomain::Date64)
                | (Self::Timestamp(_), EventTimeDomain::Timestamp { .. })
        );
        if !matches {
            return Err(CdfError::contract(
                "watermark value kind does not match its event-time domain",
            ));
        }
        Ok(())
    }
}

/// Epoch-boundary shape intended for canonical identity. Host timing lives in
/// [`EpochClosureEvidence`]; A7 must persist identity from this frontier rather
/// than from the enclosing control-evidence artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedEpochFrontier", deny_unknown_fields)]
pub struct EpochFrontier {
    pub version: u16,
    pub policy_version: u16,
    pub epoch_ordinal: u64,
    pub frontier: SourcePosition,
    pub input_low: Option<SourcePosition>,
    pub input_high: SourcePosition,
    pub carryover: Option<SourcePosition>,
    pub watermark: Option<WatermarkClaim>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedEpochFrontier {
    version: u16,
    policy_version: u16,
    epoch_ordinal: u64,
    frontier: SourcePosition,
    input_low: Option<SourcePosition>,
    input_high: SourcePosition,
    carryover: Option<SourcePosition>,
    watermark: Option<WatermarkClaim>,
}

impl TryFrom<UncheckedEpochFrontier> for EpochFrontier {
    type Error = CdfError;

    fn try_from(value: UncheckedEpochFrontier) -> Result<Self> {
        let frontier = Self {
            version: value.version,
            policy_version: value.policy_version,
            epoch_ordinal: value.epoch_ordinal,
            frontier: value.frontier,
            input_low: value.input_low,
            input_high: value.input_high,
            carryover: value.carryover,
            watermark: value.watermark,
        };
        frontier.validate()?;
        Ok(frontier)
    }
}

impl EpochFrontier {
    pub fn validate(&self) -> Result<()> {
        require_version("epoch frontier", self.version, EPOCH_FRONTIER_VERSION)?;
        require_version(
            "epoch frontier policy",
            self.policy_version,
            STREAM_EPOCH_POLICY_VERSION,
        )?;
        if self.frontier.version() == 0 || self.input_high.version() == 0 {
            return Err(CdfError::contract(
                "epoch frontier positions must use a nonzero version",
            ));
        }
        if self
            .input_low
            .as_ref()
            .is_some_and(|position| position.version() == 0)
            || self
                .carryover
                .as_ref()
                .is_some_and(|position| position.version() == 0)
        {
            return Err(CdfError::contract(
                "epoch frontier optional positions must use a nonzero version",
            ));
        }
        if let Some(watermark) = &self.watermark {
            watermark.validate()?;
        }
        Ok(())
    }
}

/// Truthful control evidence for why and when a canonical frontier closed.
/// A7 must persist this through an explicitly nonidentity evidence channel.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedEpochClosureEvidence", deny_unknown_fields)]
pub struct EpochClosureEvidence {
    pub version: u16,
    pub frontier: EpochFrontier,
    pub trigger: EpochClosureTrigger,
    pub observation: EpochClosureObservation,
    pub observed_at_unix_milliseconds: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedEpochClosureEvidence {
    version: u16,
    frontier: EpochFrontier,
    trigger: EpochClosureTrigger,
    observation: EpochClosureObservation,
    observed_at_unix_milliseconds: u64,
}

impl TryFrom<UncheckedEpochClosureEvidence> for EpochClosureEvidence {
    type Error = CdfError;

    fn try_from(value: UncheckedEpochClosureEvidence) -> Result<Self> {
        let evidence = Self {
            version: value.version,
            frontier: value.frontier,
            trigger: value.trigger,
            observation: value.observation,
            observed_at_unix_milliseconds: value.observed_at_unix_milliseconds,
        };
        evidence.validate()?;
        Ok(evidence)
    }
}

impl EpochClosureEvidence {
    pub fn validate(&self) -> Result<()> {
        require_version(
            "epoch closure evidence",
            self.version,
            EPOCH_CLOSURE_EVIDENCE_VERSION,
        )?;
        self.frontier.validate()?;
        self.trigger.validate("epoch closure")?;
        self.observation.validate_against(&self.trigger)?;
        if self.observed_at_unix_milliseconds == 0 {
            return Err(CdfError::contract(
                "epoch closure observation time must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EpochClosureObservation {
    Batches {
        observed: u64,
        overshoot: u64,
    },
    Rows {
        observed: u64,
        overshoot: u64,
    },
    Bytes {
        observed: u64,
        overshoot: u64,
    },
    Elapsed {
        observed_milliseconds: u64,
        overshoot_milliseconds: u64,
    },
    WatermarkAdvance {
        observed_units: u64,
        overshoot_units: u64,
    },
}

impl EpochClosureObservation {
    fn validate_against(&self, trigger: &EpochClosureTrigger) -> Result<()> {
        let consistent = match (self, trigger) {
            (
                Self::Batches {
                    observed,
                    overshoot,
                },
                EpochClosureTrigger::Batches { count },
            )
            | (
                Self::Rows {
                    observed,
                    overshoot,
                },
                EpochClosureTrigger::Rows { count },
            )
            | (
                Self::Bytes {
                    observed,
                    overshoot,
                },
                EpochClosureTrigger::Bytes { count },
            ) => observed.checked_sub(*count) == Some(*overshoot),
            (
                Self::Elapsed {
                    observed_milliseconds,
                    overshoot_milliseconds,
                },
                EpochClosureTrigger::Elapsed { milliseconds },
            ) => observed_milliseconds.checked_sub(*milliseconds) == Some(*overshoot_milliseconds),
            (
                Self::WatermarkAdvance {
                    observed_units,
                    overshoot_units,
                },
                EpochClosureTrigger::WatermarkAdvance { units },
            ) => observed_units.checked_sub(*units) == Some(*overshoot_units),
            _ => false,
        };
        if !consistent {
            return Err(CdfError::contract(
                "epoch closure observation must match its trigger dimension and exact overshoot",
            ));
        }
        Ok(())
    }
}

fn require_version(name: &str, actual: u16, expected: u16) -> Result<()> {
    if actual != expected {
        return Err(CdfError::contract(format!(
            "{name} version {actual} is unsupported; expected version {expected}"
        )));
    }
    Ok(())
}

fn require_nonempty(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(CdfError::contract(format!("{name} cannot be empty")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CursorPosition, CursorValue};

    #[test]
    fn bounded_extent_has_one_current_versioned_artifact_shape() {
        let extent = ExecutionExtent::bounded();
        extent.validate_for_plan().unwrap();

        let value = serde_json::to_value(&extent).unwrap();
        assert_eq!(value, serde_json::json!({"kind": "bounded", "version": 1}));
        assert_eq!(
            serde_json::from_value::<ExecutionExtent>(value).unwrap(),
            extent
        );

        let legacy = serde_json::json!({"kind": "bounded"});
        assert!(serde_json::from_value::<ExecutionExtent>(legacy).is_err());
        let wrong_version = serde_json::json!({"kind": "bounded", "version": 2});
        let error = serde_json::from_value::<ExecutionExtent>(wrong_version).unwrap_err();
        assert!(error.to_string().contains("expected version 1"));
    }

    #[test]
    fn drain_extent_requires_complete_valid_policy() {
        let extent = ExecutionExtent::Drain {
            version: EXECUTION_EXTENT_VERSION,
            policy: sample_policy(),
            termination: DrainTermination::Records { count: 10_000 },
        };
        extent.validate_for_plan().unwrap();
        let encoded = serde_json::to_vec(&extent).unwrap();
        let decoded: ExecutionExtent = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, extent);

        let mut invalid = sample_policy();
        invalid.package_rotation = EpochClosureTrigger::Bytes { count: 0 };
        let error = ExecutionExtent::Drain {
            version: EXECUTION_EXTENT_VERSION,
            policy: invalid,
            termination: DrainTermination::Quiescent,
        }
        .validate_for_plan()
        .unwrap_err();
        assert!(error.message.contains("package rotation"));

        let incomplete = serde_json::json!({"kind": "drain", "version": 1});
        assert!(serde_json::from_value::<ExecutionExtent>(incomplete).is_err());
    }

    #[test]
    fn watermark_closure_and_claims_are_typed_and_fail_closed() {
        let mut invalid = sample_policy();
        invalid.checkpoint_cadence = EpochClosureTrigger::WatermarkAdvance { units: 1 };
        let error = invalid.validate().unwrap_err();
        assert!(error.message.contains("enabled watermark policy"));

        let claim = WatermarkClaim {
            version: WATERMARK_CLAIM_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            event_time_field: "occurred_at".into(),
            domain: EventTimeDomain::Timestamp {
                unit: CanonicalArrowTimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            },
            value: WatermarkValue::Timestamp(1_700_000_000_000_000),
            partition_id: PartitionId::new("partition-0").unwrap(),
            source_position: cursor_position(),
            authority: WatermarkAuthority::Source,
            observation_context: WatermarkObservationContext::SourcePoll,
        };
        claim.validate().unwrap();

        let mut mismatched = claim;
        mismatched.value = WatermarkValue::Signed(7);
        let error = mismatched.validate().unwrap_err();
        assert!(error.message.contains("does not match"));
    }

    #[test]
    fn resident_extent_is_recordable_but_not_yet_plan_legal() {
        let extent = ExecutionExtent::Resident {
            version: EXECUTION_EXTENT_VERSION,
            policy: sample_policy(),
        };
        extent.validate().unwrap();
        let error = extent.validate_for_plan().unwrap_err();
        assert!(error.message.contains("resident execution is not enabled"));
    }

    #[test]
    fn canonical_frontier_excludes_nondeterministic_trigger_timing() {
        let position = cursor_position();
        let frontier = EpochFrontier {
            version: EPOCH_FRONTIER_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            epoch_ordinal: 4,
            frontier: position.clone(),
            input_low: None,
            input_high: position,
            carryover: None,
            watermark: None,
        };
        frontier.validate().unwrap();
        let identity = serde_json::to_value(&frontier).unwrap();
        assert!(identity.get("trigger").is_none());
        assert!(identity.get("observed_at_unix_milliseconds").is_none());
        assert!(identity.get("overshoot").is_none());

        let evidence = EpochClosureEvidence {
            version: EPOCH_CLOSURE_EVIDENCE_VERSION,
            frontier,
            trigger: EpochClosureTrigger::Elapsed { milliseconds: 500 },
            observation: EpochClosureObservation::Elapsed {
                observed_milliseconds: 508,
                overshoot_milliseconds: 8,
            },
            observed_at_unix_milliseconds: 1_700_000_000_000,
        };
        evidence.validate().unwrap();
        let recorded = serde_json::to_value(evidence).unwrap();
        assert_eq!(recorded["trigger"]["kind"], "elapsed");
        assert_eq!(recorded["observation"]["observed_milliseconds"], 508);
        assert_eq!(recorded["observation"]["overshoot_milliseconds"], 8);
    }

    #[test]
    fn every_versioned_nested_artifact_rejects_invalid_deserialization() {
        let mut policy = serde_json::to_value(sample_policy()).unwrap();
        policy["version"] = 2.into();
        assert!(serde_json::from_value::<StreamEpochPolicy>(policy).is_err());

        let claim = WatermarkClaim {
            version: WATERMARK_CLAIM_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            event_time_field: "occurred_at".into(),
            domain: EventTimeDomain::Timestamp {
                unit: CanonicalArrowTimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            },
            value: WatermarkValue::Timestamp(42),
            partition_id: PartitionId::new("partition-0").unwrap(),
            source_position: cursor_position(),
            authority: WatermarkAuthority::Source,
            observation_context: WatermarkObservationContext::SourcePoll,
        };
        let mut invalid_claim = serde_json::to_value(&claim).unwrap();
        invalid_claim["policy_version"] = 2.into();
        assert!(serde_json::from_value::<WatermarkClaim>(invalid_claim).is_err());

        let frontier = EpochFrontier {
            version: EPOCH_FRONTIER_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            epoch_ordinal: 1,
            frontier: cursor_position(),
            input_low: None,
            input_high: cursor_position(),
            carryover: None,
            watermark: Some(claim),
        };
        let mut invalid_frontier = serde_json::to_value(&frontier).unwrap();
        invalid_frontier["version"] = 2.into();
        assert!(serde_json::from_value::<EpochFrontier>(invalid_frontier).is_err());

        let evidence = EpochClosureEvidence {
            version: EPOCH_CLOSURE_EVIDENCE_VERSION,
            frontier,
            trigger: EpochClosureTrigger::WatermarkAdvance { units: 10 },
            observation: EpochClosureObservation::WatermarkAdvance {
                observed_units: 12,
                overshoot_units: 2,
            },
            observed_at_unix_milliseconds: 1,
        };
        let mut invalid_evidence = serde_json::to_value(evidence).unwrap();
        invalid_evidence["observation"]["overshoot_units"] = 1.into();
        assert!(serde_json::from_value::<EpochClosureEvidence>(invalid_evidence).is_err());
    }

    #[test]
    fn kernel_extent_artifacts_have_no_runtime_dependency() {
        let manifest = include_str!("../Cargo.toml");
        for forbidden in [
            "datafusion",
            "tokio",
            "cdf-runtime",
            "cdf-engine",
            "cdf-cli",
            "cdf-source-",
        ] {
            assert!(
                !manifest.contains(forbidden),
                "cdf-kernel manifest contains runtime dependency {forbidden}"
            );
        }
    }

    fn sample_policy() -> StreamEpochPolicy {
        StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 1_000 },
            package_rotation: EpochClosureTrigger::Bytes {
                count: 64 * 1024 * 1024,
            },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        }
    }

    fn cursor_position() -> SourcePosition {
        SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "offset".to_owned(),
            value: CursorValue::U64(42),
        })
    }
}
