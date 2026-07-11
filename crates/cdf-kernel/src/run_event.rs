use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};

use crate::{
    error::{CdfError, Result},
    ids::{
        CheckpointId, DestinationId, PackageHash, PartitionId, PlanId, PromotionId, ReceiptId,
        ResourceId, RunId, SchemaHash, TargetName,
    },
    scope::ScopeKey,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEvent {
    pub run_id: RunId,
    pub sequence: u64,
    pub timestamp_ms: i64,
    pub kind: RunEventKind,
    pub resource_id: Option<ResourceId>,
    pub scope: Option<ScopeKey>,
    pub partition_id: Option<PartitionId>,
    pub package_id: Option<String>,
    pub package_hash: Option<PackageHash>,
    pub package_path: Option<String>,
    pub checkpoint_id: Option<CheckpointId>,
    pub receipt_id: Option<ReceiptId>,
    pub destination_id: Option<DestinationId>,
    pub plan_id: Option<PlanId>,
    pub details: RunEventDetails,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEventAppend {
    pub kind: RunEventKind,
    pub resource_id: Option<ResourceId>,
    pub scope: Option<ScopeKey>,
    pub partition_id: Option<PartitionId>,
    pub package_id: Option<String>,
    pub package_hash: Option<PackageHash>,
    pub package_path: Option<String>,
    pub checkpoint_id: Option<CheckpointId>,
    pub receipt_id: Option<ReceiptId>,
    pub destination_id: Option<DestinationId>,
    pub plan_id: Option<PlanId>,
    pub details: RunEventDetails,
}

pub const PROMOTION_PUBLICATION_EVENT_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionPublicationEvent {
    pub version: u16,
    pub promotion_id: PromotionId,
    pub resource_id: ResourceId,
    pub old_schema_hash: SchemaHash,
    pub new_schema_hash: SchemaHash,
    pub installed_lock_sha256: String,
    pub targets: Vec<PromotionPublicationTarget>,
    pub published_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionPublicationTarget {
    pub destination_id: DestinationId,
    pub target: TargetName,
    pub correction_package_hash: PackageHash,
    pub receipt_id: ReceiptId,
    pub checkpoint_id: CheckpointId,
}

impl PromotionPublicationEvent {
    pub fn validate(&self) -> Result<()> {
        if self.version != PROMOTION_PUBLICATION_EVENT_VERSION
            || self.installed_lock_sha256.trim().is_empty()
            || self.targets.is_empty()
            || self.published_at_ms <= 0
        {
            return Err(CdfError::contract(
                "promotion publication requires the current version, installed lock hash, targets, and publication time",
            ));
        }
        if self.targets.windows(2).any(|pair| {
            (&pair[0].destination_id, &pair[0].target) >= (&pair[1].destination_id, &pair[1].target)
        }) {
            return Err(CdfError::contract(
                "promotion publication targets must be unique and sorted",
            ));
        }
        Ok(())
    }

    pub fn same_authority(&self, other: &Self) -> bool {
        self.version == other.version
            && self.promotion_id == other.promotion_id
            && self.resource_id == other.resource_id
            && self.old_schema_hash == other.old_schema_hash
            && self.new_schema_hash == other.new_schema_hash
            && self.installed_lock_sha256 == other.installed_lock_sha256
            && self.targets == other.targets
    }
}

impl RunEventAppend {
    pub fn new(kind: RunEventKind) -> Self {
        Self {
            kind,
            resource_id: None,
            scope: None,
            partition_id: None,
            package_id: None,
            package_hash: None,
            package_path: None,
            checkpoint_id: None,
            receipt_id: None,
            destination_id: None,
            plan_id: None,
            details: RunEventDetails::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunEventKind {
    RunStarted,
    PlanRecorded,
    PackageStarted,
    PackageSegmentRecorded,
    PackageFinalized,
    DestinationCommitStarted,
    DestinationSegmentAcknowledged,
    DestinationReceiptRecorded,
    CheckpointProposed,
    CheckpointCommitted,
    PackageStatusUpdated,
    RunSucceeded,
    RunFailed,
    RunResumed,
    ReplayRecorded,
    ValidationDepthTransitionRecorded,
    PhaseMeasured,
}

impl RunEventKind {
    pub const ALL: [Self; 17] = [
        Self::RunStarted,
        Self::PlanRecorded,
        Self::PackageStarted,
        Self::PackageSegmentRecorded,
        Self::PackageFinalized,
        Self::DestinationCommitStarted,
        Self::DestinationSegmentAcknowledged,
        Self::DestinationReceiptRecorded,
        Self::CheckpointProposed,
        Self::CheckpointCommitted,
        Self::PackageStatusUpdated,
        Self::RunSucceeded,
        Self::RunFailed,
        Self::RunResumed,
        Self::ReplayRecorded,
        Self::ValidationDepthTransitionRecorded,
        Self::PhaseMeasured,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RunStarted => "run_started",
            Self::PlanRecorded => "plan_recorded",
            Self::PackageStarted => "package_started",
            Self::PackageSegmentRecorded => "package_segment_recorded",
            Self::PackageFinalized => "package_finalized",
            Self::DestinationCommitStarted => "destination_commit_started",
            Self::DestinationSegmentAcknowledged => "destination_segment_acknowledged",
            Self::DestinationReceiptRecorded => "destination_receipt_recorded",
            Self::CheckpointProposed => "checkpoint_proposed",
            Self::CheckpointCommitted => "checkpoint_committed",
            Self::PackageStatusUpdated => "package_status_updated",
            Self::RunSucceeded => "run_succeeded",
            Self::RunFailed => "run_failed",
            Self::RunResumed => "run_resumed",
            Self::ReplayRecorded => "replay_recorded",
            Self::ValidationDepthTransitionRecorded => "validation_depth_transition_recorded",
            Self::PhaseMeasured => "phase_measured",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "run_started" => Ok(Self::RunStarted),
            "plan_recorded" => Ok(Self::PlanRecorded),
            "package_started" => Ok(Self::PackageStarted),
            "package_segment_recorded" => Ok(Self::PackageSegmentRecorded),
            "package_finalized" => Ok(Self::PackageFinalized),
            "destination_commit_started" => Ok(Self::DestinationCommitStarted),
            "destination_segment_acknowledged" => Ok(Self::DestinationSegmentAcknowledged),
            "destination_receipt_recorded" => Ok(Self::DestinationReceiptRecorded),
            "checkpoint_proposed" => Ok(Self::CheckpointProposed),
            "checkpoint_committed" => Ok(Self::CheckpointCommitted),
            "package_status_updated" => Ok(Self::PackageStatusUpdated),
            "run_succeeded" => Ok(Self::RunSucceeded),
            "run_failed" => Ok(Self::RunFailed),
            "run_resumed" => Ok(Self::RunResumed),
            "replay_recorded" => Ok(Self::ReplayRecorded),
            "validation_depth_transition_recorded" => Ok(Self::ValidationDepthTransitionRecorded),
            "phase_measured" => Ok(Self::PhaseMeasured),
            other => Err(CdfError::data(format!("unknown run event kind {other:?}"))),
        }
    }
}

impl fmt::Display for RunEventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEventDetails {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, RunEventValue>,
}

impl RunEventDetails {
    pub fn new(attributes: impl IntoIterator<Item = (impl Into<String>, RunEventValue)>) -> Self {
        Self {
            attributes: attributes
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        for (key, value) in &self.attributes {
            validate_event_value(key, value)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum RunEventValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    String(String),
    SecretRef(SecretReference),
    List(Vec<RunEventValue>),
    Object(BTreeMap<String, RunEventValue>),
    PhaseMetric(RunPhaseMetric),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    PackageExecution,
    Decode,
    ValidationNormalization,
    SegmentEncode,
    PersistHash,
    PackageFinalize,
    DestinationWriteReceipt,
    CheckpointGate,
}

impl RunPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PackageExecution => "package_execution",
            Self::Decode => "decode",
            Self::ValidationNormalization => "validation_normalization",
            Self::SegmentEncode => "segment_encode",
            Self::PersistHash => "persist_hash",
            Self::PackageFinalize => "package_finalize",
            Self::DestinationWriteReceipt => "destination_write_receipt",
            Self::CheckpointGate => "checkpoint_gate",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhaseStatus {
    Completed,
    Failed,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunPhaseMetric {
    pub phase: RunPhase,
    pub status: RunPhaseStatus,
    pub duration_ns: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub operations: u64,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SecretReference(String);

impl SecretReference {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let rest = value
            .strip_prefix("secret://")
            .ok_or_else(|| CdfError::contract("secret reference must use the secret:// scheme"))?;
        let (provider, key) = rest
            .split_once('/')
            .ok_or_else(|| CdfError::contract("secret reference must use secret://provider/key"))?;
        if provider.trim().is_empty() || key.trim().is_empty() {
            return Err(CdfError::contract(
                "secret reference must use secret://provider/key",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for SecretReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for SecretReference {
    type Error = CdfError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<SecretReference> for String {
    fn from(value: SecretReference) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunEventSinkResult {
    /// The sink accepted the event for live processing.
    Accepted,
    /// The sink is full or otherwise declined the event without failing the run.
    Dropped,
}

/// Non-blocking live subscriber for durable run events.
///
/// Implementations must return promptly. A full, slow, or backpressured subscriber
/// reports `Dropped`; it must not sleep, retry, or turn display/telemetry loss into
/// run failure.
pub trait RunEventSink: Send + Sync {
    fn try_emit(&self, event: &RunEvent) -> RunEventSinkResult;
}

fn validate_event_value(key: &str, value: &RunEventValue) -> Result<()> {
    if key.trim().is_empty() {
        return Err(CdfError::contract("run event detail keys cannot be empty"));
    }
    if is_sensitive_key(key) && !value_contains_only_secret_refs(value) {
        return Err(CdfError::contract(format!(
            "run event detail {key:?} must use secret references"
        )));
    }
    match value {
        RunEventValue::String(value) => {
            if value.contains("secret://") {
                return Err(CdfError::contract(
                    "run event detail strings must use SecretRef for secret references",
                ));
            }
            Ok(())
        }
        RunEventValue::List(values) => {
            for value in values {
                validate_event_value(key, value)?;
            }
            Ok(())
        }
        RunEventValue::Object(values) => {
            for (nested_key, value) in values {
                validate_event_value(nested_key, value)?;
            }
            Ok(())
        }
        RunEventValue::Bool(_)
        | RunEventValue::I64(_)
        | RunEventValue::U64(_)
        | RunEventValue::SecretRef(_)
        | RunEventValue::PhaseMetric(_) => Ok(()),
    }
}

fn value_contains_only_secret_refs(value: &RunEventValue) -> bool {
    match value {
        RunEventValue::SecretRef(_) => true,
        RunEventValue::List(values) => values.iter().all(value_contains_only_secret_refs),
        RunEventValue::Object(values) => values.values().all(value_contains_only_secret_refs),
        RunEventValue::Bool(_)
        | RunEventValue::I64(_)
        | RunEventValue::U64(_)
        | RunEventValue::String(_)
        | RunEventValue::PhaseMetric(_) => false,
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("secret")
        || key.contains("token")
        || key.contains("password")
        || key.contains("credential")
        || key.contains("authorization")
        || key.contains("api_key")
        || key.contains("apikey")
        || key.contains("connection_string")
        || key.contains("dsn")
}
