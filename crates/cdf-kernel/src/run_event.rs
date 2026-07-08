use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};

use crate::{
    error::{CdfError, Result},
    ids::{
        CheckpointId, DestinationId, PackageHash, PartitionId, PlanId, ReceiptId, ResourceId, RunId,
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
    PackageFinalized,
    DestinationCommitStarted,
    DestinationReceiptRecorded,
    CheckpointProposed,
    CheckpointCommitted,
    PackageStatusUpdated,
    RunSucceeded,
    RunFailed,
    RunResumed,
    ReplayRecorded,
    ValidationDepthTransitionRecorded,
}

impl RunEventKind {
    pub const ALL: [Self; 14] = [
        Self::RunStarted,
        Self::PlanRecorded,
        Self::PackageStarted,
        Self::PackageFinalized,
        Self::DestinationCommitStarted,
        Self::DestinationReceiptRecorded,
        Self::CheckpointProposed,
        Self::CheckpointCommitted,
        Self::PackageStatusUpdated,
        Self::RunSucceeded,
        Self::RunFailed,
        Self::RunResumed,
        Self::ReplayRecorded,
        Self::ValidationDepthTransitionRecorded,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RunStarted => "run_started",
            Self::PlanRecorded => "plan_recorded",
            Self::PackageStarted => "package_started",
            Self::PackageFinalized => "package_finalized",
            Self::DestinationCommitStarted => "destination_commit_started",
            Self::DestinationReceiptRecorded => "destination_receipt_recorded",
            Self::CheckpointProposed => "checkpoint_proposed",
            Self::CheckpointCommitted => "checkpoint_committed",
            Self::PackageStatusUpdated => "package_status_updated",
            Self::RunSucceeded => "run_succeeded",
            Self::RunFailed => "run_failed",
            Self::RunResumed => "run_resumed",
            Self::ReplayRecorded => "replay_recorded",
            Self::ValidationDepthTransitionRecorded => "validation_depth_transition_recorded",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "run_started" => Ok(Self::RunStarted),
            "plan_recorded" => Ok(Self::PlanRecorded),
            "package_started" => Ok(Self::PackageStarted),
            "package_finalized" => Ok(Self::PackageFinalized),
            "destination_commit_started" => Ok(Self::DestinationCommitStarted),
            "destination_receipt_recorded" => Ok(Self::DestinationReceiptRecorded),
            "checkpoint_proposed" => Ok(Self::CheckpointProposed),
            "checkpoint_committed" => Ok(Self::CheckpointCommitted),
            "package_status_updated" => Ok(Self::PackageStatusUpdated),
            "run_succeeded" => Ok(Self::RunSucceeded),
            "run_failed" => Ok(Self::RunFailed),
            "run_resumed" => Ok(Self::RunResumed),
            "replay_recorded" => Ok(Self::ReplayRecorded),
            "validation_depth_transition_recorded" => Ok(Self::ValidationDepthTransitionRecorded),
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
        | RunEventValue::SecretRef(_) => Ok(()),
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
        | RunEventValue::String(_) => false,
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
