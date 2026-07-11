use crate::prelude::*;

use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DestinationDescription {
    pub destination_id: DestinationId,
    pub schemes: &'static [&'static str],
    pub label: String,
}

impl DestinationDescription {
    pub fn new(
        destination_id: DestinationId,
        schemes: &'static [&'static str],
        label: impl Into<String>,
    ) -> Self {
        Self {
            destination_id,
            schemes,
            label: label.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationIngressMode {
    FinalizedPackageOnly,
    StagedDurableSegments,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationWriterModel {
    SingleWriter,
    ConcurrentSegments,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationRuntimeCapabilities {
    pub ingress_mode: DestinationIngressMode,
    pub writer_model: DestinationWriterModel,
    pub max_in_flight_segments: Option<u16>,
    pub max_in_flight_bytes: Option<u64>,
    pub bulk_path: Option<String>,
    pub bulk_evidence_version: Option<String>,
    pub replay_requires_explicit_target: bool,
    pub replay_target_hint: Option<String>,
    pub replay_policy_values: BTreeMap<String, Vec<String>>,
}

impl Default for DestinationRuntimeCapabilities {
    fn default() -> Self {
        Self {
            ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
            writer_model: DestinationWriterModel::SingleWriter,
            max_in_flight_segments: Some(1),
            max_in_flight_bytes: None,
            bulk_path: None,
            bulk_evidence_version: None,
            replay_requires_explicit_target: false,
            replay_target_hint: None,
            replay_policy_values: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationHealthProbe {
    pub probe_id: String,
    pub description: String,
    pub requires_credentials: bool,
    pub mutates_destination: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationHealthStatus {
    Passed,
    Failed,
    Skipped,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationHealthResult {
    pub probe_id: String,
    pub status: DestinationHealthStatus,
    pub message: String,
    pub details: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationInspection {
    pub description: DestinationDescription,
    pub sheet_artifact: DestinationSheetArtifact,
    pub sheet_artifact_hash: String,
    pub runtime: DestinationRuntimeCapabilities,
    pub health_probes: Vec<DestinationHealthProbe>,
}
