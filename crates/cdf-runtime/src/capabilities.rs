use crate::BlockingLaneSpec;
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
    pub blocking_lanes: Vec<BlockingLaneSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_ingress_lane: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_binding_lane: Option<String>,
    pub ingress_mode: DestinationIngressMode,
    pub staged_ingress: Option<StagedIngressCapabilities>,
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
            blocking_lanes: Vec::new(),
            staged_ingress_lane: None,
            final_binding_lane: None,
            ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
            staged_ingress: None,
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

impl DestinationRuntimeCapabilities {
    pub fn validate(&self) -> Result<()> {
        let mut lane_ids = BTreeMap::new();
        for lane in &self.blocking_lanes {
            lane.validate()?;
            if lane_ids.insert(lane.lane_id.as_str(), ()).is_some() {
                return Err(CdfError::contract(
                    "destination blocking lane ids must be unique",
                ));
            }
        }
        for (operation, lane_id) in [
            ("staged ingress", self.staged_ingress_lane.as_deref()),
            ("final binding", self.final_binding_lane.as_deref()),
        ] {
            if let Some(lane_id) = lane_id
                && !lane_ids.contains_key(lane_id)
            {
                return Err(CdfError::contract(format!(
                    "destination {operation} references undeclared blocking lane `{lane_id}`"
                )));
            }
        }
        match (&self.ingress_mode, &self.staged_ingress) {
            (DestinationIngressMode::FinalizedPackageOnly, None) => {}
            (DestinationIngressMode::StagedDurableSegments, Some(staging)) => {
                if !staging.abort_idempotent || !staging.lifecycle_cleanup {
                    return Err(CdfError::contract(
                        "staged ingress requires idempotent abort and lifecycle cleanup",
                    ));
                }
                if self.max_in_flight_segments == Some(0)
                    || self.max_in_flight_bytes == Some(0)
                    || self.max_in_flight_segments.is_none()
                    || self.max_in_flight_bytes.is_none()
                {
                    return Err(CdfError::contract(
                        "staged ingress requires nonzero segment and byte bounds",
                    ));
                }
            }
            _ => {
                return Err(CdfError::contract(
                    "destination ingress mode and staged ingress declaration disagree",
                ));
            }
        }
        Ok(())
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
