use std::path::{Component, Path};

use cdf_kernel::{LateDataAction, PartitionId, SourcePosition, WatermarkClaim, WatermarkValue};
use serde::{Deserialize, Serialize};

pub const LATE_DATA_EVIDENCE_VERSION: u16 = 1;
pub const LATE_DATA_EVIDENCE_FILE: &str = "stats/late-data.json";
pub const LATE_DATA_PAYLOAD_CATALOG_VERSION: u16 = 1;
pub const LATE_DATA_PAYLOAD_CATALOG_FILE: &str = "stats/late-data-payloads.json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LateDataPayloadLocation {
    AdmittedOutput,
    ArtifactRow {
        artifact_ordinal: u64,
        row_ordinal: u64,
    },
}

/// Identity-bearing evidence for one row observed behind the effective global watermark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LateDataRecord {
    pub source_row_ordinal: u64,
    pub partition_id: PartitionId,
    pub source_position: Option<SourcePosition>,
    pub event_time: WatermarkValue,
    pub effective_watermark: WatermarkClaim,
    pub action: LateDataAction,
    pub payload: LateDataPayloadLocation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LateDataEvidence {
    pub version: u16,
    pub records: Vec<LateDataRecord>,
}

impl LateDataEvidence {
    pub fn new(records: Vec<LateDataRecord>) -> Self {
        Self {
            version: LATE_DATA_EVIDENCE_VERSION,
            records,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LateDataPayloadArtifact {
    pub artifact_ordinal: u64,
    pub action: LateDataAction,
    pub path: String,
    pub byte_count: u64,
    pub sha256: String,
    pub row_count: u64,
}

impl LateDataPayloadArtifact {
    pub fn validate(&self) -> cdf_kernel::Result<()> {
        let expected_prefix = match self.action {
            LateDataAction::Quarantine => "quarantine/",
            LateDataAction::RecaptureNextEpoch => "carryover/",
            LateDataAction::AdmitWithAnnotation => {
                return Err(cdf_kernel::CdfError::contract(
                    "admitted late data is already retained in package output and cannot declare a payload artifact",
                ));
            }
        };
        let path = Path::new(&self.path);
        if !self.path.starts_with(expected_prefix)
            || path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(cdf_kernel::CdfError::data(
                "late-data payload artifact path does not match its action",
            ));
        }
        if self.byte_count == 0 || self.row_count == 0 {
            return Err(cdf_kernel::CdfError::data(
                "late-data payload artifact requires nonzero bytes and rows",
            ));
        }
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(cdf_kernel::CdfError::data(
                "late-data payload artifact requires a hexadecimal SHA-256",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(action: LateDataAction, path: &str) -> LateDataPayloadArtifact {
        LateDataPayloadArtifact {
            artifact_ordinal: 0,
            action,
            path: path.to_owned(),
            byte_count: 128,
            sha256: "a".repeat(64),
            row_count: 1,
        }
    }

    #[test]
    fn payload_catalog_requires_canonical_action_paths_and_ordinals() {
        assert!(
            LateDataPayloadCatalog::new(vec![artifact(
                LateDataAction::Quarantine,
                "quarantine/late-data-000.arrow",
            )])
            .is_ok()
        );

        let mut wrong_path = artifact(
            LateDataAction::RecaptureNextEpoch,
            "quarantine/late-data-000.arrow",
        );
        assert!(wrong_path.validate().is_err());
        wrong_path.path = "carryover/../quarantine/late-data-000.arrow".to_owned();
        assert!(wrong_path.validate().is_err());

        let mut noncanonical =
            artifact(LateDataAction::Quarantine, "quarantine/late-data-000.arrow");
        noncanonical.artifact_ordinal = 1;
        assert!(LateDataPayloadCatalog::new(vec![noncanonical]).is_err());
    }

    #[test]
    fn admitted_rows_cannot_claim_a_duplicate_payload_artifact() {
        let admitted = artifact(
            LateDataAction::AdmitWithAnnotation,
            "quarantine/late-data-000.arrow",
        );
        assert!(admitted.validate().is_err());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LateDataPayloadCatalog {
    pub version: u16,
    pub artifacts: Vec<LateDataPayloadArtifact>,
}

impl LateDataPayloadCatalog {
    pub fn new(artifacts: Vec<LateDataPayloadArtifact>) -> cdf_kernel::Result<Self> {
        let catalog = Self {
            version: LATE_DATA_PAYLOAD_CATALOG_VERSION,
            artifacts,
        };
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn validate(&self) -> cdf_kernel::Result<()> {
        if self.version != LATE_DATA_PAYLOAD_CATALOG_VERSION {
            return Err(cdf_kernel::CdfError::contract(format!(
                "unsupported late-data payload catalog version {}",
                self.version
            )));
        }
        for (expected, artifact) in self.artifacts.iter().enumerate() {
            artifact.validate()?;
            if artifact.artifact_ordinal
                != u64::try_from(expected).map_err(|_| {
                    cdf_kernel::CdfError::data("late-data payload artifact count exceeds u64")
                })?
            {
                return Err(cdf_kernel::CdfError::data(
                    "late-data payload artifact ordinals must be contiguous canonical order",
                ));
            }
        }
        Ok(())
    }
}
