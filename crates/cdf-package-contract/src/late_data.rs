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
    AdmittedOutput {
        package_row_ordinal: u64,
    },
    ArtifactRow {
        artifact_ordinal: u64,
        row_ordinal: u64,
    },
}

/// Identity-bearing evidence for one row observed behind the effective global watermark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedLateDataRecord", deny_unknown_fields)]
pub struct LateDataRecord {
    pub source_row_ordinal: u64,
    pub partition_id: PartitionId,
    pub source_position: Option<SourcePosition>,
    pub event_time: WatermarkValue,
    pub effective_watermark: WatermarkClaim,
    pub action: LateDataAction,
    pub payload: LateDataPayloadLocation,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedLateDataRecord {
    source_row_ordinal: u64,
    partition_id: PartitionId,
    source_position: Option<SourcePosition>,
    event_time: WatermarkValue,
    effective_watermark: WatermarkClaim,
    action: LateDataAction,
    payload: LateDataPayloadLocation,
}

impl TryFrom<UncheckedLateDataRecord> for LateDataRecord {
    type Error = cdf_kernel::CdfError;

    fn try_from(value: UncheckedLateDataRecord) -> Result<Self, Self::Error> {
        let record = Self {
            source_row_ordinal: value.source_row_ordinal,
            partition_id: value.partition_id,
            source_position: value.source_position,
            event_time: value.event_time,
            effective_watermark: value.effective_watermark,
            action: value.action,
            payload: value.payload,
        };
        record.validate()?;
        Ok(record)
    }
}

impl LateDataRecord {
    pub fn validate(&self) -> cdf_kernel::Result<()> {
        self.effective_watermark.validate()?;
        if let Some(position) = &self.source_position {
            position.validate()?;
        }
        if !self
            .effective_watermark
            .domain
            .matches_value(&self.event_time)
        {
            return Err(cdf_kernel::CdfError::data(
                "late-data event time does not match its watermark domain",
            ));
        }
        if !self
            .effective_watermark
            .classifies_as_late(&self.event_time)?
        {
            return Err(cdf_kernel::CdfError::data(
                "late-data event time is not behind its effective watermark",
            ));
        }
        match (&self.action, &self.payload) {
            (
                LateDataAction::AdmitWithAnnotation,
                LateDataPayloadLocation::AdmittedOutput { .. },
            )
            | (
                LateDataAction::Quarantine | LateDataAction::RecaptureNextEpoch,
                LateDataPayloadLocation::ArtifactRow { .. },
            ) => Ok(()),
            _ => Err(cdf_kernel::CdfError::data(
                "late-data action does not match its payload location",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedLateDataEvidence", deny_unknown_fields)]
pub struct LateDataEvidence {
    pub version: u16,
    pub records: Vec<LateDataRecord>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedLateDataEvidence {
    version: u16,
    records: Vec<LateDataRecord>,
}

impl TryFrom<UncheckedLateDataEvidence> for LateDataEvidence {
    type Error = cdf_kernel::CdfError;

    fn try_from(value: UncheckedLateDataEvidence) -> Result<Self, Self::Error> {
        let evidence = Self {
            version: value.version,
            records: value.records,
        };
        evidence.validate()?;
        Ok(evidence)
    }
}

impl LateDataEvidence {
    pub fn new(records: Vec<LateDataRecord>) -> cdf_kernel::Result<Self> {
        let evidence = Self {
            version: LATE_DATA_EVIDENCE_VERSION,
            records,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    pub fn validate(&self) -> cdf_kernel::Result<()> {
        if self.version != LATE_DATA_EVIDENCE_VERSION {
            return Err(cdf_kernel::CdfError::contract(format!(
                "unsupported late-data evidence version {}",
                self.version
            )));
        }
        for record in &self.records {
            record.validate()?;
        }
        Ok(())
    }

    pub fn validate_payloads(
        &self,
        catalog: Option<&LateDataPayloadCatalog>,
        output_row_count: u64,
    ) -> cdf_kernel::Result<()> {
        self.validate()?;
        if let Some(catalog) = catalog {
            catalog.validate()?;
        }
        for record in &self.records {
            match record.payload {
                LateDataPayloadLocation::AdmittedOutput {
                    package_row_ordinal,
                } => {
                    if package_row_ordinal >= output_row_count {
                        return Err(cdf_kernel::CdfError::data(
                            "late-data admitted-output ordinal exceeds package row authority",
                        ));
                    }
                }
                LateDataPayloadLocation::ArtifactRow {
                    artifact_ordinal,
                    row_ordinal,
                } => {
                    let artifact = catalog
                        .and_then(|catalog| {
                            usize::try_from(artifact_ordinal)
                                .ok()
                                .and_then(|ordinal| catalog.artifacts.get(ordinal))
                        })
                        .ok_or_else(|| {
                            cdf_kernel::CdfError::data(
                                "late-data evidence references a missing payload artifact",
                            )
                        })?;
                    if artifact.artifact_ordinal != artifact_ordinal
                        || artifact.action != record.action
                        || row_ordinal >= artifact.row_count
                    {
                        return Err(cdf_kernel::CdfError::data(
                            "late-data evidence payload reference exceeds its artifact authority",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedLateDataPayloadArtifact", deny_unknown_fields)]
pub struct LateDataPayloadArtifact {
    pub artifact_ordinal: u64,
    pub action: LateDataAction,
    pub path: String,
    pub byte_count: u64,
    pub sha256: String,
    pub row_count: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedLateDataPayloadArtifact {
    artifact_ordinal: u64,
    action: LateDataAction,
    path: String,
    byte_count: u64,
    sha256: String,
    row_count: u64,
}

impl TryFrom<UncheckedLateDataPayloadArtifact> for LateDataPayloadArtifact {
    type Error = cdf_kernel::CdfError;

    fn try_from(value: UncheckedLateDataPayloadArtifact) -> Result<Self, Self::Error> {
        let artifact = Self {
            artifact_ordinal: value.artifact_ordinal,
            action: value.action,
            path: value.path,
            byte_count: value.byte_count,
            sha256: value.sha256,
            row_count: value.row_count,
        };
        artifact.validate()?;
        Ok(artifact)
    }
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
    use cdf_kernel::{
        CursorPosition, CursorValue, EventTimeDomain, SOURCE_POSITION_VERSION,
        STREAM_EPOCH_POLICY_VERSION, WATERMARK_CLAIM_VERSION, WatermarkAuthority,
        WatermarkObservationContext,
    };

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

    fn record(action: LateDataAction, payload: LateDataPayloadLocation) -> LateDataRecord {
        LateDataRecord {
            source_row_ordinal: 4,
            partition_id: PartitionId::new("partition-0").unwrap(),
            source_position: None,
            event_time: WatermarkValue::Signed(10),
            effective_watermark: WatermarkClaim {
                version: WATERMARK_CLAIM_VERSION,
                policy_version: STREAM_EPOCH_POLICY_VERSION,
                event_time_field: "occurred_at".into(),
                domain: EventTimeDomain::SignedInteger,
                value: WatermarkValue::Signed(20),
                partition_id: PartitionId::new("partition-0").unwrap(),
                source_position: SourcePosition::Cursor(CursorPosition {
                    version: SOURCE_POSITION_VERSION,
                    field: "offset".to_owned(),
                    value: CursorValue::I64(4),
                }),
                authority: WatermarkAuthority::Source,
                observation_context: WatermarkObservationContext::SourcePoll,
            },
            action,
            payload,
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

    #[test]
    fn evidence_deserialization_and_payload_join_fail_closed() {
        let admitted = record(
            LateDataAction::AdmitWithAnnotation,
            LateDataPayloadLocation::AdmittedOutput {
                package_row_ordinal: 1,
            },
        );
        let evidence = LateDataEvidence::new(vec![admitted]).unwrap();
        evidence.validate_payloads(None, 2).unwrap();
        assert!(evidence.validate_payloads(None, 1).is_err());

        let mut wrong_version = serde_json::to_value(&evidence).unwrap();
        wrong_version["version"] = serde_json::json!(2);
        assert!(serde_json::from_value::<LateDataEvidence>(wrong_version).is_err());

        let mismatched = record(
            LateDataAction::Quarantine,
            LateDataPayloadLocation::AdmittedOutput {
                package_row_ordinal: 0,
            },
        );
        assert!(mismatched.validate().is_err());

        let quarantined = LateDataEvidence::new(vec![record(
            LateDataAction::Quarantine,
            LateDataPayloadLocation::ArtifactRow {
                artifact_ordinal: 0,
                row_ordinal: 1,
            },
        )])
        .unwrap();
        let catalog = LateDataPayloadCatalog::new(vec![artifact(
            LateDataAction::Quarantine,
            "quarantine/late-data-000.arrow",
        )])
        .unwrap();
        assert!(quarantined.validate_payloads(Some(&catalog), 0).is_err());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedLateDataPayloadCatalog", deny_unknown_fields)]
pub struct LateDataPayloadCatalog {
    pub version: u16,
    pub artifacts: Vec<LateDataPayloadArtifact>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedLateDataPayloadCatalog {
    version: u16,
    artifacts: Vec<LateDataPayloadArtifact>,
}

impl TryFrom<UncheckedLateDataPayloadCatalog> for LateDataPayloadCatalog {
    type Error = cdf_kernel::CdfError;

    fn try_from(value: UncheckedLateDataPayloadCatalog) -> Result<Self, Self::Error> {
        Self::new(value.artifacts).and_then(|catalog| {
            if value.version != catalog.version {
                return Err(cdf_kernel::CdfError::contract(format!(
                    "unsupported late-data payload catalog version {}",
                    value.version
                )));
            }
            Ok(catalog)
        })
    }
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
