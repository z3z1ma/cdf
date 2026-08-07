use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{CdfError, Result, SchemaHash};

pub const KEYED_EFFECT_AUTHORITY_VERSION: u16 = 1;
pub const KEYED_EFFECT_ORDER_VERSION: u16 = 1;
pub const DEDUP_KEY_ENCODING_VERSION: &str = "cdf-dedup-key-v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageSegmentKind {
    Row,
    Upsert,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PackageContentAuthority {
    Rows {
        logical_schema_hash: SchemaHash,
    },
    KeyedChanges {
        logical_schema_hash: SchemaHash,
        upsert_schema_hash: SchemaHash,
        delete_schema_hash: SchemaHash,
        key: KeyAuthority,
        reduction: KeyedEffectReductionAuthority,
        deletion_capture: DeletionCaptureAuthority,
        delete_application: DeleteApplicationAuthority,
    },
}

impl PackageContentAuthority {
    pub fn rows(logical_schema_hash: SchemaHash) -> Self {
        Self::Rows {
            logical_schema_hash,
        }
    }

    pub fn logical_schema_hash(&self) -> &SchemaHash {
        match self {
            Self::Rows {
                logical_schema_hash,
            }
            | Self::KeyedChanges {
                logical_schema_hash,
                ..
            } => logical_schema_hash,
        }
    }

    pub fn validate(&self) -> Result<()> {
        SchemaHash::new(self.logical_schema_hash().as_str()).map(drop)?;
        match self {
            Self::Rows { .. } => Ok(()),
            Self::KeyedChanges {
                logical_schema_hash,
                upsert_schema_hash,
                delete_schema_hash,
                key,
                reduction,
                deletion_capture,
                delete_application,
            } => {
                SchemaHash::new(upsert_schema_hash.as_str()).map(drop)?;
                SchemaHash::new(delete_schema_hash.as_str()).map(drop)?;
                if upsert_schema_hash != logical_schema_hash {
                    return Err(CdfError::data(
                        "keyed-change upsert schema must equal the complete logical output schema",
                    ));
                }
                key.validate()?;
                reduction.validate(key)?;
                deletion_capture.validate()?;
                delete_application.validate(deletion_capture)
            }
        }
    }

    pub fn validate_segment_rows<'a>(
        &self,
        segments: impl IntoIterator<Item = (&'a PackageSegmentKind, u64)>,
    ) -> Result<()> {
        self.validate()?;
        let mut upserts = 0_u64;
        let mut deletes = 0_u64;
        for (kind, row_count) in segments {
            match (self, kind) {
                (Self::Rows { .. }, PackageSegmentKind::Row) => {}
                (Self::KeyedChanges { .. }, PackageSegmentKind::Upsert) => {
                    upserts = upserts
                        .checked_add(row_count)
                        .ok_or_else(|| CdfError::data("upsert effect count overflowed u64"))?;
                }
                (Self::KeyedChanges { .. }, PackageSegmentKind::Delete) => {
                    deletes = deletes
                        .checked_add(row_count)
                        .ok_or_else(|| CdfError::data("delete effect count overflowed u64"))?;
                }
                (Self::Rows { .. }, _) => {
                    return Err(CdfError::data(
                        "ordinary-row package contains a keyed-effect segment",
                    ));
                }
                (Self::KeyedChanges { .. }, PackageSegmentKind::Row) => {
                    return Err(CdfError::data(
                        "keyed-change package contains an ordinary-row segment",
                    ));
                }
            }
        }
        if let Self::KeyedChanges { reduction, .. } = self {
            if upserts != reduction.surviving.upserts || deletes != reduction.surviving.deletes {
                return Err(CdfError::data(
                    "keyed-change segment row counts do not match surviving effect authority",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyAuthority {
    pub version: u16,
    pub fields: Vec<String>,
    pub encoding: String,
    pub schema_hash: SchemaHash,
}

impl KeyAuthority {
    pub fn validate(&self) -> Result<()> {
        if self.version != KEYED_EFFECT_AUTHORITY_VERSION
            || self.fields.is_empty()
            || self.encoding != DEDUP_KEY_ENCODING_VERSION
        {
            return Err(CdfError::data(
                "key authority requires the current version, a nonempty field vector, and cdf-dedup-key-v1 encoding",
            ));
        }
        let mut unique = BTreeSet::new();
        if self
            .fields
            .iter()
            .any(|field| field.is_empty() || !unique.insert(field))
        {
            return Err(CdfError::data(
                "key authority fields must be unique and nonempty",
            ));
        }
        SchemaHash::new(self.schema_hash.as_str()).map(drop)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyedEffectWinnerPolicy {
    Fail,
    First,
    Last,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum KeyedEffectInputOrder {
    Unordered,
    CanonicalPackageRows {
        version: u16,
    },
    SourceProtocol {
        protocol: String,
        version: u16,
        scope_sha256: String,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyedEffectCounts {
    pub upserts: u64,
    pub deletes: u64,
}

impl KeyedEffectCounts {
    pub fn total(self) -> Result<u64> {
        self.upserts
            .checked_add(self.deletes)
            .ok_or_else(|| CdfError::data("keyed effect count overflowed u64"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyedEffectReductionAuthority {
    pub version: u16,
    pub winner: KeyedEffectWinnerPolicy,
    pub input_order: KeyedEffectInputOrder,
    pub input: KeyedEffectCounts,
    pub duplicate_key_count: u64,
    pub surviving: KeyedEffectCounts,
    pub provenance_format: String,
    pub provenance_version: u16,
}

impl KeyedEffectReductionAuthority {
    pub fn validate(&self, key: &KeyAuthority) -> Result<()> {
        key.validate()?;
        let input = self.input.total()?;
        let surviving = self.surviving.total()?;
        if self.version != KEYED_EFFECT_AUTHORITY_VERSION
            || self.provenance_format != "parquet"
            || self.provenance_version == 0
            || surviving > input
            || input.saturating_sub(surviving) != self.duplicate_key_count
        {
            return Err(CdfError::data(
                "keyed effect reduction counts or provenance authority are inconsistent",
            ));
        }
        match (&self.winner, &self.input_order) {
            (KeyedEffectWinnerPolicy::Fail, KeyedEffectInputOrder::Unordered)
            | (
                KeyedEffectWinnerPolicy::First | KeyedEffectWinnerPolicy::Last,
                KeyedEffectInputOrder::CanonicalPackageRows { version: 1 },
            ) => Ok(()),
            (
                KeyedEffectWinnerPolicy::Last,
                KeyedEffectInputOrder::SourceProtocol {
                    protocol,
                    version,
                    scope_sha256,
                },
            ) if !protocol.is_empty() && *version != 0 && valid_sha256(scope_sha256) => Ok(()),
            _ => Err(CdfError::data(
                "keyed effect winner policy lacks a compatible authoritative input order",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionCaptureSupport {
    Unsupported,
    Optional,
    Inherent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionCaptureAuthority {
    pub support: DeletionCaptureSupport,
    pub enabled: bool,
    pub semantics_sha256: String,
}

impl DeletionCaptureAuthority {
    pub fn unsupported(semantics_sha256: impl Into<String>) -> Self {
        Self {
            support: DeletionCaptureSupport::Unsupported,
            enabled: false,
            semantics_sha256: semantics_sha256.into(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if !valid_sha256(&self.semantics_sha256)
            || (self.support == DeletionCaptureSupport::Unsupported && self.enabled)
            || (self.support == DeletionCaptureSupport::Inherent && !self.enabled)
        {
            return Err(CdfError::data(
                "deletion capture support, selection, and semantics hash are inconsistent",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeleteApplicationAuthority {
    NotApplicable,
    Apply { policy: DeleteApplicationPolicy },
}

impl DeleteApplicationAuthority {
    pub fn validate(&self, capture: &DeletionCaptureAuthority) -> Result<()> {
        capture.validate()?;
        if let Self::Apply {
            policy: DeleteApplicationPolicy::Soft { marker_field },
        } = self
            && marker_field.trim().is_empty()
        {
            return Err(CdfError::contract(
                "soft delete application requires a nonempty Boolean marker field",
            ));
        }
        match (self, capture.enabled) {
            (Self::NotApplicable, false) | (Self::Apply { .. }, true) => Ok(()),
            (Self::NotApplicable, true) => Err(CdfError::contract(
                "enabled deletion capture requires an explicit delete application policy",
            )),
            (Self::Apply { .. }, false) => Err(CdfError::contract(
                "delete application policy is invalid when deletion capture is disabled",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeleteApplicationPolicy {
    Ignore,
    Hard,
    Soft { marker_field: String },
}

fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyed_content(upserts: u64, deletes: u64) -> PackageContentAuthority {
        let schema_hash = SchemaHash::new("sha256:logical-schema").unwrap();
        let key_hash = SchemaHash::new("sha256:key-schema").unwrap();
        PackageContentAuthority::KeyedChanges {
            logical_schema_hash: schema_hash.clone(),
            upsert_schema_hash: schema_hash,
            delete_schema_hash: key_hash.clone(),
            key: KeyAuthority {
                version: KEYED_EFFECT_AUTHORITY_VERSION,
                fields: vec!["id".to_owned()],
                encoding: DEDUP_KEY_ENCODING_VERSION.to_owned(),
                schema_hash: key_hash,
            },
            reduction: KeyedEffectReductionAuthority {
                version: KEYED_EFFECT_AUTHORITY_VERSION,
                winner: KeyedEffectWinnerPolicy::Last,
                input_order: KeyedEffectInputOrder::CanonicalPackageRows { version: 1 },
                input: KeyedEffectCounts { upserts, deletes },
                duplicate_key_count: 0,
                surviving: KeyedEffectCounts { upserts, deletes },
                provenance_format: "parquet".to_owned(),
                provenance_version: 1,
            },
            deletion_capture: DeletionCaptureAuthority {
                support: DeletionCaptureSupport::Optional,
                enabled: true,
                semantics_sha256:
                    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_owned(),
            },
            delete_application: DeleteApplicationAuthority::Apply {
                policy: DeleteApplicationPolicy::Hard,
            },
        }
    }

    #[test]
    fn keyed_content_requires_exact_typed_segment_row_counts() {
        let content = keyed_content(3, 2);
        content
            .validate_segment_rows([
                (&PackageSegmentKind::Upsert, 3),
                (&PackageSegmentKind::Delete, 2),
            ])
            .unwrap();

        let error = content
            .validate_segment_rows([
                (&PackageSegmentKind::Upsert, 2),
                (&PackageSegmentKind::Delete, 2),
            ])
            .unwrap_err();
        assert!(error.message.contains("row counts"));
    }

    #[test]
    fn rows_and_keyed_changes_reject_cross_family_segments() {
        let rows = PackageContentAuthority::rows(SchemaHash::new("sha256:rows").unwrap());
        let error = rows
            .validate_segment_rows([(&PackageSegmentKind::Delete, 1)])
            .unwrap_err();
        assert!(error.message.contains("ordinary-row"));

        let error = keyed_content(1, 0)
            .validate_segment_rows([(&PackageSegmentKind::Row, 1)])
            .unwrap_err();
        assert!(error.message.contains("keyed-change"));
    }
}
