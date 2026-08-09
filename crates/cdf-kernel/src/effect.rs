use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::{
    CanonicalArrowSchema, CdfError, OutputBindingId, Result, RouteTargetFamily, SchemaHash,
    SegmentId,
};

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
        reduction: Box<KeyedEffectReductionAuthority>,
        deletion_capture: DeletionCaptureAuthority,
        delete_application: DeleteApplicationAuthority,
    },
    Routed {
        family: RouteTargetFamily,
        outputs: Vec<RoutedOutputContentAuthority>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutedOutputContentAuthority {
    pub output_binding: OutputBindingId,
    pub schema: CanonicalArrowSchema,
    pub content: Box<PackageContentAuthority>,
    pub segment_ids: Vec<SegmentId>,
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
            Self::Routed { family, .. } => &family.schema_family_hash,
        }
    }

    pub fn zero_commit_counts(&self) -> Result<crate::CommitCounts> {
        match self {
            Self::Rows { .. } => Ok(crate::CommitCounts::default()),
            Self::KeyedChanges { reduction, .. } => Ok(crate::CommitCounts::keyed_changes(
                reduction.surviving,
                Some(0),
                Some(0),
                None,
                None,
                None,
                None,
            )),
            Self::Routed { family, outputs } => {
                let targets = family
                    .bindings
                    .iter()
                    .zip(outputs)
                    .map(|(binding, output)| {
                        Ok(crate::RoutedTargetCommitCounts {
                            output_binding: binding.output_binding.clone(),
                            target: binding.physical_target.clone(),
                            schema_hash: binding.schema_hash.clone(),
                            counts: Box::new(output.content.zero_commit_counts()?),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(crate::CommitCounts::Routed { targets })
            }
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
            Self::Routed { family, outputs } => {
                family.validate()?;
                if outputs.len() != family.bindings.len() {
                    return Err(CdfError::data(
                        "routed package content must bind every admitted family output exactly once",
                    ));
                }
                for (output, binding) in outputs.iter().zip(&family.bindings) {
                    let schema = output.schema.to_arrow()?;
                    if output.output_binding != binding.output_binding
                        || crate::canonical_arrow_schema_hash(schema.as_ref())?
                            != binding.schema_hash
                        || output.content.logical_schema_hash() != &binding.schema_hash
                        || matches!(output.content.as_ref(), Self::Routed { .. })
                    {
                        return Err(CdfError::data(
                            "routed package output content does not match its canonical output/schema binding",
                        ));
                    }
                    output.content.validate()?;
                }
                let kinds = outputs
                    .iter()
                    .map(|output| matches!(output.content.as_ref(), Self::Rows { .. }))
                    .collect::<BTreeSet<_>>();
                if kinds.len() != 1 {
                    return Err(CdfError::data(
                        "one routed package cannot mix ordinary-row and keyed-change outputs",
                    ));
                }
                Ok(())
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
                (Self::Routed { outputs, .. }, PackageSegmentKind::Row)
                    if outputs
                        .iter()
                        .all(|output| matches!(output.content.as_ref(), Self::Rows { .. })) => {}
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
                (Self::Routed { outputs, .. }, PackageSegmentKind::Upsert)
                    if outputs.iter().all(|output| {
                        matches!(output.content.as_ref(), Self::KeyedChanges { .. })
                    }) =>
                {
                    upserts = upserts
                        .checked_add(row_count)
                        .ok_or_else(|| CdfError::data("upsert effect count overflowed u64"))?;
                }
                (Self::Routed { outputs, .. }, PackageSegmentKind::Delete)
                    if outputs.iter().all(|output| {
                        matches!(output.content.as_ref(), Self::KeyedChanges { .. })
                    }) =>
                {
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
                (Self::Routed { .. }, _) => {
                    return Err(CdfError::data(
                        "routed package segment kind does not match its output content authority",
                    ));
                }
            }
        }
        if let Self::KeyedChanges { reduction, .. } = self
            && (upserts != reduction.surviving.upserts || deletes != reduction.surviving.deletes)
        {
            return Err(CdfError::data(
                "keyed-change segment row counts do not match surviving effect authority",
            ));
        }
        if let Self::Routed { outputs, .. } = self {
            let expected =
                outputs
                    .iter()
                    .try_fold(KeyedEffectCounts::default(), |mut total, output| {
                        if let Self::KeyedChanges { reduction, .. } = output.content.as_ref() {
                            total.upserts = total
                                .upserts
                                .checked_add(reduction.surviving.upserts)
                                .ok_or_else(|| {
                                    CdfError::data("routed upsert count overflowed u64")
                                })?;
                            total.deletes = total
                                .deletes
                                .checked_add(reduction.surviving.deletes)
                                .ok_or_else(|| {
                                    CdfError::data("routed delete count overflowed u64")
                                })?;
                        }
                        Ok::<_, CdfError>(total)
                    })?;
            if expected.upserts != upserts || expected.deletes != deletes {
                return Err(CdfError::data(
                    "routed keyed-change segment counts do not match per-output effect authority",
                ));
            }
        }
        Ok(())
    }

    pub fn validate_routed_segment_rows<'a>(
        &self,
        segments: impl IntoIterator<Item = (&'a SegmentId, &'a PackageSegmentKind, u64)>,
    ) -> Result<()> {
        let Self::Routed { family, outputs } = self else {
            return Err(CdfError::internal(
                "output-bound segment validation requires routed package content",
            ));
        };
        let segments = segments.into_iter().collect::<Vec<_>>();
        let mut observed = BTreeSet::new();
        if segments
            .iter()
            .any(|(segment_id, _, _)| !observed.insert(*segment_id))
        {
            return Err(CdfError::data(
                "routed package contains a duplicate segment identity",
            ));
        }
        let mut assigned = BTreeSet::new();
        for (binding, output) in family.bindings.iter().zip(outputs) {
            if output.segment_ids.iter().any(|id| !assigned.insert(id)) {
                return Err(CdfError::data(
                    "routed package assigns a segment to more than one output",
                ));
            }
            output.content.validate_segment_rows(
                segments
                    .iter()
                    .filter(|(candidate, _, _)| output.segment_ids.contains(candidate))
                    .map(|(_, kind, rows)| (*kind, *rows)),
            )?;
            if output.output_binding != binding.output_binding {
                return Err(CdfError::data(
                    "routed package output order differs from its target family",
                ));
            }
        }
        if observed
            .iter()
            .any(|segment_id| !assigned.contains(segment_id))
        {
            return Err(CdfError::data(
                "routed package contains a segment absent from its output authority",
            ));
        }
        if assigned != observed {
            return Err(CdfError::data(
                "routed package output authority references a missing segment",
            ));
        }
        Ok(())
    }

    pub fn validate_segments<'a>(
        &self,
        segments: impl IntoIterator<Item = (&'a SegmentId, &'a PackageSegmentKind, u64)>,
    ) -> Result<()> {
        let segments = segments.into_iter().collect::<Vec<_>>();
        if matches!(self, Self::Routed { .. }) {
            self.validate_routed_segment_rows(segments)
        } else {
            self.validate_segment_rows(
                segments
                    .into_iter()
                    .map(|(_, kind, row_count)| (kind, row_count)),
            )
        }
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
            || self.duplicate_key_count > surviving
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyedEffectPlanAuthority {
    pub deletion_capture: DeletionCaptureAuthority,
    pub delete_application: DeleteApplicationAuthority,
}

impl KeyedEffectPlanAuthority {
    pub fn deletes_unsupported() -> Self {
        Self {
            deletion_capture: DeletionCaptureAuthority::unsupported(format!(
                "sha256:{:x}",
                sha2::Sha256::digest(b"cdf-source-deletion-capture-unsupported-v1")
            )),
            delete_application: DeleteApplicationAuthority::NotApplicable,
        }
    }

    pub fn validate_for_disposition(&self, disposition: &crate::WriteDisposition) -> Result<()> {
        self.deletion_capture.validate()?;
        self.delete_application.validate(&self.deletion_capture)?;
        match disposition {
            crate::WriteDisposition::Append | crate::WriteDisposition::Replace
                if self.deletion_capture.support != DeletionCaptureSupport::Unsupported =>
            {
                Err(CdfError::contract(
                    "append and replace cannot bind a delete-capable source plan",
                ))
            }
            crate::WriteDisposition::CdcApply if !self.deletion_capture.enabled => {
                Err(CdfError::contract(
                    "cdc_apply requires enabled source deletion capture and an explicit delete application policy",
                ))
            }
            _ => Ok(()),
        }
    }
}

fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};

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
            reduction: Box::new(KeyedEffectReductionAuthority {
                version: KEYED_EFFECT_AUTHORITY_VERSION,
                winner: KeyedEffectWinnerPolicy::Last,
                input_order: KeyedEffectInputOrder::CanonicalPackageRows { version: 1 },
                input: KeyedEffectCounts { upserts, deletes },
                duplicate_key_count: 0,
                surviving: KeyedEffectCounts { upserts, deletes },
                provenance_format: "parquet".to_owned(),
                provenance_version: 1,
            }),
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

    #[test]
    fn routed_content_requires_an_exact_schema_and_segment_partition() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let canonical = CanonicalArrowSchema::from_arrow(&schema).unwrap();
        let schema_hash = crate::canonical_arrow_schema_hash(&schema).unwrap();
        let values = StringArray::from(vec!["orders", "invoices"]);
        let family = RouteTargetFamily::new(
            crate::RoutePlan::new("source_collection", 2).unwrap(),
            crate::TargetName::new("events").unwrap(),
            Some(128),
            (0..2).map(|row| {
                (
                    crate::RouteScalar::from_array(&values, row).unwrap(),
                    schema_hash.clone(),
                )
            }),
        )
        .unwrap();
        let first = SegmentId::new("route-orders-000001").unwrap();
        let second = SegmentId::new("route-invoices-000001").unwrap();
        let content = PackageContentAuthority::Routed {
            outputs: family
                .bindings
                .iter()
                .zip([first.clone(), second.clone()])
                .map(|(binding, segment_id)| RoutedOutputContentAuthority {
                    output_binding: binding.output_binding.clone(),
                    schema: canonical.clone(),
                    content: Box::new(PackageContentAuthority::rows(schema_hash.clone())),
                    segment_ids: vec![segment_id],
                })
                .collect(),
            family,
        };

        content
            .validate_segments([
                (&first, &PackageSegmentKind::Row, 2),
                (&second, &PackageSegmentKind::Row, 3),
            ])
            .unwrap();

        let error = content
            .validate_segments([(&first, &PackageSegmentKind::Row, 2)])
            .unwrap_err();
        assert!(error.message.contains("missing segment"));

        let error = content
            .validate_segments([
                (&first, &PackageSegmentKind::Row, 2),
                (&first, &PackageSegmentKind::Row, 3),
            ])
            .unwrap_err();
        assert!(error.message.contains("duplicate segment"));
    }
}
