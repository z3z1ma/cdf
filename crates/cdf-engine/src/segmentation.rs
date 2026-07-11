use std::collections::BTreeMap;

use cdf_kernel::{
    CdfError, CompositePosition, CursorPosition, CursorValue, FileManifest, FilePosition, Result,
    SegmentId, SourcePosition,
};
use cdf_memory::MemorySnapshot;
use serde::{Deserialize, Serialize};

pub const SEGMENTATION_POLICY_VERSION: u16 = 1;
pub const POSITION_ALGEBRA_VERSION: u16 = 1;
pub const SEGMENT_ID_NAMESPACE: &str = "partition-segment-ordinal-v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalSegmentationPolicy {
    pub version: u16,
    pub target_rows: u32,
    pub target_bytes: u64,
    pub maximum_rows: u32,
    pub maximum_bytes: u64,
    pub microbatch_minimum_rows: u32,
    pub microbatch_maximum_rows: u32,
    pub microbatch_minimum_bytes: u64,
    pub microbatch_maximum_bytes: u64,
    pub segment_id_namespace: String,
    pub position_algebra_version: u16,
}

impl CanonicalSegmentationPolicy {
    pub fn p3_v1() -> Self {
        Self {
            version: SEGMENTATION_POLICY_VERSION,
            target_rows: 64 * 1024,
            target_bytes: 8 * 1024 * 1024,
            maximum_rows: 64 * 1024,
            maximum_bytes: 32 * 1024 * 1024,
            microbatch_minimum_rows: 8 * 1024,
            microbatch_maximum_rows: 64 * 1024,
            microbatch_minimum_bytes: 1024 * 1024,
            microbatch_maximum_bytes: 32 * 1024 * 1024,
            segment_id_namespace: SEGMENT_ID_NAMESPACE.to_owned(),
            position_algebra_version: POSITION_ALGEBRA_VERSION,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != SEGMENTATION_POLICY_VERSION
            || self.position_algebra_version != POSITION_ALGEBRA_VERSION
            || self.segment_id_namespace != SEGMENT_ID_NAMESPACE
        {
            return Err(CdfError::contract(
                "unsupported canonical segmentation policy version or namespace",
            ));
        }
        if self.microbatch_minimum_rows == 0
            || self.microbatch_minimum_rows > self.microbatch_maximum_rows
            || self.microbatch_maximum_rows > self.maximum_rows
            || self.microbatch_minimum_bytes == 0
            || self.microbatch_minimum_bytes > self.microbatch_maximum_bytes
            || self.target_bytes > self.maximum_bytes
            || self.microbatch_maximum_bytes > self.maximum_bytes
        {
            return Err(CdfError::contract(
                "canonical segmentation row/byte bounds are inconsistent",
            ));
        }
        Ok(())
    }

    pub fn segment_id(&self, partition_ordinal: u32, segment_ordinal: u32) -> Result<SegmentId> {
        self.validate()?;
        SegmentId::new(format!("p{partition_ordinal:08}-s{segment_ordinal:08}"))
    }
}

#[derive(Clone, Debug)]
pub struct AdaptiveMicrobatchController {
    policy: CanonicalSegmentationPolicy,
}

impl AdaptiveMicrobatchController {
    pub fn new(policy: CanonicalSegmentationPolicy) -> Result<Self> {
        policy.validate()?;
        Ok(Self { policy })
    }

    pub fn target_rows(&self, observed_row_bytes: u64, memory: &MemorySnapshot) -> u32 {
        let row_bytes = observed_row_bytes.max(1);
        let byte_target = self
            .policy
            .target_bytes
            .min(self.policy.microbatch_maximum_bytes);
        let pressure_available = memory
            .budget_bytes
            .saturating_sub(memory.current_bytes)
            .max(self.policy.microbatch_minimum_bytes);
        let rows = byte_target
            .min(pressure_available)
            .checked_div(row_bytes)
            .unwrap_or(1)
            .clamp(
                u64::from(self.policy.microbatch_minimum_rows),
                u64::from(self.policy.microbatch_maximum_rows),
            );
        u32::try_from(rows).unwrap_or(self.policy.microbatch_maximum_rows)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PositionJoin {
    Joined(SourcePosition),
    Boundary,
}

pub fn join_positions(left: &SourcePosition, right: &SourcePosition) -> Result<PositionJoin> {
    Ok(match (left, right) {
        (SourcePosition::FileManifest(left), SourcePosition::FileManifest(right)) => {
            PositionJoin::Joined(SourcePosition::FileManifest(join_file_manifests(
                left, right,
            )?))
        }
        (SourcePosition::Cursor(left), SourcePosition::Cursor(right)) => join_cursors(left, right)?
            .map_or(PositionJoin::Boundary, |position| {
                PositionJoin::Joined(SourcePosition::Cursor(position))
            }),
        (SourcePosition::Log(left), SourcePosition::Log(right))
            if left.version == right.version
                && left.log == right.log
                && left.sequence == right.sequence =>
        {
            PositionJoin::Joined(SourcePosition::Log(if left.offset >= right.offset {
                left.clone()
            } else {
                right.clone()
            }))
        }
        (SourcePosition::Composite(left), SourcePosition::Composite(right))
            if left.version == right.version
                && left.positions.keys().eq(right.positions.keys()) =>
        {
            let mut positions = BTreeMap::new();
            for (key, left_position) in &left.positions {
                match join_positions(left_position, &right.positions[key])? {
                    PositionJoin::Joined(position) => {
                        positions.insert(key.clone(), position);
                    }
                    PositionJoin::Boundary => return Ok(PositionJoin::Boundary),
                }
            }
            PositionJoin::Joined(SourcePosition::Composite(CompositePosition {
                version: left.version,
                positions,
            }))
        }
        _ if left == right => PositionJoin::Joined(left.clone()),
        _ => PositionJoin::Boundary,
    })
}

fn join_file_manifests(left: &FileManifest, right: &FileManifest) -> Result<FileManifest> {
    if left.version != right.version {
        return Err(CdfError::contract(
            "file-manifest position versions cannot be joined",
        ));
    }
    let mut files = BTreeMap::<String, FilePosition>::new();
    for file in left.files.iter().chain(&right.files) {
        match files.get(&file.path) {
            Some(existing) if existing != file => {
                return Err(CdfError::contract(format!(
                    "file-manifest position has conflicting identity for `{}`",
                    file.path
                )));
            }
            Some(_) => {}
            None => {
                files.insert(file.path.clone(), file.clone());
            }
        }
    }
    Ok(FileManifest {
        version: left.version,
        files: files.into_values().collect(),
    })
}

fn join_cursors(left: &CursorPosition, right: &CursorPosition) -> Result<Option<CursorPosition>> {
    if left.version != right.version || left.field != right.field {
        return Ok(None);
    }
    let value = match (&left.value, &right.value) {
        (CursorValue::I64(left), CursorValue::I64(right)) => CursorValue::I64((*left).max(*right)),
        (CursorValue::U64(left), CursorValue::U64(right)) => CursorValue::U64((*left).max(*right)),
        (
            CursorValue::TimestampMicros {
                micros: left,
                timezone: left_tz,
            },
            CursorValue::TimestampMicros {
                micros: right,
                timezone: right_tz,
            },
        ) if left_tz == right_tz => CursorValue::TimestampMicros {
            micros: (*left).max(*right),
            timezone: left_tz.clone(),
        },
        (left, right) if left == right => left.clone(),
        _ => return Ok(None),
    };
    Ok(Some(CursorPosition {
        version: left.version,
        field: left.field.clone(),
        value,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_kernel::{CursorPosition, FileManifest};

    #[test]
    fn ids_depend_only_on_partition_and_segment_ordinals() {
        let policy = CanonicalSegmentationPolicy::p3_v1();
        assert_eq!(
            policy.segment_id(2, 7).unwrap().as_str(),
            "p00000002-s00000007"
        );
        assert_eq!(policy, CanonicalSegmentationPolicy::p3_v1());
    }

    #[test]
    fn adaptive_targets_stay_bounded_and_outside_policy_identity() {
        let policy = CanonicalSegmentationPolicy::p3_v1();
        let controller = AdaptiveMicrobatchController::new(policy.clone()).unwrap();
        let roomy = MemorySnapshot {
            budget_bytes: 4 * 1024 * 1024 * 1024,
            ..MemorySnapshot::default()
        };
        let pressured = MemorySnapshot {
            budget_bytes: 64 * 1024 * 1024,
            current_bytes: 63 * 1024 * 1024,
            ..MemorySnapshot::default()
        };
        assert_eq!(controller.target_rows(64, &roomy), 64 * 1024);
        assert_eq!(controller.target_rows(1024, &pressured), 8 * 1024);
        assert_eq!(policy, CanonicalSegmentationPolicy::p3_v1());
    }

    #[test]
    fn typed_position_join_unions_files_advances_numeric_cursor_and_refuses_tokens() {
        let file = |path: &str| FilePosition {
            path: path.to_owned(),
            size_bytes: 1,
            etag: Some(path.to_owned()),
            sha256: None,
        };
        let left = SourcePosition::FileManifest(FileManifest {
            version: 1,
            files: vec![file("b"), file("a")],
        });
        let right = SourcePosition::FileManifest(FileManifest {
            version: 1,
            files: vec![file("c")],
        });
        let PositionJoin::Joined(SourcePosition::FileManifest(joined)) =
            join_positions(&left, &right).unwrap()
        else {
            panic!("file manifests should join");
        };
        assert_eq!(
            joined
                .files
                .iter()
                .map(|f| f.path.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );

        let cursor = |value| {
            SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "id".to_owned(),
                value: CursorValue::U64(value),
            })
        };
        assert_eq!(
            join_positions(&cursor(2), &cursor(9)).unwrap(),
            PositionJoin::Joined(cursor(9))
        );
        assert_eq!(
            join_positions(
                &SourcePosition::PageToken(cdf_kernel::PageToken {
                    version: 1,
                    token: "a".to_owned()
                }),
                &SourcePosition::PageToken(cdf_kernel::PageToken {
                    version: 1,
                    token: "b".to_owned()
                })
            )
            .unwrap(),
            PositionJoin::Boundary
        );
    }
}
