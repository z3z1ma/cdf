use std::collections::BTreeMap;

use arrow_array::RecordBatch;
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

#[derive(Clone, Debug)]
pub struct CanonicalSegment {
    pub segment_id: SegmentId,
    pub partition_ordinal: u32,
    pub segment_ordinal: u32,
    pub batches: Vec<RecordBatch>,
    pub output_position: Option<SourcePosition>,
    pub row_count: u64,
    pub retained_bytes: u64,
}

pub struct CanonicalSegmentAssembler {
    policy: CanonicalSegmentationPolicy,
    partition_ordinal: u32,
    next_segment_ordinal: u32,
    batches: Vec<RecordBatch>,
    output_position: Option<SourcePosition>,
    rows: u64,
    retained_bytes: u64,
}

impl CanonicalSegmentAssembler {
    pub fn new(policy: CanonicalSegmentationPolicy, partition_ordinal: u32) -> Result<Self> {
        policy.validate()?;
        Ok(Self {
            policy,
            partition_ordinal,
            next_segment_ordinal: 0,
            batches: Vec::new(),
            output_position: None,
            rows: 0,
            retained_bytes: 0,
        })
    }

    pub fn push(
        &mut self,
        mut batch: RecordBatch,
        position: Option<SourcePosition>,
    ) -> Result<Vec<CanonicalSegment>> {
        let mut emitted = Vec::new();
        if self.rows > 0 {
            match (&self.output_position, &position) {
                (Some(left), Some(right)) => match join_positions(left, right)? {
                    PositionJoin::Joined(joined) => self.output_position = Some(joined),
                    PositionJoin::Boundary => emitted.push(self.flush()?.unwrap()),
                },
                (None, None) => {}
                _ => emitted.push(self.flush()?.unwrap()),
            }
        }
        if self.rows == 0 {
            self.output_position = position.clone();
        }
        while batch.num_rows() > 0 {
            let remaining_rows = u64::from(self.policy.target_rows).saturating_sub(self.rows);
            let take = usize::try_from(remaining_rows)
                .unwrap_or(usize::MAX)
                .min(batch.num_rows());
            if take < batch.num_rows() && position.is_some() {
                if self.rows > 0 {
                    emitted.push(self.flush()?.unwrap());
                    continue;
                }
                return Err(CdfError::data(
                    "oversized positioned batch requires exact slice-position authority",
                ));
            }
            let chunk = if take == batch.num_rows() {
                let schema = batch.schema();
                std::mem::replace(&mut batch, RecordBatch::new_empty(schema))
            } else {
                let chunk = batch.slice(0, take);
                batch = batch.slice(take, batch.num_rows() - take);
                chunk
            };
            let chunk_bytes = u64::try_from(chunk.get_array_memory_size())
                .map_err(|_| CdfError::data("canonical segment memory exceeds u64"))?;
            if chunk_bytes > self.policy.maximum_bytes {
                return Err(CdfError::data(format!(
                    "one canonical segment chunk retains {chunk_bytes} bytes above the {}-byte policy maximum",
                    self.policy.maximum_bytes
                )));
            }
            self.rows += u64::try_from(chunk.num_rows())
                .map_err(|_| CdfError::data("canonical segment rows exceed u64"))?;
            self.retained_bytes = self.retained_bytes.saturating_add(chunk_bytes);
            self.batches.push(chunk);
            if self.rows >= u64::from(self.policy.target_rows) {
                emitted.push(self.flush()?.unwrap());
            }
        }
        Ok(emitted)
    }

    pub fn finish(&mut self) -> Result<Vec<CanonicalSegment>> {
        Ok(self.flush()?.into_iter().collect())
    }

    fn flush(&mut self) -> Result<Option<CanonicalSegment>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let segment_ordinal = self.next_segment_ordinal;
        self.next_segment_ordinal = self
            .next_segment_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("canonical segment ordinal overflow"))?;
        Ok(Some(CanonicalSegment {
            segment_id: self
                .policy
                .segment_id(self.partition_ordinal, segment_ordinal)?,
            partition_ordinal: self.partition_ordinal,
            segment_ordinal,
            batches: std::mem::take(&mut self.batches),
            output_position: self.output_position.take(),
            row_count: std::mem::take(&mut self.rows),
            retained_bytes: std::mem::take(&mut self.retained_bytes),
        }))
    }
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
    use arrow_array::{Array, Int64Array};
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

    fn test_policy() -> CanonicalSegmentationPolicy {
        CanonicalSegmentationPolicy {
            target_rows: 4,
            maximum_rows: 4,
            microbatch_minimum_rows: 1,
            microbatch_maximum_rows: 4,
            microbatch_minimum_bytes: 1,
            ..CanonicalSegmentationPolicy::p3_v1()
        }
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_from_iter([(
            "value",
            std::sync::Arc::new(Int64Array::from(values.to_vec())) as _,
        )])
        .unwrap()
    }

    fn assemble(chunks: &[&[i64]]) -> Vec<CanonicalSegment> {
        let mut assembler = CanonicalSegmentAssembler::new(test_policy(), 3).unwrap();
        let mut output = Vec::new();
        for chunk in chunks {
            output.extend(assembler.push(batch(chunk), None).unwrap());
        }
        output.extend(assembler.finish().unwrap());
        output
    }

    #[test]
    fn assembler_is_source_rechunking_invariant_and_coalesces_tiny_inputs() {
        let one = assemble(&[&[1, 2, 3, 4, 5, 6]]);
        let many = assemble(&[&[1], &[2, 3], &[4], &[5, 6]]);
        for segments in [&one, &many] {
            assert_eq!(segments.len(), 2);
            assert_eq!(segments[0].segment_id.as_str(), "p00000003-s00000000");
            assert_eq!(segments[0].row_count, 4);
            assert_eq!(segments[1].row_count, 2);
        }
        let values = |segments: &[CanonicalSegment]| {
            segments
                .iter()
                .flat_map(|segment| &segment.batches)
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                })
                .copied()
                .collect::<Vec<_>>()
        };
        assert_eq!(values(&one), values(&many));
    }

    #[test]
    fn positioned_oversize_requires_slice_authority_instead_of_inventing_cursor() {
        let mut assembler = CanonicalSegmentAssembler::new(test_policy(), 0).unwrap();
        let error = assembler
            .push(
                batch(&[1, 2, 3, 4, 5]),
                Some(SourcePosition::Cursor(CursorPosition {
                    version: 1,
                    field: "id".to_owned(),
                    value: CursorValue::U64(5),
                })),
            )
            .unwrap_err();
        assert!(error.message.contains("slice-position authority"));
    }
}
