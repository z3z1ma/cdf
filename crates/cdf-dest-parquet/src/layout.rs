use cdf_kernel::{CdfError, Result, SegmentId};

const DEFAULT_TARGET_PACKAGE_BYTES_PER_OBJECT: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_SEGMENTS_PER_OBJECT: u16 = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParquetSegmentLayout {
    pub(crate) segment_id: SegmentId,
    pub(crate) package_byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParquetObjectLayout {
    pub(crate) ordinal: u32,
    pub(crate) segments: Vec<ParquetSegmentLayout>,
    pub(crate) package_byte_count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetObjectLayoutPolicy {
    target_package_bytes: u64,
    max_segments: u16,
}

impl ParquetObjectLayoutPolicy {
    pub fn new(target_package_bytes: u64, max_segments: u16) -> Result<Self> {
        Self {
            target_package_bytes,
            max_segments,
        }
        .validate()
    }

    pub(crate) fn validate(self) -> Result<Self> {
        if self.target_package_bytes == 0 || self.max_segments == 0 {
            return Err(CdfError::contract(
                "Parquet object layout bounds must be nonzero",
            ));
        }
        Ok(self)
    }

    pub const fn target_package_bytes(self) -> u64 {
        self.target_package_bytes
    }

    pub const fn max_segments(self) -> u16 {
        self.max_segments
    }

    pub(crate) fn closes_before(
        self,
        current_segments: usize,
        current_package_bytes: u64,
        next_package_bytes: u64,
    ) -> bool {
        current_segments != 0
            && (current_segments >= usize::from(self.max_segments)
                || current_package_bytes.saturating_add(next_package_bytes)
                    > self.target_package_bytes)
    }

    pub(crate) fn plan(
        self,
        segments: impl IntoIterator<Item = ParquetSegmentLayout>,
    ) -> Result<Vec<ParquetObjectLayout>> {
        let policy = self.validate()?;
        let mut layouts = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = 0_u64;
        for segment in segments {
            if policy.closes_before(current.len(), current_bytes, segment.package_byte_count) {
                push_layout(&mut layouts, &mut current, &mut current_bytes)?;
            }
            current_bytes = current_bytes
                .checked_add(segment.package_byte_count)
                .ok_or_else(|| CdfError::data("Parquet object package byte count overflow"))?;
            current.push(segment);
        }
        if !current.is_empty() {
            push_layout(&mut layouts, &mut current, &mut current_bytes)?;
        }
        Ok(layouts)
    }
}

impl Default for ParquetObjectLayoutPolicy {
    fn default() -> Self {
        Self {
            target_package_bytes: DEFAULT_TARGET_PACKAGE_BYTES_PER_OBJECT,
            max_segments: DEFAULT_MAX_SEGMENTS_PER_OBJECT,
        }
    }
}

fn push_layout(
    layouts: &mut Vec<ParquetObjectLayout>,
    current: &mut Vec<ParquetSegmentLayout>,
    current_bytes: &mut u64,
) -> Result<()> {
    let ordinal = u32::try_from(layouts.len())
        .map_err(|_| CdfError::data("Parquet object layout exceeds u32 objects"))?;
    layouts.push(ParquetObjectLayout {
        ordinal,
        segments: std::mem::take(current),
        package_byte_count: std::mem::take(current_bytes),
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_is_deterministic_and_bounds_non_oversized_groups() {
        let policy = ParquetObjectLayoutPolicy::new(100, 3).unwrap();
        let segments = [60_u64, 40, 1, 1, 200, 1]
            .into_iter()
            .enumerate()
            .map(|(index, package_byte_count)| ParquetSegmentLayout {
                segment_id: SegmentId::new(format!("seg-{index}")).unwrap(),
                package_byte_count,
            })
            .collect::<Vec<_>>();
        let layouts = policy.plan(segments.clone()).unwrap();
        let repeated = policy.plan(segments).unwrap();
        assert_eq!(layouts, repeated);
        assert_eq!(layouts.len(), 4);
        assert_eq!(layouts[0].package_byte_count, 100);
        assert_eq!(layouts[0].segments.len(), 2);
        assert_eq!(layouts[1].segments.len(), 2);
        assert_eq!(layouts[2].package_byte_count, 200);
        assert_eq!(layouts[3].package_byte_count, 1);
        assert_eq!(layouts[3].ordinal, 3);
    }

    #[test]
    fn oversized_segment_is_a_singleton_without_weakening_later_groups() {
        let policy = ParquetObjectLayoutPolicy::new(100, 3).unwrap();
        let layouts = policy
            .plan([200_u64, 50, 50, 1].into_iter().enumerate().map(
                |(index, package_byte_count)| ParquetSegmentLayout {
                    segment_id: SegmentId::new(format!("seg-{index}")).unwrap(),
                    package_byte_count,
                },
            ))
            .unwrap();
        assert_eq!(
            layouts
                .iter()
                .map(|layout| (layout.package_byte_count, layout.segments.len()))
                .collect::<Vec<_>>(),
            vec![(200, 1), (100, 2), (1, 1)]
        );
    }

    #[test]
    fn default_layout_groups_seventeen_canonical_segments_as_eight_eight_one() {
        let policy = ParquetObjectLayoutPolicy::default();
        let segment_bytes = policy.target_package_bytes() / u64::from(policy.max_segments());
        let segments = (0..17).map(|index| ParquetSegmentLayout {
            segment_id: SegmentId::new(format!("seg-{index:06}")).unwrap(),
            package_byte_count: segment_bytes,
        });

        let first = policy.plan(segments.clone()).unwrap();
        let second = policy.plan(segments).unwrap();

        assert_eq!(first, second);
        assert_eq!(
            first
                .iter()
                .map(|object| object.segments.len())
                .collect::<Vec<_>>(),
            vec![8, 8, 1]
        );
        assert_eq!(first[0].ordinal, 0);
        assert_eq!(first[1].ordinal, 1);
        assert_eq!(first[2].ordinal, 2);
        assert_eq!(first[0].segments[0].segment_id.as_str(), "seg-000000");
        assert_eq!(first[1].segments[0].segment_id.as_str(), "seg-000008");
        assert_eq!(first[2].segments[0].segment_id.as_str(), "seg-000016");
    }
}
