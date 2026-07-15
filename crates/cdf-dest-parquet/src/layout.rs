use cdf_kernel::{CdfError, Result, SegmentId};

pub(crate) const PHYSICAL_PLAN_PATH: &str = "arrow_ipc_to_parquet";
pub(crate) const PHYSICAL_PLAN_VERSION: u16 = 3;
pub(crate) const TARGET_PACKAGE_BYTES_PER_OBJECT: u64 = 256 * 1024 * 1024;
pub(crate) const MAX_SEGMENTS_PER_OBJECT: u16 = 8;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ParquetObjectLayoutPolicy {
    pub(crate) target_package_bytes: u64,
    pub(crate) max_segments: u16,
}

impl ParquetObjectLayoutPolicy {
    pub(crate) const fn current() -> Self {
        Self {
            target_package_bytes: TARGET_PACKAGE_BYTES_PER_OBJECT,
            max_segments: MAX_SEGMENTS_PER_OBJECT,
        }
    }

    pub(crate) fn validate(self) -> Result<Self> {
        if self.target_package_bytes == 0 || self.max_segments == 0 {
            return Err(CdfError::contract(
                "Parquet object layout bounds must be nonzero",
            ));
        }
        Ok(self)
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
        let policy = ParquetObjectLayoutPolicy {
            target_package_bytes: 100,
            max_segments: 3,
        };
        let layouts = policy
            .plan([60_u64, 40, 1, 1, 200, 1].into_iter().enumerate().map(
                |(index, package_byte_count)| ParquetSegmentLayout {
                    segment_id: SegmentId::new(format!("seg-{index}")).unwrap(),
                    package_byte_count,
                },
            ))
            .unwrap();
        assert_eq!(layouts.len(), 4);
        assert_eq!(layouts[0].package_byte_count, 100);
        assert_eq!(layouts[0].segments.len(), 2);
        assert_eq!(layouts[1].segments.len(), 2);
        assert_eq!(layouts[2].package_byte_count, 200);
        assert_eq!(layouts[3].package_byte_count, 1);
        assert_eq!(layouts[3].ordinal, 3);
    }

    #[test]
    fn current_layout_groups_seventeen_canonical_segments_as_eight_eight_one() {
        let segment_bytes = TARGET_PACKAGE_BYTES_PER_OBJECT / u64::from(MAX_SEGMENTS_PER_OBJECT);
        let segments = (0..17).map(|index| ParquetSegmentLayout {
            segment_id: SegmentId::new(format!("seg-{index:06}")).unwrap(),
            package_byte_count: segment_bytes,
        });

        let first = ParquetObjectLayoutPolicy::current()
            .plan(segments.clone())
            .unwrap();
        let second = ParquetObjectLayoutPolicy::current().plan(segments).unwrap();

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
