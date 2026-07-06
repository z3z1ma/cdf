use crate::{package::LoadedSegment, *};

pub(crate) fn write_parquet_segment(segment: &LoadedSegment) -> Result<Vec<u8>> {
    firn_package::transcode_record_batches_to_parquet_bytes(&segment.batches)
}
