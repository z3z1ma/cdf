use crate::*;

pub(crate) fn write_parquet_segment(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    cdf_package::transcode_record_batches_to_parquet_bytes(batches)
}
