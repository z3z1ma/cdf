//! OCF block planning and schema/projection preparation.

use std::ops::Range;

use arrow_avro::{
    reader::{AsyncAvroFileReader, HeaderInfo, ReaderBuilder},
    schema::FingerprintAlgorithm,
};
use arrow_schema::SchemaRef;
use cdf_kernel::{CdfError, Result};
use cdf_runtime::{ByteExtent, ByteSource, DecodeUnitPlan};

use crate::byte_source::{AvroByteSource, AvroLongDecoder};
use crate::errors::{avro_arrow_error, avro_error};
use crate::options::{
    MAXIMUM_VLQ_HEADER_BYTES, MAXIMUM_WORKING_SET_BYTES, OCF_SYNC_MARKER_BYTES, OcfOptions,
    SingleObjectOptions,
};

pub(crate) fn schema_from_header(
    reader: AvroByteSource,
    size: u64,
    header: HeaderInfo,
) -> Result<SchemaRef> {
    AsyncAvroFileReader::builder(reader, size, 1)
        .with_range(header.header_len()..header.header_len())
        .with_strict_mode(true)
        .build_with_header(header)
        .map(|reader| reader.schema())
        .map_err(avro_error)
}

pub(crate) fn single_object_schema(
    options: &SingleObjectOptions,
    projection: Option<&[String]>,
    target_batch_rows: usize,
) -> Result<(SchemaRef, String)> {
    let schema = options.writer_schema()?;
    let fingerprint = schema
        .fingerprint(FingerprintAlgorithm::Rabin)
        .map_err(avro_arrow_error)?;
    let builder = ReaderBuilder::new()
        .with_writer_schema_store(options.schema_store()?)
        .with_batch_size(target_batch_rows.max(1))
        .with_strict_mode(true);
    let decoder = builder.build_decoder().map_err(avro_arrow_error)?;
    let schema_ref = decoder.schema();
    let indices = projection_indices(schema_ref.as_ref(), projection)?;
    if let Some(indices) = indices {
        let projected = ReaderBuilder::new()
            .with_writer_schema_store(options.schema_store()?)
            .with_batch_size(target_batch_rows.max(1))
            .with_strict_mode(true)
            .with_projection(indices)
            .build_decoder()
            .map_err(avro_arrow_error)?;
        return Ok((projected.schema(), format!("{fingerprint:?}")));
    }
    Ok((schema_ref, format!("{fingerprint:?}")))
}

pub(crate) async fn ocf_units(
    source: &dyn ByteSource,
    size: u64,
    header_len: u64,
    sync: [u8; 16],
    options: OcfOptions,
    target_batch_bytes: u64,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<(Vec<DecodeUnitPlan>, Vec<Range<u64>>)> {
    if header_len > size {
        return Err(CdfError::data("Avro OCF header exceeds source length"));
    }
    if header_len == size {
        return Ok((
            vec![DecodeUnitPlan {
                unit_id: "ocf-schema-only".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: target_batch_bytes.max(1),
                independently_retryable: true,
            }],
            std::iter::once(0..size).collect(),
        ));
    }
    let first_sync_start = header_len
        .checked_sub(OCF_SYNC_MARKER_BYTES)
        .ok_or_else(|| CdfError::data("Avro OCF header is too short to contain its sync marker"))?;
    let mut units = Vec::new();
    let mut ranges = Vec::new();
    let mut block_header_offset = header_len;
    while block_header_offset < size {
        cancellation.check()?;
        let ordinal = u64::try_from(units.len())
            .map_err(|_| CdfError::data("Avro OCF block count exceeds u64"))?;
        if ordinal >= u64::from(options.maximum_blocks) {
            return Err(CdfError::data(format!(
                "Avro OCF contains more than the configured {} block maximum; increase format_options.maximum_blocks only when the resulting planning metadata is acceptable",
                options.maximum_blocks
            )));
        }

        let header_end = block_header_offset
            .checked_add(MAXIMUM_VLQ_HEADER_BYTES)
            .ok_or_else(|| CdfError::data("Avro OCF block-header range overflowed"))?
            .min(size);
        let header_extent = ByteExtent::new(block_header_offset, header_end - block_header_offset)?;
        let header_bytes = source
            .read_exact_range(header_extent, cancellation.clone())
            .await?;
        let (record_count, count_bytes) = decode_avro_long(header_bytes.payload())?;
        let (encoded_bytes, size_bytes) = decode_avro_long(&header_bytes.payload()[count_bytes..])?;
        let record_count = u64::try_from(record_count).map_err(|_| {
            CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} declares a negative record count"
            ))
        })?;
        if record_count == 0 || record_count > options.maximum_block_records {
            return Err(CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} declares {record_count} records outside the configured 1..={} record authority; increase format_options.maximum_block_records only for a trusted producer",
                options.maximum_block_records
            )));
        }
        let encoded_bytes = u64::try_from(encoded_bytes).map_err(|_| {
            CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} declares a negative encoded size"
            ))
        })?;
        if encoded_bytes > options.maximum_block_bytes {
            return Err(CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} declares {encoded_bytes} encoded bytes above the configured {}-byte maximum; increase format_options.maximum_block_bytes only for a trusted producer",
                options.maximum_block_bytes
            )));
        }
        let block_header_bytes = u64::try_from(count_bytes + size_bytes)
            .map_err(|_| CdfError::data("Avro OCF block-header length exceeds u64"))?;
        let sync_start = block_header_offset
            .checked_add(block_header_bytes)
            .and_then(|offset| offset.checked_add(encoded_bytes))
            .ok_or_else(|| CdfError::data("Avro OCF block extent overflowed"))?;
        let block_end = sync_start
            .checked_add(OCF_SYNC_MARKER_BYTES)
            .ok_or_else(|| CdfError::data("Avro OCF sync extent overflowed"))?;
        if block_end > size {
            return Err(CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} extends to {block_end} beyond the {size}-byte source"
            )));
        }
        let observed_sync = source
            .read_exact_range(
                ByteExtent::new(sync_start, OCF_SYNC_MARKER_BYTES)?,
                cancellation.clone(),
            )
            .await?;
        if observed_sync.payload() != sync {
            return Err(CdfError::data(format!(
                "Avro OCF block at offset {block_header_offset} has a sync marker that does not match the file header"
            )));
        }

        let range_start = if ordinal == 0 {
            first_sync_start
        } else {
            block_header_offset
                .checked_sub(OCF_SYNC_MARKER_BYTES)
                .ok_or_else(|| CdfError::data("Avro OCF block range underflowed"))?
        };
        let range = range_start..block_end;
        let extent = ByteExtent::new(range.start, range.end - range.start)?;
        let unit = DecodeUnitPlan {
            unit_id: format!("block-{ordinal:08}-{}-{}", range.start, range.end),
            ordinal,
            extent: Some(extent),
            estimated_working_set_bytes: extent
                .length
                .checked_add(target_batch_bytes)
                .ok_or_else(|| CdfError::contract("Avro OCF unit estimate overflowed"))?
                .min(MAXIMUM_WORKING_SET_BYTES),
            independently_retryable: true,
        };
        unit.validate()?;
        units.push(unit);
        ranges.push(range);
        block_header_offset = block_end;
    }
    Ok((units, ranges))
}

pub(crate) fn decode_avro_long(bytes: &[u8]) -> Result<(i64, usize)> {
    let mut decoder = AvroLongDecoder::default();
    for (index, byte) in bytes.iter().copied().enumerate() {
        if let Some(value) = decoder.push(byte)? {
            return Ok((value, index + 1));
        }
    }
    Err(CdfError::data(
        "Avro OCF source ended inside a block-header long",
    ))
}

pub(crate) fn projection_indices(
    schema: &arrow_schema::Schema,
    projection: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    projection
        .map(|projection| {
            projection
                .iter()
                .map(|name| {
                    schema.index_of(name).map_err(|_| {
                        CdfError::contract(format!(
                            "Avro projection field {name:?} is absent from the writer schema"
                        ))
                    })
                })
                .collect()
        })
        .transpose()
}
