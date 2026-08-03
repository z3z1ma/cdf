use std::collections::BTreeMap;

use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{CdfError, CommitSegment, DestinationCommitRequest, Result, SegmentId};
use cdf_package_contract::VerifiedPackageAccess;
use sha2::{Digest, Sha256};

use crate::{
    client::MAXIMUM_INPUT_BATCH_BYTES,
    mapping::{PACKAGE_HASH_COLUMN, columns_for_schema},
    models::{ClickHouseExpectedSegment, ClickHouseLoadPlan, ClickHouseSessionSegments},
};

pub(crate) const MAXIMUM_SEGMENTS_PER_PACKAGE: usize = 10_000;
pub(crate) const MAXIMUM_STATE_JSON_BYTES: usize = 2 * 1024 * 1024;

pub(crate) fn expected_segments_for_session(
    package: &dyn VerifiedPackageAccess,
    plan: &ClickHouseLoadPlan,
    request: &DestinationCommitRequest,
) -> Result<ClickHouseSessionSegments> {
    if package.package_hash() != plan.package_hash.as_str()
        || request.package_hash.as_str() != package.package_hash()
    {
        return Err(CdfError::data(
            "ClickHouse destination package, plan, and commit hashes differ",
        ));
    }
    if plan.segments.len() > MAXIMUM_SEGMENTS_PER_PACKAGE {
        return Err(CdfError::contract(format!(
            "ClickHouse destination package has {} segments beyond the {MAXIMUM_SEGMENTS_PER_PACKAGE}-segment settlement and deduplication ceiling",
            plan.segments.len()
        )));
    }
    let state_json = serde_json::to_vec(&plan.state_delta).map_err(|error| {
        CdfError::internal(format!("encode ClickHouse state mirror preflight: {error}"))
    })?;
    if state_json.len() > MAXIMUM_STATE_JSON_BYTES {
        return Err(CdfError::contract(format!(
            "ClickHouse state mirror needs {} bytes beyond the {MAXIMUM_STATE_JSON_BYTES}-byte verification ceiling",
            state_json.len()
        )));
    }
    let plan_by_id = plan
        .segments
        .iter()
        .map(|segment| (segment.segment_id.clone(), segment))
        .collect::<BTreeMap<_, _>>();
    if plan_by_id.len() != plan.segments.len() {
        return Err(CdfError::data(
            "ClickHouse destination plan contains duplicate segment identifiers",
        ));
    }
    let request_by_id = request
        .segments
        .iter()
        .map(|segment| (segment.segment_id.clone(), segment))
        .collect::<BTreeMap<_, _>>();
    if request_by_id.len() != request.segments.len() || request_by_id != plan_by_id {
        return Err(CdfError::data(
            "ClickHouse destination plan segments differ from the commit request",
        ));
    }
    let mut manifest = BTreeMap::new();
    let mut order = Vec::<SegmentId>::new();
    package.for_each_identity_segment(&mut |segment| {
        if manifest
            .insert(segment.segment_id.clone(), segment.clone())
            .is_some()
        {
            return Err(CdfError::data(
                "ClickHouse destination package manifest contains duplicate segments",
            ));
        }
        order.push(segment.segment_id.clone());
        Ok(())
    })?;
    if manifest.len() != plan_by_id.len() {
        return Err(CdfError::data(
            "ClickHouse destination package manifest and plan segment counts differ",
        ));
    }
    let mut expected = BTreeMap::new();
    for segment_id in order {
        let manifest_segment = manifest
            .get(&segment_id)
            .ok_or_else(|| CdfError::internal("ClickHouse package manifest lost a segment"))?;
        let state = plan_by_id.get(&segment_id).ok_or_else(|| {
            CdfError::data(format!(
                "ClickHouse destination plan omits package segment {segment_id}"
            ))
        })?;
        if state.row_count != manifest_segment.row_count {
            return Err(CdfError::data(format!(
                "ClickHouse destination segment {segment_id} row count differs from package manifest"
            )));
        }
        expected.insert(
            segment_id,
            ClickHouseExpectedSegment {
                state: (*state).clone(),
                package_byte_count: manifest_segment.byte_count,
                package_row_ord_start: manifest_segment.package_row_ord_start,
            },
        );
    }
    Ok(ClickHouseSessionSegments { expected })
}

pub(crate) fn validate_commit_segment(
    segment: &CommitSegment,
    expected: &ClickHouseExpectedSegment,
    plan: &ClickHouseLoadPlan,
) -> Result<()> {
    if segment.state != expected.state || segment.package_byte_count != expected.package_byte_count
    {
        return Err(CdfError::data(format!(
            "ClickHouse commit segment {} differs from finalized package authority",
            segment.state.segment_id
        )));
    }
    let mut rows = 0_u64;
    for batch in &segment.batches {
        let logical = cdf_package_contract::logical_output_schema(batch.schema().as_ref())?;
        if columns_for_schema(&logical)? != plan.columns {
            return Err(CdfError::data(format!(
                "ClickHouse commit segment {} schema differs from its prepared mapping",
                segment.state.segment_id
            )));
        }
        rows = rows
            .checked_add(
                u64::try_from(batch.num_rows()).map_err(|_| {
                    CdfError::data("ClickHouse segment batch row count exceeds u64")
                })?,
            )
            .ok_or_else(|| CdfError::data("ClickHouse segment row count overflowed"))?;
    }
    if rows != expected.state.row_count {
        return Err(CdfError::data(format!(
            "ClickHouse segment {} payload row count {rows} differs from manifest {}",
            segment.state.segment_id, expected.state.row_count
        )));
    }
    cdf_package_contract::validate_package_row_ord_batches(
        &segment.batches,
        expected.package_row_ord_start,
        expected.state.row_count,
    )
}

pub(crate) fn add_package_hash(
    batch: RecordBatch,
    package_hash: &cdf_kernel::PackageHash,
) -> Result<RecordBatch> {
    let digest = decode_package_hash(package_hash)?;
    let byte_count = batch
        .num_rows()
        .checked_mul(digest.len())
        .ok_or_else(|| CdfError::data("ClickHouse package provenance allocation overflowed"))?;
    let logical_bytes = u64::try_from(batch.get_array_memory_size())
        .map_err(|_| CdfError::data("ClickHouse Arrow batch memory size exceeds u64"))?;
    let physical_bytes =
        logical_bytes
            .checked_add(u64::try_from(byte_count).map_err(|_| {
                CdfError::data("ClickHouse package provenance allocation exceeds u64")
            })?)
            .ok_or_else(|| CdfError::data("ClickHouse physical batch size overflowed"))?;
    if physical_bytes > MAXIMUM_INPUT_BATCH_BYTES {
        return Err(CdfError::data(format!(
            "ClickHouse physical Arrow batch needs {physical_bytes} bytes beyond the {MAXIMUM_INPUT_BATCH_BYTES}-byte admitted ceiling"
        )));
    }
    let mut values = Vec::with_capacity(byte_count);
    for _ in 0..batch.num_rows() {
        values.extend_from_slice(&digest);
    }
    let package_hashes =
        FixedSizeBinaryArray::try_new(32, Buffer::from(values), None).map_err(|_| {
            CdfError::internal("ClickHouse package hash array construction violated its shape")
        })?;
    let schema = batch.schema();
    let (ordinal_field, logical_fields) = schema.fields().split_last().ok_or_else(|| {
        CdfError::data("ClickHouse canonical segment batch has no package ordinal")
    })?;
    if !cdf_package_contract::is_package_row_ord_field(ordinal_field) {
        return Err(CdfError::data(
            "ClickHouse canonical segment batch has invalid package ordinal field",
        ));
    }
    let mut fields = logical_fields.to_vec();
    fields.push(Field::new(PACKAGE_HASH_COLUMN, DataType::FixedSizeBinary(32), false).into());
    fields.push(ordinal_field.clone());
    let mut columns = batch
        .columns()
        .get(..batch.num_columns().saturating_sub(1))
        .ok_or_else(|| CdfError::data("ClickHouse canonical batch lost logical columns"))?
        .to_vec();
    columns.push(std::sync::Arc::new(package_hashes) as ArrayRef);
    columns.push(
        batch
            .columns()
            .last()
            .cloned()
            .ok_or_else(|| CdfError::data("ClickHouse canonical batch lost ordinal values"))?,
    );
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )
    .map_err(|_| CdfError::internal("ClickHouse physical batch construction violated its schema"))
}

pub(crate) fn decode_package_hash(package_hash: &cdf_kernel::PackageHash) -> Result<[u8; 32]> {
    let text = package_hash
        .as_str()
        .strip_prefix("sha256:")
        .unwrap_or(package_hash.as_str());
    if text.len() != 64 || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CdfError::data(
            "ClickHouse package hash must contain exactly 64 hexadecimal SHA-256 digits",
        ));
    }
    let mut output = [0_u8; 32];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(output)
}

pub(crate) fn package_hash_hex(package_hash: &cdf_kernel::PackageHash) -> Result<String> {
    decode_package_hash(package_hash)?;
    Ok(package_hash
        .as_str()
        .strip_prefix("sha256:")
        .unwrap_or(package_hash.as_str())
        .to_ascii_uppercase())
}

fn hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(CdfError::data(
            "ClickHouse package hash contains non-hex text",
        )),
    }
}

pub(crate) fn state_sha256(state: Option<&cdf_kernel::StateDelta>) -> Result<String> {
    let bytes = serde_json::to_vec(&state).map_err(|error| {
        CdfError::internal(format!("encode ClickHouse state mirror evidence: {error}"))
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
